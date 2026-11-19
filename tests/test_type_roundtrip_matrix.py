"""Every contract type, every engine, every write target: what goes in comes back (2026-11-19).

Bugs found by agents within seconds of a release - timestamp_tz losing its zone in the cast, the
Postgres writer failing on every naive timestamp - were all one class: a type nobody's test used.
This matrix writes a row set covering EVERY registry type (with edge values and nulls) through the
real DataProcessor, on Polars and DuckDB, to every target, merges it twice, reads it back and
compares value by value.

Server databases run when LAKELOGIC_TEST_PG_URL / _MYSQL_URL / _MSSQL_URL name throwaway
databases; DuckDB, SQLite, Parquet and Delta always run.
"""
from __future__ import annotations

import datetime as dt
import json
import math
import os
import uuid
from decimal import Decimal

import pytest

pl = pytest.importorskip("polars")

from lakelogic.core import db_writer  # noqa: E402

PLUS2 = dt.timezone(dt.timedelta(hours=2))

# name -> (contract type, polars input values, expected values as plain Python)
COLUMNS = {
    "c_string": ("string", ["héllo ✓ 日本", "", None], ["héllo ✓ 日本", "", None]),
    "c_varchar": ("varchar", ["abc", "x" * 50, None], ["abc", "x" * 50, None]),
    "c_tinyint": ("tinyint", [127, -128, None], [127, -128, None]),
    "c_smallint": ("smallint", [32767, -32768, None], [32767, -32768, None]),
    "c_int": ("int", [2147483647, -1, None], [2147483647, -1, None]),
    "c_bigint": ("bigint", [9007199254740993, -5, None], [9007199254740993, -5, None]),
    "c_float": ("float", [1.5, -0.25, None], [1.5, -0.25, None]),
    "c_double": ("double", [1234567.890123, -1e-9, None], [1234567.890123, -1e-9, None]),
    "c_decimal": ("decimal(18,4)", [Decimal("12345.6789"), Decimal("-0.0001"), None],
                  [Decimal("12345.6789"), Decimal("-0.0001"), None]),
    "c_boolean": ("boolean", [True, False, None], [True, False, None]),
    "c_date": ("date", [dt.date(2026, 11, 18), dt.date(1999, 1, 1), None],
               [dt.date(2026, 11, 18), dt.date(1999, 1, 1), None]),
    "c_ts": ("timestamp", [dt.datetime(2026, 11, 18, 9, 30, 15, 123456), dt.datetime(2000, 1, 1), None],
             [dt.datetime(2026, 11, 18, 9, 30, 15, 123456), dt.datetime(2000, 1, 1), None]),
    "c_ts_ntz": ("timestamp_ntz", [dt.datetime(2026, 11, 18, 9, 30), dt.datetime(1970, 1, 2), None],
                 [dt.datetime(2026, 11, 18, 9, 30), dt.datetime(1970, 1, 2), None]),
    "c_ts_tz": ("timestamp_tz", [dt.datetime(2026, 11, 18, 17, 0, tzinfo=PLUS2),
                                 dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc), None],
                [dt.datetime(2026, 11, 18, 15, 0), dt.datetime(2026, 1, 1), None]),   # compared in UTC
    "c_binary": ("binary", [b"\x00\x01\xff", b"", None], [b"\x00\x01\xff", b"", None]),
    "c_json": ("json", ['{"a": 1, "b": [1, 2]}', "[1, 2]", None], [{"a": 1, "b": [1, 2]}, [1, 2], None]),
}


def _frame():
    data = {"id": [1, 2, 3]}
    for name, (_t, values, _e) in COLUMNS.items():
        data[name] = values
    schema = {"id": pl.Int64}
    for name, (ctype, values, _e) in COLUMNS.items():
        if ctype == "tinyint":
            schema[name] = pl.Int8
        elif ctype == "smallint":
            schema[name] = pl.Int16
        elif ctype == "int":
            schema[name] = pl.Int32
        elif ctype == "float":
            schema[name] = pl.Float32
        elif ctype.startswith("decimal"):
            schema[name] = pl.Decimal(18, 4)
        elif ctype == "timestamp_tz":
            schema[name] = pl.Datetime("us", "UTC")
    df = pl.DataFrame(data, schema_overrides=schema, strict=False)
    return df


def _contract(materialization: dict) -> dict:
    fields = [{"name": "id", "type": "bigint"}] + [{"name": n, "type": t} for n, (t, _v, _e) in COLUMNS.items()]
    return {"version": "1.0.0", "info": {"title": "types"}, "dataset": "types", "primary_key": ["id"],
            "model": {"fields": fields}, "materialization": materialization}


# ── reading back, normalised to plain Python ────────────────────────────────────────────────── #

def _norm(ctype: str, value):
    if value is None:
        return None
    if ctype in ("tinyint", "smallint", "int", "bigint"):
        return int(value)
    if ctype in ("float", "double"):
        return float(value)
    if ctype.startswith("decimal"):
        return Decimal(str(value)).quantize(Decimal("0.0001"))
    if ctype == "boolean":
        return bool(int(value)) if isinstance(value, (int, str)) and str(value) in ("0", "1") else bool(value)
    if ctype == "date":
        return value if isinstance(value, dt.date) and not isinstance(value, dt.datetime) else \
            (value.date() if isinstance(value, dt.datetime) else dt.date.fromisoformat(str(value)[:10]))
    if ctype.startswith("timestamp"):
        if not isinstance(value, dt.datetime):
            value = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if value.tzinfo is not None:
            value = value.astimezone(dt.timezone.utc).replace(tzinfo=None)
        return value
    if ctype == "binary":
        return bytes(value)
    if ctype == "json":
        return value if isinstance(value, (dict, list)) else json.loads(value)
    return value


def _same(ctype: str, got, want) -> bool:
    if got is None or want is None:
        return got is None and want is None
    if ctype == "float":
        return math.isclose(got, want, rel_tol=1e-6)
    if ctype == "double":
        return math.isclose(got, want, rel_tol=1e-12, abs_tol=1e-15)
    return got == want


def _check(rows_by_id: dict, skip=frozenset()) -> list:
    """Every mismatch as 'column: id N got X, expected Y'."""
    problems = []
    for name, (ctype, _v, expected) in COLUMNS.items():
        if name in skip:
            continue
        for i, want in enumerate(expected, start=1):
            row = rows_by_id.get(i)
            if row is None:
                problems.append(f"row id {i} missing")
                continue
            try:
                got = _norm(ctype, row.get(name))
            except Exception as exc:  # noqa: BLE001
                problems.append(f"{name} ({ctype}): id {i} unreadable {row.get(name)!r}: {exc}")
                continue
            if not _same(ctype, got, _norm(ctype, want) if ctype != "timestamp_tz" else want):
                problems.append(f"{name} ({ctype}): id {i} got {got!r}, expected {want!r}")
    return sorted(set(problems))


# ── targets ─────────────────────────────────────────────────────────────────────────────────── #

SERVER_URLS = {"postgresql": os.environ.get("LAKELOGIC_TEST_PG_URL"),
               "mysql": os.environ.get("LAKELOGIC_TEST_MYSQL_URL"),
               "mssql": os.environ.get("LAKELOGIC_TEST_MSSQL_URL")}


def _db_rows(url_env: str, table: str):
    writer = db_writer.writer_for(f"env:{url_env}")
    if hasattr(writer, "_exec"):  # duckdb / sqlite
        cur = writer._exec(f"SELECT * FROM {table}")
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    conn = writer._connection()
    if writer.dialect == "mssql":
        import struct

        def _dto(raw):
            y, mo, d, h, mi, s, ns, tzh, tzm = struct.unpack("<6hI2h", raw)
            return dt.datetime(y, mo, d, h, mi, s, ns // 1000, dt.timezone(dt.timedelta(hours=tzh, minutes=tzm)))

        conn.add_output_converter(-155, _dto)
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM {table}")
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.commit()
    return rows


TARGETS = ["postgresql", "mysql", "mssql", "duckdb", "sqlite", "parquet", "delta"]


def _text_frame():
    """The same rows as text, the way a CSV landing file arrives (bronze reads everything as
    strings); the contract's types must cast them back to the same values."""
    import base64  # noqa: F401  (binary is not text-castable; left out of the text variant)

    df = _frame()
    out = {}
    for name in df.columns:
        if name == "c_binary":
            continue
        s = df[name]
        if s.dtype == pl.Datetime("us", "UTC"):
            out[name] = s.dt.strftime("%Y-%m-%dT%H:%M:%S%.f+00:00")
        elif isinstance(s.dtype, pl.Datetime):
            out[name] = s.dt.strftime("%Y-%m-%d %H:%M:%S%.f")
        else:
            out[name] = s.cast(pl.String)
    return pl.DataFrame(out)


@pytest.mark.parametrize("engine", ["polars", "duckdb"])
@pytest.mark.parametrize("target", TARGETS)
@pytest.mark.parametrize("strategy", ["merge", "append", "overwrite"])
@pytest.mark.parametrize("source", ["typed", "text"])
def test_every_type_round_trips(engine, target, strategy, source, tmp_path, monkeypatch):
    from lakelogic import DataProcessor

    db_writer._WRITERS.clear()
    if target in SERVER_URLS:
        if not SERVER_URLS[target]:
            pytest.skip(f"set LAKELOGIC_TEST_{ {'postgresql': 'PG'}.get(target, target.upper()) }_URL")
        monkeypatch.setenv("LL_RT_URL", SERVER_URLS[target])
        table = f"ll_rt.t_{engine}_{uuid.uuid4().hex[:6]}"
        mat = {"format": "database", "connection": "env:LL_RT_URL", "table": table, "strategy": strategy}
    elif target in ("duckdb", "sqlite"):
        monkeypatch.setenv("LL_RT_URL", f"{target}:///{(tmp_path / ('rt.' + target)).as_posix()}")
        table = "ll_rt.t" if target == "duckdb" else "t"
        mat = {"format": "database", "connection": "env:LL_RT_URL", "table": table, "strategy": strategy}
    else:
        if target == "delta":
            pytest.importorskip("deltalake")
        if target == "parquet" and strategy == "merge":
            pytest.skip("parquet files have no merge")
        mat = {"format": target, "target_path": str(tmp_path / "out"), "strategy": strategy}

    contract = _contract(mat)
    if source == "text":
        contract["model"]["fields"] = [f for f in contract["model"]["fields"] if f["name"] != "c_binary"]
    frame = _frame() if source == "typed" else _text_frame()
    runs = 1 if strategy == "append" else 2   # merge/overwrite twice: idempotent, no type drift
    for _ in range(runs):
        proc = DataProcessor(contract, engine=engine)
        result = proc.run(frame)
        assert len(result.bad) == 0, f"rows quarantined: {result.bad.to_dicts()[:1]}"
        proc.materialize(result.good, result.bad)

    if target in ("parquet", "delta"):
        frame = pl.read_delta(str(tmp_path / "out")) if target == "delta" else pl.read_parquet(str(tmp_path / "out" / "**" / "*.parquet"))
        rows = frame.to_dicts()
    else:
        rows = _db_rows("LL_RT_URL", table)
    by_id = {int(r["id"]): r for r in rows}
    assert len(rows) == 3, f"{len(rows)} rows after two merges"
    problems = _check(by_id, skip={"c_binary"} if source == "text" else set())
    for w in db_writer._WRITERS.values():
        w.close()
    db_writer._WRITERS.clear()
    assert not problems, "\n".join(problems)
