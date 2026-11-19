"""format: database - the native writer, one suite for every adapter (2026-11-18).

Postgres runs when LAKELOGIC_TEST_PG_URL names a THROWAWAY database (tables under schema
ll_test are dropped); DuckDB and SQLite always run, in a temp folder.
"""
from __future__ import annotations

import os
import uuid

import pytest

pl = pytest.importorskip("polars")

from lakelogic.core import db_writer  # noqa: E402
from lakelogic.core.db_writer import DatabaseWriteError, writer_for  # noqa: E402

PG = os.environ.get("LAKELOGIC_TEST_PG_URL")
MYSQL = os.environ.get("LAKELOGIC_TEST_MYSQL_URL")   # mysql://user:pw@host:port/db (throwaway)
MSSQL = os.environ.get("LAKELOGIC_TEST_MSSQL_URL")   # mssql://user:pw@host:port/db?TrustServerCertificate=yes


@pytest.fixture(params=["postgresql", "mysql", "mssql", "duckdb", "sqlite"])
def target(request, tmp_path, monkeypatch):
    """(connection reference, table, query(sql) -> rows) for one adapter, fresh each test."""
    db_writer._WRITERS.clear()
    if request.param in ("mysql", "mssql"):
        url = MYSQL if request.param == "mysql" else MSSQL
        if not url:
            pytest.skip(f"set LAKELOGIC_TEST_{request.param.upper()}_URL to a throwaway database")
        monkeypatch.setenv("LL_DB_URL", url)
        table = f"ll_test.t_{uuid.uuid4().hex[:8]}"

        def query(sql_text):
            writer = writer_for("env:LL_DB_URL")
            conn = writer._connection()
            if request.param == "mssql":  # pyodbc cannot fetch DATETIMEOFFSET without a converter
                import datetime as _dt
                import struct

                def _dto(raw):
                    y, mo, d, h, mi, s, ns, tzh, tzm = struct.unpack("<6hI2h", raw)
                    return _dt.datetime(y, mo, d, h, mi, s, ns // 1000, _dt.timezone(_dt.timedelta(hours=tzh, minutes=tzm)))

                conn.add_output_converter(-155, _dto)
            cur = conn.cursor()
            cur.execute(sql_text)
            rows = [tuple(r) for r in cur.fetchall()]
            writer._connection().commit()
            return rows
    elif request.param == "postgresql":
        if not PG:
            pytest.skip("set LAKELOGIC_TEST_PG_URL to a throwaway Postgres to run the Postgres adapter")
        import psycopg2

        monkeypatch.setenv("LL_DB_URL", PG)
        table = f"ll_test.t_{uuid.uuid4().hex[:8]}"

        def query(sql_text):
            conn = psycopg2.connect(PG)
            try:
                with conn.cursor() as cur:
                    cur.execute(sql_text)
                    return cur.fetchall()
            finally:
                conn.close()
    else:
        path = (tmp_path / f"t.{request.param}").as_posix()
        monkeypatch.setenv("LL_DB_URL", f"{request.param}:///{path}")
        table = "ll_test.t" if request.param == "duckdb" else "t"

        def query(sql_text):
            writer = writer_for("env:LL_DB_URL")
            return writer._exec(sql_text).fetchall()
    yield "env:LL_DB_URL", table, query
    for w in db_writer._WRITERS.values():
        w.close()
    db_writer._WRITERS.clear()


def _bars(rows):
    return pl.DataFrame(rows, schema=["symbol", "ts", "px"], orient="row")


def _write(conn, table, df, **kw):
    return writer_for(conn).write(df, table=table, **kw)


def test_append_creates_the_table_and_writes(target):
    conn, table, query = target
    out = _write(conn, table, _bars([("A", 1, 1.0), ("B", 1, 2.0)]), primary_key=["symbol", "ts"])
    assert out["rows_written"] == 2
    assert query(f"SELECT count(*) FROM {table}")[0][0] == 2


def test_append_twice_with_a_key_errors_and_writes_nothing_more(target):
    conn, table, query = target
    df = _bars([("A", 1, 1.0)])
    _write(conn, table, df, primary_key=["symbol", "ts"])
    with pytest.raises(DatabaseWriteError):
        _write(conn, table, _bars([("A", 1, 1.0), ("Z", 9, 9.0)]), primary_key=["symbol", "ts"])
    assert query(f"SELECT count(*) FROM {table}")[0][0] == 1          # rolled back, Z not written


def test_append_on_conflict_ignore_keeps_the_count(target):
    conn, table, query = target
    df = _bars([("A", 1, 1.0), ("B", 1, 2.0)])
    _write(conn, table, df, primary_key=["symbol", "ts"])
    _write(conn, table, df, primary_key=["symbol", "ts"], write={"on_conflict": "ignore"})
    assert query(f"SELECT count(*) FROM {table}")[0][0] == 2


def test_a_retried_batch_replaces_itself(target):
    conn, table, query = target
    df = _bars([("A", 1, 1.0), ("B", 1, 2.0)])
    _write(conn, table, df, batch_key="file-1@2026-11-18")
    _write(conn, table, df, batch_key="file-1@2026-11-18")           # retry: same key
    _write(conn, table, _bars([("C", 1, 3.0)]), batch_key="file-2")
    assert query(f"SELECT count(*) FROM {table}")[0][0] == 3


def test_merge_inserts_updates_and_skips_unchanged(target):
    conn, table, query = target
    pk = ["symbol", "ts"]
    _write(conn, table, _bars([("A", 1, 1.0), ("A", 2, 2.0)]), strategy="merge", primary_key=pk)
    out = _write(conn, table, _bars([("A", 1, 1.0), ("A", 2, 2.5), ("B", 1, 9.0)]), strategy="merge", primary_key=pk)
    rows = sorted(query(f"SELECT symbol, ts, px FROM {table}"))
    assert rows == [("A", 1, 1.0), ("A", 2, 2.5), ("B", 1, 9.0)]
    if out.get("updated") is not None:                                 # Postgres counts them
        assert out["inserted"] == 1 and out["updated"] == 1


def test_merge_of_unchanged_rows_rewrites_nothing_on_postgres(target):
    conn, table, query = target
    if not os.environ.get("LL_DB_URL", "").startswith("postgres"):
        pytest.skip("row versions (xmin) are a Postgres check")
    pk = ["symbol", "ts"]
    df = _bars([("A", 1, 1.0), ("B", 1, 2.0)])
    _write(conn, table, df, strategy="merge", primary_key=pk)
    before = sorted(query(f"SELECT symbol, xmin::text FROM {table}"))
    out = _write(conn, table, df, strategy="merge", primary_key=pk)
    assert out["updated"] == 0 and sorted(query(f"SELECT symbol, xmin::text FROM {table}")) == before


def test_merge_refuses_duplicate_keys_in_the_batch(target):
    conn, table, query = target
    _write(conn, table, _bars([("A", 1, 1.0)]), strategy="merge", primary_key=["symbol", "ts"])
    with pytest.raises(DatabaseWriteError, match="more than once"):
        _write(conn, table, _bars([("A", 1, 5.0), ("A", 1, 6.0)]), strategy="merge", primary_key=["symbol", "ts"])
    assert query(f"SELECT px FROM {table}") == [(1.0,)]


def test_merge_needs_a_primary_key(target):
    conn, table, _ = target
    with pytest.raises(DatabaseWriteError, match="primary_key"):
        _write(conn, table, _bars([("A", 1, 1.0)]), strategy="merge")


def test_overwrite_replaces_and_refuses_an_empty_batch(target):
    conn, table, query = target
    _write(conn, table, _bars([("A", 1, 1.0), ("B", 1, 2.0)]))
    _write(conn, table, _bars([("C", 1, 3.0)]), strategy="overwrite")
    assert query(f"SELECT symbol FROM {table}") == [("C",)]
    with pytest.raises(DatabaseWriteError, match="0 rows"):
        _write(conn, table, _bars([("C", 1, 3.0)]).clear(), strategy="overwrite")
    assert query(f"SELECT count(*) FROM {table}")[0][0] == 1


def test_a_new_column_is_added_and_strict_refuses_it(target):
    conn, table, query = target
    _write(conn, table, _bars([("A", 1, 1.0)]))
    wider = _bars([("B", 1, 2.0)]).with_columns(pl.lit("x").alias("venue"))
    with pytest.raises(DatabaseWriteError, match="strict"):
        _write(conn, table, wider, evolution="strict")
    _write(conn, table, wider)
    assert query(f"SELECT count(*) FROM {table}")[0][0] == 2


def test_credentials_never_appear_in_errors(monkeypatch):
    db_writer._WRITERS.clear()
    monkeypatch.setenv("LL_BAD_URL", "postgresql://user:S3cretPw@127.0.0.1:1/nodb")
    with pytest.raises(DatabaseWriteError) as err:
        writer_for("env:LL_BAD_URL").write(_bars([("A", 1, 1.0)]), table="x.t")
    assert "S3cretPw" not in str(err.value)
    db_writer._WRITERS.clear()


def test_an_unsupported_database_says_to_use_dlt(monkeypatch):
    db_writer._WRITERS.clear()
    monkeypatch.setenv("LL_ORA", "oracle://u:p@h/db")
    with pytest.raises(DatabaseWriteError, match="format: dlt"):
        writer_for("env:LL_ORA")


def test_unknown_write_options_are_rejected():
    from types import SimpleNamespace

    with pytest.raises(DatabaseWriteError, match="Unknown write option"):
        db_writer.database_config(SimpleNamespace(connection="env:X", table="t", write={"turbo": True}))


def test_processor_writes_output_quarantine_and_run_log_through_the_writer(target, tmp_path):
    """End to end: format: database for the output, the quarantine and the run log."""
    from lakelogic import DataProcessor

    conn, table, query = target
    prefix = table.rsplit(".", 1)[0] + "." if "." in table else ""
    contract = {
        "version": "1.0.0", "info": {"title": "bars"}, "dataset": "bars", "primary_key": ["symbol", "ts"],
        "model": {"fields": [{"name": "symbol", "type": "string"}, {"name": "ts", "type": "integer"},
                             {"name": "close", "type": "double"}]},
        "quality": {"row_rules": [{"name": "close_positive", "sql": "close > 0"}]},
        "materialization": {"format": "database", "connection": conn, "table": table, "strategy": "merge"},
        "quarantine": {"format": "database", "connection": conn, "table": f"{prefix}q_{table.rsplit('.', 1)[-1]}"},
        "metadata": {"run_log_table": f"{prefix}log_{table.rsplit('.', 1)[-1]}", "run_log_backend": "database",
                     "run_log_connection": conn},
    }
    proc = DataProcessor(contract, engine="polars")
    result = proc.run(pl.DataFrame({"symbol": ["A", "B"], "ts": [1, 1], "close": [1.0, -5.0]}))
    proc.materialize(result.good, result.bad)
    assert proc.write_run_log()
    assert query(f"SELECT symbol FROM {table}") == [("A",)]
    assert query(f"SELECT symbol FROM {contract['quarantine']['table']}") == [("B",)]
    assert query(f"SELECT status FROM {contract['metadata']['run_log_table']}") == [("success",)]


def test_timestamp_and_timestamp_tz_columns_are_written(target):
    """A timestamp_tz lost its zone in the cast and the Postgres writer formatted every timestamp
    with an offset, so ANY contract with a timestamp column failed to write (2026-11-19)."""
    import datetime as dt

    from lakelogic import DataProcessor

    conn, table, query = target
    contract = {
        "version": "1.0.0", "info": {"title": "bars"}, "dataset": "bars", "primary_key": ["symbol", "ts"],
        "model": {"fields": [{"name": "symbol", "type": "string"}, {"name": "ts", "type": "timestamp_tz"},
                             {"name": "seen", "type": "timestamp"}, {"name": "px", "type": "double"}]},
        "materialization": {"format": "database", "connection": conn, "table": table, "strategy": "merge"},
    }
    plus2 = dt.timezone(dt.timedelta(hours=2))
    df = pl.DataFrame({"symbol": ["A"], "ts": [dt.datetime(2026, 11, 18, 17, 0, tzinfo=plus2)],
                       "seen": [dt.datetime(2026, 11, 18, 9, 30)], "px": [1.0]})
    proc = DataProcessor(contract, engine="polars")
    result = proc.run(df)
    assert result.good.schema["ts"] == pl.Datetime("us", "UTC")          # the zone survives the cast
    proc.materialize(result.good, result.bad)
    proc.materialize(result.good, result.bad)                            # merge again: no error, no dupes
    rows = query(f"SELECT ts, seen FROM {table}")
    assert len(rows) == 1
    ts, seen = rows[0]
    ts = ts if isinstance(ts, dt.datetime) else dt.datetime.fromisoformat(str(ts))
    utc = ts.astimezone(dt.timezone.utc).replace(tzinfo=None) if ts.tzinfo else ts
    assert utc == dt.datetime(2026, 11, 18, 15, 0)                        # 17:00+02:00 is 15:00 UTC
    seen = seen if isinstance(seen, dt.datetime) else dt.datetime.fromisoformat(str(seen))
    assert seen.replace(tzinfo=None) == dt.datetime(2026, 11, 18, 9, 30)
