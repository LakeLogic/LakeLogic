"""Write-safety bugs an adversarial agent test found (2026-11-19). Each one lost or blocked data.

1. A retried append from ONE named file (no wildcard) doubled its rows: no retry key.
2. run(df) after run_source() reused the file's retry key, so the DataFrame's write DELETED the
   file's rows.
3. A NULL primary key passed validation, then failed the whole batch's database write.
4. NaN / infinity failed MySQL and SQL Server mid-write with a driver message.
5. quarantine_scope let a failing group through when its scope value was NULL.
6. A merge whose batch lacked a declared column overwrote the stored values with NULL.
"""

from __future__ import annotations

import os

import pytest

pl = pytest.importorskip("polars")

from lakelogic import DataProcessor  # noqa: E402
from lakelogic.core import db_writer  # noqa: E402


@pytest.fixture(autouse=True)
def _fresh_writers():
    db_writer._WRITERS.clear()
    yield
    for w in db_writer._WRITERS.values():
        w.close()
    db_writer._WRITERS.clear()


def _duck(tmp_path, monkeypatch):
    monkeypatch.setenv("LL_WS_URL", f"duckdb:///{(tmp_path / 'wsdb.duckdb').as_posix()}")
    return {"format": "database", "connection": "env:LL_WS_URL", "table": "ws.t"}


def _count(table="ws.t"):
    return db_writer.writer_for("env:LL_WS_URL")._exec(f"SELECT COUNT(*) FROM {table}").fetchone()[0]


def _rows(sql):
    return db_writer.writer_for("env:LL_WS_URL")._exec(sql).fetchall()


def _contract(mat, fields=(("sym", "string"), ("px", "double")), pk=None, **extra):
    c = {
        "version": "1.0.0",
        "info": {"title": "ws"},
        "dataset": "ws",
        "model": {"fields": [{"name": n, "type": t} for n, t in fields]},
        "materialization": mat,
    }
    if pk:
        c["primary_key"] = pk
    c.update(extra)
    return c


def test_retried_append_from_one_named_file_does_not_double(tmp_path, monkeypatch):
    csv = tmp_path / "trades.csv"
    csv.write_text("sym,px\nA,1.0\nB,2.0\n", encoding="utf-8")
    c = _contract(
        {**_duck(tmp_path, monkeypatch), "strategy": "append"},
        source={"type": "landing", "path": str(csv), "format": "csv"},
    )
    for _ in range(2):
        p = DataProcessor(c, engine="polars")
        r = p.run_source()
        p.materialize(r.good, r.bad)
    assert _count() == 2


def test_dataframe_run_after_run_source_does_not_delete_the_files_rows(tmp_path, monkeypatch):
    csv = tmp_path / "trades.csv"
    csv.write_text("sym,px\nA,1.0\nB,2.0\n", encoding="utf-8")
    c = _contract(
        {**_duck(tmp_path, monkeypatch), "strategy": "append"},
        source={"type": "landing", "path": str(csv), "format": "csv"},
    )
    p = DataProcessor(c, engine="polars")
    r = p.run_source()
    p.materialize(r.good, r.bad)
    for sym in ("X", "Y"):
        r = p.run(pl.DataFrame({"sym": [sym], "px": [9.0]}))
        p.materialize(r.good, r.bad)
    assert _count() == 4


def test_null_primary_key_is_quarantined_not_a_failed_batch(tmp_path, monkeypatch):
    c = _contract({**_duck(tmp_path, monkeypatch), "strategy": "merge"}, pk=["sym"])
    p = DataProcessor(c, engine="polars")
    r = p.run(pl.DataFrame({"sym": ["A", None, "B"], "px": [1.0, 2.0, 3.0]}))
    assert len(r.good) == 2 and len(r.bad) == 1
    p.materialize(r.good, r.bad)
    assert _count() == 2


def test_null_scope_group_that_fails_is_quarantined():
    c = _contract(
        {"format": "parquet", "target_path": "unused", "strategy": "append"},
        quality={
            "on_dataset_failure": "quarantine",
            "quarantine_scope": ["sym"],
            "dataset_rules": [{"name": "px_positive", "sql": "SELECT MIN(px) FROM source", "must_be_greater_than": 0}],
        },
    )
    r = DataProcessor(c, engine="polars").run(
        pl.DataFrame({"sym": ["A", "A", None, None], "px": [1.0, 2.0, -1.0, 3.0]})
    )
    assert sorted(r.good["sym"].to_list()) == ["A", "A"]
    assert len(r.bad) == 2


@pytest.mark.parametrize("fmt", ["database", "delta"])
def test_merge_without_a_declared_column_keeps_its_stored_values(fmt, tmp_path, monkeypatch):
    if fmt == "delta":
        pytest.importorskip("deltalake")
        mat = {"format": "delta", "target_path": str(tmp_path / "dl"), "strategy": "merge"}
    else:
        mat = {**_duck(tmp_path, monkeypatch), "strategy": "merge"}
    c = _contract(mat, fields=(("sym", "string"), ("hi", "double"), ("low", "double")), pk=["sym"])
    p = DataProcessor(c, engine="polars")
    r = p.run(pl.DataFrame({"sym": ["A"], "hi": [1.0], "low": [99.0]}))
    p.materialize(r.good, r.bad)
    p = DataProcessor(c, engine="polars")
    r = p.run(pl.DataFrame({"sym": ["A", "B"], "hi": [2.0, 5.0]}))
    p.materialize(r.good, r.bad)
    if fmt == "delta":
        got = sorted((d["sym"], d["hi"], d["low"]) for d in pl.read_delta(str(tmp_path / "dl")).to_dicts())
    else:
        got = sorted(_rows("SELECT sym, hi, low FROM ws.t"))
    assert got == [("A", 2.0, 99.0), ("B", 5.0, None)]


@pytest.mark.parametrize("env", ["LAKELOGIC_TEST_MYSQL_URL", "LAKELOGIC_TEST_MSSQL_URL"])
def test_nan_and_infinity_are_refused_before_writing(env, monkeypatch):
    url = os.environ.get(env)
    if not url:
        pytest.skip(f"set {env}")
    monkeypatch.setenv("LL_WS_URL", url)
    w = db_writer.writer_for("env:LL_WS_URL")
    with pytest.raises(db_writer.DatabaseWriteError, match=r"NaN or infinity.*px \(2 row"):
        w.write(
            pl.DataFrame({"sym": ["A", "B", "C"], "px": [1.0, float("nan"), float("inf")]}),
            table="ll_ws.nan_t",
            strategy="append",
        )


def test_duckdb_schema_named_like_the_file_is_written(tmp_path, monkeypatch):
    """sales.duckdb with schema sales: "sales"."t" was ambiguous and every write failed."""
    monkeypatch.setenv("LL_WS_URL", f"duckdb:///{(tmp_path / 'sales.duckdb').as_posix()}")
    w = db_writer.writer_for("env:LL_WS_URL")
    for _ in range(2):
        w.write(pl.DataFrame({"k": [1, 2], "v": ["a", "b"]}), table="sales.t", strategy="merge", primary_key=["k"])
    assert w._exec("SELECT COUNT(*) FROM sales.sales.t").fetchone()[0] == 2


def _bars_contract(mat, qmat):
    return {
        "version": "1.0.0",
        "info": {"title": "bars"},
        "dataset": "bars",
        "primary_key": ["sym", "i"],
        "model": {
            "fields": [
                {"name": "sym", "type": "string"},
                {"name": "i", "type": "int"},
                {"name": "lo", "type": "double"},
                {"name": "hi", "type": "double"},
            ]
        },
        "quality": {
            "on_dataset_failure": "quarantine",
            "quarantine_scope": ["sym"],
            "dataset_rules": [
                {"name": "hi_ge_lo", "sql": "SELECT COUNT(*) FROM source WHERE hi < lo", "must_be_less_than": 1}
            ],
        },
        "quarantine": qmat,
        "materialization": mat,
    }


def test_rejected_groups_are_reported_and_not_requarantined_on_rerun(tmp_path, monkeypatch):
    """Run log said `success` with groups set aside; and each re-run of the same DataFrame added
    its rejects again (13, 26, 39 rows)."""
    mat = {**_duck(tmp_path, monkeypatch), "strategy": "merge"}
    c = _bars_contract(mat, {"format": "database", "connection": "env:LL_WS_URL", "table": "ws.rejects"})
    df = pl.DataFrame(
        {"sym": ["A"] * 3 + ["B"] * 3, "i": [1, 2, 3] * 2, "lo": [1.0] * 6, "hi": [2.0, 2.0, 2.0, 0.5, 2.0, 2.0]}
    )
    for _ in range(3):
        p = DataProcessor(c, engine="polars")
        r = p.run(df)
        p.materialize(r.good, r.bad)
        assert p.last_report["status"] == "success_with_rejects"
        assert "B (hi_ge_lo)" in p.last_report["groups_quarantined_reason"]
        assert not p.last_report.get("error_message")
        from lakelogic.core.run_log import _flatten_report

        assert "B (hi_ge_lo)" in _flatten_report(p.last_report)["rejects_reason"]
    assert _count("ws.t") == 3
    assert _count("ws.rejects") == 3


@pytest.mark.parametrize("env", ["LAKELOGIC_TEST_PG_URL", "LAKELOGIC_TEST_MYSQL_URL", "LAKELOGIC_TEST_MSSQL_URL"])
def test_table_dropped_outside_lakelogic_is_recreated(env, monkeypatch):
    """The table cache outlived a migration that dropped the table: "does not exist" until restart."""
    url = os.environ.get(env)
    if not url:
        pytest.skip(f"set {env}")
    monkeypatch.setenv("LL_WS_URL", url)
    w = db_writer.writer_for("env:LL_WS_URL")
    df = pl.DataFrame({"k": [1], "v": ["a"]})
    w.write(df, table="ll_ws.dropped", strategy="merge", primary_key=["k"])
    conn = w._connect() if hasattr(w, "_connect") else w._connection()
    cur = conn.cursor()
    cur.execute("DROP TABLE ll_ws.dropped")
    conn.commit()
    assert w.write(df, table="ll_ws.dropped", strategy="merge", primary_key=["k"])["rows_written"] == 1


@pytest.mark.parametrize(
    "sql,cmp",
    [
        ("SELECT COUNT(*) FROM source WHERE hi < lo", {"must_be_less_than": 1}),
        ("SELECT MIN(lo) FROM source WHERE hi > 100", {"must_be_greater_than": 0}),  # empty WHERE -> NULL -> fails
        ("SELECT AVG(hi) FROM source", {"must_be_between": [1.0, 3.0]}),
        ("SELECT COUNT(*) FROM (SELECT * FROM source WHERE hi < lo) x", {"must_be_less_than": 1}),  # loop path
    ],
)
def test_grouped_scope_check_matches_group_by_group(sql, cmp):
    """The one-query GROUP BY path must reject exactly the groups the per-group loop rejects."""
    from lakelogic.core import processor as proc_mod

    df = pl.DataFrame(
        {
            "sym": ["A", "A", "B", "B", None, None, "C"],
            "lo": [1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            "hi": [2.0, 2.0, 0.5, 2.0, 0.2, 9.0, 150.0],
        }
    )
    rule = type(
        "R",
        (),
        {
            "name": "r",
            "sql": sql,
            "must_be_between": None,
            "must_be_less_than": None,
            "must_be_greater_than": None,
            **cmp,
        },
    )()
    adapter = type("A", (), {"get_dataset_rules": lambda self: [rule]})()
    fast = proc_mod._quarantine_failing_groups(adapter, df, [{"name": "r"}], ["sym"], "bars")
    orig = proc_mod._grouped_rule_values
    try:
        proc_mod._grouped_rule_values = lambda *a, **k: None
        slow = proc_mod._quarantine_failing_groups(adapter, df, [{"name": "r"}], ["sym"], "bars")
    finally:
        proc_mod._grouped_rule_values = orig

    def norm(res):
        return None if res is None else (sorted(map(str, res[1]["sym"].to_list())), res[2])

    assert norm(fast) == norm(slow)


def test_duckdb_run_log_schema_named_like_the_file(tmp_path):
    """ws.duckdb with run_log_table ws.runs: "ws.runs" was ambiguous in the run log too."""
    c = _contract(
        {"format": "parquet", "target_path": str(tmp_path / "out"), "strategy": "append"},
        metadata={
            "run_log_backend": "duckdb",
            "run_log_table": "ws.runs",
            "run_log_database": str(tmp_path / "ws.duckdb"),
        },
    )
    for _ in range(2):
        p = DataProcessor(c, engine="polars")
        r = p.run(pl.DataFrame({"sym": ["A"], "px": [1.0]}))
        p.materialize(r.good, r.bad)
        p.write_run_log()
    import duckdb

    con = duckdb.connect(str(tmp_path / "ws.duckdb"))
    assert con.execute("SELECT COUNT(*) FROM ws.ws.runs").fetchone()[0] == 2
    con.close()


@pytest.mark.parametrize("backend", ["duckdb", "sqlite"])
def test_rejects_reason_reaches_the_run_log_table(backend, tmp_path):
    c = _bars_contract(
        {"format": "parquet", "target_path": str(tmp_path / "out"), "strategy": "append"},
        {"format": "parquet", "target": str(tmp_path / "q")},
    )
    c["metadata"] = {
        "run_log_backend": backend,
        "run_log_table": "runs",
        "run_log_database": str(tmp_path / f"rl.{backend}"),
    }
    p = DataProcessor(c, engine="polars")
    r = p.run(pl.DataFrame({"sym": ["A", "B"], "i": [1, 1], "lo": [1.0, 1.0], "hi": [2.0, 0.5]}))
    p.materialize(r.good, r.bad)
    p.write_run_log()
    if backend == "duckdb":
        import duckdb

        con = duckdb.connect(str(tmp_path / "rl.duckdb"))
    else:
        import sqlite3

        con = sqlite3.connect(str(tmp_path / "rl.sqlite"))
    status, reason, err = con.execute("SELECT status, rejects_reason, error_message FROM runs").fetchone()
    con.close()
    assert status == "success_with_rejects" and "B (hi_ge_lo)" in reason and not err


@pytest.mark.parametrize("fmt", ["parquet", "csv", "delta"])
def test_file_quarantine_is_not_duplicated_on_rerun(fmt, tmp_path):
    if fmt == "delta":
        pytest.importorskip("deltalake")
    c = _bars_contract(
        {"format": "parquet", "target_path": str(tmp_path / "out"), "strategy": "append"},
        {"format": fmt, "target": str(tmp_path / "q")},
    )
    df = pl.DataFrame({"sym": ["A", "A", "B", "B"], "i": [1, 2, 1, 2], "lo": [1.0] * 4, "hi": [2.0, 2.0, 0.5, 2.0]})
    for _ in range(3):
        p = DataProcessor(c, engine="polars")
        r = p.run(df)
        written = p.materialize(r.good, r.bad)
        assert "quarantine_error" not in (written or {})
    path = str(tmp_path / "q" / ("bars" if fmt == "delta" else f"bars.{fmt}"))
    q = pl.read_delta(path) if fmt == "delta" else (pl.read_csv(path) if fmt == "csv" else pl.read_parquet(path))
    assert q.height == 2


# ── Second adversarial pass (2026-11-19) ────────────────────────────────────────────────────── #


def _rule_contract(scope_type, rule, cmp, quality_extra=None):
    q = {
        "on_dataset_failure": "quarantine",
        "quarantine_scope": ["g"],
        "dataset_rules": [{"name": "d", "sql": rule, **cmp}],
    }
    q.update(quality_extra or {})
    return {
        "version": "1.0.0",
        "info": {"title": "t"},
        "dataset": "t",
        "model": {"fields": [{"name": "g", "type": scope_type}, {"name": "amt", "type": "double"}]},
        "quality": q,
    }


def test_reject_set_that_shrinks_does_not_duplicate(tmp_path, monkeypatch):
    """One key for the whole reject set: when run 2 rejected fewer rows, the remaining reject
    was stored a second time."""
    mat = {**_duck(tmp_path, monkeypatch), "strategy": "merge"}
    c = _contract(
        mat,
        fields=(("id", "int"), ("v", "double")),
        pk=["id"],
        quality={"row_rules": [{"name": "v_pos", "sql": "v > 0"}]},
        quarantine={"format": "database", "connection": "env:LL_WS_URL", "table": "ws.rejects"},
    )
    for vs in ([-1.0, -2.0, 3.0], [-1.0, 2.0, 3.0], [-1.0, -2.0, 3.0]):
        p = DataProcessor(c, engine="polars")
        r = p.run(pl.DataFrame({"id": [10, 11, 12], "v": vs}))
        p.materialize(r.good, r.bad)
    assert sorted(_rows("SELECT id, v FROM ws.rejects")) == [(10, -1.0), (11, -2.0)]


@pytest.mark.parametrize("engine", ["polars", "duckdb"])
def test_nan_scope_group_that_fails_is_quarantined(engine):
    c = _rule_contract("double", "SELECT COUNT(*) - COUNT(DISTINCT amt) FROM source", {"must_be_less_than": 1})
    r = DataProcessor(c, engine=engine).run(
        pl.DataFrame({"g": [float("nan")] * 2 + [1.0, 2.0, 2.0], "amt": [1.0, 1.0, 2.0, 3.0, 3.0]})
    )
    assert r.good.select("g", "amt").rows() == [(1.0, 2.0)]


@pytest.mark.parametrize("engine", ["polars", "duckdb"])
def test_rule_whose_where_matches_nothing_passes(engine):
    """SUM over no rows is NULL: it failed every healthy group, and every clean batch."""
    c = _rule_contract("string", "SELECT SUM(amt) FROM source WHERE amt < 0", {"must_be_greater_than": -5})
    r = DataProcessor(c, engine=engine).run(pl.DataFrame({"g": ["A", "A", "B", "C"], "amt": [-10.0, 1.0, 2.0, -1.0]}))
    assert sorted(r.bad.select("g", "amt").rows()) == [("A", -10.0), ("A", 1.0)]
    clean = DataProcessor(c, engine=engine).run(pl.DataFrame({"g": ["B"], "amt": [2.0]}))
    assert len(clean.good) == 1 and len(clean.bad) == 0
    # Rows matched but the value is NULL: unmeasured, still a failure.
    c2 = _rule_contract("string", "SELECT AVG(amt) FROM source WHERE g = 'B'", {"must_be_greater_than": 0})
    unmeasured = DataProcessor(c2, engine=engine).run(
        pl.DataFrame({"g": ["B"], "amt": [None]}, schema={"g": pl.String, "amt": pl.Float64})
    )
    assert len(unmeasured.bad) == 1


@pytest.mark.parametrize("env", ["LAKELOGIC_TEST_MYSQL_URL", "LAKELOGIC_TEST_MSSQL_URL"])
def test_nan_rejects_are_kept_in_a_mysql_or_sqlserver_quarantine(env, monkeypatch):
    url = os.environ.get(env)
    if not url:
        pytest.skip(f"set {env}")
    monkeypatch.setenv("LL_WS_URL", url)
    import uuid as _uuid

    table = f"ll_ws.q_{_uuid.uuid4().hex[:6]}"
    c = {
        "version": "1.0.0",
        "info": {"title": "t"},
        "dataset": "t",
        "model": {"fields": [{"name": "id", "type": "int"}, {"name": "v", "type": "double"}]},
        "quality": {"row_rules": [{"name": "finite", "sql": "v BETWEEN -1.7e308 AND 1.7e308"}]},
        "quarantine": {"format": "database", "connection": "env:LL_WS_URL", "table": table},
        "materialization": {"format": "parquet", "target_path": "unused", "strategy": "append"},
    }
    p = DataProcessor(c, engine="polars")
    r = p.run(pl.DataFrame({"id": [1, 2, 3], "v": [1.0, float("nan"), float("inf")]}))
    assert len(r.bad) == 2
    from lakelogic.core.quarantine import materialize_quarantine

    out = materialize_quarantine(r.bad, p.contract, engine_name="polars")
    assert out["rows_written"] == 2


def test_failed_quarantine_of_a_rejected_batch_is_reported(tmp_path):
    blocker = tmp_path / "q"
    blocker.write_text("a file where the quarantine folder should be", encoding="utf-8")
    c = _rule_contract("string", "SELECT COUNT(*) FROM source WHERE amt < 0", {"must_be_less_than": 1})
    del c["quality"]["quarantine_scope"]
    c["quarantine"] = {"format": "parquet", "target": str(blocker / "sub")}
    c["materialization"] = {"format": "parquet", "target_path": str(tmp_path / "out"), "strategy": "append"}
    p = DataProcessor(c, engine="polars")
    r = p.run(pl.DataFrame({"g": ["A"], "amt": [-1.0]}))
    written = p.materialize(r.good, r.bad)
    assert "quarantine_error" in written
    assert p.last_report["status"] == "quarantine_not_saved"
