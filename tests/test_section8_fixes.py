"""Fixes found by an agent running LakeLogic 2.4.0 in a trading app (2026-11-18)."""
from __future__ import annotations

import pytest

pl = pytest.importorskip("polars")


def _contract(tmp_path, **quality) -> dict:
    return {
        "version": "1.0.0",
        "info": {"title": "bars"},
        "dataset": "bars",
        "model": {"fields": [{"name": "symbol", "type": "string"}, {"name": "ts", "type": "integer"}]},
        "quality": {"dataset_rules": [{"unique": "symbol"}], **quality},
        "materialization": {"strategy": "append", "format": "parquet", "target_path": str(tmp_path / "out")},
        "quarantine": {"target": str(tmp_path / "q"), "format": "parquet"},
    }


def test_run_source_accepts_a_dataframe():
    from lakelogic import DataProcessor

    c = {"version": "1.0.0", "info": {"title": "x"}, "model": {"fields": [{"name": "a", "type": "integer"}]}}
    good, bad = DataProcessor(c, engine="polars").run_source(pl.DataFrame({"a": [1, 2]}))
    assert len(good) == 2


def test_the_last_batch_flag_does_not_block_other_data(tmp_path):
    """A batch-quarantined run must not make a LATER materialize of other data write nothing
    (99 good symbols were lost)."""
    from lakelogic import DataProcessor

    proc = DataProcessor(_contract(tmp_path, on_dataset_failure="quarantine"), engine="polars")
    proc.run(pl.DataFrame({"symbol": ["A", "A"], "ts": [1, 2]}))           # fails unique -> quarantined
    assert proc.batch_quarantined
    combined = pl.DataFrame({"symbol": ["B", "C"], "ts": [1, 1]})
    written = proc.materialize(combined, None)
    assert not written.get("batch_quarantined")
    assert pl.read_parquet(str(tmp_path / "out" / "*.parquet")).height == 2


def test_the_run_log_says_a_batch_was_quarantined(tmp_path):
    from lakelogic import DataProcessor

    c = _contract(tmp_path, on_dataset_failure="quarantine")
    c["metadata"] = {"run_log_dir": str(tmp_path / "logs")}
    proc = DataProcessor(c, engine="polars")
    proc.run(pl.DataFrame({"symbol": ["A", "A"], "ts": [1, 2]}))
    assert proc.last_report["status"] == "batch_quarantined"
    assert proc.last_report["error_message"].startswith("Batch rejected: symbol_unique")
    where = proc.write_run_log()
    assert where and where.endswith(".json")                              # says where it went

    ok = DataProcessor(_contract(tmp_path), engine="polars")
    ok.run(pl.DataFrame({"symbol": ["A", "B"], "ts": [1, 2]}))
    assert ok.last_report["status"] == "success"


def test_write_run_log_returns_the_database_target(tmp_path, monkeypatch):
    pytest.importorskip("dlt")
    duckdb = pytest.importorskip("duckdb")
    from lakelogic import DataProcessor

    monkeypatch.chdir(tmp_path)
    db = str(tmp_path / "lake.duckdb")
    c = _contract(tmp_path)
    c["metadata"] = {"run_log_table": "run_logs", "run_log_backend": "dlt", "dlt_destination": "duckdb",
                     "dlt_credentials": db, "dlt_dataset_name": "ops"}
    proc = DataProcessor(c, engine="polars")
    proc.run(pl.DataFrame({"symbol": ["A"], "ts": [1]}))
    where = proc.write_run_log()
    assert where, "write_run_log returned None after a database write"
    con = duckdb.connect(db, read_only=True)
    assert con.execute("SELECT status FROM ops.run_logs").fetchone()[0] == "success"
    con.close()


def test_the_run_complete_line_says_the_final_outcome(tmp_path):
    from loguru import logger

    from lakelogic import DataProcessor

    lines: list = []
    sink = logger.add(lambda m: lines.append(m.record["message"]), level="INFO")
    try:
        DataProcessor(_contract(tmp_path, on_dataset_failure="quarantine"), engine="polars").run(
            pl.DataFrame({"symbol": ["A", "A"], "ts": [1, 2]}))
    finally:
        logger.remove(sink)
    complete = [m for m in lines if m.startswith("Run complete")]
    assert complete and "ok=0" in complete[0] and "batch quarantined" in complete[0]


def test_sql_rule_reasons_read_plainly():
    from lakelogic.core.processor import _describe_dataset_failure

    assert _describe_dataset_failure({"name": "one_bar_per_time", "value": "1 (expected < 1.0)"}) == \
        "one_bar_per_time: got 1, must be < 1"
