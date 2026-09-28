"""dlt run log: an existing `run_logs` dataset keeps receiving rows; a new estate gets
`lakelogic_run_log` (2026-09-26, owner decision (a): nothing moves, history is not split)."""

import pytest

dlt = pytest.importorskip("dlt")
duckdb = pytest.importorskip("duckdb")

from lakelogic.core.models import DataContract
from lakelogic.core.run_log import _write_run_log_table


def _contract(db):
    return DataContract(
        version="1.0.0",
        dataset="trips",
        metadata={
            "run_log_backend": "dlt",
            "run_log_table": "trips_log",
            "dlt_destination": "duckdb",
            "dlt_credentials": str(db),
        },
    )


def _report():
    return {"run_id": "r1", "status": "success", "counts": {"source": 1, "total": 1, "good": 1}}


def _schemas(db):
    con = duckdb.connect(str(db))
    try:
        return {r[0] for r in con.execute("select schema_name from information_schema.schemata").fetchall()}
    finally:
        con.close()


def test_new_estate_writes_to_the_new_dataset(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    db = tmp_path / "rl.duckdb"
    out = _write_run_log_table(_report(), _contract(db))
    assert out and out.endswith(":lakelogic_run_log.trips_log")
    assert "lakelogic_run_log" in _schemas(db) and "run_logs" not in _schemas(db)


def test_existing_run_logs_dataset_keeps_receiving_rows(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    db = tmp_path / "rl.duckdb"
    con = duckdb.connect(str(db))
    con.execute("create schema run_logs")
    con.close()
    out = _write_run_log_table(_report(), _contract(db))
    assert out and out.endswith(":run_logs.trips_log")
    assert "lakelogic_run_log" not in _schemas(db)
