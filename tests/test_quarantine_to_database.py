"""Quarantine to a database through dlt, with no path target (2026-11-18).

A fresh-agent test found that ``quarantine: {format: dlt, dlt_destination: …}`` discarded every
reject (the processor saved quarantine only when ``target`` was set), and that with OLC 0.33
declaring the ``dlt_*`` keys, reading them from ``model_extra`` lost the destination. Wherever
data can be written, its rejects can be written too.
"""

from __future__ import annotations

import pytest

pytest.importorskip("dlt")
duckdb = pytest.importorskip("duckdb")


def _contract(db_path: str) -> dict:
    return {
        "version": "1.0.0",
        "info": {"title": "orders"},
        "dataset": "orders",
        "model": {"fields": [{"name": "order_id", "type": "integer"}, {"name": "amount", "type": "integer"}]},
        "quality": {"row_rules": [{"name": "amount_positive", "sql": "amount > 0"}]},
        "quarantine": {
            "format": "dlt",
            "dlt_destination": "duckdb",
            "dlt_credentials": db_path,
            "dlt_dataset_name": "rejects",
        },
    }


def _rows(db_path: str, sql: str):
    con = duckdb.connect(db_path, read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def test_declared_dlt_keys_reach_the_writer():
    from lakelogic.core.models import DataContract
    from lakelogic.core.quarantine import has_destination, quarantine_dlt_config

    c = DataContract(**_contract("x.duckdb"))
    assert has_destination(c.quarantine)
    assert quarantine_dlt_config(c.quarantine) == {
        "dlt_destination": "duckdb",
        "dlt_credentials": "x.duckdb",
        "dlt_dataset_name": "rejects",
    }


def test_rejects_land_in_the_named_database(tmp_path, monkeypatch):
    from lakelogic import DataProcessor

    monkeypatch.chdir(tmp_path)  # a stray database would appear here
    db = str(tmp_path / "lake.duckdb")
    (tmp_path / "orders.csv").write_text("order_id,amount\n1,10\n2,-5\n3,-1\n", encoding="utf-8")
    proc = DataProcessor(_contract(db), engine="polars")
    result = proc.run_source(str(tmp_path / "orders.csv"))
    assert len(result.bad) == 2

    from lakelogic.core.quarantine import materialize_quarantine

    written = materialize_quarantine(result.bad, proc.contract, engine_name="polars")
    assert written["rows_written"] == 2
    ids = sorted(r[0] for r in _rows(db, "SELECT order_id FROM rejects.rejects"))
    assert ids == [2, 3]
    stray = [p.name for p in tmp_path.glob("*.duckdb") if p.name != "lake.duckdb"]
    assert not stray, f"rejects went to a stray database: {stray}"


def test_a_standalone_run_writes_its_run_log_to_a_folder(tmp_path):
    from lakelogic import DataProcessor

    (tmp_path / "orders.csv").write_text("order_id,amount\n1,10\n2,-5\n", encoding="utf-8")
    c = _contract(str(tmp_path / "lake.duckdb"))
    c["metadata"] = {"run_log_dir": str(tmp_path / "logs")}
    proc = DataProcessor(c, engine="polars")
    proc.run_source(str(tmp_path / "orders.csv"))
    assert not (tmp_path / "logs").exists(), "run() itself must not write the log (callers do)"
    proc.write_run_log()
    assert len(list((tmp_path / "logs").glob("*.json"))) == 1


def test_a_standalone_run_writes_its_run_log_to_a_database(tmp_path, monkeypatch):
    from lakelogic import DataProcessor

    monkeypatch.chdir(tmp_path)
    db = str(tmp_path / "lake.duckdb")
    (tmp_path / "orders.csv").write_text("order_id,amount\n1,10\n2,-5\n", encoding="utf-8")
    c = _contract(db)
    c["metadata"] = {
        "run_log_table": "run_logs",
        "run_log_backend": "dlt",
        "dlt_destination": "duckdb",
        "dlt_credentials": db,
        "dlt_dataset_name": "ops",
    }
    proc = DataProcessor(c, engine="polars")
    proc.run_source(str(tmp_path / "orders.csv"))
    proc.write_run_log()
    assert _rows(db, "SELECT count(*) FROM ops.run_logs")[0][0] == 1


def test_cli_run_writes_the_run_log(tmp_path):
    import yaml
    from typer.testing import CliRunner

    from lakelogic.cli.main import app

    (tmp_path / "orders.csv").write_text("order_id,amount\n1,10\n", encoding="utf-8")
    c = _contract(str(tmp_path / "lake.duckdb"))
    c.pop("quarantine")
    c["metadata"] = {"run_log_dir": str(tmp_path / "logs")}
    (tmp_path / "c.yaml").write_text(yaml.safe_dump(c), encoding="utf-8")
    out = CliRunner().invoke(app, ["run", "-c", str(tmp_path / "c.yaml"), "--source", str(tmp_path / "orders.csv")])
    assert out.exit_code == 0, out.output
    assert len(list((tmp_path / "logs").glob("*.json"))) == 1
