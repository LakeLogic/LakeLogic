import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import yaml

from lakelogic.cli import driver


def test_parse_layers_strict_valid():
    assert driver.parse_layers("bronze", strict=True) == ["bronze"]
    assert driver.parse_layers("bronze,silver", strict=True) == ["bronze", "silver"]
    assert driver.parse_layers("silver,gold", strict=True) == ["silver", "gold"]
    assert driver.parse_layers("gold", strict=True) == ["gold"]
    assert driver.parse_layers("bronze,silver,gold,reference", strict=True) == ["bronze", "silver", "gold", "reference"]
    assert driver.parse_layers("bronze,reference", strict=True) == ["bronze", "reference"]
    assert driver.parse_layers("ref,bronze", strict=False) == ["reference", "bronze"]


def test_parse_layers_strict_invalid():
    try:
        driver.parse_layers("gold,silver", strict=True)
    except ValueError as exc:
        assert "Invalid layer order" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_parse_window_range_validation():
    try:
        driver.parse_window("range", None, None, None, None, None)
    except ValueError as exc:
        assert "window-start-date" in str(exc)
    else:
        raise AssertionError("Expected ValueError")


def test_run_log_reader_sqlite(tmp_path: Path):
    db_path = tmp_path / "run_logs.sqlite"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE run_logs (contract TEXT, timestamp TEXT)")
    conn.execute(
        "INSERT INTO run_logs (contract, timestamp) VALUES (?, ?)",
        ("test_contract", datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()
    conn.close()

    contract = driver.DataContract(
        version="1.0.0",
        metadata={
            "run_log_table": "run_logs",
            "run_log_backend": "sqlite",
            "run_log_database": str(db_path),
        },
        dataset="test_contract",
    )

    reader = driver.RunLogReader("polars")
    ts, reason = reader.last_success_info(contract)
    assert ts is not None
    assert reason is None


def test_driver_summary_written(tmp_path: Path):
    data_dir = tmp_path / "data"
    out_dir = tmp_path / "out"
    data_dir.mkdir()
    out_dir.mkdir()

    data_file = data_dir / "sample.csv"
    data_file.write_text("id,value\n1,10\n", encoding="utf-8")

    contract_path = tmp_path / "contract.yaml"
    contract = {
        "version": "1.0.0",
        "dataset": "bronze_sample",
        "server": {"type": "local", "path": str(data_file), "format": "csv"},
        "source": {"type": "landing", "path": str(data_dir), "pattern": "sample.csv", "load_mode": "full"},
        "model": {"fields": [{"name": "id", "type": "integer"}, {"name": "value", "type": "integer"}]},
        "materialization": {"strategy": "overwrite", "target_path": str(out_dir / "sample"), "format": "csv"},
        "quarantine": {"enabled": True, "target": str(out_dir / "quarantine")},
    }
    contract_path.write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")

    registry_path = tmp_path / "_registry.yaml"
    registry = {
        "entries": [
            {
                "entity": "sample",
                "enabled": True,
                "contracts": {"bronze": contract_path.name},
            }
        ]
    }
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")

    summary_path = tmp_path / "summary.json"
    drv = driver.PipelineDriver("polars", max_workers=1, summary_path=summary_path, fail_fast=True)
    drv.run({"system": registry_path}, ["bronze"], driver.Window(None, None, "full"), False)

    assert summary_path.exists()
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["metrics"]["successful"] == 1
