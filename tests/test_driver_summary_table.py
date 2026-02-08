import sqlite3
from pathlib import Path

import yaml

from lakelogic.cli import driver


def test_driver_summary_table_sqlite(tmp_path: Path) -> None:
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

    db_path = tmp_path / "summary.sqlite"
    drv = driver.PipelineDriver(
        "polars",
        max_workers=1,
        summary_table="pipeline_runs",
        summary_backend="sqlite",
        summary_database=str(db_path),
        fail_fast=True,
    )
    drv.run({"system": registry_path}, ["bronze"], driver.Window(None, None, "full"), False)

    conn = sqlite3.connect(str(db_path))
    try:
        rows = conn.execute("SELECT run_id, successful FROM pipeline_runs").fetchall()
    finally:
        conn.close()

    assert rows
    assert rows[0][1] == 1
