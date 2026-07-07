import json
from pathlib import Path

import yaml

from lakelogic.cli import driver


def test_driver_metrics_path(tmp_path: Path) -> None:
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

    metrics_path = tmp_path / "metrics.json"
    drv = driver.PipelineDriver(
        "polars",
        max_workers=1,
        summary_path=tmp_path / "summary.json",
        metrics_path=metrics_path,
        metrics_tags={"env": "test"},
        fail_fast=True,
    )
    drv.run({"system": registry_path}, ["bronze"], driver.Window(None, None, "full"), False)

    assert metrics_path.exists()
    payload = json.loads(metrics_path.read_text(encoding="utf-8"))
    assert payload["metrics"]["successful"] == 1
    assert payload["tags"]["env"] == "test"


def test_prometheus_formatting(tmp_path: Path) -> None:
    drv = driver.PipelineDriver(
        "polars",
        max_workers=1,
        metrics_prefix="lakelogic",
        metrics_tags={"env": "test"},
    )
    drv.metrics_snapshot = {
        "timestamp": "2026-02-06T00:00:00+00:00",
        "tags": {"env": "test"},
        "metrics": {"successful": 3, "failed": 1},
    }
    payload = drv._format_prometheus()
    assert "lakelogic_successful" in payload
    assert 'env="test"' in payload
