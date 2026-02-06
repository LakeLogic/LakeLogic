from pathlib import Path

import yaml

from lakeguard.cli import driver


def test_driver_apply_overrides(tmp_path: Path) -> None:
    contract_path = tmp_path / "contract.yaml"
    contract = {
        "version": "1.0.0",
        "dataset": "bronze_sample",
        "source": {"type": "landing", "path": "old_path", "pattern": "*.csv", "load_mode": "full"},
        "model": {"fields": [{"name": "id", "type": "integer"}]},
        "materialization": {"strategy": "overwrite", "target_path": str(tmp_path / "out"), "format": "csv"},
    }
    contract_path.write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")

    contract_obj = driver.DataContract(**contract)
    drv = driver.PipelineDriver("polars", max_workers=1, overrides={"source.path": "new_path"})
    updated = drv._apply_overrides(contract_obj)
    assert updated.source.path == "new_path"
