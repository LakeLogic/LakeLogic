import json
from pathlib import Path

import yaml

from lakeguard.cli import driver


def test_missing_upstream_reported(tmp_path: Path) -> None:
    contract_path = tmp_path / "gold_contract.yaml"
    contract = {
        "version": "1.0.0",
        "dataset": "gold_fact_claims",
        "upstream": ["silver_claims"],
        "source": {"type": "landing", "path": "unused.csv", "load_mode": "full"},
        "model": {"fields": [{"name": "claim_id", "type": "string"}]},
        "materialization": {"strategy": "overwrite", "target_path": str(tmp_path / "out"), "format": "csv"},
        "quarantine": {"enabled": True, "target": str(tmp_path / "quarantine")},
    }
    contract_path.write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")

    registry_path = tmp_path / "_registry.yaml"
    registry = {
        "entries": [
            {
                "entity": "fact_claims",
                "enabled": True,
                "contracts": {"gold": contract_path.name},
            }
        ]
    }
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")

    summary_path = tmp_path / "summary.json"
    drv = driver.PipelineDriver("polars", max_workers=1, summary_path=summary_path, fail_fast=True)
    drv.run({"gold": registry_path}, ["gold"], driver.Window(None, None, "last_success"), False)

    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["metrics"]["skipped_missing_upstream"] == 1
    assert summary["metrics"]["missing_upstreams"] == 1
    run = summary["runs"][0]
    assert run["status"] == "skipped"
    assert run["reason"] == "missing_upstream"
    assert run["missing_upstreams"][0]["reason"] == "missing_contract"
