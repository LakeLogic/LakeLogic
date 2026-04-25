from pathlib import Path

import pytest
import yaml

from lakelogic.cli import driver


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


def test_driver_apply_policy_pack_warns_when_missing(tmp_path: Path) -> None:
    contract = driver.DataContract(
        version="1.0.0",
        dataset="bronze_sample",
        source={"type": "landing", "path": "input", "pattern": "*.csv", "load_mode": "full"},
        model={"fields": [{"name": "id", "type": "integer"}]},
        materialization={"strategy": "overwrite", "target_path": str(tmp_path / "out"), "format": "csv"},
    )
    drv = driver.PipelineDriver("polars", max_workers=1, policy_pack="missing_pack", policy_pack_dir=tmp_path)

    updated = drv._apply_policy_pack(contract, "bronze")

    assert updated is contract


def test_driver_apply_policy_pack_merges_defaults_and_stage(tmp_path: Path) -> None:
    pack_path = tmp_path / "baseline.yaml"
    pack_path.write_text(
        yaml.safe_dump(
            {
                "defaults": {
                    "metadata": {"owner": "platform"},
                    "transformations": [{"sql": "SELECT * FROM source"}],
                    "transformations_mode": "append",
                },
                "bronze_defaults": {
                    "materialization": {"format": "parquet"},
                    "transformations": [{"filter": {"sql": "id IS NOT NULL"}}],
                    "transformations_mode": "prepend",
                },
                "quality": {
                    "row_rules": [{"name": "positive_id", "sql": "id > 0"}],
                    "dataset_rules": [{"name": "row_count", "sql": "COUNT(*) > 0"}],
                },
                "service_levels": {
                    "freshness": {"description": "Daily", "threshold": "24h", "field": "updated_at"}
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    contract = driver.DataContract(
        version="1.0.0",
        dataset="bronze_sample",
        source={"type": "landing", "path": "input", "pattern": "*.csv", "load_mode": "full"},
        model={"fields": [{"name": "id", "type": "integer"}]},
        materialization={"strategy": "overwrite", "target_path": str(tmp_path / "out"), "format": "csv"},
        metadata={"team": "analytics"},
        quality={"row_rules": [{"name": "existing", "sql": "id >= 0"}], "dataset_rules": []},
        transformations=[{"derive": {"field": "source_system", "sql": "'csv'"}}],
    )
    contract._base_path = tmp_path
    drv = driver.PipelineDriver("polars", max_workers=1, policy_pack="baseline", policy_pack_dir=tmp_path)

    updated = drv._apply_policy_pack(contract, "bronze")

    assert updated.metadata["team"] == "analytics"
    assert updated.metadata["owner"] == "platform"
    assert updated.materialization.format == "parquet"
    assert [step.filter.sql if step.filter else None for step in updated.transformations] == [
        "id IS NOT NULL",
        None,
        None,
    ]
    assert updated.transformations[1].derive.field == "source_system"
    assert updated.transformations[2].sql == "SELECT * FROM source"
    assert [rule.name for rule in updated.quality.row_rules] == ["existing", "positive_id"]
    assert updated.quality.dataset_rules[0].name == "row_count"
    assert updated.service_levels.freshness.threshold == "24h"
    assert updated._base_path == tmp_path


def test_driver_apply_policy_pack_metadata_override_and_replace_mode(tmp_path: Path) -> None:
    pack_path = tmp_path / "explicit.yaml"
    pack_path.write_text(
        yaml.safe_dump(
            {
                "transformations": [{"sql": "SELECT id FROM source"}],
                "transformations_mode": "replace",
                "silver_transformations": [{"derive": {"field": "segment", "sql": "'vip'"}}],
                "silver_transformations_mode": "append",
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    contract = driver.DataContract(
        version="1.0.0",
        dataset="silver_sample",
        source={"type": "landing", "path": "input", "pattern": "*.csv", "load_mode": "full"},
        model={"fields": [{"name": "id", "type": "integer"}]},
        materialization={"strategy": "overwrite", "target_path": str(tmp_path / "out"), "format": "csv"},
        metadata={"policy_pack": str(pack_path)},
        transformations=[{"filter": {"sql": "id > 10"}}],
    )
    drv = driver.PipelineDriver("polars", max_workers=1, policy_pack="ignored", policy_pack_dir=tmp_path)

    updated = drv._apply_policy_pack(contract, "silver")

    assert len(updated.transformations) == 2
    assert updated.transformations[0].sql == "SELECT id FROM source"
    assert updated.transformations[1].derive.field == "segment"


def test_driver_state_helpers_and_cloud_config(tmp_path: Path, monkeypatch) -> None:
    state_path = tmp_path / "state" / "driver_state.json"
    drv = driver.PipelineDriver("polars", max_workers=1, state_path=state_path)

    key = drv._state_key("orders", "bronze", type("Window", (), {"label": "full", "start": None, "end": None})())
    assert drv._state_completed(key) is False
    drv._state_mark_completed(key)
    assert drv._state_completed(key) is True
    assert state_path.exists()

    registry_path = tmp_path / "registry.yaml"
    registry_path.write_text(
        yaml.safe_dump(
            {
                "cloud": {
                    "enabled": True,
                    "report_url": "${REPORT_URL}",
                    "api_key": "${REPORT_KEY}",
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("REPORT_URL", "https://observer.example/api")
    monkeypatch.setenv("REPORT_KEY", "secret-key")
    monkeypatch.setenv("LAKELOGIC_REMOTE_OBSERVER", "")
    monkeypatch.setenv("LINEAGELOGIC_REPORT_URL", "")
    monkeypatch.setenv("LINEAGELOGIC_API_KEY", "")

    drv._apply_cloud_config(registry_path)

    assert driver.os.environ["LAKELOGIC_REMOTE_OBSERVER"] == "true"
    assert driver.os.environ["LINEAGELOGIC_REPORT_URL"] == "https://observer.example/api"
    assert driver.os.environ["LINEAGELOGIC_API_KEY"] == "secret-key"


def test_driver_approval_gates_thresholds_drift_and_bypass(tmp_path: Path) -> None:
    contract = driver.DataContract(
        version="1.0.0",
        dataset="orders",
        source={"type": "landing", "path": "input", "pattern": "*.csv", "load_mode": "full"},
        model={"fields": [{"name": "id", "type": "integer"}]},
        materialization={"strategy": "overwrite", "target_path": str(tmp_path / "out"), "format": "csv"},
        metadata={
            "approval_required": True,
            "approval_quarantine_ratio_threshold": 25,
            "approval_schema_drift": True,
        },
    )
    drv = driver.PipelineDriver("polars", max_workers=1)
    report = {
        "counts": {"quarantine_ratio": 0.40},
        "schema_drift": {"unknown_fields": ["new_col"], "missing_fields": []},
    }

    with pytest.raises(RuntimeError, match="Approval required for orders") as exc_info:
        drv._evaluate_approvals(report, contract, "orders")

    message = str(exc_info.value)
    assert "quarantine_ratio 0.40 > 0.25" in message
    assert "schema_drift" in message

    approval_file = tmp_path / "approval.txt"
    approval_file.write_text("approved", encoding="utf-8")
    bypass_contract = contract.model_copy(update={"metadata": {**contract.metadata, "approval_file": str(approval_file)}})
    drv._evaluate_approvals(report, bypass_contract, "orders")


def test_driver_nested_value_and_deep_merge_helpers() -> None:
    data = {"source": {"path": "old"}, "metadata": {"owner": "analytics"}}
    driver.PipelineDriver._set_nested_value(data, "source.path", "new")
    driver.PipelineDriver._set_nested_value(data, "server.schema_policy.unknown_fields", "drop")

    assert data["source"]["path"] == "new"
    assert data["server"]["schema_policy"]["unknown_fields"] == "drop"

    target = {"metadata": {"owner": "analytics", "tags": ["a"]}, "server": {"cast_to_string": False}}
    driver.PipelineDriver._deep_merge(target, {"metadata": {"domain": "sales"}, "server": {"cast_to_string": True}})

    assert target == {
        "metadata": {"owner": "analytics", "tags": ["a"], "domain": "sales"},
        "server": {"cast_to_string": True},
    }
