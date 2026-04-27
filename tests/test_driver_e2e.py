from pathlib import Path

import pytest

pd = pytest.importorskip("pandas")
import yaml

from lakelogic.cli import driver


def test_e2e_full(tmp_path: Path) -> None:
    data_dir = tmp_path / "landing"
    ref_dir = tmp_path / "reference"
    out_dir = tmp_path / "out"
    data_dir.mkdir()
    ref_dir.mkdir()
    out_dir.mkdir()

    # Reference data
    ref_file = ref_dir / "claim_status.csv"
    pd.DataFrame(
        [
            {"status_code": "ACTIVE", "status_desc": "Active"},
            {"status_code": "LAPSED", "status_desc": "Lapsed"},
        ]
    ).to_csv(ref_file, index=False)

    # Landing (bronze) data with one bad row (null policy_id)
    landing_file = data_dir / "policies_2026-02-05.csv"
    pd.DataFrame(
        [
            {"policy_id": "P-100", "customer_id": "C-1", "status": "ACTIVE", "premium": 100.0},
            {"policy_id": None, "customer_id": "C-2", "status": "LAPSED", "premium": 80.0},
        ]
    ).to_csv(landing_file, index=False)

    # Reference contract
    ref_contract = {
        "version": "1.0.0",
        "dataset": "ref_claim_status",
        "source": {"type": "landing", "path": str(ref_dir), "pattern": "claim_status.csv", "load_mode": "full"},
        "model": {
            "fields": [
                {"name": "status_code", "type": "string"},
                {"name": "status_desc", "type": "string"},
            ]
        },
        "materialization": {"strategy": "overwrite", "target_path": str(out_dir / "ref_claim_status"), "format": "csv"},
    }
    ref_contract_path = tmp_path / "ref_contract.yaml"
    ref_contract_path.write_text(yaml.safe_dump(ref_contract, sort_keys=False), encoding="utf-8")

    # Bronze contract
    bronze_contract = {
        "version": "1.0.0",
        "dataset": "bronze_policies",
        "source": {"type": "landing", "path": str(data_dir), "pattern": "*.csv", "load_mode": "full"},
        "model": {
            "fields": [
                {"name": "policy_id", "type": "string"},
                {"name": "customer_id", "type": "string"},
                {"name": "status", "type": "string"},
                {"name": "premium", "type": "double"},
            ]
        },
        "materialization": {"strategy": "overwrite", "target_path": str(out_dir / "bronze_policies"), "format": "csv"},
        "quarantine": {"enabled": True, "target": str(out_dir / "bronze_quarantine")},
    }
    bronze_contract_path = tmp_path / "bronze_contract.yaml"
    bronze_contract_path.write_text(yaml.safe_dump(bronze_contract, sort_keys=False), encoding="utf-8")

    # Silver contract with quality rule and lookup link
    silver_contract = {
        "version": "1.0.0",
        "dataset": "silver_policies",
        "upstream": ["bronze_policies", "ref_claim_status"],
        "source": {
            "type": "landing",
            "path": str(out_dir / "bronze_policies"),
            "pattern": "data.csv",
            "load_mode": "full",
        },
        "model": {
            "fields": [
                {"name": "policy_id", "type": "string", "required": True},
                {"name": "customer_id", "type": "string"},
                {"name": "status", "type": "string"},
                {"name": "premium", "type": "double"},
            ]
        },
        "links": [{"name": "ref_claim_status", "path": str(ref_file), "type": "csv"}],
        "transformations": [{"derive": {"field": "is_active", "sql": "status = 'ACTIVE'"}}],
        "quality": {"row_rules": [{"not_null": {"field": "policy_id", "name": "policy_id_not_null"}}]},
        "materialization": {
            "strategy": "overwrite",
            "target_path": str(out_dir / "silver_policies"),
            "format": "parquet",
        },
        "quarantine": {"enabled": True, "target": str(out_dir / "silver_quarantine")},
    }
    silver_contract_path = tmp_path / "silver_contract.yaml"
    silver_contract_path.write_text(yaml.safe_dump(silver_contract, sort_keys=False), encoding="utf-8")

    # Gold contract (simple aggregate)
    gold_contract = {
        "version": "1.0.0",
        "dataset": "gold_policy_counts",
        "upstream": ["silver_policies"],
        "source": {
            "type": "landing",
            "path": str(out_dir / "silver_policies"),
            "pattern": "*.parquet",
            "load_mode": "full",
        },
        "transformations": [{"sql": "SELECT customer_id, COUNT(*) AS policy_count FROM source GROUP BY customer_id"}],
        "materialization": {
            "strategy": "overwrite",
            "target_path": str(out_dir / "gold_policy_counts"),
            "format": "csv",
        },
    }
    gold_contract_path = tmp_path / "gold_contract.yaml"
    gold_contract_path.write_text(yaml.safe_dump(gold_contract, sort_keys=False), encoding="utf-8")

    # Registries
    system_registry = {
        "entries": [
            {
                "entity": "policies",
                "enabled": True,
                "contracts": {"bronze": bronze_contract_path.name, "silver": silver_contract_path.name},
            }
        ]
    }
    reference_registry = {
        "entries": [{"entity": "reference", "enabled": True, "contracts": {"reference": ref_contract_path.name}}]
    }
    gold_registry = {"entries": [{"entity": "gold", "enabled": True, "contracts": {"gold": gold_contract_path.name}}]}

    system_registry_path = tmp_path / "_system_registry.yaml"
    reference_registry_path = tmp_path / "_reference_registry.yaml"
    gold_registry_path = tmp_path / "_gold_registry.yaml"
    system_registry_path.write_text(yaml.safe_dump(system_registry, sort_keys=False), encoding="utf-8")
    reference_registry_path.write_text(yaml.safe_dump(reference_registry, sort_keys=False), encoding="utf-8")
    gold_registry_path.write_text(yaml.safe_dump(gold_registry, sort_keys=False), encoding="utf-8")

    drv = driver.PipelineDriver("polars", max_workers=1, summary_path=tmp_path / "summary.json", fail_fast=True)
    drv.run(
        {"system": system_registry_path, "reference": reference_registry_path, "gold": gold_registry_path},
        ["reference", "bronze", "silver", "gold"],
        driver.Window(None, None, "full"),
        False,
    )

    assert (out_dir / "bronze_policies" / "data.csv").exists()
    assert (out_dir / "silver_policies" / "data.parquet").exists()
    assert (out_dir / "gold_policy_counts" / "data.csv").exists()
    # Quarantine is written in the same format as the materialization target
    assert (out_dir / "silver_quarantine" / "silver_policies.parquet").exists()


def test_e2e_inc(tmp_path: Path) -> None:
    import os

    # The incremental validator requires a run-log backend in production.
    # This test exercises file-window selection only — bypass the check.
    os.environ["LAKELOGIC_SKIP_INCREMENTAL_CHECK"] = "1"
    try:
        _run_incremental_window_selection(tmp_path)
    finally:
        os.environ.pop("LAKELOGIC_SKIP_INCREMENTAL_CHECK", None)


def _run_incremental_window_selection(tmp_path: Path) -> None:
    data_dir = tmp_path / "landing"
    out_dir = tmp_path / "out"
    data_dir.mkdir()
    out_dir.mkdir()

    file_day1 = data_dir / "policies_2026-02-05.csv"
    file_day2 = data_dir / "policies_2026-02-06.csv"
    pd.DataFrame([{"policy_id": "P-1", "premium": 10.0}]).to_csv(file_day1, index=False)
    pd.DataFrame([{"policy_id": "P-2", "premium": 20.0}]).to_csv(file_day2, index=False)

    contract = {
        "version": "1.0.0",
        "dataset": "bronze_policies",
        "source": {"type": "landing", "path": str(data_dir), "pattern": "*.csv", "load_mode": "incremental"},
        "model": {"fields": [{"name": "policy_id", "type": "string"}, {"name": "premium", "type": "double"}]},
        "materialization": {"strategy": "overwrite", "target_path": str(out_dir / "bronze_policies"), "format": "csv"},
    }
    contract_path = tmp_path / "contract.yaml"
    contract_path.write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")

    registry_path = tmp_path / "_registry.yaml"
    registry = {"entries": [{"entity": "policies", "enabled": True, "contracts": {"bronze": contract_path.name}}]}
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")

    start = driver.datetime(2026, 2, 5, tzinfo=driver.timezone.utc)
    end = driver.datetime(2026, 2, 6, tzinfo=driver.timezone.utc)
    window = driver.Window(start, end, "range")

    drv = driver.PipelineDriver("polars", max_workers=1, summary_path=tmp_path / "summary.json", fail_fast=True)
    drv.run({"system": registry_path}, ["bronze"], window, False)

    out_file = out_dir / "bronze_policies" / "data.csv"
    df = pd.read_csv(out_file)
    assert len(df) == 1
    assert df.iloc[0]["policy_id"] == "P-1"


def test_e2e_reproc(tmp_path: Path) -> None:
    data_dir = tmp_path / "landing"
    out_dir = tmp_path / "out"
    data_dir.mkdir()
    out_dir.mkdir()

    file_day1 = data_dir / "policies_2026-02-05.csv"
    pd.DataFrame([{"policy_id": "P-1", "run_date": "2026-02-05"}]).to_csv(file_day1, index=False)

    contract = {
        "version": "1.0.0",
        "dataset": "silver_policies",
        "source": {"type": "landing", "path": str(data_dir), "pattern": "*.csv", "load_mode": "full"},
        "model": {"fields": [{"name": "policy_id", "type": "string"}, {"name": "run_date", "type": "string"}]},
        "materialization": {
            "strategy": "append",
            "partition_by": ["run_date"],
            "target_path": str(out_dir / "silver_policies"),
            "format": "csv",
        },
    }
    contract_path = tmp_path / "contract.yaml"
    contract_path.write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")

    registry_path = tmp_path / "_registry.yaml"
    registry = {"entries": [{"entity": "policies", "enabled": True, "contracts": {"silver": contract_path.name}}]}
    registry_path.write_text(yaml.safe_dump(registry, sort_keys=False), encoding="utf-8")

    drv = driver.PipelineDriver("polars", max_workers=1, summary_path=tmp_path / "summary.json", fail_fast=True)
    drv.run({"system": registry_path}, ["silver"], driver.Window(None, None, "full"), False)

    file_day1.write_text("policy_id,run_date\nP-2,2026-02-05\n", encoding="utf-8")
    drv.run({"system": registry_path}, ["silver"], driver.Window(None, None, "reprocess"), True)

    partition_dir = out_dir / "silver_policies" / "run_date=2026-02-05"
    csv_files = list(partition_dir.glob("data*.csv"))
    assert csv_files, f"No CSV files found in {partition_dir}"
    df = pd.read_csv(csv_files[0])
    assert len(df) == 1
    assert df.iloc[0]["policy_id"] == "P-2"
