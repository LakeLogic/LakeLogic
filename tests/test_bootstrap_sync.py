from pathlib import Path

import yaml
import pytest
typer = pytest.importorskip("typer")
pd = pytest.importorskip("pandas")
from lakelogic.cli import main as cli_main_module
from lakelogic.cli.main import bootstrap


def test_bootstrap_sync_adds_new_entities(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()
    (landing / "customers.csv").write_text("id,name\n1,Alice\n", encoding="utf-8")

    output_dir = tmp_path / "contracts"
    registry_path = output_dir / "_registry.yaml"

    bootstrap(
        landing=landing,
        output_dir=output_dir,
        registry=registry_path,
        format="csv",
        pattern="*.csv",
        layer="bronze",
        sample_rows=10,
    )

    (landing / "orders.csv").write_text("id,amount\n1,10.0\n", encoding="utf-8")

    bootstrap(
        landing=landing,
        output_dir=output_dir,
        registry=registry_path,
        format="csv",
        pattern="*.csv",
        layer="bronze",
        sample_rows=10,
        sync=True,
    )

    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    entries = registry.get("contracts", [])
    entities = {e.get("entity") for e in entries}
    assert "customers" in entities
    assert "orders" in entities


def test_bootstrap_sync_update_schema_appends_new_fields_for_json(tmp_path: Path, monkeypatch) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()
    sample_path = landing / "orders.json"
    sample_path.write_text('{"id": 1}\n', encoding="utf-8")

    output_dir = tmp_path / "contracts"
    registry_path = output_dir / "_registry.yaml"

    calls = {"count": 0}

    def _read_json(path, lines=True):
        calls["count"] += 1
        if calls["count"] == 1:
            return pd.DataFrame({"id": [1]})
        return pd.DataFrame({"id": [1], "status": ["new"]})

    monkeypatch.setattr("pandas.read_json", _read_json)

    bootstrap(
        landing=landing,
        output_dir=output_dir,
        registry=registry_path,
        format="json",
        pattern="*.json",
        layer="bronze",
        sample_rows=10,
    )

    bootstrap(
        landing=landing,
        output_dir=output_dir,
        registry=registry_path,
        format="json",
        pattern="*.json",
        layer="bronze",
        sample_rows=10,
        sync=True,
        sync_update_schema=True,
    )

    contract = yaml.safe_load((output_dir / "bronze_orders.yaml").read_text(encoding="utf-8"))
    field_names = [field["name"] for field in contract["model"]["fields"]]
    assert field_names == ["id", "status"]


def test_bootstrap_sync_preserves_existing_registry_entries(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()
    (landing / "customers.csv").write_text("id,name\n1,Alice\n", encoding="utf-8")

    output_dir = tmp_path / "contracts"
    registry_path = output_dir / "_registry.yaml"
    registry_path.parent.mkdir(parents=True, exist_ok=True)
    registry_path.write_text(
        yaml.safe_dump(
            {
                "domain": "finance",
                "system": "erp",
                "owner": "team-data",
                "contracts": [
                    {"entity": "legacy", "enabled": True, "contracts": {"bronze": "bronze_legacy.yaml"}}
                ],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    bootstrap(
        landing=landing,
        output_dir=output_dir,
        registry=registry_path,
        format="csv",
        pattern="*.csv",
        layer="bronze",
        sample_rows=10,
        sync=True,
    )

    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    entities = [entry["entity"] for entry in registry["contracts"]]
    assert registry["owner"] == "team-data"
    assert entities == ["customers", "legacy"]
