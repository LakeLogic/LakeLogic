import json
from pathlib import Path
import sys
import types

import yaml
import pytest
typer = pytest.importorskip("typer")
pd = pytest.importorskip("pandas")
from lakelogic.cli import main as cli_main_module  # noqa: F811 — avoid shadowed name
from lakelogic.cli.main import bootstrap


def test_bootstrap_generates_contracts(tmp_path: Path) -> None:
    landing = tmp_path / "landing"
    landing.mkdir()
    (landing / "customers.csv").write_text("id,name\n1,Alice\n", encoding="utf-8")
    (landing / "orders.csv").write_text("id,amount\n1,10.0\n", encoding="utf-8")

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

    assert registry_path.exists()
    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    assert len(registry.get("contracts", [])) == 2
    for entry in registry["contracts"]:
        contract_path = output_dir / entry["contracts"]["bronze"]
        assert contract_path.exists()


def test_bootstrap_parquet_profile_pii_rules_and_ai(tmp_path: Path, monkeypatch) -> None:
    landing = tmp_path / "landing_sales" / "erp"
    entity_dir = landing / "customers"
    entity_dir.mkdir(parents=True)
    (entity_dir / "part-001.parquet").write_text("placeholder", encoding="utf-8")

    profile_dir = tmp_path / "profiles"
    output_dir = tmp_path / "contracts"
    registry_path = output_dir / "_registry.yaml"

    frame = pd.DataFrame(
        {
            "id": [1, 2],
            "email": ["a@example.com", "b@example.com"],
            "status": ["active", "inactive"],
            "updated_at": ["2024-01-01", "2024-01-02"],
        }
    )
    monkeypatch.setattr("pandas.read_parquet", lambda path: frame.copy())

    class FakeAnalyzer:
        def analyze(self, text, language="en"):
            if "@" in text:
                return [types.SimpleNamespace(entity_type="EMAIL_ADDRESS")]
            return []

    monkeypatch.setitem(
        sys.modules,
        "dataprofiler",
        types.SimpleNamespace(Profiler=lambda df: types.SimpleNamespace(profile={"rows": len(df), "columns": list(df.columns)})),
    )
    monkeypatch.setitem(sys.modules, "presidio_analyzer", types.SimpleNamespace(AnalyzerEngine=lambda: FakeAnalyzer()))

    fake_ai_module = types.ModuleType("lakelogic.ai.contract_enricher")

    def _enrich_contract(contract, sample_df=None, provider=None, model=None):
        contract["info"]["description"] = f"AI enriched via {provider}:{model}"
        contract["tags"] = ["ai-enriched", str(len(sample_df.columns))]
        return contract

    fake_ai_module.enrich_contract = _enrich_contract
    monkeypatch.setitem(sys.modules, "lakelogic.ai.contract_enricher", fake_ai_module)

    bootstrap(
        landing=landing,
        output_dir=output_dir,
        registry=registry_path,
        format="parquet",
        pattern="*.parquet",
        layer="bronze",
        sample_rows=10,
        profile=True,
        detect_pii=True,
        suggest_rules=True,
        profile_output_dir=profile_dir,
        pii_sample_size=5,
        ai=True,
        ai_provider="openai",
        ai_model="gpt-test",
    )

    contract = yaml.safe_load((output_dir / "bronze_customers.yaml").read_text(encoding="utf-8"))
    email_field = next(field for field in contract["model"]["fields"] if field["name"] == "email")
    assert email_field["pii"] is True
    assert email_field["classification"] == "email_address"
    assert contract["quality"]["row_rules"]
    assert contract["quality"]["dataset_rules"]
    assert contract["info"]["description"] == "AI enriched via openai:gpt-test"
    assert contract["tags"] == ["ai-enriched", "4"]

    profile_data = json.loads((profile_dir / "customers_profile.json").read_text(encoding="utf-8"))
    assert profile_data["rows"] == 2

    registry = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    assert registry["domain"] == "sales"
    assert registry["system"] == "erp"
    assert registry["contracts"][0]["entity"] == "customers"


def test_bootstrap_raises_for_missing_landing(tmp_path: Path) -> None:
    with pytest.raises(typer.Exit) as exc_info:
        bootstrap(
            landing=tmp_path / "missing",
            output_dir=tmp_path / "contracts",
            registry=tmp_path / "contracts" / "_registry.yaml",
            format="csv",
            pattern="*.csv",
            layer="bronze",
            sample_rows=10,
        )

    assert exc_info.value.exit_code == 1
