from __future__ import annotations

import yaml
import pytest

from lakelogic.core.registry import (
    CloudReporting,
    DomainRegistry,
    EnvironmentConfig,
    RegistryContract,
    _deep_merge,
    _resolve_placeholders,
)


def test_registry_helper_models_and_merge(monkeypatch):
    monkeypatch.setenv("REPORT_URL", "https://reports.example/api")
    monkeypatch.setenv("ENV_CATALOG", "main")

    reporting = CloudReporting(report_url="${REPORT_URL}", api_key="plain")
    assert reporting.report_url == "https://reports.example/api"
    assert reporting.api_key == "plain"

    env_cfg = EnvironmentConfig(catalog="${ENV_CATALOG}", storage_account="acct")
    assert env_cfg.catalog == "main"

    merged = _deep_merge({"a": {"b": 1}, "list": [1]}, {"a": {"c": 2}, "list": [2]})
    assert merged == {"a": {"b": 1, "c": 2}, "list": [2]}
    assert _resolve_placeholders({"path": "{domain}/{system}", "items": ["{domain}"]}, {"domain": "sales", "system": "crm"}) == {
        "path": "sales/crm",
        "items": ["sales"],
    }


def test_registry_validates_unique_entities():
    with pytest.raises(ValueError, match="Duplicate contract entity"):
        DomainRegistry(
            domain="sales",
            system="crm",
            contracts=[
                RegistryContract(layer="bronze", entity="orders", path="orders.yaml"),
                RegistryContract(layer="silver", entity="orders", path="orders_silver.yaml"),
            ],
        )


def test_registry_from_yaml_resolves_inheritance_and_contracts(tmp_path):
    domain_dir = tmp_path / "sales"
    system_dir = domain_dir / "crm"
    contracts_dir = system_dir / "contracts"
    contracts_dir.mkdir(parents=True)

    domain_yaml = {
        "domain": "sales",
        "bronze_layer": "bronze",
        "notifications_enabled": False,
        "lineage": {"timestamp_column_name": "_loaded_at"},
        "materialization": {"bronze": {"format": "delta"}},
        "metadata": {"owner": "data-eng"},
        "cost": {"currency": "USD"},
    }
    (domain_dir / "_domain.yaml").write_text(yaml.safe_dump(domain_yaml, sort_keys=False), encoding="utf-8")

    contract_yaml = {
        "info": {"title": "Orders", "version": "1.0.0", "table_name": "{bronze_layer}_{system}_orders"},
        "model": {"fields": [{"name": "id", "type": "integer"}]},
        "materialization": {"target_path": "{bronze_root}/orders"},
        "compliance": {"data_residency": "EU"},
    }
    (contracts_dir / "orders.yaml").write_text(yaml.safe_dump(contract_yaml, sort_keys=False), encoding="utf-8")

    registry_yaml = {
        "system": "crm",
        "environments": {"dev": {"catalog": "main", "storage_account": "acct", "region": "EU"}},
        "storage": {
            "bronze_root": "/Volumes/{catalog}/{domain}/bronze",
            "bronze_path": "abfss://{domain}@{storage_account}.dfs.core.windows.net/bronze",
            "landing_path": "abfss://{domain}@{storage_account}.dfs.core.windows.net/landing",
        },
        "contracts": [
            {"layer": "bronze", "entity": "orders", "path": "contracts/orders.yaml", "schedule": {"cron": "0 0 * * *"}},
            {"layer": "bronze", "entity": "missing", "path": "contracts/missing.yaml"},
        ],
    }
    registry_path = system_dir / "_registry.yaml"
    registry_path.write_text(yaml.safe_dump(registry_yaml, sort_keys=False), encoding="utf-8")

    registry = DomainRegistry.from_yaml(str(registry_path), environment="dev", storage_mode="direct")
    assert registry.domain == "sales"
    assert registry.notifications_enabled is False
    assert registry.storage.bronze_path == "abfss://sales@acct.dfs.core.windows.net/bronze"

    active = registry.get_active_contracts("bronze")
    assert len(active) == 1
    contract = active[0]
    assert contract.entity == "orders"
    assert contract.resolved_path.endswith("contracts\\orders.yaml") or contract.resolved_path.endswith("contracts/orders.yaml")
    assert contract.contract_dict["info"]["table_name"] == "bronze_crm_orders"
    assert contract.contract_dict["materialization"]["target_path"] == "abfss://sales@acct.dfs.core.windows.net/bronze/orders"
    assert contract.contract_dict["schedule"]["cron"] == "0 0 * * *"