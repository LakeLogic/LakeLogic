from __future__ import annotations

import types

from lakelogic.core import paths


def test_uri_detection_and_sql_table_refs():
    assert paths.is_uri_path("abfss://container@acct/path") is True
    assert paths.is_uri_path(r"abfss:\\container@acct\path") is True
    assert paths.is_uri_path(r"C:\\data\\lake") is False
    assert paths.make_table_name("bronze", "crm", "orders") == "bronze_crm_orders"
    assert paths.to_sql_table_ref("abfss://container/path", "spark") == "delta.`abfss://container/path`"
    assert paths.to_sql_table_ref("abfss://container/path", "duckdb") == "delta_scan('abfss://container/path')"
    assert paths.to_sql_table_ref("abfss://container/path", "polars") == "abfss://container/path"
    assert paths.to_sql_table_ref("`catalog.schema.table`", "spark") == "catalog.schema.table"
    assert paths.resolve_run_log_ref("`catalog.schema.run_log`", "duckdb") == "catalog.schema.run_log"


def test_resolve_materialization_path_priority_and_fallbacks():
    contract_dict = {"materialization": {"path": "dict-path"}}
    assert paths.resolve_materialization_path(contract=contract_dict, override_path="override") == "override"
    assert paths.resolve_materialization_path(contract=contract_dict) == "dict-path"

    materialization = types.SimpleNamespace(path=None, target_path=None, table="catalog.schema.orders")
    effective = types.SimpleNamespace(path="effective/path")
    contract = types.SimpleNamespace(materialization=materialization, effective_server=lambda: effective)
    assert paths.resolve_materialization_path(contract=contract) == "catalog.schema.orders"

    contract = types.SimpleNamespace(materialization=types.SimpleNamespace(path=None, target_path=None, table=None), effective_server=lambda: effective)
    assert paths.resolve_materialization_path(contract=contract) == "effective/path"

    wrapped_contract = types.SimpleNamespace(contract=types.SimpleNamespace(effective_server=lambda: effective))
    assert paths.resolve_materialization_path(contract=wrapped_contract) == "effective/path"

    registry_contract = types.SimpleNamespace(contract_dict={"materialization": {"target_path": "registry/path"}}, materialization=None)
    assert paths.resolve_materialization_path(contract=registry_contract) == "registry/path"

    broken = types.SimpleNamespace(materialization=None, effective_server=lambda: (_ for _ in ()).throw(RuntimeError("bad")))
    storage = types.SimpleNamespace(external_location_root="abfss://lake", bronze_path="/bronze", bronze_root="/root")
    assert paths.resolve_materialization_path(contract=broken, registry_storage=storage, layer="bronze", system="crm", entity="orders") == "abfss://lake/bronze_crm_orders"

    storage = types.SimpleNamespace(external_location_root=None, bronze_path="/bronze", bronze_root="/root")
    assert paths.resolve_materialization_path(registry_storage=storage, layer="bronze", system="crm", entity="orders") == "/bronze/bronze_crm_orders"

    storage = types.SimpleNamespace(external_location_root=None, bronze_path=None, bronze_root="/root")
    assert paths.resolve_materialization_path(registry_storage=storage, layer="bronze", system="crm", entity="orders") == "/root/orders"
    assert paths.resolve_materialization_path() is None


def test_resolve_quarantine_path_and_azure_storage_options(monkeypatch):
    dict_contract = {"quarantine": {"target": "dict/quarantine"}}
    assert paths.resolve_quarantine_path(contract=dict_contract) == "dict/quarantine"

    obj_contract = types.SimpleNamespace(quarantine=types.SimpleNamespace(target="object/quarantine"))
    assert paths.resolve_quarantine_path(contract=obj_contract) == "object/quarantine"

    storage = types.SimpleNamespace(quarantine_path="/qpath", quarantine_root="/qroot", external_location_root="abfss://lake")
    assert paths.resolve_quarantine_path(registry_storage=storage, layer="silver", system="crm", entity="orders") == "/qpath/silver_crm_orders"

    storage = types.SimpleNamespace(quarantine_path=None, quarantine_root="/qroot", external_location_root="abfss://lake")
    assert paths.resolve_quarantine_path(registry_storage=storage, layer="silver", system="crm", entity="orders") == "/qroot/silver_crm_orders"

    storage = types.SimpleNamespace(quarantine_path=None, quarantine_root=None, external_location_root="abfss://lake")
    assert paths.resolve_quarantine_path(registry_storage=storage, layer="silver", system="crm", entity="orders") == "abfss://lake/_quarantine/silver_crm_orders"
    assert paths.resolve_quarantine_path() is None

    monkeypatch.setenv("AZURE_USE_AZURE_CLI", "true")
    enriched = paths.enrich_azure_storage_options({"account_key": "k", "account_name": "n", "bearer_token": "t"})
    assert enriched["azure_storage_account_key"] == "k"
    assert enriched["azure_storage_account_name"] == "n"
    assert enriched["azure_storage_bearer_token"] == "t"
    assert enriched["azure_storage_use_cli"] == "true"