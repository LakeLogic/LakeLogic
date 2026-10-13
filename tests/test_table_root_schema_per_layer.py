"""A schema per layer (catalog per domain): the pipeline writes each layer to its own root.

`storage.bronze_root` / `silver_root` / `gold_root` were read by the SLO checks only; the
pipeline wrote every layer to `domain_catalog`. So an estate laid out as

    `dev_marketplace`.bronze / .silver / .gold / .quarantine / .lakelogic

could be measured but not built (2026-10-13).
"""

from __future__ import annotations

import types

from lakelogic.core.registry import RegistryStorage, table_root
from lakelogic.pipeline.runner import LakehousePipeline

PER_LAYER = RegistryStorage(
    domain_catalog="`dev_marketplace`.lakelogic",
    bronze_root="`dev_marketplace`.bronze",
    silver_root="`dev_marketplace`.silver",
    gold_root="`dev_marketplace`.gold",
    quarantine_root="`dev_marketplace`.quarantine",
    quarantine_table_name="quarantine_{table}",
    run_log_table="`dev_marketplace`.lakelogic._lakelogic_run_log",
)
ONE_SCHEMA = RegistryStorage(
    domain_catalog="`dev`.marketplace", quarantine_root="`dev`.marketplace", quarantine_table_name="quarantine_{table}"
)


def test_table_root_is_the_layer_root_when_declared_else_the_domain_catalog():
    assert table_root(PER_LAYER, "gold") == "`dev_marketplace`.gold"
    assert table_root(PER_LAYER, "Bronze") == "`dev_marketplace`.bronze"
    assert table_root(ONE_SCHEMA, "gold") == "`dev`.marketplace"
    assert table_root(PER_LAYER, None) == "`dev_marketplace`.lakelogic"
    assert table_root(PER_LAYER, "platinum") == "`dev_marketplace`.lakelogic"  # no such root: the fallback
    assert table_root(None, "gold") is None


def _resolved(storage, layer, table):
    pipeline = types.SimpleNamespace(
        registry=types.SimpleNamespace(
            storage=storage,
            quarantine={"enabled": True},
            lineage=None,
            materialization={},
            server=None,
            metadata=None,
            slo=None,
            compliance=None,
        ),
        storage_mode="catalog",
        _looks_like_catalog_ref=LakehousePipeline._looks_like_catalog_ref,
    )
    doc = {"info": {"table_name": table, "target_layer": layer, "domain": "marketplace", "system": "rideflow"}}
    return LakehousePipeline._resolve_uc_paths(pipeline, doc)


def test_each_layer_is_written_to_its_own_schema():
    gold = _resolved(PER_LAYER, "gold", "gold_dim_driver")
    assert gold["materialization"]["target_path"] == "table:`dev_marketplace`.gold.gold_dim_driver"
    assert gold["quarantine"]["target"] == "table:`dev_marketplace`.quarantine.quarantine_gold_dim_driver"
    assert gold["metadata"]["run_log_table"] == "`dev_marketplace`.lakelogic._lakelogic_run_log"
    bronze = _resolved(PER_LAYER, "bronze", "bronze_rideflow_trips")
    assert bronze["materialization"]["target_path"] == "table:`dev_marketplace`.bronze.bronze_rideflow_trips"


def test_an_estate_with_one_schema_per_domain_is_unchanged():
    gold = _resolved(ONE_SCHEMA, "gold", "gold_dim_driver")
    assert gold["materialization"]["target_path"] == "table:`dev`.marketplace.gold_dim_driver"
    assert gold["quarantine"]["target"] == "table:`dev`.marketplace.quarantine_gold_dim_driver"


def test_an_explicit_target_is_never_overridden():
    pipeline = types.SimpleNamespace(
        registry=types.SimpleNamespace(storage=PER_LAYER, quarantine={}, lineage=None, materialization={}, server=None),
        storage_mode="catalog",
        _looks_like_catalog_ref=LakehousePipeline._looks_like_catalog_ref,
    )
    doc = {
        "info": {"table_name": "gold_x", "target_layer": "gold"},
        "materialization": {"target_path": "table:other.schema.gold_x"},
    }
    assert (
        LakehousePipeline._resolve_uc_paths(pipeline, doc)["materialization"]["target_path"]
        == "table:other.schema.gold_x"
    )
