"""Strict schema must not reject a type-1 (merge) dimension for lacking its surrogate key.

The materializer injects `scd1.surrogate_key` AFTER schema enforcement, like the SCD2
mechanics columns. Only SCD2 was exempt, so every row of Build Centre's code dimensions
(gold_rideflow_dim_trip_type, ...) quarantined with "Missing fields: trip_type_sk"
under `evolution: strict` (2026-09-30).
"""
from types import SimpleNamespace

from lakelogic.engines.base import EngineAdapter


def _injected(materialization):
    return EngineAdapter._scd2_injected_columns(SimpleNamespace(contract=SimpleNamespace(materialization=materialization)))


def test_scd1_surrogate_key_is_injected_not_missing():
    mat = SimpleNamespace(strategy="merge", scd1={"surrogate_key": "trip_type_sk", "surrogate_key_strategy": "hash"})
    assert _injected(mat) == {"trip_type_sk"}


def test_merge_without_a_surrogate_key_exempts_nothing():
    assert _injected(SimpleNamespace(strategy="merge", scd1=None)) == set()
    assert _injected(SimpleNamespace(strategy="merge", scd1={})) == set()


def test_no_materialization_exempts_nothing():
    assert _injected(None) == set()


def test_scd2_is_unchanged():
    mat = SimpleNamespace(strategy="scd2", scd2={"surrogate_key": "rider_sk", "version_column": "version_number"})
    assert {"rider_sk", "effective_from", "effective_to", "is_current", "version_number"} <= _injected(mat)


def test_every_engine_subtracts_injected_columns_before_flagging_missing():
    """The Spark, Snowflake and BigQuery adapters flagged `missing` without consulting the
    injected set (polars and duckdb already did)."""
    from pathlib import Path

    engines = Path(__file__).resolve().parents[1] / "lakelogic" / "engines"
    for name in ("spark", "snowflake", "bigquery", "polars", "duckdb"):
        src = (engines / f"{name}.py").read_text(encoding="utf-8")
        flag = src.index('if evolution == "strict" and missing and not _has_post_sql:')
        assert "_scd2_injected_columns()" in src[max(0, flag - 6000):flag], name
