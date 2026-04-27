from __future__ import annotations

import types

from lakelogic.core import cost_provider as cp


def test_avg_nodes_none_and_scaling_modes():
    assert cp._resolve_avg_nodes() == 1.0
    config = {"min_nodes": 2, "max_nodes": 6}
    assert cp._resolve_avg_nodes(config) == 4.0
    assert cp._resolve_avg_nodes({**config, "scaling_assumption": "peak"}) == 6.0
    assert cp._resolve_avg_nodes({**config, "scaling_assumption": "min"}) == 2.0
    assert cp._resolve_avg_nodes({**config, "scaling_assumption": "p75"}) == 5.0


def test_none_and_manual_cost_providers():
    assert cp.NoneCostProvider().estimate().estimated_cost == 0.0

    provider = cp.ManualCostProvider(
        dbu_per_hour=1.2,
        currency="GBP",
        attribution="duration_proportional",
        cluster_config={"min_nodes": 2, "max_nodes": 4},
        layer_rates={"gold": {"dbu_per_hour": 2.0, "cluster": {"min_nodes": 1, "max_nodes": 5, "scaling_assumption": "peak"}}},
    )

    assert provider._rate_for_layer("silver") == (1.2, 3.0)
    assert provider._rate_for_layer("gold") == (2.0, 5.0)

    estimate = provider.estimate(duration_seconds=1800, layer="gold")
    assert estimate.estimated_cost == 5.0
    assert estimate.currency == "GBP"
    assert estimate.confidence == "estimated"


def test_databricks_cost_provider_exact_and_fallback(monkeypatch):
    rows = [{"total_dbus": 2.5}]
    spark = types.SimpleNamespace(sql=lambda query: types.SimpleNamespace(collect=lambda: rows))
    provider = cp.DatabricksUCCostProvider(spark=spark, fallback_dbu_rate=0.4, currency="EUR")
    exact = provider.estimate(run_id="run-1")
    assert exact.estimated_cost == 1.0
    assert exact.confidence == "exact"
    assert exact.attribution_method == "direct"

    debug_messages = []
    monkeypatch.setattr(cp.logger, "debug", debug_messages.append)
    failing_spark = types.SimpleNamespace(sql=lambda query: (_ for _ in ()).throw(RuntimeError("billing down")))
    provider = cp.DatabricksUCCostProvider(spark=failing_spark, fallback_dbu_rate=0.5)
    fallback = provider.estimate(run_id="run-2", duration_seconds=7200)
    assert fallback.estimated_cost == 1.0
    assert fallback.confidence == "estimated"
    assert any("falling back to duration estimate" in message for message in debug_messages)

    assert cp.DatabricksUCCostProvider(spark=None, fallback_dbu_rate=0.5).estimate(duration_seconds=3600).estimated_cost == 0.5


def test_cost_provider_factory_currency_and_budget(monkeypatch):
    warnings = []
    monkeypatch.setattr(cp.logger, "warning", warnings.append)

    assert isinstance(cp.resolve_cost_provider(), cp.NoneCostProvider)
    assert isinstance(cp.resolve_cost_provider({"provider": "none"}), cp.NoneCostProvider)
    assert isinstance(
        cp.resolve_cost_provider({"provider": "manual", "rates": {"dbu_per_hour": 1.5}, "layer_rates": {}}, spark="ignored"),
        cp.ManualCostProvider,
    )
    databricks = cp.resolve_cost_provider({"provider": "databricks", "billing_tag_key": "run_tag"}, spark="spark")
    assert isinstance(databricks, cp.DatabricksUCCostProvider)
    assert databricks.tag_key == "run_tag"
    assert isinstance(cp.resolve_cost_provider({"provider": "unknown"}), cp.NoneCostProvider)
    assert any("Unknown cost provider 'unknown'" in message for message in warnings)

    assert cp.validate_cost_currency({"currency": "USD"}, {"currency": "EUR"}) == "USD"
    assert cp.validate_cost_currency({"currency": "USD"}, {}) == "USD"
    assert cp.validate_cost_currency({}, {"currency": "GBP"}) == "GBP"
    assert cp.validate_cost_currency({}, {}) == "USD"
    assert any("Cost currency mismatch" in message for message in warnings)

    budget = {"budget": {"per_run_anomaly_multiplier": 2}}
    assert cp.check_budget_threshold(estimated_cost=10.0, cost_config=budget, entity="orders", historical_avg=4.0) == (
        "Cost anomaly: orders run cost $10.0000 exceeds 2× historical average $4.0000"
    )
    assert cp.check_budget_threshold(estimated_cost=5.0, cost_config=budget, entity="orders", historical_avg=4.0) is None
    assert cp.check_budget_threshold(estimated_cost=5.0, cost_config={}, entity="orders", historical_avg=4.0) is None
