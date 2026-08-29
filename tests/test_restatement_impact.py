"""Tests for the advisory restatement impact report."""

from __future__ import annotations

import types

import pytest

from lakelogic.pipeline import impact, runner


def _c(layer, entity, depends_on=None, table=None):
    return types.SimpleNamespace(
        layer=layer,
        entity=entity,
        depends_on=list(depends_on or []),
        contract_dict={"info": {"table_name": table or f"{layer}_{entity}"}},
    )


# ── Downstream index ─────────────────────────────────────────────────────────


def test_transitive_downstream_crosses_layers():
    contracts = [
        _c("bronze", "orders"),
        _c("silver", "curated_orders", ["orders"]),  # resolves cross-layer to bronze.orders
        _c("gold", "revenue", ["curated_orders"]),
    ]
    index = impact.build_downstream_index(contracts)
    downstream, cyclic = index.downstream_of(["bronze.orders"])

    # bronze → silver → gold, transitively
    assert downstream == ["gold.revenue", "silver.curated_orders"]
    assert cyclic == []
    assert index.unresolved == []
    assert index.ambiguous == []


def test_same_entity_name_in_two_layers_links_upstream_not_itself():
    contracts = [_c("bronze", "orders"), _c("silver", "orders", ["orders"])]
    index = impact.build_downstream_index(contracts)
    assert index.downstream_of(["bronze.orders"])[0] == ["silver.orders"]
    # No self-edge, and it is not reported as unresolved.
    assert index.downstream_of(["silver.orders"])[0] == []
    assert index.unresolved == []


def test_ambiguous_dependency_links_all_candidates_and_is_reported():
    contracts = [
        _c("bronze", "orders"),
        _c("silver", "orders", ["orders"]),
        _c("gold", "revenue", ["orders"]),  # matches bronze AND silver
    ]
    index = impact.build_downstream_index(contracts)
    assert index.ambiguous == [
        {
            "consumer": "gold.revenue",
            "declared_dependency": "orders",
            "candidates": ["bronze.orders", "silver.orders"],
        }
    ]
    assert index.downstream_of(["bronze.orders"])[0] == ["gold.revenue", "silver.orders"]


def test_entity_with_no_dependents_returns_empty_set_not_error():
    contracts = [_c("bronze", "orders"), _c("bronze", "customers")]
    index = impact.build_downstream_index(contracts)
    downstream, cyclic = index.downstream_of(["bronze.customers"])
    assert downstream == []
    assert cyclic == []


def test_unresolvable_depends_on_is_reported_as_unknown_not_dropped():
    contracts = [
        _c("bronze", "orders"),
        _c("silver", "curated_orders", ["orders"]),
        _c("gold", "revenue", ["ghost_entity"]),
    ]
    index = impact.build_downstream_index(contracts)
    assert index.unresolved == [
        {"consumer": "gold.revenue", "consumer_layer": "gold", "declared_dependency": "ghost_entity"}
    ]

    report = impact.build_restatement_impact(
        contracts,
        restated=[("bronze", "orders")],
        in_run_scope=[("bronze", "orders")],
    )
    assert report["unknown_dependencies"] == [
        {"consumer": "gold.revenue", "consumer_layer": "gold", "declared_dependency": "ghost_entity"}
    ]
    rendered = impact.format_restatement_impact(report)
    assert "impact UNKNOWN, not 'none'" in rendered
    assert "gold.revenue declares depends_on 'ghost_entity'" in rendered


def test_circular_dependency_does_not_hang_or_crash_the_report():
    contracts = [
        _c("silver", "a", ["b"]),
        _c("silver", "b", ["a"]),
    ]
    # The topological sort still raises — behaviour unchanged.
    with pytest.raises(ValueError, match="Circular dependency"):
        impact.topological_order(contracts)

    # The impact report does not: it terminates and reports the cycle.
    report = impact.build_restatement_impact(
        contracts,
        restated=[("silver", "a")],
        in_run_scope=[("silver", "a"), ("silver", "b")],
    )
    assert report["error"] is None
    assert [d["node_id"] for d in report["downstream"]] == ["silver.b"]
    assert report["cyclic_dependencies"] == ["silver.a"]
    assert "reachable from itself" in impact.format_restatement_impact(report)


def test_downstream_not_in_run_targets_is_the_actionable_subset():
    contracts = [
        _c("bronze", "orders"),
        _c("silver", "curated_orders", ["orders"]),
        _c("gold", "revenue", ["curated_orders"]),
    ]
    report = impact.build_restatement_impact(
        contracts,
        restated=[("bronze", "orders")],
        in_run_scope=[("bronze", "orders")],  # silver/gold omitted from targets
    )
    assert [d["node_id"] for d in report["downstream_not_in_run_scope"]] == [
        "gold.revenue",
        "silver.curated_orders",
    ]
    assert all(d["in_run_scope"] is False for d in report["downstream_not_in_run_scope"])

    rendered = impact.format_restatement_impact(report)
    assert "2 downstream contract(s) were not in this run's targets" in rendered
    # Impact, not verdict.
    assert "MAY now be built on superseded data" in rendered
    # Honest about what the graph is derived from.
    assert "lower bound, not a completeness guarantee" in rendered


def test_downstream_in_scope_is_not_flagged_as_actionable():
    contracts = [_c("bronze", "orders"), _c("silver", "curated_orders", ["orders"])]
    report = impact.build_restatement_impact(
        contracts,
        restated=[("bronze", "orders")],
        in_run_scope=[("bronze", "orders"), ("silver", "curated_orders")],
    )
    assert report["downstream_not_in_run_scope"] == []
    assert "Every declared downstream consumer was in this run's targets" in impact.format_restatement_impact(report)


# ── Restatement detection ────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "kwargs,expected",
    [
        ({}, False),
        ({"reprocess_from": "2026-01-01"}, True),
        ({"reprocess_to": "2026-01-31"}, True),
        ({"reprocess_column": "order_id"}, False),  # column alone is not a reprocess
        ({"reprocess_column": "order_id", "reprocess_values": ["1"]}, True),
        ({"reprocess_values": ["1"]}, False),
    ],
)
def test_is_restatement_run_mirrors_the_processor_reprocess_signal(kwargs, expected):
    assert impact.is_restatement_run(**kwargs) is expected


# ── Runner integration ───────────────────────────────────────────────────────


def _pipeline(monkeypatch, contracts):
    registry = types.SimpleNamespace(
        domain="commerce",
        system="erp",
        storage_mode="uc",
        storage=None,
        get_active_contracts=lambda: list(contracts),
    )
    pipeline = runner.LakehousePipeline(registry, engine="polars")
    monkeypatch.setattr(pipeline, "_resolve_uc_paths", lambda contract_dict: contract_dict)

    def fake_process(contract, layer, summary, *args, **kwargs):
        summary.append(contract.entity, layer, "success", rows=10)

    monkeypatch.setattr(pipeline, "_process_contract_with_retry", fake_process)
    return pipeline


def test_non_reprocess_run_produces_no_impact_report(monkeypatch):
    contracts = [
        _c("bronze", "orders"),
        _c("silver", "curated_orders", ["orders"]),
        _c("gold", "revenue", ["curated_orders"]),
    ]
    pipeline = _pipeline(monkeypatch, contracts)

    summary = pipeline.run(target_layers="bronze")

    assert summary.restatement_impact is None
    assert "restatement_impact" not in summary.to_dict()
    assert "RESTATEMENT IMPACT" not in str(summary)


def test_bronze_reprocess_reports_out_of_scope_downstream(monkeypatch):
    contracts = [
        _c("bronze", "orders"),
        _c("silver", "curated_orders", ["orders"]),
        _c("gold", "revenue", ["curated_orders"]),
    ]
    pipeline = _pipeline(monkeypatch, contracts)

    summary = pipeline.run(target_layers="bronze", reprocess_from="2026-01-01", reprocess_to="2026-01-31")

    report = summary.restatement_impact
    assert report is not None
    assert [r["node_id"] for r in report["restated"]] == ["bronze.orders"]
    assert [d["node_id"] for d in report["downstream_not_in_run_scope"]] == [
        "gold.revenue",
        "silver.curated_orders",
    ]
    assert summary.to_dict()["restatement_impact"] is report
    assert "RESTATEMENT IMPACT" in str(summary)
    # Advisory: nothing was failed by the report.
    assert not summary.has_failures()


def test_impact_report_failure_never_breaks_the_run(monkeypatch):
    contracts = [_c("bronze", "orders")]
    pipeline = _pipeline(monkeypatch, contracts)
    monkeypatch.setattr(
        runner,
        "build_restatement_impact",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("graph exploded")),
    )

    summary = pipeline.run(target_layers="bronze", reprocess_from="2026-01-01")

    assert summary.restatement_impact is None
    assert [r["status"] for r in summary.results] == ["success"]


# ── Refactor guard: _topological_sort behaviour is unchanged ─────────────────


def test_topological_sort_behaviour_unchanged():
    contracts = [
        types.SimpleNamespace(entity="bronze_orders", depends_on=[]),
        types.SimpleNamespace(entity="silver_orders", depends_on=["bronze_orders"]),
        types.SimpleNamespace(entity="gold_orders", depends_on=["silver_orders"]),
        types.SimpleNamespace(entity="silver_customers", depends_on=[]),
    ]
    ordered = runner.LakehousePipeline._topological_sort(contracts)
    assert [c.entity for c in ordered] == [
        "bronze_orders",
        "silver_customers",
        "silver_orders",
        "gold_orders",
    ]

    # Dependencies on entities outside the set are ignored for ordering
    # (they no longer block the sort) — unchanged from before the refactor.
    external = [
        types.SimpleNamespace(entity="silver_orders", depends_on=["not_loaded"]),
    ]
    assert [c.entity for c in runner.LakehousePipeline._topological_sort(external)] == ["silver_orders"]

    cyclic = [
        types.SimpleNamespace(entity="a", depends_on=["b"]),
        types.SimpleNamespace(entity="b", depends_on=["a"]),
    ]
    with pytest.raises(ValueError, match="Circular dependency detected among contracts"):
        runner.LakehousePipeline._topological_sort(cyclic)
