"""A contract's `source` and `links` already say what it reads, so the registry's
`depends_on` only has to state what they do not (2026-10-13).

Before this, ordering inside a layer came from `depends_on` alone: a fact that linked to a
dimension but was missing the `depends_on` line ran first, and every surrogate key fell
back to the unknown member.
"""

from __future__ import annotations

import types

from lakelogic.pipeline import impact


def _c(entity, *, layer="gold", depends_on=None, links=None, source=None, table=None):
    doc = {"info": {"table_name": table or entity}, "dataset": entity}
    if links is not None:
        doc["links"] = links
    if source is not None:
        doc["source"] = source
    return types.SimpleNamespace(layer=layer, entity=entity, depends_on=list(depends_on or []), contract_dict=doc)


def _link(name, path):
    return {"name": name, "type": "table", "path": path}


def test_a_link_to_a_same_layer_contract_becomes_a_dependency():
    dim = _c("gold_dim_driver")
    fact = _c("gold_fact_trips", links=[_link("d", "table:`cat`.marketplace.gold_dim_driver")])
    added = impact.infer_dependencies([fact, dim])
    assert added == {"gold_fact_trips": ["gold_dim_driver"]}
    assert [c.entity for c in impact.topological_order([fact, dim])] == ["gold_dim_driver", "gold_fact_trips"]


def test_a_same_layer_source_becomes_a_dependency_and_another_layer_does_not():
    base = _c("gold_fact_trips", source={"type": "table", "path": "table:cat.m.silver_trips"})
    agg = _c("gold_agg_daily", source={"type": "table", "path": "table:cat.m.gold_fact_trips"})
    silver = _c("silver_trips", layer="silver")
    assert impact.infer_dependencies([agg, base, silver]) == {"gold_agg_daily": ["gold_fact_trips"]}
    assert base.depends_on == []  # silver is ordered by the layer sequence, not by depends_on


def test_explicit_depends_on_is_kept_first_and_not_repeated():
    dim, other = _c("gold_dim_driver"), _c("gold_dim_date")
    fact = _c(
        "gold_fact_trips",
        depends_on=["gold_dim_date", "gold_dim_driver"],
        links=[_link("gold_dim_driver", "table:c.m.gold_dim_driver")],
    )
    assert impact.infer_dependencies([fact, dim, other]) == {}
    assert fact.depends_on == ["gold_dim_date", "gold_dim_driver"]


def test_a_dependency_only_the_registry_knows_still_orders_the_run():
    """What `depends_on` is for: an ordering no `source` or `links` line shows."""
    late = _c("gold_export", depends_on=["gold_fact_trips"])  # reads it inside custom logic
    fact = _c("gold_fact_trips")
    assert impact.infer_dependencies([late, fact]) == {}
    assert [c.entity for c in impact.topological_order([late, fact])] == ["gold_fact_trips", "gold_export"]


def test_a_link_is_matched_by_table_name_alias_or_dataset():
    dim = _c("driver_dim", table="{catalog}.m.gold_dim_driver")
    dim.contract_dict["dataset"] = "dim_driver_ds"
    by_table = _c("f1", links=[_link("x", "table:c.m.gold_dim_driver")])
    by_dataset = _c("f2", links=[_link("dim_driver_ds", "/Volumes/x/files")])  # a file path names no table
    by_entity = _c("f3", links=[_link("driver_dim", "abfss://c@a/x")])
    added = impact.infer_dependencies([by_table, by_dataset, by_entity, dim])
    assert added == {"f1": ["driver_dim"], "f2": ["driver_dim"], "f3": ["driver_dim"]}


def test_inference_never_closes_a_cycle_and_ignores_a_self_reference():
    a = _c("a", depends_on=["b"])
    b = _c("b", links=[_link("a", "table:c.m.a"), _link("b", "table:c.m.b")])
    assert impact.infer_dependencies([a, b]) == {}
    assert b.depends_on == []
    assert [c.entity for c in impact.topological_order([a, b])] == ["b", "a"]


def test_a_contract_that_was_not_loaded_is_left_alone():
    ghost = types.SimpleNamespace(layer="gold", entity="ghost", depends_on=[], contract_dict=None)
    fact = _c("gold_fact", links=[_link("ghost", "table:c.m.ghost")])
    assert impact.infer_dependencies([fact, ghost]) == {}
