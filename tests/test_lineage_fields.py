"""The runtime DataContract is lineage-aware of the nested OLC lineage fields.

Guards that `upstream_sources` (source -> landing -> bronze), `upstream_contracts`, and the
nested `downstream.consumers` (table -> semantic_model -> report) parse into TYPED objects,
not just accepted-and-ignored extras. These classes are imported from `olc.models._nested`,
so this also proves the runtime tracks the OLC spec's multi-hop lineage.
"""
from __future__ import annotations

from lakelogic.core.models import DataContract

_BASE = {
    "version": "1.0.0",
    "info": {"title": "Bronze GA4", "table_name": "bronze_ga4"},
    "model": {"fields": [{"name": "event_name", "type": "string"}]},
}


def _contract(**extra) -> DataContract:
    return DataContract(**{**_BASE, **extra})


def test_upstream_sources_nested_is_typed():
    c = _contract(
        upstream_sources=[
            {
                "type": "landing",
                "name": "GA4 app_events landing",
                "path": "{landing_root}/app_events",
                "format": "json",
                "upstream_sources": [
                    {"type": "source_system", "name": "Google Analytics 4", "system": "Google LLC"},
                ],
            }
        ]
    )
    top = c.upstream_sources[0]
    assert top.type == "landing" and top.name == "GA4 app_events landing"
    # nested origin (the source system behind the landing zone) is a typed UpstreamSource
    nested = top.upstream_sources[0]
    assert nested.type == "source_system"
    assert nested.name == "Google Analytics 4"
    assert nested.system == "Google LLC"


def test_downstream_consumers_nested_is_typed():
    c = _contract(
        downstream=[
            {
                "type": "semantic_model",
                "name": "RideFlow Gold",
                "platform": "powerbi",
                "consumers": [{"type": "report", "name": "Exec", "platform": "powerbi"}],
            }
        ]
    )
    model = c.downstream[0]
    assert model.type == "semantic_model"
    assert model.consumers[0].type == "report"
    assert model.consumers[0].name == "Exec"


def test_upstream_contracts_is_typed():
    c = _contract(upstream_contracts=[{"contract": "silver_ga4_sessions", "layer": "silver"}])
    assert c.upstream_contracts[0].contract == "silver_ga4_sessions"
    assert c.upstream_contracts[0].layer == "silver"


def test_lineage_fields_default_empty():
    c = _contract()
    assert c.upstream_sources == []
    assert c.upstream_contracts == []
    assert c.downstream == []
