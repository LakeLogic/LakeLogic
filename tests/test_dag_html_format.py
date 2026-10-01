"""DAG card format: column headers instead of per-card layer badges, no repeated system line,
version chip only when it says something (not the 1.0.0 default)."""

from types import SimpleNamespace

from lakelogic.pipeline.runner import LakehousePipeline


def _c(layer, entity, title, version="1.0.0", depends_on=()):
    return SimpleNamespace(
        layer=layer,
        entity=entity,
        depends_on=list(depends_on),
        contract_dict={"info": {"title": title}, "version": version, "model": {"fields": []}},
    )


def _render(contracts):
    reg = SimpleNamespace(
        get_active_contracts=lambda: contracts,
        external_sources=[],
        storage=None,
        domain="marketplace",
        system="rideflow",
    )
    p = object.__new__(LakehousePipeline)
    p.registry = reg
    return p.visualize_dag()


CONTRACTS = [
    _c("bronze", "trips", "Bronze — Trips"),
    _c("silver", "trips_clean", "Silver — Trips", version="1.2.0"),
]


def test_headers_only_for_used_layers():
    html = _render(CONTRACTS)
    assert ">BRONZE</div>" in html and ">SILVER</div>" in html
    assert ">GOLD</div>" not in html and ">DOWNSTREAM</div>" not in html


def test_layer_prefix_stripped_full_title_kept():
    html = _render(CONTRACTS)
    assert '<div class="dag-ttl">Trips</div>' in html
    assert 'title="Bronze — Trips"' in html
    assert ">Bronze — Trips<" not in html


def test_version_chip_only_when_not_default():
    html = _render(CONTRACTS)
    assert ">V1.2.0<" in html
    assert "V1.0.0" not in html


def test_single_system_not_repeated_on_cards():
    html = _render(CONTRACTS)
    assert ">RIDEFLOW<" not in html
    assert 'class="dag-sys"' not in html


def test_edges_carry_endpoints_for_hover():
    html = _render(CONTRACTS)
    assert 'data-src="bronze_trips" data-dst="silver_trips_clean"' in html
