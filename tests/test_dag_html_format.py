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


def test_title_uses_product_name():
    assert "LakeLogic Pipeline DAG" in _render(CONTRACTS)
    assert "Lakelogic Pipeline DAG" not in _render(CONTRACTS)


def test_subtitle_names_the_layers_instead_of_counting_columns():
    # Source/Downstream columns are drawn too, so a bare "2 layers" read as a miscount.
    html = _render(CONTRACTS)
    assert "2 contracts across Bronze and Silver" in html
    assert " layers<" not in html
    gold = CONTRACTS + [_c("gold", "fact_trips", "Gold — Trips")]
    assert "3 contracts across Bronze, Silver and Gold" in _render(gold)


def test_tall_layer_wraps_into_sub_columns_by_dependency_depth():
    # 7 dims and 3 facts that read them: gold must not be one column of 10.
    dims = [_c("gold", f"dim_{i}", f"Dim {i}") for i in range(7)]
    facts = [_c("gold", f"fact_{i}", f"Fact {i}", depends_on=["dim_0"]) for i in range(3)]
    html = _render(CONTRACTS + dims + facts)
    assert html.count('class="dag-col-hdr"') == 3  # still one GOLD header, not one per sub-column
    import re

    tops = [int(t) for t in re.findall(r"top:(\d+)px;", html)]
    assert max(tops) < 40 + 10 + 5 * 180  # at most five rows tall


def test_short_layers_keep_a_single_column():
    html = _render(CONTRACTS)
    assert html.count('class="dag-col-hdr"') == 2


def test_fit_to_width_toggle_is_offered():
    html = _render(CONTRACTS)
    assert ">Fit to width</button>" in html
    assert 'class="dag-canvas"' in html


def test_theme_follows_the_host_by_default():
    # Databricks dark mode inverts the output frame without reporting dark; the old
    # prefers-color-scheme counter-invert never fired and the DAG came out grey (2026-10-30).
    html = _render(CONTRACTS)
    assert 'data-theme="auto"' in html
    assert "invert(1) hue-rotate(180deg)" not in html.split("<script>")[0]
    assert "--dag-bg:#121212" in html and "--dag-bg:#f8f9fa" in html
    assert "setProperty('filter','none','important')" in html


def test_explicit_theme_is_forced():
    p = object.__new__(LakehousePipeline)
    p.registry = SimpleNamespace(
        get_active_contracts=lambda: CONTRACTS, external_sources=[], storage=None, domain="d", system="s"
    )
    assert 'data-theme="light"' in p.visualize_dag(theme="light")
    assert 'data-theme="dark"' in p.visualize_dag(theme="dark")
