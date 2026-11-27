"""Business ontology SVG: same layout as the platform page, measured sizes, domain filter, CLI."""

from __future__ import annotations

import copy
import re

from typer.testing import CliRunner

from lakelogic.cli.main import app
from lakelogic.core.business_map import derive_business_map
from lakelogic.core.ontology_render import layout, load_contract_folder, radius, render_ontology_svg


def _c(name, key, fields, system, domain):
    return {
        "name": name,
        "system": system,
        "_domain": domain,
        "primary_key": key,
        "model": {"fields": [{"name": f} for f in fields]},
        "info": {"title": name},
    }


ESTATE = [
    _c("silver_rideflow_driver_profiles", ["driver_id"], ["driver_id", "city_code"], "rideflow", "marketplace"),
    _c("silver_rideflow_trips", ["trip_id"], ["trip_id", "driver_id", "rider_id"], "rideflow", "marketplace"),
    _c("silver_rideflow_riders", ["rider_id"], ["rider_id"], "rideflow", "marketplace"),
    _c("silver_stripe_payouts", ["payout_id"], ["payout_id", "driver_id"], "stripe", "payments"),
]


def test_sizes_are_measured_and_layout_is_deterministic():
    m = derive_business_map(copy.deepcopy(ESTATE))
    a = layout(m["entities"], [])
    b = layout(m["entities"], [])
    assert a == b
    drv = next(p for p in a[0] if p["name"] == "Driver")
    assert drv["r"] == radius(3)  # driver_id is in three Silver contracts
    for i, p in enumerate(a[0]):
        for q in a[0][i + 1 :]:
            assert ((p["x"] - q["x"]) ** 2 + (p["y"] - q["y"]) ** 2) ** 0.5 > p["r"] + q["r"]


def test_svg_is_standalone_and_themed():
    m = derive_business_map(copy.deepcopy(ESTATE))
    light, dark = render_ontology_svg(m, "light"), render_ontology_svg(m, "dark")
    assert light.startswith("<svg xmlns=") and light.endswith("</svg>")
    assert 'fill="#ffffff"' in light and 'fill="#0b0e12"' in dark
    assert ">Driver<" in light and "3 contracts" in light
    assert "Marketplace" in light and "Payments" in light


def test_domain_filter_keeps_colours():
    m = derive_business_map(copy.deepcopy(ESTATE))
    full = render_ontology_svg(m)
    only = render_ontology_svg(m, domains=["Payments"])
    assert ">Payout<" in only and ">Driver<" not in only

    def colour(svg, name):
        return re.search(rf'stroke="(#[0-9a-f]+)"[^>]*/>\s*(?:<circle[^>]*/>\s*)?<text[^>]*>{name}<', svg).group(1)

    assert colour(full, "Payout") == colour(only, "Payout")


def test_folder_loader_reads_domains_and_skips_registry_files(tmp_path):
    d = tmp_path / "payments" / "stripe"
    d.mkdir(parents=True)
    (d / "silver_stripe_payouts.yaml").write_text("model:\n  fields:\n    - name: payout_id\n", encoding="utf-8")
    (d / "_system.yaml").write_text("model:\n  fields: []\n", encoding="utf-8")
    docs = load_contract_folder(str(tmp_path))
    assert [(x["_domain"], x["_source_file"]) for x in docs] == [
        ("payments", "payments/stripe/silver_stripe_payouts.yaml")
    ]


def test_cli_writes_light_and_dark(tmp_path):
    src = tmp_path / "contracts" / "marketplace"
    src.mkdir(parents=True)
    (src / "silver_rideflow_riders.yaml").write_text(
        "system: rideflow\nprimary_key: [rider_id]\nmodel:\n  fields:\n    - name: rider_id\n", encoding="utf-8"
    )
    out = tmp_path / "out"
    r = CliRunner().invoke(app, ["ontology", str(tmp_path / "contracts"), "--out", str(out), "-d", "marketplace"])
    assert r.exit_code == 0, r.output
    assert (out / "business_ontology_light.svg").exists() and (out / "business_ontology_dark.svg").exists()
    bad = CliRunner().invoke(app, ["ontology", str(tmp_path / "missing")])
    assert bad.exit_code == 1


def test_gold_only_concepts_are_drawn_dashed_and_lines_avoid_circles():
    estate = copy.deepcopy(ESTATE) + [
        _c("gold_fact_connections_at_risk", ["trip_id", "rider_id"], ["trip_id", "rider_id"], "rideflow", "shared"),
    ]
    estate[-1]["name"] = "gold_fact_connections_at_risk"
    m = derive_business_map(estate)
    gold_only = [e for e in m["entities"] if e.get("contracts") and not e.get("appears_in")]
    svg = render_ontology_svg(m)
    if gold_only:
        assert "Gold only" in svg and 'stroke-dasharray="5 4"' in svg
    from lakelogic.core.ontology_render import _measured, line_crossings, stated_links

    ents = _measured(m["entities"])
    links = stated_links(m["relationships"], {e["name"] for e in ents})
    placed, _ = layout(ents, links)
    assert line_crossings(placed, links) == []


def test_shared_keys_are_drawn_dashed_and_the_calendar_is_not_drawn() -> None:
    from lakelogic.core.ontology_render import _measured

    ents = [
        {"name": "Coupon", "contracts": [{"name": "silver_coupons"}], "appears_in": ["silver_coupons"]},
        {"name": "Flight", "contracts": [], "appears_in": ["silver_coupons"], "shared_key": True},
        {"name": "Date", "contracts": [{"name": "gold_dim_date"}], "appears_in": [], "calendar": True},
    ]
    rels = [{"from": "Date", "to": "Coupon", "kind": "join"}, {"from": "Flight", "to": "Coupon", "kind": "shared_key"}]
    assert [e["name"] for e in _measured(ents, rels)] == ["Coupon", "Flight"]
