"""The DAG's EXTERNAL column comes from bronze contracts' own `source:` blocks.

It came only from `external_sources` hand-written in _system.yaml, so the RideFlow DAG showed
one "RideFlow Platform API" box while seven bronze contracts each declared a landing folder
(2026-10-01). One node per source LOCATION; bronze contracts sharing it share the node.
"""
from lakelogic.pipeline.runner import LakehousePipeline

loc = LakehousePipeline._dag_source_location
ROOT = "/Volumes/cat/nondelta/landing_marketplace/rideflow"


def test_a_landing_folder_is_shown_by_its_container():
    src = {"type": "landing", "path": "{landing_root}/driver_profiles", "format": "csv"}
    assert loc(src, landing_root=ROOT) == ("landing_marketplace/rideflow", "landing · csv")


def test_globs_and_file_names_are_dropped():
    root = "/Volumes/cat/nondelta/landing_operations/checkr"
    src = {"type": "landing", "path": "{landing_root}/driver_licences/**/*.pdf"}
    assert loc(src, landing_root=root) == ("landing_operations/checkr", "landing")


def test_two_bronzes_on_one_landing_zone_share_a_node():
    a = loc({"type": "landing", "path": "{landing_root}/driver_profiles"}, landing_root=ROOT)
    b = loc({"type": "landing", "path": "{landing_root}/trip_completed"}, landing_root=ROOT)
    assert a == b


def test_a_table_source_is_shown_by_its_table():
    assert loc({"type": "table", "path": "table:`cat`.crm.customers"}) == ("cat.crm.customers", "table")


def test_no_declared_source_is_no_node():
    assert loc(None) is None and loc({"type": "landing"}) is None


def test_one_landing_zone_is_one_node_whatever_its_formats():
    """CSV and JSON feeds in one landing zone rendered as two identical nodes (2026-10-01)."""
    from lakelogic.core.registry import DomainRegistry
    import re
    from pathlib import Path

    sysf = Path(r"C:\_Personal\_SaaS\lakelogic-databricks-data-mesh-lakehouse\domains_rideflow\marketplace\rideflow\_system.yaml")
    if not sysf.exists():
        import pytest
        pytest.skip("demo repo not present")
    html = LakehousePipeline(DomainRegistry.from_yaml(path=str(sysf), environment="dev"), engine="polars").visualize_dag()
    assert html.count(">landing_marketplace/rideflow<") == 1
