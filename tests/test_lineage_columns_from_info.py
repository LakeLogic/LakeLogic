"""`_lakelogic_domain` / `_system` / `_contract_name` were null on every registry-run OLC
contract: domain/system were read only from `metadata`, the name only from a file path
(2026-09-26, seen on Databricks)."""

import polars as pl

from lakelogic.core import lineage as ln
from lakelogic.core.models import DataContract


def _apply(contract):
    good, _ = ln.inject_lineage(
        pl.DataFrame({"a": [1]}),
        pl.DataFrame({"a": []}, schema={"a": pl.Int64}),
        contract,
        "polars",
        None,
        source_path="src.csv",
    )
    return good


def test_domain_system_and_name_come_from_info_when_metadata_and_path_are_absent():
    c = DataContract(
        version="1.0.0",
        dataset="bronze_rideflow_driver_profiles",
        info={"title": "Bronze — Driver Profiles", "domain": "marketplace", "system": "rideflow"},
        lineage={"enabled": True},
    )
    row = _apply(c).row(0, named=True)
    assert row["_lakelogic_domain"] == "marketplace"
    assert row["_lakelogic_system"] == "rideflow"
    assert row["_lakelogic_contract_name"].startswith("bronze_rideflow_driver_profiles")


def test_metadata_still_wins_when_set():
    c = DataContract(
        version="1.0.0",
        dataset="d",
        info={"title": "t", "domain": "info_dom"},
        metadata={"domain": "meta_dom"},
        lineage={"enabled": True},
    )
    assert _apply(c).row(0, named=True)["_lakelogic_domain"] == "meta_dom"
