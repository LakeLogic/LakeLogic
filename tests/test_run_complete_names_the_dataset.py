"""A "Run complete" line must name the dataset it is reporting on.

A system emits one of these per contract, so domain/system/layer alone repeat
verbatim. Observed on a real marketplace/silver run — three consecutive lines,
identical but for their numbers::

    Run complete [domain=marketplace, system=rideflow, layer=silver] | Source: 287 ...
    Run complete [domain=marketplace, system=rideflow, layer=silver] | Source: 1683 ...
    Run complete [domain=marketplace, system=rideflow, layer=silver] | Source: 391 ...

Only the ERROR that followed named an entity, so the successful lines could not be
attributed to a table at all: you could see that a run dropped 228 rows, but not
which table dropped them.
"""

from __future__ import annotations

import pytest

pl = pytest.importorskip("polars")

from lakelogic.core.processor import DataProcessor


def _contract(table_name="silver_rideflow_trip_cancellations"):
    return {
        "version": "1.0.0",
        "info": {
            "title": "Trip cancellations",
            "table_name": table_name,
            "domain": "marketplace",
            "system": "rideflow",
            "target_layer": "silver",
        },
        "model": {"fields": [{"name": "id", "type": "int"}]},
    }


def _run_and_capture(contract_dict, capfd):
    """Read the emitted log off stderr.

    A loguru sink added here does not survive: the processor reconfigures logging
    mid-run, so the handler is gone before the line is written (and removing it
    afterwards raises). stderr is what a data engineer actually reads anyway.
    """
    capfd.readouterr()  # drop anything buffered from an earlier run
    DataProcessor(contract_dict, engine="polars").run(pl.DataFrame({"id": [1, 2, 3]}))
    err = capfd.readouterr().err
    return [ln for ln in err.splitlines() if "Run complete" in ln]


def test_run_complete_names_the_dataset(capfd):
    done = _run_and_capture(_contract(), capfd)
    assert done, "no 'Run complete' line was emitted"
    assert any("dataset=silver_rideflow_trip_cancellations" in ln for ln in done), done


def test_the_existing_tags_are_kept(capfd):
    """The dataset is added to the tags, not swapped in for them."""
    line = _run_and_capture(_contract(), capfd)[0]
    for tag in ("domain=marketplace", "system=rideflow", "layer=silver"):
        assert tag in line, f"{tag} missing from: {line}"


def test_two_contracts_in_one_system_are_tellable_apart(capfd):
    """The point of the change: same domain/system/layer, different lines."""
    a = _run_and_capture(_contract("silver_rideflow_trips"), capfd)[0].split("Run complete")[1].split("|")[0]
    b = _run_and_capture(_contract("silver_rideflow_trip_cancellations"), capfd)[0].split("Run complete")[1].split("|")[0]
    assert a != b, f"tags identical for two different tables: {a!r}"


def test_no_dataset_tag_when_the_name_is_unknown(capfd):
    """An unnamed contract must not print 'dataset=unknown' noise."""
    bare = {"version": "1.0.0", "model": {"fields": [{"name": "id", "type": "int"}]}}
    lines = _run_and_capture(bare, capfd)
    assert lines, "no 'Run complete' line was emitted"
    assert "dataset=unknown" not in lines[0], lines[0]
