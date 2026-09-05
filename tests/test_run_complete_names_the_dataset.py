"""A "Run complete" line must name the dataset it is reporting on.

A system emits one of these per contract, so domain/system/layer alone repeat
verbatim. Observed on a real marketplace/silver run — three consecutive lines,
identical but for their numbers::

    Run complete [domain=marketplace, system=rideflow, layer=silver] | Source: 287 ...
    Run complete [domain=marketplace, system=rideflow, layer=silver] | Source: 1683 ...
    Run complete [domain=marketplace, system=rideflow, layer=silver] | Source: 391 ...

The tag is now a path — `[marketplace/rideflow/silver/trip_cancellations]` — which
carries the same four facts in half the characters, so the line fits one row.

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
    assert any("/trip_cancellations]" in ln for ln in done), done


def test_the_existing_tags_are_kept(capfd):
    """The dataset is added to the tags, not swapped in for them."""
    line = _run_and_capture(_contract(), capfd)[0]
    assert "[marketplace/rideflow/silver/" in line, line


def test_two_contracts_in_one_system_are_tellable_apart(capfd):
    """The point of the change: same domain/system/layer, different lines."""
    a = _run_and_capture(_contract("silver_rideflow_trips"), capfd)[0].split("Run complete")[1].split("|")[0]
    b = (
        _run_and_capture(_contract("silver_rideflow_trip_cancellations"), capfd)[0]
        .split("Run complete")[1]
        .split("|")[0]
    )
    assert a != b, f"tags identical for two different tables: {a!r}"


def test_no_dataset_tag_when_the_name_is_unknown(capfd):
    """An unnamed contract must not print 'dataset=unknown' noise."""
    bare = {"version": "1.0.0", "model": {"fields": [{"name": "id", "type": "int"}]}}
    lines = _run_and_capture(bare, capfd)
    assert lines, "no 'Run complete' line was emitted"
    assert "dataset=unknown" not in lines[0], lines[0]


# ── which name wins ──────────────────────────────────────────────────────────
#
# table_name is what the run WRITES, so it outranks everything. `dataset` is the
# contract's own identifier. `title` is prose and comes last: before this order
# was fixed, a contract carrying both a dataset and a title logged the title, so
# the line named something that matches no table in the catalog.


def _bare(**kw):
    c = {"version": "1.0.0", "model": {"fields": [{"name": "id", "type": "int"}]}}
    c.update(kw)
    return c


def test_table_name_outranks_dataset_and_title(capfd):
    line = _run_and_capture(
        _bare(dataset="orders_ds", info={"title": "Nice Title", "table_name": "silver_trips"}), capfd
    )[0]
    assert "[silver_trips]" in line, line


def test_dataset_outranks_title(capfd):
    """The regression: a prose title must not stand in for a real identifier."""
    line = _run_and_capture(_bare(dataset="orders_ds", info={"title": "Nice Title"}), capfd)[0]
    assert "[orders_ds]" in line, line
    assert "Nice Title" not in line, line


def test_title_is_used_only_as_a_last_resort(capfd):
    """Still better than an unattributable line when nothing else is given."""
    line = _run_and_capture(_bare(info={"title": "Nice Title"}), capfd)[0]
    assert "[Nice Title]" in line, line


# ── one line, one row ────────────────────────────────────────────────────────


def test_the_line_stays_short_enough_to_read_in_one_row(capfd):
    """The Databricks log pane wraps past ~150 characters including loguru's own
    73-character prefix, and a wrapped line is why these were unreadable. Budget the
    MESSAGE at 100 so a realistic scope + counts still fits on one row."""
    line = _run_and_capture(_contract(), capfd)[0]
    message = line.split(" - ", 1)[1] if " - " in line else line
    assert len(message) <= 100, f"{len(message)} chars, too long for one row: {message}"


def test_the_redundant_layer_and_system_prefixes_are_dropped(capfd):
    """`bronze_rideflow_rider_profiles` under `marketplace/rideflow/bronze/` repeats
    both facts; the tag keeps only the part that identifies the table."""
    contract = _contract("bronze_rideflow_rider_profiles")
    contract["info"]["target_layer"] = "bronze"
    line = _run_and_capture(contract, capfd)[0]
    assert "[marketplace/rideflow/bronze/rider_profiles]" in line, line
