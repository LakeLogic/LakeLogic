"""A dual-write target must work whether it arrives as a dict or an OLC model.

`secondary_targets` used to be raw dicts straight off the YAML, and this package
read them with `sec.get(...)`. OLC 0.8.0 (published 2026-09-04T19:53) began parsing
them into typed `SecondaryTarget` models, so every one of those reads raised::

    AttributeError: 'SecondaryTarget' object has no attribute 'get'

ANY contract declaring a dual-write then failed at materialization — on a
dependency's release, with no change in this package. It surfaced as the
07_dlt_prefect_pipeline example notebook going red: bronze failed, silver and gold
were skipped as "no new data", and the notebook's final cell reported the missing
gold table rather than the actual error.
"""

from __future__ import annotations

import pytest


def _mapper():
    """Imported lazily so the behavioural test below still COLLECTS against code
    that predates the helper — otherwise the regression test cannot be run to
    demonstrate the failure it guards."""
    from lakelogic.core.materialization import _secondary_target_as_mapping

    return _secondary_target_as_mapping


def test_a_plain_dict_still_works():
    sec = {"format": "dlt", "dlt_destination": "duckdb", "table_name": "weather"}
    assert _mapper()(sec) == sec


def test_an_olc_model_is_read_like_a_dict():
    SecondaryTarget = pytest.importorskip("olc.models._nested").SecondaryTarget
    sec = SecondaryTarget(
        format="dlt",
        dlt_destination="duckdb",
        dlt_dataset_name="analytics",
        table_name="weather_summary",
    )
    mapped = _mapper()(sec)

    # The exact reads the materializer performs.
    assert mapped.get("format", "dlt") == "dlt"
    assert mapped.get("table_name") == "weather_summary"
    assert mapped.get("dlt_destination", "duckdb") == "duckdb"
    assert mapped.get("dlt_dataset_name", "lakelogic") == "analytics"


def test_unset_fields_do_not_override_the_readers_defaults():
    """`model_dump()` would emit dlt_destination=None, and `.get(k, default)` returns
    that None instead of the default — a silent behaviour change rather than a crash."""
    SecondaryTarget = pytest.importorskip("olc.models._nested").SecondaryTarget
    mapped = _mapper()(SecondaryTarget(format="dlt"))
    assert mapped.get("dlt_destination", "duckdb") == "duckdb"
    assert mapped.get("fail_on_error", False) is False


def test_the_model_is_not_mutated_by_being_read():
    SecondaryTarget = pytest.importorskip("olc.models._nested").SecondaryTarget
    sec = SecondaryTarget(format="dlt", table_name="weather")
    _mapper()(sec)["table_name"] = "clobbered"
    assert sec.table_name == "weather"


def test_an_object_without_pydantic_still_degrades_to_its_attributes():
    class Legacy:
        def __init__(self):
            self.format = "dlt"
            self.table_name = "weather"

    mapped = _mapper()(Legacy())
    assert mapped.get("format", "parquet") == "dlt"


def test_the_write_loop_reads_an_olc_model_without_raising():
    """The real regression, through the public entry point rather than the helper.

    An unsupported format is used deliberately: it exercises the same `sec.get(...)`
    reads the dlt path uses and then stops, so the test needs no dlt destination. On
    the pre-fix code this raised AttributeError before reaching that branch.
    """
    SecondaryTarget = pytest.importorskip("olc.models._nested").SecondaryTarget
    from lakelogic.core.materialization import write_to_secondary_targets

    pl = pytest.importorskip("polars")
    out = write_to_secondary_targets(
        [SecondaryTarget(format="not_a_real_format", table_name="weather")],
        pl.DataFrame({"id": [1]}),
        "gold_weather_summary",
    )
    assert isinstance(out, list)
