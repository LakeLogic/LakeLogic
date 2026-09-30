"""`_lakelogic_*` columns keep their types when `cast_to_string` makes bronze all-text.

Polars and DuckDB cast EVERY column to string under `cast_to_string` (Spark did when the
contract had no model), so `_lakelogic_processed_at` became text - and it is the watermark,
the run-log timestamp and the SLO freshness column. Owner rule (2026-09-30): LakeLogic's
own columns are always typed, even where every source field is text.
"""
from datetime import datetime, timezone

import polars as pl
import pytest

from lakelogic.core.models import DataContract
from lakelogic.engines.duckdb import DuckDBAdapter
from lakelogic.engines.polars import PolarsAdapter

TS = datetime(2026, 9, 30, 10, 0, tzinfo=timezone.utc)


def _contract():
    return DataContract(
        version="1.0.0",
        server={"type": "local", "path": "memory", "cast_to_string": True},
        model={"fields": [{"name": "trip_id", "type": "string"}, {"name": "fare", "type": "float"}]},
    )


def _frame():
    return pl.DataFrame({"trip_id": ["t1"], "fare": [12.5], "_lakelogic_processed_at": [TS]})


def test_polars_keeps_lakelogic_columns_typed():
    out, _ = PolarsAdapter(_contract())._apply_schema(_frame().lazy())
    schema = out.collect_schema()
    assert schema["fare"] == pl.Utf8
    assert "_lakelogic_processed_at" not in schema or schema["_lakelogic_processed_at"] != pl.Utf8


@pytest.mark.parametrize("adapter", [DuckDBAdapter, PolarsAdapter])
def test_source_fields_become_text_but_the_timestamp_does_not(adapter):
    good, _ = adapter(_contract()).execute(_frame())
    assert good["fare"].dtype == pl.String
    if "_lakelogic_processed_at" in good.columns:
        assert good["_lakelogic_processed_at"].dtype != pl.String


def test_the_rule_is_one_helper():
    a = PolarsAdapter(_contract())
    assert a._keeps_its_type("_lakelogic_processed_at")
    assert not a._keeps_its_type("fare")
