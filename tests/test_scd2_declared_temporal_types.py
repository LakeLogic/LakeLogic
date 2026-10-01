"""An SCD2 dimension written by the polars/pandas path keeps the types its contract declares.

`_scd2_frames` builds `effective_from` / `effective_to` as text ("1900-01-01", "9999-12-31",
an ISO "now" with a +00:00 offset), and the partitioned write turned the frame straight into
Arrow, so the dimension landed with STRING dates where the contract — and the Spark path —
have timestamps. Every fact joining it on `dropoff_at >= effective_from` then failed on every
row and quarantined the lot (RideFlow gold facts, 2026-10-01).
"""

import polars as pl
import pyarrow as pa
import pytest
from types import SimpleNamespace

from lakelogic import DataProcessor
from lakelogic.core.materialization import _coerce_declared_temporal

deltalake = pytest.importorskip("deltalake")


def _contract(target, partition_by):
    return {
        "version": "1.0",
        "info": {"title": "dim driver", "target_layer": "gold"},
        "primary_key": ["driver_id"],
        "model": {
            "fields": [
                {"name": "driver_id", "type": "string", "required": True},
                {"name": "city_code", "type": "string"},
                {"name": "rating", "type": "double"},
                {"name": "updated_at", "type": "timestamp"},
                {"name": "driver_sk", "type": "string"},
                {"name": "effective_from", "type": "timestamp"},
                {"name": "effective_to", "type": "timestamp"},
                {"name": "is_current", "type": "boolean"},
                {"name": "version_number", "type": "integer"},
            ]
        },
        "materialization": {
            "strategy": "scd2",
            "format": "delta",
            "target_path": str(target),
            **({"partition_by": partition_by} if partition_by else {}),
            "scd2": {
                "surrogate_key": "driver_sk",
                "surrogate_key_strategy": "hash",
                "timestamp_field": "updated_at",
                "effective_from_field": "effective_from",
                "effective_to_field": "effective_to",
                "current_flag_field": "is_current",
                "version_column": "version_number",
                "track_columns": ["city_code", "rating"],
            },
        },
    }


def _rows():
    from datetime import datetime

    return pl.DataFrame({
        "driver_id": ["d1", "d2", "d3"],
        "city_code": ["LON", "LON", "MAN"],
        "rating": [4.8, 4.5, 4.9],
        "updated_at": [datetime(2026, 9, 1), datetime(2026, 9, 2), datetime(2026, 9, 3)],
    })


@pytest.mark.parametrize("partition_by", [["city_code"], None])
def test_scd2_effective_dates_are_written_as_declared(tmp_path, partition_by):
    target = tmp_path / "dim_driver"
    proc = DataProcessor(_contract(target, partition_by), engine="polars")
    proc.run(_rows(), materialize=True, materialize_target=str(target))

    schema = pa.schema(deltalake.DeltaTable(str(target)).schema().to_arrow())
    for name in ("effective_from", "effective_to"):
        assert pa.types.is_timestamp(schema.field(name).type), (name, schema.field(name).type)

    df = pl.from_arrow(deltalake.DeltaTable(str(target)).to_pyarrow_table())
    current = df.filter(pl.col("driver_id") == "d1")
    assert current["effective_from"][0].year == 1900
    assert current["effective_to"][0].year == 9999


def test_a_fact_can_join_the_dimension_on_its_effective_dates(tmp_path):
    """The failure the string dates caused: a range join on the dimension's dates."""
    import duckdb

    target = tmp_path / "dim_driver"
    DataProcessor(_contract(target, ["city_code"]), engine="polars").run(
        _rows(), materialize=True, materialize_target=str(target))
    dim = deltalake.DeltaTable(str(target)).to_pyarrow_table()  # noqa: F841 - read by duckdb
    hit = duckdb.sql(
        "SELECT count(*) FROM dim WHERE TIMESTAMP '2026-09-15 10:00:00' >= effective_from "
        "AND TIMESTAMP '2026-09-15 10:00:00' < effective_to").fetchone()[0]
    assert hit == 3


def _contract_obj(fields):
    return SimpleNamespace(model=SimpleNamespace(fields=[SimpleNamespace(name=n, type=t) for n, t in fields]))


def test_iso_strings_with_an_offset_parse():
    table = pa.table({"effective_from": ["1900-01-01", "2026-10-01T16:37:31+00:00"]})
    out = _coerce_declared_temporal(table, _contract_obj([("effective_from", "timestamp")]))
    assert pa.types.is_timestamp(out.schema.field("effective_from").type)


def test_unparseable_values_are_left_as_text_not_nulled():
    table = pa.table({"effective_from": ["1900-01-01", "not a date"]})
    out = _coerce_declared_temporal(table, _contract_obj([("effective_from", "timestamp")]))
    assert out.column("effective_from").to_pylist() == ["1900-01-01", "not a date"]


def test_a_table_already_holding_text_keeps_text():
    """An existing table an older version wrote as strings must keep accepting writes."""
    table = pa.table({"effective_from": ["1900-01-01"]})
    existing = pa.schema([pa.field("effective_from", pa.string())])
    out = _coerce_declared_temporal(table, _contract_obj([("effective_from", "timestamp")]), existing)
    assert pa.types.is_string(out.schema.field("effective_from").type)


def test_declared_date_becomes_date():
    table = pa.table({"kpi_date": ["2026-10-01"]})
    out = _coerce_declared_temporal(table, _contract_obj([("kpi_date", "date")]))
    assert pa.types.is_date32(out.schema.field("kpi_date").type)
