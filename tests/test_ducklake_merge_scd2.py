"""DuckLake merge and SCD2 use the same frame logic as every other target (2026-10-24).

The DuckLake writer did merge AND scd2 as delete-by-key + insert, so an SCD2 dimension on
DuckLake kept no history: a changed driver replaced its old row instead of closing it.
"""

from datetime import datetime

import polars as pl
import pytest

from lakelogic import DataProcessor

duckdb = pytest.importorskip("duckdb")


def _contract(tmp_path, strategy):
    mat = {"strategy": strategy, "format": "ducklake", "target_path": "table:lake.dims.dim_driver"}
    if strategy == "scd2":
        mat["scd2"] = {
            "timestamp_field": "updated_at",
            "effective_from_field": "effective_from",
            "effective_to_field": "effective_to",
            "current_flag_field": "is_current",
            "track_columns": ["city_code"],
        }
    return {
        "version": "1.0",
        "info": {"title": "dim driver", "target_layer": "gold"},
        "metadata": {
            "ducklake_metadata": str(tmp_path / "catalog.ducklake"),
            "ducklake_data_path": str(tmp_path / "data") + "/",
        },
        "primary_key": ["driver_id"],
        "model": {
            "fields": [
                {"name": "driver_id", "type": "string", "required": True},
                {"name": "city_code", "type": "string"},
                {"name": "updated_at", "type": "timestamp"},
            ]
        },
        "materialization": mat,
    }


def _batch(city, day):
    return pl.DataFrame(
        {"driver_id": ["d1", "d2"], "city_code": [city, "MAN"], "updated_at": [datetime(2026, 9, day)] * 2}
    )


def _rows(tmp_path):
    con = duckdb.connect()
    try:
        con.execute("INSTALL ducklake; LOAD ducklake;")
        con.execute(f"ATTACH 'ducklake:{tmp_path / 'catalog.ducklake'}' AS lake (DATA_PATH '{tmp_path / 'data'}/')")
        return con.execute("SELECT * FROM lake.dims.dim_driver").pl()
    finally:
        con.close()


def _run(tmp_path, strategy, batches):
    for b in batches:
        DataProcessor(_contract(tmp_path, strategy), engine="polars").run(b, materialize=True)
    return _rows(tmp_path)


def test_scd2_on_ducklake_keeps_the_old_version(tmp_path):
    rows = _run(tmp_path, "scd2", [_batch("LON", 1), _batch("LDS", 5)])
    d1 = rows.filter(pl.col("driver_id") == "d1").sort("effective_from")
    assert d1["city_code"].to_list() == ["LON", "LDS"]
    assert d1["is_current"].to_list() == [False, True]
    # d2 did not change: still one, current, version
    assert rows.filter(pl.col("driver_id") == "d2").height == 1


def test_merge_on_ducklake_still_replaces_by_key(tmp_path):
    rows = _run(tmp_path, "merge", [_batch("LON", 1), _batch("LDS", 5)])
    assert rows.height == 2
    assert rows.filter(pl.col("driver_id") == "d1")["city_code"].to_list() == ["LDS"]
