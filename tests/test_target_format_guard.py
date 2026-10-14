"""A run stops before writing over data already stored in another table format.

A layer's format comes from ``_system.yaml``. Changed on a table that already holds data (Delta to
Iceberg, say), the next run used to replace the table on Spark or write the new format into the
old table's folder on Polars/DuckDB.
"""

import polars as pl
import pytest

from lakelogic.core.materialization import (
    TargetFormatMismatch,
    _existing_table_format,
    _guard_target_format,
)


def _delta(path, rows):
    from deltalake import write_deltalake

    write_deltalake(str(path), pl.DataFrame(rows, schema={"id": pl.Int64}).to_arrow())
    return path


def test_a_delta_table_with_rows_is_recognised(tmp_path):
    assert _existing_table_format(str(_delta(tmp_path / "t", [{"id": 1}]))) == ("delta", True)


def test_nothing_there_is_not_a_table(tmp_path):
    assert _existing_table_format(str(tmp_path / "missing")) is None
    (tmp_path / "plain").mkdir()
    assert _existing_table_format(str(tmp_path / "plain")) is None


def test_switching_a_delta_table_with_data_to_iceberg_stops_the_run(tmp_path):
    target = _delta(tmp_path / "silver_trips", [{"id": 1}])
    with pytest.raises(TargetFormatMismatch, match=r"already holds data as a Delta table.*now writes iceberg"):
        _guard_target_format(str(target), "iceberg")


def test_switching_to_plain_parquet_over_a_delta_table_also_stops(tmp_path):
    target = _delta(tmp_path / "t", [{"id": 1}])
    with pytest.raises(TargetFormatMismatch):
        _guard_target_format(str(target), "parquet")


def test_the_same_format_writes_as_before(tmp_path):
    _guard_target_format(str(_delta(tmp_path / "t", [{"id": 1}])), "delta")


def test_an_empty_table_in_the_old_format_continues_with_a_warning(tmp_path):
    from deltalake import write_deltalake

    write_deltalake(str(tmp_path / "t"), pl.DataFrame(schema={"id": pl.Int64}).to_arrow())
    assert _existing_table_format(str(tmp_path / "t")) == ("delta", False)
    _guard_target_format(str(tmp_path / "t"), "iceberg")  # no error


def test_a_catalog_table_is_read_from_its_provider():
    import types

    class Spark:
        catalog = types.SimpleNamespace(tableExists=lambda name: True)

        def sql(self, stmt):
            return types.SimpleNamespace(collect=lambda: [("id", "bigint"), ("Provider", "delta")])

        def table(self, name):
            return types.SimpleNamespace(limit=lambda n: types.SimpleNamespace(collect=lambda: [1]))

    assert _existing_table_format("table:cat.silver.trips", Spark()) == ("delta", True)
    with pytest.raises(TargetFormatMismatch, match="cat.silver.trips already holds data"):
        _guard_target_format("table:cat.silver.trips", "iceberg", Spark())
