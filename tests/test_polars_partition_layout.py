"""`partition_by` added to a table that already exists, on the polars / delta-rs path.

delta-rs refuses a write whose partitioning differs from the table's ("Specified table
partitioning does not match table partitioning") for append AND overwrite — the same defect the
Spark path had (`tests/test_spark_partition_layout.py`), measured against deltalake 1.6.3. So a
contract that gained `partition_by` after its first run failed every later run until someone
deleted the table. These use the real deltalake writer, not a stub.
"""

from __future__ import annotations

import pytest

pytest.importorskip("deltalake")
pl = pytest.importorskip("polars")

from deltalake import DeltaTable  # noqa: E402
from loguru import logger  # noqa: E402

from lakelogic.core.materialization import materialize_dataframe  # noqa: E402
from lakelogic.core.models import DataContract  # noqa: E402


@pytest.fixture()
def warnings_log():
    messages = []
    sink = logger.add(lambda m: messages.append(m.record["message"]), level="WARNING")
    yield messages
    logger.remove(sink)


def _contract(target, strategy, partition_by=None):
    mat = {"strategy": strategy, "format": "delta", "target_path": str(target)}
    if partition_by:
        mat["partition_by"] = list(partition_by)
    return DataContract(
        **{
            "version": "1.0",
            "info": {"title": "trips"},
            "primary_key": ["trip_id"],
            "model": {
                "fields": [
                    {"name": "trip_id", "type": "string"},
                    {"name": "country_code", "type": "string"},
                ]
            },
            "materialization": mat,
        }
    )


def _frame():
    return pl.DataFrame({"trip_id": ["t1", "t2"], "country_code": ["GB", "JP"]})


@pytest.mark.parametrize("strategy", ["append", "overwrite"])
def test_partition_by_declared_on_an_existing_table_does_not_fail_the_write(tmp_path, warnings_log, strategy):
    target = tmp_path / strategy
    materialize_dataframe(_frame(), _contract(target, strategy), engine_name="polars")
    assert DeltaTable(str(target)).metadata().partition_columns == []

    result = materialize_dataframe(_frame(), _contract(target, strategy, ["country_code"]), engine_name="polars")

    assert result["rows_written"] == 2
    assert DeltaTable(str(target)).metadata().partition_columns == [], "cannot change in place"
    assert any("cannot change the partitioning of an existing table" in w for w in warnings_log)


def test_a_new_table_is_created_with_the_declared_partitioning(tmp_path, warnings_log):
    target = tmp_path / "fresh"
    materialize_dataframe(_frame(), _contract(target, "overwrite", ["country_code"]), engine_name="polars")

    assert DeltaTable(str(target)).metadata().partition_columns == ["country_code"]
    assert not any("partitioning of an existing table" in w for w in warnings_log)


def test_a_matching_existing_layout_writes_silently(tmp_path, warnings_log):
    target = tmp_path / "match"
    contract = _contract(target, "append", ["country_code"])
    materialize_dataframe(_frame(), contract, engine_name="polars")
    materialize_dataframe(_frame(), contract, engine_name="polars")

    assert DeltaTable(str(target)).metadata().partition_columns == ["country_code"]
    assert not any("partitioning of an existing table" in w for w in warnings_log)
