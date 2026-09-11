"""`partition_by` on Spark: applied to every table a pipeline creates, and never fatal on one that exists.

Two defects, both measured against real Delta tables:

* Merge and SCD2 CREATED their target without ``partitionBy`` — only append/overwrite passed it —
  so a silver or gold table built by its first pipeline run was unpartitioned whatever the
  contract said.
* Declaring ``partition_by`` on a table that already existed failed every later write: Delta
  refuses a ``partitionBy`` that differs from the table's (``Partition columns do not match``),
  for append and overwrite alike. The write now keeps the table's layout and says how to change it.

Skipped when Spark, Delta or Java are unavailable.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
delta = pytest.importorskip("delta")

from loguru import logger  # noqa: E402

from lakelogic.core.materialization import _materialize_spark_dataframe  # noqa: E402
from lakelogic.core.models import DataContract  # noqa: E402


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName("lakelogic-partition-layout-tests")
        .master("local[1]")
        .config("spark.sql.warehouse.dir", tmp_path_factory.mktemp("warehouse").as_posix())
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
    )
    try:
        session = configure_spark_with_delta_pip(builder).getOrCreate()
    except Exception as exc:  # no Java, no network for the Delta jars, …
        pytest.skip(f"Spark with Delta is unavailable here: {exc}")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture()
def warnings_log():
    messages = []
    sink = logger.add(lambda m: messages.append(m.record["message"]), level="WARNING")
    yield messages
    logger.remove(sink)


def _contract(strategy, partition_by=("country_code",)):
    mat = {"strategy": strategy, "format": "delta", "partition_by": list(partition_by)}
    if strategy == "scd2":
        mat["scd2"] = {"timestamp_field": "updated_at"}
    return DataContract(
        **{
            "version": "1.0",
            "info": {"title": "drivers"},
            "primary_key": ["driver_id"],
            "model": {
                "fields": [
                    {"name": "driver_id", "type": "string"},
                    {"name": "country_code", "type": "string"},
                    {"name": "updated_at", "type": "timestamp"},
                ]
            },
            "materialization": mat,
        }
    )


def _frame(spark):
    from datetime import datetime

    return spark.createDataFrame(
        [("D1", "GB", datetime(2026, 9, 1)), ("D2", "JP", datetime(2026, 9, 1))],
        ["driver_id", "country_code", "updated_at"],
    )


def _partitions(spark, ref):
    return list(spark.sql(f"DESCRIBE DETAIL {ref}").collect()[0]["partitionColumns"] or [])


@pytest.mark.parametrize("strategy", ["merge", "scd2"])
def test_a_table_created_by_merge_or_scd2_is_partitioned(spark, tmp_path, strategy):
    """THE DEFECT: merge / SCD2 created their target with no partitionBy at all."""
    target = (tmp_path / f"drivers_{strategy}").as_posix()
    _materialize_spark_dataframe(_frame(spark), _contract(strategy), Path(target), "delta")

    assert _partitions(spark, f"delta.`{target}`") == ["country_code"]


def test_a_catalog_table_created_by_merge_is_partitioned(spark):
    spark.sql("CREATE DATABASE IF NOT EXISTS ll_partition")
    spark.sql("DROP TABLE IF EXISTS ll_partition.drivers")
    _materialize_spark_dataframe(_frame(spark), _contract("merge"), "table:ll_partition.drivers", "delta")

    assert _partitions(spark, "ll_partition.drivers") == ["country_code"]
    spark.sql("DROP TABLE IF EXISTS ll_partition.drivers")


@pytest.mark.parametrize("strategy", ["append", "overwrite"])
def test_partition_by_declared_on_an_existing_table_does_not_fail_the_write(spark, tmp_path, warnings_log, strategy):
    """THE DEFECT: Delta refused the write (`Partition columns do not match`) until the table was dropped."""
    target = (tmp_path / f"drivers_{strategy}").as_posix()
    _frame(spark).write.format("delta").save(target)  # created before partition_by was declared

    result = _materialize_spark_dataframe(_frame(spark), _contract(strategy), Path(target), "delta")

    assert result["rows_written"] == 2
    assert _partitions(spark, f"delta.`{target}`") == [], "an existing table's layout cannot change in place"
    assert any("cannot change the partitioning of an existing table" in w for w in warnings_log)


def test_a_matching_existing_layout_writes_silently(spark, tmp_path, warnings_log):
    target = (tmp_path / "drivers_match").as_posix()
    _frame(spark).write.format("delta").partitionBy("country_code").save(target)

    _materialize_spark_dataframe(_frame(spark), _contract("append"), Path(target), "delta")

    assert _partitions(spark, f"delta.`{target}`") == ["country_code"]
    assert not any("partitioning of an existing table" in w for w in warnings_log)


def test_columns_the_data_lacks_are_still_pruned(spark, tmp_path, warnings_log):
    target = (tmp_path / "drivers_pruned").as_posix()
    contract = _contract("merge", partition_by=("country_code", "event_date"))
    _materialize_spark_dataframe(_frame(spark), contract, Path(target), "delta")

    assert _partitions(spark, f"delta.`{target}`") == ["country_code"]
    assert any("pruned" in w and "event_date" in w for w in warnings_log)
