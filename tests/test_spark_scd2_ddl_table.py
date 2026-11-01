"""SCD2's first load into a table DDL created: the write matches the table.

Measured on Fabric (2026-11-01): Salesforce `gold_dim_accounts` failed with DELTA_METADATA_MISMATCH.
DDL had created the table from the model, but the first load overwrote it with a different shape:
the materializer's `_change_reason` column the table lacked, and silver columns the gold model
does not declare (`Name`, `attributes`, `CreatedDate`). Open-source Delta refuses the second too.

Skipped when Spark, Delta or Java are unavailable.
"""

from __future__ import annotations

import pytest

pyspark = pytest.importorskip("pyspark")
delta = pytest.importorskip("delta")

from lakelogic.core.ddl import generate_ddl  # noqa: E402
from lakelogic.core.materialization import _materialize_spark_dataframe  # noqa: E402
from lakelogic.core.models import DataContract  # noqa: E402


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    # Reuse a Delta-enabled session another module already started (one SparkContext per JVM).
    _active = SparkSession.getActiveSession()
    if _active is not None and "DeltaSparkSessionExtension" in (_active.conf.get("spark.sql.extensions", "") or ""):
        yield _active
        return

    builder = (
        SparkSession.builder.appName("lakelogic-scd2-ddl-tests")
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


def _contract():
    return DataContract(
        **{
            "version": "1.0",
            "info": {"title": "dim_accounts"},
            "primary_key": ["Id"],
            "model": {
                "fields": [
                    {"name": "accounts_sk", "type": "string"},
                    {"name": "Id", "type": "string"},
                    {"name": "Industry", "type": "string"},
                    {"name": "effective_from", "type": "timestamp"},
                    {"name": "effective_to", "type": "timestamp"},
                    {"name": "is_current", "type": "boolean"},
                    {"name": "version_number", "type": "long"},
                ]
            },
            "materialization": {
                "strategy": "scd2",
                "format": "delta",
                "scd2": {
                    "surrogate_key": "accounts_sk",
                    "effective_from_field": "effective_from",
                    "effective_to_field": "effective_to",
                    "current_flag_field": "is_current",
                    "version_column": "version_number",
                    "track_columns": ["Industry"],
                },
            },
        }
    )


def test_the_first_load_matches_the_table_ddl_created(spark):
    spark.sql("CREATE SCHEMA IF NOT EXISTS ll_scd2")
    contract = _contract()
    spark.sql(generate_ddl(contract, "spark", table_name="ll_scd2.dim_accounts"))
    table_columns = [f.name for f in spark.table("ll_scd2.dim_accounts").schema]
    assert "_change_reason" in table_columns

    # Silver brings columns the gold model does not declare.
    df = spark.createDataFrame([("A1", "Transport", "Acme", "{}")], ["Id", "Industry", "Name", "attributes"])
    _materialize_spark_dataframe(df, contract, "table:ll_scd2.dim_accounts", "delta")

    written = spark.table("ll_scd2.dim_accounts")
    assert [f.name for f in written.schema] == table_columns
    # Not the timestamps: the first load stamps 1900-01-01, which Python on Windows cannot convert.
    row = written.select("Id", "Industry", "_change_reason").collect()[0]
    assert row["Id"] == "A1" and row["Industry"] == "Transport" and row["_change_reason"] == "initial"

    # A rerun takes the merge path, which rewrites the table: the same columns must not break it.
    changed = spark.createDataFrame([("A1", "Logistics", "Acme", "{}")], ["Id", "Industry", "Name", "attributes"])
    _materialize_spark_dataframe(changed, contract, "table:ll_scd2.dim_accounts", "delta")
    rerun = spark.table("ll_scd2.dim_accounts")
    assert [f.name for f in rerun.schema] == table_columns
    assert sorted(r["Industry"] for r in rerun.select("Industry").collect()) == ["Logistics", "Transport"]
