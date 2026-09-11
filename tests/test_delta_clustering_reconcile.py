"""`cluster_by` reaches a Delta table however the table came to exist.

It used to arrive only through the CREATE TABLE that DDL mode emits. A table created by its first
pipeline write got `partitionBy` and no clustering (there was no `clusterBy` on the write path),
and a table that already existed was skipped by `CREATE TABLE IF NOT EXISTS` — so declaring
`cluster_by` on a live Databricks table changed nothing, and said nothing.

These run against REAL Delta tables with delta-spark, because the fix is SQL
(`DESCRIBE DETAIL`, `ALTER TABLE ... CLUSTER BY`) that no test had ever executed. Skipped when
Spark, Delta or Java are unavailable.
"""

from __future__ import annotations

import shutil

import pytest

pyspark = pytest.importorskip("pyspark")
delta = pytest.importorskip("delta")

from lakelogic.core import clustering  # noqa: E402


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("warehouse").as_posix()
    builder = (
        SparkSession.builder.appName("lakelogic-clustering-tests")
        .master("local[1]")
        .config("spark.sql.warehouse.dir", warehouse)
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
def table(spark, tmp_path):
    """A plain Delta table, created the way a first pipeline write creates one."""

    def make(partition_by=None):
        path = (tmp_path / "trips").as_posix()
        df = spark.createDataFrame([("t1", "GB", 10.0), ("t2", "JP", 12.5)], ["trip_id", "country_code", "fare"])
        writer = df.write.format("delta").mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.save(path)
        return f"delta.`{path}`"

    yield make
    shutil.rmtree(tmp_path, ignore_errors=True)


def _clustering(spark, ref):
    return list(spark.sql(f"DESCRIBE DETAIL {ref}").collect()[0].asDict().get("clusteringColumns") or [])


def _version(spark, ref):
    return spark.sql(f"DESCRIBE HISTORY {ref}").agg({"version": "max"}).collect()[0][0]


def test_a_table_created_by_a_write_gets_its_clustering(spark, table):
    """THE DEFECT: the first write created the table unclustered, and nothing added it."""
    ref = table()
    assert _clustering(spark, ref) == [], "precondition: a plain write leaves the table unclustered"

    assert clustering.reconcile_delta_clustering(spark, ref, ["country_code"]) == clustering.APPLIED
    assert _clustering(spark, ref) == ["country_code"]


def test_reconciling_again_changes_nothing(spark, table):
    """Runs after every write, so it must be a no-op once the table matches — no new commit."""
    ref = table()
    clustering.reconcile_delta_clustering(spark, ref, ["country_code"])
    before = _version(spark, ref)
    assert clustering.reconcile_delta_clustering(spark, ref, ["country_code"]) == clustering.UNCHANGED
    assert _version(spark, ref) == before, "an unchanged table was written to anyway"


def test_a_changed_cluster_by_is_applied(spark, table):
    ref = table()
    clustering.reconcile_delta_clustering(spark, ref, ["country_code"])
    assert clustering.reconcile_delta_clustering(spark, ref, ["country_code", "trip_id"]) == clustering.APPLIED
    assert _clustering(spark, ref) == ["country_code", "trip_id"]


def test_a_partitioned_table_is_reported_not_altered(spark, table):
    """Delta refuses liquid clustering on a partitioned table; say so rather than error."""
    ref = table(partition_by=["country_code"])
    assert clustering.reconcile_delta_clustering(spark, ref, ["trip_id"]) == clustering.PARTITIONED
    assert _clustering(spark, ref) == []


def test_columns_the_table_lacks_are_pruned(spark, table):
    """A layer-wide default names a superset; the table keeps the columns it has."""
    ref = table()
    assert clustering.reconcile_delta_clustering(spark, ref, ["country_code", "event_date"]) == clustering.APPLIED
    assert _clustering(spark, ref) == ["country_code"]


def test_no_present_column_means_nothing_to_do(spark, table):
    ref = table()
    assert clustering.reconcile_delta_clustering(spark, ref, ["event_date"]) == clustering.NO_COLUMNS
    assert _clustering(spark, ref) == []


def test_nothing_declared_touches_nothing(spark, table):
    ref = table()
    before = _version(spark, ref)
    assert clustering.reconcile_delta_clustering(spark, ref, []) == clustering.NOT_DECLARED
    assert _version(spark, ref) == before


def test_a_failure_is_reported_never_raised(spark, tmp_path):
    """An optimisation must not fail the write that came before it."""
    missing = f"delta.`{(tmp_path / 'does_not_exist').as_posix()}`"
    assert clustering.reconcile_delta_clustering(spark, missing, ["country_code"]) == clustering.UNSUPPORTED


# ── Wired into the write path ────────────────────────────────────────────────


def test_the_post_write_hook_clusters_a_table_its_first_write_created(spark):
    """End to end through `_spark_apply_table_metadata` — the function every Spark write to a
    catalog table calls afterwards (merge, SCD2, append / overwrite). A plain `saveAsTable` is
    exactly how a first pipeline write creates the table: unclustered."""
    from lakelogic.core.materialization import _spark_apply_table_metadata
    from lakelogic.core.models import DataContract

    spark.sql("CREATE DATABASE IF NOT EXISTS ll_clustering")
    spark.sql("DROP TABLE IF EXISTS ll_clustering.trips")
    df = spark.createDataFrame([("t1", "GB"), ("t2", "JP")], ["trip_id", "country_code"])
    df.write.format("delta").mode("overwrite").saveAsTable("ll_clustering.trips")
    assert _clustering(spark, "ll_clustering.trips") == [], "precondition: the write left it unclustered"

    contract = DataContract(
        **{
            "version": "1.0",
            "info": {"title": "trips"},
            "model": {"fields": [{"name": "trip_id", "type": "string"}, {"name": "country_code", "type": "string"}]},
            "materialization": {"cluster_by": ["country_code"]},
        }
    )
    _spark_apply_table_metadata(spark, "ll_clustering.trips", contract)

    assert _clustering(spark, "ll_clustering.trips") == ["country_code"]
    spark.sql("DROP TABLE IF EXISTS ll_clustering.trips")


def test_the_hook_leaves_a_contract_without_cluster_by_alone(spark):
    from lakelogic.core.materialization import _spark_apply_table_metadata
    from lakelogic.core.models import DataContract

    spark.sql("CREATE DATABASE IF NOT EXISTS ll_clustering")
    spark.sql("DROP TABLE IF EXISTS ll_clustering.plain")
    spark.createDataFrame([("t1",)], ["trip_id"]).write.format("delta").saveAsTable("ll_clustering.plain")
    before = _version(spark, "ll_clustering.plain")
    contract = DataContract(
        **{"version": "1.0", "info": {"title": "plain"}, "model": {"fields": [{"name": "trip_id", "type": "string"}]}}
    )
    _spark_apply_table_metadata(spark, "ll_clustering.plain", contract)
    assert _version(spark, "ll_clustering.plain") == before
    spark.sql("DROP TABLE IF EXISTS ll_clustering.plain")


# ── Wired into DDL mode ──────────────────────────────────────────────────────


def _ddl_contract(table):
    from lakelogic.core.models import DataContract

    return DataContract(
        **{
            "version": "1.0",
            "info": {"title": "trips"},
            "model": {"fields": [{"name": "trip_id", "type": "string"}, {"name": "country_code", "type": "string"}]},
            "materialization": {"format": "delta", "target_path": f"table:{table}", "cluster_by": ["country_code"]},
        }
    )


def test_ddl_mode_clusters_a_table_that_already_existed(spark):
    """THE DEFECT: `CREATE TABLE IF NOT EXISTS ... CLUSTER BY` skips an existing table, and DDL
    mode runs no write afterwards, so the post-write hook never saw it either."""
    from lakelogic.core.ddl import create_table

    spark.sql("CREATE DATABASE IF NOT EXISTS ll_clustering")
    spark.sql("DROP TABLE IF EXISTS ll_clustering.ddl_trips")
    spark.createDataFrame([("t1", "GB")], ["trip_id", "country_code"]).write.format("delta").saveAsTable(
        "ll_clustering.ddl_trips"
    )
    assert _clustering(spark, "ll_clustering.ddl_trips") == [], "precondition: the table exists unclustered"

    create_table(_ddl_contract("ll_clustering.ddl_trips"), "spark", table_name="ll_clustering.ddl_trips")

    assert _clustering(spark, "ll_clustering.ddl_trips") == ["country_code"]
    spark.sql("DROP TABLE IF EXISTS ll_clustering.ddl_trips")


def test_ddl_mode_on_a_new_table_does_not_alter_it_again(spark):
    """A table the CREATE just made already carries its clustering — reconcile is a no-op."""
    from lakelogic.core.ddl import create_table

    spark.sql("CREATE DATABASE IF NOT EXISTS ll_clustering")
    spark.sql("DROP TABLE IF EXISTS ll_clustering.ddl_new")
    create_table(_ddl_contract("ll_clustering.ddl_new"), "spark", table_name="ll_clustering.ddl_new")

    assert _clustering(spark, "ll_clustering.ddl_new") == ["country_code"]
    assert _version(spark, "ll_clustering.ddl_new") == 0, "a freshly created table was altered anyway"
    spark.sql("DROP TABLE IF EXISTS ll_clustering.ddl_new")


# ── Wired into Spark writes to a storage PATH ────────────────────────────────


def _path_contract(strategy, fmt="delta"):
    from lakelogic.core.models import DataContract

    return DataContract(
        **{
            "version": "1.0",
            "info": {"title": "trips"},
            "primary_key": ["trip_id"],
            "model": {"fields": [{"name": "trip_id", "type": "string"}, {"name": "country_code", "type": "string"}]},
            "materialization": {"strategy": strategy, "format": fmt, "cluster_by": ["country_code"]},
        }
    )


@pytest.mark.parametrize("strategy", ["append", "overwrite", "merge"])
def test_a_spark_write_by_path_gets_its_clustering(spark, tmp_path, strategy):
    """THE DEFECT: only catalog (`table:`) targets were reconciled, so a Delta table written to a
    storage path never got the clustering its contract declared."""
    from pathlib import Path

    from lakelogic.core.materialization import _materialize_spark_dataframe

    target = (tmp_path / f"trips_{strategy}").as_posix()
    df = spark.createDataFrame([("t1", "GB"), ("t2", "JP")], ["trip_id", "country_code"])
    _materialize_spark_dataframe(df, _path_contract(strategy), Path(target), "delta")

    assert _clustering(spark, f"delta.`{target}`") == ["country_code"]


def test_a_parquet_write_by_path_is_left_alone(spark, tmp_path, monkeypatch):
    """Liquid clustering is Delta-only; a parquet target must not even be asked."""
    from pathlib import Path

    from lakelogic.core import materialization

    calls = []
    monkeypatch.setattr(clustering, "reconcile_delta_clustering", lambda *a, **k: calls.append(a))
    df = spark.createDataFrame([("t1", "GB")], ["trip_id", "country_code"])
    target = (tmp_path / "trips_parquet").as_posix()
    materialization._materialize_spark_dataframe(df, _path_contract("overwrite", "parquet"), Path(target), "parquet")

    assert calls == []
