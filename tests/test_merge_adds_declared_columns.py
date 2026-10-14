"""A MERGE into a table that lacks a column the contract declares adds it first.

Live on Databricks (2026-10-14): gold tables created before the contract derived `system`
failed every MERGE with "Cannot resolve system in UPDATE clause". The contract declares the
column, so the table is behind the contract: the merge adds it. An UNDECLARED new column
follows the schema policy: added, unless `strict`.
"""

from types import SimpleNamespace

import pytest

from lakelogic.core.materialization import _merge_columns_to_add
from lakelogic.core.models import DataContract


def _contract(evolution=None, extra_fields=()):
    doc = {
        "version": "1.0",
        "info": {"title": "dim_platform"},
        "primary_key": ["platform_id"],
        "model": {
            "fields": [
                {"name": "platform_id", "type": "string"},
                {"name": "platform", "type": "string"},
                {"name": "system", "type": "string"},
                *[{"name": n, "type": "string"} for n in extra_fields],
            ]
        },
        "materialization": {"strategy": "merge", "format": "delta"},
    }
    if evolution:
        doc["schema_policy"] = {"evolution": evolution}
    return DataContract(**doc)


def _fields(*names):
    return [SimpleNamespace(name=n) for n in names]


@pytest.mark.parametrize("evolution", [None, "allow"])
def test_a_declared_column_the_table_lacks_is_added(evolution):
    new = _merge_columns_to_add(
        {"platform_id", "platform"}, _fields("platform_id", "platform", "system"), _contract(evolution)
    )
    assert [f.name for f in new] == ["system"]


def test_an_undeclared_new_column_is_added_unless_strict():
    have = {"platform_id", "platform", "system"}
    incoming = _fields("platform_id", "platform", "system", "surprise")
    assert [f.name for f in _merge_columns_to_add(have, incoming, _contract("allow"))] == ["surprise"]
    with pytest.raises(ValueError, match="policy=strict.*surprise"):
        _merge_columns_to_add(have, incoming, _contract("strict"))


def test_nothing_to_add_when_the_table_has_every_column():
    have = {"platform_id", "platform", "system"}
    assert _merge_columns_to_add(have, _fields(*have), _contract("strict")) == []


# ── the real Delta MERGE ────────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    pytest.importorskip("delta")
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    _active = SparkSession.getActiveSession()
    if _active is not None and "DeltaSparkSessionExtension" in (_active.conf.get("spark.sql.extensions", "") or ""):
        yield _active
        return
    builder = (
        SparkSession.builder.appName("lakelogic-merge-columns-tests")
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


def test_delta_merge_adds_the_declared_column_and_keeps_the_rows(spark, tmp_path):
    from lakelogic.core.materialization import _spark_merge_dataframe

    path = (tmp_path / "dim_platform").as_posix()
    spark.createDataFrame([("p1", "ios"), ("p2", "android")], "platform_id string, platform string").write.format(
        "delta"
    ).save(path)

    incoming = spark.createDataFrame(
        [("p2", "android", "rideflow"), ("p3", "web", "rideflow")], "platform_id string, platform string, system string"
    )
    _spark_merge_dataframe(spark, incoming, path, ["platform_id"], "delta", contract=_contract())

    rows = {r.platform_id: r.system for r in spark.read.format("delta").load(path).collect()}
    assert rows == {"p1": None, "p2": "rideflow", "p3": "rideflow"}


def test_delta_merge_under_strict_refuses_an_undeclared_column_and_leaves_the_table(spark, tmp_path):
    from lakelogic.core.materialization import MergeFailed, _spark_merge_dataframe

    path = (tmp_path / "dim_platform_strict").as_posix()
    spark.createDataFrame(
        [("p1", "ios", "rideflow")], "platform_id string, platform string, system string"
    ).write.format("delta").save(path)

    incoming = spark.createDataFrame(
        [("p1", "ios", "rideflow", "x")], "platform_id string, platform string, system string, surprise string"
    )
    with pytest.raises(MergeFailed, match="policy=strict"):
        _spark_merge_dataframe(spark, incoming, path, ["platform_id"], "delta", contract=_contract("strict"))
    assert spark.read.format("delta").load(path).columns == ["platform_id", "platform", "system"]


# ── the table follows the contract: plan ────────────────────────────────────────────────────

from lakelogic.core.schema_alignment import is_widening, plan_alignment  # noqa: E402


def test_plan_adds_widens_keeps_and_flags_rewrites():
    plan = plan_alignment(
        {
            "id": "string",
            "qty": "int",
            "price": "float",
            "code": "int",
            "old_col": "string",
            "_lakelogic_run": "string",
        },
        {"id": "string", "qty": "bigint", "price": "double", "code": "string", "system": "string"},
    )
    assert plan.add == [("system", "string")]
    assert plan.widen == [("qty", "int", "bigint"), ("price", "float", "double")]
    assert plan.rewrite == [("code", "int", "string")]
    assert plan.kept == ["old_col"]


def test_widening_rules():
    assert is_widening("int", "bigint") and is_widening("decimal(10,2)", "decimal(18,2)")
    assert not is_widening("bigint", "int")
    assert not is_widening("string", "int")
    assert not is_widening("decimal(10,4)", "decimal(10,2)")  # fewer digits after the point
    assert not is_widening("decimal(10,2)", "decimal(10,4)")  # fewer before it


def test_matching_is_case_insensitive_as_spark_resolves_columns():
    assert plan_alignment({"Amount": "int"}, {"amount": "int"}).changes_table is False


# ── the table follows the contract: real Spark Delta ────────────────────────────────────────


def _typed_contract(fields, strategy="append"):
    return DataContract(
        **{
            "version": "1.0",
            "info": {"title": "orders"},
            "primary_key": ["order_id"],
            "model": {"fields": [{"name": n, "type": t} for n, t in fields]},
            "materialization": {"strategy": strategy, "format": "delta"},
        }
    )


def _types(spark, path):
    return {f.name: f.dataType.simpleString() for f in spark.read.format("delta").load(path).schema}


def test_append_adds_a_new_declared_column_and_widens_a_type_in_place(spark, tmp_path):
    from pathlib import Path

    from lakelogic.core.materialization import _materialize_spark_dataframe

    path = (tmp_path / "orders_widen").as_posix()
    spark.createDataFrame([("o1", 1)], "order_id string, qty int").write.format("delta").save(path)

    contract = _typed_contract([("order_id", "string"), ("qty", "long"), ("channel", "string")])
    df = spark.createDataFrame([("o2", 5, "web")], "order_id string, qty bigint, channel string")
    _materialize_spark_dataframe(df, contract, Path(path), "delta")

    assert _types(spark, path) == {"order_id": "string", "qty": "bigint", "channel": "string"}
    assert spark.read.format("delta").load(path).count() == 2


def test_a_type_change_that_needs_a_rewrite_stops_the_run_and_leaves_the_table(spark, tmp_path):
    from pathlib import Path

    from lakelogic.core.materialization import _materialize_spark_dataframe
    from lakelogic.core.schema_alignment import SchemaChangeNeedsMigration

    path = (tmp_path / "orders_rewrite").as_posix()
    spark.createDataFrame([("o1", 1)], "order_id string, code int").write.format("delta").save(path)

    contract = _typed_contract([("order_id", "string"), ("code", "string")])
    df = spark.createDataFrame([("o2", "A-1")], "order_id string, code string")
    with pytest.raises(SchemaChangeNeedsMigration, match="lakelogic migrate"):
        _materialize_spark_dataframe(df, contract, Path(path), "delta")
    assert _types(spark, path)["code"] == "int" and spark.read.format("delta").load(path).count() == 1


def test_migrate_rewrites_to_the_contract_type_and_keeps_every_row(spark, tmp_path):
    from lakelogic.core.schema_alignment import migrate_spark_table

    path = (tmp_path / "orders_migrate").as_posix()
    spark.createDataFrame([("o1", "10"), ("o2", "20")], "order_id string, code string").write.format("delta").save(path)

    out = migrate_spark_table(spark, f"delta.`{path}`", _typed_contract([("order_id", "string"), ("code", "int")]))
    assert out["changed"] == ["code: string -> int"] and out["previous_version"] == 0
    assert _types(spark, path)["code"] == "int"
    assert sorted(r.code for r in spark.read.format("delta").load(path).collect()) == [10, 20]


def test_migrate_fails_on_a_value_that_does_not_convert_and_leaves_the_table(spark, tmp_path):
    from lakelogic.core.schema_alignment import migrate_spark_table

    path = (tmp_path / "orders_bad").as_posix()
    spark.createDataFrame([("o1", "10"), ("o2", "N/A")], "order_id string, code string").write.format("delta").save(
        path
    )

    with pytest.raises(Exception, match="(?i)cast|N/A"):
        migrate_spark_table(spark, f"delta.`{path}`", _typed_contract([("order_id", "string"), ("code", "int")]))
    assert _types(spark, path)["code"] == "string"
    assert sorted(r.code for r in spark.read.format("delta").load(path).collect()) == ["10", "N/A"]


# ── unknown member on a table with NOT NULL columns (Databricks, 2026-10-14) ────────────────


def test_unknown_member_fills_not_null_columns_instead_of_failing(spark, tmp_path):
    from lakelogic.core.materialization import _spark_merge_dataframe

    name = f"dim_device_category_{tmp_path.name[-6:]}".replace("-", "_")
    spark.sql(f"DROP TABLE IF EXISTS {name}")
    spark.sql(
        f"CREATE TABLE {name} (device_category_sk STRING, device_category STRING NOT NULL, "
        f"system STRING NOT NULL, rank INT NOT NULL, note STRING) USING delta"
    )
    spark.sql(f"INSERT INTO {name} VALUES ('a1', 'mobile', 'ga', 1, NULL)")

    incoming = spark.createDataFrame(
        [("a2", "desktop", "ga", 2, None)],
        "device_category_sk string, device_category string, system string, rank int, note string",
    )
    scd1 = {"surrogate_key": "device_category_sk", "unknown_member": {"enabled": True}}
    _spark_merge_dataframe(spark, incoming, f"table:{name}", ["device_category"], "delta", scd1_cfg=scd1)

    rows = {r.device_category_sk: r for r in spark.table(name).collect()}
    unknown = rows["-1"]
    assert unknown.system == "Unknown" and unknown.rank == -1 and unknown.note is None
    assert unknown.device_category  # a sentinel, not null
    # idempotent: a second merge does not add a second unknown member
    _spark_merge_dataframe(spark, incoming, f"table:{name}", ["device_category"], "delta", scd1_cfg=scd1)
    assert spark.table(name).filter("device_category_sk = '-1'").count() == 1


def test_engine_managed_scd2_columns_are_not_retyped():
    """Conformance OLC-SCD2-001: the contract declares version/effective_* loosely (int, string);
    the engine stores them as bigint/timestamp. That is not a type change to migrate."""
    plan = plan_alignment(
        {"id": "string", "version": "bigint", "effective_from": "timestamp"},
        {"id": "string", "version": "int", "effective_from": "string"},
        managed={"version", "effective_from"},
    )
    assert plan.widen == [] and plan.rewrite == []


def test_strict_refuses_even_a_declared_new_column():
    """Conformance OLC-EVO-001 (OLC: strict means the incoming schema must match exactly)."""
    from lakelogic.core.schema_alignment import AlignmentPlan, _refuse_if_strict

    with pytest.raises(ValueError, match="policy=strict"):
        _refuse_if_strict("t", AlignmentPlan(add=[("tier", "string")]), _contract("strict"))
    _refuse_if_strict("t", AlignmentPlan(add=[("tier", "string")]), _contract())  # default: allowed
