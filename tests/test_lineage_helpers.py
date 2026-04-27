from __future__ import annotations

import sys
import types

import pytest

pl = pytest.importorskip("polars")
pd = pytest.importorskip("pandas")

from lakelogic.core import lineage


class FakeSparkFrame:
    def __init__(self, columns, values=None):
        self.columns = list(columns)
        self.values = dict(values or {})

    def withColumnRenamed(self, src, dst):
        cols = [dst if col == src else col for col in self.columns]
        values = {dst if key == src else key: val for key, val in self.values.items()}
        return FakeSparkFrame(cols, values)

    def withColumn(self, name, value):
        cols = [col for col in self.columns if col != name] + [name]
        values = dict(self.values)
        values[name] = value
        return FakeSparkFrame(cols, values)

    def drop(self, *names):
        names = set(names)
        cols = [col for col in self.columns if col not in names]
        values = {key: val for key, val in self.values.items() if key not in names}
        return FakeSparkFrame(cols, values)

    def select(self, ordered):
        return FakeSparkFrame(list(ordered), self.values)


def test_preserve_upstream_lineage_for_polars_pandas_and_spark(monkeypatch):
    polars_df = pl.DataFrame({"_lakelogic_source": ["src"], "value": [1]})
    preserved_polars = lineage._preserve_upstream_lineage(polars_df, ["_lakelogic_source"], "_upstream", "polars")
    assert "_upstream_lakelogic_source" in preserved_polars.columns

    pandas_df = pd.DataFrame({"_lakelogic_run_id": ["run-1"], "value": [1]})
    preserved_pandas = lineage._preserve_upstream_lineage(pandas_df, ["_lakelogic_run_id"], "_upstream", "pandas")
    assert "_upstream_lakelogic_run_id" in preserved_pandas.columns

    spark_df = FakeSparkFrame(["_lakelogic_source", "value"])
    preserved_spark = lineage._preserve_upstream_lineage(spark_df, ["_lakelogic_source"], "_upstream", "spark")
    assert "_upstream_lakelogic_source" in preserved_spark.columns
    assert lineage._preserve_upstream_lineage(None, ["_lakelogic_source"], "_upstream", "spark") is None


def test_add_columns_for_polars_pandas_and_spark(monkeypatch):
    polars_df = pl.DataFrame({"value": [1], "_source_file": ["/landing/file.csv"]})
    updated_polars = lineage.add_columns(
        polars_df,
        {"_lakelogic_source": "contract.csv", "_lakelogic_run_id": "run-1"},
        "polars",
    )
    assert "_source_file" not in updated_polars.columns
    assert updated_polars.columns[-2:] == ["_lakelogic_source", "_lakelogic_run_id"]

    pandas_df = pd.DataFrame({"value": [1]})
    updated_pandas = lineage.add_columns(pandas_df, {"_lakelogic_run_id": "run-1"}, "pandas")
    assert updated_pandas.columns[-1] == "_lakelogic_run_id"

    fake_functions = types.SimpleNamespace(
        col=lambda name: f"col:{name}",
        lit=lambda value: types.SimpleNamespace(cast=lambda dtype: f"cast:{value}:{dtype}") if isinstance(value, str) else value,
    )
    fake_types = types.ModuleType("pyspark.sql.types")
    fake_types.TimestampType = lambda: "timestamp"
    fake_sql = types.ModuleType("pyspark.sql")
    fake_sql.functions = fake_functions
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", fake_functions)
    monkeypatch.setitem(sys.modules, "pyspark.sql.types", fake_types)

    spark_df = FakeSparkFrame(["value", "_source_file"])
    updated_spark = lineage.add_columns(
        spark_df,
        {"_lakelogic_source": "contract.csv", "_lakelogic_created_at": "2024-01-01T00:00:00"},
        "spark",
    )
    assert "_source_file" not in updated_spark.columns
    assert updated_spark.columns[-2:] == ["_lakelogic_source", "_lakelogic_created_at"]


def test_inject_lineage_builds_expected_metadata(monkeypatch):
    monkeypatch.setattr(lineage, "add_columns", lambda df, columns, engine_name: {"frame": df, "columns": columns, "engine": engine_name})
    monkeypatch.setattr("getpass.getuser", lambda: "tester")

    contract = types.SimpleNamespace(
        lineage=types.SimpleNamespace(
            enabled=True,
            preserve_upstream=["_lakelogic_source"],
            upstream_prefix="_upstream",
            run_id_source="pipeline_run_id",
            capture_source_path=True,
            capture_timestamp=True,
            capture_run_id=True,
            capture_contract_name=True,
            contract_name_column_name="_lakelogic_contract",
            capture_domain=True,
            domain_column_name="_lakelogic_domain",
            capture_system=True,
            system_column_name="_lakelogic_system",
            capture_created_at=True,
            created_at_column_name="_lakelogic_created_at",
            capture_created_by=True,
            created_by_column_name="_lakelogic_created_by",
            source_column_name="_lakelogic_source",
            timestamp_column_name="_lakelogic_loaded_at",
            run_id_column_name="_lakelogic_run_id",
            created_by_override=None,
        ),
        _contract_path="contracts/orders.yaml",
        info=types.SimpleNamespace(version="1.2.3"),
        metadata={"domain": "commerce", "system": "erp"},
    )

    monkeypatch.setattr(lineage, "_preserve_upstream_lineage", lambda df, columns, prefix, engine_name: {"preserved": True, "df": df})
    good, bad = lineage.inject_lineage(
        good_df={"good": True},
        bad_df={"bad": True},
        contract=contract,
        engine_name="polars",
        last_run_id=None,
        pipeline_run_id="pipe-1",
        source_path="/landing/orders.csv",
    )
    assert good["engine"] == "polars"
    assert good["columns"]["_lakelogic_run_id"] == "pipe-1"
    assert good["columns"]["_lakelogic_contract"] == "orders.yaml (v1.2.3)"
    assert good["columns"]["_lakelogic_domain"] == "commerce"
    assert good["columns"]["_lakelogic_system"] == "erp"
    assert good["columns"]["_lakelogic_created_by"] == "tester"
    assert bad["columns"]["_lakelogic_source"] == "/landing/orders.csv"


def test_inject_lineage_noop_without_enabled_lineage():
    contract = types.SimpleNamespace(lineage=types.SimpleNamespace(enabled=False))
    good_df = {"good": True}
    bad_df = {"bad": True}
    assert lineage.inject_lineage(good_df, bad_df, contract, "polars", "run-1") == (good_df, bad_df)


def test_preserve_upstream_lineage_and_add_columns_for_duckdb(monkeypatch):
    executed = []

    class FakeConnection:
        def execute(self, sql):
            executed.append(sql)
            return types.SimpleNamespace(fetchall=lambda: [("_lakelogic_source",), ("value",)])

        def sql(self, sql):
            executed.append(sql)
            return {"sql": sql}

    class FakeRelation:
        def __init__(self):
            self.columns = ["_lakelogic_source", "value"]
            self.connection = FakeConnection()

        def sql_query(self):
            return "SELECT * FROM source_table"

    fake_duckdb = types.ModuleType("duckdb")
    fake_duckdb.DuckDBPyRelation = FakeRelation
    monkeypatch.setitem(sys.modules, "duckdb", fake_duckdb)

    relation = FakeRelation()
    preserved = lineage._preserve_upstream_lineage(relation, ["_lakelogic_source"], "_upstream", "duckdb")
    assert '_upstream_lakelogic_source' in preserved["sql"]

    updated = lineage.add_columns(relation, {"_lakelogic_run_id": "run-1", "_lakelogic_flag": True}, "duckdb")
    assert 'TRUE AS "_lakelogic_flag"' in updated["sql"]
    assert '"value"' in updated["sql"]


def test_inject_lineage_generates_run_id_and_respects_override(monkeypatch):
    monkeypatch.setattr(lineage, "add_columns", lambda df, columns, engine_name: columns)
    monkeypatch.setattr(lineage, "_preserve_upstream_lineage", lambda df, columns, prefix, engine_name: df)
    monkeypatch.setattr("uuid.uuid4", lambda: "generated-run-id")

    contract = types.SimpleNamespace(
        lineage=types.SimpleNamespace(
            enabled=True,
            preserve_upstream=[],
            upstream_prefix="_upstream",
            run_id_source="run_id",
            capture_source_path=False,
            capture_timestamp=False,
            capture_run_id=True,
            run_id_column_name="_lakelogic_run_id",
            capture_contract_name=False,
            contract_name_column_name="_lakelogic_contract",
            capture_domain=False,
            domain_column_name="_lakelogic_domain",
            capture_system=False,
            system_column_name="_lakelogic_system",
            capture_created_at=False,
            created_at_column_name="_lakelogic_created_at",
            capture_created_by=True,
            created_by_column_name="_lakelogic_created_by",
            created_by_override="service-principal",
            source_column_name="_lakelogic_source",
            timestamp_column_name="_lakelogic_loaded_at",
        ),
        _contract_path=None,
        info=None,
        metadata={},
    )

    good, bad = lineage.inject_lineage(
        good_df={"good": True},
        bad_df={"bad": True},
        contract=contract,
        engine_name="polars",
        last_run_id=None,
        pipeline_run_id=None,
        source_path=None,
    )
    assert good["_lakelogic_run_id"] == "generated-run-id"
    assert good["_lakelogic_created_by"] == "service-principal"
    assert bad["_lakelogic_run_id"] == "generated-run-id"