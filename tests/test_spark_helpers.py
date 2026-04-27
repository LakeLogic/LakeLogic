from __future__ import annotations

import re
import sys
import types

from lakelogic.core.models import DataContract
from lakelogic.engines.spark import SparkAdapter


class FakeExpr:
    def __init__(self, value):
        self.value = value
        self.alias_name = None

    def cast(self, spark_type):
        return FakeExpr(("cast", self.value, spark_type))

    def alias(self, name):
        self.alias_name = name
        return self

    def isNotNull(self):
        return FakeExpr(("isNotNull", self.value))

    def isNull(self):
        return FakeExpr(("isNull", self.value))

    def __and__(self, other):
        return FakeExpr(("and", self.value, other.value))

    def __or__(self, other):
        return FakeExpr(("or", self.value, other.value))

    def __invert__(self):
        return FakeExpr(("not", self.value))

    def __gt__(self, other):
        return FakeExpr(("gt", self.value, other))

    def __eq__(self, other):
        other_value = other.value if hasattr(other, "value") else other
        return FakeExpr(("eq", self.value, other_value))

    def asc(self):
        return FakeExpr(("asc", self.value))

    def desc(self):
        return FakeExpr(("desc", self.value))

    def over(self, window):
        return FakeExpr(("over", self.value, window))


class FakeWhen:
    def __init__(self, condition, value):
        self.chain = [(condition, value)]

    def when(self, condition, value):
        self.chain.append((condition, value))
        return self

    def otherwise(self, value):
        rendered = []
        for condition, candidate in self.chain:
            rendered.append((condition.value, candidate.value if hasattr(candidate, "value") else candidate))
        return FakeExpr(("when", rendered, value.value if hasattr(value, "value") else value))


class FakeFunctions:
    @staticmethod
    def col(name):
        return FakeExpr(("col", name))

    @staticmethod
    def lit(value):
        return FakeExpr(("lit", value))

    @staticmethod
    def when(condition, value):
        return FakeWhen(condition, value)

    @staticmethod
    def array(*values):
        return FakeExpr(("array", [value.value if hasattr(value, "value") else value for value in values]))

    @staticmethod
    def expr(sql):
        return FakeExpr(("expr", sql))

    @staticmethod
    def size(value):
        return FakeExpr(("size", value.value if hasattr(value, "value") else value))

    @staticmethod
    def coalesce(*values):
        return FakeExpr(("coalesce", [value.value if hasattr(value, "value") else value for value in values]))

    @staticmethod
    def split(value, delimiter):
        return FakeExpr(("split", value.value, delimiter))

    @staticmethod
    def explode(value):
        return FakeExpr(("explode", value.value))

    @staticmethod
    def lower(value):
        return FakeExpr(("lower", value.value))

    @staticmethod
    def upper(value):
        return FakeExpr(("upper", value.value))

    @staticmethod
    def trim(value):
        return FakeExpr(("trim", value.value))

    @staticmethod
    def ltrim(value):
        return FakeExpr(("ltrim", value.value))

    @staticmethod
    def rtrim(value):
        return FakeExpr(("rtrim", value.value))

    @staticmethod
    def row_number():
        return FakeExpr(("row_number",))


class FakeWindowSpec:
    def __init__(self, columns):
        self.columns = columns
        self.ordering = []

    def orderBy(self, *columns):
        self.ordering = list(columns)
        return self


class FakeWindowModule:
    @staticmethod
    def partitionBy(*columns):
        return FakeWindowSpec(columns)


class FakeStringType:
    pass


class FakeField:
    def __init__(self, data_type):
        self.dataType = data_type


class FakeSchema:
    def __init__(self, fields):
        self.fields = fields


class FakeSelectableDataFrame:
    def __init__(self, columns, schema_fields=None):
        self.columns = list(columns)
        self.selected = None
        self.with_columns = []
        self.schema = FakeSchema(schema_fields or [])

    def select(self, *exprs):
        self.selected = exprs
        selected_names = [getattr(expr, "alias_name", None) or expr.value[1] for expr in exprs if hasattr(expr, "value")]
        clone = FakeSelectableDataFrame(selected_names, self.schema.fields)
        clone.selected = exprs
        clone.with_columns = list(self.with_columns)
        return clone

    def withColumn(self, name, expr):
        clone = FakeSelectableDataFrame(self.columns, self.schema.fields)
        clone.selected = self.selected
        clone.with_columns = self.with_columns + [(name, expr.value)]
        return clone


class FakeRuleResult:
    def __init__(self, value):
        self.value = value

    def collect(self):
        return [[self.value]]


class FakeSparkForRules:
    def __init__(self, values, failures=None):
        self.values = values
        self.failures = failures or set()

    def sql(self, sql):
        if sql in self.failures:
            raise RuntimeError("boom")
        return FakeRuleResult(self.values[sql])


class FakeRuleDataFrame:
    def __init__(self, spark):
        self.sparkSession = spark
        self.views = []

    def createOrReplaceTempView(self, name):
        self.views.append(name)


class FakeRefDataFrame:
    def __init__(self, columns):
        self.columns = list(columns)
        self.selected = None
        self.views = []

    def select(self, *columns):
        clone = FakeRefDataFrame(columns)
        clone.selected = columns
        return clone

    def createOrReplaceTempView(self, name):
        self.views.append(name)


class FakeRead:
    def __init__(self):
        self.option_calls = []
        self.paths = []

    def option(self, key, value):
        self.option_calls.append((key, value))
        return self

    def csv(self, path):
        self.paths.append(("csv", path))
        return FakeRefDataFrame(["id", "name", "extra"])

    def parquet(self, path):
        self.paths.append(("parquet", path))
        return FakeRefDataFrame(["id", "amount"])


class FakeSparkForLinks:
    def __init__(self):
        self.read = FakeRead()
        self.tables = []
        self.views = {}

    def table(self, name):
        self.tables.append(name)
        df = FakeRefDataFrame(["id", "status", "tier"])
        self.views[name] = df
        return df


def _install_fake_pyspark(monkeypatch):
    fake_sql_module = types.SimpleNamespace(functions=FakeFunctions, Window=FakeWindowModule, types=types.SimpleNamespace(StringType=FakeStringType))
    monkeypatch.setitem(sys.modules, "pyspark", types.SimpleNamespace())
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", FakeFunctions)
    monkeypatch.setitem(sys.modules, "pyspark.sql.types", types.SimpleNamespace(StringType=FakeStringType))


class FakeSparkSessionExec:
    def __init__(self):
        self.sql_calls = []
        self.base_columns = []

    def createDataFrame(self, rows):
        self.base_columns = list(rows[0].keys()) if rows else []
        return FakeExecDataFrame(self, self.base_columns)

    def sql(self, sql):
        self.sql_calls.append(sql)
        upper_sql = sql.upper()
        columns = list(self.base_columns)
        if "SELECT" in upper_sql and "FROM" in upper_sql:
            select_part = sql[upper_sql.index("SELECT") + 6 : upper_sql.index("FROM")]
            parsed = []
            for chunk in select_part.split(","):
                token = chunk.strip()
                if not token:
                    continue
                if token == "*" or token.endswith(".*"):
                    parsed.extend(self.base_columns)
                    continue
                alias_match = re.search(r"\bAS\s+([A-Za-z_][A-Za-z0-9_]*)$", token, re.IGNORECASE)
                if alias_match:
                    parsed.append(alias_match.group(1))
                else:
                    parsed.append(token.split(".")[-1].strip())
            if parsed:
                columns = parsed
        self.base_columns = list(dict.fromkeys(columns))
        return FakeExecDataFrame(self, self.base_columns)


class FakeSparkSessionModule:
    active_session = None

    class Builder:
        def getOrCreate(self):
            if FakeSparkSessionModule.active_session is None:
                FakeSparkSessionModule.active_session = FakeSparkSessionExec()
            return FakeSparkSessionModule.active_session

    builder = Builder()

    @staticmethod
    def getActiveSession():
        return FakeSparkSessionModule.active_session


class FakeExecDataFrame:
    def __init__(self, spark, columns):
        self.sparkSession = spark
        self.columns = list(columns)
        self.views = []
        self.with_columns = []
        self.filters = []
        self.drop_calls = []
        self.select_calls = []
        self.union_args = []
        self.drop_duplicates_on = None

    def _clone(self, columns=None):
        clone = FakeExecDataFrame(self.sparkSession, self.columns if columns is None else columns)
        clone.views = list(self.views)
        clone.with_columns = list(self.with_columns)
        clone.filters = list(self.filters)
        clone.drop_calls = list(self.drop_calls)
        clone.select_calls = list(self.select_calls)
        clone.union_args = list(self.union_args)
        clone.drop_duplicates_on = self.drop_duplicates_on
        return clone

    def createOrReplaceTempView(self, name):
        self.views.append(name)
        self.sparkSession.base_columns = list(self.columns)

    def withColumn(self, name, expr):
        columns = list(self.columns)
        if name not in columns:
            columns.append(name)
        clone = self._clone(columns)
        clone.with_columns.append((name, expr.value if hasattr(expr, "value") else expr))
        return clone

    def withColumnRenamed(self, old, new):
        columns = [new if column == old else column for column in self.columns]
        return self._clone(columns)

    def select(self, *columns):
        if len(columns) == 1 and isinstance(columns[0], (list, tuple)):
            selected = list(columns[0])
        else:
            selected = list(columns)
        clone = self._clone(selected)
        clone.select_calls.append(selected)
        return clone

    def drop(self, *columns):
        kept = [column for column in self.columns if column not in columns]
        clone = self._clone(kept)
        clone.drop_calls.append(list(columns))
        return clone

    def filter(self, expr):
        clone = self._clone()
        clone.filters.append(expr.value if hasattr(expr, "value") else expr)
        return clone

    def unionByName(self, other, allowMissingColumns=False):
        columns = list(dict.fromkeys(self.columns + other.columns))
        clone = self._clone(columns)
        clone.union_args.append((sorted(other.columns), allowMissingColumns))
        return clone

    def dropDuplicates(self, columns):
        clone = self._clone()
        clone.drop_duplicates_on = list(columns)
        return clone


def _install_fake_exec_pyspark(monkeypatch):
    spark_session = FakeSparkSessionExec()
    FakeSparkSessionModule.active_session = spark_session
    fake_sql_module = types.SimpleNamespace(
        DataFrame=FakeExecDataFrame,
        SparkSession=FakeSparkSessionModule,
        functions=FakeFunctions,
        Window=FakeWindowModule,
    )
    monkeypatch.setitem(sys.modules, "pyspark", types.SimpleNamespace())
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", FakeFunctions)
    return spark_session


def test_spark_helper_join_type_and_type_mapping():
    contract = DataContract(
        version="1.0.0",
        dataset="orders",
        links=[{"name": "ref", "broadcast": True}],
    )
    adapter = SparkAdapter(contract)

    assert adapter._quote_ident("my`col") == "`my``col`"
    assert adapter._to_spark_type("integer") == "long"
    assert adapter._to_spark_type("struct<a:string>") == "struct<a:string>"
    assert adapter._to_spark_type("unknown") is None
    assert adapter._should_broadcast("ref") is True
    assert adapter._should_broadcast("missing") is False

    join_cfg = types.SimpleNamespace(
        type="full",
        fields=["status", "tier"],
        prefix="ref_",
        defaults={"status": "unknown"},
        reference="customer_ref",
        on="customer_id",
        key="id",
    )
    sql = adapter._build_join_sql(join_cfg, broadcast=True, source_table="orders")
    assert "FULL OUTER JOIN customer_ref ref" in sql
    assert "/*+ BROADCAST(ref) */" in sql
    assert "COALESCE(ref.status, 'unknown') AS ref_status" in sql
    assert "ref.tier AS ref_tier" in sql


def test_spark_helper_run_dataset_rules(monkeypatch):
    contract = DataContract(
        version="1.0.0",
        dataset="orders",
        quality={
            "dataset_rules": [
                {"name": "between", "sql": "between_sql", "must_be_between": [1, 5]},
                {"name": "less_than", "sql": "less_sql", "must_be_less_than": 10},
                {"name": "greater_than", "sql": "greater_sql", "must_be_greater_than": 3},
                {"name": "null_rule", "sql": "null_sql"},
                {"name": "broken", "sql": "broken_sql"},
            ]
        },
    )
    adapter = SparkAdapter(contract)

    infos = []
    errors = []
    monkeypatch.setattr("lakelogic.engines.spark.logger.info", lambda message: infos.append(message))
    monkeypatch.setattr("lakelogic.engines.spark.logger.error", lambda message: errors.append(message))

    spark = FakeSparkForRules(
        values={"between_sql": 3, "less_sql": 8, "greater_sql": 2, "null_sql": None},
        failures={"broken_sql"},
    )
    adapter._run_dataset_rules(FakeRuleDataFrame(spark))

    assert len(adapter.dataset_rule_results) == 4
    assert adapter.dataset_rule_results[0]["passed"] is True
    assert adapter.dataset_rule_results[2]["passed"] is False
    assert adapter.dataset_rule_results[3]["passed"] is False
    assert any("Quality Check (Spark): between" in message for message in infos)
    assert any("broken" in message for message in errors)


def test_spark_helper_register_links_and_apply_schema(monkeypatch, tmp_path):
    _install_fake_pyspark(monkeypatch)

    csv_path = tmp_path / "lookup.csv"
    csv_path.write_text("id,name\n1,A\n", encoding="utf-8")
    parquet_path = tmp_path / "lookup.parquet"
    parquet_path.write_text("parquet", encoding="utf-8")
    txt_path = tmp_path / "lookup.txt"
    txt_path.write_text("x", encoding="utf-8")

    contract = DataContract(
        version="1.0.0",
        dataset="orders",
        links=[
            {"name": "table_ref", "table": "catalog.table", "columns": ["id", "status"]},
            {"name": "remote_ref", "path": "abfss://container/ref.csv"},
            {"name": "missing_ref", "path": str(tmp_path / "missing.csv")},
            {"name": "csv_ref", "path": str(csv_path), "columns": ["id", "name"]},
            {"name": "pq_ref", "path": str(parquet_path)},
            {"name": "bad_ref", "path": str(txt_path)},
        ],
        model={
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "name", "type": "string"},
                {"name": "missing_col", "type": "string"},
            ]
        },
        server={"type": "local", "path": "x", "schema_policy": {"unknown_fields": "quarantine", "evolution": "strict"}},
    )
    adapter = SparkAdapter(contract)

    warnings = []
    debugs = []
    monkeypatch.setattr("lakelogic.engines.spark.logger.warning", lambda message: warnings.append(message))
    monkeypatch.setattr("lakelogic.engines.spark.logger.debug", lambda message: debugs.append(message))

    spark = FakeSparkForLinks()
    adapter._register_links(spark)
    assert spark.tables == ["catalog.table"]
    assert ("csv", csv_path.as_posix()) in spark.read.paths
    assert ("parquet", parquet_path.as_posix()) in spark.read.paths
    assert any("remote path" in message for message in warnings)
    assert any("Link file not found" in message for message in warnings)
    assert any("Unsupported link format" in message for message in warnings)
    assert any("projected to 2 columns" in message for message in debugs)

    schema_df = FakeSelectableDataFrame(
        ["id", "name", "extra", "_lakelogic_run_id"],
        schema_fields=[FakeField(object()), FakeField(object()), FakeField(object())],
    )
    selected_df, schema_errors = adapter._apply_schema(schema_df)
    assert any("Missing fields" in error for error in schema_errors)
    assert any("Unknown fields present" in error for error in schema_errors)
    assert adapter.schema_drift["unknown_fields"] == ["extra"]
    alias_names = [expr.alias_name for expr in selected_df.selected if getattr(expr, "alias_name", None)]
    assert "id" in alias_names
    assert "name" in alias_names
    assert "missing_col" in alias_names
    assert any(name.startswith("__type_err_") for name in alias_names)


def test_spark_helper_cast_to_contract_types(monkeypatch):
    _install_fake_pyspark(monkeypatch)
    contract = DataContract(
        version="1.0.0",
        model={
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "amount", "type": "double"},
                {"name": "status", "type": "string"},
            ]
        },
    )
    adapter = SparkAdapter(contract)

    debug_messages = []
    monkeypatch.setattr("lakelogic.engines.spark.logger.debug", lambda message: debug_messages.append(message))

    all_string_df = FakeSelectableDataFrame(
        ["id", "amount", "status"],
        schema_fields=[FakeField(FakeStringType()), FakeField(FakeStringType()), FakeField(FakeStringType())],
    )
    casted_df = adapter._cast_to_contract_types(all_string_df)
    assert casted_df.with_columns == [
        ("id", ("cast", ("col", "id"), "INT")),
        ("amount", ("cast", ("col", "amount"), "DOUBLE")),
    ]
    assert any("Auto-cast 2 columns" in message for message in debug_messages)

    mixed_df = FakeSelectableDataFrame(
        ["id"],
        schema_fields=[FakeField(object())],
    )
    assert adapter._cast_to_contract_types(mixed_df) is mixed_df


def test_spark_helper_execute_handles_list_input_and_post_rules(monkeypatch):
    spark = _install_fake_exec_pyspark(monkeypatch)
    contract = types.SimpleNamespace(
        dataset="orders",
        transformations=[object()],
        quarantine=types.SimpleNamespace(include_error_reason=False),
    )
    adapter = SparkAdapter(contract)
    adapter._type_err_cols = ["__type_err_id"]
    adapter.get_row_rules = lambda: [
        types.SimpleNamespace(phase="pre", sql="id IS NOT NULL", name="pre_rule", category="quality"),
        types.SimpleNamespace(phase="post", sql="status IS NOT NULL", name="post_rule", category="quality"),
    ]

    calls = []
    adapter._register_links = lambda spark_session: calls.append(("links", spark_session))
    adapter._apply_pre_transformations = lambda df: calls.append("pre") or df
    adapter._apply_schema = lambda df: (df.withColumn("__type_err_id", FakeFunctions.lit(None)), ["schema mismatch"])
    adapter._run_dataset_rules = lambda df: calls.append(("dataset", list(df.columns)))
    adapter._apply_post_transformations = lambda df: calls.append("post") or df.withColumn("derived", FakeFunctions.lit("ok"))

    good_df, bad_df = adapter.execute([{"id": 1, "status": "ok"}])

    assert isinstance(good_df, FakeExecDataFrame)
    assert isinstance(bad_df, FakeExecDataFrame)
    assert calls[0] == ("links", spark)
    assert "pre" in calls
    assert "post" in calls
    assert any(item[0] == "dataset" for item in calls if isinstance(item, tuple))
    assert any("CAST((id IS NOT NULL) AS BOOLEAN) as _rule_0" in sql for sql in spark.sql_calls)
    assert any("CAST((status IS NOT NULL) AS BOOLEAN) as _post_rule_0" in sql for sql in spark.sql_calls)
    assert "quarantine_state" in bad_df.columns
    assert "quarantine_reprocessed" in bad_df.columns
    assert adapter.ERROR_COLUMN not in bad_df.columns
    assert adapter.CATEGORY_COLUMN not in bad_df.columns


def test_spark_helper_pre_transformations_cover_column_operations(monkeypatch):
    _install_fake_pyspark(monkeypatch)
    warnings = []
    monkeypatch.setattr("lakelogic.engines.spark.logger.warning", warnings.append)

    def _transformation(**kwargs):
        base = {
            "phase": "pre",
            "sql": None,
            "derive": None,
            "pivot": None,
            "unpivot": None,
            "rename": None,
            "select": None,
            "drop": None,
            "cast": None,
            "trim": None,
            "lower": None,
            "upper": None,
            "coalesce": None,
            "split": None,
            "explode": None,
            "map_values": None,
            "filter": None,
            "deduplicate": None,
        }
        base.update(kwargs)
        return types.SimpleNamespace(**base)

    contract = types.SimpleNamespace(
        dataset="orders",
        transformations=[
            _transformation(sql="SELECT id, status FROM source"),
            _transformation(derive=types.SimpleNamespace(field="derived")),
            _transformation(rename=types.SimpleNamespace(iter_pairs=lambda: [("missing", "skipped"), ("id", "order_id")])),
            _transformation(select=types.SimpleNamespace(columns=["order_id", "status", "derived"])),
            _transformation(drop=types.SimpleNamespace(columns=["status"])),
            _transformation(cast=types.SimpleNamespace(columns={"order_id": "integer"})),
            _transformation(trim=types.SimpleNamespace(fields=["order_id"], side="both")),
            _transformation(lower=types.SimpleNamespace(fields=["order_id"])),
            _transformation(upper=types.SimpleNamespace(fields=["order_id"])),
            _transformation(coalesce=types.SimpleNamespace(field="status_filled", sources=["status"], default="unknown", output="status_filled")),
            _transformation(split=types.SimpleNamespace(field="order_id", delimiter="-", output="parts")),
            _transformation(explode=types.SimpleNamespace(field="parts", output="part")),
            _transformation(map_values=types.SimpleNamespace(field="part", mapping={"A": "active"}, default="other", output="mapped")),
            _transformation(filter=types.SimpleNamespace(sql="mapped IS NOT NULL")),
            _transformation(deduplicate=types.SimpleNamespace(on=["mapped"], sort_by=None)),
        ],
    )
    adapter = SparkAdapter(contract)
    adapter._transpile_derive_sql = lambda derive: "UPPER(order_id)"

    spark = FakeSparkSessionExec()
    df = FakeExecDataFrame(spark, ["id", "status"])
    transformed = adapter._apply_pre_transformations(df)

    assert any("SELECT id, status FROM source" in sql for sql in spark.sql_calls)
    assert "order_id" in transformed.columns
    assert "derived" in transformed.columns
    assert "status" not in transformed.columns
    assert "status_filled" in transformed.columns
    assert "parts" in transformed.columns
    assert "part" in transformed.columns
    assert "mapped" in transformed.columns
    assert transformed.drop_duplicates_on == ["mapped"]
    assert transformed.filters[-1] == "mapped IS NOT NULL"
    assert any("column not found" in message for message in warnings)


def test_spark_helper_post_transformations_cover_sql_lookup_and_filter(monkeypatch):
    _install_fake_pyspark(monkeypatch)

    def _transformation(**kwargs):
        base = {
            "phase": "post",
            "sql": None,
            "rollup": None,
            "pivot": None,
            "unpivot": None,
            "derive": None,
            "bucket": None,
            "date_diff": None,
            "lookup": None,
            "join": None,
            "filter": None,
        }
        base.update(kwargs)
        return types.SimpleNamespace(**base)

    contract = types.SimpleNamespace(
        dataset="orders",
        transformations=[
            _transformation(sql="SELECT id, status FROM source"),
            _transformation(rollup=types.SimpleNamespace(field="rollup")),
            _transformation(pivot=types.SimpleNamespace(field="pivot")),
            _transformation(unpivot=types.SimpleNamespace(field="unpivot")),
            _transformation(derive=types.SimpleNamespace(field="derived")),
            _transformation(bucket=types.SimpleNamespace(field="bucketed")),
            _transformation(date_diff=types.SimpleNamespace(field="days_open")),
            _transformation(lookup=types.SimpleNamespace(field="status_name", reference="status_ref", on="status_id", key="id", value="name", default_value="unknown")),
            _transformation(join=types.SimpleNamespace(reference="customer_ref")),
            _transformation(filter=types.SimpleNamespace(sql="status_name IS NOT NULL")),
        ],
    )
    adapter = SparkAdapter(contract)
    adapter._cast_to_contract_types = lambda df: df
    adapter._transpile_derive_sql = lambda derive: "UPPER(status)"
    adapter._build_rollup_sql = lambda cfg, source_table="source": "SELECT id, SUM(amount) AS total_amount FROM source"
    adapter._build_pivot_sql = lambda cfg, source_table="source": "SELECT id, status FROM source"
    adapter._build_unpivot_sql = lambda cfg, source_table="source": "SELECT id, metric, value FROM source"
    adapter._build_bucket_sql = lambda cfg, source_table="temp_src": "SELECT *, (CASE WHEN amount > 10 THEN 'high' ELSE 'low' END) AS bucketed FROM temp_src"
    adapter._build_date_diff_sql = lambda cfg, source_table="temp_src": "SELECT *, (DATEDIFF(closed_at, opened_at)) AS days_open FROM temp_src"
    adapter._build_join_sql = lambda join_cfg, broadcast=False, source_table="source": "SELECT source.*, ref.segment AS segment FROM source LEFT JOIN customer_ref ref ON source.customer_id = ref.id"
    adapter._should_broadcast = lambda reference: True

    spark = FakeSparkSessionExec()
    df = FakeExecDataFrame(spark, ["id", "status", "status_id", "amount", "opened_at", "closed_at", "customer_id"])
    transformed = adapter._apply_post_transformations(df)

    assert any("SELECT id, status FROM source" in sql for sql in spark.sql_calls)
    assert any("SUM(amount) AS total_amount" in sql for sql in spark.sql_calls)
    assert any("LEFT JOIN status_ref ref" in sql for sql in spark.sql_calls)
    assert any("LEFT JOIN customer_ref ref" in sql for sql in spark.sql_calls)
    assert "derived" in transformed.columns
    assert "bucketed" in transformed.columns
    assert "days_open" in transformed.columns
    assert transformed.filters[-1] == "status_name IS NOT NULL"
