from __future__ import annotations

import types

import pytest

from lakelogic.core.models import (
    DatasetRuleNullRatio,
    DatasetRuleRowCountBetween,
    DatasetRuleUnique,
    QualityRule,
    RowRuleAcceptedValues,
    RowRuleLifecycleWindow,
    RowRuleNotNull,
    RowRuleRange,
    RowRuleReferentialIntegrity,
    RowRuleRegexMatch,
)
from lakelogic.engines.base import EngineAdapter


class DummyAdapter(EngineAdapter):
    def execute(self, df):
        return df, None


def _make_adapter(*, engine_name="duckdb", engine_dialect="", dataset="orders", lineage=None):
    contract = types.SimpleNamespace(dataset=dataset, quality=None, model=None, lineage=lineage)
    adapter = DummyAdapter(contract)
    adapter.engine_name = engine_name
    adapter.engine_dialect = engine_dialect
    return adapter


def test_base_sql_helper_generation_and_lineage_columns():
    lineage = types.SimpleNamespace(
        enabled=True,
        capture_source_path=True,
        source_column_name="_src",
        capture_timestamp=True,
        timestamp_column_name="_ts",
        capture_run_id=False,
        run_id_column_name="_run_id",
        capture_contract_name=True,
        contract_name_column_name="_contract",
        capture_domain=False,
        domain_column_name="_domain",
        capture_system=True,
        system_column_name="_system",
    )
    adapter = _make_adapter(engine_name="bigquery", lineage=lineage)

    assert adapter._format_literal(None) == "NULL"
    assert adapter._format_literal(True) == "TRUE"
    assert adapter._format_literal("O'Brien") == "'O''Brien'"
    assert adapter._quote_ident('full"name') == '"full""name"'
    assert adapter._lineage_columns() == {"_src", "_ts", "_contract", "_system"}

    rollup = types.SimpleNamespace(
        group_by=["country"],
        aggregations={"revenue": "SUM(amount)"},
        keys=["id", "region"],
        key_expr=None,
        rollup_keys_column="rollup_keys",
        rollup_keys_count_column="rollup_key_count",
        upstream_run_id_column="run_id",
        upstream_run_ids_column="upstream_run_ids",
        distinct=True,
    )
    rollup_sql = adapter._build_rollup_sql(rollup)
    assert 'SUM(amount) AS "revenue"' in rollup_sql
    assert 'GROUP BY "country"' in rollup_sql
    assert 'ARRAY_AGG(DISTINCT CONCAT_WS(\'||\', "id", "region"))' in rollup_sql
    assert "ANY_VALUE(expr)" == adapter._pivot_agg_expr("first", "expr")
    assert "COUNT(DISTINCT expr)" == adapter._pivot_agg_expr("count_distinct", "expr")

    pivot = types.SimpleNamespace(
        pivot_col="status",
        pivot_cols=[],
        value_cols=["amount"],
        values=["A", "B"],
        pivot_values=None,
        id_vars=["customer_id"],
        value_aliases={"A": "active"},
        aggs={"amount": "sum"},
        agg="first",
        separator="_",
        name_template=None,
        fill_value=0,
    )
    pivot_sql = adapter._build_pivot_sql(pivot)
    assert 'COALESCE(SUM(CASE WHEN "status" = ' in pivot_sql
    assert 'AS "amount_active"' in pivot_sql
    assert 'GROUP BY "customer_id"' in pivot_sql

    unpivot = types.SimpleNamespace(
        id_vars=["customer_id"],
        value_vars=["amount_a", "amount_b"],
        value_cols=None,
        key_field="status",
        value_field="amount",
        include_nulls=False,
        value_aliases={"amount_a": "A"},
    )
    unpivot_sql = adapter._build_unpivot_sql(unpivot)
    assert "UNION ALL" in unpivot_sql
    assert 'WHERE "amount_a" IS NOT NULL' in unpivot_sql
    assert "'A' AS \"status\"" in unpivot_sql

    bucket = types.SimpleNamespace(
        field="size_bucket",
        source="amount",
        bins=[types.SimpleNamespace(gte=0, lt=100, label="small"), types.SimpleNamespace(gte=100, label="large")],
        default="unknown",
    )
    bucket_sql = adapter._build_bucket_sql(bucket)
    assert 'WHEN "amount" >= 0 AND "amount" < 100 THEN ' in bucket_sql
    assert "ELSE " in bucket_sql

    date_diff = types.SimpleNamespace(field="days_open", from_col="opened_at", to_col="closed_at", unit="days")
    date_diff_sql = adapter._build_date_diff_sql(date_diff)
    assert 'DATEDIFF(\'day\', CAST("opened_at" AS DATE), CAST("closed_at" AS DATE))' in date_diff_sql


def test_base_date_diff_and_transform_validation_paths():
    polars_adapter = _make_adapter(engine_name="polars")
    polars_sql = polars_adapter._build_date_diff_sql(
        types.SimpleNamespace(field="minutes_open", from_col="opened_at", to_col="closed_at", unit="minutes")
    )
    assert "EXTRACT(EPOCH FROM STRPTIME" in polars_sql
    assert "/ 60 AS INTEGER" in polars_sql

    spark_adapter = _make_adapter(engine_name="spark")
    spark_sql = spark_adapter._build_date_diff_sql(
        types.SimpleNamespace(field="days_open", from_col="opened_at", to_col="closed_at", unit="days")
    )
    # Columns are wrapped in a tolerant to_date (handles ISO + compact YYYYMMDD).
    assert "DATEDIFF(to_date(" in spark_sql
    assert '"closed_at"' in spark_sql and '"opened_at"' in spark_sql
    assert "RLIKE" in spark_sql  # the YYYYMMDD normalisation branch

    assert (
        polars_adapter._build_pivot_sql(
            types.SimpleNamespace(pivot_col=None, pivot_cols=[], value_cols=["x"], values=["a"])
        )
        is None
    )
    assert polars_adapter._build_unpivot_sql(types.SimpleNamespace(id_vars=["id"], value_vars=[])) is None
    assert (
        polars_adapter._build_bucket_sql(types.SimpleNamespace(field="bucket", source="amount", bins=[], default=None))
        is None
    )
    assert (
        polars_adapter._build_date_diff_sql(types.SimpleNamespace(field=None, from_col="a", to_col="b", unit="days"))
        is None
    )


def test_expand_row_rules_for_supported_structured_types():
    adapter = _make_adapter()

    not_null_rules = adapter._expand_row_rule(
        RowRuleNotNull(not_null={"fields": ["email", {"field": "status", "severity": "warn"}]})
    )
    assert len(not_null_rules) == 2
    assert not_null_rules[0].sql == '"email" IS NOT NULL'
    assert not_null_rules[1].severity == "warn"

    accepted = adapter._expand_row_rule(
        RowRuleAcceptedValues(accepted_values={"field": "status", "values": ["A", "B"], "severity": "warn"})
    )
    assert accepted.sql == "\"status\" IN ('A', 'B')"
    assert accepted.severity == "warn"

    regex = adapter._expand_row_rule(RowRuleRegexMatch(regex_match={"field": "email", "pattern": ".+@.+"}))
    assert "REGEXP_MATCHES" in regex.sql

    range_rule = adapter._expand_row_rule(
        RowRuleRange(range={"field": "age", "min": 18, "max": 65, "inclusive": False})
    )
    assert range_rule.sql == '"age" > 18 AND "age" < 65'

    ref_rule = adapter._expand_row_rule(
        RowRuleReferentialIntegrity(
            referential_integrity={"field": "customer_id", "reference": "dim_customers", "key": "id"}
        )
    )
    assert ref_rule.sql == '"customer_id" IN (SELECT "id" FROM dim_customers)'

    lifecycle = adapter._expand_row_rule(
        RowRuleLifecycleWindow(
            lifecycle_window={
                "event_ts": "event_at",
                "event_key": "customer_id",
                "reference": "dim_customers",
                "reference_key": "id",
            }
        )
    )
    assert (
        'COALESCE((SELECT r."end_date" FROM dim_customers r WHERE r."id" = "customer_id"), \'9999-12-31\')'
        in lifecycle.sql
    )

    rule = QualityRule(name="raw", sql="1=1")
    assert adapter._expand_row_rule(rule) is rule
    assert adapter._expand_row_rule(RowRuleAcceptedValues(accepted_values={"field": "status"})) is None


def test_expand_dataset_rules_and_transpile_helpers(monkeypatch):
    adapter = _make_adapter(engine_name="pandas", dataset="orders_source")

    unique = adapter._expand_dataset_rule(DatasetRuleUnique(unique="order_id"))
    assert unique.sql == 'SELECT COUNT(*) - COUNT(DISTINCT "order_id") FROM orders_source'
    assert unique.must_be_less_than == 1

    null_ratio = adapter._expand_dataset_rule(DatasetRuleNullRatio(null_ratio={"field": "email", "max": 25}))
    assert null_ratio.must_be_less_than == 0.25
    assert "NULLIF(COUNT(*), 0)" in null_ratio.sql

    row_count_between = adapter._expand_dataset_rule(
        DatasetRuleRowCountBetween(row_count_between={"min": 10, "max": 20})
    )
    assert row_count_between.must_be_between == [10, 20]

    min_only = adapter._expand_dataset_rule(DatasetRuleRowCountBetween(row_count_between={"min": 10}))
    assert min_only.must_be_greater_than == 10

    max_only = adapter._expand_dataset_rule(DatasetRuleRowCountBetween(row_count_between={"max": 20}))
    assert max_only.must_be_less_than == 20

    passthrough = QualityRule(name="raw_dataset", sql="SELECT 1")
    assert adapter._expand_dataset_rule(passthrough) is passthrough
    assert adapter._expand_dataset_rule(DatasetRuleNullRatio(null_ratio={"field": "email"})) is None
    assert adapter._normalize_engine() == "duckdb"
    assert adapter._resolve_dialect() == "duckdb"

    monkeypatch.setattr("sqlglot.transpile", lambda sql, read, write: [f"{write}:{sql}"])
    assert adapter._transpile("SELECT 1", read_dialect="spark") == "duckdb:SELECT 1"

    derive = types.SimpleNamespace(sql="timestamp_micros(ts)", sql_duckdb="", sql_spark="")
    transpile_calls = []

    def fake_transpile(sql, read, write):
        transpile_calls.append((sql, read, write))
        return [sql]

    monkeypatch.setattr("sqlglot.transpile", fake_transpile)
    rendered = adapter._transpile_derive_sql(derive)
    assert "make_timestamp(CAST(ts AS BIGINT))" in rendered
    assert transpile_calls[0][1:] == ("spark", "duckdb")

    duckdb_override = types.SimpleNamespace(sql="ignored", sql_duckdb="custom_duckdb_sql", sql_spark=None)
    assert adapter._transpile_derive_sql(duckdb_override) == "custom_duckdb_sql"


def test_add_trace_and_row_count_helpers():
    polars = pytest.importorskip("polars")
    adapter = _make_adapter()

    lazy = polars.DataFrame({"id": [1, 2]}).lazy()
    assert adapter._get_row_count(lazy) == 2
    assert adapter._get_row_count(types.SimpleNamespace(height=3)) == 3
    assert adapter._get_row_count(types.SimpleNamespace(count=lambda: 4)) == 4
    assert adapter._get_row_count([1, 2, 3]) == 3
    assert adapter._get_row_count(None) == 0

    adapter._add_trace("validate", input_rows=2, output_rows=1, duration_ms=5.5, details={"rule": "email"})
    assert len(adapter.trace) == 1
    assert adapter.trace[0].step == "validate"
    assert adapter.trace[0].details == {"rule": "email"}
