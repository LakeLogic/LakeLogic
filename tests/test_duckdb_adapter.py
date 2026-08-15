from __future__ import annotations

import types
from pathlib import Path

import pytest

from lakelogic.core.models import DataContract
from lakelogic.engines.duckdb import DuckDBAdapter


pl = pytest.importorskip("polars")
pytest.importorskip("duckdb")


def test_duckdb_adapter_execute_splits_good_bad_rows_and_runs_dataset_rules() -> None:
    contract = DataContract(
        version="1.0.0",
        dataset="orders",
        model={"fields": [{"name": "id", "type": "int", "required": True}, {"name": "amount", "type": "double"}]},
        quality={
            "row_rules": [{"name": "positive_amount", "sql": "amount > 0", "category": "quality"}],
            "dataset_rules": [
                {"name": "row_count", "sql": "SELECT COUNT(*) FROM {dataset}", "must_be_between": [1, 10]}
            ],
        },
    )
    adapter = DuckDBAdapter(contract)

    good, bad = adapter.execute(pl.DataFrame({"id": ["1", "bad", "3"], "amount": [10.5, 5.0, -1.0]}))

    assert good["id"].to_list() == [1]
    assert len(bad) == 2
    assert "_lakelogic_errors" in bad.columns
    assert "schema" in bad.filter(pl.col("id").is_null())["_lakelogic_categories"][0].to_list()
    assert adapter.dataset_rule_results == [
        # Dataset rules now evaluate the POST-split good data (1 row; 2 were
        # quarantined), matching the Polars engine — previously it wrongly counted
        # the original 3 input rows.
        {"name": "row_count", "value": "1 (expected 1.0 to 10.0)", "passed": True, "description": None}
    ]
    assert [step.step for step in adapter.trace] == [
        "Load Source",
        "Schema Enforcement",
        "Row Rules Evaluation",
    ]
    adapter.close()
    assert adapter.con is None


def test_duckdb_adapter_schema_policies_missing_unknown_and_quarantine_output() -> None:
    strict = DataContract(
        version="1.0.0",
        server={
            "type": "local",
            "path": "memory",
            "schema_policy": {"evolution": "strict", "unknown_fields": "quarantine"},
        },
        model={"fields": [{"name": "id", "type": "int"}, {"name": "required_name", "type": "string"}]},
        quarantine={"include_error_reason": False},
    )
    adapter = DuckDBAdapter(strict)

    good, bad = adapter.execute(pl.DataFrame({"id": [1], "extra": ["x"], "_lakelogic_source": ["upstream"]}))

    assert len(good) == 0
    assert len(bad) == 1
    assert "_lakelogic_errors" not in bad.columns
    assert adapter.schema_drift["missing_fields"] == ["required_name"]
    assert adapter.schema_drift["unknown_fields"] == ["extra"]

    drop = DataContract(
        version="1.0.0",
        server={"type": "local", "path": "memory", "schema_policy": {"unknown_fields": "drop"}},
        model={"fields": [{"name": "id", "type": "int"}]},
    )
    good, bad = DuckDBAdapter(drop).execute(pl.DataFrame({"id": [1], "extra": ["x"]}))
    assert good.columns == ["id"]
    assert len(bad) == 0


def test_duckdb_adapter_pre_and_post_transformations_cover_sql_derive_filter_rename_rollup_pivot_unpivot() -> None:
    contract = DataContract(
        version="1.0.0",
        dataset="source",
        model={"fields": [{"name": "customer", "type": "string"}, {"name": "amount", "type": "double"}]},
        transformations=[
            {"rename": {"mappings": {"customer_id": "customer"}}, "phase": "pre"},
            {"derive": {"field": "amount", "sql": "raw_amount * 2"}, "phase": "pre"},
            {"filter": "amount >= 20", "phase": "pre"},
            {"sql": "SELECT customer, amount, CURRENT_DATE AS run_date FROM source", "phase": "post"},
            {
                "rollup": {
                    "group_by": ["customer"],
                    "aggregations": {"total_amount": "SUM(amount)"},
                    "keys": None,
                    "upstream_run_id_column": None,
                },
                "phase": "post",
            },
        ],
    )
    good, bad = DuckDBAdapter(contract).execute(pl.DataFrame({"customer_id": ["a", "b"], "raw_amount": [5.0, 20.0]}))

    assert len(bad) == 0
    assert good["customer"].to_list() == ["b"]
    assert good["total_amount"].to_list() == [40.0]


def test_duckdb_adapter_deduplicate_removes_duplicate_keys() -> None:
    # Regression: the duckdb engine previously had no `deduplicate` branch in its
    # pre-transform pass, so duplicate keys survived where Polars/Spark removed
    # them. `deduplicate` is applied regardless of `phase` (parity with Polars).
    contract = DataContract(
        version="1.0.0",
        dataset="source",
        model={"fields": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}]},
        transformations=[{"deduplicate": {"by": ["id"]}}],
    )
    good, bad = DuckDBAdapter(contract).execute(pl.DataFrame({"id": [1, 1, 1, 2], "name": ["a", "a", "a", "b"]}))
    assert len(bad) == 0
    assert sorted(good["id"].to_list()) == [1, 2]  # three id=1 rows collapse to one


def test_duckdb_adapter_deduplicate_sort_by_keeps_ordered_survivor() -> None:
    contract = DataContract(
        version="1.0.0",
        dataset="source",
        model={"fields": [{"name": "id", "type": "int"}, {"name": "v", "type": "int"}]},
        transformations=[{"deduplicate": {"by": ["id"], "sort_by": ["v"], "order": "desc"}}],
    )
    good, _ = DuckDBAdapter(contract).execute(pl.DataFrame({"id": [1, 1, 1], "v": [10, 30, 20]}))
    assert good["id"].to_list() == [1]
    assert good["v"].to_list() == [30]  # order=desc keeps the highest v

    pivot_contract = DataContract(
        version="1.0.0",
        transformations=[
            {
                "pivot": {
                    "id_vars": ["customer"],
                    "pivot_col": "status",
                    "value_cols": ["amount"],
                    "values": ["paid", "open"],
                    "aggs": {"amount": "sum"},
                    "fill_value": 0,
                }
            }
        ],
    )
    pivot_good, _ = DuckDBAdapter(pivot_contract).execute(
        pl.DataFrame({"customer": ["a", "a"], "status": ["paid", "open"], "amount": [3, 4]})
    )
    assert {"amount_paid", "amount_open"} <= set(pivot_good.columns)

    unpivot_contract = DataContract(
        version="1.0.0",
        transformations=[
            {
                "unpivot": {
                    "id_vars": ["customer"],
                    "value_vars": ["paid", "open"],
                    "key_field": "status",
                    "value_field": "amount",
                }
            }
        ],
    )
    unpivot_good, _ = DuckDBAdapter(unpivot_contract).execute(
        pl.DataFrame({"customer": ["a"], "paid": [3], "open": [4]})
    )
    assert sorted(unpivot_good["status"].to_list()) == ["open", "paid"]


def test_duckdb_adapter_links_and_register_df_variants(tmp_path: Path) -> None:
    pd = pytest.importorskip("pandas")
    parquet_path = tmp_path / "lookup.parquet"
    csv_path = tmp_path / "lookup.csv"
    pl.DataFrame({"id": [1], "name": ["alpha"], "unused": ["x"]}).write_parquet(parquet_path)
    pl.DataFrame({"code": ["A"], "label": ["active"]}).write_csv(csv_path)

    contract = DataContract(
        version="1.0.0",
        links=[
            {"name": "dim_customer", "path": str(parquet_path), "type": "parquet", "columns": ["id", "name"]},
            {"name": "dim_status", "path": str(csv_path), "type": "csv"},
            {"name": "missing", "path": str(tmp_path / "missing.parquet"), "type": "parquet"},
            {"name": "table_link", "path": "table:catalog.schema.table", "type": "table"},
            {"name": "unsupported", "path": str(csv_path), "type": "xlsx"},
        ],
        transformations=[
            {
                "sql": """
                SELECT s.id, d.name, st.label
                FROM source s
                LEFT JOIN dim_customer d ON s.id = d.id
                LEFT JOIN dim_status st ON s.status = st.code
                """
            }
        ],
    )
    adapter = DuckDBAdapter(contract)
    good, bad = adapter.execute(pl.DataFrame({"id": [1], "status": ["A"]}).lazy())

    assert bad.is_empty()
    assert good["name"].to_list() == ["alpha"]
    assert good["label"].to_list() == ["active"]

    class SparkLike:
        def __init__(self):
            self.columns = ("id",)

        def toPandas(self):
            return pd.DataFrame({"id": [1]})

    adapter._register_df("spark_like", SparkLike())
    assert adapter._get_columns(SparkLike()) == ["id"]
    assert adapter._get_columns(object()) == []

    adapter._register_df("pandas_like", pd.DataFrame({"id": [2]}))
    assert adapter.con.sql("SELECT id FROM pandas_like").fetchone()[0] == 2


def test_duckdb_adapter_parquet_directory_link_and_base_path_resolution(tmp_path: Path, monkeypatch) -> None:
    # Link paths are STORAGE references — resolved from CWD after the
    # registry expands {silver_path}/etc placeholders. _base_path is
    # explicitly NOT consulted for links (it's reserved for contract-local
    # files like external_logic.path). chdir to tmp_path so the CWD-relative
    # "links" glob lands where the fixture was written.
    monkeypatch.chdir(tmp_path)
    link_dir = tmp_path / "links"
    link_dir.mkdir()
    pl.DataFrame({"id": [1], "name": ["alpha"]}).write_parquet(link_dir / "part-0.parquet")

    contract = DataContract(
        version="1.0.0",
        links=[
            {"name": "dim", "path": "links", "type": "parquet"},
            {"name": "no_path"},
        ],
        transformations=[{"sql": "SELECT source.id, dim.name FROM source LEFT JOIN dim ON source.id = dim.id"}],
    )
    contract._base_path = tmp_path  # set, but engine no longer uses it for links

    good, bad = DuckDBAdapter(contract).execute(pl.DataFrame({"id": [1]}))

    assert bad.is_empty()
    assert good["name"].to_list() == ["alpha"]


def test_duckdb_adapter_sql_normalization_and_error_paths(monkeypatch: pytest.MonkeyPatch) -> None:
    normalized = DuckDBAdapter._normalize_spark_sql(
        "SELECT NOW(), CURRENT_TIMESTAMP, CURRENT_DATE, CAST(x AS STRING), "
        "timestamp_micros(ts), try_to_date(ds, 'yyyyMMdd')"
    )
    assert "TIMESTAMP" in normalized
    assert "CAST(x AS VARCHAR)" in normalized
    assert "make_timestamp(CAST(ts AS BIGINT))" in normalized
    assert "strptime(ds, '%Y%m%d')::DATE" in normalized

    adapter = DuckDBAdapter(DataContract(version="1.0.0"))
    with pytest.raises(Exception):
        adapter._apply_sql_transformation("source", "SELECT * FROM missing_table")

    adapter.dataset_rule_results = []
    adapter.con.sql("CREATE OR REPLACE VIEW source AS SELECT 1 AS id")
    adapter.get_dataset_rules = lambda: [
        types.SimpleNamespace(
            name="bad_rule",
            sql="SELECT missing FROM {dataset}",
            must_be_between=None,
            must_be_less_than=None,
            must_be_greater_than=None,
            description="bad",
        ),
        types.SimpleNamespace(
            name="greater",
            sql="SELECT COUNT(*) FROM {dataset}",
            must_be_between=None,
            must_be_less_than=None,
            must_be_greater_than=2,
            description=None,
        ),
        types.SimpleNamespace(
            name="less",
            sql="SELECT COUNT(*) FROM {dataset}",
            must_be_between=None,
            must_be_less_than=2,
            must_be_greater_than=None,
            description=None,
        ),
    ]

    adapter._run_dataset_rules("source")

    assert adapter.dataset_rule_results[-2]["passed"] is False
    assert adapter.dataset_rule_results[-1]["passed"] is True

    adapter.get_dataset_rules = lambda: [
        types.SimpleNamespace(
            name="none_rule",
            sql="SELECT NULL FROM {dataset}",
            must_be_between=None,
            must_be_less_than=None,
            must_be_greater_than=None,
            description=None,
        )
    ]
    adapter._run_dataset_rules("source")
    assert adapter.dataset_rule_results[-1]["passed"] is False


def test_duckdb_adapter_cast_to_string_and_transform_failure_branches(monkeypatch: pytest.MonkeyPatch) -> None:
    cast_contract = DataContract(
        version="1.0.0",
        server={"type": "local", "path": "memory", "cast_to_string": True},
        model={"fields": [{"name": "id", "type": "int"}]},
    )
    good, bad = DuckDBAdapter(cast_contract).execute(pl.DataFrame({"id": [1]}))
    assert bad.is_empty()
    assert good["id"].dtype == pl.String

    contract = DataContract(
        version="1.0.0",
        transformations=[
            {"sql": "SELECT * FROM source", "phase": "pre"},
            {"derive": {"field": "id", "sql": "missing + 1"}, "phase": "pre"},
            {"filter": "missing > 0", "phase": "pre"},
            {"sql": "SELECT * FROM source", "phase": "post"},
            {"derive": {"field": "id", "sql": "missing + 1"}, "phase": "post"},
            {"rollup": {"group_by": ["missing"], "aggregations": {"n": "COUNT(*)"}}, "phase": "post"},
            {
                "pivot": {"id_vars": ["missing"], "pivot_col": "id", "value_cols": ["id"], "values": [1]},
                "phase": "post",
            },
            {"unpivot": {"id_vars": ["id"], "value_vars": ["missing"]}, "phase": "post"},
        ],
    )
    adapter = DuckDBAdapter(contract)
    adapter._register_df("source", pl.DataFrame({"id": [1]}))

    original_apply = adapter._apply_sql_transformation
    monkeypatch.setattr(
        adapter,
        "_apply_sql_transformation",
        lambda table_name, sql: (_ for _ in ()).throw(RuntimeError("forced")),
    )
    assert adapter._apply_pre_transformations("source") == "source"
    assert adapter._apply_post_transformations("source") == "source"

    monkeypatch.setattr(adapter, "_apply_sql_transformation", original_apply)
    adapter.contract.transformations = [
        types.SimpleNamespace(
            phase="post",
            sql=None,
            derive=None,
            filter="missing > 0",
            rollup=None,
            pivot=None,
            unpivot=None,
        )
    ]
    assert adapter._apply_post_transformations("source") == "source"

    contract_ok = DataContract(
        version="1.0.0",
        transformations=[
            {"derive": {"field": "id", "sql": "id + 1"}, "phase": "post"},
        ],
    )
    ok_adapter = DuckDBAdapter(contract_ok)
    ok_adapter._register_df("source", pl.DataFrame({"id": [1]}))
    out = ok_adapter._apply_post_transformations("source")
    assert ok_adapter.con.sql(f"SELECT id FROM {out}").fetchone()[0] == 2


def test_duckdb_adapter_to_output_df_falls_back_to_pandas(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = DuckDBAdapter(DataContract(version="1.0.0"))

    class Relation:
        def pl(self):
            raise ImportError("no polars")

        def df(self):
            return "pandas-frame"

    assert adapter._to_output_df(Relation()) == "pandas-frame"
