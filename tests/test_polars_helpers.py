from __future__ import annotations

import sys
import types
from datetime import date

import polars as pl
import pytest

from lakelogic.core.models import DataContract
from lakelogic.engines.polars import PolarsAdapter


def test_polars_helper_sql_normalization_and_native_derives():
    adapter = PolarsAdapter(DataContract(version="1.0.0", dataset="orders"))

    normalized = adapter._normalize_sql(
        "SELECT NOW(), CURRENT_TIMESTAMP, CURRENT_DATE, CURRENT_TIME, CAST(id AS STRING), CAST(x AS LONG) FROM source"
    )
    assert "TIMESTAMP '" in normalized
    assert "DATE '" in normalized
    assert "TIME '" in normalized
    assert "AS VARCHAR" in normalized
    assert "AS BIGINT" in normalized

    lf = pl.DataFrame({"dt_text": ["20240131"], "ts_micros": [1700000000000000], "id": ["A"], "suffix": [7]}).lazy()  # placeholder
    date_lf = adapter._try_native_polars_derive("try_to_date(CAST(dt_text AS STRING), 'yyyyMMdd')", "parsed_date", lf)
    date_df = date_lf.collect()
    assert "parsed_date" in date_df.columns
    assert str(date_df["parsed_date"][0]) == "2024-01-31"

    ts_lf = adapter._try_native_polars_derive("timestamp_micros(ts_micros)", "parsed_ts", lf)
    ts_df = ts_lf.collect()
    assert "parsed_ts" in ts_df.columns
    assert ts_df.schema["parsed_ts"].time_unit == "us"

    concat_lf = adapter._try_native_polars_derive("CONCAT(id, '||', CAST(suffix AS VARCHAR))", "composite_key", lf)
    concat_df = concat_lf.collect()
    assert concat_df["composite_key"].to_list() == ["A||7"]
    assert adapter._try_native_polars_derive("complex_sql(x, y)", "ignored", lf) is None


def test_polars_helper_dtype_schema_and_join_sql():
    contract = DataContract(
        version="1.0.0",
        dataset="orders",
        model={
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "tags", "type": "string"},
                {"name": "missing_col", "type": "string"},
            ]
        },
        server={"type": "local", "path": "x", "schema_policy": {"unknown_fields": "drop", "evolution": "strict"}},
    )
    adapter = PolarsAdapter(contract)

    lf = pl.DataFrame({"id": ["1"], "tags": [["a", "b"]], "extra": ["x"], "_lakelogic_run_id": ["r1"]}).lazy()
    casted_lf, schema_errors = adapter._apply_schema(lf)
    casted = casted_lf.collect()
    assert casted.schema["id"] == pl.Int64
    assert casted.schema["tags"] == pl.Utf8
    assert casted["tags"].to_list() == ['["a", "b"]']
    assert "extra" not in casted.columns
    assert "missing_col" in casted.columns
    assert any("Missing fields" in error for error in schema_errors)
    assert adapter.schema_drift["unknown_fields"] == ["extra"]

    ingest_contract = DataContract(
        version="1.0.0",
        dataset="orders",
        server={"type": "local", "path": "x", "mode": "ingest", "cast_to_string": True},
    )
    ingest_adapter = PolarsAdapter(ingest_contract)
    ingest_lf, ingest_errors = ingest_adapter._apply_schema(pl.DataFrame({"id": [1], "amount": [2.5]}).lazy())
    ingest_df = ingest_lf.collect()
    assert ingest_errors == []
    assert ingest_df.schema["id"] == pl.Utf8
    assert ingest_df.schema["amount"] == pl.Utf8

    assert adapter._to_polars_dtype("string") == pl.Utf8
    assert adapter._to_polars_dtype("boolean") == pl.Boolean
    assert adapter._to_polars_dtype("unknown") is None
    assert adapter._format_sql_literal("O'Brien") == "'O''Brien'"

    join_cfg = types.SimpleNamespace(
        type="full",
        fields=["status", "tier"],
        prefix="ref_",
        defaults={"status": "missing"},
        reference="ref_table",
        on="customer_id",
        key="id",
    )
    join_sql = adapter._build_join_sql(join_cfg)
    assert "FULL OUTER JOIN ref_table ref" in join_sql
    assert "COALESCE(ref.status, 'missing') AS ref_status" in join_sql
    assert "ref.tier AS ref_tier" in join_sql


def test_polars_helper_register_links_handles_projection_cache_and_warnings(monkeypatch, tmp_path):
    csv_path = tmp_path / "lookup.csv"
    pl.DataFrame({"id": [1], "name": ["A"], "extra": ["x"]}).write_csv(csv_path)
    parquet_path = tmp_path / "lookup.parquet"
    pl.DataFrame({"id": [2], "amount": [9.5]}).write_parquet(parquet_path)
    txt_path = tmp_path / "lookup.txt"
    txt_path.write_text("id|name\n1|A\n", encoding="utf-8")

    contract = types.SimpleNamespace(
        dataset="orders",
        _base_path=tmp_path,
        metadata={"cache_reference_links": True},
        links=[
            types.SimpleNamespace(name="table_ref", table="catalog.orders", type=None, path=None, columns=None),
            types.SimpleNamespace(
                name="remote_ref", table=None, type=None, path="abfss://container/ref.csv", columns=None
            ),
            types.SimpleNamespace(name="missing_ref", table=None, type=None, path="missing.csv", columns=None),
            types.SimpleNamespace(name="csv_ref", table=None, type=None, path="lookup.csv", columns=["id", "name"]),
            types.SimpleNamespace(name="pq_ref", table=None, type=None, path=str(parquet_path), columns=None),
            types.SimpleNamespace(name="bad_ref", table=None, type=None, path="lookup.txt", columns=None),
        ],
    )
    adapter = PolarsAdapter(contract)
    adapter._link_cache.clear()

    class FakeContext:
        def __init__(self):
            self.registered = []

        def register(self, name, frame):
            self.registered.append((name, frame))

    warnings = []
    debugs = []
    monkeypatch.setattr("lakelogic.engines.polars.logger.warning", warnings.append)
    monkeypatch.setattr("lakelogic.engines.polars.logger.debug", debugs.append)

    first_ctx = FakeContext()
    adapter._register_links(first_ctx)
    second_ctx = FakeContext()
    adapter._register_links(second_ctx)

    first_registered = {name: frame for name, frame in first_ctx.registered}
    second_registered = {name: frame for name, frame in second_ctx.registered}
    assert set(first_registered) == {"csv_ref", "pq_ref"}
    assert first_registered["csv_ref"].collect().columns == ["id", "name"]
    assert second_registered["csv_ref"] is first_registered["csv_ref"]
    assert "csv_ref:" in next(iter(adapter._link_cache))
    assert any("references table" in message for message in warnings)
    assert any("Failed to load remote link" in message for message in warnings)
    assert any("Link file not found" in message for message in warnings)
    assert any("Unsupported link format" in message for message in warnings)
    assert any("projected to 2 columns" in message for message in debugs)


def test_polars_helper_apply_sql_transformation_replaces_duplicate_alias(monkeypatch):
    adapter = PolarsAdapter(types.SimpleNamespace(dataset="orders", links=[], metadata={}))
    lf = pl.DataFrame({"id": [1, 2], "status": ["new", "done"]}).lazy()

    transformed = adapter._apply_sql_transformation(lf, "SELECT *, id + 1 AS next_id FROM source").collect()
    assert transformed["next_id"].to_list() == [2, 3]
    assert transformed["status"].to_list() == ["new", "done"]

    selected = adapter._apply_sql_transformation(
        lf,
        "SELECT status, id + 2 AS shifted_id FROM source",
    ).collect()
    assert selected.columns == ["status", "shifted_id"]
    assert selected["shifted_id"].to_list() == [3, 4]


def test_polars_helper_run_dataset_rules_evaluates_thresholds_and_errors(monkeypatch):
    adapter = PolarsAdapter(types.SimpleNamespace(dataset="orders", quality=None))
    adapter.get_dataset_rules = lambda: [
        types.SimpleNamespace(
            name="between",
            sql="between_sql",
            must_be_between=[1, 5],
            must_be_less_than=None,
            must_be_greater_than=None,
            description="between",
        ),
        types.SimpleNamespace(
            name="less_than",
            sql="less_sql",
            must_be_between=None,
            must_be_less_than=10,
            must_be_greater_than=None,
            description="lt",
        ),
        types.SimpleNamespace(
            name="greater_than",
            sql="greater_sql",
            must_be_between=None,
            must_be_less_than=None,
            must_be_greater_than=3,
            description="gt",
        ),
        types.SimpleNamespace(
            name="null_rule",
            sql="null_sql",
            must_be_between=None,
            must_be_less_than=None,
            must_be_greater_than=None,
            description="null",
        ),
        types.SimpleNamespace(
            name="broken",
            sql="broken_sql",
            must_be_between=None,
            must_be_less_than=None,
            must_be_greater_than=None,
            description="broken",
        ),
    ]

    class FakeScalarFrame:
        def __init__(self, value):
            self.value = value

        def row(self, index):
            return (self.value,)

    class FakeResult:
        def __init__(self, value):
            self.value = value

        def collect(self):
            return FakeScalarFrame(self.value)

    class FakeContext:
        def __init__(self):
            self.register_calls = []

        def register(self, name, frame):
            self.register_calls.append((name, frame))

        def execute(self, sql):
            values = {
                "between_sql": 3,
                "less_sql": 8,
                "greater_sql": 2,
                "null_sql": None,
            }
            if sql == "broken_sql":
                raise RuntimeError("boom")
            return FakeResult(values[sql])

    infos = []
    errors = []
    monkeypatch.setattr("lakelogic.engines.polars.logger.info", infos.append)
    monkeypatch.setattr("lakelogic.engines.polars.logger.error", errors.append)

    ctx = FakeContext()
    adapter._run_dataset_rules(pl.DataFrame({"id": [1]}).lazy(), ctx)

    assert ctx.register_calls[0][0] == "orders"
    assert len(adapter.dataset_rule_results) == 4
    assert adapter.dataset_rule_results[0]["passed"] is True
    assert adapter.dataset_rule_results[2]["passed"] is False
    assert adapter.dataset_rule_results[3]["passed"] is False
    assert any("Quality Check: between" in message for message in infos)
    assert any("broken" in message for message in errors)


def test_polars_helper_apply_sql_transformation_duckdb_fallback_and_strict_mode(monkeypatch):
    adapter = PolarsAdapter(types.SimpleNamespace(dataset="orders", links=[], metadata={}, contract=None))
    lf = pl.DataFrame({"id": [1, 2], "status": ["new", "done"]}).lazy()
    monkeypatch.setattr(pl.DataFrame, "to_arrow", lambda self: self)

    class BrokenSQLContext:
        def register(self, name, frame):
            return None

        def execute(self, sql):
            raise RuntimeError("polars sql failed")

    class FakeDuckRelation:
        def df(self):
            import pandas as pd

            return pd.DataFrame({"id": [1, 2], "status": ["new", "done"], "next_id": [2, 3]})

        def pl(self):
            return pl.DataFrame({"id": [1, 2], "status": ["new", "done"], "next_id": [2, 3]})

    class FakeDuckConnection:
        def register(self, name, frame):
            return None

        def execute(self, sql):
            return None

        def query(self, sql):
            return FakeDuckRelation()

        def close(self):
            return None

    monkeypatch.setattr(pl, "SQLContext", BrokenSQLContext)
    monkeypatch.setitem(
        sys.modules, "duckdb", types.SimpleNamespace(connect=lambda database=":memory:": FakeDuckConnection())
    )
    fallback = adapter._apply_sql_transformation(lf, "SELECT *, id + 1 AS next_id FROM source").collect()
    assert fallback["next_id"].to_list() == [2, 3]

    monkeypatch.setenv("LAKELOGIC_STRICT_POLARS", "true")
    with pytest.raises(RuntimeError, match="polars sql failed"):
        adapter._apply_sql_transformation(lf, "SELECT * FROM source")
    monkeypatch.delenv("LAKELOGIC_STRICT_POLARS", raising=False)


def test_polars_helper_pre_transformations_cover_column_operations():
    contract = DataContract(
        version="1.0.0",
        dataset="orders",
        transformations=[
            {"rename": {"mappings": {"Name": "name"}}},
            {"json_extract": {"source": "payload", "path": "$.city", "field": "city"}, "phase": "pre"},
            {"derive": {"field": "derived", "sql": "CONCAT(name, '-x')"}, "phase": "pre"},
            {"cast": {"columns": {"id": "integer"}}},
            {"trim": {"fields": ["name"], "side": "both"}},
            {"lower": {"fields": ["name"]}},
            {"upper": {"fields": ["code"]}},
            {
                "coalesce": {
                    "field": "nickname",
                    "sources": ["nickname", "name"],
                    "output": "display_name",
                    "default": "missing",
                }
            },
            {"split": {"field": "tags", "delimiter": "|", "output": "tag_list"}},
            {"explode": {"field": "tag_list"}},
            {
                "map_values": {
                    "field": "status",
                    "mapping": {"A": "active"},
                    "default": "other",
                    "output": "status_norm",
                }
            },
            {"filter": {"sql": "id > 1"}},
            {"deduplicate": {"on": ["id", "tag_list"], "sort_by": ["code"], "order": "desc"}},
        ],
    )
    adapter = PolarsAdapter(contract)
    lf = pl.DataFrame(
        {
            "id": [1, 2, 2],
            "Name": [" Alice ", " Bob ", " Bob "],
            "nickname": [None, None, None],
            "code": ["ab", "xy", "xy"],
            "tags": ["a|b", "c|d", "c|d"],
            "status": ["A", "B", "B"],
            "payload": ['{"city":"Paris"}', '{"city":"Berlin"}', '{"city":"Berlin"}'],
        }
    ).lazy()

    transformed = adapter._apply_pre_transformations(lf).collect().sort(["id", "tag_list"])
    assert transformed["id"].to_list() == [2, 2]
    assert transformed["name"].to_list() == ["bob", "bob"]
    assert transformed["code"].to_list() == ["XY", "XY"]
    assert transformed["display_name"].to_list() == [" Bob ", " Bob "] or transformed["display_name"].to_list() == [
        "bob",
        "bob",
    ]
    assert transformed["tag_list"].to_list() == ["c", "d"]
    assert transformed["status_norm"].to_list() == ["other", "other"]
    assert transformed["city"].to_list() == ["Berlin", "Berlin"]
    assert transformed["derived"].to_list() == [" Bob -x", " Bob -x"] or transformed["derived"].to_list() == [
        "bob-x",
        "bob-x",
    ]


def test_polars_helper_post_transformations_cover_sql_lookup_join_and_date_ranges(monkeypatch):
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
            "json_extract": None,
            "date_range_explode": None,
            "lookup": None,
            "join": None,
            "filter": None,
        }
        base.update(kwargs)
        return types.SimpleNamespace(**base)

    contract = types.SimpleNamespace(
        dataset="orders",
        links=[],
        metadata={},
        transformations=[
            _transformation(sql="SELECT *, amount + 1 AS amount_plus_one FROM {source}"),
            _transformation(rollup=types.SimpleNamespace(name="rollup")),
            _transformation(pivot=types.SimpleNamespace(name="pivot")),
            _transformation(unpivot=types.SimpleNamespace(name="unpivot")),
            _transformation(bucket=types.SimpleNamespace(field="bucketed")),
            _transformation(date_diff=types.SimpleNamespace(field="days_open")),
            _transformation(
                json_extract=types.SimpleNamespace(source="payload", path="$.city", field="city", cast=None)
            ),
            _transformation(
                date_range_explode=types.SimpleNamespace(
                    start_col="start_date",
                    end_col="end_date",
                    output="active_date",
                    interval="1d",
                )
            ),
            _transformation(
                lookup=types.SimpleNamespace(
                    field="status_name",
                    reference="status_ref",
                    on="status_id",
                    key="id",
                    value="name",
                )
            ),
            _transformation(join=types.SimpleNamespace(reference="segment_ref")),
            _transformation(filter=types.SimpleNamespace(sql="status_name IS NOT NULL")),
        ],
    )
    adapter = PolarsAdapter(contract)
    monkeypatch.setattr(adapter, "_build_rollup_sql", lambda cfg, source_table="orders": "SELECT * FROM orders")
    monkeypatch.setattr(adapter, "_build_pivot_sql", lambda cfg, source_table="orders": "SELECT * FROM orders")
    monkeypatch.setattr(adapter, "_build_unpivot_sql", lambda cfg, source_table="orders": "SELECT * FROM orders")
    monkeypatch.setattr(
        adapter,
        "_build_bucket_sql",
        lambda cfg, source_table="orders": (
            "SELECT *, CASE WHEN amount > 10 THEN 'high' ELSE 'low' END AS bucketed FROM orders"
        ),
    )
    monkeypatch.setattr(
        adapter,
        "_build_date_diff_sql",
        lambda cfg, source_table="orders": "SELECT *, 2 AS days_open FROM orders",
    )
    monkeypatch.setattr(
        adapter,
        "_build_join_sql",
        lambda join_cfg, tbl_name="orders": (
            "SELECT src.*, ref.segment AS segment FROM orders src LEFT JOIN segment_ref ref ON src.customer_id = ref.id"
        ),
    )

    lf = pl.DataFrame(
        {
            "id": [1, 2],
            "status_id": [1, 2],
            "customer_id": [10, 20],
            "amount": [5, 20],
            "payload": ['{"city":"Paris"}', '{"city":"Berlin"}'],
            "start_date": ["2024-01-01", "2024-01-03"],
            "end_date": ["2024-01-02", "2024-01-03"],
        }
    ).lazy()
    ctx = pl.SQLContext()
    ctx.register("orders", lf)
    ctx.register("source", lf)
    ctx.register("status_ref", pl.DataFrame({"id": [1, 2], "name": ["active", "inactive"]}).lazy())
    ctx.register("segment_ref", pl.DataFrame({"id": [10, 20], "segment": ["vip", "std"]}).lazy())

    transformed = adapter._apply_post_transformations(lf, ctx).collect().sort(["id", "active_date"])

    assert "amount_plus_one" in transformed.columns
    assert "bucketed" in transformed.columns
    assert "days_open" in transformed.columns
    assert transformed["city"].to_list() == ["Paris", "Paris", "Berlin"]
    assert transformed["status_name"].to_list() == ["active", "active", "inactive"]
    assert transformed["segment"].to_list() == ["vip", "vip", "std"]
    assert transformed["active_date"].to_list() == [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)]


def test_polars_helper_post_transformations_date_range_and_derive_failure_paths(monkeypatch):
    warnings_seen = []
    monkeypatch.setattr("warnings.warn", lambda message, stacklevel=2: warnings_seen.append(str(message)))

    class BrokenContext:
        def register(self, name, frame):
            return None

        def execute(self, sql):
            raise RuntimeError("sql failed")

    monkeypatch.setattr(pl, "SQLContext", BrokenContext)

    contract = types.SimpleNamespace(
        dataset="orders",
        links=[],
        metadata={},
        transformations=[
            types.SimpleNamespace(
                phase="post",
                sql=None,
                rollup=None,
                pivot=None,
                unpivot=None,
                derive=types.SimpleNamespace(field="broken_field", sql="UNSUPPORTED(expr)"),
                bucket=None,
                date_diff=None,
                json_extract=None,
                date_range_explode=None,
                lookup=None,
                join=None,
                filter=None,
            ),
            types.SimpleNamespace(
                phase="post",
                sql=None,
                rollup=None,
                pivot=None,
                unpivot=None,
                derive=None,
                bucket=None,
                date_diff=None,
                json_extract=None,
                date_range_explode=types.SimpleNamespace(
                    start_col="start_date",
                    end_col="end_date",
                    output="active_date",
                    interval="1d",
                ),
                lookup=None,
                join=None,
                filter=None,
            ),
        ],
    )
    adapter = PolarsAdapter(contract)
    monkeypatch.setattr(adapter, "_transpile_derive_sql", lambda derive: "UNSUPPORTED(expr)")
    monkeypatch.setattr(adapter, "_try_native_polars_derive", lambda raw_sql, field_name, lf: None)
    monkeypatch.setitem(
        sys.modules,
        "duckdb",
        types.SimpleNamespace(
            connect=lambda database=":memory:": types.SimpleNamespace(
                register=lambda *args, **kwargs: None,
                execute=lambda sql: (_ for _ in ()).throw(RuntimeError("duckdb failed")),
            )
        ),
    )

    lf = pl.DataFrame(
        {
            "id": [1, 2],
            "start_date": ["2024-01-05", "bad-date"],
            "end_date": ["2024-01-03", None],
        }
    ).lazy()

    transformed = adapter._apply_post_transformations(lf, BrokenContext()).collect()

    assert transformed["broken_field"].to_list() == [None, None]
    assert any("FAILED all engines" in message for message in warnings_seen)
    assert transformed["active_date"].len() == 2
