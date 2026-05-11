from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

pl = pytest.importorskip("polars")

from lakelogic.core import materialization as mat
from lakelogic.core import quarantine as q


def _contract(**overrides):
    base = types.SimpleNamespace(
        quarantine=types.SimpleNamespace(target="quarantine/out", table=None, format=None, write_mode=None),
        metadata={},
        materialization=types.SimpleNamespace(format="parquet"),
        dataset="orders",
        info=types.SimpleNamespace(table_name="orders", title="Orders"),
        source=types.SimpleNamespace(path="landing/orders.csv"),
        lineage=None,
        _base_path=None,
    )
    for key, value in overrides.items():
        setattr(base, key, value)
    return base


def test_quarantine_backend_defaults_and_dispatch(monkeypatch, tmp_path):
    assert q._default_quarantine_db(tmp_path, "duckdb") == tmp_path / ".lakelogic" / "quarantine.duckdb"
    assert q._default_quarantine_db(tmp_path, "sqlite") == tmp_path / ".lakelogic" / "quarantine.sqlite"
    assert q._normalize_quarantine_backend({}, "polars") == "duckdb"
    assert q._normalize_quarantine_backend({}, "spark") == "spark"
    assert q._normalize_quarantine_backend({"quarantine_table_backend": "sqlite"}, None) == "sqlite"
    assert q._prepare_table_name("schema.table", "sqlite") == "schema_table"
    assert q._prepare_table_name("schema.table", "duckdb") == "schema.table"

    calls = []
    monkeypatch.setattr(q, "_write_quarantine_table_duckdb", lambda *args: calls.append("duckdb") or {"target": "duck"})
    monkeypatch.setattr(
        q, "_write_quarantine_table_sqlite", lambda *args: calls.append("sqlite") or {"target": "sqlite"}
    )
    monkeypatch.setattr(q, "_write_quarantine_table_spark", lambda *args: calls.append("spark") or {"target": "spark"})

    contract = _contract(metadata={"quarantine_table_backend": "duckdb"})
    assert q._write_quarantine_table(object(), contract, "events", engine_name="polars") == {"target": "duck"}
    contract.metadata["quarantine_table_backend"] = "sqlite"
    assert q._write_quarantine_table(object(), contract, "events", engine_name="polars") == {"target": "sqlite"}
    contract.metadata["quarantine_table_backend"] = "spark"
    assert q._write_quarantine_table(object(), contract, "events", engine_name="polars") == {"target": "spark"}
    contract.metadata["quarantine_table_backend"] = "unsupported"
    assert q._write_quarantine_table(object(), contract, "events", engine_name="polars") == {}
    assert calls == ["duckdb", "sqlite", "spark"]


def test_stamp_quarantine_lineage_and_fallback(monkeypatch):
    df = pl.DataFrame({"id": [1]})
    lineage_calls = []
    fake_lineage_module = types.ModuleType("lakelogic.core.lineage")
    fake_lineage_module.add_columns = lambda frame, values, engine_name=None: (
        lineage_calls.append((values, engine_name)) or {"engine": engine_name, "values": values}
    )
    monkeypatch.setitem(sys.modules, "lakelogic.core.lineage", fake_lineage_module)

    contract = _contract(
        metadata={"domain": "commerce", "system": "erp", "lineage": {"run_id_column_name": "run_id_col"}}
    )
    stamped = q._stamp_quarantine_lineage(df, contract, run_id="run-1")
    assert stamped["engine"] == "polars"
    assert stamped["values"]["run_id_col"] == "run-1"
    assert stamped["values"]["_lakelogic_contract_name"] == "Orders"

    monkeypatch.setitem(sys.modules, "lakelogic.core.lineage", types.ModuleType("lakelogic.core.lineage"))
    monkeypatch.setattr(q.logger, "warning", lambda message: lineage_calls.append(("warning", message)))
    assert q._stamp_quarantine_lineage(df, contract) is df
    assert any(item[0] == "warning" for item in lineage_calls if isinstance(item, tuple))


def test_materialize_quarantine_main_branches(monkeypatch, tmp_path):
    df = pl.DataFrame({"id": [1], "value": ["x"]})
    empty_df = pl.DataFrame({"id": [], "value": []})
    no_cols_df = pl.DataFrame([])

    monkeypatch.setattr(q, "_stamp_quarantine_lineage", lambda frame, contract: frame)
    monkeypatch.setattr(q, "_frame_has_columns", lambda frame: len(getattr(frame, "columns", [])) > 0)
    monkeypatch.setattr(
        q, "_row_count", lambda frame: frame.height if hasattr(frame, "height") else None, raising=False
    )
    monkeypatch.setattr(q, "_resolve_path", lambda raw, base: Path(base or tmp_path) / raw)
    monkeypatch.setattr(q, "_is_polars_frame", lambda frame: isinstance(frame, pl.DataFrame))
    monkeypatch.setattr(q, "_append_without_pandas", lambda frame, path, fmt: 2)
    monkeypatch.setattr(q, "_read_frame", lambda path, fmt: None)
    monkeypatch.setattr(q, "_pandas_available", lambda: False)

    writes = []

    def fake_write_frame(frame, path, fmt):
        path_obj = Path(path)
        path_obj.parent.mkdir(parents=True, exist_ok=True)
        path_obj.write_text("written", encoding="utf-8")
        writes.append((path_obj, fmt, getattr(frame, "height", None)))

    monkeypatch.setattr(q, "_write_frame", fake_write_frame)
    table_calls = []
    monkeypatch.setattr(
        q,
        "_write_quarantine_table",
        lambda frame, contract, table_name, engine_name=None: (
            table_calls.append((table_name, engine_name)) or {"target": table_name, "rows_written": 1}
        ),
    )

    assert q.materialize_quarantine(df, None) == {}
    assert q.materialize_quarantine(df, types.SimpleNamespace(quarantine=None)) == {}

    stamped_contract = _contract(lineage=types.SimpleNamespace(run_id_column_name="run_id_col"))
    stamped_df = pl.DataFrame({"id": [1], "run_id_col": ["existing"]})
    result = q.materialize_quarantine(stamped_df, stamped_contract, target_path=tmp_path / "override")
    assert result["target"].replace("\\", "/").endswith("override/orders.parquet")

    empty_result = q.materialize_quarantine(empty_df, _contract(), target_path=tmp_path / "empty")
    assert empty_result["rows_written"] == 0

    table_contract = _contract(
        quarantine=types.SimpleNamespace(
            target="unused", table="catalog.quarantine_orders", format=None, write_mode=None
        )
    )
    assert q.materialize_quarantine(df, table_contract, quarantine_mode="table", engine_name="polars") == {
        "target": "catalog.quarantine_orders",
        "rows_written": 1,
    }

    fallback_contract = _contract(
        quarantine=types.SimpleNamespace(target="quarantine/out", table=None, format=None, write_mode=None)
    )
    assert q.materialize_quarantine(df, fallback_contract, quarantine_mode="table", engine_name="polars")[
        "target"
    ].endswith("orders.parquet")

    unresolved_contract = _contract(
        quarantine=types.SimpleNamespace(target="{quarantine_path}", table=None, format=None, write_mode=None)
    )
    with pytest.raises(ValueError, match="not fully resolved"):
        q.materialize_quarantine(df, unresolved_contract)

    file_contract = _contract(_base_path=tmp_path)
    overwrite = q.materialize_quarantine(df, file_contract, output_format="csv")
    append = q.materialize_quarantine(df, file_contract, output_format="csv")
    assert overwrite["format"] == "csv"
    assert append["rows_written"] == 2
    assert any(item[1] == "csv" for item in writes)

    no_cols_result = q.materialize_quarantine(no_cols_df, _contract(_base_path=tmp_path), output_format="csv")
    assert no_cols_result["rows_written"] == 0


def test_materialize_quarantine_table_prefix_and_spark_path(monkeypatch, tmp_path):
    table_calls = []
    monkeypatch.setattr(
        q,
        "_write_quarantine_table",
        lambda frame, contract, table_name, engine_name=None: (
            table_calls.append((table_name, engine_name)) or {"target": table_name}
        ),
    )
    monkeypatch.setattr(q, "_stamp_quarantine_lineage", lambda frame, contract: frame)
    monkeypatch.setattr(q, "_frame_has_columns", lambda frame: True)
    monkeypatch.setattr(q, "_row_count", lambda frame: None, raising=False)

    table_target_contract = _contract(
        quarantine=types.SimpleNamespace(
            target="table:catalog.quarantine_orders", table=None, format=None, write_mode=None
        )
    )
    assert q.materialize_quarantine(pl.DataFrame({"id": [1]}), table_target_contract, engine_name="duckdb") == {
        "target": "catalog.quarantine_orders"
    }
    assert table_calls == [("catalog.quarantine_orders", "duckdb")]

    write_events = []

    class FakeWriter:
        def __init__(self):
            self.fmt = None
            self.mode_value = None
            self.options = {}

        def format(self, fmt):
            self.fmt = fmt
            return self

        def mode(self, mode):
            self.mode_value = mode
            return self

        def option(self, key, value):
            self.options[key] = value
            return self

        def save(self, path):
            write_events.append((self.fmt, self.mode_value, dict(self.options), path))

    spark_df = types.SimpleNamespace(write=FakeWriter(), count=lambda: 3, columns=["id"], sparkSession=object())
    spark_contract = _contract(
        _base_path=tmp_path,
        quarantine=types.SimpleNamespace(target="spark_quarantine", table=None, format="json", write_mode=None),
        metadata={"quarantine_table_mode": "append"},
    )
    result = q.materialize_quarantine(spark_df, spark_contract, engine_name="spark")
    assert result["rows_written"] == 3
    assert write_events[0][0] == "json"
    assert write_events[0][2]["header"] == "true"


def test_quarantine_table_duckdb_and_sqlite_writers(monkeypatch, tmp_path):
    pd = pytest.importorskip("pandas")
    pdf = pd.DataFrame({"id": [1], "new_col": ["x"]})
    monkeypatch.setattr(q, "_to_pandas", lambda frame: pdf)

    executed = []

    class FakeDuckConnection:
        def execute(self, stmt):
            executed.append(stmt)
            return self

        def register(self, name, frame):
            executed.append((name, list(frame.columns)))

        def fetchall(self):
            return [("id",)]

        def close(self):
            executed.append("closed")

    monkeypatch.setitem(sys.modules, "duckdb", types.SimpleNamespace(connect=lambda database: FakeDuckConnection()))
    contract = _contract(_base_path=tmp_path, metadata={})
    duck_result = q._write_quarantine_table_duckdb(pdf, contract, "analytics.quarantine_orders", {})
    assert duck_result["rows_written"] == 1
    assert duck_result["format"] == "duckdb"
    assert any(
        'ALTER TABLE analytics.quarantine_orders ADD COLUMN IF NOT EXISTS "new_col" VARCHAR' in str(stmt)
        for stmt in executed
    )

    sqlite_contract = _contract(_base_path=tmp_path, metadata={})
    sqlite_result = q._write_quarantine_table_sqlite(pdf, sqlite_contract, "analytics.quarantine_orders", {})
    assert sqlite_result["rows_written"] == 1
    assert sqlite_result["format"] == "sqlite"
    assert q._default_quarantine_db(tmp_path, "sqlite").exists()


def test_quarantine_table_snowflake_and_bigquery_writers(monkeypatch):
    pd = pytest.importorskip("pandas")
    pdf = pd.DataFrame({"id": [1], "email": ["a@example.com"]})
    monkeypatch.setattr(q, "_to_pandas", lambda frame: pdf)
    monkeypatch.setattr(q, "_resolve_env_value", lambda value: value)

    snowflake_calls = []

    class FakeSnowflakeConnection:
        def close(self):
            snowflake_calls.append("closed")

    connector_module = types.ModuleType("snowflake.connector")
    connector_module.connect = lambda **kwargs: snowflake_calls.append(kwargs) or FakeSnowflakeConnection()
    pandas_tools_module = types.ModuleType("snowflake.connector.pandas_tools")
    pandas_tools_module.write_pandas = (
        lambda conn, frame, table_name, database=None, schema=None, auto_create_table=True, overwrite=False: (
            snowflake_calls.append((table_name, database, schema, len(frame)))
        )
    )
    snowflake_root = types.ModuleType("snowflake")
    snowflake_root.connector = connector_module
    monkeypatch.setitem(sys.modules, "snowflake", snowflake_root)
    monkeypatch.setitem(sys.modules, "snowflake.connector", connector_module)
    monkeypatch.setitem(sys.modules, "snowflake.connector.pandas_tools", pandas_tools_module)

    metadata = {
        "snowflake_account": "acct",
        "snowflake_user": "user",
        "snowflake_password": "secret",
        "snowflake_warehouse": "wh",
    }
    snowflake_result = q._write_quarantine_table_snowflake(
        pdf, _contract(metadata=metadata), "raw.audit.quarantine_orders", metadata
    )
    assert snowflake_result["target"] == "raw.audit.quarantine_orders"
    assert any(isinstance(call, tuple) and call[0] == "quarantine_orders" for call in snowflake_calls)

    bq_calls = []

    class FakeLoadJob:
        def result(self):
            bq_calls.append("result")

    class FakeBigQueryClient:
        def __init__(self, project=None):
            self.project = project

        def load_table_from_dataframe(self, frame, table_id, job_config=None):
            bq_calls.append((self.project, table_id, job_config.write_disposition, len(frame)))
            return FakeLoadJob()

    class FakeLoadJobConfig:
        def __init__(self, write_disposition=None, create_disposition=None, autodetect=None):
            self.write_disposition = write_disposition
            self.create_disposition = create_disposition
            self.autodetect = autodetect

    google_root = types.ModuleType("google")
    cloud_module = types.ModuleType("google.cloud")
    bigquery_module = types.ModuleType("google.cloud.bigquery")
    bigquery_module.Client = FakeBigQueryClient
    bigquery_module.LoadJobConfig = FakeLoadJobConfig
    cloud_module.bigquery = bigquery_module
    google_root.cloud = cloud_module
    monkeypatch.setitem(sys.modules, "google", google_root)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_module)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", bigquery_module)

    bq_result = q._write_quarantine_table_bigquery(
        pdf,
        _contract(metadata={"bigquery_project": "demo"}),
        "analytics.quarantine_orders",
        {"bigquery_project": "demo"},
    )
    assert bq_result["target"] == "demo.analytics.quarantine_orders"
    assert ("demo", "demo.analytics.quarantine_orders", "WRITE_APPEND", 1) in bq_calls


def test_quarantine_table_iceberg_writer(monkeypatch):
    pa = pytest.importorskip("pyarrow")

    class FakeIcebergTable:
        def __init__(self):
            self.appended = []

        def append(self, arrow_table):
            self.appended.append(arrow_table.num_rows)

    class FakeCatalog:
        def __init__(self):
            self.table = FakeIcebergTable()
            self.created = []

        def load_table(self, name):
            raise RuntimeError("missing")

        def create_table(self, name, schema=None):
            self.created.append((name, schema))
            return self.table

    fake_catalog = FakeCatalog()
    catalog_module = types.ModuleType("pyiceberg.catalog")
    catalog_module.load_catalog = lambda name, **kwargs: fake_catalog
    pyiceberg_root = types.ModuleType("pyiceberg")
    pyiceberg_root.catalog = catalog_module
    monkeypatch.setitem(sys.modules, "pyiceberg", pyiceberg_root)
    monkeypatch.setitem(sys.modules, "pyiceberg.catalog", catalog_module)

    class FakePolarsFrame:
        def to_arrow(self):
            return pa.table({"id": [1], "reason": ["bad"]})

    result = q._write_quarantine_table_iceberg(FakePolarsFrame(), _contract(metadata={}), "audit.quarantine_orders", {})
    assert result["target"] == "audit.quarantine_orders"
    assert result["rows_written"] == 1
    assert fake_catalog.created[0][0] == "audit.quarantine_orders"


def test_write_quarantine_table_spark_variants_and_guard(monkeypatch):
    commands = []
    save_calls = []

    class FakeWriter:
        def __init__(self):
            self.mode_value = None
            self.format_value = None
            self.options = {}

        def mode(self, mode):
            self.mode_value = mode
            return self

        def format(self, fmt):
            self.format_value = fmt
            return self

        def option(self, key, value):
            self.options[key] = value
            return self

        def saveAsTable(self, name):
            save_calls.append((name, self.mode_value, self.format_value, dict(self.options)))

    class FakeSparkSession:
        def sql(self, statement):
            commands.append(statement)

    spark_df = types.SimpleNamespace(write=FakeWriter(), sparkSession=FakeSparkSession(), count=lambda: 4)
    delta_result = q._write_quarantine_table_spark(
        spark_df,
        _contract(metadata={"quarantine_table_format": "delta", "quarantine_table_mode": "overwrite"}),
        "analytics.quarantine_orders",
        {"quarantine_table_format": "delta", "quarantine_table_mode": "overwrite"},
    )
    assert delta_result["rows_written"] == 4
    assert commands == ["CREATE DATABASE IF NOT EXISTS analytics"]
    assert save_calls[0][3]["mergeSchema"] == "true"

    commands.clear()
    save_calls.clear()
    iceberg_result = q._write_quarantine_table_spark(
        spark_df,
        _contract(metadata={"quarantine_table_format": "iceberg"}),
        "catalog.analytics.quarantine_orders",
        {"quarantine_table_format": "iceberg"},
    )
    assert iceberg_result["format"] == "iceberg"
    assert commands == ["CREATE SCHEMA IF NOT EXISTS catalog.analytics"]
    assert save_calls[0][3]["merge-schema"] == "true"

    with pytest.raises(ValueError, match="Spark DataFrame"):
        q._write_quarantine_table_spark(object(), _contract(metadata={}), "audit.quarantine", {})


def test_materialize_quarantine_delta_and_iceberg_file_modes(monkeypatch, tmp_path):
    pa = pytest.importorskip("pyarrow")
    monkeypatch.setattr(q, "_stamp_quarantine_lineage", lambda frame, contract: frame)
    monkeypatch.setattr(q, "_frame_has_columns", lambda frame: True)
    monkeypatch.setattr(q, "_resolve_path", lambda raw, base: Path(base or tmp_path) / raw)

    delta_calls = []
    writer_module = types.ModuleType("deltalake.writer")
    writer_module.write_deltalake = lambda path, data, **kwargs: delta_calls.append(
        (path, data.num_rows, kwargs.copy())
    )
    deltalake_module = types.ModuleType("deltalake")
    deltalake_module.DeltaTable = lambda path, storage_options=None: (_ for _ in ()).throw(
        RuntimeError("table doesn't exist")
    )
    deltalake_module.writer = writer_module
    monkeypatch.setitem(sys.modules, "deltalake", deltalake_module)
    monkeypatch.setitem(sys.modules, "deltalake.writer", writer_module)
    monkeypatch.setattr(mat, "_build_storage_options", lambda storage_options=None: {"token": "abc"})
    monkeypatch.setattr(mat, "_is_remote_path", lambda path: False)
    monkeypatch.setattr(mat, "_get_pyarrow_schema", lambda dt: pa.schema([pa.field("id", pa.int64())]))

    delta_contract = _contract(
        _base_path=tmp_path,
        quarantine=types.SimpleNamespace(target="quarantine/delta", table=None, format="delta", write_mode="append"),
    )
    delta_result = q.materialize_quarantine(pl.DataFrame({"id": [1], "value": ["bad"]}), delta_contract)
    assert delta_result["format"] == "delta"
    assert delta_calls[0][1] == 1
    assert delta_calls[0][2]["schema_mode"] == "merge"

    catalog_events = []

    class FakeIcebergTable:
        def append(self, arrow_table):
            catalog_events.append(("append", arrow_table.num_rows))

    class FakeCatalog:
        def load_table(self, name):
            raise RuntimeError("missing")

        def create_table(self, name, schema=None):
            catalog_events.append(("create", name, schema))
            return FakeIcebergTable()

    pyiceberg_catalog = types.ModuleType("pyiceberg.catalog")
    pyiceberg_catalog.load_catalog = lambda name, **kwargs: FakeCatalog()
    pyiceberg_root = types.ModuleType("pyiceberg")
    pyiceberg_root.catalog = pyiceberg_catalog
    monkeypatch.setitem(sys.modules, "pyiceberg", pyiceberg_root)
    monkeypatch.setitem(sys.modules, "pyiceberg.catalog", pyiceberg_catalog)

    iceberg_contract = _contract(
        _base_path=tmp_path,
        quarantine=types.SimpleNamespace(
            target="quarantine/iceberg", table=None, format="iceberg", write_mode="append"
        ),
    )
    iceberg_result = q.materialize_quarantine(pl.DataFrame({"id": [1]}), iceberg_contract)
    assert iceberg_result["format"] == "iceberg"
    assert catalog_events[0][0] == "create"
    assert catalog_events[1] == ("append", 1)


def test_materialize_quarantine_delta_aligns_to_existing_schema(monkeypatch, tmp_path):
    pytest.importorskip("pandas")
    pa = pytest.importorskip("pyarrow")
    monkeypatch.setattr(q, "_stamp_quarantine_lineage", lambda frame, contract: frame)
    monkeypatch.setattr(q, "_frame_has_columns", lambda frame: True)
    monkeypatch.setattr(q, "_resolve_path", lambda raw, base: Path(base or tmp_path) / raw)

    fake_materialization = types.ModuleType("lakelogic.core.materialization")
    fake_materialization.URIPath = Path
    fake_materialization._build_storage_options = lambda storage_options=None: {"token": "abc"}
    fake_materialization._is_remote_path = lambda path: False
    fake_materialization._get_pyarrow_schema = lambda dt: pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("event_ts", pa.timestamp("us", tz="UTC")),
            pa.field("event_date", pa.date32()),
            pa.field("missing_col", pa.string()),
        ]
    )
    monkeypatch.setitem(sys.modules, "lakelogic.core.materialization", fake_materialization)

    fake_writer = types.ModuleType("deltalake.writer")
    fake_writer.write_deltalake = lambda target, arrow_data, **kwargs: captured.update(
        {"target": target, "arrow": arrow_data, "kwargs": kwargs}
    )
    fake_delta = types.ModuleType("deltalake")
    fake_delta.DeltaTable = lambda path, storage_options=None: types.SimpleNamespace(
        path=path, storage_options=storage_options
    )
    fake_delta.writer = fake_writer
    monkeypatch.setitem(sys.modules, "deltalake", fake_delta)
    monkeypatch.setitem(sys.modules, "deltalake.writer", fake_writer)

    captured = {}

    contract = _contract(
        _base_path=tmp_path,
        quarantine=types.SimpleNamespace(target="quarantine/aligned", table=None, format="delta", write_mode="append"),
    )
    df = pl.DataFrame(
        {
            "id": ["1", "bad"],
            "event_ts": ["2024-01-01T00:00:00Z", "not-a-ts"],
            "event_date": ["2024-01-01", "not-a-date"],
            "new_col": ["x", "y"],
        }
    )

    result = q.materialize_quarantine(df, contract)

    assert result["format"] == "delta"
    assert result["rows_written"] == 2
    assert captured["arrow"].schema.names == ["id", "event_ts", "event_date", "missing_col", "new_col"]
    assert captured["arrow"]["id"].to_pylist() == [1, None]
    assert captured["arrow"]["missing_col"].to_pylist() == [None, None]
    assert captured["kwargs"]["schema_mode"] == "merge"


def test_materialize_quarantine_unsupported_format_raises(monkeypatch, tmp_path):
    monkeypatch.setattr(q, "_stamp_quarantine_lineage", lambda frame, contract: frame)
    monkeypatch.setattr(q, "_frame_has_columns", lambda frame: True)
    monkeypatch.setattr(q, "_resolve_path", lambda raw, base: Path(base or tmp_path) / raw)

    contract = _contract(
        _base_path=tmp_path,
        quarantine=types.SimpleNamespace(target="quarantine/raw", table=None, format="jsonl", write_mode="append"),
    )
    with pytest.raises(ValueError, match="Unsupported quarantine format"):
        q.materialize_quarantine(pl.DataFrame({"id": [1]}), contract)
