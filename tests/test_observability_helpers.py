from __future__ import annotations

import json
import sys
import types

import pytest

from lakelogic.cli import observability as obs


def _summary():
    return {
        "run_id": "run-1",
        "started_at": "2026-01-01T00:00:00+00:00",
        "finished_at": "2026-01-01T00:01:00+00:00",
        "duration_seconds": 60.0,
        "engine": "spark",
        "metrics": {
            "total_contracts": 2,
            "successful": 1,
            "failed": 1,
            "skipped_missing_upstream": 0,
            "skipped_no_sources": 0,
            "full_loads": 1,
            "full_loads_due_to_missing_logs": 0,
            "missing_upstreams": 0,
        },
    }


def test_write_summary_spark_create_and_merge_paths(monkeypatch):
    infos = []
    monkeypatch.setattr(obs.logger, "info", infos.append)
    monkeypatch.setattr(obs.logger, "warning", infos.append)

    writes = []
    sql_calls = []
    dropped = []

    class FakeSparkWriter:
        def mode(self, value):
            writes.append(("mode", value))
            return self

        def format(self, value):
            writes.append(("format", value))
            return self

        def saveAsTable(self, value):
            writes.append(("saveAsTable", value))

    class FakeDataFrame:
        def __init__(self):
            self.write = FakeSparkWriter()

        def createOrReplaceTempView(self, name):
            writes.append(("temp_view", name))

    class FakeCatalog:
        def __init__(self):
            self.exists = False

        def tableExists(self, table_name):
            return self.exists

        def dropTempView(self, name):
            dropped.append(name)

    class FakeSpark:
        def __init__(self):
            self.catalog = FakeCatalog()

        def sql(self, statement):
            sql_calls.append(statement)

        def createDataFrame(self, rows):
            return FakeDataFrame()

        def table(self, table_name):
            return types.SimpleNamespace(columns=["run_id"])

    fake_spark = FakeSpark()
    fake_sql_module = types.ModuleType("pyspark.sql")
    fake_sql_module.SparkSession = types.SimpleNamespace(builder=types.SimpleNamespace(getOrCreate=lambda: fake_spark))
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql_module)

    record = obs.flatten_summary(_summary())
    obs._write_summary_spark(record, "catalog.metrics", None, merge_on_run_id=False)
    assert any(call == ("saveAsTable", "catalog.metrics") for call in writes)
    assert any("CREATE DATABASE IF NOT EXISTS catalog" in stmt for stmt in sql_calls)

    fake_spark.catalog.exists = True
    writes.clear()
    obs._write_summary_spark(record, "catalog.schema.metrics", "delta", merge_on_run_id=True)
    assert any("ALTER TABLE catalog.schema.metrics ADD COLUMNS (summary_json STRING)" in stmt for stmt in sql_calls)
    assert any("MERGE INTO catalog.schema.metrics" in stmt for stmt in sql_calls)
    assert len(dropped) == 1


def test_write_summary_snowflake_and_bigquery_backends(monkeypatch):
    pytest.importorskip("pandas")
    warnings = []
    infos = []
    monkeypatch.setattr(obs.logger, "warning", warnings.append)
    monkeypatch.setattr(obs.logger, "info", infos.append)

    record = obs.flatten_summary(_summary())

    connect_calls = []

    class FakeCursor:
        def execute(self, statement):
            connect_calls.append(statement)
            return self

    class FakeSnowflakeConnection:
        def cursor(self):
            return FakeCursor()

        def close(self):
            connect_calls.append("close")

    connector_module = types.ModuleType("snowflake.connector")
    connector_module.connect = lambda **kwargs: connect_calls.append(kwargs) or FakeSnowflakeConnection()
    pandas_tools_module = types.ModuleType("snowflake.connector.pandas_tools")
    pandas_tools_module.write_pandas = (
        lambda conn, pdf, table_name, database=None, schema=None, auto_create_table=True, overwrite=False: (
            connect_calls.append((table_name, database, schema, len(pdf)))
        )
    )
    snowflake_root = types.ModuleType("snowflake")
    snowflake_root.connector = connector_module
    monkeypatch.setitem(sys.modules, "snowflake", snowflake_root)
    monkeypatch.setitem(sys.modules, "snowflake.connector", connector_module)
    monkeypatch.setitem(sys.modules, "snowflake.connector.pandas_tools", pandas_tools_module)

    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "acct")
    monkeypatch.setenv("SNOWFLAKE_USER", "user")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "secret")
    obs._write_summary_snowflake(record, "raw.audit.pipeline_runs")
    assert any(isinstance(call, tuple) and call[0] == "pipeline_runs" for call in connect_calls)

    bq_calls = []

    class FakeTable:
        def __init__(self, table_id, schema=None):
            self.table_id = table_id
            self.schema = schema or []

    class FakeLoadJob:
        def result(self):
            bq_calls.append("result")

    class FakeLoadJobConfig:
        def __init__(self, write_disposition=None, create_disposition=None, schema=None):
            self.write_disposition = write_disposition
            self.create_disposition = create_disposition
            self.schema = schema

    class FakeSchemaField:
        def __init__(self, name, field_type):
            self.name = name
            self.field_type = field_type

    class FakeBigQueryClient:
        def __init__(self, project=None):
            self.project = project

        def get_table(self, table_id):
            raise RuntimeError("missing")

        def create_table(self, table, exists_ok=False):
            bq_calls.append(("create", table.table_id, exists_ok, len(table.schema)))
            return table

        def load_table_from_dataframe(self, pdf, table_id, job_config=None):
            bq_calls.append(("load", table_id, job_config.write_disposition, len(pdf)))
            return FakeLoadJob()

    google_root = types.ModuleType("google")
    cloud_module = types.ModuleType("google.cloud")
    bigquery_module = types.ModuleType("google.cloud.bigquery")
    bigquery_module.Client = FakeBigQueryClient
    bigquery_module.LoadJobConfig = FakeLoadJobConfig
    bigquery_module.SchemaField = FakeSchemaField
    bigquery_module.Table = FakeTable
    cloud_module.bigquery = bigquery_module
    google_root.cloud = cloud_module
    monkeypatch.setitem(sys.modules, "google", google_root)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_module)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", bigquery_module)
    monkeypatch.setenv("BIGQUERY_PROJECT", "demo")

    obs._write_summary_bigquery(record, "analytics.pipeline_runs")
    assert ("create", "demo.analytics.pipeline_runs", True, 14) in bq_calls
    assert ("load", "demo.analytics.pipeline_runs", "WRITE_APPEND", 1) in bq_calls


def test_emit_statsd_and_prometheus_server_failure(monkeypatch):
    infos = []
    warnings = []
    monkeypatch.setattr(obs.logger, "info", infos.append)
    monkeypatch.setattr(obs.logger, "warning", warnings.append)

    sent = []

    class FakeSocket:
        def sendto(self, payload, address):
            sent.append((payload.decode("utf-8"), address))

        def close(self):
            sent.append("closed")

    socket_module = types.ModuleType("socket")
    socket_module.AF_INET = 2
    socket_module.SOCK_DGRAM = 2
    socket_module.socket = lambda *args: FakeSocket()
    monkeypatch.setitem(sys.modules, "socket", socket_module)

    obs._emit_statsd({"successful": 2, "failed": None}, None, None, "demo", {"env": "test"})
    assert sent[0][0] == "demo.successful:2|g|#env:test"
    assert sent[0][1] == ("127.0.0.1", 8125)

    class BrokenServer:
        def __init__(self, *args, **kwargs):
            raise OSError("busy")

    monkeypatch.setattr(obs, "HTTPServer", BrokenServer)
    server, thread = obs.start_prometheus_server("127.0.0.1", 9100, lambda: {"metrics": {}}, "demo")
    assert server is None
    assert thread is None
    assert any("Failed to start Prometheus server" in message for message in warnings)


def test_finalize_emit_metrics_sqlite_and_prometheus_helpers(monkeypatch, tmp_path):
    infos = []
    warnings = []
    monkeypatch.setattr(obs.logger, "info", infos.append)
    monkeypatch.setattr(obs.logger, "warning", warnings.append)

    summary = _summary()
    summary_path = tmp_path / "logs" / "summary.json"
    obs.finalize_summary(summary, summary_path)
    written_summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert "finished_at" in written_summary
    assert written_summary["duration_seconds"] >= 0

    metrics_path = tmp_path / "logs" / "metrics.json"
    snapshot = obs.emit_metrics(summary, metrics_path, "prometheus", None, None, "demo", {"env": "test"})
    written_metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    assert written_metrics["tags"] == {"env": "test"}
    assert snapshot["metrics"]["successful"] == 1
    assert 'demo_successful{env="test"} 1\n' in obs.format_prometheus(snapshot, "demo")

    sqlite_db = tmp_path / "logs" / "summary.sqlite"
    obs._write_summary_sqlite(obs.flatten_summary(summary), "analytics.pipeline_runs", str(sqlite_db))
    assert sqlite_db.exists()
    assert any("SQLite does not support schemas" in message for message in warnings)

    obs.write_summary_table(summary, "table_name", "unsupported", None, None, False, "polars")
    assert any("Unsupported summary backend" in message for message in warnings)


def test_start_and_stop_prometheus_server(monkeypatch):
    infos = []
    monkeypatch.setattr(obs.logger, "info", infos.append)

    shutdown_calls = []

    class FakeServer:
        def serve_forever(self):
            return None

        def shutdown(self):
            shutdown_calls.append("shutdown")

        def server_close(self):
            shutdown_calls.append("close")

    class FakeThread:
        def __init__(self, target=None, daemon=None):
            self.target = target
            self.daemon = daemon

        def start(self):
            shutdown_calls.append("start")

    monkeypatch.setattr(obs, "HTTPServer", lambda addr, handler: FakeServer())
    monkeypatch.setattr(obs, "Thread", FakeThread)

    server, thread = obs.start_prometheus_server(None, None, lambda: {"metrics": {"successful": 2}}, "demo")

    assert server is not None
    assert thread is not None
    assert "start" in shutdown_calls
    obs.stop_prometheus_server(server, thread)
    assert shutdown_calls[-2:] == ["shutdown", "close"]
