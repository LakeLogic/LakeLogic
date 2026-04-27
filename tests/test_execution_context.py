from __future__ import annotations

import builtins
import sys
import types
from datetime import datetime, timezone

from lakelogic.core import execution_context as ec


def test_capture_universal_context_with_error_and_duration(monkeypatch):
    class FakeDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 4, 19, 12, 0, 10, tzinfo=timezone.utc)

    monkeypatch.setattr(ec, "datetime", FakeDateTime)
    monkeypatch.setattr(ec, "_get_engine_version", lambda engine: "1.2.3")
    monkeypatch.setattr(ec, "_get_peak_memory_mb", lambda: 123.4)

    error = ValueError("boom")
    ctx = ec.capture_universal_context(
        "polars",
        error=error,
        error_stage="validate",
        start_time=datetime(2026, 4, 19, 12, 0, 0, tzinfo=timezone.utc),
    )

    assert ctx["engine"] == "polars"
    assert ctx["engine_version"] == "1.2.3"
    assert ctx["peak_memory_mb"] == 123.4
    assert ctx["wall_clock_seconds"] == 10.0
    assert ctx["error_type"] == "ValueError"
    assert ctx["error_stage"] == "validate"
    assert len(ctx["error_traceback_hash"]) == 12


def test_capture_polars_and_duckdb_contexts(monkeypatch):
    fake_polars = types.SimpleNamespace(__version__="0.20.0", thread_pool_size=lambda: 8)
    monkeypatch.setitem(sys.modules, "polars", fake_polars)
    monkeypatch.setenv("POLARS_MAX_THREADS", "6")
    polars_ctx = ec.capture_polars_context()
    assert polars_ctx == {
        "engine_version": "0.20.0",
        "streaming": False,
        "thread_pool_size": 6,
        "predicate_pushdown": True,
        "projection_pushdown": True,
    }

    class FakeConn:
        def execute(self, query):
            if "threads" in query:
                return types.SimpleNamespace(fetchone=lambda: [4])
            if "memory_limit" in query:
                return types.SimpleNamespace(fetchone=lambda: ["1GB"])
            return types.SimpleNamespace(fetchall=lambda: [("httpfs",), ("parquet",)])

        def close(self):
            return None

    fake_duckdb = types.SimpleNamespace(__version__="1.1.0", connect=lambda _: FakeConn())
    monkeypatch.setitem(sys.modules, "duckdb", fake_duckdb)
    duck_ctx = ec.capture_duckdb_context()
    assert duck_ctx["engine_version"] == "1.1.0"
    assert duck_ctx["threads"] == 4
    assert duck_ctx["memory_limit"] == "1GB"
    assert duck_ctx["extensions"] == ["httpfs", "parquet"]


def test_capture_duckdb_context_handles_import_and_query_failures(monkeypatch):
    original_import = builtins.__import__

    def blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "duckdb":
            raise ImportError("missing")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", blocked_import)
    assert ec.capture_duckdb_context() == {}
    monkeypatch.setattr(builtins, "__import__", original_import)

    class BrokenConn:
        def execute(self, query):
            raise RuntimeError("bad")

        def close(self):
            return None

    monkeypatch.setitem(sys.modules, "duckdb", types.SimpleNamespace(__version__="1.1.0", connect=lambda _: BrokenConn()))
    assert ec.capture_duckdb_context() == {"engine_version": "1.1.0"}


def test_capture_spark_context_paths(monkeypatch):
    fake_conf = types.SimpleNamespace(
        get=lambda key, default=None: {
            "spark.driver.memory": "4g",
            "spark.executor.memory": "8g",
            "spark.executor.cores": "2",
            "spark.databricks.clusterUsageTags.clusterNodeType": "Standard_DS3_v2",
        }.get(key, default)
    )
    fake_sc = types.SimpleNamespace(
        version="3.5.0",
        applicationId="app-1",
        appName="lake-job",
        getConf=lambda: fake_conf,
        _jsc=types.SimpleNamespace(sc=lambda: types.SimpleNamespace(getExecutorMemoryStatus=lambda: {"driver": 1, "exec1": 1, "exec2": 1})),
        uiWebUrl="http://spark-ui",
    )
    fake_session = types.SimpleNamespace(sparkContext=fake_sc)
    fake_spark_sql = types.SimpleNamespace(SparkSession=types.SimpleNamespace(getActiveSession=lambda: fake_session))
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_spark_sql)
    monkeypatch.setenv("DATABRICKS_JOB_ID", "10")
    monkeypatch.setenv("DATABRICKS_RUN_ID", "11")
    monkeypatch.setenv("DATABRICKS_TASK_KEY", "task")
    monkeypatch.setenv("DATABRICKS_CLUSTER_ID", "cluster")
    monkeypatch.setenv("DATABRICKS_HOST", "https://workspace")

    ctx = ec.capture_spark_context()
    assert ctx["engine_version"] == "3.5.0"
    assert ctx["num_workers"] == 2
    assert ctx["spark_ui_url"] == "http://spark-ui"
    assert ctx["dbx_job_id"] == "10"
    assert ctx["cluster_type"] == "Standard_DS3_v2"

    fallback_sc = types.SimpleNamespace(
        version="3.5.0",
        applicationId="app-2",
        appName="lake-job",
        getConf=lambda: types.SimpleNamespace(get=lambda key, default=None: {"spark.executor.instances": "unknown"}.get(key, default)),
        _jsc=types.SimpleNamespace(sc=lambda: (_ for _ in ()).throw(RuntimeError("bad"))),
        uiWebUrl=None,
    )
    fake_spark_sql.SparkSession = types.SimpleNamespace(getActiveSession=lambda: types.SimpleNamespace(sparkContext=fallback_sc))
    fallback_ctx = ec.capture_spark_context()
    assert fallback_ctx["num_workers"] == "unknown"

    fake_spark_sql.SparkSession = types.SimpleNamespace(getActiveSession=lambda: None)
    assert ec.capture_spark_context() == {}


def test_capture_pandas_context_and_dispatch(monkeypatch):
    fake_pandas = types.SimpleNamespace(
        __version__="2.2.0",
        options=types.SimpleNamespace(mode=types.SimpleNamespace(copy_on_write=True, dtype_backend="pyarrow")),
    )
    monkeypatch.setitem(sys.modules, "pandas", fake_pandas)
    pandas_ctx = ec.capture_pandas_context()
    assert pandas_ctx == {"engine_version": "2.2.0", "copy_on_write": True, "dtype_backend": "pyarrow"}

    monkeypatch.setattr(ec, "capture_universal_context", lambda *args, **kwargs: {"engine": "polars", "engine_version": "old"})
    monkeypatch.setattr(ec, "capture_polars_context", lambda: {"engine_version": "new", "streaming": False})
    assert ec.capture_execution_context("polars") == {"engine": "polars", "engine_version": "new", "polars": {"streaming": False}}
    assert ec.capture_execution_context("unknown") == {"engine": "polars", "engine_version": "old"}


def test_engine_version_and_peak_memory_helpers(monkeypatch):
    monkeypatch.setitem(sys.modules, "polars", types.SimpleNamespace(__version__="0.20.0"))
    monkeypatch.setitem(sys.modules, "pandas", types.SimpleNamespace(__version__="2.2.0"))
    monkeypatch.setitem(sys.modules, "duckdb", types.SimpleNamespace(__version__="1.1.0"))
    monkeypatch.setitem(sys.modules, "pyspark", types.SimpleNamespace(__version__="3.5.0"))
    assert ec._get_engine_version("polars") == "0.20.0"
    assert ec._get_engine_version("pandas") == "2.2.0"
    assert ec._get_engine_version("duckdb") == "1.1.0"
    assert ec._get_engine_version("spark") == "3.5.0"
    assert ec._get_engine_version("missing") is None

    fake_resource = types.SimpleNamespace(
        RUSAGE_SELF=1,
        getrusage=lambda target: types.SimpleNamespace(ru_maxrss=2048),
    )
    monkeypatch.setitem(sys.modules, "resource", fake_resource)
    monkeypatch.setattr(ec.sys, "platform", "linux")
    assert ec._get_peak_memory_mb() == 2.0

    monkeypatch.delitem(sys.modules, "resource", raising=False)
    monkeypatch.setitem(
        sys.modules,
        "psutil",
        types.SimpleNamespace(Process=lambda: types.SimpleNamespace(memory_info=lambda: types.SimpleNamespace(rss=3 * 1024 * 1024))),
    )
    assert ec._get_peak_memory_mb() == 3.0
