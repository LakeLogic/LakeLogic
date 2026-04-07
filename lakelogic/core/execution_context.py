"""
Execution context capture for LakeLogic run-log observability.

Collects engine-specific runtime metadata that powers AI-driven
root-cause analysis in LakeLogic Cloud.  Every field captured here
is serialised to a single ``execution_context_json`` column in the
run log — no schema changes needed when new fields are added.

Universal fields (all engines):
    engine, engine_version, python_version, os_platform,
    peak_memory_mb, error_type, error_traceback_hash, error_stage

Engine-specific fields live under a sub-key:
    polars:  lazy_mode, streaming, predicate_pushdown, projection_pushdown
    duckdb:  memory_limit, threads, extensions
    spark:   app_id, spark_ui_url, cluster_type, num_workers, shuffle_spill_gb
    pandas:  dtype_backend, copy_on_write
"""

from __future__ import annotations

import hashlib
import os
import platform
import sys
import traceback
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def capture_universal_context(
    engine_name: str,
    *,
    error: Optional[BaseException] = None,
    error_stage: Optional[str] = None,
    start_time: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    Capture engine-agnostic runtime context.

    Args:
        engine_name: Engine identifier (polars, pandas, duckdb, spark).
        error: Exception that caused the failure, if any.
        error_stage: Pipeline stage where the error occurred
                     (ingest, validate, transform, materialize).
        start_time: Pipeline start timestamp for wall-clock duration.

    Returns:
        Dict with universal context fields.
    """
    ctx: Dict[str, Any] = {
        "engine": engine_name,
        "python_version": platform.python_version(),
        "os_platform": f"{platform.system()}-{platform.machine()}",
    }

    # Engine version
    ctx["engine_version"] = _get_engine_version(engine_name)

    # Peak memory (RSS) — best-effort via resource or psutil
    ctx["peak_memory_mb"] = _get_peak_memory_mb()

    # Wall-clock duration
    if start_time:
        elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
        ctx["wall_clock_seconds"] = round(elapsed, 2)

    # Error enrichment
    if error is not None:
        ctx["error_type"] = type(error).__name__
        ctx["error_stage"] = error_stage
        tb_str = "".join(traceback.format_exception(type(error), error, error.__traceback__))
        ctx["error_traceback_hash"] = hashlib.sha256(tb_str.encode()).hexdigest()[:12]

    return ctx


def capture_polars_context() -> Dict[str, Any]:
    """Capture Polars-specific execution context."""
    ctx: Dict[str, Any] = {}
    try:
        import polars as pl

        ctx["engine_version"] = pl.__version__
        ctx["streaming"] = False  # default — caller overrides if streaming was used
        ctx["thread_pool_size"] = int(os.getenv("POLARS_MAX_THREADS", pl.thread_pool_size()))
        # Polars lazy plan optimisations
        ctx["predicate_pushdown"] = True  # on by default in Polars
        ctx["projection_pushdown"] = True
    except ImportError:
        pass
    return ctx


def capture_duckdb_context() -> Dict[str, Any]:
    """Capture DuckDB-specific execution context."""
    ctx: Dict[str, Any] = {}
    try:
        import duckdb

        ctx["engine_version"] = duckdb.__version__
        # Connect briefly to read config
        conn = duckdb.connect(":memory:")
        ctx["threads"] = conn.execute("SELECT current_setting('threads')").fetchone()[0]
        ctx["memory_limit"] = conn.execute("SELECT current_setting('memory_limit')").fetchone()[0]
        # Check extensions
        try:
            exts = conn.execute("SELECT extension_name FROM duckdb_extensions() WHERE loaded = true").fetchall()
            ctx["extensions"] = [e[0] for e in exts]
        except Exception:
            ctx["extensions"] = []
        conn.close()
    except ImportError:
        pass
    except Exception:
        pass
    return ctx


def capture_spark_context() -> Dict[str, Any]:
    """
    Capture Spark/Databricks-specific execution context.

    When running on Databricks, also captures cluster info and job metadata
    from environment variables set by the Databricks runtime.
    """
    ctx: Dict[str, Any] = {}
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.getActiveSession()
        if spark is None:
            return ctx

        sc = spark.sparkContext
        ctx["engine_version"] = sc.version
        ctx["app_id"] = sc.applicationId
        ctx["app_name"] = sc.appName

        # Cluster sizing
        conf = sc.getConf()
        ctx["driver_memory"] = conf.get("spark.driver.memory", "unknown")
        ctx["executor_memory"] = conf.get("spark.executor.memory", "unknown")
        ctx["executor_cores"] = conf.get("spark.executor.cores", "unknown")

        # Num executors (dynamic allocation may override this)
        try:
            num_executors = len(sc._jsc.sc().getExecutorMemoryStatus()) - 1  # subtract driver
            ctx["num_workers"] = max(num_executors, 0)
        except Exception:
            ctx["num_workers"] = conf.get("spark.executor.instances", "unknown")

        # Spark UI URL
        try:
            ctx["spark_ui_url"] = sc.uiWebUrl
        except Exception:
            pass

        # Databricks-specific (set by the Databricks runtime)
        dbx_job_id = os.getenv("DATABRICKS_JOB_ID")
        if dbx_job_id:
            ctx["dbx_job_id"] = dbx_job_id
            ctx["dbx_run_id"] = os.getenv("DATABRICKS_RUN_ID")
            ctx["dbx_task_key"] = os.getenv("DATABRICKS_TASK_KEY")
            ctx["dbx_cluster_id"] = os.getenv("DATABRICKS_CLUSTER_ID")
            ctx["dbx_workspace_url"] = os.getenv("DATABRICKS_HOST")

        # Cluster type (Databricks-specific env)
        node_type = os.getenv("DATABRICKS_NODE_TYPE") or conf.get(
            "spark.databricks.clusterUsageTags.clusterNodeType", None
        )
        if node_type:
            ctx["cluster_type"] = node_type

    except ImportError:
        pass
    except Exception:
        pass
    return ctx


def capture_pandas_context() -> Dict[str, Any]:
    """Capture Pandas-specific execution context."""
    ctx: Dict[str, Any] = {}
    try:
        import pandas as pd

        ctx["engine_version"] = pd.__version__

        # Copy-on-write mode (Pandas 2.x+)
        try:
            ctx["copy_on_write"] = pd.options.mode.copy_on_write
        except Exception:
            ctx["copy_on_write"] = False

        # NumPy backend vs PyArrow backend
        try:
            ctx["dtype_backend"] = pd.options.mode.dtype_backend
        except Exception:
            ctx["dtype_backend"] = "numpy"

    except ImportError:
        pass
    return ctx


def capture_execution_context(
    engine_name: str,
    *,
    error: Optional[BaseException] = None,
    error_stage: Optional[str] = None,
    start_time: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    Capture full execution context for a pipeline run.

    Combines universal fields with engine-specific metadata.
    The result is serialised to ``execution_context_json`` in the run log.

    Args:
        engine_name: Engine identifier (polars, pandas, duckdb, spark).
        error: Exception that caused the failure, if any.
        error_stage: Pipeline stage where the error occurred.
        start_time: Pipeline start timestamp.

    Returns:
        Complete execution context dict.
    """
    ctx = capture_universal_context(
        engine_name,
        error=error,
        error_stage=error_stage,
        start_time=start_time,
    )

    # Engine-specific sub-key
    engine_key = engine_name.lower() if engine_name else "unknown"
    engine_ctx: Dict[str, Any] = {}

    if engine_key == "polars":
        engine_ctx = capture_polars_context()
    elif engine_key == "duckdb":
        engine_ctx = capture_duckdb_context()
    elif engine_key == "spark":
        engine_ctx = capture_spark_context()
    elif engine_key == "pandas":
        engine_ctx = capture_pandas_context()

    if engine_ctx:
        # Promote engine_version to top level if captured
        if "engine_version" in engine_ctx:
            ctx["engine_version"] = engine_ctx.pop("engine_version")
        ctx[engine_key] = engine_ctx

    return ctx


# ── Internal helpers ──────────────────────────────────────────────────


def _get_engine_version(engine_name: str) -> Optional[str]:
    """Best-effort engine version detection."""
    try:
        name = (engine_name or "").lower()
        if name == "polars":
            import polars as pl

            return pl.__version__
        elif name == "pandas":
            import pandas as pd

            return pd.__version__
        elif name == "duckdb":
            import duckdb

            return duckdb.__version__
        elif name == "spark":
            from pyspark import __version__ as spark_ver

            return spark_ver
    except Exception:
        pass
    return None


def _get_peak_memory_mb() -> Optional[float]:
    """Get peak RSS in MB. Unix uses resource module; falls back to psutil."""
    try:
        import resource

        # maxrss is in KB on Linux, bytes on macOS
        maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform == "darwin":
            return round(maxrss / (1024 * 1024), 1)
        return round(maxrss / 1024, 1)
    except Exception:
        pass
    try:
        import psutil

        proc = psutil.Process()
        return round(proc.memory_info().rss / (1024 * 1024), 1)
    except Exception:
        pass
    return None
