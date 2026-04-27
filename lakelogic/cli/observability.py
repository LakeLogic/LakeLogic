"""
Pipeline observability — summary tables, metrics, and Prometheus.

Extracted from ``driver.py`` to keep the pipeline driver module focused on
orchestration logic.  These are implemented as standalone functions that
receive the driver's state as explicit arguments, to keep coupling loose.
"""

import json
import os
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Thread
from typing import Any, Dict, Optional
from uuid import uuid4

from loguru import logger


# ── Summary ──────────────────────────────────────────────────────────────────


def flatten_summary(summary: Dict[str, Any]) -> Dict[str, object]:
    """Flatten summary data into a table-oriented record."""
    metrics = summary.get("metrics", {})
    return {
        "run_id": summary.get("run_id"),
        "started_at": summary.get("started_at"),
        "finished_at": summary.get("finished_at"),
        "duration_seconds": summary.get("duration_seconds"),
        "engine": summary.get("engine"),
        "total_contracts": metrics.get("total_contracts"),
        "successful": metrics.get("successful"),
        "failed": metrics.get("failed"),
        "skipped_missing_upstream": metrics.get("skipped_missing_upstream"),
        "skipped_no_sources": metrics.get("skipped_no_sources"),
        "full_loads": metrics.get("full_loads"),
        "full_loads_due_to_missing_logs": metrics.get("full_loads_due_to_missing_logs"),
        "missing_upstreams": metrics.get("missing_upstreams"),
        "summary_json": json.dumps(summary, default=str),
    }


def finalize_summary(
    summary: Dict[str, Any],
    summary_path: Optional[Path],
) -> None:
    """Finalize and optionally persist the run summary to JSON file."""
    summary["finished_at"] = datetime.now(timezone.utc).isoformat()
    started = summary.get("started_at")
    if started:
        try:
            started_dt = datetime.fromisoformat(started)
            summary["duration_seconds"] = round((datetime.now(timezone.utc) - started_dt).total_seconds(), 2)
        except Exception:  # pragma: no cover
            pass  # pragma: no cover

    if summary_path:
        summary_path.parent.mkdir(parents=True, exist_ok=True)
        summary_path.write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
        logger.info(f"Wrote pipeline summary to {summary_path}")


# ── Summary Table Writers ────────────────────────────────────────────────────


def write_summary_table(
    summary: Dict[str, Any],
    summary_table: Optional[str],
    summary_backend: Optional[str],
    summary_database: Optional[str],
    summary_table_format: Optional[str],
    summary_merge_on_run_id: bool,
    engine: str,
) -> None:
    """Write a pipeline summary row to a table backend."""
    if not summary_table:
        return

    backend = (summary_backend or ("spark" if engine == "spark" else "duckdb")).lower()
    record = flatten_summary(summary)

    if backend == "spark":
        _write_summary_spark(record, summary_table, summary_table_format, summary_merge_on_run_id)  # pragma: no cover
    elif backend == "duckdb":
        _write_summary_duckdb(record, summary_table, summary_database)
    elif backend == "sqlite":
        _write_summary_sqlite(record, summary_table, summary_database)
    elif backend == "snowflake":
        _write_summary_snowflake(record, summary_table)  # pragma: no cover
    elif backend == "bigquery":
        _write_summary_bigquery(record, summary_table)  # pragma: no cover
    else:
        logger.warning(f"Unsupported summary backend: {backend}")


def _write_summary_spark(
    record: Dict[str, object],
    table_name: str,
    table_format: Optional[str],
    merge_on_run_id: bool,
) -> None:
    try:
        from pyspark.sql import SparkSession
    except Exception as exc:  # pragma: no cover
        logger.warning(f"Summary table backend 'spark' unavailable: {exc}")  # pragma: no cover
        return  # pragma: no cover

    spark = SparkSession.builder.getOrCreate()
    parts = table_name.split(".")
    if len(parts) == 2:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {parts[0]}")
    elif len(parts) >= 3:
        schema = ".".join(parts[:-1])
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    df = spark.createDataFrame([record])
    if spark.catalog.tableExists(table_name):
        try:
            existing_cols = set(spark.table(table_name).columns)
            if "summary_json" not in existing_cols:
                spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS (summary_json STRING)")
        except Exception as exc:  # pragma: no cover
            logger.warning(f"Failed to align summary table schema for {table_name}: {exc}")  # pragma: no cover

        if merge_on_run_id:
            view_name = f"lakelogic_summary_updates_{uuid4().hex}"
            df.createOrReplaceTempView(view_name)
            try:
                spark.sql(f"""
                    MERGE INTO {table_name} AS target
                    USING {view_name} AS source
                    ON target.run_id = source.run_id
                    WHEN MATCHED THEN UPDATE SET *
                    WHEN NOT MATCHED THEN INSERT *
                """)
            except Exception as exc:  # pragma: no cover
                logger.warning(f"Summary table merge failed for {table_name}: {exc}")  # pragma: no cover
                return  # pragma: no cover
            finally:
                try:
                    spark.catalog.dropTempView(view_name)
                except Exception:  # pragma: no cover
                    pass  # pragma: no cover
        else:  # pragma: no cover
            fmt = table_format or "delta"  # pragma: no cover
            df.write.mode("append").format(fmt).saveAsTable(table_name)  # pragma: no cover
    else:
        fmt = table_format or "delta"
        df.write.mode("overwrite").format(fmt).saveAsTable(table_name)
    logger.info(f"Wrote pipeline summary to Spark table {table_name}")


_SUMMARY_COLUMNS = [
    "run_id",
    "started_at",
    "finished_at",
    "duration_seconds",
    "engine",
    "total_contracts",
    "successful",
    "failed",
    "skipped_missing_upstream",
    "skipped_no_sources",
    "full_loads",
    "full_loads_due_to_missing_logs",
    "missing_upstreams",
    "summary_json",
]


def _write_summary_duckdb(record: Dict[str, object], table_name: str, database: Optional[str]) -> None:
    try:
        import duckdb
    except Exception as exc:  # pragma: no cover
        logger.warning(f"Summary table backend 'duckdb' unavailable: {exc}")  # pragma: no cover
        return  # pragma: no cover

    db_path = Path(database or "logs/lakelogic_pipeline_runs.duckdb")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database=str(db_path))
    try:
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                run_id VARCHAR, started_at VARCHAR, finished_at VARCHAR,
                duration_seconds DOUBLE, engine VARCHAR,
                total_contracts BIGINT, successful BIGINT, failed BIGINT,
                skipped_missing_upstream BIGINT, skipped_no_sources BIGINT,
                full_loads BIGINT, full_loads_due_to_missing_logs BIGINT,
                missing_upstreams BIGINT, summary_json VARCHAR
            )
        """)
        con.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS summary_json VARCHAR")
        placeholders = ", ".join(["?"] * len(_SUMMARY_COLUMNS))
        cols = ", ".join(_SUMMARY_COLUMNS)
        con.execute(
            f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})",
            [record[c] for c in _SUMMARY_COLUMNS],
        )
    finally:
        con.close()
    logger.info(f"Wrote pipeline summary to DuckDB table {table_name} ({db_path})")


def _write_summary_sqlite(record: Dict[str, object], table_name: str, database: Optional[str]) -> None:
    import sqlite3

    db_path = Path(database or "logs/lakelogic_pipeline_runs.sqlite")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    sanitised_table = table_name.replace(".", "_")
    if sanitised_table != table_name:
        logger.warning(f"SQLite does not support schemas. Using '{sanitised_table}' instead of '{table_name}'.")
    con = sqlite3.connect(str(db_path))
    try:
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {sanitised_table} (
                run_id TEXT, started_at TEXT, finished_at TEXT,
                duration_seconds REAL, engine TEXT,
                total_contracts INTEGER, successful INTEGER, failed INTEGER,
                skipped_missing_upstream INTEGER, skipped_no_sources INTEGER,
                full_loads INTEGER, full_loads_due_to_missing_logs INTEGER,
                missing_upstreams INTEGER, summary_json TEXT
            )
        """)
        try:
            cols = [row[1] for row in con.execute(f"PRAGMA table_info({sanitised_table})").fetchall()]
            if "summary_json" not in cols:
                con.execute(f"ALTER TABLE {sanitised_table} ADD COLUMN summary_json TEXT")  # pragma: no cover
        except Exception:  # pragma: no cover
            pass  # pragma: no cover
        placeholders = ", ".join(["?"] * len(_SUMMARY_COLUMNS))
        col_list = ", ".join(_SUMMARY_COLUMNS)
        con.execute(
            f"INSERT INTO {sanitised_table} ({col_list}) VALUES ({placeholders})",
            [record[c] for c in _SUMMARY_COLUMNS],
        )
        con.commit()
    finally:
        con.close()
    logger.info(f"Wrote pipeline summary to SQLite table {sanitised_table} ({db_path})")


def _write_summary_snowflake(record: Dict[str, object], table_name: str) -> None:
    try:
        import snowflake.connector
        from snowflake.connector.pandas_tools import write_pandas
    except Exception as exc:  # pragma: no cover
        logger.warning(f"Summary table backend 'snowflake' unavailable: {exc}")  # pragma: no cover
        return  # pragma: no cover

    params = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "role": os.getenv("SNOWFLAKE_ROLE"),
    }
    missing = [k for k, v in params.items() if k in ["account", "user", "password"] and not v]
    if missing:
        logger.warning(f"Snowflake summary write missing required fields: {', '.join(missing)}")  # pragma: no cover
        return  # pragma: no cover

    parts = table_name.split(".")
    if len(parts) >= 3:
        params["database"] = parts[-3]
        params["schema"] = parts[-2]
        table_only = parts[-1]
    elif len(parts) == 2:  # pragma: no cover
        params["schema"] = parts[-2]  # pragma: no cover
        table_only = parts[-1]  # pragma: no cover
    else:  # pragma: no cover
        table_only = table_name  # pragma: no cover

    try:
        import pandas as pd
    except Exception as exc:  # pragma: no cover
        logger.warning(f"Snowflake summary write requires pandas: {exc}")  # pragma: no cover
        return  # pragma: no cover

    pdf = pd.DataFrame([record])
    conn = snowflake.connector.connect(**{k: v for k, v in params.items() if v})
    try:
        ddl_columns = [
            ("run_id", "STRING"),
            ("started_at", "STRING"),
            ("finished_at", "STRING"),
            ("duration_seconds", "FLOAT"),
            ("engine", "STRING"),
            ("total_contracts", "NUMBER"),
            ("successful", "NUMBER"),
            ("failed", "NUMBER"),
            ("skipped_missing_upstream", "NUMBER"),
            ("skipped_no_sources", "NUMBER"),
            ("full_loads", "NUMBER"),
            ("full_loads_due_to_missing_logs", "NUMBER"),
            ("missing_upstreams", "NUMBER"),
            ("summary_json", "VARIANT"),
        ]
        column_ddl = ", ".join(f"{name} {dtype}" for name, dtype in ddl_columns)
        conn.cursor().execute(f"CREATE TABLE IF NOT EXISTS {table_only} ({column_ddl})")
        for name, dtype in ddl_columns:
            conn.cursor().execute(f"ALTER TABLE {table_only} ADD COLUMN IF NOT EXISTS {name} {dtype}")
        write_pandas(
            conn,
            pdf,
            table_name=table_only,
            database=params.get("database"),
            schema=params.get("schema"),
            auto_create_table=True,
            overwrite=False,
        )
    finally:
        try:
            conn.close()
        except Exception:  # pragma: no cover
            pass  # pragma: no cover
    logger.info(f"Wrote pipeline summary to Snowflake table {table_only}")


def _write_summary_bigquery(record: Dict[str, object], table_name: str) -> None:
    try:
        from google.cloud import bigquery  # type: ignore
    except Exception as exc:  # pragma: no cover
        logger.warning(f"Summary table backend 'bigquery' unavailable: {exc}")  # pragma: no cover
        return  # pragma: no cover

    parts = table_name.split(".")
    project = os.getenv("BIGQUERY_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    if len(parts) == 3:
        project, dataset, table_only = parts  # pragma: no cover
    elif len(parts) == 2:
        dataset, table_only = parts
    else:
        logger.warning("BigQuery summary table name must be dataset.table or project.dataset.table")  # pragma: no cover
        return  # pragma: no cover

    if not project:
        logger.warning("BigQuery summary write missing project (BIGQUERY_PROJECT or GOOGLE_CLOUD_PROJECT).")  # pragma: no cover
        return  # pragma: no cover

    try:
        import pandas as pd
    except Exception as exc:  # pragma: no cover
        logger.warning(f"BigQuery summary write requires pandas: {exc}")  # pragma: no cover
        return  # pragma: no cover

    client = bigquery.Client(project=project)
    pdf = pd.DataFrame([record])
    table_id = f"{project}.{dataset}.{table_only}"
    desired_schema = [
        bigquery.SchemaField("run_id", "STRING"),
        bigquery.SchemaField("started_at", "STRING"),
        bigquery.SchemaField("finished_at", "STRING"),
        bigquery.SchemaField("duration_seconds", "FLOAT"),
        bigquery.SchemaField("engine", "STRING"),
        bigquery.SchemaField("total_contracts", "INTEGER"),
        bigquery.SchemaField("successful", "INTEGER"),
        bigquery.SchemaField("failed", "INTEGER"),
        bigquery.SchemaField("skipped_missing_upstream", "INTEGER"),
        bigquery.SchemaField("skipped_no_sources", "INTEGER"),
        bigquery.SchemaField("full_loads", "INTEGER"),
        bigquery.SchemaField("full_loads_due_to_missing_logs", "INTEGER"),
        bigquery.SchemaField("missing_upstreams", "INTEGER"),
        bigquery.SchemaField("summary_json", "STRING"),
    ]
    try:
        table = client.get_table(table_id)
        existing = {field.name for field in table.schema}  # pragma: no cover
        updates = [field for field in desired_schema if field.name not in existing]  # pragma: no cover
        if updates:  # pragma: no cover
            table.schema = list(table.schema) + updates  # pragma: no cover
            client.update_table(table, ["schema"])  # pragma: no cover
    except Exception:
        table = bigquery.Table(table_id, schema=desired_schema)
        client.create_table(table, exists_ok=True)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        schema=desired_schema,
    )
    job = client.load_table_from_dataframe(pdf, table_id, job_config=job_config)
    job.result()
    logger.info(f"Wrote pipeline summary to BigQuery table {table_id}")


# ── Metrics ──────────────────────────────────────────────────────────────────


def emit_metrics(
    summary: Dict[str, Any],
    metrics_path: Optional[Path],
    metrics_backend: Optional[str],
    metrics_host: Optional[str],
    metrics_port: Optional[int],
    metrics_prefix: Optional[str],
    metrics_tags: Dict[str, str],
) -> Dict[str, Any]:
    """Emit metrics to a JSON file or StatsD endpoint. Returns the snapshot."""
    record = flatten_summary(summary)
    metrics = {
        "run_id": record.get("run_id"),
        "engine": record.get("engine"),
        "duration_seconds": record.get("duration_seconds"),
        "total_contracts": record.get("total_contracts"),
        "successful": record.get("successful"),
        "failed": record.get("failed"),
        "skipped_missing_upstream": record.get("skipped_missing_upstream"),
        "skipped_no_sources": record.get("skipped_no_sources"),
        "full_loads": record.get("full_loads"),
        "full_loads_due_to_missing_logs": record.get("full_loads_due_to_missing_logs"),
        "missing_upstreams": record.get("missing_upstreams"),
    }
    snapshot = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "tags": metrics_tags,
        "metrics": metrics,
    }

    if metrics_path:
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        metrics_path.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
        logger.info(f"Wrote metrics payload to {metrics_path}")

    backend = (metrics_backend or "").lower()
    if backend == "prometheus":
        return snapshot
    if backend == "statsd":
        _emit_statsd(metrics, metrics_host, metrics_port, metrics_prefix, metrics_tags)

    return snapshot


def _emit_statsd(
    metrics: Dict[str, Any],
    host: Optional[str],
    port: Optional[int],
    prefix: Optional[str],
    tags: Dict[str, str],
) -> None:
    host = host or "127.0.0.1"
    port = int(port or 8125)
    prefix = prefix or "lakelogic"

    tag_str = ""
    if tags:
        tag_str = "|#" + ",".join(f"{k}:{v}" for k, v in tags.items())

    lines = []
    for name, value in metrics.items():
        if value is None:
            continue
        lines.append(f"{prefix}.{name}:{value}|g{tag_str}")
    if not lines:
        return  # pragma: no cover

    message = "\n".join(lines).encode("utf-8")
    try:
        import socket

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.sendto(message, (host, port))
        sock.close()
        logger.info(f"Emitted metrics to StatsD at {host}:{port}")
    except Exception as exc:  # pragma: no cover
        logger.warning(f"Failed to emit metrics to StatsD: {exc}")  # pragma: no cover


# ── Prometheus ───────────────────────────────────────────────────────────────


def format_prometheus(
    snapshot: Optional[Dict[str, Any]],
    prefix: str = "lakelogic",
) -> str:
    """Format metrics in Prometheus exposition format."""
    snapshot = snapshot or {}
    metrics = snapshot.get("metrics") or {}
    tags = snapshot.get("tags") or {}

    def _labels(extra: Optional[Dict[str, str]] = None) -> str:
        label_items = dict(tags)
        if extra:
            label_items.update(extra)  # pragma: no cover
        if not label_items:
            return ""
        pairs = ",".join(f'{k}="{v}"' for k, v in label_items.items())
        return "{" + pairs + "}"

    lines = []
    for key, value in metrics.items():
        if value is None:
            continue
        name = f"{prefix}_{key}"
        lines.append(f"{name}{_labels()} {value}")
    return "\n".join(lines) + "\n"


def start_prometheus_server(
    host: Optional[str],
    port: Optional[int],
    get_snapshot_fn,
    prefix: str = "lakelogic",
) -> tuple:
    """
    Start a lightweight Prometheus /metrics HTTP server.

    Returns:
        Tuple of (HTTPServer, Thread) or (None, None) if failed.
    """
    host = host or "0.0.0.0"
    port = int(port or 9100)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):  # noqa: N802
            if self.path not in ["/metrics", "/metrics/"]:
                self.send_response(404)
                self.end_headers()
                return
            payload = format_prometheus(get_snapshot_fn(), prefix).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format, *args):  # noqa: A003
            return

    try:
        server = HTTPServer((host, port), Handler)
    except Exception as exc:
        logger.warning(f"Failed to start Prometheus server on {host}:{port}: {exc}")
        return None, None

    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    logger.info(f"Prometheus metrics server running at http://{host}:{port}/metrics")
    return server, thread


def stop_prometheus_server(server, thread) -> None:
    """Stop the Prometheus HTTP server if running."""
    if not server:
        return
    try:
        server.shutdown()
        server.server_close()
    except Exception:  # pragma: no cover
        pass  # pragma: no cover
