"""
Run-log persistence for LakeLogic.

Handles writing run reports to JSON files and multi-backend table targets
(Spark, DuckDB, SQLite, Delta, Iceberg), as well as reading watermarks for incremental loads.

Supports cloud storage paths (ADLS, S3, GCS) via fsspec for JSON run logs.

Extracted from materialization.py to keep concerns focused.
"""

import datetime
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from loguru import logger


# ── Cloud path helpers ────────────────────────────────────────────────────────

_CLOUD_PREFIXES = ("abfss://", "abfs://", "s3://", "s3a://", "gs://", "gcs://")


def _is_cloud_path(path: str) -> bool:
    """Return True if the path is a cloud storage URI (ADLS, S3, GCS)."""
    return any(str(path).startswith(prefix) for prefix in _CLOUD_PREFIXES)


def _build_cloud_opts(path: str) -> Dict[str, str]:
    """Build fsspec storage_options from environment variables for a cloud path.

    For Azure ``abfss://container@account.dfs.core.windows.net/...`` URIs,
    adlfs automatically extracts ``account_name`` from the URL.  We only
    inject ``account_name`` when it cannot be inferred from the URI, and
    always pass ``account_key`` when available.
    """
    import os

    opts: Dict[str, str] = {}
    p = str(path).lower()
    if p.startswith(("abfss://", "abfs://")):
        # Detect whether account_name is embedded in the URL
        uri_has_account = "@" in p.split("//", 1)[-1].split("/", 1)[0]

        acct = os.getenv("AZURE_STORAGE_ACCOUNT_NAME") or os.getenv("AZURE_STORAGE_ACCOUNT")
        if acct and not uri_has_account:
            opts["account_name"] = acct

        acct_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
        if acct_key:
            opts["account_key"] = acct_key

        for env_key, opt_key in [
            ("AZURE_TENANT_ID", "tenant_id"),
            ("AZURE_CLIENT_ID", "client_id"),
            ("AZURE_CLIENT_SECRET", "client_secret"),
        ]:
            val = os.getenv(env_key)
            if val:
                opts[opt_key] = val
    elif p.startswith(("s3://", "s3a://")):
        for env_key, opt_key in [
            ("AWS_ACCESS_KEY_ID", "key"),
            ("AWS_SECRET_ACCESS_KEY", "secret"),
        ]:
            val = os.getenv(env_key)
            if val:
                opts[opt_key] = val
    return opts


def _cloud_write_json(cloud_path: str, data: Dict[str, Any]) -> None:
    """
    Write a JSON dict to a cloud storage path using fsspec.

    Args:
        cloud_path: Cloud URI (abfss://, s3://, gs://).
        data: Dict to serialize as JSON.

    Raises:
        ImportError: If fsspec is not installed.
    """
    import fsspec

    opts = _build_cloud_opts(cloud_path)
    with fsspec.open(cloud_path, "w", encoding="utf-8", **opts) as f:
        json.dump(data, f, indent=2, default=str)


def _cloud_read_json(cloud_path: str) -> Optional[Dict[str, Any]]:
    """
    Read a JSON file from cloud storage using fsspec.

    Args:
        cloud_path: Cloud URI (abfss://, s3://, gs://).

    Returns:
        Parsed dict or None on failure.
    """
    try:
        import fsspec

        opts = _build_cloud_opts(cloud_path)
        with fsspec.open(cloud_path, "r", encoding="utf-8", **opts) as f:
            return json.load(f)
    except Exception:
        return None


def _cloud_list_json(cloud_dir: str, pattern: str = "run_*.json") -> List[str]:
    """
    List JSON files in a cloud directory matching a glob pattern.

    Args:
        cloud_dir: Cloud directory URI.
        pattern: Glob pattern for filenames.

    Returns:
        List of full cloud paths, sorted newest first (by name, descending).
    """
    try:
        import fsspec

        # Normalize trailing slash
        cloud_dir = cloud_dir.rstrip("/") + "/"
        opts = _build_cloud_opts(cloud_dir)
        fs, fs_path = fsspec.core.url_to_fs(cloud_dir, **opts)

        fs_path = fs_path.rstrip("/")
        matches = fs.glob(f"{fs_path}/{pattern}")

        def get_mtime(m):
            try:
                info = fs.info(m)
                dt = (
                    info.get("last_modified")
                    or info.get("LastModified")
                    or info.get("updated")
                    or info.get("mtime")
                    or 0
                )
                if hasattr(dt, "timestamp"):
                    return dt.timestamp()
                return float(dt)
            except Exception:
                return 0

        sorted_matches = sorted(matches, key=get_mtime, reverse=True)
        # Reconstruct full URI. We know each match is just a file in cloud_dir.
        return [f"{cloud_dir}{m.split('/')[-1]}" for m in sorted_matches]
    except Exception:
        return []


def _cloud_install_hint(path: str) -> str:
    """Return the specific pip install command for a cloud path's provider."""
    p = str(path).lower()
    if p.startswith(("abfss://", "abfs://")):
        return "fsspec adlfs"
    if p.startswith(("s3://", "s3a://")):
        return "fsspec s3fs"
    if p.startswith(("gs://", "gcs://")):
        return "fsspec gcsfs"
    return "fsspec"


# ── Shared helpers (duplicated intentionally to keep run_log self-contained) ──


def _resolve_path(raw_path: str, base_path: Optional[Path]) -> Path:
    """Resolve a path, honoring the contract base path for relative values."""
    path = Path(raw_path)
    if not path.is_absolute() and base_path:
        path = base_path / path
    return path


def _prepare_table_name(name: str, backend: str) -> str:
    """Normalize table names for backend constraints (e.g., SQLite schemas)."""
    if backend == "sqlite":
        if "." in name:
            cleaned = name.replace(".", "_")
            logger.warning(f"SQLite does not support schemas. Using table name '{cleaned}' instead of '{name}'.")
            return cleaned
    return name


# ── Report flattening ──


def _flatten_report(report: Dict[str, Any]) -> Dict[str, Any]:
    """
    Flatten a run report into a row-oriented structure for table logging.

    The schema is intentionally compact (~22 top-level columns).  SLO
    metrics are consolidated into a single ``slo_json`` column, and
    rarely-queried detail fields are folded into ``report_json``.

    Top-level columns are reserved for fields that are frequently
    filtered, grouped, or joined on (identity, status, counts,
    watermark).  Everything else lives in JSON for drill-down.

    Args:
        report: Run report dict.

    Returns:
        Flat dict of run log fields.
    """
    counts = report.get("counts") or {}
    slos = report.get("slos") or {}
    freshness = slos.get("freshness") or {}
    availability = slos.get("availability") or {}

    def _num(value):
        """Coerce a value to float when possible."""
        try:
            return float(value) if value is not None else None
        except Exception:
            return None

    def _int(value):
        """Coerce a value to int when possible."""
        try:
            return int(value) if value is not None else None
        except Exception:
            return None

    # ── SLO metrics → single JSON column ──────────────────────────────
    slo_obj = {
        "freshness": {
            "seconds": _num(freshness.get("age_seconds")),
            "pass": freshness.get("passed"),
            "threshold_seconds": _num(freshness.get("threshold_seconds")),
            "source_seconds": _num(freshness.get("source_age_seconds")),
            "source_pass": freshness.get("source_passed"),
        },
        "availability": {
            "ratio": _num(availability.get("ratio")),
            "pass": availability.get("passed"),
            "threshold": _num(availability.get("threshold")),
        },
        "row_count": {
            "min": _int(report.get("slo_row_count_min")),
            "max": _int(report.get("slo_row_count_max")),
            "anomaly_pass": report.get("slo_row_count_anomaly_pass"),
            "anomaly_ratio": _num(report.get("slo_row_count_anomaly_ratio")),
        },
        "quality": {
            "pass": report.get("slo_quality_pass"),
            "ratio": _num(report.get("slo_quality_ratio")),
            "severity": report.get("slo_quality_severity"),
        },
        "schedule": {
            "pass": report.get("slo_schedule_pass"),
            "duration_seconds": _num(report.get("slo_duration_seconds")),
        },
    }

    return {
        # ── Identity ──────────────────────────────────────────────────
        "pipeline_run_id": report.get("pipeline_run_id"),
        "run_id": report.get("run_id"),
        "timestamp": report.get("timestamp"),
        "start_time": report.get("start_time"),
        "end_time": report.get("end_time"),
        "run_duration_seconds": _num(report.get("run_duration_seconds")),
        # ── Context ───────────────────────────────────────────────────
        "engine": report.get("engine"),
        "contract": report.get("contract"),
        "contract_version": report.get("contract_version"),
        "stage": report.get("stage"),
        "dataset": report.get("dataset"),
        "domain": report.get("domain"),
        "system": report.get("system"),
        "environment": report.get("environment"),
        "data_layer": report.get("data_layer"),
        "status": report.get("status"),
        "error_message": report.get("error_message"),
        "source_path": report.get("source_path"),
        # ── Counts (high-frequency dashboard metrics) ─────────────────
        "counts_source": counts.get("source"),
        "counts_total": counts.get("total"),
        "counts_good": counts.get("good"),
        "counts_quarantined": counts.get("quarantined"),
        "counts_aggregated": _int(counts.get("aggregated_rows")),
        "counts_dropped": _int(counts.get("pre_transform_dropped")),
        "quarantine_ratio": _num(counts.get("quarantine_ratio")),
        # ── Cost observability ────────────────────────────────────────
        "estimated_cost": _num(report.get("estimated_cost")),
        "cost_currency": report.get("cost_currency"),
        "cost_confidence": report.get("cost_confidence"),
        # ── Watermark (critical for incremental loads) ────────────────
        "max_source_mtime": report.get("max_source_mtime"),
        "max_watermark_value": report.get("max_watermark_value"),
        # ── Consolidated JSON columns ─────────────────────────────────
        "dlt_state_json": report.get("dlt_state_json"),
        "slo_json": json.dumps(slo_obj, default=str),
        "report_json": json.dumps(report, default=str),
    }


# ── Table write ──


def _write_run_log_table(report: Dict[str, Any], contract, engine_name: Optional[str] = None) -> Optional[str]:
    """
    Append a run report into a table backend (Spark/DuckDB/SQLite).

    Args:
        report: Run report dict.
        contract: DataContract with metadata.
        engine_name: Engine name for backend defaults.

    Returns:
        Identifier of the table target written to, if any.
    """
    metadata = contract.metadata or {}
    table_name = metadata.get("run_log_table")
    if not table_name:
        return None

    backend = (metadata.get("run_log_backend") or "").lower()
    if not backend:
        backend = "spark" if engine_name == "spark" else "delta"

    # When running on Spark, write an Iceberg run-log table THROUGH Spark's own
    # Iceberg support (the SparkSession's already-configured catalog) rather than
    # pyiceberg — which needs a separately-provided catalog URI
    # (PYICEBERG_CATALOG__DEFAULT__URI) and otherwise fails. This mirrors the
    # medallion and quarantine writers so all three land in the same catalog.
    if backend == "iceberg" and engine_name == "spark":
        metadata = {**metadata, "run_log_table_format": metadata.get("run_log_table_format") or "iceberg"}
        backend = "spark"

    record = _flatten_report(report)

    if backend == "spark":
        try:
            from pyspark.sql import SparkSession
        except Exception as exc:
            logger.warning(f"Run log table backend 'spark' unavailable: {exc}")
            return None

        spark = SparkSession.builder.getOrCreate()
        parts = table_name.split(".")
        if len(parts) == 2:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {parts[0]}")
        elif len(parts) >= 3:
            schema = ".".join(parts[:-1])
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

        # Build explicit schema to avoid CANNOT_DETERMINE_TYPE when
        # record values are None (Spark cannot infer type from null).
        from pyspark.sql.types import (
            StructType,
            StructField,
            StringType,
            LongType,
            DoubleType,
        )

        _run_log_schema = StructType(
            [
                StructField("pipeline_run_id", StringType(), True),
                StructField("run_id", StringType(), True),
                StructField("timestamp", StringType(), True),
                StructField("start_time", StringType(), True),
                StructField("end_time", StringType(), True),
                StructField("run_duration_seconds", DoubleType(), True),
                StructField("engine", StringType(), True),
                StructField("contract", StringType(), True),
                StructField("contract_version", StringType(), True),
                StructField("stage", StringType(), True),
                StructField("dataset", StringType(), True),
                StructField("domain", StringType(), True),
                StructField("system", StringType(), True),
                StructField("environment", StringType(), True),
                StructField("data_layer", StringType(), True),
                StructField("status", StringType(), True),
                StructField("error_message", StringType(), True),
                StructField("source_path", StringType(), True),
                StructField("counts_source", LongType(), True),
                StructField("counts_total", LongType(), True),
                StructField("counts_good", LongType(), True),
                StructField("counts_quarantined", LongType(), True),
                StructField("quarantine_ratio", DoubleType(), True),
                StructField("estimated_cost", DoubleType(), True),
                StructField("cost_currency", StringType(), True),
                StructField("cost_confidence", StringType(), True),
                StructField("max_source_mtime", DoubleType(), True),
                StructField("max_watermark_value", StringType(), True),
                StructField("dlt_state_json", StringType(), True),
                StructField("slo_json", StringType(), True),
                StructField("report_json", StringType(), True),
            ]
        )
        # Only include fields that are defined in the schema
        _schema_fields = {f.name for f in _run_log_schema.fields}
        _typed_record = {k: v for k, v in record.items() if k in _schema_fields}
        # Add any extra record keys not in schema as StringType
        _extra_fields = []
        for k, v in record.items():
            if k not in _schema_fields:
                _extra_fields.append(StructField(k, StringType(), True))
                _typed_record[k] = str(v) if v is not None else None
        if _extra_fields:
            _run_log_schema = StructType(_run_log_schema.fields + _extra_fields)

        # Pass dict directly so createDataFrame maps by key name,
        # avoiding positional mapping errors that occur with Row(**kwargs).
        df = spark.createDataFrame([_typed_record], schema=_run_log_schema)
        merge_on_run_id = metadata.get("run_log_merge_on_run_id", True)
        table_format = metadata.get("run_log_table_format") or "delta"

        # run_log_table_partition_by: list of column names to partition the table.
        # Applied only on first-write (CREATE); existing tables inherit their
        # existing partition spec and the MERGE / append writes honour it automatically.
        # Useful columns: domain, system, data_layer, contract, stage
        partition_by: list = metadata.get("run_log_table_partition_by") or []
        if isinstance(partition_by, str):
            partition_by = [c.strip() for c in partition_by.split(",") if c.strip()]

        # Validate that requested partition columns exist in the record
        unknown_parts = [c for c in partition_by if c not in record]
        if unknown_parts:
            logger.warning(
                f"run_log_table_partition_by references unknown columns {unknown_parts}. "
                f"Available: {sorted(record.keys())}"
            )
            partition_by = [c for c in partition_by if c in record]

        if spark.catalog.tableExists(table_name):
            try:
                existing_cols = set(spark.table(table_name).columns)
                missing_cols = []
                for col_name, col_type in [
                    ("pipeline_run_id", "STRING"),
                    ("start_time", "STRING"),
                    ("end_time", "STRING"),
                    ("run_duration_seconds", "DOUBLE"),
                    ("stage", "STRING"),
                    ("dataset", "STRING"),
                    ("domain", "STRING"),
                    ("system", "STRING"),
                    ("environment", "STRING"),
                    ("data_layer", "STRING"),
                    ("counts_source", "BIGINT"),
                    ("counts_good", "BIGINT"),
                    ("counts_quarantined", "BIGINT"),
                    ("quarantine_ratio", "DOUBLE"),
                    ("estimated_cost", "DOUBLE"),
                    ("cost_currency", "STRING"),
                    ("cost_confidence", "STRING"),
                    ("max_source_mtime", "DOUBLE"),
                    ("max_watermark_value", "STRING"),
                    ("status", "STRING"),
                    ("error_message", "STRING"),
                    ("dlt_state_json", "STRING"),
                    ("slo_json", "STRING"),
                ]:
                    if col_name not in existing_cols:
                        missing_cols.append(f"{col_name} {col_type}")
                if missing_cols:
                    spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS ({', '.join(missing_cols)})")
            except Exception as exc:
                logger.warning(f"Failed to align run log table schema for {table_name}: {exc}")
            if merge_on_run_id:
                view_name = f"lakelogic_run_log_updates_{uuid4().hex}"
                df.createOrReplaceTempView(view_name)
                try:
                    spark.sql(f"""
                        MERGE INTO {table_name} AS target
                        USING {view_name} AS source
                        ON target.run_id = source.run_id
                        WHEN MATCHED THEN UPDATE SET *
                        WHEN NOT MATCHED THEN INSERT *
                    """)
                except Exception as exc:
                    logger.warning(f"Run log merge failed for {table_name}: {exc}")
                    return None
                finally:
                    try:
                        spark.catalog.dropTempView(view_name)
                    except Exception:
                        pass
            else:
                # Table already exists — append without re-partitioning
                df.write.mode("append").format(table_format).saveAsTable(table_name)
        else:
            # First write: apply format + partition spec
            writer = df.write.mode("overwrite").format(table_format)
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            writer.saveAsTable(table_name)
            if partition_by:
                logger.info(f"Created run log table {table_name} partitioned by {partition_by} (format={table_format})")
        logger.info(f"Wrote run log to Spark table {table_name}")
        return table_name

    if backend == "duckdb":
        try:
            import duckdb
        except Exception as exc:
            logger.warning(f"Run log table backend 'duckdb' unavailable: {exc}")
            return None

        # Storage path — resolved by registry placeholders, not by the contract YAML dir.

        base_path = None  # see materialization.py and quarantine.py for rationale

        # When `run_log_table` is a filesystem-style path (e.g. resolved from
        # `{log_path}` → `./lakehouse_polars/marketplace/_logs`), treat the path
        # as the directory for the .duckdb file and use a default `run_logs`
        # table inside it. This lets users keep one set of storage variables
        # in _system.yaml whether they pick delta or duckdb as the backend.
        _looks_like_path = "/" in table_name or "\\" in table_name or table_name.startswith(".") or "://" in table_name

        if _looks_like_path:
            dir_path = _resolve_path(str(table_name), base_path)
            dir_path.mkdir(parents=True, exist_ok=True)
            db_path = dir_path / "lakelogic_run_logs.duckdb"
            schema_name = None
            table_only = "run_logs"
        else:
            db_path = metadata.get("run_log_database") or "logs/lakelogic_run_logs.duckdb"
            db_path = _resolve_path(str(db_path), base_path)
            db_path.parent.mkdir(parents=True, exist_ok=True)

            table_name = _prepare_table_name(table_name, backend)
            schema_name = None
            table_only = table_name
            parts = table_name.split(".")
            if len(parts) >= 2:
                schema_name = parts[-2]
                table_only = parts[-1]
                logger.warning(
                    f"DuckDB backend uses schema '{schema_name}' and table "
                    f"'{table_only}' (ignoring catalog parts if provided)."
                )
        con = duckdb.connect(database=str(db_path))
        if schema_name:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            full_table = f"{schema_name}.{table_only}"
        else:
            full_table = table_only
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {full_table} (
                pipeline_run_id VARCHAR,
                run_id VARCHAR,
                timestamp VARCHAR,
                start_time VARCHAR,
                end_time VARCHAR,
                run_duration_seconds DOUBLE,
                engine VARCHAR,
                contract VARCHAR,
                stage VARCHAR,
                dataset VARCHAR,
                domain VARCHAR,
                system VARCHAR,
                environment VARCHAR,
                data_layer VARCHAR,
                status VARCHAR,
                error_message VARCHAR,
                source_path VARCHAR,
                counts_source BIGINT,
                counts_total BIGINT,
                counts_good BIGINT,
                counts_quarantined BIGINT,
                quarantine_ratio DOUBLE,
                estimated_cost DOUBLE,
                cost_currency VARCHAR,
                cost_confidence VARCHAR,
                max_source_mtime DOUBLE,
                max_watermark_value VARCHAR,
                dlt_state_json VARCHAR,
                slo_json VARCHAR,
                report_json VARCHAR
            )
        """)
        try:
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS pipeline_run_id VARCHAR")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS start_time VARCHAR")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS end_time VARCHAR")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS run_duration_seconds DOUBLE")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS stage VARCHAR")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS dataset VARCHAR")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS domain VARCHAR")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS system VARCHAR")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS environment VARCHAR")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS data_layer VARCHAR")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS counts_source BIGINT")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS counts_good BIGINT")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS counts_quarantined BIGINT")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS quarantine_ratio DOUBLE")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS estimated_cost DOUBLE")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS cost_currency VARCHAR")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS cost_confidence VARCHAR")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS max_source_mtime DOUBLE")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS max_watermark_value VARCHAR")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS dlt_state_json VARCHAR")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS slo_json VARCHAR")
        except Exception:
            pass
        columns = [
            "pipeline_run_id",
            "run_id",
            "timestamp",
            "start_time",
            "end_time",
            "run_duration_seconds",
            "engine",
            "contract",
            "stage",
            "dataset",
            "domain",
            "system",
            "environment",
            "data_layer",
            "status",
            "error_message",
            "source_path",
            "counts_source",
            "counts_total",
            "counts_good",
            "counts_quarantined",
            "quarantine_ratio",
            "estimated_cost",
            "cost_currency",
            "cost_confidence",
            "max_source_mtime",
            "max_watermark_value",
            "dlt_state_json",
            "slo_json",
            "report_json",
        ]
        values = [record.get(col) for col in columns]
        placeholders = ", ".join(["?"] * len(columns))
        con.execute(
            f"INSERT INTO {full_table} ({', '.join(columns)}) VALUES ({placeholders})",
            values,
        )
        con.close()
        logger.info(f"Wrote run log to DuckDB table {full_table} ({db_path})")
        return f"{db_path}:{full_table}"

    if backend == "sqlite":
        import sqlite3

        # Storage path — resolved by registry placeholders, not by the contract YAML dir.

        base_path = None  # see materialization.py and quarantine.py for rationale
        db_path = metadata.get("run_log_database") or "logs/lakelogic_run_logs.sqlite"
        db_path = _resolve_path(str(db_path), base_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        table_name = _prepare_table_name(table_name, backend)
        con = sqlite3.connect(str(db_path))
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                pipeline_run_id TEXT,
                run_id TEXT,
                timestamp TEXT,
                start_time TEXT,
                end_time TEXT,
                run_duration_seconds REAL,
                engine TEXT,
                contract TEXT,
                stage TEXT,
                dataset TEXT,
                domain TEXT,
                system TEXT,
                environment TEXT,
                data_layer TEXT,
                status TEXT,
                error_message TEXT,
                source_path TEXT,
                counts_source INTEGER,
                counts_total INTEGER,
                counts_good INTEGER,
                counts_quarantined INTEGER,
                quarantine_ratio REAL,
                estimated_cost REAL,
                cost_currency TEXT,
                cost_confidence TEXT,
                max_source_mtime REAL,
                max_watermark_value TEXT,
                dlt_state_json TEXT,
                slo_json TEXT,
                report_json TEXT
            )
        """)
        try:
            cols = [row[1] for row in con.execute(f"PRAGMA table_info({table_name})").fetchall()]
            if "pipeline_run_id" not in cols:
                con.execute(f"ALTER TABLE {table_name} ADD COLUMN pipeline_run_id TEXT")
            for col_name, col_type in [
                ("start_time", "TEXT"),
                ("end_time", "TEXT"),
                ("run_duration_seconds", "REAL"),
                ("stage", "TEXT"),
                ("dataset", "TEXT"),
                ("domain", "TEXT"),
                ("system", "TEXT"),
                ("environment", "TEXT"),
                ("data_layer", "TEXT"),
                ("counts_source", "INTEGER"),
                ("counts_good", "INTEGER"),
                ("counts_quarantined", "INTEGER"),
                ("quarantine_ratio", "REAL"),
                ("estimated_cost", "REAL"),
                ("cost_currency", "TEXT"),
                ("cost_confidence", "TEXT"),
                ("max_source_mtime", "REAL"),
                ("max_watermark_value", "TEXT"),
                ("dlt_state_json", "TEXT"),
                ("slo_json", "TEXT"),
            ]:
                if col_name not in cols:
                    con.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}")
        except Exception:
            pass
        columns = [
            "pipeline_run_id",
            "run_id",
            "timestamp",
            "start_time",
            "end_time",
            "run_duration_seconds",
            "engine",
            "contract",
            "stage",
            "dataset",
            "domain",
            "system",
            "environment",
            "data_layer",
            "status",
            "error_message",
            "source_path",
            "counts_source",
            "counts_total",
            "counts_good",
            "counts_quarantined",
            "quarantine_ratio",
            "estimated_cost",
            "cost_currency",
            "cost_confidence",
            "max_source_mtime",
            "max_watermark_value",
            "dlt_state_json",
            "slo_json",
            "report_json",
        ]
        values = [record.get(col) for col in columns]
        placeholders = ", ".join(["?"] * len(columns))
        con.execute(
            f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})",
            values,
        )
        con.commit()
        con.close()
        logger.info(f"Wrote run log to SQLite table {table_name} ({db_path})")
        return f"{db_path}:{table_name}"

    if backend == "delta":
        # Normalize to POSIX separators — table_name is a logical/URI path,
        # not an OS filesystem path, so backslashes from Windows must be fixed.
        table_name = table_name.replace("\\", "/")

        try:
            import pyarrow as pa
            from deltalake import DeltaTable, write_deltalake
        except ImportError as exc:
            logger.warning(
                f"Run log table backend 'delta' requires 'deltalake' and 'pyarrow'. "
                f"Install with: pip install deltalake pyarrow. Error: {exc}"
            )
            return None

        # Guard: reject unresolved or invalid table paths
        if not table_name or table_name == "None" or "{" in table_name:
            logger.warning(
                f"Run log table path not fully resolved: '{table_name}'. "
                f"Check that metadata.run_log_table template variables (e.g. {{log_path}}) "
                f"are defined in _system.yaml storage and environments."
            )
            return None

        # Build storage options for cloud paths
        storage_options = _build_cloud_opts(table_name) if _is_cloud_path(table_name) else None

        # Convert flat record to Arrow table
        schema = pa.schema(
            [
                ("pipeline_run_id", pa.string()),
                ("run_id", pa.string()),
                ("timestamp", pa.string()),
                ("start_time", pa.string()),
                ("end_time", pa.string()),
                ("run_duration_seconds", pa.float64()),
                ("engine", pa.string()),
                ("contract", pa.string()),
                ("stage", pa.string()),
                ("dataset", pa.string()),
                ("domain", pa.string()),
                ("system", pa.string()),
                ("environment", pa.string()),
                ("data_layer", pa.string()),
                ("status", pa.string()),
                ("error_message", pa.string()),
                ("source_path", pa.string()),
                ("counts_source", pa.int64()),
                ("counts_total", pa.int64()),
                ("counts_good", pa.int64()),
                ("counts_quarantined", pa.int64()),
                ("quarantine_ratio", pa.float64()),
                ("estimated_cost", pa.float64()),
                ("cost_currency", pa.string()),
                ("cost_confidence", pa.string()),
                ("max_source_mtime", pa.float64()),
                ("max_watermark_value", pa.string()),
                ("dlt_state_json", pa.string()),
                ("slo_json", pa.string()),
                ("report_json", pa.string()),
            ]
        )

        arrays = []
        for field in schema:
            val = record.get(field.name)
            arrays.append(pa.array([val], type=field.type))
        arrow_table = pa.table(arrays, schema=schema)

        merge_on_run_id = metadata.get("run_log_merge_on_run_id", True)

        try:
            from deltalake import DeltaTable, write_deltalake

            def _safe_write_deltalake(target, data, **kwargs):
                if hasattr(data, "__len__") and len(data) == 0:
                    kwargs.pop("schema_mode", None)
                    kwargs.pop("engine", None)

                import inspect

                sig = inspect.signature(write_deltalake)
                if "engine" in sig.parameters and kwargs.get("schema_mode") == "merge":
                    kwargs["engine"] = "rust"
                elif "engine" not in sig.parameters and "engine" in kwargs:
                    del kwargs["engine"]
                write_deltalake(target, data, **kwargs)

            # Handle different deltalake versions
            def check_is_deltatable(target, storage_options=None):
                try:
                    DeltaTable(target, storage_options=storage_options)
                    return True
                except Exception:
                    return False

            import time
            import random

            max_log_retries = 5
            last_exc = None

            for attempt in range(max_log_retries):
                try:
                    if check_is_deltatable(table_name, storage_options=storage_options):
                        if merge_on_run_id:
                            dt = DeltaTable(table_name, storage_options=storage_options)
                            (
                                dt.merge(
                                    source=arrow_table,
                                    predicate="target.run_id = source.run_id",
                                    source_alias="source",
                                    target_alias="target",
                                )
                                .when_matched_update_all()
                                .when_not_matched_insert_all()
                                .execute()
                            )
                        else:
                            _safe_write_deltalake(
                                table_name,
                                arrow_table,
                                mode="append",
                                storage_options=storage_options,
                                schema_mode="merge",
                            )
                    else:
                        _safe_write_deltalake(
                            table_name,
                            arrow_table,
                            mode="overwrite",
                            storage_options=storage_options,
                        )
                    logger.info(f"Wrote run log to Delta table {table_name}")
                    return table_name
                except Exception as exc:
                    last_exc = exc
                    time.sleep((2**attempt) * 0.1 + random.uniform(0.05, 0.2))

            logger.warning(f"Failed to write run log to Delta table {table_name}: {last_exc}")
            return None
        except Exception as outer_exc:
            logger.warning(f"Failed to write run log to Delta table {table_name}: {outer_exc}")
            return None

    if backend == "iceberg":
        try:
            from pyiceberg.catalog import load_catalog
            import pyarrow as pa
        except ImportError as exc:
            logger.warning(
                f"Run log table backend 'iceberg' requires 'pyiceberg' and 'pyarrow'. "
                f"Install with: pip install pyiceberg pyarrow. Error: {exc}"
            )
            return None

        catalog_name = metadata.get("iceberg_catalog_name") or os.getenv("ICEBERG_CATALOG_NAME", "default")
        catalog_props = {}
        catalog_uri = metadata.get("iceberg_catalog_uri") or os.getenv("ICEBERG_CATALOG_URI")
        if catalog_uri:
            catalog_props["uri"] = catalog_uri
        catalog_warehouse = metadata.get("iceberg_catalog_warehouse") or os.getenv("ICEBERG_CATALOG_WAREHOUSE")
        if catalog_warehouse:
            catalog_props["warehouse"] = catalog_warehouse

        catalog = load_catalog(catalog_name, **catalog_props)

        # Reuse the Arrow schema from the delta backend
        schema = pa.schema(
            [
                ("pipeline_run_id", pa.string()),
                ("run_id", pa.string()),
                ("timestamp", pa.string()),
                ("start_time", pa.string()),
                ("end_time", pa.string()),
                ("run_duration_seconds", pa.float64()),
                ("engine", pa.string()),
                ("contract", pa.string()),
                ("stage", pa.string()),
                ("dataset", pa.string()),
                ("domain", pa.string()),
                ("system", pa.string()),
                ("environment", pa.string()),
                ("data_layer", pa.string()),
                ("status", pa.string()),
                ("error_message", pa.string()),
                ("source_path", pa.string()),
                ("counts_source", pa.int64()),
                ("counts_total", pa.int64()),
                ("counts_good", pa.int64()),
                ("counts_quarantined", pa.int64()),
                ("quarantine_ratio", pa.float64()),
                ("estimated_cost", pa.float64()),
                ("cost_currency", pa.string()),
                ("cost_confidence", pa.string()),
                ("max_source_mtime", pa.float64()),
                ("max_watermark_value", pa.string()),
                ("dlt_state_json", pa.string()),
                ("slo_json", pa.string()),
                ("report_json", pa.string()),
            ]
        )

        arrays = []
        for field in schema:
            val = record.get(field.name)
            arrays.append(pa.array([val], type=field.type))
        arrow_table = pa.table(arrays, schema=schema)

        # Parse namespace and table
        parts = table_name.split(".")
        if len(parts) >= 2:
            full_id = table_name
        else:
            full_id = f"default.{table_name}"

        try:
            iceberg_table = catalog.load_table(full_id)
            iceberg_table.append(arrow_table)
        except Exception:
            iceberg_table = catalog.create_table(full_id, schema=arrow_table.schema)
            iceberg_table.append(arrow_table)

        logger.info(f"Wrote run log to Iceberg table {full_id}")
        return full_id

    if backend == "dlt":
        try:
            import dlt as _dlt
            import pyarrow as pa
        except ImportError as exc:
            logger.warning(
                f"Run log table backend 'dlt' requires 'dlt' and 'pyarrow'. "
                f"Install with: pip install dlt pyarrow. Error: {exc}"
            )
            return None

        # Build Arrow schema for run log
        schema = pa.schema(
            [
                ("pipeline_run_id", pa.string()),
                ("run_id", pa.string()),
                ("timestamp", pa.string()),
                ("start_time", pa.string()),
                ("end_time", pa.string()),
                ("run_duration_seconds", pa.float64()),
                ("engine", pa.string()),
                ("contract", pa.string()),
                ("stage", pa.string()),
                ("dataset", pa.string()),
                ("domain", pa.string()),
                ("system", pa.string()),
                ("environment", pa.string()),
                ("data_layer", pa.string()),
                ("status", pa.string()),
                ("error_message", pa.string()),
                ("source_path", pa.string()),
                ("counts_source", pa.int64()),
                ("counts_total", pa.int64()),
                ("counts_good", pa.int64()),
                ("counts_quarantined", pa.int64()),
                ("quarantine_ratio", pa.float64()),
                ("estimated_cost", pa.float64()),
                ("cost_currency", pa.string()),
                ("cost_confidence", pa.string()),
                ("max_source_mtime", pa.float64()),
                ("max_watermark_value", pa.string()),
                ("dlt_state_json", pa.string()),
                ("slo_json", pa.string()),
                ("report_json", pa.string()),
            ]
        )

        arrays = []
        for field in schema:
            val = record.get(field.name)
            arrays.append(pa.array([val], type=field.type))
        arrow_table = pa.table(arrays, schema=schema)

        # Build dlt config from metadata
        dlt_config = {k: v for k, v in metadata.items() if k.startswith("dlt_")}
        destination = dlt_config.get("dlt_destination", "duckdb")
        dataset_name = dlt_config.get("dlt_dataset_name", "run_logs")
        credentials = dlt_config.get("dlt_credentials")

        dest_kwargs = {}
        if credentials:
            dest_kwargs["credentials"] = credentials
        for k, v in dlt_config.items():
            if k not in ("dlt_destination", "dlt_credentials", "dlt_dataset_name"):
                dest_kwargs[k[4:]] = v

        rl_table_name = table_name

        @_dlt.resource(
            name=rl_table_name,
            write_disposition="append",
        )
        def _rl_sink():
            yield arrow_table

        pipeline = _dlt.pipeline(
            pipeline_name=f"lakelogic_{rl_table_name}_run_log",
            destination=_dlt.destinations.__dict__.get(destination, destination)(**dest_kwargs)
            if dest_kwargs
            else destination,
            dataset_name=dataset_name,
        )

        pipeline.run(_rl_sink())
        logger.info(f"Wrote run log via dlt to {destination}:{dataset_name}.{rl_table_name}")
        return f"{destination}:{dataset_name}.{rl_table_name}"

    logger.warning(f"Unsupported run_log_backend: {backend}")
    return None


# ── SLO Checks table ──


_SLO_CHECKS_COLUMNS = [
    "check_run_id",
    "pipeline_run_id",
    "checked_at",
    "domain",
    "system",
    "layer",
    "entity",
    "check_type",
    "passed",
    "severity",
    "status",
    "delay_minutes",
    "slo_max_minutes",
    "source_delay_minutes",
    "source_slo_max_minutes",
    "source_column_used",
    "row_count",
    "slo_min_rows",
    "slo_max_rows",
    "anomaly_ratio",
    "anomaly_baseline",
    "quality_ratio",
    "quality_severity",
    "duration_seconds",
    "details_json",
]


def _flatten_slo_check(
    result: Any,
    check_run_id: str,
    pipeline_run_id: Optional[str],
    checked_at: str,
    domain: str,
    system: str,
) -> Dict[str, Any]:
    """Flatten one SLOCheckResult into a row dict for _slo_checks."""
    return {
        "check_run_id": check_run_id,
        "pipeline_run_id": pipeline_run_id,
        "checked_at": checked_at,
        "domain": domain,
        "system": system,
        "layer": result.layer,
        "entity": result.entity,
        "check_type": result.check_type,
        "passed": result.passed,
        "severity": result.severity,
        "status": result.status,
        "delay_minutes": result.delay_minutes,
        "slo_max_minutes": result.slo_max_minutes,
        "source_delay_minutes": result.source_delay_minutes,
        "source_slo_max_minutes": result.source_slo_max_minutes,
        "source_column_used": result.source_column_used,
        "row_count": result.row_count,
        "slo_min_rows": result.slo_min_rows,
        "slo_max_rows": result.slo_max_rows,
        "anomaly_ratio": result.anomaly_ratio,
        "anomaly_baseline": result.anomaly_baseline,
        "quality_ratio": result.quality_ratio,
        "quality_severity": result.quality_severity,
        "duration_seconds": result.duration_seconds,
        "details_json": result.model_dump_json(),
    }


def _write_slo_checks_table(
    registry: Any,
    records: List[Dict[str, Any]],
) -> Optional[str]:
    """
    Append a batch of SLO check results to the configured _slo_checks table.

    Reads slo_checks_table and slo_checks_backend from registry.storage /
    registry.metadata — mirroring the run_log_table / run_log_backend pattern.
    Pure append — no merge. check_run_id groups results from one invocation.
    """
    if not records:
        return None

    storage = getattr(registry, "storage", None)
    metadata = getattr(registry, "metadata", {}) or {}

    table_name = (getattr(storage, "slo_checks_table", None) if storage else None) or metadata.get("slo_checks_table")
    if not table_name:
        return None

    backend = (metadata.get("slo_checks_backend") or "delta").lower()

    # ── Spark ──────────────────────────────────────────────────────────────
    if backend == "spark":
        try:
            from pyspark.sql import SparkSession
            from pyspark.sql.types import (
                BooleanType,
                DoubleType,
                LongType,
                StringType,
                StructField,
                StructType,
            )
        except Exception as exc:
            logger.warning(f"SLO checks Spark backend unavailable: {exc}")
            return None

        spark = SparkSession.builder.getOrCreate()
        schema = StructType(
            [
                StructField("check_run_id", StringType(), True),
                StructField("pipeline_run_id", StringType(), True),
                StructField("checked_at", StringType(), True),
                StructField("domain", StringType(), True),
                StructField("system", StringType(), True),
                StructField("layer", StringType(), True),
                StructField("entity", StringType(), True),
                StructField("check_type", StringType(), True),
                StructField("passed", BooleanType(), True),
                StructField("severity", StringType(), True),
                StructField("status", StringType(), True),
                StructField("delay_minutes", DoubleType(), True),
                StructField("slo_max_minutes", LongType(), True),
                StructField("source_delay_minutes", DoubleType(), True),
                StructField("source_slo_max_minutes", LongType(), True),
                StructField("source_column_used", StringType(), True),
                StructField("row_count", LongType(), True),
                StructField("slo_min_rows", LongType(), True),
                StructField("slo_max_rows", LongType(), True),
                StructField("anomaly_ratio", DoubleType(), True),
                StructField("anomaly_baseline", DoubleType(), True),
                StructField("quality_ratio", DoubleType(), True),
                StructField("quality_severity", StringType(), True),
                StructField("duration_seconds", DoubleType(), True),
                StructField("details_json", StringType(), True),
            ]
        )
        df = spark.createDataFrame(records, schema=schema)
        if spark.catalog.tableExists(table_name):
            df.write.mode("append").format("delta").saveAsTable(table_name)
        else:
            df.write.mode("overwrite").format("delta").saveAsTable(table_name)
        logger.info(f"Wrote {len(records)} SLO check rows to Spark table {table_name}")
        return table_name

    # ── DuckDB ─────────────────────────────────────────────────────────────
    if backend == "duckdb":
        try:
            import duckdb
        except Exception as exc:
            logger.warning(f"SLO checks DuckDB backend unavailable: {exc}")
            return None

        # Same convenience as the run_log path-style fix: when the configured
        # slo_checks_table looks like a filesystem path (e.g. resolved from
        # `{slo_checks_path}` → `./lakehouse_polars/marketplace/_slo_checks`),
        # treat it as the directory for the .duckdb file and use a default
        # `slo_checks` table inside. Avoids `Parser Error: syntax error at "/"`.
        _looks_like_path = "/" in table_name or "\\" in table_name or table_name.startswith(".") or "://" in table_name

        if _looks_like_path:
            dir_path = Path(str(table_name))
            dir_path.mkdir(parents=True, exist_ok=True)
            db_path = dir_path / "lakelogic_slo_checks.duckdb"
            schema_name = None
            table_only = "slo_checks"
        else:
            db_path = (
                metadata.get("slo_checks_database")
                or metadata.get("run_log_database")
                or "logs/lakelogic_slo_checks.duckdb"
            )
            db_path = Path(db_path)
            db_path.parent.mkdir(parents=True, exist_ok=True)

            tbl = _prepare_table_name(table_name, "duckdb")
            parts = tbl.split(".")
            schema_name = parts[-2] if len(parts) >= 2 else None
            table_only = parts[-1]
        con = duckdb.connect(database=str(db_path))
        if schema_name:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            full_table = f"{schema_name}.{table_only}"
        else:
            full_table = table_only
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {full_table} (
                check_run_id VARCHAR, pipeline_run_id VARCHAR, checked_at VARCHAR,
                domain VARCHAR, system VARCHAR, layer VARCHAR, entity VARCHAR,
                check_type VARCHAR, passed BOOLEAN, severity VARCHAR, status VARCHAR,
                delay_minutes DOUBLE, slo_max_minutes BIGINT,
                source_delay_minutes DOUBLE, source_slo_max_minutes BIGINT,
                source_column_used VARCHAR, row_count BIGINT,
                slo_min_rows BIGINT, slo_max_rows BIGINT,
                anomaly_ratio DOUBLE, anomaly_baseline DOUBLE,
                quality_ratio DOUBLE, quality_severity VARCHAR,
                duration_seconds DOUBLE, details_json VARCHAR
            )
        """)
        placeholders = ", ".join(["?"] * len(_SLO_CHECKS_COLUMNS))
        for rec in records:
            values = [rec.get(c) for c in _SLO_CHECKS_COLUMNS]
            con.execute(
                f"INSERT INTO {full_table} ({', '.join(_SLO_CHECKS_COLUMNS)}) VALUES ({placeholders})",
                values,
            )
        con.close()
        logger.info(f"Wrote {len(records)} SLO check rows to DuckDB {db_path}:{full_table}")
        return f"{db_path}:{full_table}"

    # ── SQLite ─────────────────────────────────────────────────────────────
    if backend == "sqlite":
        import sqlite3

        db_path = metadata.get("slo_checks_database") or "logs/lakelogic_slo_checks.sqlite"
        db_path = Path(db_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        tbl = _prepare_table_name(table_name, "sqlite")
        con = sqlite3.connect(str(db_path))
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {tbl} (
                check_run_id TEXT, pipeline_run_id TEXT, checked_at TEXT,
                domain TEXT, system TEXT, layer TEXT, entity TEXT,
                check_type TEXT, passed INTEGER, severity TEXT, status TEXT,
                delay_minutes REAL, slo_max_minutes INTEGER,
                source_delay_minutes REAL, source_slo_max_minutes INTEGER,
                source_column_used TEXT, row_count INTEGER,
                slo_min_rows INTEGER, slo_max_rows INTEGER,
                anomaly_ratio REAL, anomaly_baseline REAL,
                quality_ratio REAL, quality_severity TEXT,
                duration_seconds REAL, details_json TEXT
            )
        """)
        placeholders = ", ".join(["?"] * len(_SLO_CHECKS_COLUMNS))
        for rec in records:
            values = [
                int(rec[c]) if c == "passed" and rec.get(c) is not None else rec.get(c) for c in _SLO_CHECKS_COLUMNS
            ]
            con.execute(
                f"INSERT INTO {tbl} ({', '.join(_SLO_CHECKS_COLUMNS)}) VALUES ({placeholders})",
                values,
            )
        con.commit()
        con.close()
        logger.info(f"Wrote {len(records)} SLO check rows to SQLite {db_path}:{tbl}")
        return f"{db_path}:{tbl}"

    # ── Delta (default) ────────────────────────────────────────────────────
    if backend == "delta":
        table_name = table_name.replace("\\", "/")

        if not table_name or table_name == "None" or "{" in table_name:
            logger.warning(
                f"SLO checks table path not fully resolved: '{table_name}'. "
                f"Check that metadata.slo_checks_table template variables are defined."
            )
            return None

        try:
            import pyarrow as pa
            from deltalake import DeltaTable, write_deltalake
        except ImportError as exc:
            logger.warning(f"SLO checks Delta backend requires 'deltalake' and 'pyarrow': {exc}")
            return None

        storage_options = _build_cloud_opts(table_name) if _is_cloud_path(table_name) else None

        schema = pa.schema(
            [
                ("check_run_id", pa.string()),
                ("pipeline_run_id", pa.string()),
                ("checked_at", pa.string()),
                ("domain", pa.string()),
                ("system", pa.string()),
                ("layer", pa.string()),
                ("entity", pa.string()),
                ("check_type", pa.string()),
                ("passed", pa.bool_()),
                ("severity", pa.string()),
                ("status", pa.string()),
                ("delay_minutes", pa.float64()),
                ("slo_max_minutes", pa.int64()),
                ("source_delay_minutes", pa.float64()),
                ("source_slo_max_minutes", pa.int64()),
                ("source_column_used", pa.string()),
                ("row_count", pa.int64()),
                ("slo_min_rows", pa.int64()),
                ("slo_max_rows", pa.int64()),
                ("anomaly_ratio", pa.float64()),
                ("anomaly_baseline", pa.float64()),
                ("quality_ratio", pa.float64()),
                ("quality_severity", pa.string()),
                ("duration_seconds", pa.float64()),
                ("details_json", pa.string()),
            ]
        )

        arrays = []
        for field in schema:
            vals = [rec.get(field.name) for rec in records]
            arrays.append(pa.array(vals, type=field.type))
        arrow_table = pa.table(arrays, schema=schema)

        import time
        import random

        for attempt in range(5):
            try:
                try:
                    DeltaTable(table_name, storage_options=storage_options)
                    exists = True
                except Exception:
                    exists = False

                write_deltalake(
                    table_name,
                    arrow_table,
                    mode="append" if exists else "overwrite",
                    storage_options=storage_options,
                    schema_mode="overwrite" if not exists else None,
                )
                logger.info(f"Wrote {len(records)} SLO check rows to Delta {table_name}")
                return table_name
            except Exception as exc:
                if attempt == 4:
                    logger.warning(f"Failed to write SLO checks to Delta {table_name}: {exc}")
                    return None
                time.sleep((2**attempt) * 0.1 + random.uniform(0.05, 0.2))

    logger.warning(f"Unsupported slo_checks_backend: {backend}")
    return None


def write_slo_checks(
    registry: Any,
    results: List[Any],
    check_run_id: str,
    pipeline_run_id: Optional[str] = None,
) -> Optional[str]:
    """
    Write SLO check results to the configured _slo_checks table.

    Args:
        registry:        DomainRegistry instance.
        results:         List of SLOCheckResult from SLOValidator.run_checks().
        check_run_id:    UUID grouping all results from one run_checks() call.
        pipeline_run_id: Optional FK to run_log — set when triggered post-pipeline,
                         None when running independently on a schedule.

    Returns:
        Table identifier written to, or None if not configured / write failed.
    """
    if not results:
        return None

    checked_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    domain = getattr(registry, "domain", "")
    system = getattr(registry, "system", "")

    records = [_flatten_slo_check(r, check_run_id, pipeline_run_id, checked_at, domain, system) for r in results]
    return _write_slo_checks_table(registry, records)


# ── Public API ──


def write_run_log(
    report: Dict[str, Any],
    contract,
    engine_name: Optional[str] = None,
    run_log_mode: Optional[str] = None,
) -> Optional[str]:
    """
    Write run logs to JSON and/or table backends.

    Args:
        report: Run report dict.
        contract: DataContract with metadata.
        engine_name: Engine name for backend defaults.
        run_log_mode: Runtime override for which backends to use.
            "dir"   — file-based only (run_log_dir / run_log_path)
            "table" — table-based only (run_log_table)
            "all"   — both (default when None)

    Returns:
        Path to the JSON log file if written.
    """
    if not report or not contract:
        return None

    metadata = contract.metadata or {}
    path_value = metadata.get("run_log_path")
    dir_value = metadata.get("run_log_dir")
    table_value = metadata.get("run_log_table")

    if not path_value and not dir_value and not table_value:
        return None

    # Normalise mode
    mode = (run_log_mode or "all").lower().strip()
    _write_file = mode in ("all", "dir", "file")
    _write_table = mode in ("all", "table")

    log_path = None
    if (path_value or dir_value) and _write_file:
        raw = str(path_value or dir_value)

        # Guard: skip if path contains unresolved template vars
        if "{" in raw:
            logger.warning(f"Run log path not fully resolved: '{raw}'. Check _system.yaml storage and environments.")
        elif _is_cloud_path(raw):
            # ── Cloud storage (ADLS / S3 / GCS) via fsspec ──
            if path_value:
                cloud_target = str(path_value)
            else:
                run_id = report.get("run_id", "unknown")
                cloud_target = raw.rstrip("/") + f"/run_{run_id}.json"
            try:
                _cloud_write_json(cloud_target, report)
                logger.info(f"Wrote run log to {cloud_target}")
                log_path = cloud_target  # type: ignore[assignment]
            except ImportError:
                _pkg = _cloud_install_hint(cloud_target)
                logger.warning(
                    f"Cloud run log path detected but required packages are not installed. "
                    f"Install with: pip install {_pkg}"
                )
            except Exception as exc:
                logger.warning(f"Failed to write cloud run log to {cloud_target}: {exc}")
        else:
            # ── Local filesystem ──
            # Storage path — resolved by registry placeholders, not by the contract YAML dir.

            base_path = None  # see materialization.py and quarantine.py for rationale
            if path_value:
                log_path = _resolve_path(str(path_value), base_path)
            else:
                run_id = report.get("run_id", "unknown")
                log_path = _resolve_path(str(dir_value), base_path) / f"run_{run_id}.json"

            log_path.parent.mkdir(parents=True, exist_ok=True)
            with open(log_path, "w", encoding="utf-8") as handle:
                json.dump(report, handle, indent=2, default=str)

            logger.info(f"Wrote run log to {log_path}")

    if _write_table:
        _write_run_log_table(report, contract, engine_name=engine_name)

    # ── Secondary target fan-out for run logs ────────────────────────────────────
    # If the contract inherited global secondary_targets (via materialization._all),
    # also write the run log record there so logs land in the same database.
    try:
        mat_cfg = getattr(contract, "materialization", None)
        sec_targets = getattr(mat_cfg, "secondary_targets", None) if mat_cfg else None
        if sec_targets and isinstance(sec_targets, list):
            import pyarrow as pa
            from lakelogic.core.materialization import write_to_secondary_targets

            # Flatten nested counts → flat columns (same as _write_run_log_table)
            _counts = report.get("counts") or {}
            _flat = dict(report)
            _flat["counts_source"] = _counts.get("source")
            _flat["counts_total"] = _counts.get("total")
            _flat["counts_good"] = _counts.get("good")
            _flat["counts_quarantined"] = _counts.get("quarantined")
            _qr = _counts.get("quarantine_ratio")
            _flat["quarantine_ratio"] = float(_qr) if _qr is not None else None

            # Build a single-row Arrow table from the flattened record
            _rl_cols = [
                ("pipeline_run_id", pa.string()),
                ("run_id", pa.string()),
                ("timestamp", pa.string()),
                ("start_time", pa.string()),
                ("end_time", pa.string()),
                ("run_duration_seconds", pa.float64()),
                ("engine", pa.string()),
                ("contract", pa.string()),
                ("stage", pa.string()),
                ("dataset", pa.string()),
                ("domain", pa.string()),
                ("system", pa.string()),
                ("environment", pa.string()),
                ("data_layer", pa.string()),
                ("status", pa.string()),
                ("error_message", pa.string()),
                ("source_path", pa.string()),
                ("counts_source", pa.int64()),
                ("counts_total", pa.int64()),
                ("counts_good", pa.int64()),
                ("counts_quarantined", pa.int64()),
                ("quarantine_ratio", pa.float64()),
            ]
            schema = pa.schema(_rl_cols)
            arrays = [pa.array([_flat.get(f.name)], type=f.type) for f in schema]
            arrow_tbl = pa.table(arrays, schema=schema)

            write_to_secondary_targets(
                sec_targets,
                arrow_tbl,
                "_run_logs",
                strategy="append",
            )
    except Exception as _sec_exc:
        logger.debug(f"Run log secondary fan-out skipped: {_sec_exc}")

    # ── Observatory Telemetry Push ────────────────────────────────────────────────
    observatory_cfg = getattr(contract, "observatory", None)
    if not observatory_cfg and hasattr(contract, "model_dump"):
        _dumped = contract.model_dump()
        observatory_cfg = _dumped.get("observatory")
        if not observatory_cfg:
            # Check model_extra for extra="allow" fields
            _extras = getattr(contract, "model_extra", None) or getattr(contract, "__fields_extra__", None) or {}
            observatory_cfg = _extras.get("observatory") if isinstance(_extras, dict) else None

    # Also check contract dict source (registry injects into contract_dict before parsing)
    if not observatory_cfg:
        _raw_dict = getattr(contract, "__dict__", {})
        observatory_cfg = _raw_dict.get("observatory")

    # Merge in env-var convenience config (${VAR} interpolation + the
    # LAKELOGIC_CLOUD_API_KEY / _ENDPOINT one-liner path). This runs even when
    # there is NO YAML observatory block, so a bare env var is enough to connect.
    from .observatory_spool import resolve_observatory_config

    observatory_cfg = resolve_observatory_config(observatory_cfg)

    logger.info(
        f"📡 [1/5] Observatory config resolved: enabled={observatory_cfg.get('enabled', False)}, "
        f"endpoint={bool(observatory_cfg.get('endpoint'))}, key={bool(observatory_cfg.get('api_key'))}"
    )

    if observatory_cfg and isinstance(observatory_cfg, dict) and observatory_cfg.get("enabled"):
        endpoint = observatory_cfg.get("endpoint")
        emit_on = observatory_cfg.get("emit_on", ["success", "partial", "failed"])
        target_envs = observatory_cfg.get("environments", [])
        target_layers = observatory_cfg.get("layers", [])

        current_env = report.get("environment", "unknown")
        current_layer = report.get("data_layer", "unknown")
        status = str(report.get("status", "unknown")).lower()
        # Normalise engine status aliases → emit_on values
        _status_aliases = {"succeeded": "success", "succeed": "success"}
        status = _status_aliases.get(status, status)

        # Check environment, layer, and status triggers
        is_target_env = not target_envs or current_env in target_envs
        is_target_layer = not target_layers or current_layer in target_layers
        is_target_status = status in [e.lower() for e in emit_on]

        logger.info(
            f"📡 [2/5] Filters: env={current_env} in {target_envs} → {is_target_env} | "
            f"layer={current_layer} in {target_layers} → {is_target_layer} | "
            f"status={status} in {emit_on} → {is_target_status}"
        )

        if endpoint and is_target_env and is_target_layer and is_target_status:
            try:
                import requests as _requests

                api_key = observatory_cfg.get("api_key")
                headers = {"Content-Type": "application/json"}
                if api_key:
                    headers["X-API-Key"] = api_key

                # ── Map engine report → RunLogIngest schema ──────────────
                _counts = report.get("counts") or {}
                _counts_good = _counts.get("good") or report.get("counts_good", 0) or 0
                _counts_source = _counts.get("source") or report.get("counts_source", 0) or 0
                _counts_quarantined = _counts.get("quarantined") or report.get("counts_quarantined", 0) or 0
                _counts_total = _counts.get("total") or report.get("counts_total", 0) or 0

                _quality_score = float(_counts_good) / float(_counts_source) if _counts_source > 0 else 1.0

                # Rule attribution: which rules failed and how often. Built from the
                # rule-annotation columns only — name, SQL, category, count. Failing
                # source rows are never captured or transmitted. The payload field is
                # named `quarantined_rows` for wire compatibility; it holds no rows.
                #
                # Sent by DEFAULT. It used to be gated behind `include_quarantine_sample`,
                # whose name implied it carried records — so it was defaulted off, and the
                # SaaS's whole failure-attribution path was dead for a default install:
                # a run reported "150 rows quarantined" with nothing saying which rule.
                # An explicit `include_quarantine_sample: false` is still honoured for
                # anyone who set it; the key is deprecated, not removed, because it ships
                # in existing _domain.yaml files.
                _raw_failures = report.get("row_rule_failures") or []
                _opted_out = observatory_cfg.get("include_quarantine_sample") is False
                _rule_failures = None if _opted_out else (_raw_failures[:50] or None)
                _rule_failures_truncated = bool(_rule_failures) and len(_raw_failures) > 50

                # Dataset-level rule outcomes carry their own pass/fail.
                _dataset_rules = report.get("dataset_rules") or []
                _dataset_failed = [r for r in _dataset_rules if isinstance(r, dict) and not r.get("passed")]

                # Rule counts. Emitted ONLY when the contract lets us count the rules that
                # were configured — an absent count must stay absent rather than become a
                # confident 0, which is what the Observatory rendered for every OSS run
                # before this ("0 evaluated, 0 passed, 0 failed" on runs that ran rules).
                _rules_evaluated = _rules_passed = _rules_failed = None
                try:
                    _quality_cfg = getattr(contract, "quality", None)
                    _row_rules = list(getattr(_quality_cfg, "row_rules", None) or []) if _quality_cfg else []
                    _cfg_dataset_rules = list(getattr(_quality_cfg, "dataset_rules", None) or []) if _quality_cfg else []
                    if _row_rules or _cfg_dataset_rules or _dataset_rules:
                        _rules_evaluated = len(_row_rules) + max(len(_cfg_dataset_rules), len(_dataset_rules))
                        # A rule is counted once whether it failed on 1 row or 10,000.
                        _rules_failed = len(_raw_failures) + len(_dataset_failed)
                        _rules_passed = max(0, _rules_evaluated - _rules_failed)
                except Exception:  # pragma: no cover - counts are best-effort, never fatal
                    _rules_evaluated = _rules_passed = _rules_failed = None

                # Contract version + a schema fingerprint, so the SaaS can correlate
                # an incident to the contract revision that shipped it (feeds
                # breaking-change / drift attribution). Fingerprint is best-effort.
                _contract_version = report.get("contract_version")
                _contract_fp = None
                try:
                    import hashlib as _hashlib

                    _fields = None
                    _model = getattr(contract, "model", None)
                    if _model is not None:
                        _fields = getattr(_model, "fields", None)
                    if _fields:
                        _sig = ",".join(
                            sorted(f"{getattr(fld, 'name', '')}:{getattr(fld, 'type', '')}" for fld in _fields)
                        )
                        _contract_fp = _hashlib.sha256(f"{_contract_version or ''}|{_sig}".encode("utf-8")).hexdigest()[
                            :16
                        ]
                except Exception:
                    _contract_fp = None

                payload = {
                    "contract_name": report.get("contract") or report.get("dataset"),
                    "status": status,
                    "engine": report.get("engine"),
                    "tier": report.get("data_layer"),
                    "started_at": report.get("start_time"),
                    "finished_at": report.get("end_time"),
                    "duration_seconds": report.get("run_duration_seconds"),
                    "rows_input": _counts_source,
                    "rows_valid": _counts_good,
                    "rows_quarantined": _counts_quarantined,
                    "rows_output": _counts_total,
                    "quality_score": round(_quality_score, 6),
                    "error_message": report.get("error_message"),
                    # Legacy field name — kept so older SaaS builds keep reading
                    # attribution. It holds rule descriptors, never rows.
                    "quarantined_rows": _rule_failures,
                    "metadata": {
                        "domain": report.get("domain"),
                        "system": report.get("system"),
                        "environment": report.get("environment"),
                        "source_path": report.get("source_path"),
                        "pipeline_run_id": report.get("pipeline_run_id"),
                        "run_id": report.get("run_id"),
                        "slo_json": report.get("slo_json"),
                        "contract_version": _contract_version,
                        "contract_fingerprint": _contract_fp,
                        # Attribution under its own name — what the SaaS's per-rule
                        # strategy actually looks for. Same list as `quarantined_rows`.
                        "row_rule_failures": _rule_failures,
                        "row_rule_failures_truncated": _rule_failures_truncated,
                        # Field NAMES only: {missing_fields, unknown_fields, policy,
                        # evolution}. The report has carried this all along and the
                        # emitter simply never mapped it, so the SaaS's schema-drift
                        # path could never fire.
                        "schema_drift": report.get("schema_drift") or None,
                        "dataset_rule_failures": _dataset_failed or None,
                    },
                    # Cost observability
                    "estimated_cost": report.get("estimated_cost"),
                    "cost_currency": report.get("cost_currency"),
                    "cost_confidence": report.get("cost_confidence"),
                }

                # Omitted, not zeroed, when the rules could not be counted: the SaaS
                # renders an absent count as "not reported" but a 0 as a measurement.
                if _rules_evaluated is not None:
                    payload["rules_evaluated"] = _rules_evaluated
                    payload["rules_passed"] = _rules_passed
                    payload["rules_failed"] = _rules_failed

                logger.info(f"📡 [3/5] Posting to {endpoint} (contract={payload['contract_name']})")

                # Fire and forget (short timeout to prevent blocking the pipeline)
                from .observatory_spool import flush_spool, spool_payload

                resp = _requests.post(endpoint, json=payload, headers=headers, timeout=3.0)
                logger.info(f"📡 [4/5] Response: HTTP {resp.status_code}")
                if resp.status_code < 300:
                    logger.info(f"📡 [5/5] ✅ Ingested: {resp.text[:200]}")
                    # SaaS just confirmed reachable — opportunistically drain any
                    # run logs buffered during a previous outage (bounded + time-
                    # budgeted, so a backlog can't stall the pipeline).
                    flush_spool(observatory_cfg, endpoint, headers)
                elif resp.status_code in (408, 429) or resp.status_code >= 500:
                    # Transient / server-side failure — buffer for retry on a
                    # later run instead of dropping (quarantine sample stripped).
                    spool_payload(observatory_cfg, payload)
                    logger.warning(
                        f"📡 [5/5] ⏳ Observatory {resp.status_code}; buffered run log for retry: {resp.text[:300]}"
                    )
                else:
                    # 4xx (bad payload / auth) — retrying won't help; drop.
                    logger.warning(
                        f"📡 [5/5] ❌ Observatory returned {resp.status_code} (not retried): {resp.text[:500]}"
                    )
            except Exception as exc:
                # Network error / timeout — buffer for retry (metadata only) so a
                # transient outage doesn't silently lose this run's telemetry.
                try:
                    from .observatory_spool import spool_payload as _spool_payload

                    _spool_payload(observatory_cfg, payload)
                except Exception:
                    pass
                logger.warning(f"📡 [ERR] Push failed ({exc}); buffered run log for retry")
        else:
            logger.info(
                f"📡 [SKIP] Push skipped: endpoint={bool(endpoint)}, "
                f"env_match={is_target_env}, layer_match={is_target_layer}, status_match={is_target_status}"
            )
    else:
        logger.info(
            f"📡 [SKIP] Observatory disabled or not configured: "
            f"cfg={observatory_cfg is not None}, "
            f"enabled={observatory_cfg.get('enabled') if isinstance(observatory_cfg, dict) else 'N/A'}"
        )

    return str(log_path) if log_path else None


def get_last_run_watermark(
    contract,
    contract_title: str,
    stage: str,
    engine_name: Optional[str] = None,
    dataset: Optional[str] = None,
    data_layer: Optional[str] = None,
) -> Optional[float]:
    """
    Fetch the last max_source_mtime for a contract from run logs.

    Uses ``dataset`` + ``data_layer`` for precise filtering when available,
    falling back to ``contract_title`` + ``stage`` for backward compatibility.
    """
    if not contract:
        return None

    metadata = contract.metadata or {}
    table_value = metadata.get("run_log_table")
    backend = (metadata.get("run_log_backend") or "").lower()
    if table_value and not backend:
        backend = "spark" if engine_name == "spark" else "delta"

    # Build filter criteria: prefer dataset + data_layer, fallback to contract + stage
    _use_precise = bool(dataset)

    if table_value and backend == "duckdb":
        try:
            import duckdb
        except Exception:
            return None
        # Storage path — resolved by registry placeholders, not by the contract YAML dir.

        base_path = None  # see materialization.py and quarantine.py for rationale
        db_path = metadata.get("run_log_database") or "logs/lakelogic_run_logs.duckdb"
        db_path = _resolve_path(str(db_path), base_path)
        if not Path(db_path).exists():
            return None
        try:
            con = duckdb.connect(database=str(db_path), read_only=True)
        except Exception:
            try:
                con = duckdb.connect(database=str(db_path))
            except Exception:
                return None
        try:
            table_name = _prepare_table_name(table_value, backend)
            parts = table_name.split(".")
            if len(parts) >= 2:
                schema_name = parts[-2]
                table_only = parts[-1]
                full_table = f"{schema_name}.{table_only}"
            else:
                full_table = table_name
            if _use_precise:
                where = "dataset = ? AND stage != 'no_new_data' AND stage != 'reprocess' AND status != 'failed'"
                params = [dataset]
                if data_layer:
                    where += " AND data_layer = ?"
                    params.append(data_layer)
            else:
                where = "contract = ? AND stage != 'no_new_data' AND stage != 'reprocess' AND status != 'failed'"
                params = [contract_title]
                if data_layer:
                    where += " AND data_layer = ?"
                    params.append(data_layer)
            res = con.execute(
                f"SELECT max(max_source_mtime) FROM {full_table} WHERE {where}",
                params,
            ).fetchone()
            return res[0] if res and res[0] is not None else None
        except Exception:
            return None
        finally:
            try:
                con.close()
            except Exception:
                pass

    if table_value and backend == "sqlite":
        import sqlite3

        # Storage path — resolved by registry placeholders, not by the contract YAML dir.

        base_path = None  # see materialization.py and quarantine.py for rationale
        db_path = metadata.get("run_log_database") or "logs/lakelogic_run_logs.sqlite"
        db_path = _resolve_path(str(db_path), base_path)
        if not Path(db_path).exists():
            return None
        con = sqlite3.connect(str(db_path))
        try:
            table_name = _prepare_table_name(table_value, backend)
            if _use_precise:
                where = "dataset = ? AND stage != 'no_new_data' AND stage != 'reprocess' AND status != 'failed'"
                params = (dataset,)
                if data_layer:
                    where += " AND data_layer = ?"
                    params = (dataset, data_layer)
            else:
                where = "contract = ? AND stage != 'no_new_data' AND stage != 'reprocess' AND status != 'failed'"
                params = (contract_title,)
                if data_layer:
                    where += " AND data_layer = ?"
                    params = (contract_title, data_layer)
            cursor = con.execute(
                f"SELECT max(max_source_mtime) FROM {table_name} WHERE {where}",
                params,
            )
            res = cursor.fetchone()
            return res[0] if res and res[0] is not None else None
        except Exception:
            return None
        finally:
            con.close()

    if table_value and backend == "spark":
        try:
            from pyspark.sql import SparkSession
            from pyspark.sql import functions as F
        except Exception:
            return None
        try:
            spark = SparkSession.builder.getOrCreate()
            df = spark.table(table_value)
            if _use_precise:
                filt = (
                    (F.col("dataset") == dataset)
                    & (F.col("stage") != "no_new_data")
                    & (F.col("stage") != "reprocess")
                    & (F.col("status") != "failed")
                )
                if data_layer:
                    filt = filt & (F.col("data_layer") == data_layer)
            else:
                filt = (
                    (F.col("contract") == contract_title)
                    & (F.col("stage") != "no_new_data")
                    & (F.col("stage") != "reprocess")
                    & (F.col("status") != "failed")
                )
                if data_layer:
                    filt = filt & (F.col("data_layer") == data_layer)
            res = df.filter(filt).agg(F.max(F.col("max_source_mtime")).alias("max_mtime")).collect()
            if res:
                return res[0]["max_mtime"]
        except Exception:
            return None

    if table_value and backend == "delta":
        try:
            from deltalake import DeltaTable
        except ImportError:
            return None
        storage_options = _build_cloud_opts(table_value) if _is_cloud_path(table_value) else None

        # Check if table exists
        try:
            DeltaTable(table_value, storage_options=storage_options)
        except Exception:
            return None

        try:
            dt = DeltaTable(table_value, storage_options=storage_options)
            if _use_precise:
                filters = [("dataset", "=", dataset)]
                if data_layer:
                    filters.append(("data_layer", "=", data_layer))
            else:
                filters = [
                    ("contract", "=", contract_title),
                ]
                if data_layer:
                    filters.append(("data_layer", "=", data_layer))

            df = dt.to_pyarrow_table(
                columns=["max_source_mtime", "stage", "status"],
                filters=filters,
            )
            if len(df) == 0:
                return None
            # Exclude reprocess, no_new_data, and failed entries
            import pyarrow.compute as pc

            mask = pc.and_(
                pc.and_(
                    pc.not_equal(df.column("stage"), "no_new_data"),
                    pc.not_equal(df.column("stage"), "reprocess"),
                ),
                pc.not_equal(df.column("status"), "failed"),
            )
            df = df.filter(mask)
            if len(df) == 0:
                return None
            max_val = pc.max(df.column("max_source_mtime")).as_py()
            return max_val
        except Exception:
            return None

    dir_value = metadata.get("run_log_dir")
    if dir_value:
        raw_dir = str(dir_value)
        if _is_cloud_path(raw_dir):
            # ── Cloud directory scan ──
            try:
                candidates = _cloud_list_json(raw_dir, "run_*.json")
                for cloud_path in candidates:
                    data = _cloud_read_json(cloud_path)
                    if data and data.get("contract") == contract_title and data.get("stage") == stage:
                        return data.get("max_source_mtime")
            except Exception:
                return None
        else:
            # ── Local directory scan ──
            # Storage path — resolved by registry placeholders, not by the contract YAML dir.

            base_path = None  # see materialization.py and quarantine.py for rationale
            log_dir = _resolve_path(raw_dir, base_path)
            if not log_dir.exists():
                return None
            try:
                candidates = sorted(
                    log_dir.glob("run_*.json"),
                    key=lambda p: p.stat().st_mtime,
                    reverse=True,
                )
                for path in candidates:
                    try:
                        with open(path, "r", encoding="utf-8") as handle:
                            data = json.load(handle)
                        if data.get("contract") == contract_title and data.get("stage") == stage:
                            return data.get("max_source_mtime")
                    except Exception:
                        continue
            except Exception:
                return None

    path_value = metadata.get("run_log_path")
    if path_value:
        raw_path = str(path_value)
        if _is_cloud_path(raw_path):
            # ── Cloud single file ──
            data = _cloud_read_json(raw_path)
            if data and data.get("contract") == contract_title and data.get("stage") == stage:
                return data.get("max_source_mtime")
        else:
            # ── Local single file ──
            # Storage path — resolved by registry placeholders, not by the contract YAML dir.

            base_path = None  # see materialization.py and quarantine.py for rationale
            log_path = _resolve_path(raw_path, base_path)
            if log_path.exists():
                try:
                    with open(log_path, "r", encoding="utf-8") as handle:
                        data = json.load(handle)
                    if data.get("contract") == contract_title and data.get("stage") == stage:
                        return data.get("max_source_mtime")
                except Exception:
                    return None

    return None


def get_last_run_dlt_state(
    contract,
    contract_title: str,
    stage: str,
    engine_name: Optional[str] = None,
    dataset: Optional[str] = None,
    data_layer: Optional[str] = None,
) -> Optional[str]:
    """
    Fetch the last dlt_state_json for a contract from run logs.
    Ordered by timestamp DESC to get the latest state.
    """
    if not contract:
        return None

    metadata = contract.metadata or {}
    table_value = metadata.get("run_log_table")
    backend = (metadata.get("run_log_backend") or "").lower()
    if table_value and not backend:
        backend = "spark" if engine_name == "spark" else "delta"

    _use_precise = bool(dataset)

    if table_value and backend == "duckdb":
        try:
            import duckdb
        except Exception:
            return None
        # Storage path — resolved by registry placeholders, not by the contract YAML dir.

        base_path = None  # see materialization.py and quarantine.py for rationale
        db_path = metadata.get("run_log_database") or "logs/lakelogic_run_logs.duckdb"
        db_path = _resolve_path(str(db_path), base_path)
        if not Path(db_path).exists():
            return None
        try:
            con = duckdb.connect(database=str(db_path), read_only=True)
        except Exception:
            try:
                con = duckdb.connect(database=str(db_path))
            except Exception:
                return None
        try:
            table_name = _prepare_table_name(table_value, backend)
            parts = table_name.split(".")
            if len(parts) >= 2:
                full_table = f"{parts[-2]}.{parts[-1]}"
            else:
                full_table = table_name
            if _use_precise:
                where = "dataset = ? AND stage != 'no_new_data' AND stage != 'reprocess' AND status != 'failed'"
                params = [dataset]
                if data_layer:
                    where += " AND data_layer = ?"
                    params.append(data_layer)
            else:
                where = "contract = ? AND stage != 'no_new_data' AND stage != 'reprocess' AND status != 'failed'"
                params = [contract_title]
                if data_layer:
                    where += " AND data_layer = ?"
                    params.append(data_layer)
            res = con.execute(
                f"SELECT dlt_state_json FROM {full_table} WHERE {where} AND dlt_state_json IS NOT NULL ORDER BY timestamp DESC LIMIT 1",  # noqa: E501
                params,
            ).fetchone()
            return res[0] if res and res[0] is not None else None
        except Exception:
            return None
        finally:
            try:
                con.close()
            except Exception:
                pass

    if table_value and backend == "sqlite":
        import sqlite3

        # Storage path — resolved by registry placeholders, not by the contract YAML dir.

        base_path = None  # see materialization.py and quarantine.py for rationale
        db_path = metadata.get("run_log_database") or "logs/lakelogic_run_logs.sqlite"
        db_path = _resolve_path(str(db_path), base_path)
        if not Path(db_path).exists():
            return None
        con = sqlite3.connect(str(db_path))
        try:
            table_name = _prepare_table_name(table_value, backend)
            if _use_precise:
                where = "dataset = ? AND stage != 'no_new_data' AND stage != 'reprocess' AND status != 'failed'"
                params = [dataset]
                if data_layer:
                    where += " AND data_layer = ?"
                    params.append(data_layer)
            else:
                where = "contract = ? AND stage != 'no_new_data' AND stage != 'reprocess' AND status != 'failed'"
                params = [contract_title]
                if data_layer:
                    where += " AND data_layer = ?"
                    params.append(data_layer)
            cursor = con.execute(
                f"SELECT dlt_state_json FROM {table_name} WHERE {where} AND dlt_state_json IS NOT NULL ORDER BY timestamp DESC LIMIT 1",  # noqa: E501
                params,
            )
            res = cursor.fetchone()
            return res[0] if res and res[0] is not None else None
        except Exception:
            return None
        finally:
            con.close()

    if table_value and backend == "spark":
        try:
            from pyspark.sql import SparkSession
            from pyspark.sql import functions as F
        except Exception:
            return None
        try:
            spark = SparkSession.builder.getOrCreate()
            df = spark.table(table_value)
            if _use_precise:
                filt = (
                    (F.col("dataset") == dataset)
                    & (F.col("stage") != "no_new_data")
                    & (F.col("stage") != "reprocess")
                    & (F.col("status") != "failed")
                    & (F.col("dlt_state_json").isNotNull())
                )
                if data_layer:
                    filt = filt & (F.col("data_layer") == data_layer)
            else:
                filt = (
                    (F.col("contract") == contract_title)
                    & (F.col("stage") != "no_new_data")
                    & (F.col("stage") != "reprocess")
                    & (F.col("status") != "failed")
                    & (F.col("dlt_state_json").isNotNull())
                )
                if data_layer:
                    filt = filt & (F.col("data_layer") == data_layer)
            # Take highest timestamp
            res = df.filter(filt).orderBy(F.col("timestamp").desc()).limit(1).collect()
            if res:
                return res[0]["dlt_state_json"]
        except Exception:
            return None

    if table_value and backend == "delta":
        try:
            from deltalake import DeltaTable
            import pyarrow.compute as pc
        except ImportError:
            return None
        storage_options = _build_cloud_opts(table_value) if _is_cloud_path(table_value) else None
        try:
            dt = DeltaTable(table_value, storage_options=storage_options)
            if _use_precise:
                filters = [("dataset", "=", dataset)]
                if data_layer:
                    filters.append(("data_layer", "=", data_layer))
            else:
                filters = [("contract", "=", contract_title)]
                if data_layer:
                    filters.append(("data_layer", "=", data_layer))

            df = dt.to_pyarrow_table(
                columns=["dlt_state_json", "stage", "status", "timestamp"],
                filters=filters,
            )
            if len(df) == 0:
                return None

            mask = pc.and_(
                pc.and_(
                    pc.not_equal(df.column("stage"), "no_new_data"),
                    pc.not_equal(df.column("stage"), "reprocess"),
                ),
                pc.and_(pc.not_equal(df.column("status"), "failed"), pc.is_valid(df.column("dlt_state_json"))),
            )
            df = df.filter(mask)
            if len(df) == 0:
                return None

            # Sort by timestamp decending and get first
            import pyarrow.compute as pc

            indices = pc.sort_indices(df, sort_keys=[("timestamp", "descending")])
            sorted_df = df.take(indices)
            return sorted_df.column("dlt_state_json")[0].as_py()
        except Exception:
            return None

    return None
