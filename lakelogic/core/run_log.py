"""
Run-log persistence for LakeLogic.

Handles writing run reports to JSON files and multi-backend table targets
(Spark, DuckDB, SQLite), as well as reading watermarks for incremental loads.

Supports cloud storage paths (ADLS, S3, GCS) via fsspec for JSON run logs.

Extracted from materialization.py to keep concerns focused.
"""

import json
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

        base_path = getattr(contract, "_base_path", None)
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

        base_path = getattr(contract, "_base_path", None)
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
            logger.warning(f"Failed to write run log to Delta table {table_name}: {exc}")
            return None

    logger.warning(f"Unsupported run_log_backend: {backend}")
    return None


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
            base_path = getattr(contract, "_base_path", None)
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
        base_path = getattr(contract, "_base_path", None)
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

        base_path = getattr(contract, "_base_path", None)
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
            base_path = getattr(contract, "_base_path", None)
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
            base_path = getattr(contract, "_base_path", None)
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
        base_path = getattr(contract, "_base_path", None)
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

        base_path = getattr(contract, "_base_path", None)
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
