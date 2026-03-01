"""
Run-log persistence for LakeLogic.

Handles writing run reports to JSON files and multi-backend table targets
(Spark, DuckDB, SQLite), as well as reading watermarks for incremental loads.

Extracted from materialization.py to keep concerns focused.
"""

import json
from pathlib import Path
from typing import Any, Dict, Optional
from uuid import uuid4

from loguru import logger


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

    return {
        "run_id": report.get("run_id"),
        "pipeline_run_id": report.get("pipeline_run_id"),
        "timestamp": report.get("timestamp"),
        "engine": report.get("engine"),
        "contract": report.get("contract"),
        "stage": report.get("stage"),
        "dataset": report.get("dataset"),
        "domain": report.get("domain"),
        "system": report.get("system"),
        "data_layer": report.get("data_layer"),
        "source_path": report.get("source_path"),
        "counts_source": counts.get("source"),
        "counts_total": counts.get("total"),
        "counts_good": counts.get("good"),
        "counts_quarantined": counts.get("quarantined"),
        "counts_pre_transform_dropped": counts.get("pre_transform_dropped"),
        "quarantine_ratio": _num(counts.get("quarantine_ratio")),
        "max_source_mtime": report.get("max_source_mtime"),
        "source_files_json": json.dumps(report.get("source_files", []), default=str),
        "freshness_seconds": _num(freshness.get("age_seconds")),
        "freshness_pass": freshness.get("passed"),
        "freshness_threshold_seconds": _num(freshness.get("threshold_seconds")),
        "availability_ratio": _num(availability.get("ratio")),
        "availability_pass": availability.get("passed"),
        "availability_threshold": _num(availability.get("threshold")),
        "dataset_rules_json": json.dumps(report.get("dataset_rules", []), default=str),
        "row_rule_failures_json": json.dumps(report.get("row_rule_failures", []), default=str),
        "schema_drift_json": json.dumps(report.get("schema_drift", {}), default=str),
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
        backend = "spark" if engine_name == "spark" else "duckdb"

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

        df = spark.createDataFrame([record])
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
                    ("stage", "STRING"),
                    ("dataset", "STRING"),
                    ("domain", "STRING"),
                    ("system", "STRING"),
                    ("data_layer", "STRING"),
                    ("counts_source", "BIGINT"),
                    ("counts_pre_transform_dropped", "BIGINT"),
                    ("max_source_mtime", "DOUBLE"),
                    ("source_files_json", "STRING"),
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
                run_id VARCHAR,
                pipeline_run_id VARCHAR,
                timestamp VARCHAR,
                engine VARCHAR,
                contract VARCHAR,
                stage VARCHAR,
                dataset VARCHAR,
                domain VARCHAR,
                system VARCHAR,
                data_layer VARCHAR,
                source_path VARCHAR,
                counts_source BIGINT,
                counts_total BIGINT,
                counts_good BIGINT,
                counts_quarantined BIGINT,
                counts_pre_transform_dropped BIGINT,
                quarantine_ratio DOUBLE,
                max_source_mtime DOUBLE,
                source_files_json VARCHAR,
                freshness_seconds DOUBLE,
                freshness_pass BOOLEAN,
                freshness_threshold_seconds DOUBLE,
                availability_ratio DOUBLE,
                availability_pass BOOLEAN,
                availability_threshold DOUBLE,
                dataset_rules_json VARCHAR,
                row_rule_failures_json VARCHAR,
                schema_drift_json VARCHAR,
                report_json VARCHAR
            )
        """)
        try:
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS pipeline_run_id VARCHAR")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS stage VARCHAR")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS dataset VARCHAR")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS domain VARCHAR")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS system VARCHAR")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS data_layer VARCHAR")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS counts_source BIGINT")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS counts_pre_transform_dropped BIGINT")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS max_source_mtime DOUBLE")
            con.execute(f"ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS source_files_json VARCHAR")
        except Exception:
            pass
        columns = [
            "run_id",
            "pipeline_run_id",
            "timestamp",
            "engine",
            "contract",
            "stage",
            "dataset",
            "domain",
            "system",
            "data_layer",
            "source_path",
            "counts_source",
            "counts_total",
            "counts_good",
            "counts_quarantined",
            "counts_pre_transform_dropped",
            "quarantine_ratio",
            "max_source_mtime",
            "source_files_json",
            "freshness_seconds",
            "freshness_pass",
            "freshness_threshold_seconds",
            "availability_ratio",
            "availability_pass",
            "availability_threshold",
            "dataset_rules_json",
            "row_rule_failures_json",
            "schema_drift_json",
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
                run_id TEXT,
                pipeline_run_id TEXT,
                timestamp TEXT,
                engine TEXT,
                contract TEXT,
                stage TEXT,
                dataset TEXT,
                domain TEXT,
                system TEXT,
                data_layer TEXT,
                source_path TEXT,
                counts_source INTEGER,
                counts_total INTEGER,
                counts_good INTEGER,
                counts_quarantined INTEGER,
                counts_pre_transform_dropped INTEGER,
                quarantine_ratio REAL,
                max_source_mtime REAL,
                source_files_json TEXT,
                freshness_seconds REAL,
                freshness_pass INTEGER,
                freshness_threshold_seconds REAL,
                availability_ratio REAL,
                availability_pass INTEGER,
                availability_threshold REAL,
                dataset_rules_json TEXT,
                row_rule_failures_json TEXT,
                schema_drift_json TEXT,
                report_json TEXT
            )
        """)
        try:
            cols = [row[1] for row in con.execute(f"PRAGMA table_info({table_name})").fetchall()]
            if "pipeline_run_id" not in cols:
                con.execute(f"ALTER TABLE {table_name} ADD COLUMN pipeline_run_id TEXT")
            for col_name, col_type in [
                ("stage", "TEXT"),
                ("dataset", "TEXT"),
                ("domain", "TEXT"),
                ("system", "TEXT"),
                ("data_layer", "TEXT"),
                ("counts_source", "INTEGER"),
                ("counts_pre_transform_dropped", "INTEGER"),
                ("max_source_mtime", "REAL"),
                ("source_files_json", "TEXT"),
            ]:
                if col_name not in cols:
                    con.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {col_type}")
        except Exception:
            pass
        columns = [
            "run_id",
            "pipeline_run_id",
            "timestamp",
            "engine",
            "contract",
            "stage",
            "dataset",
            "domain",
            "system",
            "data_layer",
            "source_path",
            "counts_source",
            "counts_total",
            "counts_good",
            "counts_quarantined",
            "counts_pre_transform_dropped",
            "quarantine_ratio",
            "max_source_mtime",
            "source_files_json",
            "freshness_seconds",
            "freshness_pass",
            "freshness_threshold_seconds",
            "availability_ratio",
            "availability_pass",
            "availability_threshold",
            "dataset_rules_json",
            "row_rule_failures_json",
            "schema_drift_json",
            "report_json",
        ]
        values = []
        for col in columns:
            if col == "freshness_pass":
                value = record.get(col)
                values.append(1 if value else 0 if value is not None else None)
            elif col == "availability_pass":
                value = record.get(col)
                values.append(1 if value else 0 if value is not None else None)
            else:
                values.append(record.get(col))
        placeholders = ", ".join(["?"] * len(columns))
        con.execute(
            f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})",
            values,
        )
        con.commit()
        con.close()
        logger.info(f"Wrote run log to SQLite table {table_name} ({db_path})")
        return f"{db_path}:{table_name}"

    logger.warning(f"Unsupported run_log_backend: {backend}")
    return None


# ── Public API ──


def write_run_log(report: Dict[str, Any], contract, engine_name: Optional[str] = None) -> Optional[str]:
    """
    Write run logs to JSON and/or table backends.

    Args:
        report: Run report dict.
        contract: DataContract with metadata.
        engine_name: Engine name for backend defaults.

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

    log_path = None
    if path_value or dir_value:
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

    _write_run_log_table(report, contract, engine_name=engine_name)
    return str(log_path) if log_path else None


def get_last_run_watermark(
    contract, contract_title: str, stage: str, engine_name: Optional[str] = None
) -> Optional[float]:
    """
    Fetch the last max_source_mtime for a contract/stage from run logs.
    """
    if not contract:
        return None

    metadata = contract.metadata or {}
    table_value = metadata.get("run_log_table")
    backend = (metadata.get("run_log_backend") or "").lower()
    if table_value and not backend:
        backend = "spark" if engine_name == "spark" else "duckdb"

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
            res = con.execute(
                f"""
                SELECT max(max_source_mtime) FROM {full_table}
                WHERE contract = ? AND stage = ?
                """,
                [contract_title, stage],
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
            cursor = con.execute(
                f"SELECT max(max_source_mtime) FROM {table_name} WHERE contract = ? AND stage = ?",
                (contract_title, stage),
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
            res = (
                df.filter((F.col("contract") == contract_title) & (F.col("stage") == stage))
                .agg(F.max(F.col("max_source_mtime")).alias("max_mtime"))
                .collect()
            )
            if res:
                return res[0]["max_mtime"]
        except Exception:
            return None

    dir_value = metadata.get("run_log_dir")
    if dir_value:
        base_path = getattr(contract, "_base_path", None)
        log_dir = _resolve_path(str(dir_value), base_path)
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
        base_path = getattr(contract, "_base_path", None)
        log_path = _resolve_path(str(path_value), base_path)
        if log_path.exists():
            try:
                with open(log_path, "r", encoding="utf-8") as handle:
                    data = json.load(handle)
                if data.get("contract") == contract_title and data.get("stage") == stage:
                    return data.get("max_source_mtime")
            except Exception:
                return None

    return None
