"""
Quarantine materialization for LakeLogic.

Handles writing quarantined (bad) records to file-based targets and
multi-backend table targets (Spark, DuckDB, SQLite, Snowflake, BigQuery).

Extracted from materialization.py to keep concerns focused.
"""

import os
from pathlib import Path
from typing import Any, Dict, Optional

from loguru import logger

# Import shared helpers from materialization to avoid duplication.
from lakelogic.core.materialization import (
    _resolve_path,
    _resolve_env_value,
    _to_pandas,
    _write_frame,
    _read_frame,
    _is_polars_frame,
    _row_count,
    _frame_has_columns,
    _append_without_pandas,
    _pandas_available,
)


def _default_quarantine_db(base_path: Optional[Path], backend: str) -> Path:
    """
    Resolve a default database path for quarantine table backends.

    Args:
        base_path: Contract base path.
        backend: Backend identifier.

    Returns:
        Path to the backend database file.
    """
    root = base_path or Path.cwd()
    folder = root / ".lakelogic"
    folder.mkdir(parents=True, exist_ok=True)
    filename = "quarantine.duckdb" if backend == "duckdb" else "quarantine.sqlite"
    return folder / filename


def _normalize_quarantine_backend(metadata: Dict[str, Any], engine_name: Optional[str]) -> str:
    """
    Normalize quarantine table backend selection.

    Args:
        metadata: Contract metadata.
        engine_name: Execution engine name.

    Returns:
        Backend identifier string.
    """
    backend = (metadata.get("quarantine_table_backend") or "").lower()
    if backend:
        return backend
    if engine_name:
        if engine_name in ["polars", "pandas", "duckdb"]:
            return "duckdb"
        return engine_name.lower()
    return "duckdb"


def _prepare_table_name(name: str, backend: str) -> str:
    """Normalize table names for backend constraints (e.g., SQLite schemas)."""
    if backend == "sqlite":
        if "." in name:
            cleaned = name.replace(".", "_")
            logger.warning(f"SQLite does not support schemas. Using table name '{cleaned}' instead of '{name}'.")
            return cleaned
    return name


# ── Table writers ──


def _write_quarantine_table(
    df: Any,
    contract,
    table_name: str,
    *,
    engine_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Write quarantined records to a table backend.

    Args:
        df: Engine dataframe for quarantined data.
        contract: DataContract with metadata.
        table_name: Target table name.
        engine_name: Engine name for backend defaults.

    Returns:
        Metadata about the write.
    """
    metadata = contract.metadata or {}
    backend = _normalize_quarantine_backend(metadata, engine_name)

    if backend == "spark":
        return _write_quarantine_table_spark(df, contract, table_name, metadata)
    if backend == "duckdb":
        return _write_quarantine_table_duckdb(df, contract, table_name, metadata)
    if backend == "sqlite":
        return _write_quarantine_table_sqlite(df, contract, table_name, metadata)
    if backend == "snowflake":
        return _write_quarantine_table_snowflake(df, contract, table_name, metadata)
    if backend == "bigquery":
        return _write_quarantine_table_bigquery(df, contract, table_name, metadata)

    logger.warning(f"Unsupported quarantine table backend: {backend}")
    return {}


def _write_quarantine_table_spark(df: Any, contract, table_name: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Write quarantined records to a Spark table with schema evolution enabled.

    Args:
        df: Spark DataFrame.
        contract: DataContract instance.
        table_name: Table name.
        metadata: Contract metadata.

    Returns:
        Metadata about the write.
    """
    if not hasattr(df, "write"):
        raise ValueError("Spark quarantine table requires a Spark DataFrame.")

    # Default to 'delta' on Spark if not specified (more robust for Databricks/Unity Catalog).
    table_format = (metadata.get("quarantine_table_format") or "delta").lower()
    mode = (metadata.get("quarantine_table_mode") or "append").lower()

    spark = df.sparkSession
    parts = table_name.split(".")
    if len(parts) == 2:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {parts[0]}")
    elif len(parts) >= 3:
        # Avoid issues with quoted catalog names by ensuring we don't double-quote or break the string
        catalog_schema = ".".join(parts[:-1])
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_schema}")

    writer = df.write.mode(mode).format(table_format)
    # Enable schema evolution so new quarantine columns (from contract changes)
    # are merged into the existing table rather than causing a schema mismatch.
    if table_format == "delta":
        writer = writer.option("mergeSchema", "true")
    elif table_format == "iceberg":
        writer = writer.option("merge-schema", "true")

    writer.saveAsTable(table_name)

    # Use spark.sql count to avoid df.count() which triggers RDD operations
    # not permitted on Databricks Unity Catalog shared / serverless clusters.
    try:
        rows_written = spark.sql(f"SELECT COUNT(*) FROM {table_name}").collect()[0][0]
    except Exception as e:
        logger.warning(f"Failed to count quarantined rows in {table_name}: {e}")
        rows_written = 0

    logger.info(
        f"Wrote {rows_written} quarantined rows to Spark table {table_name} (format={table_format}, mode={mode})"
    )
    return {"target": table_name, "rows_written": rows_written, "format": table_format}


def _write_quarantine_table_duckdb(df: Any, contract, table_name: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Write quarantined records to a DuckDB table with schema evolution.

    New columns present in the incoming frame but absent from the existing
    table are added automatically via ``ALTER TABLE ADD COLUMN IF NOT EXISTS``
    before inserting, so quarantine never fails as the contract schema grows.

    Args:
        df: Engine dataframe for quarantined data.
        contract: DataContract instance.
        table_name: Table name.
        metadata: Contract metadata.

    Returns:
        Metadata about the write.
    """
    try:
        import duckdb
    except Exception as exc:
        raise ValueError("DuckDB backend requires duckdb installed.") from exc

    base_path = getattr(contract, "_base_path", None)
    db_path = metadata.get("quarantine_table_database")
    if db_path:
        db_path = _resolve_path(str(db_path), base_path)
    else:
        db_path = _default_quarantine_db(base_path, "duckdb")

    pdf = _to_pandas(df)
    table_name = _prepare_table_name(table_name, "duckdb")
    parts = table_name.split(".")
    if len(parts) >= 2:
        schema_name = parts[-2]
        table_only = parts[-1]
        full_table = f"{schema_name}.{table_only}"
    else:
        schema_name = None
        full_table = table_name

    con = duckdb.connect(database=str(db_path))
    try:
        if schema_name:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")

        con.register("incoming_quarantine", pdf)

        # Create table from first batch if it doesn't exist yet.
        con.execute(f"CREATE TABLE IF NOT EXISTS {full_table} AS SELECT * FROM incoming_quarantine WHERE 1=0")

        # ── Schema evolution: add any new columns from the incoming frame ──
        # Query the existing column names, then ALTER TABLE for each new one.
        existing_cols = {
            row[0].lower()
            for row in con.execute(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_name = '{full_table.split('.')[-1]}'"
                + (f" AND table_schema = '{schema_name}'" if schema_name else "")
            ).fetchall()
        }
        # Map pandas dtypes → DuckDB types for ALTER TABLE
        _PD_TO_DUCK = {
            "object": "VARCHAR",
            "string": "VARCHAR",
            "int64": "BIGINT",
            "int32": "INTEGER",
            "float64": "DOUBLE",
            "float32": "FLOAT",
            "bool": "BOOLEAN",
            "boolean": "BOOLEAN",
            "datetime64[ns]": "TIMESTAMP",
            "datetime64[ns, UTC]": "TIMESTAMPTZ",
        }
        for col in pdf.columns:
            if col.lower() not in existing_cols:
                dtype = str(pdf[col].dtype)
                duck_type = _PD_TO_DUCK.get(dtype, "VARCHAR")
                try:
                    con.execute(f'ALTER TABLE {full_table} ADD COLUMN IF NOT EXISTS "{col}" {duck_type}')
                    logger.debug(f"Quarantine schema evolved: added column '{col}' ({duck_type}) to {full_table}")
                except Exception as e:
                    logger.warning(f"Could not add column '{col}' to quarantine table: {e}")

        con.execute(f"INSERT INTO {full_table} SELECT * FROM incoming_quarantine")
    finally:
        con.close()

    rows_written = len(pdf)
    logger.info(f"Wrote {rows_written} quarantined rows to DuckDB table {full_table} (schema evolution enabled)")
    return {
        "target": f"{db_path}:{full_table}",
        "rows_written": rows_written,
        "format": "duckdb",
    }


def _write_quarantine_table_sqlite(df: Any, contract, table_name: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Write quarantined records to a SQLite table.

    Args:
        df: Engine dataframe for quarantined data.
        contract: DataContract instance.
        table_name: Table name.
        metadata: Contract metadata.

    Returns:
        Metadata about the write.
    """
    import sqlite3

    base_path = getattr(contract, "_base_path", None)
    db_path = metadata.get("quarantine_table_database")
    if db_path:
        db_path = _resolve_path(str(db_path), base_path)
    else:
        db_path = _default_quarantine_db(base_path, "sqlite")

    pdf = _to_pandas(df)
    table_name = _prepare_table_name(table_name, "sqlite")
    conn = sqlite3.connect(str(db_path))
    try:
        pdf.to_sql(table_name, conn, if_exists="append", index=False)
    finally:
        conn.close()

    rows_written = len(pdf)
    logger.info(f"Wrote {rows_written} quarantined rows to SQLite table {table_name}")
    return {
        "target": f"{db_path}:{table_name}",
        "rows_written": rows_written,
        "format": "sqlite",
    }


def _write_quarantine_table_snowflake(df: Any, contract, table_name: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Write quarantined records to a Snowflake table.

    Args:
        df: Engine dataframe for quarantined data.
        contract: DataContract instance.
        table_name: Table name.
        metadata: Contract metadata.

    Returns:
        Metadata about the write.
    """
    try:
        import snowflake.connector
        from snowflake.connector.pandas_tools import write_pandas
    except Exception as exc:
        raise ValueError("Snowflake backend requires snowflake-connector-python installed.") from exc

    params = {
        "account": _resolve_env_value(metadata.get("snowflake_account") or os.getenv("SNOWFLAKE_ACCOUNT")),
        "user": _resolve_env_value(metadata.get("snowflake_user") or os.getenv("SNOWFLAKE_USER")),
        "password": _resolve_env_value(metadata.get("snowflake_password") or os.getenv("SNOWFLAKE_PASSWORD")),
        "warehouse": _resolve_env_value(metadata.get("snowflake_warehouse") or os.getenv("SNOWFLAKE_WAREHOUSE")),
        "database": _resolve_env_value(metadata.get("snowflake_database") or os.getenv("SNOWFLAKE_DATABASE")),
        "schema": _resolve_env_value(metadata.get("snowflake_schema") or os.getenv("SNOWFLAKE_SCHEMA")),
        "role": _resolve_env_value(metadata.get("snowflake_role") or os.getenv("SNOWFLAKE_ROLE")),
    }
    missing = [k for k, v in params.items() if k in ["account", "user", "password"] and not v]
    if missing:
        raise ValueError(f"Snowflake connection missing required fields: {', '.join(missing)}")

    parts = table_name.split(".")
    if len(parts) >= 3:
        params["database"] = parts[-3]
        params["schema"] = parts[-2]
        table_only = parts[-1]
    elif len(parts) == 2:
        params["schema"] = parts[-2]
        table_only = parts[-1]
    else:
        table_only = table_name

    pdf = _to_pandas(df)
    conn = snowflake.connector.connect(**{k: v for k, v in params.items() if v})
    try:
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
        except Exception:
            pass

    rows_written = len(pdf)
    target_full = ".".join([p for p in [params.get("database"), params.get("schema"), table_only] if p])
    logger.info(f"Wrote {rows_written} quarantined rows to Snowflake table {target_full}")
    return {"target": target_full, "rows_written": rows_written, "format": "snowflake"}


def _write_quarantine_table_bigquery(df: Any, contract, table_name: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Write quarantined records to a BigQuery table.

    Args:
        df: Engine dataframe for quarantined data.
        contract: DataContract instance.
        table_name: Table name.
        metadata: Contract metadata.

    Returns:
        Metadata about the write.
    """
    try:
        from google.cloud import bigquery  # type: ignore
    except Exception as exc:
        raise ValueError("BigQuery backend requires google-cloud-bigquery installed.") from exc

    parts = table_name.split(".")
    project = metadata.get("bigquery_project") or os.getenv("BIGQUERY_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    if len(parts) == 3:
        project = parts[0]
        dataset = parts[1]
        table_only = parts[2]
    elif len(parts) == 2:
        dataset = parts[0]
        table_only = parts[1]
    else:
        raise ValueError("BigQuery table name must be dataset.table or project.dataset.table")

    if not project:
        raise ValueError("BigQuery project not provided (bigquery_project or GOOGLE_CLOUD_PROJECT).")

    table_id = f"{project}.{dataset}.{table_only}"
    client = bigquery.Client(project=project)
    pdf = _to_pandas(df)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True,
    )
    job = client.load_table_from_dataframe(pdf, table_id, job_config=job_config)
    job.result()

    rows_written = len(pdf)
    logger.info(f"Wrote {rows_written} quarantined rows to BigQuery table {table_id}")
    return {"target": table_id, "rows_written": rows_written, "format": "bigquery"}


# ── Public API ──


def materialize_quarantine(
    df: Any,
    contract,
    target_path: Optional[Path] = None,
    *,
    output_format: Optional[str] = None,
    engine_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Materialize quarantined records to the quarantine target.

    Args:
        df: Engine dataframe for quarantined data.
        contract: DataContract with quarantine settings.
        target_path: Optional override target path.
        output_format: Optional override output format.

    Returns:
        Metadata about the write.
    """
    if contract is None or contract.quarantine is None or not contract.quarantine.target:
        return {}

    base_path = getattr(contract, "_base_path", None)
    raw_target = str(target_path or contract.quarantine.target)

    if raw_target.startswith("table:"):
        table_name = raw_target[len("table:") :]
        return _write_quarantine_table(
            df,
            contract,
            table_name,
            engine_name=engine_name,
        )

    quarantine_target = _resolve_path(raw_target, base_path)

    metadata = contract.metadata or {}

    # ── Format resolution (precedence: arg → quarantine.format → metadata → extension → default) ──
    q = contract.quarantine
    explicit_format = output_format or getattr(q, "format", None) or metadata.get("quarantine_format")
    resolved_format = str(explicit_format).lower() if explicit_format else None

    # ── Write-mode resolution (quarantine.write_mode → metadata → default append) ──
    write_mode = (getattr(q, "write_mode", None) or metadata.get("quarantine_table_mode") or "append").lower()

    # ── Resolve target path / extension ──────────────────────────────────────
    quarantine_target.parent.mkdir(parents=True, exist_ok=True)
    target_file = quarantine_target
    if target_file.suffix == "":
        resolved_format = resolved_format or "parquet"
        if resolved_format == "delta":
            # Delta is a directory — keep path as-is (no suffix)
            pass
        else:
            target_file = target_file.with_suffix(f".{resolved_format}")
    else:
        if resolved_format is None:
            resolved_format = target_file.suffix.lstrip(".").lower()

    resolved_format = resolved_format or "parquet"

    # ── Spark engine ──────────────────────────────────────────────────────────
    if engine_name == "spark" and hasattr(df, "write"):
        spark_formats = {"parquet", "csv", "json", "delta", "iceberg"}
        if resolved_format in spark_formats:
            spark_write_mode = metadata.get("quarantine_table_mode") or write_mode
            writer = df.write.format(resolved_format).mode(spark_write_mode)
            if resolved_format in ["csv", "json"]:
                writer = writer.option("header", "true")
            writer.save(str(target_file))
            rows_written = int(df.count())
            logger.info(
                f"Wrote {rows_written} quarantined rows to {target_file} ({resolved_format}, mode={spark_write_mode})"
            )
            return {
                "target": str(target_file),
                "rows_written": rows_written,
                "format": resolved_format,
                "write_mode": spark_write_mode,
            }

    if not _frame_has_columns(df):
        logger.info("Quarantine materialization skipped: dataframe has no columns.")
        return {
            "target": str(target_file),
            "rows_written": 0,
            "format": resolved_format,
        }

    # ── Delta format (Polars / DuckDB via deltalake) ──────────────────────────
    if resolved_format == "delta":
        try:
            from deltalake.writer import write_deltalake
        except ImportError as exc:
            raise ImportError("Delta quarantine format requires the deltalake package: pip install deltalake") from exc

        delta_path = str(target_file)
        delta_write_mode = "overwrite" if write_mode == "overwrite" else "append"

        if _is_polars_frame(df):
            collected = df.collect() if hasattr(df, "collect") else df
            # schema_mode="merge" enables schema evolution — new columns from
            # evolving contracts are added to the Delta table automatically.
            write_deltalake(
                delta_path,
                collected.to_arrow(),
                mode=delta_write_mode,
                schema_mode="merge",
            )
            rows_written = collected.height
        elif hasattr(df, "write"):
            # Spark DataFrame passed but engine_name not set — fall through to Spark
            df.write.format("delta").mode(delta_write_mode).save(delta_path)
            rows_written = int(df.count())
        else:
            pdf = _to_pandas(df)
            import pyarrow as pa

            write_deltalake(delta_path, pa.Table.from_pandas(pdf), mode=delta_write_mode)
            rows_written = len(pdf)

        logger.info(f"Wrote {rows_written} quarantined rows to Delta table {delta_path} (mode={delta_write_mode})")
        return {
            "target": delta_path,
            "rows_written": rows_written,
            "format": "delta",
            "write_mode": delta_write_mode,
        }

    # ── File formats: csv / parquet ───────────────────────────────────────────
    if resolved_format not in ["csv", "parquet"]:
        raise ValueError(
            f"Unsupported quarantine format '{resolved_format}'. "
            "Supported: parquet, csv, delta (requires deltalake), "
            "or use Spark for iceberg/json."
        )

    # Prefer native Polars writes to avoid pyarrow dependency.
    if _is_polars_frame(df):
        if write_mode == "overwrite" or not target_file.exists():
            _write_frame(df, target_file, resolved_format)
            rows_written = _row_count(df)
            if rows_written is None and hasattr(df, "collect"):
                try:
                    rows_written = int(df.collect().height)
                except Exception:
                    pass
        else:
            rows_written = _append_without_pandas(df, target_file, resolved_format)
        logger.info(
            f"Wrote {rows_written if rows_written is not None else '?'} "
            f"quarantined rows to {target_file} (mode={write_mode})"
        )
        return {
            "target": str(target_file),
            "rows_written": rows_written,
            "format": resolved_format,
            "write_mode": write_mode,
        }

    if not _pandas_available():
        if write_mode == "overwrite" or not target_file.exists():
            _write_frame(df, target_file, resolved_format)
            rows_written = _row_count(df)
        else:
            rows_written = _append_without_pandas(df, target_file, resolved_format)
        logger.info(
            f"Wrote {rows_written if rows_written is not None else '?'} "
            f"quarantined rows to {target_file} (mode={write_mode})"
        )
        return {
            "target": str(target_file),
            "rows_written": rows_written,
            "format": resolved_format,
            "write_mode": write_mode,
        }

    import pandas as pd

    pdf = _to_pandas(df)
    if write_mode == "overwrite" or not target_file.exists():
        _write_frame(pdf, target_file, resolved_format)
        rows_written = len(pdf)
    else:
        existing = _read_frame(target_file, resolved_format)
        combined = pd.concat([existing, pdf], ignore_index=True)
        _write_frame(combined, target_file, resolved_format)
        rows_written = len(combined)

    logger.info(f"Wrote {rows_written} quarantined rows to {target_file} (mode={write_mode})")
    return {
        "target": str(target_file),
        "rows_written": rows_written,
        "format": resolved_format,
        "write_mode": write_mode,
    }
