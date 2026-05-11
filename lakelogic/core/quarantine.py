"""
Quarantine materialization for LakeLogic.

Handles writing quarantined (bad) records to file-based targets and
multi-backend table targets (Spark, DuckDB, SQLite, Snowflake, BigQuery, Iceberg).

Extracted from materialization.py to keep concerns focused.
"""

import os
import uuid
from datetime import datetime, timezone
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
    if backend == "iceberg":
        return _write_quarantine_table_iceberg(df, contract, table_name, metadata)

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

    # Count rows before writing — counting the table after append would
    # return the cumulative total, not just the rows from this run.
    try:
        rows_written = df.count()
    except Exception:
        rows_written = 0

    writer = df.write.mode(mode).format(table_format)
    # Enable schema evolution so new quarantine columns (from contract changes)
    # are merged into the existing table rather than causing a schema mismatch.
    if table_format == "delta":
        writer = writer.option("mergeSchema", "true")
    elif table_format == "iceberg":
        writer = writer.option("merge-schema", "true")

    q_location = getattr(contract.quarantine, "location", None) if getattr(contract, "quarantine", None) else None
    if q_location:
        writer = writer.option("path", q_location)

    writer.saveAsTable(table_name)

    logger.info(f"Wrote {rows_written} quarantined rows to {table_name} (format={table_format}, mode={mode})")
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
    logger.info(f"Wrote {rows_written} quarantined rows to {table_name}")
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
    logger.info(f"Wrote {rows_written} quarantined rows to {target_full}")
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
    logger.info(f"Wrote {rows_written} quarantined rows to {table_id}")
    return {"target": table_id, "rows_written": rows_written, "format": "bigquery"}


def _write_quarantine_table_iceberg(df: Any, contract, table_name: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Write quarantined records to an Apache Iceberg table via pyiceberg.

    Requires ``pyiceberg`` and ``pyarrow``.  The catalog is resolved from
    metadata (``iceberg_catalog_name``, ``iceberg_catalog_uri``) or
    environment variables (``ICEBERG_CATALOG_NAME``, ``ICEBERG_CATALOG_URI``).

    Args:
        df: Engine dataframe for quarantined data.
        contract: DataContract instance.
        table_name: Fully qualified Iceberg table identifier (e.g. ``db.quarantine_orders``).
        metadata: Contract metadata.

    Returns:
        Metadata about the write.
    """
    try:
        from pyiceberg.catalog import load_catalog
        import pyarrow as pa
    except ImportError as exc:
        raise ImportError(
            "Iceberg quarantine backend requires pyiceberg and pyarrow: pip install pyiceberg pyarrow"
        ) from exc

    catalog_name = metadata.get("iceberg_catalog_name") or os.getenv("ICEBERG_CATALOG_NAME", "default")
    catalog_props = {}
    catalog_uri = metadata.get("iceberg_catalog_uri") or os.getenv("ICEBERG_CATALOG_URI")
    if catalog_uri:
        catalog_props["uri"] = catalog_uri
    catalog_warehouse = metadata.get("iceberg_catalog_warehouse") or os.getenv("ICEBERG_CATALOG_WAREHOUSE")
    if catalog_warehouse:
        catalog_props["warehouse"] = catalog_warehouse

    catalog = load_catalog(catalog_name, **catalog_props)

    # Convert to Arrow table
    if hasattr(df, "to_arrow"):
        # Polars DataFrame
        collected = df.collect() if hasattr(df, "collect") else df
        arrow_table = collected.to_arrow()
    elif hasattr(df, "to_pandas"):
        import pyarrow as pa

        arrow_table = pa.Table.from_pandas(df.to_pandas())
    else:
        import pyarrow as pa

        arrow_table = pa.Table.from_pandas(_to_pandas(df))

    rows_written = arrow_table.num_rows

    # Parse namespace and table
    parts = table_name.split(".")
    if len(parts) >= 2:
        namespace = tuple(parts[:-1])
        tbl_name = parts[-1]
    else:
        namespace = ("default",)
        tbl_name = table_name

    try:
        iceberg_table = catalog.load_table(f"{'.'.join(namespace)}.{tbl_name}")
    except Exception:
        # Table doesn't exist — create it
        iceberg_table = catalog.create_table(
            f"{'.'.join(namespace)}.{tbl_name}",
            schema=arrow_table.schema,
        )

    iceberg_table.append(arrow_table)

    logger.info(f"Wrote {rows_written} quarantined rows to Iceberg table {table_name}")
    return {"target": table_name, "rows_written": rows_written, "format": "iceberg"}


# ── Lineage stamping ──


def _stamp_quarantine_lineage(
    df: Any,
    contract,
    *,
    run_id: Optional[str] = None,
) -> Any:
    """
    Inject _lakelogic_* lineage columns into the quarantine DataFrame.

    This ensures every quarantined record can be traced back to the
    pipeline run, contract, domain, and system that produced it.

    Supports Spark, Polars, and Pandas DataFrames.
    """
    now_iso = datetime.now(timezone.utc).isoformat()
    rid = run_id or str(uuid.uuid4())

    # Extract lineage config from contract metadata
    metadata = getattr(contract, "metadata", None) or {}
    lineage_cfg = metadata.get("lineage") or {}

    # Resolve column names from lineage config or use defaults
    col_run_id = lineage_cfg.get("run_id_column_name", "_lakelogic_run_id")
    col_processed_at = lineage_cfg.get("timestamp_column_name", "_lakelogic_processed_at")
    col_source = lineage_cfg.get("source_column_name", "_lakelogic_source")
    col_contract = lineage_cfg.get("contract_name_column_name", "_lakelogic_contract_name")
    col_domain = lineage_cfg.get("domain_column_name", "_lakelogic_domain")
    col_system = lineage_cfg.get("system_column_name", "_lakelogic_system")

    # Resolve values
    contract_name = ""
    if hasattr(contract, "info") and contract.info:
        contract_name = getattr(contract.info, "title", "") or ""
    if not contract_name:
        contract_name = getattr(contract, "dataset", "") or ""

    domain_name = metadata.get("domain", "")
    system_name = metadata.get("system", "")
    source_path = ""
    if hasattr(contract, "source") and contract.source:
        source_path = getattr(contract.source, "path", "") or ""

    lineage_values = {
        col_run_id: rid,
        col_processed_at: now_iso,
        col_source: str(source_path),
        col_contract: contract_name,
        col_domain: domain_name,
        col_system: system_name,
    }

    # Use the unified lineage injector which correctly sorts all _lakelogic_*
    # columns (including errors and categories) to the far right of the schema.
    try:
        from lakelogic.core.lineage import add_columns

        # Determine engine name for add_columns
        engine_name = "pandas"
        if hasattr(df, "sparkSession"):
            engine_name = "spark"
        elif _is_polars_frame(df):
            engine_name = "polars"
        elif hasattr(df, "connection") or type(df).__name__ == "DuckDBPyRelation":
            engine_name = "duckdb"

        return add_columns(df, lineage_values, engine_name=engine_name)
    except Exception as e:
        logger.warning(f"Failed to stamp quarantine lineage: {e}")
        return df


# ──
def materialize_quarantine(
    df: Any,
    contract,
    target_path: Optional[Path] = None,
    *,
    output_format: Optional[str] = None,
    engine_name: Optional[str] = None,
    quarantine_mode: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Materialize quarantined records to the quarantine target.

    Args:
        df: Engine dataframe for quarantined data.
        contract: DataContract with quarantine settings.
        target_path: Optional override target path.
        output_format: Optional override output format.
        engine_name: Engine name for backend defaults.
        quarantine_mode: Runtime mode — "path" (file target), "table" (shared table).
            Defaults to "path". When "table", uses quarantine.table instead of quarantine.target.

    Returns:
        Metadata about the write.
    """
    if contract is None or contract.quarantine is None:
        return {}

    # Stamp lineage columns onto the quarantine DataFrame so every bad
    # record can be traced back to the pipeline run that produced it.
    # SKIP if lineage was already injected by the processor's inject_lineage()
    # (indicated by the presence of _lakelogic_run_id or equivalent column).
    _already_stamped = False
    _df_cols = df.columns if hasattr(df, "columns") else []
    lineage_cfg = getattr(contract, "lineage", None)
    _run_id_col = (
        getattr(lineage_cfg, "run_id_column_name", "_lakelogic_run_id") if lineage_cfg else "_lakelogic_run_id"
    )
    if _run_id_col in _df_cols:
        _already_stamped = True
    if not _already_stamped:
        df = _stamp_quarantine_lineage(df, contract)

    # ── Empty row guard (skip writing altogether to avoid writer crashes) ──
    try:
        from lakelogic.core.materialization import _row_count

        _rc = _row_count(df)
        if _rc is None and hasattr(df, "collect"):
            _rc = int(df.collect().height)

        if _rc == 0:
            logger.info("Quarantine materialization skipped: 0 bad rows.")
            _qtarget = contract.quarantine.target if contract.quarantine else "quarantine"
            return {
                "target": str(target_path or _qtarget),
                "rows_written": 0,
                "format": output_format or "unknown",
            }
    except Exception:
        pass

    mode = (quarantine_mode or "path").lower().strip()
    q = contract.quarantine

    # ── Table mode: route to quarantine.table via table backend ──
    if mode == "table":
        table_name = getattr(q, "table", None)
        if not table_name:
            logger.warning("quarantine_mode='table' but no quarantine.table defined — falling back to path")
            mode = "path"
        else:
            return _write_quarantine_table(
                df,
                contract,
                table_name,
                engine_name=engine_name,
            )

    # ── Path mode (default): write to quarantine.target ──
    if not q.target:
        return {}

    base_path = getattr(contract, "_base_path", None)
    raw_target = str(target_path or contract.quarantine.target)

    # Guard: reject unresolved or invalid targets — quarantine data loss is
    # a data integrity issue, so we fail hard rather than silently skipping.
    if not raw_target or raw_target == "None" or "{" in raw_target:
        raise ValueError(
            f"Quarantine target not fully resolved: '{raw_target}'. "
            f"Check that quarantine.target template variables (e.g. {{quarantine_path}}) "
            f"are defined in _system.yaml storage and environments."
        )

    if raw_target.startswith("table:"):
        table_name = raw_target[len("table:") :]
        return _write_quarantine_table(
            df,
            contract,
            table_name,
            engine_name=engine_name,
        )

    from lakelogic.core.materialization import URIPath

    if raw_target.startswith("table:") or "://" in raw_target:
        quarantine_target = URIPath(raw_target)
    else:
        quarantine_target = _resolve_path(raw_target, base_path)

    metadata = contract.metadata or {}

    # ── Format resolution (precedence: arg → quarantine.format → metadata → materialization.format → extension → default) ──  # noqa: E501
    q = contract.quarantine
    _mat_format = None
    if contract.materialization:
        _mat_format = getattr(contract.materialization, "format", None)
    explicit_format = output_format or getattr(q, "format", None) or metadata.get("quarantine_format") or _mat_format
    resolved_format = str(explicit_format).lower() if explicit_format else None

    # ── Write-mode resolution (quarantine.write_mode → metadata → default append) ──
    write_mode = (getattr(q, "write_mode", None) or metadata.get("quarantine_table_mode") or "append").lower()

    # ── Resolve target path / extension ──────────────────────────────────────
    is_cloud = any(raw_target.startswith(p) for p in ("abfss://", "abfs://", "s3://", "s3a://", "gs://", "gcs://"))

    target_file = quarantine_target

    # Auto-append dataset name to the path to ensure table isolation
    dataset_name = contract.dataset or (getattr(contract.info, "table_name", None) if contract.info else None)
    if dataset_name and not raw_target.startswith("table:"):
        _target_str = str(target_file).replace("\\", "/")
        if not _target_str.endswith(dataset_name):
            target_file = target_file / dataset_name

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

    if not is_cloud:
        target_file.parent.mkdir(parents=True, exist_ok=True)

    resolved_format = resolved_format or "parquet"

    # ── Spark engine ──────────────────────────────────────────────────────────
    if engine_name == "spark" and hasattr(df, "write"):
        spark_formats = {"parquet", "csv", "json", "delta", "iceberg"}
        if resolved_format in spark_formats:
            spark_write_mode = metadata.get("quarantine_table_mode") or write_mode
            writer = df.write.format(resolved_format).mode(spark_write_mode)
            if resolved_format in ["csv", "json"]:
                writer = writer.option("header", "true")
            elif resolved_format in ["delta", "iceberg"]:
                writer = writer.option("mergeSchema", "true")
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

            def _safe_write_deltalake(path, data, **kwargs):
                if hasattr(data, "__len__") and len(data) == 0:
                    kwargs.pop("schema_mode", None)
                    kwargs.pop("engine", None)

                import inspect

                sig = inspect.signature(write_deltalake)
                if "engine" in sig.parameters and kwargs.get("schema_mode") == "merge":
                    kwargs["engine"] = "rust"
                elif "engine" not in sig.parameters and "engine" in kwargs:
                    del kwargs["engine"]
                return write_deltalake(path, data, **kwargs)

        except ImportError as exc:
            raise ImportError("Delta quarantine format requires the deltalake package: pip install deltalake") from exc

        delta_path = str(target_file)
        delta_write_mode = "overwrite" if write_mode == "overwrite" else "append"

        if hasattr(df, "write") and not _is_polars_frame(df):
            # Spark DataFrame passed
            df.write.format("delta").mode(delta_write_mode).save(delta_path)
            rows_written = int(df.count())
        else:
            if _is_polars_frame(df):
                collected = df.collect() if hasattr(df, "collect") else df
                arrow_data = collected.to_arrow()
                rows_written = collected.height
            else:
                pdf = _to_pandas(df)
                import pyarrow as pa

                arrow_data = pa.Table.from_pandas(pdf)
                rows_written = len(pdf)

            # ── Align Arrow schema to existing Delta table schema ──────────
            # Quarantine tables may have been written by a different engine
            # (e.g. DuckDB → Polars switch), causing type divergence.
            # We rebuild the Arrow table to EXACTLY match the Delta schema,
            # casting columns where possible and null-filling the rest.
            try:
                from deltalake import DeltaTable as _DT
                from lakelogic.core.materialization import _build_storage_options, _is_remote_path, _get_pyarrow_schema
                import pyarrow as pa

                _dt_opts = _build_storage_options() if _is_remote_path(delta_path) else None
                _existing_dt = _DT(delta_path, storage_options=_dt_opts)
                # Use the robust helper to extract a native PyArrow Schema
                delta_schema = _get_pyarrow_schema(_existing_dt)

                # Case-insensitive lookup: incoming column name → index
                incoming_by_name = {f.name.lower(): i for i, f in enumerate(arrow_data.schema)}

                result_columns = []
                result_fields = []
                cast_count = 0

                # Phase 1: Emit all Delta-schema columns in Delta order
                for delta_field in delta_schema:
                    key = delta_field.name.lower()
                    if key in incoming_by_name:
                        idx = incoming_by_name[key]
                        col = arrow_data.column(idx)

                        if col.type == delta_field.type:
                            result_columns.append(col)
                            result_fields.append(delta_field)
                        else:
                            # Cast incoming column to Delta type
                            try:
                                is_str = pa.types.is_string(col.type) or pa.types.is_large_string(col.type)
                                tgt_type = delta_field.type

                                # Handle string -> numeric cast by coercing errors to Null
                                if is_str and (pa.types.is_integer(tgt_type) or pa.types.is_floating(tgt_type)):
                                    import pandas as pd

                                    pd_series = col.to_pandas()
                                    pd_num = pd.to_numeric(pd_series, errors="coerce").astype(
                                        "Int64" if pa.types.is_integer(tgt_type) else "Float64"
                                    )
                                    casted = pa.array(pd_num, type=tgt_type)

                                # Handle string -> timestamp cast
                                elif is_str and pa.types.is_timestamp(tgt_type):
                                    import pandas as pd

                                    pd_series = col.to_pandas()
                                    pd_ts = pd.to_datetime(pd_series, errors="coerce", utc=True)
                                    if getattr(tgt_type, "tz", None) is None:
                                        pd_ts = pd_ts.dt.tz_localize(None)
                                    casted = pa.array(pd_ts, type=tgt_type)

                                # Handle string -> date cast
                                elif is_str and pa.types.is_date(tgt_type):
                                    import pandas as pd

                                    pd_series = col.to_pandas()
                                    pd_dt = pd.to_datetime(pd_series, errors="coerce").dt.date
                                    casted = pa.array(pd_dt, type=tgt_type)

                                else:
                                    # Standard strict casting (e.g. int -> string, or string -> string)
                                    casted = col.cast(tgt_type)

                                result_columns.append(casted)
                                result_fields.append(delta_field)
                                cast_count += 1
                            except Exception:
                                # Cast failed — null-fill with the Delta type
                                logger.debug(
                                    f"Quarantine: casting '{delta_field.name}' "
                                    f"({col.type} -> {delta_field.type}) failed, null-filling"
                                )
                                result_columns.append(pa.nulls(len(arrow_data), type=delta_field.type))
                                result_fields.append(delta_field)
                                cast_count += 1
                    else:
                        # Column in Delta but not in incoming — null-fill
                        result_columns.append(pa.nulls(len(arrow_data), type=delta_field.type))
                        result_fields.append(delta_field)

                # Phase 2: Append any NEW columns (not in Delta schema)
                delta_names_lower = {f.name.lower() for f in delta_schema}
                for i, field in enumerate(arrow_data.schema):
                    if field.name.lower() not in delta_names_lower:
                        result_columns.append(arrow_data.column(i))
                        result_fields.append(field)

                arrow_data = pa.table(result_columns, schema=pa.schema(result_fields))

                if cast_count > 0:
                    logger.info(f"Aligned {cast_count} quarantine column(s) to match existing Delta table schema")

            except Exception as e:
                err_str = str(e).lower()
                if "no log files" in err_str or "doesn't exist" in err_str:
                    logger.debug(f"Quarantine table does not exist yet at {delta_path}. Skipping alignment.")
                else:
                    logger.warning(f"Quarantine schema alignment failed: {e}")

            _safe_write_deltalake(
                delta_path,
                arrow_data,
                mode=delta_write_mode,
                schema_mode="merge",
            )

        logger.info(f"Wrote {rows_written} quarantined rows to {delta_path} (mode={delta_write_mode})")
        return {
            "target": delta_path,
            "rows_written": rows_written,
            "format": "delta",
            "write_mode": delta_write_mode,
        }

    # ── Iceberg format (Polars / DuckDB via pyiceberg) ────────────────────────
    if resolved_format == "iceberg":
        try:
            from pyiceberg.catalog import load_catalog
            import pyarrow as pa
        except ImportError as exc:
            raise ImportError(
                "Iceberg quarantine format requires pyiceberg and pyarrow: pip install pyiceberg pyarrow"
            ) from exc

        metadata = contract.metadata or {}
        catalog_name = metadata.get("iceberg_catalog_name") or os.getenv("ICEBERG_CATALOG_NAME", "default")
        catalog_props = {}
        catalog_uri = metadata.get("iceberg_catalog_uri") or os.getenv("ICEBERG_CATALOG_URI")
        if catalog_uri:
            catalog_props["uri"] = catalog_uri
        catalog_warehouse = metadata.get("iceberg_catalog_warehouse") or os.getenv("ICEBERG_CATALOG_WAREHOUSE")
        if catalog_warehouse:
            catalog_props["warehouse"] = catalog_warehouse

        catalog = load_catalog(catalog_name, **catalog_props)

        # Convert to Arrow
        if _is_polars_frame(df):
            collected = df.collect() if hasattr(df, "collect") else df
            arrow_table = collected.to_arrow()
            rows_written = collected.height
        else:
            pdf = _to_pandas(df)
            arrow_table = pa.Table.from_pandas(pdf)
            rows_written = len(pdf)

        # Parse table identifier from target path
        ice_table_id = str(target_file).replace("/", ".").replace("\\", ".")
        # Strip leading dots and normalize
        ice_table_id = ice_table_id.strip(".")

        try:
            iceberg_table = catalog.load_table(ice_table_id)
        except Exception:
            iceberg_table = catalog.create_table(ice_table_id, schema=arrow_table.schema)

        iceberg_table.append(arrow_table)

        logger.info(f"Wrote {rows_written} quarantined rows to Iceberg table {ice_table_id}")
        return {
            "target": ice_table_id,
            "rows_written": rows_written,
            "format": "iceberg",
            "write_mode": write_mode,
        }

    # ── File formats: csv / parquet ───────────────────────────────────────────
    if resolved_format not in ["csv", "parquet"]:
        raise ValueError(
            f"Unsupported quarantine format '{resolved_format}'. "
            "Supported: parquet, csv, delta (requires deltalake), "
            "iceberg (requires pyiceberg), or use Spark for json."
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

    # ── dlt format ────────────────────────────────────────────────────────
    if resolved_format == "dlt":
        try:
            import dlt as _dlt
            import pyarrow as pa
        except ImportError as exc:
            raise ImportError("dlt quarantine format requires the dlt package") from exc

        dlt_config = contract.quarantine.model_extra or {} if hasattr(contract.quarantine, "model_extra") else {}
        destination = dlt_config.get("dlt_destination", "duckdb")
        dataset_name = dlt_config.get("dlt_dataset_name", "quarantine")
        credentials = dlt_config.get("dlt_credentials")

        dest_kwargs = {}
        if credentials:
            dest_kwargs["credentials"] = credentials
        for k, v in dlt_config.items():
            if k.startswith("dlt_") and k not in ("dlt_destination", "dlt_credentials", "dlt_dataset_name"):
                dest_kwargs[k[4:]] = v

        # Convert df to arrow
        if isinstance(df, pa.Table):
            arrow_data = df
        elif hasattr(df, "to_arrow"):
            arrow_data = df.to_arrow()
        elif hasattr(df, "to_pandas"):
            arrow_data = pa.Table.from_pandas(df.to_pandas())
        else:
            arrow_data = pa.Table.from_pandas(_to_pandas(df))

        rows_written = arrow_data.num_rows if hasattr(arrow_data, "num_rows") else 0
        q_table_name = getattr(contract.quarantine, "table", None) or dataset_name

        @_dlt.resource(
            name=q_table_name,
            write_disposition="append",
        )
        def _q_sink():
            yield arrow_data

        pipeline = _dlt.pipeline(
            pipeline_name=f"lakelogic_{q_table_name}_quarantine",
            destination=_dlt.destinations.__dict__.get(destination, destination)(**dest_kwargs)
            if dest_kwargs
            else destination,
            dataset_name=dataset_name,
        )

        pipeline.run(_q_sink())
        logger.info(f"Wrote {rows_written} quarantined rows via dlt to {destination}:{dataset_name}.{q_table_name}")
        return {
            "target": f"{destination}:{dataset_name}.{q_table_name}",
            "rows_written": rows_written,
            "format": "dlt",
            "dlt_destination": destination,
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
