from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from datetime import datetime, timezone
from uuid import uuid4
import json
import os
import re
import shutil

from loguru import logger


def _safe_partition_value(value: Any) -> str:
    """
    Normalize partition values to safe filesystem-friendly strings.

    Args:
        value: Partition value.

    Returns:
        A sanitized string suitable for directory names.
    """
    if value is None:
        return "null"
    text = str(value)
    return text.replace(os.sep, "_").replace(" ", "_")


def _resolve_path(raw_path: str, base_path: Optional[Path]) -> Path:
    """
    Resolve a path, honoring the contract base path for relative values.

    Args:
        raw_path: Raw path string from the contract.
        base_path: Base path to resolve relative paths against.

    Returns:
        A resolved Path.
    """
    path = Path(raw_path)
    if not path.is_absolute() and base_path:
        path = base_path / path
    return path


_ENV_PATTERN = re.compile(r"^\${ENV:([A-Z0-9_]+)}$")


def _resolve_env_value(value: Optional[str]) -> Optional[str]:
    """
    Resolve simple environment variable placeholders.

    Args:
        value: Raw value or env placeholder (env:VAR or ${ENV:VAR}).

    Returns:
        Resolved value or None.
    """
    if value is None:
        return None
    if value.startswith("env:"):
        return os.getenv(value[4:].strip())
    match = _ENV_PATTERN.match(value)
    if match:
        return os.getenv(match.group(1))
    return value


def _resolve_target(contract, override_path: Optional[Path] = None) -> Tuple[Optional[Path], Optional[str]]:
    """
    Resolve the output target path and format from contract metadata.

    Args:
        contract: DataContract instance.
        override_path: Optional override path.

    Returns:
        Tuple of (target_path, output_format).
    """
    if override_path:
        target = override_path
    else:
        mat = contract.materialization if contract else None
        mat_path = None
        if mat is not None:
            mat_path = getattr(mat, "target_path", None) or getattr(mat, "path", None)
        server_path = contract.server.path if contract and contract.server else None
        target = mat_path or server_path

    if not target:
        return None, None

    base_path = getattr(contract, "_base_path", None)
    target_str = str(target)
    if target_str.startswith("table:"):
        target_path = Path(target_str)
    else:
        target_path = _resolve_path(target_str, base_path)

    output_format = None
    if contract and contract.server and contract.server.format:
        output_format = contract.server.format
    elif contract and contract.materialization:
        output_format = getattr(contract.materialization, "format", None)
    if not output_format:
        output_format = "parquet"
    output_format = output_format.lower()

    return target_path, output_format


def _to_pandas(df: Any) -> Any:
    """
    Convert a dataframe-like object into a pandas DataFrame.

    Args:
        df: Engine dataframe.

    Returns:
        pandas.DataFrame
    """
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError("pandas is required for materialization output.") from exc

    if isinstance(df, pd.DataFrame):
        return df
    if hasattr(df, "to_pandas"):
        return df.to_pandas()
    if hasattr(df, "toPandas"):
        return df.toPandas()
    if hasattr(df, "df"):
        return df.df()
    if hasattr(df, "collect"):
        collected = df.collect()
        if hasattr(collected, "to_pandas"):
            return collected.to_pandas()
    raise TypeError(f"Unsupported dataframe type for materialization: {type(df)}")


def _write_frame(df, path: Path, output_format: str) -> None:
    """
    Write a pandas DataFrame to disk.

    Args:
        df: pandas.DataFrame to write.
        path: Destination path.
        output_format: csv or parquet.
    """
    if output_format == "csv":
        df.to_csv(path, index=False)
    elif output_format == "parquet":
        df.to_parquet(path, index=False)
    else:
        raise ValueError(f"Unsupported output format: {output_format}")


def _read_frame(path: Path, output_format: str):
    """
    Read a pandas DataFrame from disk.

    Args:
        path: Source path.
        output_format: csv or parquet.

    Returns:
        pandas.DataFrame
    """
    import pandas as pd
    if output_format == "csv":
        return pd.read_csv(path)
    if output_format == "parquet":
        return pd.read_parquet(path)
    raise ValueError(f"Unsupported output format: {output_format}")


def _merge_frames(existing, incoming, primary_key: List[str]):
    """
    Merge incoming rows into existing rows using a primary key.

    Args:
        existing: Existing dataframe.
        incoming: Incoming dataframe.
        primary_key: Primary key columns.

    Returns:
        Merged dataframe.
    """
    if not primary_key:
        raise ValueError("primary_key is required for merge strategy.")
    import pandas as pd

    existing = existing.copy()
    incoming = incoming.copy()

    all_cols = list(dict.fromkeys(list(existing.columns) + list(incoming.columns)))
    existing = existing.reindex(columns=all_cols)
    incoming = incoming.reindex(columns=all_cols)

    existing = existing.set_index(primary_key)
    incoming = incoming.set_index(primary_key)

    existing.update(incoming)
    new_rows = incoming.loc[~incoming.index.isin(existing.index)]
    merged = existing.reset_index()
    if not new_rows.empty:
        merged = pd.concat([merged, new_rows.reset_index()], ignore_index=True)
    return merged


def _scd2_frames(existing, incoming, primary_key: List[str], scd2_cfg: Dict[str, Any]):
    """
    Apply SCD2 changes by closing current records and appending new versions.

    Args:
        existing: Existing dataframe.
        incoming: Incoming dataframe.
        primary_key: Primary key columns.
        scd2_cfg: SCD2 configuration.

    Returns:
        Updated dataframe with SCD2 semantics.
    """
    if not primary_key:
        raise ValueError("primary_key is required for scd2 strategy.")

    effective_from = scd2_cfg.get("effective_from_field", "effective_from")
    effective_to = scd2_cfg.get("effective_to_field", "effective_to")
    current_flag = scd2_cfg.get("current_flag_field", "is_current")

    now_value = scd2_cfg.get("default_effective_from")
    if not now_value:
        now_value = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    existing = existing.copy()
    incoming = incoming.copy()

    if effective_from not in incoming.columns:
        incoming[effective_from] = now_value
    if effective_to not in incoming.columns:
        incoming[effective_to] = None
    if current_flag not in incoming.columns:
        incoming[current_flag] = True

    if existing.empty:
        return incoming

    if effective_from not in existing.columns:
        existing[effective_from] = None
    if effective_to not in existing.columns:
        existing[effective_to] = None
    if current_flag not in existing.columns:
        existing[current_flag] = True

    incoming_keys = incoming[primary_key].drop_duplicates()

    merged = existing.copy()

    for _, key_row in incoming_keys.iterrows():
        key_filter = None
        for col in primary_key:
            if key_filter is None:
                key_filter = merged[col] == key_row[col]
            else:
                key_filter &= merged[col] == key_row[col]

        current_mask = key_filter & (merged[current_flag] == True)
        if current_mask.any():
            new_effective_from = incoming.loc[
                (incoming[primary_key] == key_row.values).all(axis=1),
                effective_from
            ].iloc[0]
            merged.loc[current_mask, effective_to] = new_effective_from
            merged.loc[current_mask, current_flag] = False

    import pandas as pd
    merged = pd.concat([merged, incoming], ignore_index=True)
    return merged


def _partition_groups(df, partition_by: List[str]) -> Iterable[Tuple[Dict[str, Any], Any]]:
    """
    Yield dataframe groups for each partition.

    Args:
        df: pandas.DataFrame to partition.
        partition_by: Partition columns.

    Yields:
        Tuple of partition values and group dataframe.
    """
    if not partition_by:
        yield {}, df
        return

    grouped = df.groupby(partition_by, dropna=False)
    for keys, group in grouped:
        if not isinstance(keys, tuple):
            keys = (keys,)
        values = dict(zip(partition_by, keys))
        yield values, group.reset_index(drop=True)


def _materialize_spark_dataframe(df: Any, contract, target: Path, output_format: str) -> Dict[str, Any]:
    """
    Materialize Spark DataFrames to a path (CSV, Parquet, Delta, Iceberg).

    Args:
        df: Spark DataFrame to write.
        contract: DataContract with materialization settings.
        target: Target path for output.
        output_format: Format to use for Spark write.

    Returns:
        Metadata about the write.
    """
    mat = contract.materialization
    strategy = (mat.strategy or "append").lower()
    partition_by = list(mat.partition_by or [])
    reprocess_policy = getattr(mat, "reprocess_policy", "overwrite_partition")

    if strategy not in ["append", "overwrite"]:
        raise ValueError("Spark materialization supports append/overwrite only in OSS.")

    writer = df.write.format(output_format)
    if partition_by:
        writer = writer.partitionBy(*partition_by)

    mode = "append" if strategy == "append" else "overwrite"
    if strategy == "append" and reprocess_policy in ["overwrite_partition", "overwrite_partition_safe"]:
        try:
            df.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        except Exception:
            pass
        mode = "overwrite"

    target_str = str(target)
    if target_str.startswith("table:"):
        table_name = target_str[len("table:") :]
        writer.mode(mode).saveAsTable(table_name)
        logger.info(f"Materialized Spark dataframe to table {table_name} ({output_format})")
        return {"target": table_name, "rows_written": df.count(), "format": output_format}

    writer.mode(mode).save(target_str)
    logger.info(f"Materialized Spark dataframe to {target_str} ({output_format})")
    return {"target": target_str, "rows_written": df.count(), "format": output_format}


def materialize_dataframe(
    df: Any,
    contract,
    target_path: Optional[Path] = None,
    *,
    output_format: Optional[str] = None,
    engine_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Materialize validated data to the configured target.

    Args:
        df: Engine dataframe (polars/pandas/duckdb/spark).
        contract: DataContract with materialization settings.
        target_path: Optional override target path.
        output_format: Optional override output format.
        engine_name: Optional engine name for engine-specific write paths.

    Returns:
        Metadata about the write (target, rows_written, format).
    """
    if contract is None or contract.materialization is None:
        return {}

    mat = contract.materialization
    resolved_target, resolved_format = _resolve_target(contract, target_path)
    if output_format:
        resolved_format = output_format
    if resolved_format:
        resolved_format = resolved_format.lower()

    if resolved_target is None or resolved_format is None:
        logger.warning("Materialization skipped: target path or format could not be resolved.")
        return {}

    if engine_name == "spark" and hasattr(df, "sparkSession") and hasattr(df, "write"):
        return _materialize_spark_dataframe(
            df,
            contract,
            resolved_target,
            resolved_format,
        )

    strategy = (mat.strategy or "append").lower()
    partition_by = list(mat.partition_by or [])
    reprocess_policy = getattr(mat, "reprocess_policy", "overwrite_partition")

    primary_key = list(contract.primary_key or [])
    scd2_cfg = getattr(mat, "scd2", None)
    scd2_cfg = scd2_cfg if isinstance(scd2_cfg, dict) else {}

    if partition_by and strategy in ["merge", "scd2"]:
        raise ValueError("merge/scd2 strategies do not support partition_by in OSS materialization.")

    pdf = _to_pandas(df)

    if partition_by:
        missing = [col for col in partition_by if col not in pdf.columns]
        if missing:
            raise ValueError(f"Partition columns missing from data: {', '.join(missing)}")

    if strategy in ["merge", "scd2"]:
        missing_pk = [col for col in primary_key if col not in pdf.columns]
        if missing_pk:
            raise ValueError(f"Primary key columns missing from data: {', '.join(missing_pk)}")
    rows_written = 0

    is_dir_target = bool(partition_by) or resolved_target.suffix == ""
    if resolved_target.exists() and resolved_target.is_dir():
        is_dir_target = True

    if partition_by:
        base_dir = resolved_target
        base_dir.mkdir(parents=True, exist_ok=True)
        timestamp_tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")

        for part_values, group in _partition_groups(pdf, partition_by):
            part_dir = base_dir
            for col, val in part_values.items():
                part_dir = part_dir / f"{col}={_safe_partition_value(val)}"

            safe_overwrite = reprocess_policy == "overwrite_partition_safe"
            overwrite_partition = reprocess_policy == "overwrite_partition" or strategy == "overwrite" or safe_overwrite

            if overwrite_partition and not safe_overwrite:
                if part_dir.exists():
                    shutil.rmtree(part_dir)
            part_dir.mkdir(parents=True, exist_ok=True)

            if strategy == "append" and not overwrite_partition:
                file_name = f"data_{timestamp_tag}.{resolved_format}"
            else:
                file_name = f"data.{resolved_format}"

            if safe_overwrite:
                tmp_dir = part_dir.parent / f".{part_dir.name}.tmp_{uuid4().hex}"
                if tmp_dir.exists():
                    shutil.rmtree(tmp_dir)
                tmp_dir.mkdir(parents=True, exist_ok=True)
                out_path = tmp_dir / file_name
                try:
                    _write_frame(group, out_path, resolved_format)
                except Exception:
                    shutil.rmtree(tmp_dir, ignore_errors=True)
                    raise
                if part_dir.exists():
                    shutil.rmtree(part_dir)
                shutil.move(str(tmp_dir), str(part_dir))
            else:
                out_path = part_dir / file_name
                _write_frame(group, out_path, resolved_format)
            rows_written += len(group)

        logger.info(f"Materialized {rows_written} rows to partitioned path: {base_dir}")
        return {"target": str(base_dir), "rows_written": rows_written, "format": resolved_format}

    if is_dir_target:
        resolved_target.mkdir(parents=True, exist_ok=True)
        target_file = resolved_target / f"data.{resolved_format}"
    else:
        target_file = resolved_target
        if target_file.suffix == "":
            target_file = target_file.with_suffix(f".{resolved_format}")
        target_file.parent.mkdir(parents=True, exist_ok=True)

    if strategy == "overwrite":
        _write_frame(pdf, target_file, resolved_format)
        rows_written = len(pdf)
    elif strategy == "append":
        if target_file.exists():
            import pandas as pd
            existing = _read_frame(target_file, resolved_format)
            combined = pd.concat([existing, pdf], ignore_index=True)
            _write_frame(combined, target_file, resolved_format)
            rows_written = len(combined)
        else:
            _write_frame(pdf, target_file, resolved_format)
            rows_written = len(pdf)
    elif strategy == "merge":
        if target_file.exists():
            existing = _read_frame(target_file, resolved_format)
            merged = _merge_frames(existing, pdf, primary_key)
        else:
            merged = pdf
        _write_frame(merged, target_file, resolved_format)
        rows_written = len(merged)
    elif strategy == "scd2":
        if target_file.exists():
            existing = _read_frame(target_file, resolved_format)
            merged = _scd2_frames(existing, pdf, primary_key, scd2_cfg)
        else:
            merged = _scd2_frames(_to_pandas(pdf)[:0], pdf, primary_key, scd2_cfg)
        _write_frame(merged, target_file, resolved_format)
        rows_written = len(merged)
    else:
        raise ValueError(f"Unsupported materialization strategy: {strategy}")

    logger.info(f"Materialized {rows_written} rows to {target_file}")
    return {"target": str(target_file), "rows_written": rows_written, "format": resolved_format}


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
    quarantine_target.parent.mkdir(parents=True, exist_ok=True)
    target_file = quarantine_target
    explicit_format = output_format or metadata.get("quarantine_format")
    resolved_format = str(explicit_format).lower() if explicit_format else None
    if target_file.suffix == "":
        resolved_format = resolved_format or "parquet"
        target_file = target_file.with_suffix(f".{resolved_format}")
    else:
        if resolved_format is None:
            resolved_format = target_file.suffix.lstrip(".").lower()

    if engine_name == "spark" and hasattr(df, "write"):
        spark_formats = {"parquet", "csv", "json", "delta", "iceberg"}
        if resolved_format in spark_formats:
            writer = df.write.format(resolved_format).mode("append")
            if resolved_format in ["csv", "json"]:
                writer = writer.option("header", "true")
            writer.save(str(target_file))
            rows_written = int(df.count())
            logger.info(f"Wrote {rows_written} quarantined rows to {target_file} ({resolved_format})")
            return {"target": str(target_file), "rows_written": rows_written, "format": resolved_format}

    if resolved_format not in ["csv", "parquet"]:
        raise ValueError(
            f"Unsupported quarantine format '{resolved_format}'. Use csv/parquet, "
            "or run Spark for delta/iceberg/json outputs."
        )

    pdf = _to_pandas(df)

    if target_file.exists():
        import pandas as pd
        existing = _read_frame(target_file, resolved_format)
        combined = pd.concat([existing, pdf], ignore_index=True)
        _write_frame(combined, target_file, resolved_format)
        rows_written = len(combined)
    else:
        _write_frame(pdf, target_file, resolved_format)
        rows_written = len(pdf)

    logger.info(f"Wrote {rows_written} quarantined rows to {target_file}")
    return {"target": str(target_file), "rows_written": rows_written, "format": resolved_format}


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
    folder = root / ".lakeguard"
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
    Write quarantined records to a Spark table.

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

    table_format = (metadata.get("quarantine_table_format") or "iceberg").lower()
    mode = (metadata.get("quarantine_table_mode") or "append").lower()

    spark = df.sparkSession
    parts = table_name.split(".")
    if len(parts) == 2:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {parts[0]}")
    elif len(parts) >= 3:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {parts[0]}.{parts[1]}")

    df.write.mode(mode).format(table_format).saveAsTable(table_name)
    rows_written = int(df.count())
    logger.info(f"Wrote {rows_written} quarantined rows to Spark table {table_name}")
    return {"target": table_name, "rows_written": rows_written, "format": table_format}


def _write_quarantine_table_duckdb(df: Any, contract, table_name: str, metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Write quarantined records to a DuckDB table.

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
        con.execute(f"CREATE TABLE IF NOT EXISTS {full_table} AS SELECT * FROM incoming_quarantine WHERE 1=0")
        con.execute(f"INSERT INTO {full_table} SELECT * FROM incoming_quarantine")
    finally:
        con.close()

    rows_written = len(pdf)
    logger.info(f"Wrote {rows_written} quarantined rows to DuckDB table {full_table}")
    return {"target": f"{db_path}:{full_table}", "rows_written": rows_written, "format": "duckdb"}


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
    return {"target": f"{db_path}:{table_name}", "rows_written": rows_written, "format": "sqlite"}


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
        """
        Coerce a value to float when possible.

        Args:
            value: Input value.

        Returns:
            Float value or None.
        """
        try:
            return float(value) if value is not None else None
        except Exception:
            return None

    return {
        "run_id": report.get("run_id"),
        "timestamp": report.get("timestamp"),
        "engine": report.get("engine"),
        "contract": report.get("contract"),
        "source_path": report.get("source_path"),
        "counts_total": counts.get("total"),
        "counts_good": counts.get("good"),
        "counts_quarantined": counts.get("quarantined"),
        "quarantine_ratio": _num(counts.get("quarantine_ratio")),
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


def _prepare_table_name(name: str, backend: str) -> str:
    """
    Normalize table names for backend constraints (e.g., SQLite schemas).

    Args:
        name: Raw table name.
        backend: Backend identifier.

    Returns:
        Sanitized table name.
    """
    if backend == "sqlite":
        if "." in name:
            cleaned = name.replace(".", "_")
            logger.warning(f"SQLite does not support schemas. Using table name '{cleaned}' instead of '{name}'.")
            return cleaned
    return name


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

        if spark.catalog.tableExists(table_name):
            if merge_on_run_id:
                view_name = f"lakeguard_run_log_updates_{uuid4().hex}"
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
                df.write.mode("append").format(table_format).saveAsTable(table_name)
        else:
            df.write.mode("overwrite").format(table_format).saveAsTable(table_name)
        logger.info(f"Wrote run log to Spark table {table_name}")
        return table_name

    if backend == "duckdb":
        try:
            import duckdb
        except Exception as exc:
            logger.warning(f"Run log table backend 'duckdb' unavailable: {exc}")
            return None

        base_path = getattr(contract, "_base_path", None)
        db_path = metadata.get("run_log_database") or "logs/lakeguard_run_logs.duckdb"
        db_path = _resolve_path(str(db_path), base_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        table_name = _prepare_table_name(table_name, backend)
        schema_name = None
        table_only = table_name
        parts = table_name.split(".")
        if len(parts) >= 2:
            schema_name = parts[-2]
            table_only = parts[-1]
            logger.warning(f"DuckDB backend uses schema '{schema_name}' and table '{table_only}' (ignoring catalog parts if provided).")
        con = duckdb.connect(database=str(db_path))
        if schema_name:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema_name}")
            full_table = f"{schema_name}.{table_only}"
        else:
            full_table = table_only
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {full_table} (
                run_id VARCHAR,
                timestamp VARCHAR,
                engine VARCHAR,
                contract VARCHAR,
                source_path VARCHAR,
                counts_total BIGINT,
                counts_good BIGINT,
                counts_quarantined BIGINT,
                quarantine_ratio DOUBLE,
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
        con.execute(
            f"INSERT INTO {full_table} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                record["run_id"],
                record["timestamp"],
                record["engine"],
                record["contract"],
                record["source_path"],
                record["counts_total"],
                record["counts_good"],
                record["counts_quarantined"],
                record["quarantine_ratio"],
                record["freshness_seconds"],
                record["freshness_pass"],
                record["freshness_threshold_seconds"],
                record["availability_ratio"],
                record["availability_pass"],
                record["availability_threshold"],
                record["dataset_rules_json"],
                record["row_rule_failures_json"],
                record["schema_drift_json"],
                record["report_json"],
            ],
        )
        con.close()
        logger.info(f"Wrote run log to DuckDB table {full_table} ({db_path})")
        return f"{db_path}:{full_table}"

    if backend == "sqlite":
        import sqlite3

        base_path = getattr(contract, "_base_path", None)
        db_path = metadata.get("run_log_database") or "logs/lakeguard_run_logs.sqlite"
        db_path = _resolve_path(str(db_path), base_path)
        db_path.parent.mkdir(parents=True, exist_ok=True)

        table_name = _prepare_table_name(table_name, backend)
        con = sqlite3.connect(str(db_path))
        con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                run_id TEXT,
                timestamp TEXT,
                engine TEXT,
                contract TEXT,
                source_path TEXT,
                counts_total INTEGER,
                counts_good INTEGER,
                counts_quarantined INTEGER,
                quarantine_ratio REAL,
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
        con.execute(
            f"INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                record["run_id"],
                record["timestamp"],
                record["engine"],
                record["contract"],
                record["source_path"],
                record["counts_total"],
                record["counts_good"],
                record["counts_quarantined"],
                record["quarantine_ratio"],
                record["freshness_seconds"],
                1 if record["freshness_pass"] else 0 if record["freshness_pass"] is not None else None,
                record["freshness_threshold_seconds"],
                record["availability_ratio"],
                1 if record["availability_pass"] else 0 if record["availability_pass"] is not None else None,
                record["availability_threshold"],
                record["dataset_rules_json"],
                record["row_rule_failures_json"],
                record["schema_drift_json"],
                record["report_json"],
            ],
        )
        con.commit()
        con.close()
        logger.info(f"Wrote run log to SQLite table {table_name} ({db_path})")
        return f"{db_path}:{table_name}"

    logger.warning(f"Unsupported run_log_backend: {backend}")
    return None


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
