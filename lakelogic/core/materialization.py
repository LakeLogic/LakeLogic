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


def _sanitize_arrow_nulls(table):
    """
    Replace any Arrow ``null``-typed columns with ``utf8`` (string) nulls.

    Delta Lake doesn't support the Arrow ``Null`` data type.  An all-NULL
    column can end up with this type when:
      - A Pandas DataFrame goes through ``pa.Table.from_pandas`` and the
        column has dtype ``object`` with only ``None`` values.
      - A DuckDB relation with ``CAST(NULL AS VARCHAR)`` is collected to
        Pandas first, losing the SQL type information.

    This is a lossless transformation: the values stay null, only the
    schema type changes so Delta's writer accepts them.
    """
    import pyarrow as pa

    new_fields = []
    changed = False
    for i, field in enumerate(table.schema):
        if pa.types.is_null(field.type):
            new_fields.append(field.with_type(pa.utf8()))
            changed = True
        else:
            new_fields.append(field)
    if not changed:
        return table
    new_schema = pa.schema(new_fields)
    return table.cast(new_schema)


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
    safe = text.replace(os.sep, "_").replace(" ", "_").replace(":", "_")
    if os.altsep:
        safe = safe.replace(os.altsep, "_")
    return safe


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
        # Resolve environment-aware server (respects LAKELOGIC_ENV / environments block)
        eff_server = contract.effective_server() if contract else None
        server_path = eff_server.path if eff_server else None
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
    if contract and contract.materialization:
        output_format = getattr(contract.materialization, "format", None)
    if not output_format:
        # Use environment-aware server format
        eff_server = contract.effective_server() if contract else None
        if eff_server and eff_server.format:
            output_format = eff_server.format
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
    if isinstance(df, (list, tuple)):
        return pd.DataFrame(df)
    if isinstance(df, dict):
        return pd.DataFrame([df])
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
    Write a DataFrame-like object to disk.

    Args:
        df: pandas/polars DataFrame to write.
        path: Destination path.
        output_format: csv or parquet.
    """
    if output_format == "csv":
        if hasattr(df, "write_csv"):
            df.write_csv(path)
        elif hasattr(df, "to_csv"):
            df.to_csv(path, index=False)
        else:
            raise ValueError("Unsupported dataframe type for CSV materialization.")
    elif output_format == "parquet":
        if hasattr(df, "write_parquet"):
            df.write_parquet(path)
        elif hasattr(df, "to_parquet"):
            try:
                df.to_parquet(path, index=False)
            except Exception as exc:
                # Fallback to DuckDB COPY for parquet without pyarrow/fastparquet.
                try:
                    import duckdb

                    owns_connection = False
                    con = None
                    if hasattr(df, "connection") and hasattr(df, "sql_query"):
                        con = df.connection
                    else:
                        con = duckdb.connect()
                        owns_connection = True

                    try:
                        if hasattr(df, "sql_query"):
                            con.execute(f"COPY ({df.sql_query()}) TO '{path}' (FORMAT PARQUET)")
                        else:
                            con.register("incoming_df", df)
                            con.execute(f"COPY incoming_df TO '{path}' (FORMAT PARQUET)")
                    finally:
                        if owns_connection and con is not None:
                            con.close()
                    return
                except Exception:
                    pass
                try:
                    import polars as pl
                    pl.from_pandas(df).write_parquet(path)
                except Exception:
                    raise ValueError(
                        "Parquet materialization requires pyarrow/fastparquet, duckdb, or polars as a fallback."
                    ) from exc
        else:
            raise ValueError("Unsupported dataframe type for Parquet materialization.")
    elif output_format == "iceberg":
        try:
            import duckdb
            # Create a localized connection or use existing if it's a relation
            if hasattr(df, "connection") and hasattr(df, "sql_query"): 
                con = df.connection
                con.execute("INSTALL iceberg; LOAD iceberg;")
                con.execute("INSTALL httpfs; LOAD httpfs;")
                con.execute(f"COPY ({df.sql_query()}) TO '{path}' (FORMAT ICEBERG)")
            else:
                con = duckdb.connect()
                con.execute("INSTALL iceberg; LOAD iceberg;")
                con.execute("INSTALL httpfs; LOAD httpfs;")
                con.register("incoming_df", df)
                con.execute(f"COPY incoming_df TO '{path}' (FORMAT ICEBERG)")
        except ImportError:
            raise ValueError("Iceberg materialization requires 'duckdb' installed.")
        except Exception as e:
            raise ValueError(f"Iceberg materialization failed: {e}")
    elif output_format == "delta":
        try:
            from deltalake import write_deltalake
            # If it's a DuckDB relation, we need to bring it to memory (Arrow preferred)
            data = df
            if hasattr(df, "to_arrow_table"):
                data = df.to_arrow_table()
            elif hasattr(df, "to_pandas"):
                data = df.to_pandas()

            # Ensure no Arrow Null-typed columns (Delta Lake rejects them)
            import pyarrow as pa
            if not isinstance(data, pa.Table):
                data = pa.Table.from_pandas(data) if hasattr(data, "columns") else data
            if isinstance(data, pa.Table):
                data = _sanitize_arrow_nulls(data)

            write_deltalake(path, data, mode="overwrite", schema_mode="overwrite")
        except ImportError:
            raise ValueError("Delta materialization requires 'deltalake' installed (pip install deltalake).")
        except Exception as e:
            raise ValueError(f"Delta materialization failed: {e}")
    else:
        raise ValueError(f"Unsupported output format: {output_format}")


def _pandas_available() -> bool:
    try:
        import pandas  # noqa: F401
    except Exception:
        return False
    return True


def _row_count(df: Any) -> Optional[int]:
    try:
        if hasattr(df, "height"):
            return int(df.height)
        return int(len(df))
    except Exception:
        return None


def _is_polars_frame(df: Any) -> bool:
    try:
        import polars as pl
    except Exception:
        return False
    return isinstance(df, (pl.DataFrame, pl.LazyFrame))


def _frame_has_columns(df: Any) -> bool:
    """
    Best-effort check whether a frame has defined columns.
    Used to avoid writing empty/invalid files (e.g., 0-column parquet).
    """
    if df is None:
        return False
    try:
        if hasattr(df, "columns"):
            return len(df.columns) > 0
    except Exception:
        pass
    try:
        if hasattr(df, "schema"):
            schema = df.schema
            if schema is not None:
                return len(schema) > 0
    except Exception:
        pass
    try:
        if hasattr(df, "collect_schema"):
            schema = df.collect_schema()
            return len(schema) > 0
    except Exception:
        pass
    if isinstance(df, dict):
        return len(df) > 0
    if isinstance(df, (list, tuple)):
        if not df:
            return False
        first = df[0]
        if isinstance(first, dict):
            return len(first) > 0
        return True
    return True


def _append_without_pandas(df: Any, target_file: Path, output_format: str) -> int:
    try:
        import polars as pl
    except Exception as exc:
        raise ValueError("Append without pandas requires polars installed.") from exc

    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    if not isinstance(df, pl.DataFrame):
        raise ValueError("Append without pandas requires a Polars DataFrame.")

    if target_file.exists():
        if output_format == "csv":
            existing = pl.read_csv(target_file)
        elif output_format == "parquet":
            existing = pl.read_parquet(target_file)
        else:
            raise ValueError(f"Unsupported output format: {output_format}")
        combined = pl.concat([existing, df], how="vertical")
        _write_frame(combined, target_file, output_format)
        return int(combined.height)

    _write_frame(df, target_file, output_format)
    return int(df.height)


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
        try:
            return pd.read_parquet(path)
        except Exception as exc:
            try:
                import duckdb
                return duckdb.read_parquet(str(path)).df()
            except Exception:
                pass
            try:
                import polars as pl
                return pl.read_parquet(path).to_pandas()
            except Exception:
                raise ValueError(
                    "Parquet reads require pyarrow/fastparquet, duckdb, or polars as a fallback."
                ) from exc
    if output_format == "delta":
        try:
            import polars as pl
            return pl.read_delta(str(path)).to_pandas()
        except (ImportError, Exception):
            from deltalake import DeltaTable
            return DeltaTable(path).to_pandas()
    if output_format == "iceberg":
        import duckdb
        con = duckdb.connect()
        con.execute("INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;")
        return con.execute(f"SELECT * FROM iceberg_scan('{path}')").to_df()
    raise ValueError(f"Unsupported output format: {output_format}")


def _merge_frames(
    existing, 
    incoming, 
    primary_key: List[str],
    soft_delete_col: Optional[str] = None,
    soft_delete_val: Any = True,
    soft_delete_time_col: Optional[str] = None,
    soft_delete_reason_col: Optional[str] = None,
    cdc_op_field: Optional[str] = None,
    cdc_delete_values: Optional[List[Any]] = None
):
    """
    Merge incoming rows into existing rows using a primary key.
    Supports CDC delete signals and soft-deletion with metadata.

    Args:
        existing: Existing dataframe.
        incoming: Incoming dataframe (unfiltered).
        primary_key: Primary key columns.
        soft_delete_col: Optional column to flag as deleted.
        soft_delete_val: Value to set in soft_delete_col.
        soft_delete_time_col: Optional column for deletion timestamp.
        soft_delete_reason_col: Optional column for deletion reason.
        cdc_op_field: Column indicating the CDC operation (U, I, D).
        cdc_delete_values: List of values in cdc_op_field representing a delete.

    Returns:
        Merged dataframe.
    """
    if not primary_key:
        raise ValueError("primary_key is required for merge strategy.")
    import pandas as pd
    from datetime import datetime, timezone

    existing = existing.copy()
    incoming = incoming.copy()

    # 1. Handle CDC Deletes
    deletes = pd.DataFrame()
    if cdc_op_field and cdc_delete_values and cdc_op_field in incoming.columns:
        delete_mask = incoming[cdc_op_field].isin(cdc_delete_values)
        deletes = incoming[delete_mask].copy()
        incoming = incoming[~delete_mask].copy()

    all_cols = list(dict.fromkeys(list(existing.columns) + list(incoming.columns)))
    
    # Ensure metadata columns exist in schema
    metadata_cols = []
    if soft_delete_col: metadata_cols.append(soft_delete_col)
    if soft_delete_time_col: metadata_cols.append(soft_delete_time_col)
    if soft_delete_reason_col: metadata_cols.append(soft_delete_reason_col)

    for col in metadata_cols:
        if col not in all_cols:
            all_cols.append(col)
            existing[col] = None

    existing = existing.reindex(columns=all_cols)
    incoming = incoming.reindex(columns=all_cols)

    # Prepare indices
    existing = existing.set_index(primary_key)
    incoming = incoming.set_index(primary_key)

    # 2. Apply Updates/Inserts
    existing.update(incoming)
    new_rows = incoming.loc[~incoming.index.isin(existing.index)]
    
    # 3. Apply Soft Deletes
    if not deletes.empty and soft_delete_col:
        deletes = deletes.reindex(columns=all_cols)
        
        # Set metadata for the delete batch (Smart Fill)
        deletes[soft_delete_col] = soft_delete_val
        
        if soft_delete_time_col:
            # Fill only where source didn't provide a timestamp
            now_ts = datetime.now(timezone.utc).isoformat()
            if soft_delete_time_col in deletes.columns:
                deletes[soft_delete_time_col] = deletes[soft_delete_time_col].fillna(now_ts)
            else:
                deletes[soft_delete_time_col] = now_ts
                
        if soft_delete_reason_col:
            # Fill only where source didn't provide a reason
            default_reason = "cdc_delete_signal"
            if soft_delete_reason_col in deletes.columns:
                deletes[soft_delete_reason_col] = deletes[soft_delete_reason_col].fillna(default_reason).astype(str)
            else:
                deletes[soft_delete_reason_col] = default_reason
            
        deletes = deletes.set_index(primary_key)
        
        # Update existing records with the delete flag and metadata
        existing.update(deletes)
        
        # If deleted record didn't exist, we might still want to insert it as a "tombstone"
        new_deletes = deletes.loc[~deletes.index.isin(existing.index)]
        if not new_deletes.empty:
            new_rows = pd.concat([new_rows, new_deletes])

    # 4. Apply Hard Deletes (if no soft delete col)
    elif not deletes.empty:
        # Filter existing by removing matching keys from deletes
        delete_keys = deletes.set_index(primary_key).index
        existing = existing.loc[~existing.index.isin(delete_keys)]

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


def _spark_merge_dataframe(
    spark,
    incoming_df: Any,
    target: str,
    primary_key: List[str],
    output_format: str,
) -> Dict[str, Any]:
    """
    Perform a native Spark merge (upsert) without collecting to pandas.

    Uses DataFrame joins to update existing records and insert new ones.
    For Delta Lake tables, uses MERGE INTO when available.

    Args:
        spark: SparkSession.
        incoming_df: New data to merge.
        target: Target path or table name.
        primary_key: Primary key columns for matching.
        output_format: Output format (delta, parquet, etc.).

    Returns:
        Metadata about the write.
    """
    from pyspark.sql import functions as F

    is_table = target.startswith("table:")
    table_or_path = target[6:] if is_table else target

    # For Delta tables, use native MERGE INTO
    if output_format == "delta" and is_table:
        try:
            from delta.tables import DeltaTable
            if DeltaTable.isDeltaTable(spark, table_or_path) or spark.catalog.tableExists(table_or_path):
                delta_table = DeltaTable.forName(spark, table_or_path) if is_table else DeltaTable.forPath(spark, table_or_path)
                merge_condition = " AND ".join([f"target.{col} = source.{col}" for col in primary_key])
                update_cols = {col: f"source.{col}" for col in incoming_df.columns if col not in primary_key}
                insert_cols = {col: f"source.{col}" for col in incoming_df.columns}

                delta_table.alias("target").merge(
                    incoming_df.alias("source"),
                    merge_condition
                ).whenMatchedUpdate(set=update_cols).whenNotMatchedInsert(values=insert_cols).execute()

                rows_written = incoming_df.count()
                logger.info(f"Merged {rows_written} rows into Delta table {table_or_path}")
                return {"target": table_or_path, "rows_written": rows_written, "format": output_format}
        except ImportError:
            logger.debug("Delta Lake not available, falling back to DataFrame merge")
        except Exception as e:
            logger.debug(f"Delta MERGE failed, falling back to DataFrame merge: {e}")

    # Fallback: DataFrame-based merge for non-Delta or when Delta unavailable
    try:
        if is_table and spark.catalog.tableExists(table_or_path):
            existing_df = spark.table(table_or_path)
        elif not is_table and Path(table_or_path).exists():
            existing_df = spark.read.format(output_format).load(table_or_path)
        else:
            # No existing data, just write
            writer = incoming_df.write.format(output_format).mode("overwrite")
            if is_table:
                writer.saveAsTable(table_or_path)
            else:
                writer.save(table_or_path)
            rows_written = incoming_df.count()
            logger.info(f"Wrote {rows_written} rows to {table_or_path} (no existing data)")
            return {"target": table_or_path, "rows_written": rows_written, "format": output_format}
    except Exception:
        # Target doesn't exist yet
        writer = incoming_df.write.format(output_format).mode("overwrite")
        if is_table:
            writer.saveAsTable(table_or_path)
        else:
            writer.save(table_or_path)
        rows_written = incoming_df.count()
        logger.info(f"Wrote {rows_written} rows to {table_or_path} (new target)")
        return {"target": table_or_path, "rows_written": rows_written, "format": output_format}

    # Perform merge via anti-join + union
    # 1. Find rows in existing that DON'T match incoming (to keep unchanged)
    # 2. Union with all incoming rows (which are updates or inserts)
    join_condition = [existing_df[col] == incoming_df[col] for col in primary_key]

    # Get non-matching existing rows (rows not being updated)
    unchanged = existing_df.join(incoming_df, on=primary_key, how="left_anti")

    # Align columns
    all_columns = list(dict.fromkeys(existing_df.columns + [c for c in incoming_df.columns if c not in existing_df.columns]))
    for col in all_columns:
        if col not in unchanged.columns:
            unchanged = unchanged.withColumn(col, F.lit(None))
        if col not in incoming_df.columns:
            incoming_df = incoming_df.withColumn(col, F.lit(None))

    unchanged = unchanged.select(*all_columns)
    incoming_df = incoming_df.select(*all_columns)

    merged = unchanged.union(incoming_df)

    writer = merged.write.format(output_format).mode("overwrite")
    if is_table:
        writer.saveAsTable(table_or_path)
    else:
        writer.save(table_or_path)

    rows_written = merged.count()
    logger.info(f"Merged {rows_written} rows to {table_or_path}")
    return {"target": table_or_path, "rows_written": rows_written, "format": output_format}


def _spark_scd2_dataframe(
    spark,
    incoming_df: Any,
    target: str,
    primary_key: List[str],
    scd2_cfg: Dict[str, Any],
    output_format: str,
) -> Dict[str, Any]:
    """
    Perform native Spark SCD2 (Slowly Changing Dimension Type 2) without collecting to pandas.

    Closes current records and appends new versions using Spark DataFrame operations.

    Args:
        spark: SparkSession.
        incoming_df: New data with updates.
        target: Target path or table name.
        primary_key: Primary key columns.
        scd2_cfg: SCD2 configuration (effective_from_field, effective_to_field, current_flag_field).
        output_format: Output format.

    Returns:
        Metadata about the write.
    """
    from pyspark.sql import functions as F

    effective_from = scd2_cfg.get("effective_from_field", "effective_from")
    effective_to = scd2_cfg.get("effective_to_field", "effective_to")
    current_flag = scd2_cfg.get("current_flag_field", "is_current")

    now_value = scd2_cfg.get("default_effective_from")
    if not now_value:
        now_value = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    is_table = target.startswith("table:")
    table_or_path = target[6:] if is_table else target

    # Add SCD2 columns to incoming if missing
    if effective_from not in incoming_df.columns:
        incoming_df = incoming_df.withColumn(effective_from, F.lit(now_value))
    if effective_to not in incoming_df.columns:
        incoming_df = incoming_df.withColumn(effective_to, F.lit(None).cast("string"))
    if current_flag not in incoming_df.columns:
        incoming_df = incoming_df.withColumn(current_flag, F.lit(True))

    # Try to read existing data
    existing_df = None
    try:
        if is_table and spark.catalog.tableExists(table_or_path):
            existing_df = spark.table(table_or_path)
        elif not is_table and Path(table_or_path).exists():
            existing_df = spark.read.format(output_format).load(table_or_path)
    except Exception:
        pass

    if existing_df is None or existing_df.count() == 0:
        # No existing data, just write incoming
        writer = incoming_df.write.format(output_format).mode("overwrite")
        if is_table:
            writer.saveAsTable(table_or_path)
        else:
            writer.save(table_or_path)
        rows_written = incoming_df.count()
        logger.info(f"Wrote {rows_written} SCD2 rows to {table_or_path} (initial load)")
        return {"target": table_or_path, "rows_written": rows_written, "format": output_format}

    # Ensure existing has SCD2 columns
    if effective_from not in existing_df.columns:
        existing_df = existing_df.withColumn(effective_from, F.lit(None).cast("string"))
    if effective_to not in existing_df.columns:
        existing_df = existing_df.withColumn(effective_to, F.lit(None).cast("string"))
    if current_flag not in existing_df.columns:
        existing_df = existing_df.withColumn(current_flag, F.lit(True))

    # Get incoming keys
    incoming_keys = incoming_df.select(*primary_key).distinct()

    # Records to close: existing current records that have matching incoming keys
    join_condition = [existing_df[col] == incoming_keys[col] for col in primary_key]
    records_to_close = existing_df.join(incoming_keys, on=primary_key, how="inner").filter(
        F.col(current_flag) == True
    )

    # Get the effective_from from incoming for closing
    incoming_effective = incoming_df.select(
        *primary_key,
        F.col(effective_from).alias("_new_effective_from")
    ).distinct()

    # Close the records
    closed_records = records_to_close.join(incoming_effective, on=primary_key, how="left").withColumn(
        effective_to, F.col("_new_effective_from")
    ).withColumn(
        current_flag, F.lit(False)
    ).drop("_new_effective_from")

    # Records to keep unchanged: existing records NOT matching incoming keys OR already closed
    unchanged = existing_df.join(incoming_keys, on=primary_key, how="left_anti")
    already_closed = existing_df.join(incoming_keys, on=primary_key, how="inner").filter(
        F.col(current_flag) == False
    )

    # Align all columns
    all_columns = list(existing_df.columns)
    for col in incoming_df.columns:
        if col not in all_columns:
            all_columns.append(col)

    def align_columns(df, cols):
        for col in cols:
            if col not in df.columns:
                df = df.withColumn(col, F.lit(None))
        return df.select(*cols)

    unchanged = align_columns(unchanged, all_columns)
    already_closed = align_columns(already_closed, all_columns)
    closed_records = align_columns(closed_records, all_columns)
    incoming_df = align_columns(incoming_df, all_columns)

    # Union all: unchanged + already closed + newly closed + incoming
    result = unchanged.union(already_closed).union(closed_records).union(incoming_df)

    writer = result.write.format(output_format).mode("overwrite")
    if is_table:
        writer.saveAsTable(table_or_path)
    else:
        writer.save(table_or_path)

    rows_written = result.count()
    logger.info(f"Applied SCD2 with {rows_written} total rows to {table_or_path}")
    return {"target": table_or_path, "rows_written": rows_written, "format": output_format}


def _materialize_spark_dataframe(df: Any, contract, target: Path, output_format: str) -> Dict[str, Any]:
    """
    Materialize Spark DataFrames to a path (CSV, Parquet, Delta, Iceberg).

    Supports append, overwrite, merge, and scd2 strategies natively without
    collecting to pandas (avoiding driver memory issues at scale).

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
    primary_key = list(contract.primary_key or [])
    scd2_cfg = getattr(mat, "scd2", None)
    scd2_cfg = scd2_cfg if isinstance(scd2_cfg, dict) else {}

    spark = df.sparkSession
    target_str = str(target)

    # Handle merge strategy natively
    if strategy == "merge":
        if not primary_key:
            raise ValueError("primary_key is required for merge strategy.")
        return _spark_merge_dataframe(spark, df, target_str, primary_key, output_format)

    # Handle SCD2 strategy natively
    if strategy == "scd2":
        if not primary_key:
            raise ValueError("primary_key is required for scd2 strategy.")
        return _spark_scd2_dataframe(spark, df, target_str, primary_key, scd2_cfg, output_format)

    # Standard append/overwrite
    writer = df.write.format(output_format)
    if partition_by:
        writer = writer.partitionBy(*partition_by)

    mode = "append" if strategy == "append" else "overwrite"
    if strategy == "append" and reprocess_policy in ["overwrite_partition", "overwrite_partition_safe"]:
        try:
            spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        except Exception:
            pass
        mode = "overwrite"

    if target_str.startswith("table:"):
        table_name = target_str[len("table:"):]
        writer.mode(mode).saveAsTable(table_name)
        logger.info(f"Materialized Spark dataframe to table {table_name} ({output_format})")
        return {"target": table_name, "rows_written": df.count(), "format": output_format}

    writer.mode(mode).save(target_str)
    logger.info(f"Materialized Spark dataframe to {target_str} ({output_format})")
    return {"target": target_str, "rows_written": df.count(), "format": output_format}


def _partition_aware_merge(
    df: Any,
    contract,
    resolved_target: Path,
    resolved_format: str,
    strategy: str,
    partition_by: List[str],
    primary_key: List[str],
    mat,
    scd2_cfg: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Perform merge or SCD2 within each affected partition independently.

    For **delta** format, writes a single root-level Delta table with Hive-style
    partition columns so that ``pl.read_delta(root)`` and
    ``spark.read.format("delta").load(root)`` work correctly.

    For all other formats, writes per-partition files under Hive-style
    subdirectories (original behaviour).

    Args:
        df: Incoming dataframe.
        contract: DataContract.
        resolved_target: Base target directory.
        resolved_format: Output format (csv, parquet, delta, …).
        strategy: "merge" or "scd2".
        partition_by: List of partition column names.
        primary_key: Primary key columns.
        mat: Materialization config.
        scd2_cfg: SCD2 configuration dict.

    Returns:
        Metadata dict with target, rows_written, format.
    """
    import pandas as pd

    pdf = _to_pandas(df)

    # ── Empty-frame guard ─────────────────────────────────────────────────────
    # On an incremental run where the watermark filter matched 0 new rows the
    # DataFrame has the correct schema but 0 rows.  There is nothing to write;
    # return a zero-rows metadata dict so callers don't raise on missing columns.
    if pdf.empty:
        import logging as _log
        _log.getLogger(__name__).info(
            "materialize: empty DataFrame — no rows to write, skipping materialization."
        )
        return {"target": str(resolved_target), "rows_written": 0, "format": resolved_format}

    if not primary_key:
        raise ValueError("primary_key is required for merge/scd2 strategy.")

    missing_parts = [c for c in partition_by if c not in pdf.columns]
    if missing_parts:
        raise ValueError(f"Partition columns missing from data: {', '.join(missing_parts)}")
    missing_pk = [c for c in primary_key if c not in pdf.columns]
    if missing_pk:
        raise ValueError(f"Primary key columns missing from data: {', '.join(missing_pk)}")

    base_dir = resolved_target
    base_dir.mkdir(parents=True, exist_ok=True)
    total_rows = 0

    cdc_op_field = getattr(contract.source, "cdc_op_field", None) if contract.source else None
    cdc_delete_values = getattr(contract.source, "cdc_delete_values", None) if contract.source else None
    soft_delete_col = getattr(mat, "soft_delete_column", None)
    soft_delete_val = getattr(mat, "soft_delete_value", True)
    soft_delete_time_col = getattr(mat, "soft_delete_time_column", None)
    soft_delete_reason_col = getattr(mat, "soft_delete_reason_column", None)

    # ── Delta format: single root-level Delta table ───────────────────────────
    # write_deltalake writes Hive-style partition dirs under a single _delta_log
    # at the root so pl.read_delta(base_dir) works correctly.
    #
    # Strategy:
    #   First write  → write_deltalake(mode="overwrite")  — creates the table
    #   Subsequent   → DeltaTable.merge() MERGE INTO keyed on primary_key
    #                  No partition_filters needed; stable across all versions.
    if resolved_format == "delta":
        try:
            from deltalake import write_deltalake, DeltaTable
            import pyarrow as pa
        except ImportError:
            raise ImportError(
                "Writing partitioned Delta tables requires deltalake and pyarrow: "
                "pip install deltalake pyarrow"
            )

        target_str = str(base_dir)
        table_exists = base_dir.joinpath("_delta_log").exists()

        # Pre-merge all incoming partitions into their final state first,
        # then write in a single pass to minimise Delta log transactions.
        merged_parts: list = []

        for part_values, group in _partition_groups(pdf, partition_by):
            if group.empty:
                continue

            if strategy in ("merge", "scd2") and table_exists:
                existing = pd.DataFrame(columns=group.columns)
                try:
                    dt = DeltaTable(target_str)
                    part_filter = [
                        (col, "=", str(val)) for col, val in part_values.items()
                    ]
                    existing = dt.to_pandas(filters=part_filter)
                except Exception:
                    pass

                if strategy == "merge":
                    merged = _merge_frames(
                        existing, group, primary_key,
                        soft_delete_col=soft_delete_col,
                        soft_delete_val=soft_delete_val,
                        soft_delete_time_col=soft_delete_time_col,
                        soft_delete_reason_col=soft_delete_reason_col,
                        cdc_op_field=cdc_op_field,
                        cdc_delete_values=cdc_delete_values,
                    )
                else:
                    merged = _scd2_frames(existing, group, primary_key, scd2_cfg)
            else:
                merged = group  # first write or plain append — use as-is

            if not merged.empty:
                merged_parts.append(merged)

        if not merged_parts:
            logger.info(f"Partition-aware {strategy} (delta): no rows to write → {base_dir}")
            return {"target": str(base_dir), "rows_written": 0, "format": resolved_format}

        combined = pd.concat(merged_parts, ignore_index=True)
        arrow_table = pa.Table.from_pandas(combined, preserve_index=False)
        arrow_table = _sanitize_arrow_nulls(arrow_table)  # Delta rejects Arrow Null type
        total_rows = len(combined)

        if not table_exists:
            # ── First write: create the root Delta table ──────────────────
            write_deltalake(
                target_str,
                arrow_table,
                partition_by=partition_by,
                mode="overwrite",
                schema_mode="merge",
            )
        elif primary_key:
            # ── Subsequent writes: MERGE INTO via DeltaTable.merge() ─────
            # Stable across all deltalake versions; no partition_filters API.
            pk_predicate = " AND ".join(
                f"source.{pk} = target.{pk}" for pk in primary_key
            )
            dt = DeltaTable(target_str)
            (
                dt.merge(
                    source=arrow_table,
                    predicate=pk_predicate,
                    source_alias="source",
                    target_alias="target",
                )
                .when_matched_update_all()
                .when_not_matched_insert_all()
                .execute()
            )
        else:
            # No primary key defined → plain append
            write_deltalake(
                target_str,
                arrow_table,
                partition_by=partition_by,
                mode="append",
                schema_mode="merge",
            )

        logger.info(
            f"Partition-aware {strategy} (delta): materialized {total_rows} rows "
            f"→ {base_dir}"
        )
        return {"target": str(base_dir), "rows_written": total_rows, "format": resolved_format}

    # ── Non-delta formats: per-partition file approach (original behaviour) ───
    for part_values, group in _partition_groups(pdf, partition_by):
        # Resolve the partition directory
        part_dir = base_dir
        for col, val in part_values.items():
            part_dir = part_dir / f"{col}={_safe_partition_value(val)}"

        part_dir.mkdir(parents=True, exist_ok=True)
        part_file = part_dir / f"data.{resolved_format}"

        # Load existing partition data (if any)
        if part_file.exists():
            existing = _read_frame(part_file, resolved_format)
        else:
            existing = pd.DataFrame(columns=group.columns)

        # Perform merge or SCD2 within this partition
        if strategy == "merge":
            merged = _merge_frames(
                existing, group, primary_key,
                soft_delete_col=soft_delete_col,
                soft_delete_val=soft_delete_val,
                soft_delete_time_col=soft_delete_time_col,
                soft_delete_reason_col=soft_delete_reason_col,
                cdc_op_field=cdc_op_field,
                cdc_delete_values=cdc_delete_values,
            )
        elif strategy == "scd2":
            merged = _scd2_frames(existing, group, primary_key, scd2_cfg)
        else:
            merged = group

        _write_frame(merged, part_file, resolved_format)
        total_rows += len(merged)

    logger.info(f"Partition-aware {strategy}: materialized {total_rows} rows across affected partitions → {base_dir}")
    return {"target": str(base_dir), "rows_written": total_rows, "format": resolved_format}


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
        # Partition-aware merge: merge/scd2 within each affected partition
        return _partition_aware_merge(
            df, contract, resolved_target, resolved_format,
            strategy, partition_by, primary_key, mat, scd2_cfg,
        )

    is_dir_target = bool(partition_by) or resolved_target.suffix == ""
    if resolved_target.exists() and resolved_target.is_dir():
        is_dir_target = True

    if is_dir_target:
        resolved_target.mkdir(parents=True, exist_ok=True)
        target_file = resolved_target / f"data.{resolved_format}"
    else:
        target_file = resolved_target
        if target_file.suffix == "":
            target_file = target_file.with_suffix(f".{resolved_format}")
        target_file.parent.mkdir(parents=True, exist_ok=True)

    if not _frame_has_columns(df):
        logger.info("Materialization skipped: dataframe has no columns (empty incremental batch).")
        return {"target": str(target_file), "rows_written": 0, "format": resolved_format}

    if not _pandas_available():
        if partition_by:
            raise ValueError("Partitioned materialization requires pandas (or Spark). Install pandas to proceed.")
        if strategy not in ["overwrite", "append"]:
            raise ValueError(f"Materialization strategy '{strategy}' requires pandas (or Spark). Install pandas to proceed.")
        if strategy == "append" and target_file.exists():
            rows_written = _append_without_pandas(df, target_file, resolved_format)
        else:
            _write_frame(df, target_file, resolved_format)
            rows_written = _row_count(df)
        logger.info(f"Materialized {rows_written if rows_written is not None else '?'} rows to {target_file}")
        return {"target": str(target_file), "rows_written": rows_written, "format": resolved_format}

    # Prefer native Polars writes for csv/parquet to avoid pyarrow dependency.
    if _is_polars_frame(df) and resolved_format in ["csv", "parquet"] and not partition_by and strategy in ["overwrite", "append"]:
        if strategy == "append" and target_file.exists():
            rows_written = _append_without_pandas(df, target_file, resolved_format)
        else:
            _write_frame(df, target_file, resolved_format)
            rows_written = _row_count(df)
            if rows_written is None and hasattr(df, "collect"):
                try:
                    rows_written = int(df.collect().height)
                except Exception:
                    pass
        logger.info(f"Materialized {rows_written if rows_written is not None else '?'} rows to {target_file}")
        return {"target": str(target_file), "rows_written": rows_written, "format": resolved_format}

    pdf = _to_pandas(df)

    # Empty-frame guard — nothing to write, avoid partition-column validation errors
    if pdf.empty:
        logger.info("materialize: empty DataFrame — no rows to write, skipping materialization.")
        return {"target": str(target_file), "rows_written": 0, "format": resolved_format}

    if partition_by:
        missing = [col for col in partition_by if col not in pdf.columns]
        if missing:
            raise ValueError(f"Partition columns missing from data: {', '.join(missing)}")

    if strategy in ["merge", "scd2"]:
        missing_pk = [col for col in primary_key if col not in pdf.columns]
        if missing_pk:
            raise ValueError(f"Primary key columns missing from data: {', '.join(missing_pk)}")
    rows_written = 0

    # ── Delta: use write_deltalake natively (handles partitioning + schema evolution) ──
    # The Hive-style file loop below is for parquet/csv only.
    # Delta tables must be managed at the root path with partition_by passed
    # to write_deltalake, not via directory-per-partition file writes.
    if resolved_format == "delta":
        try:
            from deltalake import write_deltalake
            import pyarrow as pa
        except ImportError as exc:
            raise ImportError(
                "Delta materialization requires the deltalake package: "
                "pip install deltalake"
            ) from exc

        # Resolve Arrow table from any engine frame
        if _is_polars_frame(pdf):
            # pdf may already be a Polars frame if we took the early path
            arrow_data = (pdf.collect() if hasattr(pdf, "collect") else pdf).to_arrow()
        elif hasattr(pdf, "to_arrow"):
            arrow_data = pdf.to_arrow()
        else:
            arrow_data = pa.Table.from_pandas(pdf if hasattr(pdf, "columns") else _to_pandas(pdf))

        arrow_data = _sanitize_arrow_nulls(arrow_data)  # Delta rejects Arrow Null type

        # strategy → delta mode
        delta_mode = "overwrite" if strategy == "overwrite" else "append"
        delta_partition_by = partition_by if partition_by else None

        resolved_target.mkdir(parents=True, exist_ok=True)
        write_deltalake(
            str(resolved_target),
            arrow_data,
            mode=delta_mode,
            partition_by=delta_partition_by,
            schema_mode="merge",   # schema evolution: new columns auto-added
        )
        rows_written = len(arrow_data)
        logger.info(
            f"Materialized {rows_written} rows to Delta table: {resolved_target} "
            f"(mode={delta_mode}, partitions={delta_partition_by})"
        )
        return {"target": str(resolved_target), "rows_written": rows_written, "format": "delta"}

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
            
            # Extract CDC and Soft Delete settings
            cdc_op_field = getattr(contract.source, "cdc_op_field", None) if contract.source else None
            cdc_delete_values = getattr(contract.source, "cdc_delete_values", None) if contract.source else None
            soft_delete_col = getattr(mat, "soft_delete_column", None)
            soft_delete_val = getattr(mat, "soft_delete_value", True)
            soft_delete_time_col = getattr(mat, "soft_delete_time_column", None)
            soft_delete_reason_col = getattr(mat, "soft_delete_reason_column", None)
            
            merged = _merge_frames(
                existing, 
                pdf, 
                primary_key,
                soft_delete_col=soft_delete_col,
                soft_delete_val=soft_delete_val,
                soft_delete_time_col=soft_delete_time_col,
                soft_delete_reason_col=soft_delete_reason_col,
                cdc_op_field=cdc_op_field,
                cdc_delete_values=cdc_delete_values
            )
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



# ── Re-exports for backwards compatibility ──────────────────────────────────
# Quarantine and run-log persistence have been extracted to focused modules.
# The functions below are re-exported so existing imports continue to work.

from lakelogic.core.quarantine import materialize_quarantine  # noqa: F401, E402
from lakelogic.core.run_log import write_run_log, get_last_run_watermark  # noqa: F401, E402
