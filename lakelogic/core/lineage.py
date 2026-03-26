"""
Lineage column injection for LakeLogic.

Stamps validated and quarantined DataFrames with provenance metadata
(source path, run ID, timestamp, contract name, domain, system) across
all supported engines (Polars, Pandas, DuckDB, Spark).
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from loguru import logger


def inject_lineage(
    good_df: Any,
    bad_df: Any,
    contract: Any,
    engine_name: str,
    last_run_id: Optional[str],
    pipeline_run_id: Optional[str] = None,
    source_path: Optional[Union[str, Path]] = None,
) -> Tuple[Any, Any]:
    """
    Inject lineage columns into good and bad dataframes.

    Args:
        good_df: Validated dataframe.
        bad_df: Quarantined dataframe.
        contract: DataContract instance.
        engine_name: Current engine name.
        last_run_id: Current run ID.
        pipeline_run_id: Optional pipeline-level run ID.
        source_path: Optional source path for lineage.

    Returns:
        Tuple of (good_df, bad_df) with lineage columns injected.
    """
    lineage = contract.lineage
    if not lineage or not getattr(lineage, "enabled", False):
        return good_df, bad_df

    preserve_cols = list(getattr(lineage, "preserve_upstream", []) or [])
    if preserve_cols:
        prefix = getattr(lineage, "upstream_prefix", "_upstream") or "_upstream"
        good_df = _preserve_upstream_lineage(good_df, preserve_cols, prefix, engine_name)
        bad_df = _preserve_upstream_lineage(bad_df, preserve_cols, prefix, engine_name)

    source_value = str(source_path) if source_path else None
    timestamp_value = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    run_id_value = last_run_id
    if getattr(lineage, "run_id_source", "run_id") == "pipeline_run_id" and pipeline_run_id:
        run_id_value = pipeline_run_id
    # If run_id is still None but capture_run_id is enabled, generate one here.
    # This ensures _lakelogic_run_id is never null when the column is captured.
    if run_id_value is None and getattr(lineage, "capture_run_id", True):
        from uuid import uuid4

        run_id_value = str(uuid4())
    contract_name_value = None
    try:
        contract_path = getattr(contract, "_contract_path", None)
        if contract_path:
            contract_name_value = Path(contract_path).name
        # Append version from info block if available
        info = getattr(contract, "info", None)
        if info:
            version = getattr(info, "version", None)
            if version and contract_name_value:
                contract_name_value = f"{contract_name_value} (v{version})"
    except Exception:
        contract_name_value = None

    # Resolve created_by — the user or service principal running the pipeline.
    created_by_value = getattr(lineage, "created_by_override", None)
    if not created_by_value:
        try:
            import getpass
            created_by_value = getpass.getuser()
        except Exception:
            created_by_value = "unknown"
    # created_at timestamp — stamped on first insert; merge strategy preserves it.
    created_at_value = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    metadata = getattr(contract, "metadata", {}) or {}
    domain_value = metadata.get("domain")
    system_value = metadata.get("system")
    columns: Dict[str, Any] = {}
    if getattr(lineage, "capture_source_path", True):
        columns[lineage.source_column_name] = source_value
    if getattr(lineage, "capture_timestamp", True):
        columns[lineage.timestamp_column_name] = timestamp_value
    if getattr(lineage, "capture_run_id", True):
        columns[lineage.run_id_column_name] = run_id_value
    if getattr(lineage, "capture_contract_name", True) and contract_name_value is not None:
        columns[lineage.contract_name_column_name] = contract_name_value
    if getattr(lineage, "capture_domain", True) and domain_value is not None:
        columns[lineage.domain_column_name] = domain_value
    if getattr(lineage, "capture_system", True) and system_value is not None:
        columns[lineage.system_column_name] = system_value
    if getattr(lineage, "capture_created_at", True):
        columns[getattr(lineage, "created_at_column_name", "_lakelogic_created_at")] = created_at_value
    if getattr(lineage, "capture_created_by", True) and created_by_value is not None:
        columns[getattr(lineage, "created_by_column_name", "_lakelogic_created_by")] = created_by_value

    if not columns:
        return good_df, bad_df

    return add_columns(good_df, columns, engine_name), add_columns(bad_df, columns, engine_name)


def _preserve_upstream_lineage(df: Any, columns: List[str], prefix: str, engine_name: str) -> Any:
    """
    Rename existing lineage columns to preserve upstream lineage
    before injecting new lineage values.

    Args:
        df: Engine dataframe.
        columns: Column names to preserve.
        prefix: Prefix for preserved columns.
        engine_name: Current engine name.

    Returns:
        Updated dataframe.
    """
    if df is None:
        return df

    def _new_name(col: str) -> str:
        # Prepend prefix to the original name so the lakelogic token is preserved:
        # "_lakelogic_source" → "_upstream_lakelogic_source"
        # "_lakelogic_run_id" → "_upstream_lakelogic_run_id"
        if col.startswith("_lakelogic_"):
            return f"{prefix}{col}"  # e.g. _upstream + _lakelogic_source
        return f"{prefix}_{col.lstrip('_')}"

    rename_map: Dict[str, str] = {col: _new_name(col) for col in columns}

    try:
        import polars as pl

        if isinstance(df, pl.DataFrame):
            existing = set(df.columns)
            mapping = {src: dst for src, dst in rename_map.items() if src in existing and dst not in existing}
            return df.rename(mapping) if mapping else df
    except Exception:
        pass

    try:
        import pandas as pd

        if isinstance(df, pd.DataFrame):
            existing = set(df.columns)
            mapping = {src: dst for src, dst in rename_map.items() if src in existing and dst not in existing}
            return df.rename(columns=mapping) if mapping else df
    except Exception:
        pass

    if engine_name == "spark":
        try:
            existing = set(df.columns)
            updated = df
            for src, dst in rename_map.items():
                if src in existing and dst not in existing:
                    updated = updated.withColumnRenamed(src, dst)
            return updated
        except Exception:
            return df

    try:
        import duckdb

        if isinstance(df, duckdb.DuckDBPyRelation):
            cols = []
            try:
                cols = list(df.columns)
            except Exception:
                try:
                    cols = [row[0] for row in df.connection.execute(f"DESCRIBE {df.sql_query()}").fetchall()]
                except Exception:
                    cols = [
                        row[0] for row in df.connection.execute(f"DESCRIBE SELECT * FROM ({df.sql_query()})").fetchall()
                    ]

            existing = set(cols)
            target_set = set(rename_map.values())
            exprs = []
            for col in cols:
                if col in rename_map and rename_map[col] not in existing:
                    col_name = col.replace('"', '""')
                    new_name = rename_map[col].replace('"', '""')
                    exprs.append('"{}" AS "{}"'.format(col_name, new_name))
                elif col in target_set:
                    # Drop existing target columns to avoid duplicates.
                    continue
                else:
                    exprs.append('"{}"'.format(col.replace('"', '""')))
            query = f"SELECT {', '.join(exprs)} FROM ({df.sql_query()})"
            return df.connection.sql(query)
    except Exception:
        pass

    return df


def add_columns(df: Any, columns: Dict[str, Any], engine_name: str) -> Any:
    """
    Add constant columns to a dataframe across supported engines.

    Internal LakeLogic metadata columns (names starting with ``_lakelogic_``)
    are always placed at the **far right** of the resulting schema so that
    business columns retain natural ordering across bronze / silver / gold.

    Args:
        df: Engine dataframe.
        columns: Mapping of column name to constant value.
        engine_name: Current engine name.

    Returns:
        Updated dataframe.
    """
    if df is None:
        return df

    def _sorted_to_right(col_names: List[str]) -> List[str]:
        """Return col_names with internal LakeLogic columns moved to the far right.

        Order: business columns → _upstream_lakelogic_* → _lakelogic_*
        This makes provenance auditing easy: upstream (bronze) stamp first,
        then the current layer's (silver/gold) fresh stamp.
        """
        meta_current = [c for c in col_names if c.startswith("_lakelogic_")]
        meta_upstream = [c for c in col_names if c.startswith("_upstream_")]
        rest = [c for c in col_names if not c.startswith("_lakelogic_") and not c.startswith("_upstream_")]
        return rest + meta_upstream + meta_current

    try:
        import polars as pl

        if isinstance(df, pl.DataFrame):
            updated = df.with_columns([pl.lit(value).alias(name) for name, value in columns.items()])
            return updated.select(_sorted_to_right(updated.columns))
    except Exception:
        pass

    try:
        import pandas as pd

        if isinstance(df, pd.DataFrame):
            updated = df.copy()
            for name, value in columns.items():
                updated[name] = value
            return updated[_sorted_to_right(list(updated.columns))]
    except Exception:
        pass

    # DuckDB relation support
    try:
        import duckdb

        if isinstance(df, duckdb.DuckDBPyRelation):

            def _lit(val):
                if val is None:
                    return "NULL"
                if isinstance(val, bool):
                    return "TRUE" if val else "FALSE"
                if isinstance(val, (int, float)):
                    return str(val)
                text = str(val).replace("'", "''")
                return f"'{text}'"

            # Get existing columns from the relation
            try:
                rel_cols = list(df.columns)
            except Exception:
                rel_cols = []
            existing_cols = [c for c in rel_cols if not c.startswith("_lakelogic_")]
            meta_new = list(columns.keys())
            ordered = existing_cols + meta_new
            select_exprs: List[str] = []
            for col in ordered:
                if col in columns:
                    col_name = str(col).replace('"', '""')
                    select_exprs.append(f'{_lit(columns[col])} AS "{col_name}"')
                else:
                    col_name = str(col).replace('"', '""')
                    select_exprs.append(f'"{col_name}"')

            # Build the source sub-query from the relation
            try:
                src_sql = df.sql_query()
            except Exception:
                # Fallback: materialise to a temp view first
                _tmp = f"__lineage_src_{id(df) % 100000}"
                df.create_view(_tmp)
                src_sql = f"SELECT * FROM {_tmp}"

            query = f"SELECT {', '.join(select_exprs)} FROM ({src_sql})"

            # Get the connection — try .connection attribute, fall back to module-level
            con = getattr(df, "connection", None)
            if con is None:
                con = duckdb
            try:
                return con.sql(query)
            except Exception as exc:
                logger.warning(f"DuckDB lineage injection SQL failed: {exc}")
                # Last resort: materialise to pandas, add columns there, return
                try:
                    import pandas as pd

                    pdf = df.df()
                    for name, value in columns.items():
                        pdf[name] = value
                    return pdf[_sorted_to_right(list(pdf.columns))]
                except Exception as fallback_exc:
                    logger.error(f"DuckDB lineage fallback also failed: {fallback_exc}")
                    return df
    except ImportError:
        pass
    except Exception as exc:
        logger.warning(f"DuckDB lineage injection failed: {exc}")

    if engine_name == "spark":
        try:
            from pyspark.sql import functions as F

            updated = df
            _has_source_file = "_source_file" in (df.columns if hasattr(df, "columns") else [])
            for name, value in columns.items():
                # For the source column, prefer the per-row _source_file column
                # (captured from _metadata.file_path at read time).  Fall back
                # to input_file_name(), then the static contract-level path.
                if name.endswith("_source") and value:
                    if name in df.columns:
                        pass
                    elif _has_source_file:
                        updated = updated.withColumn(name, F.col("_source_file"))
                    else:
                        updated = updated.withColumn(name, F.lit(value))
                else:
                    # Cast timestamp columns to proper TimestampType
                    if name.endswith("_at") and isinstance(value, str) and "T" in value:
                        # Convert ISO8601 to Spark compatible timestamp
                        from pyspark.sql.types import TimestampType
                        # We cast to TimestampType directly because Spark 3.x to_timestamp is strict
                        updated = updated.withColumn(name, F.lit(value).cast(TimestampType()))
                    else:
                        updated = updated.withColumn(name, F.lit(value))
            # Drop the temporary _source_file column if present
            if _has_source_file and "_source_file" in updated.columns:
                updated = updated.drop("_source_file")
            # Move _lakelogic_* to the end
            all_cols = updated.columns
            ordered = _sorted_to_right(all_cols)
            return updated.select(ordered)
        except Exception as exc:
            logger.warning(f"Spark lineage injection failed (columns not added): {exc}")
            return df

    return df
