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
    contract_name_value = None
    try:
        contract_path = getattr(contract, "_contract_path", None)
        if contract_path:
            contract_name_value = Path(contract_path).name
    except Exception:
        contract_name_value = None

    # If any capture_* field is explicitly set, only honor those explicitly set fields.
    capture_fields = {
        "capture_source_path",
        "capture_timestamp",
        "capture_run_id",
        "capture_contract_name",
        "capture_domain",
        "capture_system",
    }
    explicit_fields = set()
    if hasattr(lineage, "model_fields_set"):
        explicit_fields = set(getattr(lineage, "model_fields_set"))
    elif hasattr(lineage, "__pydantic_fields_set__"):
        explicit_fields = set(getattr(lineage, "__pydantic_fields_set__"))
    explicit_capture = explicit_fields & capture_fields

    metadata = getattr(contract, "metadata", {}) or {}
    domain_value = metadata.get("domain")
    system_value = metadata.get("system")
    columns: Dict[str, Any] = {}
    if (("capture_source_path" in explicit_capture) if explicit_capture else True) and getattr(lineage, "capture_source_path", False):
        columns[lineage.source_column_name] = source_value
    if (("capture_timestamp" in explicit_capture) if explicit_capture else True) and getattr(lineage, "capture_timestamp", False):
        columns[lineage.timestamp_column_name] = timestamp_value
    if (("capture_run_id" in explicit_capture) if explicit_capture else True) and getattr(lineage, "capture_run_id", False):
        columns[lineage.run_id_column_name] = run_id_value
    if (("capture_contract_name" in explicit_capture) if explicit_capture else True) and getattr(lineage, "capture_contract_name", False) and contract_name_value is not None:
        columns[lineage.contract_name_column_name] = contract_name_value
    if (("capture_domain" in explicit_capture) if explicit_capture else True) and getattr(lineage, "capture_domain", False) and domain_value is not None:
        columns[lineage.domain_column_name] = domain_value
    if (("capture_system" in explicit_capture) if explicit_capture else True) and getattr(lineage, "capture_system", False) and system_value is not None:
        columns[lineage.system_column_name] = system_value

    if not columns:
        return good_df, bad_df

    return add_columns(good_df, columns, engine_name), add_columns(bad_df, columns, engine_name)


def _preserve_upstream_lineage(
    df: Any, columns: List[str], prefix: str, engine_name: str
) -> Any:
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
        if col.startswith("_lakelogic_"):
            return col.replace("_lakelogic", prefix, 1)
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
                    cols = [row[0] for row in df.connection.execute(f"DESCRIBE SELECT * FROM ({df.sql_query()})").fetchall()]

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

    Args:
        df: Engine dataframe.
        columns: Mapping of column name to constant value.
        engine_name: Current engine name.

    Returns:
        Updated dataframe.
    """
    if df is None:
        return df
    try:
        import polars as pl
        if isinstance(df, pl.DataFrame):
            return df.with_columns([pl.lit(value).alias(name) for name, value in columns.items()])
    except Exception:
        pass

    try:
        import pandas as pd
        if isinstance(df, pd.DataFrame):
            updated = df.copy()
            for name, value in columns.items():
                updated[name] = value
            return updated
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

            exprs = ["*"]
            for name, value in columns.items():
                col_name = str(name).replace('"', '""')
                exprs.append(f"{_lit(value)} AS \"{col_name}\"")
            query = f"SELECT {', '.join(exprs)} FROM ({df.sql_query()})"
            return df.connection.sql(query)
    except Exception:
        pass

    if engine_name == "spark":
        try:
            from pyspark.sql import functions as F
            updated = df
            for name, value in columns.items():
                updated = updated.withColumn(name, F.lit(value))
            return updated
        except Exception:
            return df

    return df
