# File: src/etl/lib_platform_helpers/core/data_loading_manager.py

import polars as pl
from typing import Dict, Any, Union, Optional
from loguru import logger

# Define the acceptable DataFrame types
PolarsFrame = Union[pl.DataFrame, pl.LazyFrame]
SparkFrame = Any  # Use Any for SparkFrame type hinting


def load_data_to_lakehouse(
    df: Union[PolarsFrame, SparkFrame],
    target_uri: str,
    schema: Dict[str, Any],
    engine: str = "polars",
    spark: Optional[Any] = None,
    mode: str = "merge",
) -> None:
    """
    Loads a DataFrame (Polars or Spark) into a Delta Lake table, handling
    append, overwrite, or merge operations based on the schema's configuration.

    Args:
        df: The input DataFrame (transformed data, e.g., Silver Layer ready).
        target_uri (str): The full path/name to the Delta table (local path or cloud URI/catalog name).
        schema (Dict[str, Any]): The full, loaded YAML schema dictionary.
        engine (str): The execution engine ('polars' or 'pyspark'). Defaults to 'polars'.
        spark (Optional[Any]): The active Spark session (required if engine='pyspark').
        mode (str): The requested write mode ('append', 'overwrite', or 'merge').
            Defaults to 'merge' based on the schema's intent.

    Raises:
        ValueError: If an unknown engine or unsupported mode is specified.
        ImportError: If 'pyspark' engine is requested but the library is missing.

    💡 Usage Example (Polars - Local Merge):
    ```python
    # Assume df_silver is a Polars DataFrame with the correct schema
    load_data_to_lakehouse(
        df=df_silver,
        target_uri='./.local-lakehouse/silver/market_ohlcv_master',
        schema=MARKET_OHLCV_SCHEMA,
        engine='polars',
        mode='merge'
    )
    ```
    """
    write_config = schema["model"]["write"]

    if engine.lower() == "polars":
        if not isinstance(df, (pl.DataFrame, pl.LazyFrame)):
            raise TypeError("Input 'df' must be a PolarsFrame for 'polars' engine.")
        return _load_data_polars(df, target_uri, write_config, mode)

    elif engine.lower() == "pyspark":
        if not spark:
            raise ValueError(
                "A SparkSession object must be provided when engine='pyspark'."
            )
        return _load_data_pyspark(spark, df, target_uri, write_config, mode)

    else:
        raise ValueError(
            f"Unknown loading engine: {engine}. Must be 'polars' or 'pyspark'."
        )


# --- POLARS IMPLEMENTATION (Local/Deltalake) ---
def _load_data_polars(
    df: Union[pl.DataFrame, pl.LazyFrame],
    target_uri: str,
    write_config: Dict[str, Any],
    mode: str,
) -> None:
    """
    Writes a Polars DataFrame to a Delta Lake table, utilizing DuckDB for MERGE
    operations and deltalake for standard append/overwrite.
    """
    
    try:
        from deltalake import write_deltalake, DeltaTable
        import duckdb  # CRITICAL: DuckDB is required for MERGE
        
        # Ensure DataFrame is collected/Arrow before writing
        data_to_write_arrow = df.to_arrow()
        
        # schema-evolve with EMPTY frame from incoming DF             
        incoming_cols  = data_to_write_arrow.column_names
                
        # current Delta schema
        df_existing_lazy = pl.scan_delta(target_uri)
        existing_pyarrow_schema = df_existing_lazy.limit(0).collect().to_arrow().schema
        target_cols = existing_pyarrow_schema.names                
        new_cols     = [c for c in incoming_cols if c not in target_cols]

        if new_cols:                                         
            empty_df = df.clear()                            # 0 rows, same schema
            empty_arrow = empty_df.to_arrow()

            logger.info(f"Evolving schema to add columns: {new_cols}")
            write_deltalake(
                table_or_uri=target_uri,
                data=empty_arrow,            # 0 rows – only metadata change
                mode="append",
                schema_mode="merge"
            )

    except ImportError as e:
        logger.error(
            f"Missing required library for Polars loading: {e}. Cannot proceed."
        )
        raise

    write_mode = mode.lower()

    # Extract required configurations
    partition_by = write_config.get("cluster_by", [])
    tbl_options_raw = write_config.get("options", {})

    # Preprocess options dictionary (converting lists to strings)
    tbl_options_processed = {}
    for key, value in tbl_options_raw.items():
        if isinstance(value, list):
            tbl_options_processed[key] = ",".join(map(str, value))
        else:
            tbl_options_processed[key] = str(value)

    # CRITICAL FIX for Timestamp Feature
    tbl_options_processed["delta.feature.timestampWithoutTimezone"] = "supported"
    
    # Inject Schema Evolution Option (Required for new columns in append/overwrite)
    tbl_options_processed["mergeSchema"] = "true"

    # ----------------------------------------------------------------------
    # Implement transactional MERGE INTO logic using DuckDB
    # ----------------------------------------------------------------------
    if write_mode == "merge":
        merge_keys_list = write_config.get("merge_keys")
        if not merge_keys_list:
            logger.error("Mode 'merge' requires 'merge_keys' list in schema config.")
            raise ValueError(
                "Mode 'merge' requires 'merge_keys' in the schema write config."
            )

        # 1. Build the match condition (e.g., "target.col1 = source.col1 AND target.col2 = source.col2")
        match_condition = " AND ".join(
            [f"target.{col} = source.{col}" for col in merge_keys_list]
        )

        logger.info(
            f"Executing DuckDB MERGE INTO on {target_uri} with condition: {match_condition}"
        )

        try:
            # Connect to an in-memory database
            con = duckdb.connect(database=":memory:", read_only=False)
            con.sql("INSTALL delta; LOAD delta;")

            # Register the incoming Arrow Table as a virtual table named 'source'
            con.register("source", data_to_write_arrow)
            
            # Get the column order from the incoming data (source of truth)
            source_columns = data_to_write_arrow.column_names
            source_col_list = ", ".join([f'"{col}"' for col in source_columns])

            # Create the view using explicit column ordering from source
            # This ensures consistent column alignment
            con.sql(
                f"CREATE OR REPLACE TABLE delta_main AS SELECT {source_col_list} FROM delta_scan('{target_uri}');"
            )

            # Execute the MERGE operation (SQL remains the same)
            merge_sql = f"""
            MERGE INTO delta_main AS target
            USING source AS source
            ON {match_condition}
            WHEN MATCHED THEN
              UPDATE SET *
            WHEN NOT MATCHED THEN
              INSERT *
            """
            con.sql(merge_sql)

            # Final step: Select with explicit column ordering to prevent misalignment
            merged_result = con.execute(f"SELECT {source_col_list} FROM delta_main;").fetch_arrow_table()

            # Overwrite the Delta table on disk (Completes the MERGE)
            write_deltalake(
                table_or_uri=target_uri,
                data=merged_result,
                mode="overwrite",
                partition_by=write_config.get("cluster_by", []),
                configuration=tbl_options_processed,
            )

            logger.info(
                "✅ DuckDB transactional MERGE completed and persisted to Delta."
            )
            con.close()

        except Exception as e:
            logger.error(f"DuckDB MERGE Failed: {e}")
            raise

    # ----------------------------------------------------------------------
    # Standard APPEND or OVERWRITE modes (Uses deltalake directly)
    # ----------------------------------------------------------------------
    else:
        write_deltalake(
            table_or_uri=target_uri,
            data=data_to_write_arrow,
            mode=write_mode,
            schema_mode="merge",
            partition_by=write_config.get("cluster_by", []),
            configuration=tbl_options_processed,
        )
        logger.info(
            f"✅ Data written successfully to {target_uri} in '{write_mode}' mode."
        )


# --- PYSPARK IMPLEMENTATION (Cloud/Cluster) ---


def _load_data_pyspark(
    spark: Any, df: SparkFrame, target_uri: str, write_config: Dict[str, Any], mode: str
) -> None:
    """
    Writes a Spark DataFrame to a Delta Lake table, utilizing Spark's native
    Delta support for MERGE, APPEND, and OVERWRITE operations.
    """
    try:
        from pyspark.sql import functions as F
        from pyspark.sql import DataFrame as SparkDataFrame
    except ImportError:
        raise ImportError("Cannot use engine='pyspark'. 'pyspark' library is missing.")
    
    try:
        from deltalake import DeltaTable
    except ImportError:
        raise ImportError("Cannot use deltalake. 'deltalake' library is missing.")
    
    write_mode = mode.lower()

    # Extract required configuration from schema
    partition_by = write_config.get("cluster_by", [])  # PySpark uses 'partitionBy'
    tbl_options = write_config.get("options", {})

    # Convert options dict to PySpark format (key="value")
    # TBLPROPERTIES (options) are usually passed during CREATE/ALTER TABLE (handled by schema_manager)
    # Here, we only pass options directly impacting the write job (e.g., compaction settings)

    # ----------------------------------------------------------------------
    # FIX: Implement MERGE INTO logic (Deduplication)
    # ----------------------------------------------------------------------
    if write_mode == "merge":
        merge_keys_list = write_config.get("merge_keys")
        if not merge_keys_list:
            raise ValueError(
                "Mode 'merge' requires 'merge_keys' in the schema write config."
            )

        # Build the match condition (e.g., "target.col1 = source.col1 AND target.col2 = source.col2")
        match_condition = " AND ".join(
            [f"target.{col} = source.{col}" for col in merge_keys_list]
        )

        logger.info(
            f"Executing MERGE INTO on {target_uri} with condition: {match_condition}"
        )

        # The core PySpark Delta Merge statement
        (
            DeltaTable.forName(spark, target_uri)
            .alias("target")
            .merge(source=df.alias("source"), condition=F.expr(match_condition))
            .whenMatchedUpdateAll()  # If PKs match, update all columns
            .whenNotMatchedInsertAll()  # If PKs don't match, insert the new row
            .execute()
        )
        logger.info("✅ Data merged successfully (deduplication complete).")

    # ----------------------------------------------------------------------
    # Standard APPEND or OVERWRITE modes
    # ----------------------------------------------------------------------
    else:
        # Ensure that non-merge modes don't have merge_keys defined, which would be unnecessary.

        # NOTE: Spark options are typically passed via .option() calls
        writer = df.write.format("delta").mode(write_mode).partitionBy(*partition_by)
        
        # Inject Schema Evolution Option
        writer = writer.option("mergeSchema", "true")

        # Apply standard write options
        for key, value in tbl_options.items():
            writer = writer.option(key, value)

        writer.save(target_uri)
        logger.info(
            f"✅ Data written successfully to {target_uri} in '{write_mode}' mode."
        )
