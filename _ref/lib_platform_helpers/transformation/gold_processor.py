# -*- coding: utf-8 -*-
"""
Gold Layer Processor

Handles Gold layer table creation with support for:
- SCD (Slowly Changing Dimensions) Type 1 and Type 2
- Fact table loading with incremental support
- Dependency resolution using topological sort
- SQL-based transformations
"""

import polars as pl
from typing import Dict, List, Any, Tuple, Optional
from loguru import logger
from collections import defaultdict, deque


def resolve_dependencies(gold_configs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Resolve dependencies between Gold tables using topological sort.
    
    Args:
        gold_configs: List of Gold table configurations
        
    Returns:
        List of Gold configs ordered by dependencies (dependencies first)
        
    Raises:
        ValueError: If circular dependencies are detected
    """
    # Build dependency graph
    graph = defaultdict(list)
    in_degree = defaultdict(int)
    table_map = {}
    
    for config in gold_configs:
        table_name = config['table_name']
        table_map[table_name] = config
        dependencies = config.get('dependencies', [])
        
        # Initialize in-degree
        if table_name not in in_degree:
            in_degree[table_name] = 0
        
        # Build edges
        for dep in dependencies:
            graph[dep].append(table_name)
            in_degree[table_name] += 1
    
    # Kahn's algorithm for topological sort
    queue = deque([table for table in table_map.keys() if in_degree[table] == 0])
    sorted_tables = []
    
    while queue:
        current = queue.popleft()
        sorted_tables.append(current)
        
        for neighbor in graph[current]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    
    # Check for circular dependencies
    if len(sorted_tables) != len(table_map):
        remaining = set(table_map.keys()) - set(sorted_tables)
        raise ValueError(f"Circular dependencies detected involving tables: {remaining}")
    
    # Return configs in dependency order
    return [table_map[name] for name in sorted_tables]


def apply_scd_type1(
    df_new,  # Union[pl.DataFrame, pyspark.sql.DataFrame]
    table_path: str,
    natural_key: List[str],
    compare_columns: List[str],
    engine: str = "polars"
) -> None:
    """
    Apply SCD Type 1 logic: Update changed records, insert new ones.
    
    Args:
        df_new: New data DataFrame (Polars or PySpark)
        table_path: Path to existing Delta table
        natural_key: Columns that uniquely identify a record
        compare_columns: Columns to check for changes
        engine: Processing engine (polars or pyspark)
    """
    from src.etl.library.lib_platform_helpers.data.table_reader import read_delta_table
    from src.etl.library.lib_platform_helpers.core.data_loading_manager import load_data_to_lakehouse
    
    logger.info(f"Applying SCD Type 1 to {table_path} using {engine}")
    
    try:
        # Read existing table
        df_existing = read_delta_table(table_path, engine=engine)
        
        if engine == "polars":
            import polars as pl
            
            # Ensure we have a DataFrame, not LazyFrame
            if isinstance(df_existing, pl.LazyFrame):
                df_existing = df_existing.collect()
            
            # Identify records to update (same key, different values)
            df_joined = df_new.join(
                df_existing,
                on=natural_key,
                how="left",
                suffix="_old"
            )
            
            # Find changed records
            change_conditions = [
                pl.col(col) != pl.col(f"{col}_old") 
                for col in compare_columns
            ]
            
            df_changed = df_joined.filter(
                pl.any_horizontal(change_conditions)
            )
            
            # Find new records (not in existing)
            df_new_records = df_new.join(
                df_existing.select(natural_key),
                on=natural_key,
                how="anti"
            )
            
            logger.info(f"SCD Type 1: {len(df_changed)} changed, {len(df_new_records)} new records")
            
            # Combine updates and inserts
            df_to_upsert = pl.concat([
                df_changed.select(df_new.columns),
                df_new_records
            ])
        
        elif engine == "pyspark":
            from pyspark.sql import functions as F
            
            # Identify records to update (same key, different values)
            df_joined = df_new.alias("new").join(
                df_existing.alias("old"),
                on=natural_key,
                how="left"
            )
            
            # Build change condition
            change_condition = None
            for col in compare_columns:
                cond = F.col(f"new.{col}") != F.col(f"old.{col}")
                change_condition = cond if change_condition is None else (change_condition | cond)
            
            df_changed = df_joined.filter(change_condition if change_condition else F.lit(False))
            
            # Find new records (anti join)
            df_new_records = df_new.join(
                df_existing.select(natural_key),
                on=natural_key,
                how="left_anti"
            )
            
            logger.info(f"SCD Type 1: {df_changed.count()} changed, {df_new_records.count()} new records")
            
            # Combine updates and inserts - select only columns from df_new
            df_changed_clean = df_changed.select([f"new.{c}" for c in df_new.columns])
            df_to_upsert = df_changed_clean.unionByName(df_new_records)
        
        else:
            raise ValueError(f"Unsupported processing_engine: {engine}")
        
        # Write with merge mode
        row_count = len(df_to_upsert) if engine == "polars" else df_to_upsert.count()
        if row_count > 0:
            load_data_to_lakehouse(
                df=df_to_upsert,
                target_uri=table_path,
                schema=None,
                engine=engine,
                mode="merge"
            )
        
    except FileNotFoundError:
        # Table doesn't exist yet, do initial load
        logger.info(f"Table {table_path} doesn't exist. Performing initial load.")
        
        if engine == "polars":
            from deltalake import write_deltalake
            
            # Write directly to Delta
            write_deltalake(
                table_or_uri=table_path,
                data=df_new,
                mode="append"
            )
        elif engine == "pyspark":
            # Write directly to Delta
            df_new.write.format("delta").mode("append").save(table_path)
        
        logger.info(f"Initial load complete for {table_path}")


def apply_scd_type2(
    df_new,  # Union[pl.DataFrame, pyspark.sql.DataFrame]
    table_path: str,
    scd_config: Dict[str, Any],
    engine: str = "polars"
) -> None:
    """
    Apply SCD Type 2 logic: Maintain full historical lineage.
    
    Args:
        df_new: New data DataFrame (Polars or PySpark)
        table_path: Path to existing Delta table
        scd_config: SCD configuration with keys:
            - natural_key: List of columns identifying a record
            - compare_columns: Columns to check for changes
            - effective_date_column: Column for version start date
            - expiration_date_column: Column for version end date
            - current_flag_column: Column indicating current version
        engine: Processing engine
    """
    from src.etl.library.lib_platform_helpers.data.table_reader import read_delta_table
    from src.etl.library.lib_platform_helpers.core.data_loading_manager import load_data_to_lakehouse
    import datetime
    
    logger.info(f"Applying SCD Type 2 to {table_path} using {engine}")
    
    natural_key = scd_config['natural_key']
    compare_columns = scd_config['compare_columns']
    effective_col = scd_config['effective_date_column']
    expiration_col = scd_config['expiration_date_column']
    current_flag_col = scd_config['current_flag_column']
    
    current_date = datetime.datetime.now()
    max_date = datetime.datetime(9999, 12, 31)
    
    try:
        # Read existing table
        df_existing = read_delta_table(table_path, engine=engine)
        
        if engine == "polars":
            import polars as pl
            
            # Ensure we have a DataFrame, not LazyFrame
            if isinstance(df_existing, pl.LazyFrame):
                df_existing = df_existing.collect()
            
            # Filter for current records
            df_current = df_existing.filter(pl.col(current_flag_col) == True)
            
            # Add SCD columns to new data
            df_new = df_new.with_columns([
                pl.lit(current_date).alias(effective_col),
                pl.lit(max_date).alias(expiration_col),
                pl.lit(True).alias(current_flag_col)
            ])
            
            # Join to find changed records
            df_joined = df_new.join(
                df_current,
                on=natural_key,
                how="left",
                suffix="_old"
            )
            
            # Detect changes
            change_conditions = [
                pl.col(col) != pl.col(f"{col}_old") 
                for col in compare_columns
            ]
            
            df_changed = df_joined.filter(
                pl.any_horizontal(change_conditions)
            )
            
            # Find new records
            df_new_records = df_new.join(
                df_current.select(natural_key),
                on=natural_key,
                how="anti"
            )
            
            logger.info(f"SCD Type 2: {len(df_changed)} changed, {len(df_new_records)} new records")
            
            # Expire old versions of changed records
            if len(df_changed) > 0:
                # Get keys of changed records
                changed_keys = df_changed.select(natural_key)
                
                # Update existing table: expire old versions
                df_existing_updated = df_existing.join(
                    changed_keys.with_columns(pl.lit(True).alias("_to_expire")),
                    on=natural_key,
                    how="left"
                ).with_columns([
                    pl.when(pl.col("_to_expire") == True)
                      .then(pl.lit(False))
                      .otherwise(pl.col(current_flag_col))
                      .alias(current_flag_col),
                    pl.when(pl.col("_to_expire") == True)
                      .then(pl.lit(current_date))
                      .otherwise(pl.col(expiration_col))
                      .alias(expiration_col)
                ]).drop("_to_expire")
                
                # Overwrite table with expired versions
                load_data_to_lakehouse(
                    df=df_existing_updated,
                    target_uri=table_path,
                    schema=None,
                    engine=engine,
                    mode="overwrite"
                )
                
                # Append new versions
                df_new_versions = df_changed.select(df_new.columns)
                load_data_to_lakehouse(
                    df=df_new_versions,
                    target_uri=table_path,
                    schema=None,
                    engine=engine,
                    mode="append"
                )
        
        elif engine == "pyspark":
            from pyspark.sql import functions as F
            
            # Filter for current records
            df_current = df_existing.filter(F.col(current_flag_col) == True)
            
            # Add SCD columns to new data
            df_new = df_new.withColumn(effective_col, F.lit(current_date)) \
                           .withColumn(expiration_col, F.lit(max_date)) \
                           .withColumn(current_flag_col, F.lit(True))
            
            # Join to find changed records
            df_joined = df_new.alias("new").join(
                df_current.alias("old"),
                on=natural_key,
                how="left"
            )
            
            # Build change condition
            change_condition = None
            for col in compare_columns:
                cond = F.col(f"new.{col}") != F.col(f"old.{col}")
                change_condition = cond if change_condition is None else (change_condition | cond)
            
            df_changed = df_joined.filter(change_condition if change_condition else F.lit(False))
            
            # Find new records
            df_new_records = df_new.join(
                df_current.select(natural_key),
                on=natural_key,
                how="left_anti"
            )
            
            logger.info(f"SCD Type 2: {df_changed.count()} changed, {df_new_records.count()} new records")
            
            # Expire old versions of changed records
            if df_changed.count() > 0:
                # Get keys of changed records
                changed_keys = df_changed.select([f"new.{k}" for k in natural_key]).distinct()
                
                # Update existing table: expire old versions
                df_existing_updated = df_existing.alias("existing").join(
                    changed_keys.withColumn("_to_expire", F.lit(True)),
                    on=natural_key,
                    how="left"
                ).withColumn(
                    current_flag_col,
                    F.when(F.col("_to_expire") == True, F.lit(False))
                     .otherwise(F.col(f"existing.{current_flag_col}"))
                ).withColumn(
                    expiration_col,
                    F.when(F.col("_to_expire") == True, F.lit(current_date))
                     .otherwise(F.col(f"existing.{expiration_col}"))
                ).drop("_to_expire")
                
                # Overwrite table with expired versions
                load_data_to_lakehouse(
                    df=df_existing_updated,
                    target_uri=table_path,
                    schema=None,
                    engine=engine,
                    mode="overwrite"
                )
                
                # Append new versions - clean column selection
                df_new_versions = df_changed.select([f"new.{c}" for c in df_new.columns])
                load_data_to_lakehouse(
                    df=df_new_versions,
                    target_uri=table_path,
                    schema=None,
                    engine=engine,
                    mode="append"
                )
        
        else:
            raise ValueError(f"Unsupported processing_engine: {engine}")
        
        # Insert completely new records (works for both engines)
        row_count = len(df_new_records) if engine == "polars" else df_new_records.count()
        if row_count > 0:
            if engine == "polars":
                from deltalake import write_deltalake
                write_deltalake(
                    table_or_uri=table_path,
                    data=df_new_records,
                    mode="append"
                )
            elif engine == "pyspark":
                df_new_records.write.format("delta").mode("append").save(table_path)
        
    except FileNotFoundError:
        # Initial load - table doesn't exist yet
        logger.info(f"Table {table_path} doesn't exist. Performing initial load with SCD columns.")
        
        # Add SCD columns for initial load
        if engine == "polars":
            import polars as pl
            from deltalake import write_deltalake
            
            df_new = df_new.with_columns([
                pl.lit(current_date).alias(effective_col),
                pl.lit(max_date).alias(expiration_col),
                pl.lit(True).alias(current_flag_col)
            ])
            
            # Write directly to Delta
            write_deltalake(
                table_or_uri=table_path,
                data=df_new,
                mode="append"
            )
            
        elif engine == "pyspark":
            from pyspark.sql import functions as F
            
            df_new = df_new.withColumn(effective_col, F.lit(current_date)) \
                           .withColumn(expiration_col, F.lit(max_date)) \
                           .withColumn(current_flag_col, F.lit(True))
            
            # Write directly to Delta
            df_new.write.format("delta").mode("append").save(table_path)
        
        logger.info(f"Initial load complete for {table_path}")


def load_fact_table(
    df,  # Union[pl.DataFrame, pyspark.sql.DataFrame]
    table_path: str,
    watermark_column: Optional[str],
    write_mode: str,
    engine: str = "polars"
) -> None:
    """
    Load fact table with optional incremental support.
    
    Args:
        df: Fact data DataFrame (Polars or PySpark)
        table_path: Path to fact table
        watermark_column: Column to use for incremental loads (e.g., timestamp)
        write_mode: 'append' or 'merge'
        engine: Processing engine ('polars' or 'pyspark')
    """
    from src.etl.library.lib_platform_helpers.core.data_loading_manager import load_data_to_lakehouse
    
    logger.info(f"Loading fact table {table_path} (mode: {write_mode}, engine: {engine})")
    
    # Check if DataFrame is empty
    row_count = len(df) if engine == "polars" else df.count()
    if row_count == 0:
        logger.warning("No data to load into fact table")
        return
    
    # Write directly to Delta with schema evolution enabled
    if engine == "polars":
        from deltalake import write_deltalake
        
        write_deltalake(
            table_or_uri=table_path,
            data=df,
            mode=write_mode if write_mode in ["append", "overwrite"] else "append",
            schema_mode="merge"  # Enable automatic schema evolution
        )
    elif engine == "pyspark":
        df.write.format("delta") \
            .mode(write_mode) \
            .option("mergeSchema", "true") \
            .save(table_path)
    
    logger.info(f"Loaded {row_count} rows into fact table with schema evolution enabled")


def delete_partition(
    table_path: str,
    partition_filter: dict,
    engine: str = "polars"
) -> None:
    """
    Delete specific partitions from a Delta table for reprocessing.
    Supports both single-column and multi-column (composite) partitioning.
    
    Args:
        table_path: Path to the Delta table
        partition_filter: Dictionary of column:values pairs for filtering
                         Example single: {"timestamp": ["2025-11-20", "2025-11-21"]}
                         Example composite: {"timestamp_date": ["2025-11-20"], "symbol": ["AAPL", "GOOGL"]}
        engine: Processing engine ('polars' or 'pyspark')
        
    Examples:
        # Single partition column
        delete_partition(
            table_path=".local-lakehouse/gold/fact_market_ohlcv",
            partition_filter={"timestamp_date": ["2025-11-20", "2025-11-21"]},
            engine="polars"
        )
        
        # Composite partitioning (date + symbol)
        delete_partition(
            table_path=".local-lakehouse/gold/fact_market_ohlcv",
            partition_filter={
                "timestamp_date": ["2025-11-20"],
                "canonical_symbol": ["AAPL", "GOOGL", "MSFT"]
            },
            engine="polars"
        )
    """
    logger.info(f"Deleting partitions from {table_path} with filter: {partition_filter}")
    
    if engine == "polars":
        from deltalake import DeltaTable
        
        dt = DeltaTable(table_path)
        
        # Build composite predicate from multiple columns
        predicates = []
        for col, values in partition_filter.items():
            if len(values) == 1:
                predicates.append(f"{col} = '{values[0]}'")
            else:
                values_str = "', '".join(values)
                predicates.append(f"{col} IN ('{values_str}')")
        
        # Combine all predicates with AND
        full_predicate = " AND ".join(predicates)
        
        dt.delete(full_predicate)
        logger.info(f"Deleted partitions matching: {full_predicate}")
        
    elif engine == "pyspark":
        from pyspark.sql import functions as F
        
        # Read table
        df = spark.read.format("delta").load(table_path)
        
        # Build composite filter
        filter_conditions = []
        for col, values in partition_filter.items():
            filter_conditions.append(~F.col(col).isin(values))
        
        # Combine with OR (keep rows that don't match ANY condition)
        from functools import reduce
        combined_filter = reduce(lambda a, b: a | b, filter_conditions)
        
        df_filtered = df.filter(combined_filter)
        
        # Overwrite table
        df_filtered.write.format("delta").mode("overwrite").save(table_path)
        logger.info(f"Deleted partitions: {partition_filter}")


def reprocess_fact_partition(
    df_new,  # Union[pl.DataFrame, pyspark.sql.DataFrame]
    table_path: str,
    partition_filter: dict,
    watermark_column: Optional[str] = None,
    engine: str = "polars"
) -> None:
    """
    Reprocess specific partitions by deleting old data and inserting new.
    Supports composite partitioning.
    
    Args:
        df_new: New data DataFrame
        table_path: Path to fact table
        partition_filter: Dictionary of column:values for partitions to reprocess
        watermark_column: Optional watermark for incremental logic
        engine: Processing engine
        
    Examples:
        # Reprocess specific dates
        reprocess_fact_partition(
            df_new=df_updated,
            table_path=".local-lakehouse/gold/fact_market_ohlcv",
            partition_filter={"timestamp_date": ["2025-11-20", "2025-11-21"]},
            engine="polars"
        )
        
        # Reprocess specific date + symbols
        reprocess_fact_partition(
            df_new=df_updated,
            table_path=".local-lakehouse/gold/fact_market_ohlcv",
            partition_filter={
                "timestamp_date": ["2025-11-20"],
                "canonical_symbol": ["AAPL", "GOOGL"]
            },
            engine="polars"
        )
    """
    logger.info(f"Reprocessing partitions: {partition_filter}")
    
    # Step 1: Delete old partitions
    delete_partition(table_path, partition_filter, engine)
    
    # Step 2: Insert new data
    if engine == "polars":
        from deltalake import write_deltalake
        
        write_deltalake(
            table_or_uri=table_path,
            data=df_new,
            mode="append",
            schema_mode="merge"
        )
    elif engine == "pyspark":
        df_new.write.format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(table_path)
    
    logger.info(f"Partition reprocessing complete for {partition_filter}")


def execute_gold_query(
    query: str,
    engine: str,
    silver_schema_name: str,
    source_table_path: str = "",
    temp_table_name: str = "_temp_gold_query",
    backfill_start: Optional[str] = None,
    backfill_end: Optional[str] = None
):  # Returns Union[pl.DataFrame, pyspark.sql.DataFrame]
    """
    Execute SQL query for Gold layer transformation.
    
    Args:
        query: SQL query string (can reference schema.table or use temp table)
        engine: 'polars' or 'pyspark'
        silver_schema_name: Name of the silver schema for table references
        source_table_path: Path to source Silver table (if querying a single table)
        temp_table_name: Temporary table name for query execution
        backfill_start: Start date for backfill mode (e.g., '2025-11-20')
        backfill_end: End date for backfill mode (e.g., '2025-11-22')
        
    Returns:
        Result DataFrame (Polars or PySpark depending on processing_engine)
    """
    from src.etl.library.lib_platform_helpers.data.table_reader import query_delta_with_sql, read_delta_table
    import re
    
    logger.info(f"Executing Gold layer SQL query using {engine}")
    
    # Replace schema placeholder if needed
    query = query.replace("${silver_schema}", silver_schema_name)
    
    # Replace backfill date placeholders if provided
    if backfill_start:
        query = query.replace("{{backfill_start}}", backfill_start)
    if backfill_end:
        query = query.replace("{{backfill_end}}", backfill_end)
    
    # If source_table_path is provided, read the table and use it as input
    if source_table_path:
        logger.info(f"Reading source Silver table: {source_table_path}")
        df_source = read_delta_table(source_table_path, engine=engine)
        
        # Replace schema.table references with temp table name
        # Pattern: silver.table_name or schema.table_name
        pattern = rf'{silver_schema_name}\.\w+'
        query = re.sub(pattern, temp_table_name, query)
        
        #logger.info(f"Modified query: {query}")
        
        # Execute query with input DataFrame
        try:
            df_result = query_delta_with_sql(
                target_uri="",
                sql_query=query,
                temp_table_name=temp_table_name,
                engine=engine,
                input_df=df_source
            )
        except Exception as e:
            # Handle case where Gold table doesn't exist in subquery (first run)
            if "does not exist" in str(e) and "gold" in query.lower():
                logger.warning(f"Gold table reference in query doesn't exist yet (first run). Attempting fallback...")
                # Replace entire COALESCE expression that contains Gold subquery with the default value
                # Pattern: COALESCE((SELECT ... FROM gold.table), 'default') -> TIMESTAMP 'default'
                import re
                
                # Find and replace COALESCE expressions containing Gold table references
                coalesce_pattern = r"COALESCE\s*\(\s*\(\s*SELECT\s+.+?\s+FROM\s+gold\.\w+.*?\)\s*,\s*'([^']+)'\s*\)"
                modified_query = re.sub(coalesce_pattern, r"TIMESTAMP '\1'", query, flags=re.IGNORECASE | re.DOTALL)
                
                #logger.info(f"Retrying with modified query: {modified_query}")
                df_result = query_delta_with_sql(
                    target_uri="",
                    sql_query=modified_query,
                    temp_table_name=temp_table_name,
                    engine=engine,
                    input_df=df_source
                )
            else:
                raise
    else:
        # Execute query - target_uri is empty since SQL contains table references
        # This requires the query to be self-contained or reference already loaded tables
        df_result = query_delta_with_sql(
            target_uri="",
            sql_query=query,
            temp_table_name=temp_table_name,
            engine=engine
        )
    
    row_count = len(df_result) if engine == "polars" else df_result.count()
    logger.info(f"Query returned {row_count} rows")
    return df_result
