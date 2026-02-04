from typing import List, Dict, Any, Union
from loguru import logger
from pyspark.sql import functions as F

import polars as pl


# --- List of known transformations for the dispatcher ---
KNOWN_TRANSFORMS = [
    "rename_column",
    "cast_column",
    "drop_duplicates",
    "deduplicate_by_latest",
    "trim_strings",
    "case_normalization",
    "fill_nulls",
    "derive_column",
    "replace_column_names",
    "exclude_rows",
    "replace_column_names",
    "promote_header",
    "pivot",
]

SparkFrame = Any  # Use Any here to avoid global PySpark import dependency
# Define the Polars DataFrame type hint for clarity
PolarsFrame = Union[pl.DataFrame, pl.LazyFrame]


def resolve_renamed_primary_keys(transformations, silver_transformations, primary_keys):
    """
    Resolves the latest primary key names by applying rename_column transformations
    across both the base (bronze) and silver transformation lists.

    Args:
        transformations (list): List of transformations from the base (bronze) layer.
        silver_transformations (list): List of transformations from the silver layer.
        primary_keys (list): The list of original primary key columns.

    Returns:
        list: Updated primary key column names reflecting all renames.
    """
    rename_map = {}

    # Combine both transformation lists safely
    all_transforms = (transformations or []) + (silver_transformations or [])

    # Build rename map
    for transform in all_transforms:
        if isinstance(transform, dict) and "rename_column" in transform:
            rename_conf = transform["rename_column"]
            old_name = rename_conf.get("from")
            new_name = rename_conf.get("to")
            if old_name and new_name:
                rename_map[old_name] = new_name

    # Apply rename map to primary key list
    updated_keys = [rename_map.get(col, col) for col in primary_keys]

    # Optionally log or warn if a PK was renamed
    renamed_keys = {k: v for k, v in rename_map.items() if k in primary_keys}

    return updated_keys


def _apply_transformations_pyspark(
    df: SparkFrame, transformations: List[Dict[str, Any]]
) -> SparkFrame:
    """
    Private function containing the PySpark-specific implementation logic.
    (The original logic from your 'apply_transformations' function goes here.)
    """
    # Conditional PySpark Imports
    try:
        from pyspark.sql import DataFrame as SparkDataFrame
        from pyspark.sql import functions as F
        from pyspark.sql.types import StringType
        from pyspark.sql.window import Window
    except ImportError:
        raise ImportError(
            "Cannot use engine='pyspark'. The 'pyspark' library and Spark session are required."
        )

    if not transformations:
        return df

    # Check input type consistency
    if not isinstance(df, SparkDataFrame):
        raise TypeError("Input 'df' must be a Spark DataFrame for 'pyspark' engine.")

    df_transformed = df
    logger.info(f"Applying {len(transformations)} transformation(s)...")
    for transform in transformations:
        transform_name = None
        known_transforms = KNOWN_TRANSFORMS

        for key in transform:
            if key in known_transforms:
                transform_name = key
                break

        if not transform_name:
            logger.warning(
                f"  - WARNING: Unknown transformation in config: {transform}. Skipping."
            )
            continue

        transform_config = transform[transform_name]

        # Handle cases where the YAML parsing might flatten the structure,
        # especially for keys with nested dictionaries.
        if transform_config is None:
            logger.info(
                f"  - Handling potentially flattened configuration for '{transform_name}'."
            )
            transform_config = transform

        if transform_name == "rename_column":
            logger.info(
                f"  - Renaming column '{transform_config['from']}' to '{transform_config['to']}'"
            )
            df_transformed = df_transformed.withColumnRenamed(
                transform_config["from"], transform_config["to"]
            )

        elif transform_name == "promote_header":
            # Promotes a data row (N) to the header and discards preceding rows.

            target_row_index = transform_config["row_index"]

            logger.info(
                f"  - Promoting row index {target_row_index} to new header and discarding preceding rows."
            )

            # 1. Generate Row Index
            # Create a simple, sequential row number starting from 1
            df_ranked = df_transformed.withColumn(
                "row_num", F.monotonically_increasing_id()
            )
            window_spec = Window.orderBy("row_num")
            df_ranked = df_ranked.withColumn(
                "row_index", F.row_number().over(window_spec) - 1
            )  # 0-based index

            # 2. Extract the New Header Row
            # Filter for the row that contains the new headers
            new_header_row_df = df_ranked.filter(F.col("row_index") == target_row_index)

            if new_header_row_df.count() == 0:
                logger.error(
                    f"  - ERROR: Target row index {target_row_index} not found. Header promotion aborted."
                )
                continue

            # Get the new column names by collecting the single row (expensive operation)
            # NOTE: This requires .collect() and is a performance bottleneck for large data
            new_column_names = [
                str(new_header_row_df.select(F.col(c)).collect()[0][0])
                for c in df_transformed.columns
                if c not in ["row_num", "row_index"]  # Skip temporary columns
            ]

            # 3. Filter the Data and Clean Up
            # Keep only rows *after* the promoted header row
            df_data = df_ranked.filter(F.col("row_index") > target_row_index).drop(
                "row_num", "row_index"
            )

            # 4. Apply the New Header (The PySpark Way)
            # Since Spark doesn't have a map-based rename for the entire DF,
            # we must rename columns using the new list sequentially.
            if len(new_column_names) == len(df_data.columns):
                for old_name, new_name in zip(df_data.columns, new_column_names):
                    df_data = df_data.withColumnRenamed(old_name, new_name)
                df_transformed = df_data
                logger.info(
                    "  - Header promoted and preceding rows discarded successfully."
                )
            else:
                logger.error(
                    f"  - ERROR: Column count mismatch ({len(df_data.columns)} data cols vs {len(new_column_names)} new names). Aborting rename."
                    
                )
                
        elif transform_name == "trim_strings":
            # The config value is a list of columns to trim
            trim_cols = transform_config
            if trim_cols == ["*"]:
                # Wildcard: find all string columns and trim them
                string_cols = [
                    f.name
                    for f in df_transformed.schema.fields
                    if isinstance(f.dataType, StringType)
                ]
                logger.info(
                    f"  - Trimming whitespace from all string columns: {string_cols}"
                )
                for col_name in string_cols:
                    df_transformed = df_transformed.withColumn(
                        col_name, F.trim(F.col(col_name))
                    )
            else:
                logger.info(
                    f"  - Trimming whitespace from specified columns: {trim_cols}"
                )
                for col_name in trim_cols:
                    df_transformed = df_transformed.withColumn(
                        col_name, F.trim(F.col(col_name))
                    )

        elif transform_name == "cast_column":
            col_name = transform_config["column"]
            target_type = transform_config["type"]
            options = transform_config.get("options", {})

            logger.info(f"  - Casting column '{col_name}' to type '{target_type}'")

            # # Handle date/timestamp conversions with multiple possible formats
            # if target_type in ["date", "timestamp"] and "format" in options:
            #     date_formats = options["format"]
            #     if isinstance(date_formats, str):
            #         date_formats = [date_formats]  # allow single format too

            #     parsed_exprs = [
            #         F.try_to_timestamp(F.col(col_name), F.lit(fmt))
            #         for fmt in date_formats
            #     ]

            #     # Default fallback — Spark’s automatic timestamp inference
            #     parsed_exprs.append(F.try_to_timestamp(F.col(col_name)))

            #     # Chain all parsing attempts with coalesce()
            #     combined_expr = F.coalesce(*parsed_exprs).cast(target_type)

            #     df_transformed = df_transformed.withColumn(col_name, combined_expr)

            # else:
            #     # This generic cast handles all other Spark SQL data types,
            #     # including decimals with specified precision and scale
            #     # (e.g., "decimal(18,2)"), "integer", "double", etc.
            #     df_transformed = df_transformed.withColumn(
            #         col_name, F.col(col_name).cast(target_type)
            #     )
            
            # Call the specialized PySpark helper function
            cast_expr = _apply_pyspark_cast(col_name, target_type, options)
            
            # Apply the resulting column expression to the DataFrame
            df_transformed = df_transformed.withColumn(col_name, cast_expr)

        elif transform_name == "drop_duplicates":
            dedup_cols = transform_config
            logger.info(
                f"  - Dropping duplicates based on columns: {dedup_cols} (keeps an arbitrary record)"
            )
            df_transformed = df_transformed.dropDuplicates(dedup_cols)

        elif transform_name == "deduplicate_by_latest":
            key_cols = transform_config["key_columns"]
            timestamp_col = transform_config["timestamp_column"]

            # Ensure timestamp_col is usable as ordering column (cast to timestamp if needed)
            # Handles both DATE and TIMESTAMP types safely
            col_type = dict(df_transformed.dtypes).get(timestamp_col)
            if col_type == "date":
                df_transformed = df_transformed.withColumn(
                    timestamp_col, F.col(timestamp_col).cast("timestamp")
                )

            # Check if the DataFrame is a streaming DataFrame
            if df_transformed.isStreaming:
                logger.info(
                    f"  - Applying STREAMING deduplication by latest record using timestamp '{timestamp_col}' and keys {key_cols}"
                )
                # For streaming, we must use withWatermark and dropDuplicates
                # A watermark of "1 minute" means Spark will wait 1 minute for late data before finalizing duplicates.
                df_transformed = df_transformed.withWatermark(
                    timestamp_col, "1 minute"
                ).dropDuplicates(key_cols)
            else:
                # For batch, the window function approach is correct and more flexible
                logger.info(
                    f"  - Applying BATCH deduplication by latest record using timestamp '{timestamp_col}' and keys {key_cols}"
                )
                window_spec = Window.partitionBy(*key_cols).orderBy(
                    F.col(timestamp_col).desc()
                )
                df_ranked = df_transformed.withColumn(
                    "row_num", F.row_number().over(window_spec)
                )
                df_transformed = df_ranked.filter(F.col("row_num") == 1).drop("row_num")

        elif transform_name == "case_normalization":
            columns_to_case = transform_config["columns"]
            case_type = transform_config.get("case", "lower")

            if case_type not in ["upper", "lower"]:
                logger.info(
                    f"  - WARNING: Invalid case type '{case_type}' for case_normalization. Skipping."
                )
                continue

            logger.info(f"  - Applying {case_type} case to columns: {columns_to_case}")

            target_cols = []
            if columns_to_case == ["*"]:
                target_cols = [
                    f.name
                    for f in df_transformed.schema.fields
                    if isinstance(f.dataType, StringType)
                ]
            else:
                target_cols = columns_to_case

            case_function = F.upper if case_type == "upper" else F.lower
            for col_name in target_cols:
                if col_name in df_transformed.columns:
                    df_transformed = df_transformed.withColumn(
                        col_name, case_function(F.col(col_name))
                    )

        elif transform_name == "fill_nulls":
            columns_to_fill = transform_config["columns"]
            fill_value = transform_config["value"]
            logger.info(
                f"  - Filling null values with '{fill_value}' for columns: {columns_to_fill}"
            )

            if columns_to_fill == ["*"]:
                df_transformed = df_transformed.na.fill(fill_value)
            else:
                df_transformed = df_transformed.na.fill(
                    fill_value, subset=columns_to_fill
                )

        elif transform_name == "derive_column":
            col_name = transform_config["name"]
            expression = transform_config["expression"]
            logger.info(
                f"  - Deriving new column '{col_name}' with expression: '{expression}'"
            )
            # Use F.expr to evaluate the SQL-like expression string
            df_transformed = df_transformed.withColumn(col_name, F.expr(expression))

        elif transform_name == "pivot":
            # PySpark handles the pivot operation via a groupBy and pivot function.

            id_vars = transform_config[
                "id_vars"
            ]  # Columns to remain as identifiers (GROUP BY keys)
            pivot_cols = transform_config[
                "pivot_cols"
            ]  # Column whose unique values become headers
            value_cols = transform_config[
                "value_cols"
            ]  # Columns containing the values to aggregate

            logger.info(
                f"  - Pivoting data (PySpark): IDs={id_vars}, Headers={pivot_cols}, Values={value_cols}"
            )

            if len(pivot_cols) != 1:
                logger.error(
                    "  - ERROR: PySpark pivot implementation requires exactly one pivot_cols column."
                )
                continue

            pivot_col = pivot_cols[0]

            # 1. Group by the key columns (id_vars)
            df_pivot_base = df_transformed.groupBy(*id_vars)

            # 2. Apply the pivot, specifying the column whose values become the new headers
            df_pivot_table = df_pivot_base.pivot(pivot_col)

            # 3. Apply the aggregation (PySpark requires an explicit aggregation function)
            # NOTE: We assume 'first' or 'sum' for simple ETL pivots. We use F.first().
            agg_exprs = [F.first(c).alias(c) for c in value_cols]

            # Apply the aggregation across the value columns
            df_transformed = df_pivot_table.agg(*agg_exprs)

        elif transform_name == "replace_column_names":
            # NEW: Replace characters in column names (e.g., 'order/id' -> 'order_id')
            char_from = transform_config["from"]
            char_to = transform_config["to"]

            logger.info(
                f"  - Replacing '{char_from}' with '{char_to}' in all column names."
            )

            # --- PySpark Logic (Sequential Renaming) ---

            # 1. Iterate over all existing columns
            for old_col_name in df_transformed.columns:
                # 2. Determine the new name
                new_col_name = old_col_name.replace(char_from, char_to)

                # 3. Apply the rename if a change was actually made
                if new_col_name != old_col_name:
                    logger.info(f"    -> Renaming {old_col_name} to {new_col_name}")

                    # Use the standard Spark function to rename the column
                    df_transformed = df_transformed.withColumnRenamed(
                        old_col_name, new_col_name
                    )

        elif transform_name == "exclude_rows":
            # Excludes rows where ANY of the target columns meet the criteria.

            config = transform_config
            columns_to_check = config["columns"]
            operation = config["operation"].lower()

            # 1. Resolve target columns
            if columns_to_check == ["*"]:
                target_cols = df_transformed.columns
                logger.info("  - Excluding rows based on nullity across ALL columns.")
            else:
                target_cols = columns_to_check
                logger.info(
                    f"  - Excluding rows based on nullity across specified columns: {target_cols}"
                )

            # 2. Build the primary exclusion condition expression
            exclusion_conditions = []

            for col_name in target_cols:
                # IMPORTANT: Check if column exists before building expression
                if col_name in df_transformed.columns:
                    if operation == "is_null":
                        # Exclude rows where column IS NULL (i.e., filter out nulls)
                        exclusion_conditions.append(F.col(col_name).isNull())
                    elif operation == "is_not_null":
                        # Exclude rows where column IS NOT NULL (i.e., filter out non-nulls)
                        exclusion_conditions.append(F.col(col_name).isNotNull())
                    else:
                        logger.warning(
                            f"  - WARNING: Unknown operation '{operation}'. Skipping check for {col_name}."
                        )

            if not exclusion_conditions:
                continue

            # 3. Combine conditions using the bitwise OR operator | (SQL equivalent of OR)
            # Find rows where ANY condition is TRUE (i.e., rows to be excluded)
            combined_exclusion_expr = exclusion_conditions[0]
            for expr in exclusion_conditions[1:]:
                combined_exclusion_expr = combined_exclusion_expr | expr

            # 4. Filter: Keep only rows where the combined exclusion expression is FALSE (i.e., keep only rows that DID NOT match the exclusion)
            df_transformed = df_transformed.filter(~combined_exclusion_expr)
            logger.info("  - Filter applied successfully.")

        else:
            logger.info(
                f"  - WARNING: Unknown transformation '{transform_name}'. Skipping."
            )

    logger.info("✅ Transformations applied successfully.")
    return df_transformed


# --- Main Polars Transformation Dispatcher ---


def _apply_transformations_polars(
    df: PolarsFrame, transformations: List[Dict[str, Any]]
) -> PolarsFrame:
    """
    Applies a series of transformations to a Polars DataFrame/LazyFrame based on a config list.

    This function iterates through the transformation definitions from the pipeline configuration
    and applies the corresponding Polars operation. It operates lazily if the input is a LazyFrame.

    Args:
        df (PolarsFrame): The input Polars DataFrame or LazyFrame to be transformed.
        transformations (List[Dict[str, Any]]): A list of dictionaries defining transformations.

    Returns:
        PolarsFrame: The transformed Polars DataFrame/LazyFrame.

    💡 Usage Example:

    ```python
    import polars as pl
    from your_module import apply_transformations_polars

    # Create a mock Polars DataFrame
    df_raw = pl.LazyFrame({
        "order/id": ["1", "2", "2"],
        "customer_id": ["C1 ", "C2", "C2"],
        "signup_date_str": ["12/01/2024", "01-05-2024", "10/01/2024"],
        "updated_at": [1000, 2000, 3000]
    })

    trans_config = [
        
        {"replace_column_names": {"from": "/", "to": "_"}},
        {"trim_strings": ["*"]},
        {"cast_column": {"column": "signup_date_str", "type": "DATE", "options": {"format": ["%m/%d/%Y", "%d-%m-%Y"]}}},
        {"deduplicate_by_latest": {"key_columns": ["customer_id"], "timestamp_column": "updated_at"}},
        {"exclude_rows": {"column": "customer_id", "operation": "is_null"}}
    ]

    df_silver = apply_transformations_polars(df_raw, trans_config)
    print(df_silver.collect())
    ```
    """
    if not transformations:
        logger.warning("No Polars transformations to apply.")
        return df

    logger.info(f"Applying {len(transformations)} Polars transformation(s)...")
    df_transformed = df

    # We iterate over the config list sequentially
    for transform in transformations:
        transform_name = next(
            (key for key in transform if key in KNOWN_TRANSFORMS), None
        )

        if not transform_name:
            logger.warning(
                f"  - WARNING: Unknown Polars transformation in config: {transform}. Skipping."
            )
            continue

        transform_config = transform[transform_name]

        # --- DYNAMIC DISPATCH LOGIC ---

        if transform_name == "rename_column":
            df_transformed = df_transformed.rename(
                {transform_config["from"]: transform_config["to"]}
            )
            
            
        elif transform_name == "trim_strings":
            trim_cols = transform_config
            
            # 1. Resolve target columns based on wildcard
            if trim_cols == ["*"]:
                # FIX: Filter the schema for only Utf8 (String) type columns
                schema_dict = df_transformed.collect_schema()
                cols_to_trim = [
                    name 
                    for name, dtype in schema_dict.items() 
                    if dtype == pl.Utf8
                ]
                logger.warning(
                    f"  - Wildcard trim applied. Trimming only Utf8 columns: {cols_to_trim}"
                )
            else:
                # Use the specific columns provided by the user
                cols_to_trim = trim_cols
                logger.info(
                    f"  - Trimming whitespace from specified columns: {cols_to_trim}"
                )
            
            # 2. Apply trim using pl.col().str.strip_chars()
            df_transformed = df_transformed.with_columns(
                [
                    pl.col(c).str.strip_chars().alias(c) 
                    for c in cols_to_trim if c in df_transformed.columns
                ]
            )


        elif transform_name == "promote_header":
            # NEW: Promotes a data row (N) to the header and discards preceding rows.
            # This is complex in Polars as the input df already has a schema/header.

            # The 'target_row_index' is the 0-based index of the row to use as the header.
            target_row_index = transform_config["row_index"]

            logger.info(f"  - Promoting row index {target_row_index} to new header.")

            # Step 1: Extract the new header row data (as a Series)
            new_header_row = df_transformed.head(target_row_index + 1).tail(1).collect()

            if new_header_row.shape[0] == 0:
                logger.error(
                    f"  - ERROR: Row index {target_row_index} not found for header promotion."
                )
                continue

            # Step 2: Generate the list of new column names (strings)
            # Pandas conversion is often easiest here, but we use Polars to_list()
            new_column_names = [str(x) for x in new_header_row.row(0)]

            # Step 3: Select the original data rows, starting AFTER the new header row
            # Discard all rows up to and including the target_row_index
            df_transformed = df_transformed.slice(target_row_index + 1)

            # Step 4: Apply the new column names
            # This requires a full schema/column map, as Polars rename requires old name -> new name

            # Since the original columns are likely generic (col_0, col_1...), we rename sequentially.
            old_column_names = df_transformed.columns
            if len(old_column_names) != len(new_column_names):
                logger.error(
                    "  - ERROR: Column count mismatch after slice. Cannot promote header."
                )
                continue

            rename_map = dict(zip(old_column_names, new_column_names))
            df_transformed = df_transformed.rename(rename_map)

        elif transform_name == "cast_column":
            col_name = transform_config["column"]
            target_type = transform_config["type"].upper()
            options = transform_config.get("options", {})

            logger.info(f"  - Casting column '{col_name}' to type '{target_type}'")

            df_transformed = df_transformed.with_columns(
                pl.col(col_name).pipe(_apply_polars_cast, target_type, options)
            )

        elif transform_name == "drop_duplicates":
            dedup_cols = transform_config
            df_transformed = df_transformed.unique(subset=dedup_cols, keep="first")

        elif transform_name == "deduplicate_by_latest":
            key_cols = transform_config["key_columns"]
            timestamp_col = transform_config["timestamp_column"]

            logger.info(
                f"  - Applying deduplication by latest record using timestamp '{timestamp_col}' and keys {key_cols}"
            )

            # --- FIX: Define the ranking expression *without* the 'over=' argument,
            #          and then apply the .over(key_cols) method. ---

            # 1. Define the core ranking expression (pl.col().rank().alias("row_num"))
            rank_expr = (
                pl.col(timestamp_col)
                .rank(method="dense", descending=True)
                .alias("row_num")
            )

            # 2. Apply the window context using .over(partition_by=key_cols)
            # The Polars API requires the window to wrap the ranking expression.
            window_ranking_expr = rank_expr.over(key_cols)

            # 3. Apply the ranking and filter the result
            df_transformed = (
                df_transformed
                # Apply the computed window expression
                .with_columns(window_ranking_expr)
                .filter(pl.col("row_num") == 1)
                .drop("row_num")
            )
            
        elif transform_name == "case_normalization":
            columns_to_case = transform_config["columns"]
            case_type = transform_config.get("case", "lower")
            case_func = (
                pl.col(pl.Utf8).str.to_upper
                if case_type == "upper"
                else pl.col(pl.Utf8).str.to_lower
            )

            logger.info(f"  - Normalizing case for columns: {columns_to_case}")

            # Apply case change only to the specified columns that are strings
            df_transformed = df_transformed.with_columns(
                [
                    case_func().alias(col_name)
                    if col_name in df_transformed.columns
                    else pl.col(col_name)
                    for col_name in columns_to_case
                ]
            )

        elif transform_name == "fill_nulls":
            columns_to_fill = transform_config["columns"]
            fill_value = transform_config["value"]

            logger.info(f"  - Filling null values for columns: {columns_to_fill}")

            # Polars fill_null strategy
            df_transformed = df_transformed.with_columns(
                [pl.col(c).fill_null(fill_value).alias(c) for c in columns_to_fill]
            )

        elif transform_name == "pivot":
            id_vars = transform_config["id_vars"]
            pivot_col = transform_config["pivot_cols"][0]
            value_cols = transform_config["value_cols"]
            allow_eager = transform_config.get("allow_eager_collect", False)

            logger.info(
                f"  - Executing Pivot: IDs={id_vars}. (Eager Mode: {allow_eager})"
            )

            # 1. Check if the input is a LazyFrame and if eager execution is allowed
            is_lazy = isinstance(df_transformed, pl.LazyFrame)

            if is_lazy and not allow_eager:
                logger.error(
                    "  - ERROR: Pivot requires EAGER execution (.collect()) but 'allow_eager_collect: true' "
                    "is missing or false. Skipping transformation to preserve scalability."
                )
                continue

            # --- Execute Eager Pivot (Only if permitted or already Eager) ---

            df_temp = df_transformed  # Start with the input frame

            if is_lazy:
                # If it's lazy and allowed, force collection.
                logger.warning(
                    "  - WARNING: Forcing EAGER execution via .collect() for pivot."
                )
                try:
                    df_eager = df_temp.collect()
                except pl.exceptions.ComputeError as e:
                    logger.error(
                        f"  - FATAL: Polars Compute Error during EAGER collect: {e}"
                    )
                    raise
            else:
                # If it's already an eager DataFrame (pl.DataFrame), use it directly.
                df_eager = df_temp

            # 2. Apply the Eager Pivot
            df_eager_pivoted = df_eager.pivot(
                index=id_vars,
                columns=pivot_col,
                values=value_cols,
                aggregate_function="first",
            )

            # 3. Revert to LazyFrame (CRITICAL for downstream ETL if the pipeline is defined lazily)
            df_transformed = df_eager_pivoted.lazy()
            logger.success("✅ Eager Pivot completed and reverted to LazyFrame.")

        elif transform_name == "derive_column":
            col_name = transform_config["name"]
            expression = transform_config["expression"]

            logger.info(
                f"  - Deriving new column '{col_name}' with expression: '{expression}'"
            )
            try:
                # We pass {'pl': pl} as the necessary global context for expressions like pl.col()
                # We remove the expensive schema resolution argument from the eval call.
                derived_expr = eval(expression, {"pl": pl})

                # 2. Apply the derived expression
                df_transformed = df_transformed.with_columns(
                    derived_expr.alias(col_name)
                )

            except NameError:
                # Fallback logic remains the same
                logger.warning(
                    "  - WARNING: Expression failed due to NameError. Attempting to wrap as pl.lit()."
                )
                df_transformed = df_transformed.with_columns(
                    pl.lit(expression).alias(col_name)
                )

            except Exception as e:
                logger.error(
                    f"  - FATAL: Polars failed to evaluate expression '{expression}': {e}"
                )
                raise

            else:
                # Default behavior: attempt a direct eval assuming pl.col() is used
                # NOTE: We use the globals/locals trick to make pl available in eval context
                try:
                    # Only pass the necessary global context {'pl': pl}
                    derived_expr = eval(expression, {"pl": pl})
                    # eval(expression, {'pl': pl}, df_transformed.lazy().schema)

                except Exception as e:
                    logger.error(
                        f"  - FATAL: Polars failed to evaluate complex expression '{expression}': {e}"
                    )
                    raise

            # 2. Apply the derived expression
            df_transformed = df_transformed.with_columns(derived_expr.alias(col_name))

        elif transform_name == "replace_column_names":
            # NEW: Replace characters in column names (e.g., 'order/id' -> 'order_id')
            char_from = transform_config["from"]
            char_to = transform_config["to"]

            logger.info(
                f"  - Replacing '{char_from}' with '{char_to}' in all column names."
            )
            rename_map = {
                col: col.replace(char_from, char_to) for col in df_transformed.columns
            }
            df_transformed = df_transformed.rename(rename_map)

        elif transform_name == "exclude_rows":
            # NEW: Filter rows based on nullity of multiple columns or all columns

            config = transform_config
            columns_to_check = config["columns"]  # Can be a list of strings or ["*"]
            operation = config["operation"].lower()

            # 1. Resolve target columns
            if columns_to_check == ["*"]:
                # Use all columns present in the DataFrame
                target_cols = df_transformed.columns
                logger.info("  - Excluding rows based on nullity across ALL columns.")
            else:
                target_cols = columns_to_check
                logger.info(
                    f"  - Excluding rows based on nullity across specified columns: {target_cols}"
                )

            # 2. Build the primary exclusion condition expression
            # For robustness, we check if ANY of the target columns meet the criteria.

            conditions = []
            for col_name in target_cols:
                if col_name in df_transformed.columns:
                    if operation == "is_null":
                        # Find rows where the column IS null
                        conditions.append(pl.col(col_name).is_null())
                    elif operation == "is_not_null":
                        # Find rows where the column IS NOT null
                        conditions.append(pl.col(col_name).is_not_null())
                    else:
                        logger.warning(
                            f"  - WARNING: Unknown operation '{operation}'. Skipping check for {col_name}."
                        )

            if not conditions:
                continue

            # 3. Combine conditions using OR (Find rows where ANY condition is TRUE)
            # Example: (col1 is null) OR (col2 is null) OR (col3 is null)
            combined_exclusion_expr = conditions[0]
            for expr in conditions[1:]:
                combined_exclusion_expr = combined_exclusion_expr | expr

            # 4. Filter: Keep only rows where the combined exclusion expression is FALSE (i.e., filter out bad rows)
            df_transformed = df_transformed.filter(~combined_exclusion_expr)

    logger.info("✅ Polars Transformations applied successfully.")
    return df_transformed


# --- Helper Functions for _apply_polars_cast (Used by the dispatcher) ---

# File: src/etl/lib_platform_helpers/core/transformer.py (Inside _apply_polars_cast)


def _apply_polars_cast(
    series: pl.Expr, target_type: str, options: Dict[str, Any]
) -> pl.Expr:
    """
    Handles casting a Polars Expression to the target type, including date/timestamp parsing.

    Args:
        series (pl.Expr): The input Polars Expression (pl.col(col_name)).
        target_type (str): The desired target type (e.g., 'TIMESTAMP').
        options (Dict[str, Any]): Options dictionary.

    Returns:
        pl.Expr: The resulting Polars Expression.
    """
    # 1. Standardize type name
    target_type = target_type.upper()

    if target_type in ("DATE", "TIMESTAMP", "DATETIME"):
        # pick the format(s) from options
        fmt_in = options.get("format", "%Y-%m-%d %H:%M:%S")
        fmts = [fmt_in] if isinstance(fmt_in, str) else fmt_in

        # build parsing expressions
        parsed = [
            series.str.strptime(
                # parse *with* time-zone awareness
                pl.Datetime("us", "UTC"),
                fmt,
                strict=False,
            )
            for fmt in fmts
        ]

        # 3. coalesce multiple formats
        out = parsed[0]
        for p in parsed[1:]:
            out = out.fill_null(p)

        if target_type == "DATE":
            return out.cast(pl.Date)
        return out

    # Handle generic type casting (This logic remains sound for expressions)
    pl_type_map = {
        "STRING": pl.Utf8,
        "INT": pl.Int64,
        "INT64": pl.Int64,
        "FLOAT": pl.Float64,
        "FLOAT64": pl.Float64,
        "BOOLEAN": pl.Boolean,
        "DECIMAL": pl.Decimal,
    }

    pl_dtype = pl_type_map.get(target_type)
    if pl_dtype:
        # Use the correct expression cast syntax
        return series.cast(pl_dtype)

    logger.warning(
        f"  - WARNING: Could not cast series expression to unknown type {target_type}."
    )
    return series.alias(series.name)  # Return original expression

# Assuming this helper is defined nearby or imported in your transformer module


def _apply_pyspark_cast(col_name: str, target_type: str, options: Dict[str, Any]) -> F.Column:
    """
    Handles casting a Spark Column Expression, including complex date/timestamp parsing.

    Args:
        col_name (str): The name of the column being cast.
        target_type (str): The desired target type (e.g., 'timestamp', 'decimal(18,2)').
        options (Dict[str, Any]): Options dictionary containing 'format' if applicable.

    Returns:
        F.Column: The resulting PySpark Column Expression.
    """
    
    # Handle date/timestamp conversions with multiple possible formats
    if target_type in ["date", "timestamp"] and "format" in options:
        date_formats = options["format"]
        if isinstance(date_formats, str):
            date_formats = [date_formats]

        # Use try_to_timestamp for safety (avoids crashing on unparseable data)
        parsed_exprs = [F.try_to_timestamp(F.col(col_name), F.lit(fmt)) for fmt in date_formats]

        # Default fallback — Spark’s automatic timestamp inference
        parsed_exprs.append(F.try_to_timestamp(F.col(col_name)))

        # Chain all parsing attempts with coalesce()
        combined_expr = F.coalesce(*parsed_exprs).cast(target_type)
        
        return combined_expr
        
    else:
        # This handles all other standard Spark SQL data types (e.g., "string", "decimal(18,2)", "double", etc.)
        return F.col(col_name).cast(target_type)

def apply_transformations(
    df: Union[PolarsFrame, SparkFrame],
    transformations: List[Dict[str, Any]],
    engine: str = "polars",
) -> Union[PolarsFrame, SparkFrame]:
    """
    Applies a series of transformations by dispatching the operation to the
    appropriate engine (Polars or PySpark) based on the 'engine' parameter.

    This is the public, DRY interface for all ETL transformation jobs.

    Args:
        df: The input DataFrame (Polars DataFrame/LazyFrame OR Spark DataFrame).
        transformations (List[Dict[str, Any]]): The transformation config list (from YAML).
        engine (str): The execution engine ('polars' or 'pyspark'). Defaults to 'polars'.

    Returns:
        The transformed DataFrame/LazyFrame in the original engine type.

    Raises:
        ValueError: If an unknown engine is specified.
        ImportError: If 'pyspark' engine is requested but the library is missing.

    Example:
        # Define a list of transformations
        trans_config = [
            {"rename_column": {"from": "first_name", "to": "customer_first_name"}},
            {"cast_column": {"column": "signup_date", "type": "date", "options": {"format": "MM-dd-yyyy"}}},
            {"cast_column": {"column": "signup_date2", "type": "date", "options": {"format": ['dd/mm/yyyy', 'yyyy-mm-dd']}}},
            {"cast_column": {"column": "order_total", "type": "decimal(10, 2)"}},
            {"case_normalization": {"columns": ["country"], "case": "upper"}},
            {"fill_nulls": {"columns": ["notes"], "value": "N/A"}},
            {"drop_duplicates": ["col1", "col2"]},
            {"deduplicate_by_latest": {"key_columns": ["customer_id"], "timestamp_column": "updated_at"}},
            {"trim_strings": ["*"]},
            {"trim_strings": ["col1", "col2"]},  # Trim specific columns
            {"derive_column": {"name": "full_name", "expression": "concat(first_name, ' ', last_name)"}},
            {"pivot": {"id_vars": ["time_bucket", "internal_symbol"], "pivot_cols": ["timeframe"], "value_cols": ["close", "open"], "allow_eager_collect": "true"}}

            - pivot:
                id_vars: ["time_bucket", "internal_symbol"] # Columns to keep as identifier (keys)
                pivot_cols: ["timeframe"]                  # Column whose unique values become the new columns
                value_cols: ["close", "open"]              # Columns whose values will fill the new columns
                allow_eager_collect: true                  # Collect the result immediately, explicitly allows the expensive eager operation

        ]

        # --- DERIVED COLUMN EXAMPLES (derive_column) ---
        # NOTE: The 'expression' must be syntactically correct for the chosen engine (Polars or PySpark).

        # Use Case: Concatenate two columns with a space (Polars)
        # This requires native Polars expressions (pl.col and pl.concat_str).
        {
            "derive_column": {
                "name": "internal_symbol_timeframe",
                "expression": "pl.concat_str([pl.col('internal_symbol'), pl.lit(' '), pl.col('timeframe')])"
            }
        }

        # Use Case: Calculate Gross Profit/Loss (Arithmetic)
        # Works for both Polars (via eval) and PySpark (via F.expr).
        {
            "derive_column": {
                "name": "gross_pnl",
                "expression": "pl.col('close') * pl.col('size')"
            }
        }
        # --- PySpark equivalent (requires F.expr in PySpark implementation) ---
        # {"derive_column": {"name": "gross_pnl", "expression": "close * size"}}


        # Use Case: Conditional Flagging (Type 2: Conditional Logic)
        # Polars uses pl.when().then().otherwise()
        {
            "derive_column": {
                "name": "price_tier",
                "expression": "pl.when(pl.col('open') > 100).then(pl.lit('HIGH')).otherwise(pl.lit('LOW'))"
            }
        }
        # --- PySpark equivalent (requires F.expr in PySpark implementation) ---
        # {"derive_column": {"name": "price_tier", "expression": "CASE WHEN open > 100 THEN 'HIGH' ELSE 'LOW' END"}}


        # Use Case: Date/Time Component Extraction
        # Extracts the hour component from a timestamp column.
        {
            "derive_column": {
                "name": "trade_hour",
                "expression": "pl.col('timestamp_utc').dt.hour()"
            }
        }
        # --- PySpark equivalent (requires F.expr in PySpark implementation) ---
        # {"derive_column": {"name": "trade_hour", "expression": "hour(timestamp_utc)"}}


        # Use Case: Literal Value Assignment (Lineage/Audit)
        # Assigns a constant value to a new column.
        {
            "derive_column": {
                "name": "source_version",
                "expression": "pl.lit('v1.0_2025_Q4')"
            }
        }

        # Apply the transformations to a DataFrame
        transformed_df = apply_transformations(my_spark_df, trans_config, engine="pyspark")

    ToDo:

    """

    if engine.lower() == "polars":
        # Polars: Logic is self-contained and operates on PolarsFrame
        if not isinstance(df, (pl.DataFrame, pl.LazyFrame)):
            logger.error("Input 'df' is not a PolarsFrame. Cannot run Polars engine.")
            raise TypeError(
                "Input DataFrame must be a Polars DataFrame/LazyFrame for 'polars' engine."
            )

        return _apply_transformations_polars(df, transformations)

    elif engine.lower() == "pyspark":
        # PySpark: Requires conditional import of Spark functions
        return _apply_transformations_pyspark(df, transformations)

    else:
        raise ValueError(
            f"Unknown transformation engine: {engine}. Must be 'polars' or 'pyspark'."
        )
