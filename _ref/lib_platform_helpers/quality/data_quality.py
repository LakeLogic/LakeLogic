from typing import List, Dict, Any, Tuple, Optional, Union
from loguru import logger

import polars as pl

# Conditional Imports/Type Hints
SparkFrame = Any
PolarsFrame = Union[pl.DataFrame, pl.LazyFrame]
BooleanType = Any  # PySpark type hint, handled internally


# -------------------------------------------------------------------------
# Helper – map each check to its DQ category
# -------------------------------------------------------------------------
def _get_dimension(check_name: str) -> str:
    # (The original get_dimension helper remains here, unchanged)
    categories = {
        "correctness": [
            "regex",
            "min_value",
            "range_check",
            "in_set",
            "cross_column_validation",
            "referential_integrity",
        ],
        "completeness": ["min_row_count", "not_null", "staleness_check"],
        "consistency": [
            "primary_key_uniqueness",
            "cross_column_validation",
            "referential_integrity",
        ],
    }
    for dim, checks in categories.items():
        if check_name in checks:
            return dim
    return "uncategorized"

def run_quality_checks(
    df: Union[PolarsFrame, SparkFrame],
    quality_checks: List[Dict[str, Any]],
    engine: str = "polars",
    spark: Optional[Any] = None,  # SparkSession
    schema: Optional[Dict[str, Any]] = None,
) -> Tuple[Union[PolarsFrame, SparkFrame], Union[PolarsFrame, SparkFrame]]:
    """
    Applies a series of data quality checks to a Spark DataFrame and splits it
        into 'passed' and 'failed' (quarantined) datasets.

        it categorizes failures by
        Correctness, Completeness, and Consistency.

        Args:
            spark (SparkSession): The active Spark session, needed for certain checks.
            df (DataFrame): The input Spark DataFrame to be checked.
            quality_checks (List[Dict[str, Any]]): A list of dictionaries, where
                each dictionary defines a single quality check.
            schema (Optional[Dict[str, Any]]): The schema definition for the dataset,
                used for checks that require schema context (e.g., not_null wildcard).

        Returns:
            Tuple[DataFrame, DataFrame]: A tuple containing two DataFrames:
                - The first DataFrame contains rows that passed all quality checks.
                - The second DataFrame contains rows that failed one or more checks,
                along with a 'quality_failures' column detailing the errors.

        Example:
            # Define a comprehensive list of quality checks for a transactions table
            dq_config = [
                # DataFrame-level checks (fail the whole job if they don't pass)
                {"min_row_count": 100},
                {"primary_key_uniqueness": ["transaction_id"]},
                {"staleness_check": {"column": "transaction_date", "max_days_old": 2}},

                # Row-level checks (quarantine rows that fail)
                {"not_null": ["transaction_id", "customer_id", "amount"]},
                {"regex": {"column": "product_code", "pattern": "^[A-Z]{2}-[0-9]{4}$"}},
                {"min_value": {"column": "amount", "value": 0}},
                {"range_check": {"column": "quantity", "min_value": 1, "max_value": 999}},
                {"in_set": {"column": "currency", "values": ["USD", "GBP", "EUR"]}},
                {"cross_column_validation": {"condition": "shipping_date >= order_date"}},
                {"referential_integrity": {"column": "customer_id", "reference_table": "silver.customers"}}

            ]

            # Run the quality checks
            passed_df, failed_df = run_quality_checks(spark, my_spark_df, dq_config)
    """

    if engine.lower() == "polars":
        if not isinstance(df, (pl.DataFrame, pl.LazyFrame)):
            raise TypeError("Input 'df' must be a PolarsFrame for 'polars' engine.")
        return _run_quality_checks_polars(df, quality_checks, schema)

    elif engine.lower() == "pyspark":
        # Conditional PySpark Imports for execution
        try:
            from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
        except ImportError:
            raise ImportError(
                "Cannot use engine='pyspark'. 'pyspark' library is missing."
            )

        if not spark or not isinstance(spark, SparkSession):
            raise ValueError(
                "A valid SparkSession must be provided when engine='pyspark'."
            )

        # Call the PySpark implementation (provided logic)
        return _run_quality_checks_pyspark(spark, df, quality_checks, schema)

    else:
        raise ValueError(
            f"Unknown quality check engine: {engine}. Must be 'polars' or 'pyspark'."
        )


# -------------------------------------------------------------------------
# --- POLARS IMPLEMENTATION (The New Logic) ---
# -------------------------------------------------------------------------


# Helper to replicate Spark's error accumulation for Polars
def _add_polars_error(
    df_input: PolarsFrame, condition: pl.Expr, error_msg: str, dimension: str
) -> PolarsFrame:
    """Applies a conditional append of an error message/category to the failure arrays in Polars."""

    # Error Message Append: [existing_errors] + [new_error_msg]
    # We rely on list.concat(pl.lit([value])) for appending to array columns lazily.
    error_append_expr = pl.col("quality_failures").list.concat(pl.lit([error_msg]))

    # Category Append: [existing_categories] + [new_dimension]
    category_append_expr = pl.col("quality_failure_categories").list.concat(
        pl.lit([dimension])
    )

    return df_input.with_columns(
        pl.when(condition)
        .then(error_append_expr)
        .otherwise(pl.col("quality_failures"))
        .alias("quality_failures"),
        pl.when(condition)
        .then(category_append_expr)
        .otherwise(pl.col("quality_failure_categories"))
        .alias("quality_failure_categories"),
    )


def _run_quality_checks_polars(
    df: PolarsFrame,
    quality_checks: List[Dict[str, Any]],
    schema: Optional[Dict[str, Any]] = None,
) -> Tuple[PolarsFrame, PolarsFrame]:
    # 1. Setup Initial Failure Arrays
    df_with_failures = df.with_columns(
        [
            pl.lit([]).cast(pl.List(pl.Utf8)).alias("quality_failures"),
            pl.lit([]).cast(pl.List(pl.Utf8)).alias("quality_failure_categories"),
        ]
    )

    # Filter out DataFrame-level checks (must be run externally if EAGER)
    row_level_checks = [
        qc
        for qc in quality_checks
        if not any(
            k in qc
            for k in ["min_row_count", "staleness_check", "primary_key_uniqueness"]
        )
    ]

    # 2. Execution of Row-Level Checks (Lazy)
    for check in row_level_checks:
        check_name = next(iter(check))
        check_config = check[check_name]
        dimension = _get_dimension(check_name)

        if check_name == "not_null":
            columns_to_check = check_config
            if columns_to_check == ["*"]:
                if (
                    not schema
                    or "model" not in schema
                    or "columns" not in schema["model"]
                ):
                    raise ValueError("'not_null: [\"*\"]' requires schema info.")
                columns_to_check = [
                    c["name"]
                    for c in schema["model"]["columns"]
                    if not c.get("nullable", True)
                ]

            for col_name in columns_to_check:
                condition = pl.col(col_name).is_null()
                error_msg = f"Column '{col_name}' is null."
                df_with_failures = _add_polars_error(
                    df_with_failures, condition, error_msg, dimension
                )

        # 3. REGEX Check (Polars equivalent to F.rlike)
        elif check_name == "regex":
            col_name = check_config["column"]
            pattern = check_config["pattern"]
            # Condition: is NOT matched by the regex pattern
            condition = ~pl.col(col_name).str.contains(pattern)
            error_msg = f"Column '{col_name}' fails regex {pattern}."
            df_with_failures = _add_polars_error(
                df_with_failures, condition, error_msg, dimension
            )

        # 4. MIN_VALUE / RANGE_CHECK / IN_SET / CROSS_COLUMN_VALIDATION
        # (Logic mirrors PySpark, using Polars expressions)

        elif check_name == "min_value":
            col_name = check_config["column"]
            min_val = check_config["value"]
            condition = pl.col(col_name) < min_val
            error_msg = f"Column '{col_name}' below min {min_val}."
            df_with_failures = _add_polars_error(
                df_with_failures, condition, error_msg, dimension
            )

        elif check_name == "range_check":
            col_name = check_config["column"]
            min_val = check_config.get("min_value")
            max_val = check_config.get("max_value")

            conds = []
            if min_val is not None:
                conds.append(pl.col(col_name) < min_val)
            if max_val is not None:
                conds.append(pl.col(col_name) > max_val)

            if conds:
                condition = conds[0]
                for c in conds[1:]:
                    condition = condition | c  # Polars OR is |

                error_msg = f"Column '{col_name}' out of range ({min_val}-{max_val})."
                df_with_failures = _add_polars_error(
                    df_with_failures, condition, error_msg, dimension
                )

        elif check_name == "in_set":
            col_name = check_config["column"]
            allowed_values = check_config["values"]
            condition = ~pl.col(col_name).is_in(allowed_values)
            error_msg = f"'{col_name}' not in allowed set."
            df_with_failures = _add_polars_error(
                df_with_failures, condition, error_msg, dimension
            )

        elif check_name == "cross_column_validation":
            condition_str = check_config["condition"]
            # Violation is the NOT of the condition (e.g., ~(pl.col('A') > pl.col('B')))
            condition = ~eval(condition_str, {"pl": pl})
            error_msg = f"Cross-column validation failed: {condition_str}."
            df_with_failures = _add_polars_error(
                df_with_failures, condition, error_msg, dimension
            )

        # NOTE: Referential Integrity check is omitted for the Polars wrapper due to complexity/eagerness requirements.

    # 3. Split Passed vs Failed (Lazy Output)

    # Check if the failure array is non-empty
    has_failed = pl.col("quality_failures").list.len() > 0

    # Passed: Filter where the failure array is empty. Drop audit columns.
    df_passed = df_with_failures.filter(~has_failed).drop(
        "quality_failures", "quality_failure_categories"
    )

    # Failed (Quarantine): Filter where the failure array is NOT empty.
    df_failed = (
        df_with_failures.filter(has_failed)
        .with_columns(
            [
                # Add quarantine status columns (required by the system)
                pl.lit("active").alias("quarantine_state"),
                pl.lit(False).alias("quarantine_reprocessed").cast(pl.Boolean),
                pl.lit(None)
                .alias("quarantine_reprocessing_time")
                .cast(pl.Datetime)
                .dt.replace_time_zone("UTC"),
            ]
        )
    )

    return df_passed, df_failed


def _run_quality_checks_pyspark(
    spark: Any,  # SparkSession
    df: SparkFrame,  # Spark DataFrame
    quality_checks: List[Dict[str, Any]],
    schema: Optional[Dict[str, Any]] = None,
) -> Tuple[SparkFrame, SparkFrame]:
    """
    Private function containing the original PySpark logic for data quality checks.
    """
    # Conditional PySpark Imports (Local to this function)
    try:
        from pyspark.sql import DataFrame, functions as F
        from pyspark.sql.types import BooleanType
        from pyspark.sql.window import Window
        from pyspark.sql.functions import (
            array_union,
            array,
            lit,
            when,
            size,
            col,
            isNull,
            isNotNull,
            expr,
        )
    except ImportError:
        # Should be caught by the dispatcher, but here for robustness
        raise ImportError("PySpark libraries needed for execution are missing.")

    # -------------------------------------------------------------------------
    # Basic setup
    # -------------------------------------------------------------------------
    if not quality_checks:
        logger.info("No quality checks provided.")
        return df, df.sparkSession.createDataFrame([], schema=df.schema)

    df_with_failures = df.withColumn("quality_failures", F.array()).withColumn(
        "quality_failure_categories", F.array()
    )

    # Separate DataFrame-level vs Row-level checks
    df_level_checks = [
        qc
        for qc in quality_checks
        if any(
            k in qc
            for k in ["min_row_count", "staleness_check", "primary_key_uniqueness"]
        )
    ]
    row_level_checks = [
        qc
        for qc in quality_checks
        if not any(
            k in qc
            for k in ["min_row_count", "staleness_check", "primary_key_uniqueness"]
        )
    ]

    # -------------------------------------------------------------------------
    # DataFrame-level checks
    # -------------------------------------------------------------------------
    df_level_errors = []

    for check in df_level_checks:
        check_name = next(iter(check))
        check_config = check[check_name]
        dimension = _get_dimension(check_name)
        logger.info(
            f"[{dimension.upper()}] Running DataFrame-level check: {check_name}"
        )

        try:
            if check_name == "min_row_count":
                min_rows = check_config
                actual_rows = df.count()
                if actual_rows < min_rows:
                    df_level_errors.append(
                        f"Minimum row count not met. Expected ≥ {min_rows}, found {actual_rows}."
                    )

            elif check_name == "staleness_check":
                col_name = check_config["column"]
                max_days = check_config["max_days_old"]
                max_date = df.select(F.max(F.col(col_name))).first()[0]
                if max_date:
                    staleness = (F.current_date() - F.to_date(F.lit(max_date))).cast(
                        "int"
                    )
                    if staleness > max_days:
                        df_level_errors.append(
                            f"Data staleness exceeded in '{col_name}'. {staleness} days old, limit {max_days}."
                        )

            elif check_name == "primary_key_uniqueness":
                key_columns = check_config
                dup_count = (
                    df.groupBy(*key_columns).count().where(F.col("count") > 1).count()
                )
                if dup_count > 0:
                    df_level_errors.append(
                        f"Primary key uniqueness violated on {key_columns}: {dup_count} duplicates."
                    )

        except Exception as e:
            df_level_errors.append(f"{check_name} failed to execute: {e}")

    if df_level_errors:
        summary = "\n".join(df_level_errors)
        raise ValueError(f"❌ DataFrame-level quality checks failed:\n{summary}")

    # -------------------------------------------------------------------------
    # Row-level checks
    # -------------------------------------------------------------------------
    for check in row_level_checks:
        check_name = next(iter(check))
        check_config = check[check_name]
        dimension = _get_dimension(check_name)
        logger.info(f"[{dimension.upper()}] Running Row-level check: {check_name}")

        if check_name == "not_null":
            columns_to_check = check_config
            if columns_to_check == ["*"]:
                if (
                    not schema
                    or "model" not in schema
                    or "columns" not in schema["model"]
                ):
                    raise ValueError("'not_null: [\"*\"]' requires schema info.")
                columns_to_check = [
                    c["name"]
                    for c in schema["model"]["columns"]
                    if not c.get("nullable", True)
                ]

            for col_name in columns_to_check:
                condition = F.col(col_name).isNull()
                error_msg = F.lit(f"Column '{col_name}' is null.")
                category_msg = F.lit(dimension)

                df_with_failures = df_with_failures.withColumn(
                    "quality_failures",
                    F.when(
                        condition,
                        F.array_union(F.col("quality_failures"), F.array(error_msg)),
                    ).otherwise(F.col("quality_failures")),
                ).withColumn(
                    "quality_failure_categories",
                    F.when(
                        condition,
                        F.array_union(
                            F.col("quality_failure_categories"), F.array(category_msg)
                        ),
                    ).otherwise(F.col("quality_failure_categories")),
                )

        elif check_name == "regex":
            col_name = check_config["column"]
            pattern = check_config["pattern"]
            condition = ~F.col(col_name).rlike(pattern)
            error_msg = F.lit(f"Column '{col_name}' fails regex {pattern}.")
            category_msg = F.lit(dimension)

            df_with_failures = df_with_failures.withColumn(
                "quality_failures",
                F.when(
                    condition,
                    F.array_union(F.col("quality_failures"), F.array(error_msg)),
                ).otherwise(F.col("quality_failures")),
            ).withColumn(
                "quality_failure_categories",
                F.when(
                    condition,
                    F.array_union(
                        F.col("quality_failure_categories"), F.array(category_msg)
                    ),
                ).otherwise(F.col("quality_failure_categories")),
            )

        elif check_name == "min_value":
            col_name = check_config["column"]
            min_val = check_config["value"]
            condition = F.col(col_name) < min_val
            error_msg = F.lit(f"Column '{col_name}' below min {min_val}.")
            category_msg = F.lit(dimension)

            df_with_failures = df_with_failures.withColumn(
                "quality_failures",
                F.when(
                    condition,
                    F.array_union(F.col("quality_failures"), F.array(error_msg)),
                ).otherwise(F.col("quality_failures")),
            ).withColumn(
                "quality_failure_categories",
                F.when(
                    condition,
                    F.array_union(
                        F.col("quality_failure_categories"), F.array(category_msg)
                    ),
                ).otherwise(F.col("quality_failure_categories")),
            )

        elif check_name == "range_check":
            col_name = check_config["column"]
            min_val = check_config.get("min_value")
            max_val = check_config.get("max_value")

            conds = []
            if min_val is not None:
                conds.append(F.col(col_name) < min_val)
            if max_val is not None:
                conds.append(F.col(col_name) > max_val)

            if conds:
                condition = conds[0]
                for c in conds[1:]:
                    condition = condition | c

                error_msg = F.lit(
                    f"Column '{col_name}' out of range ({min_val}-{max_val})."
                )
                category_msg = F.lit(dimension)

                df_with_failures = df_with_failures.withColumn(
                    "quality_failures",
                    F.when(
                        condition,
                        F.array_union(F.col("quality_failures"), F.array(error_msg)),
                    ).otherwise(F.col("quality_failures")),
                ).withColumn(
                    "quality_failure_categories",
                    F.when(
                        condition,
                        F.array_union(
                            F.col("quality_failure_categories"), F.array(category_msg)
                        ),
                    ).otherwise(F.col("quality_failure_categories")),
                )

        elif check_name == "in_set":
            col_name = check_config["column"]
            allowed_values = check_config["values"]
            condition = ~F.col(col_name).isin(allowed_values)
            error_msg = F.lit(f"'{col_name}' not in allowed set {allowed_values}.")
            category_msg = F.lit(dimension)

            df_with_failures = df_with_failures.withColumn(
                "quality_failures",
                F.when(
                    condition,
                    F.array_union(F.col("quality_failures"), F.array(error_msg)),
                ).otherwise(F.col("quality_failures")),
            ).withColumn(
                "quality_failure_categories",
                F.when(
                    condition,
                    F.array_union(
                        F.col("quality_failure_categories"), F.array(category_msg)
                    ),
                ).otherwise(F.col("quality_failure_categories")),
            )

        elif check_name == "cross_column_validation":
            condition_str = check_config["condition"]
            condition = ~F.expr(condition_str)
            error_msg = F.lit(f"Cross-column validation failed: {condition_str}.")
            category_msg = F.lit(dimension)

            df_with_failures = df_with_failures.withColumn(
                "quality_failures",
                F.when(
                    condition,
                    F.array_union(F.col("quality_failures"), F.array(error_msg)),
                ).otherwise(F.col("quality_failures")),
            ).withColumn(
                "quality_failure_categories",
                F.when(
                    condition,
                    F.array_union(
                        F.col("quality_failure_categories"), F.array(category_msg)
                    ),
                ).otherwise(F.col("quality_failure_categories")),
            )

        elif check_name == "referential_integrity":
            col_name = check_config["column"]
            ref_table = check_config["reference_table"]
            try:
                ref_df = (
                    spark.table(ref_table)
                    .select(F.col(col_name).alias("ref_col"))
                    .distinct()
                )
                failed_keys_df = df.select(col_name).join(
                    ref_df, F.col(col_name) == F.col("ref_col"), "left_anti"
                )

                df_with_failures = (
                    df_with_failures.alias("main")
                    .join(
                        failed_keys_df.alias("failed"),
                        F.col(f"main.{col_name}") == F.col(f"failed.{col_name}"),
                        "left",
                    )
                    .select(
                        F.col("main.*"),
                        F.when(
                            F.col(f"failed.{col_name}").isNotNull(),
                            F.array_union(
                                F.col("main.quality_failures"),
                                F.array(
                                    F.lit(
                                        f"Referential integrity failed for '{col_name}'."
                                    )
                                ),
                            ),
                        )
                        .otherwise(F.col("main.quality_failures"))
                        .alias("quality_failures"),
                        F.when(
                            F.col(f"failed.{col_name}").isNotNull(),
                            F.array_union(
                                F.col("main.quality_failure_categories"),
                                F.array(F.lit(dimension)),
                            ),
                        )
                        .otherwise(F.col("main.quality_failure_categories"))
                        .alias("quality_failure_categories"),
                    )
                )
            except Exception as e:
                df_with_failures = df_with_failures.withColumn(
                    "quality_failures",
                    F.array_union(
                        F.col("quality_failures"),
                        F.array(F.lit(f"Referential integrity check error: {e}")),
                    ),
                )
                df_with_failures = df_with_failures.withColumn(
                    "quality_failure_categories",
                    F.array_union(
                        F.col("quality_failure_categories"), F.array(F.lit(dimension))
                    ),
                )
        else:
            logger.warning(f"Skipping unknown check: {check_name}")

    # -------------------------------------------------------------------------
    # Split Passed vs Failed
    # -------------------------------------------------------------------------
    failed_df = df_with_failures.filter(F.size(F.col("quality_failures")) > 0)

    failed_df = failed_df.withColumn("quarantine_state", F.lit("active"))
    failed_df = failed_df.withColumn(
        "quarantine_reprocessed", F.lit(False).cast(BooleanType())
    )
    failed_df = failed_df.withColumn(
        "quarantine_reprocessing_time", F.lit(None).cast("timestamp")
    )

    passed_df = df_with_failures.filter(F.size(F.col("quality_failures")) == 0).drop(
        "quality_failures", "quality_failure_categories"
    )

    return passed_df, failed_df
