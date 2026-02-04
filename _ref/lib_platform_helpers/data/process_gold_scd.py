from delta.tables import DeltaTable
from pyspark.sql import functions as F
from loguru import logger


def process_gold_scd(
    spark,
    ds_gold: dict,
    ds_table_name_silver: str,
    ds_table_name_gold: str,
    primary_key_list: list,
):
    """
    Performs a Slowly Changing Dimension (SCD) Type 2 merge
    from a Silver Delta table into a Gold Delta table.

    This function identifies changed records based on the configured
    comparison columns and:
      - Closes out old (previous) records by setting an expiration date.
      - Inserts new versions of records with a default effective date of 1900-01-01.
      - Marks current active records with `is_current = True`.

    Args:
        ds_gold (dict): Gold dataset configuration containing the SCD metadata.
                        Example:
                        {
                            "slowly_changing_dimension": {
                                "type": "type2",
                                "compare_columns": ["status", "aircraft_no"],
                                "effective_date_column": "effective_date",
                                "expiration_date_column": "expiration_date",
                                "current_flag_column": "is_current"
                            }
                        }
        ds_table_name_silver (str): Full table name for the Silver source (e.g., "jetblue_dev_engines._silver.silver_engine_status").
        ds_table_name_gold (str): Full table name for the Gold target (e.g., "jetblue_dev_engines._gold.gold_engine_status").
        primary_key_list (list): Columns used as business keys (e.g., ["comp_serialnumber"]).

    Example:
        ```python
        # Define dataset configuration for SCD
        ds_gold_conf = {
            "slowly_changing_dimension": {
                "type": "type2",
                "compare_columns": ["status", "engine_variant"],
                "effective_date_column": "effective_date",
                "expiration_date_column": "expiration_date",
                "current_flag_column": "is_current"
            }
        }

        # Execute SCD merge from Silver to Gold
        process_gold_scd(
            ds_gold=ds_gold_conf,
            ds_table_name_silver="jetblue_dev_engines._silver.silver_engines_engine_status",
            ds_table_name_gold="jetblue_dev_engines._gold.gold_engine_status",
            primary_key_list=["comp_serialnumber"]
        )
        ```

    Behavior:
        ✅ New records → inserted with effective_date = 1900-01-01, is_current = True
        ✅ Changed records → old version expired with expiration_date = current_date
        ✅ Unchanged records → remain untouched
        ✅ Ensures SCD2 consistency between Silver and Gold tables
    """

    # Extract source and target table names
    src_table = ds_table_name_silver
    tgt_table = ds_table_name_gold
    key_cols = primary_key_list

    # Extract SCD configuration
    scd_conf = ds_gold["slowly_changing_dimension"]
    compare_cols = scd_conf["compare_columns"]
    eff_col = scd_conf["effective_date_column"]
    exp_col = scd_conf["expiration_date_column"]
    curr_col = scd_conf["current_flag_column"]

    logger.info("🔄 Starting SCD Type 2 merge")  # {src_table} → {tgt_table}

    # Build staged data (Silver → Gold candidate)
    staged_df = (
        spark.table(src_table)
        .withColumn(eff_col, F.lit("1900-01-01").cast("date"))  # Default effective date
        .withColumn(exp_col, F.lit(None).cast("date"))  # Expiration date initially NULL
        .withColumn(curr_col, F.lit(True))  # Mark as current
    )

    # Load the Gold Delta table
    delta_tgt = DeltaTable.forName(spark, tgt_table)

    # Build merge conditions
    merge_condition = " AND ".join([f"src.{c} = tgt.{c}" for c in key_cols])
    change_condition = " OR ".join([f"src.{c} <> tgt.{c}" for c in compare_cols])

    # Perform the SCD Type 2 merge
    (
        delta_tgt.alias("tgt")
        .merge(source=staged_df.alias("src"), condition=merge_condition)
        # 1️⃣ Update — when a record has changed
        .whenMatchedUpdate(
            condition=change_condition,
            set={exp_col: F.current_date(), curr_col: F.lit(False)},
        )
        # 2️⃣ Insert — for new records
        .whenNotMatchedInsert(
            values={
                **{c: F.col(f"src.{c}") for c in staged_df.columns},
                eff_col: F.lit("1900-01-01").cast("date"),
                exp_col: F.lit(None).cast("date"),
                curr_col: F.lit(True),
            }
        )
        .execute()
    )

    logger.info("✅ SCD Type 2 merge completed successfully")
