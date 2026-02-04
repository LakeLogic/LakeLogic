from delta.tables import DeltaTable
from pyspark.sql import functions as F


def merge_to_delta_table(
    spark,
    source_df,
    target_table_name: str,
    primary_key_list: list,
    timestamp_col: str = "dp_ingestion_timestamp",
    logger: object = None,
):
    """
    Performs an incremental upsert (MERGE) into a Delta table using Delta Lake.

    This function compares rows based on the given primary key(s) and
    updates target records only if the incoming row is newer (based on timestamp_col).
    New rows are inserted automatically.

    Args:
        spark (SparkSession): Active Spark session.
        source_df (DataFrame): The new or incremental data to merge.
        target_table_name (str): Full Delta table name (e.g., 'jetblue_dev_engines._silver.silver_engine_status').
        primary_key_list (list): List of columns to use for merge keys.
        timestamp_col (str): Column used to detect newer records (default: 'dp_ingestion_timestamp').
        logger (object, optional): Logger for structured logging. Defaults to `print` if None.

    Example:
        ```python
        # Example usage within a Databricks notebook or job

        from pyspark.sql import functions as F

        # Create a small test DataFrame (e.g., new data from Silver layer)
        data = [
            ("E1001", "ACTIVE", "2025-10-05 10:00:00"),
            ("E1002", "INACTIVE", "2025-10-06 08:00:00"),
        ]

        df_source = spark.createDataFrame(data, ["comp_serialnumber", "status", "dp_ingestion_timestamp"]) \
                         .withColumn("dp_ingestion_timestamp", F.col("dp_ingestion_timestamp").cast("timestamp"))

        # Merge into the Silver Delta table
        merge_to_delta_table(
            spark=spark,
            source_df=df_source,
            target_table_name="jetblue_dev_engines._silver.silver_engines_engine_status",
            primary_key_list=["comp_serialnumber"],
            timestamp_col="dp_ingestion_timestamp"
        )
        ```

    Behavior:
        ✅ Updates records in target if newer version exists in source  
        ✅ Inserts new records automatically  
        ✅ Skips older records (based on timestamp_col)  
        ✅ Logs progress and errors clearly
    """
    log = logger.info if logger else print
    log_err = logger.error if logger else print

    try:
        # Try to access the Delta table
        delta_table = DeltaTable.forName(spark, target_table_name)
        # log(f"✅ Target Delta table found: {target_table_name}")
    except Exception as e:
        raise Exception(f"❌ Unable to access target table '{target_table_name}': {e}")

    try:
        # Align columns between source and target
        target_cols = [field.name for field in delta_table.toDF().schema.fields]
        common_cols = [c for c in target_cols if c in source_df.columns]

        if not common_cols:
            raise Exception(
                f"No overlapping columns between source and target for table: {target_table_name}"
            )

        df_aligned = source_df.select([F.col(c) for c in common_cols])
        # log(f"ℹ️ Aligned {len(common_cols)} column(s) between source and target.")

        # Validate merge keys
        if not primary_key_list or any(not c for c in primary_key_list):
            raise ValueError(f"Invalid primary key list: {primary_key_list}")

        merge_condition = " AND ".join(
            [f"target.{c} = source.{c}" for c in primary_key_list]
        )

        # Perform Delta merge
        (
            delta_table.alias("target")
            .merge(df_aligned.alias("source"), merge_condition)
            .whenMatchedUpdateAll(
                condition=f"target.{timestamp_col} < source.{timestamp_col}"
            )
            .whenNotMatchedInsertAll()
            .execute()
        )

        log(f"✅ Merge completed successfully into {target_table_name}")

    except Exception as e:
        log_err(f"❌ Merge failed for table {target_table_name}: {e}")
        raise
