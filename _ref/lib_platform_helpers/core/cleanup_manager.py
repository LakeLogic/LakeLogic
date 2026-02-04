from typing import Optional, Any
from loguru import logger
import os
import shutil

# --- 1. Polars/Deltalake Engine Implementation (Local File System) ---


def _delete_tables_polars(
    base_path: str, table_name: Optional[str], system_prefix: str, dry_run: bool
) -> None:
    """
    (Internal Polars/Deltalake Logic) Deletes Delta tables or directories from the
    local filesystem that match a specific name or system prefix.

    This is used for local development cleanup when tables are stored as directories
    (e.g., './.local-lakehouse/silver/table_name').

    Args:
        base_path (str): The local filesystem path to the schema layer (e.g., './.local-lakehouse/silver/').
        table_name (Optional[str]): A specific table directory name to delete.
        system_prefix (str): The prefix to match table directory names against (e.g., 'silver_market').
        dry_run (bool): If True, lists the tables that would be deleted without actually dropping them.
    """
    logger.info(f"Using Polars engine for local cleanup. Base path: {base_path}")

    if not os.path.exists(base_path):
        logger.warning(f"Local path does not exist: {base_path}. Skipping cleanup.")
        return

    # 1. Get directories (which represent Delta tables)
    all_table_dirs = [
        d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))
    ]

    # 2. Determine tables to delete
    if table_name:
        # Targeted deletion
        tables_to_delete = [table_name] if table_name in all_table_dirs else []
    else:
        # Prefix-based deletion
        tables_to_delete = [
            t for t in all_table_dirs if system_prefix.lower() in t.lower()
        ]

    if not tables_to_delete:
        logger.info(
            f"No local tables found in '{base_path}' matching prefix '{system_prefix}'."
        )
        return

    # 3. Delete (or dry run)
    for _table_name in tables_to_delete:
        full_table_path = os.path.join(base_path, _table_name)

        if dry_run:
            logger.warning(
                f"DRY RUN: Would recursively delete directory → {full_table_path}"
            )
        else:
            try:
                shutil.rmtree(full_table_path)
                logger.info(
                    f"Dropped local table directory (recursively deleted) → {full_table_path}"
                )
            except Exception as e:
                logger.error(f"Failed to delete local path {full_table_path}: {e}")


# --- 2. PySpark Engine Implementation (Cloud/Cluster) ---


def _delete_tables_pyspark(
    spark: Any,  # SparkSession
    catalog_name: str,
    schema_name: str,
    table_name: Optional[str],
    system_prefix: str,
    dry_run: bool,
) -> None:
    """
    (Internal PySpark Logic) Deletes Delta tables from the Unity Catalog/Hive Metastore
    using Spark SQL DDL statements (original function logic).
    """
    # Helper functions for logging (using loguru if available, else print)
    log_func = logger.info
    log_warn_func = logger.warning
    log_error_func = logger.error

    try:
        # 1. Get table list from schema
        df_tables = spark.sql(f"SHOW TABLES IN `{catalog_name}`.`{schema_name}`")
        all_table_names = [row["tableName"] for row in df_tables.collect()]

        # 2. Determine tables to delete
        if table_name:
            tables_to_delete = [table_name]
        else:
            tables_to_delete = [
                t for t in all_table_names if system_prefix.lower() in t.lower()
            ]

        if not tables_to_delete:
            log_func(
                f"No tables found in '{catalog_name}.{schema_name}' with prefix '{system_prefix}'."
            )
            return

        # 3. Delete (or dry run)
        for _table_name in tables_to_delete:
            full_table_name = f"`{catalog_name}`.`{schema_name}`.`{_table_name}`"
            if dry_run:
                log_warn_func(f"DRY RUN: Would drop table → {full_table_name}")
            else:
                try:
                    # Using IF EXISTS to avoid immediate failure if a table has just been deleted by another job
                    spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
                    log_func(f"Dropped table → {full_table_name}")
                except Exception as e:
                    # Log and continue if drop fails for any other reason
                    log_error_func(f"Failed to drop table {full_table_name}: {e}")
                    continue

    except Exception as e:
        log_error_func(f"An error occurred in delete_tables: {e}")
        # Reraise the exception for pipeline control flow
        raise


# --- 3. Main Dispatcher Function ---


def delete_tables(
    engine: str = "polars",
    base_lakehouse_path: Optional[str] = None,
    spark: Any = "",
    catalog_name: Optional[str] = None,
    schema_name: Optional[str] = None,
    table_name: Optional[str] = None,
    system_prefix: Optional[str] = None,
    dry_run: bool = False,
) -> None:
    """
    Deletes one or more Delta tables, dispatching the operation based on the engine.

    This function supports both cloud-based deletion (PySpark) and local file system
    cleanup (Polars).

    Args:
        engine (str): The processing engine to use. Defaults to 'polars' for local development.
        base_lakehouse_path (Optional[str]): Root directory for local Polars deletion
            (e.g., './.local-lakehouse'). Required if engine='polars'.
        spark (Optional[SparkSession]): The active Spark session. Required if engine='pyspark'.
        catalog_name (Optional[str]): The name of the catalog (required if engine='pyspark').
        schema_name (Optional[str]): The name of the schema/layer (e.g., '_bronze').
        table_name (Optional[str]): A specific table name/directory to delete. Takes priority over prefix.
        system_prefix (Optional[str]): The prefix to match table names against (e.g., 'silver_market').
        dry_run (bool): If True, lists the tables/directories that would be deleted.

    Raises:
        ValueError: If essential parameters are missing for the selected engine.

    ---

    **💡 Usage Example (Local Development - Polars)**

    ```python
    # Ensure LAKEHOUSE_ROOT_PATH is set in your .env
    LOCAL_ROOT = os.getenv("LAKEHOUSE_ROOT_PATH", "./.local-lakehouse")

    delete_tables(
        engine="polars",
        base_lakehouse_path=LOCAL_ROOT,
        schema_name="silver", # Targets the .local-lakehouse/silver/ directory
        system_prefix="market_ohlcv",
        dry_run=True
    )
    # Output: DRY RUN: Would recursively delete directory → ./.local-lakehouse/silver/market_ohlcv_master
    ```

    **💡 Usage Example (Production - PySpark)**

    ```python
    # Assume spark is an active SparkSession and logger is imported from loguru

    delete_tables(
        engine="pyspark",
        spark=spark,
        catalog_name="trade_pilot_prod",
        schema_name="_bronze",
        table_name="bronze_ib_account_logs",
        system_prefix=None, # Not used when table_name is specified
        dry_run=False
    )
    # Output: Dropped table → `trade_pilot_prod`.`_bronze`.`bronze_ib_account_logs`
    ```
    """

    if system_prefix is None and table_name is None:
        logger.warning(
            "Neither table_name nor system_prefix was provided. No action taken."
        )
        return

    if engine.lower() == "polars":
        if not base_lakehouse_path or not schema_name:
            raise ValueError(
                "For engine='polars', 'base_lakehouse_path' and 'schema_name' are required."
            )

        # Construct the full local path to the schema layer (e.g., ./.local-lakehouse/silver/)
        full_schema_path = os.path.join(base_lakehouse_path, schema_name)

        return _delete_tables_polars(
            base_path=full_schema_path,
            table_name=table_name,
            system_prefix=system_prefix,
            dry_run=dry_run,
        )

    elif engine.lower() == "pyspark":
        if not spark or not catalog_name or not schema_name:
            raise ValueError(
                "For engine='pyspark', 'spark', 'catalog_name', and 'schema_name' are required."
            )

        return _delete_tables_pyspark(
            spark=spark,
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            system_prefix=system_prefix,
            dry_run=dry_run,
        )

    else:
        raise ValueError(f"Invalid engine '{engine}'. Must be 'polars' or 'pyspark'.")
