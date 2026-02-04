# File: src/etl/lib_platform_helpers/core/ingestion_manager.py

import polars as pl
from typing import Dict, Any, Union, Optional
from loguru import logger
import os
from datetime import datetime
import glob

# Define the acceptable DataFrame types
PolarsFrame = Union[pl.DataFrame, pl.LazyFrame]
SparkFrame = Any  # Use Any for PySpark DataFrame type hinting

# Define the universal accepted file formats
ACCEPTED_FORMATS = ["csv", "json", "parquet", "delta", "xml"]


def ingest_raw_files(
    file_path: str,
    file_format: str,
    engine: str = "polars",
    spark: Optional[Any] = None,
    options: Optional[Dict[str, Any]] = None,
) -> Union[PolarsFrame, SparkFrame]:
    """
    Ingests raw data from a file path/URI, dynamically applying audit metadata.

    Args:
        file_path (str): The full path or URI to the file(s) (supports wildcards/directories).
        file_format (str): The file format (csv, json, parquet, delta, xml).
        engine (str): The execution engine ('polars' or 'pyspark'). Defaults to 'polars'.
        spark (Optional[Any]): The active Spark session (required if engine='pyspark').
        options (Optional[Dict[str, Any]]): Format-specific read options (e.g., 'header', 'sep').

    Returns:
        Union[PolarsFrame, SparkFrame]: The DataFrame with audit columns added.

    Raises:
        ValueError: If an unsupported format or engine is specified.
        ImportError: If the 'pyspark' engine is requested but the library is missing.

    💡 Usage Example (Polars):
    ```python
    # Load all CSV files in a directory
    df_raw = ingest_raw_files(
        file_path='./raw_zone/my_events/*.csv',
        file_format='csv',
        engine='polars',
        options={'has_header': True, 'separator': ','}
    )
    # Each row will have correct dp_source_file_name for its source file
    ```
    """
    file_format = file_format.lower()
    if file_format not in ACCEPTED_FORMATS:
        raise ValueError(
            f"Unsupported file format: {file_format}. Allowed: {ACCEPTED_FORMATS}"
        )

    if engine.lower() == "polars":
        return _ingest_raw_files_polars(file_path, file_format, options or {})

    elif engine.lower() == "pyspark":
        if not spark:
            raise ValueError(
                "A SparkSession object must be provided when engine='pyspark'."
            )
        return _ingest_raw_files_pyspark(spark, file_path, file_format, options or {})

    else:
        raise ValueError(
            f"Unknown ingestion engine: {engine}. Must be 'polars' or 'pyspark'."
        )


# --- 2. POLARS IMPLEMENTATION ---


def _ingest_raw_files_polars(
    file_path: str, file_format: str, options: Dict[str, Any]
) -> PolarsFrame:
    """
    Polars-specific ingestion logic using LazyFrame API, adding all required
    audit metadata including source and ingestion timestamps.
    """

    ingestion_time = datetime.now()

    # 1. Read the data lazily with file name tracking
    if file_format == "csv":
        # Use scan_csv with batched for better file tracking
        df_raw = pl.scan_csv(file_path, **options)
    elif file_format == "json":
        df_raw = pl.scan_ndjson(file_path, **options)
    elif file_format == "parquet":
        df_raw = pl.scan_parquet(file_path, **options)
    elif file_format == "delta":
        df_raw = pl.scan_delta(file_path, **options)
    elif file_format == "xml":
        logger.warning("XML requires collecting data first, compromising lazy loading.")
        # For XML, we need to handle multiple files differently
        if "*" in file_path or os.path.isdir(file_path.rstrip("*")):
            # Read all XML files and concatenate
            xml_files = (
                glob.glob(file_path)
                if "*" in file_path
                else [
                    os.path.join(file_path, f)
                    for f in os.listdir(file_path)
                    if f.endswith(".xml")
                ]
            )
            dfs = []
            for xml_file in xml_files:
                df_single = pl.read_xml(xml_file, **options)
                # Add file metadata for each file
                df_single = df_single.with_columns(
                    [
                        pl.lit(ingestion_time)
                        .alias("dp_ingestion_timestamp")
                        .dt.replace_time_zone("UTC"),
                    ]
                )
                dfs.append(df_single)
            df_raw = pl.concat(dfs).lazy()
        else:
            # Single XML file
            df_raw = pl.read_xml(file_path, **options).lazy()
            df_raw = df_raw.with_columns(
                [
                    pl.lit(ingestion_time)
                    .alias("dp_ingestion_timestamp")
                    .dt.replace_time_zone("UTC"),
                ]
            )
    else:
        raise ValueError(f"Unsupported format for Polars: {file_format}")

    # 2. For formats that support it natively, add file metadata
    if file_format != "xml":  # XML already handled above
        # Get all matching files to understand the scope
        matching_files = glob.glob(file_path) if "*" in file_path else [file_path]

        if len(matching_files) == 1:
            # Single file - use simple literal approach
            df_raw = df_raw.with_columns(
                [
                    pl.lit(ingestion_time)
                    .alias("dp_ingestion_timestamp")
                    .dt.replace_time_zone("UTC"),
                ]
            )
        else:
            # Multiple files - we need a different approach for Polars
            # Since Polars doesn't have built-in file metadata like Spark,
            # we'll use a mapping strategy for formats that support it
            if file_format in ["parquet", "csv", "json"]:
                # For these formats, we can read with additional metadata
                try:
                    # Try to use input_file_name equivalent if available
                    if file_format == "parquet":
                        # For parquet, we can potentially use row_group information
                        df_raw = df_raw.with_columns(
                            [
                                pl.lit(ingestion_time)
                                .alias("dp_ingestion_timestamp")
                                .dt.replace_time_zone("UTC"),
                            ]
                        )
                        logger.warning(
                            "Polars multi-file ingestion: File-level metadata limited. Consider using PySpark for detailed file tracking."
                        )
                    else:
                        df_raw = df_raw.with_columns(
                            [
                                pl.lit(ingestion_time)
                                .alias("dp_ingestion_timestamp")
                                .dt.replace_time_zone("UTC"),
                            ]
                        )
                except Exception as e:
                    logger.warning(
                        f"Could not add detailed file metadata for multi-file read: {e}"
                    )
                    # Fallback to basic metadata
                    df_raw = df_raw.with_columns(
                        [
                            pl.lit(ingestion_time)
                            .alias("dp_ingestion_timestamp")
                            .dt.replace_time_zone("UTC"),
                        ]
                    )

        current_columns = df_raw.collect_schema().names()

        # Check if the empty string column exists
        if "" in current_columns:
            # Drop the column
            df_cleaned = df_raw.drop("")

            # Optional: Log the action for auditing
            logger.info("✅ Dropped unnamed column (CSV index) from DataFrame.")
        else:
            df_cleaned = df_raw

    return df_cleaned


# --- 3. PYSPARK IMPLEMENTATION ---


def _ingest_raw_files_pyspark(
    spark: Any, file_path: str, file_format: str, options: Dict[str, Any]
) -> SparkFrame:
    """PySpark-specific ingestion logic using SparkSession read API."""

    # Conditional PySpark Imports
    try:
        from pyspark.sql import functions as F
        from pyspark.sql import DataFrame as SparkDataFrame
    except ImportError:
        raise ImportError("Cannot use engine='pyspark'. 'pyspark' library is missing.")

    ingestion_time = datetime.now()

    # 1. Read the data using the generic format reader
    df_raw = spark.read.format(file_format).options(**options).load(file_path)

    # 2. Add all audit columns using Spark's _metadata functionality
    # Spark automatically provides file-level metadata for each row
    df_raw = df_raw.withColumn("dp_source_file_path", F.col("_metadata.file_path"))
    # df_raw = df_raw.withColumn("dp_source_file_name", F.col("_metadata.file_name"))
    # df_raw = df_raw.withColumn("dp_source_file_timestamp", F.col("_metadata.file_modification_time"))
    df_raw = df_raw.withColumn("dp_ingestion_timestamp", F.current_timestamp())

    return df_raw
