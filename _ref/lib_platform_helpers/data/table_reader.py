# File: src/etl/lib_platform_helpers/core/data_loading_manager.py


from typing import Dict, Any, Union, Optional, List, Tuple
from loguru import logger
from pyspark.sql import functions as F

import polars as pl

# Define the acceptable DataFrame types
PolarsFrame = Union[pl.DataFrame, pl.LazyFrame]
SparkFrame = Any
SparkSession = Any


def read_delta_table(
    target_uri: str,
    engine: str = "polars",
    spark: Optional[Any] = None,
    columns: Optional[List[str]] = None,  # NEW: Column Projection
    filter_condition: Optional[
        str
    ] = None,  # NEW: Row Predicate (SQL/Polars expression)
    row_limit: Optional[int] = None,  # NEW: Row Limit (head/sample)
    order_by_columns: Optional[
        List[Tuple[str, str]]
    ] = None,  # NEW: e.g., [('time_bucket', 'desc'), ('close', 'asc')]
    options: Optional[
        Dict[str, Any]
    ] = None,  # Existing Delta read options (e.g., versionAsOf)
) -> Union[PolarsFrame, SparkFrame]:
    """
    Reads a Delta Lake table, applying column projection, filtering, and row limits
    efficiently in the corresponding engine (Polars or PySpark).

    This is the primary function for reading data from the Silver and Gold layers.

    Args:
        target_uri (str): The path/name to the Delta table (local path or cloud URI/catalog name).
        engine (str): The execution engine ('polars' or 'pyspark'). Defaults to 'polars'.
        spark (Optional[Any]): The active Spark session (required if engine='pyspark').
        columns (Optional[List[str]]): List of columns to select (projection).
        filter_condition (Optional[str]): SQL/Polars expression string to filter rows (WHERE clause).
        row_limit (Optional[int]): Maximum number of rows to return (applied as .head() or .limit()).
        order_by_columns: Optional[List[Tuple[str, str]]] = None, # NEW: e.g., [('time_bucket', 'desc'), ('close', 'asc')].
        options (Optional[Dict[str, Any]]): Engine-specific read options (e.g., 'versionAsOf').

    Returns:
        Union[PolarsFrame, SparkFrame]: A DataFrame/LazyFrame in the format of the specified engine.

    Raises:
        ValueError: If an unknown engine is specified or PySpark context is missing.

    💡 Usage Example (Polars - Lazy Read with Filter & Projection):

    ```python
    # Assume LAKEHOUSE_ROOT_PATH is set and points to './.local-lakehouse'
    df_ohlcv = read_delta_table(
        target_uri='./.local-lakehouse/silver/market_ohlcv_master',
        engine='polars',
        columns=['internal_symbol', 'time_bucket', 'close'],
        filter_condition="(pl.col('close') > 100) & (pl.col('volume') > 500)",
        order_by_columns=[('time_bucket', 'desc'), ('volume', 'asc')],
        row_limit=100
    )
    # df_ohlcv is a Polars LazyFrame, ready for transformation.
    ```

    💡 Usage Example (PySpark - Filter by Date and Limit):

    ```python
    # Assume spark_session is active
    df_tickers = read_delta_table(
        target_uri='prod_catalog.reference.ticker_registry',
        engine='pyspark',
        spark=spark_session,
        columns=['internal_symbol', 'broker_api_name'],
        filter_condition="ib_exchange_code = 'GLOBEX'",
        row_limit=50
    )
    # df_tickers is a Spark DataFrame, ready for broadcast join or further processing.
    ```
    """
    options = options or {}

    if engine.lower() == "polars":
        df = _read_delta_table_polars(target_uri, options)
        df = _apply_delta_read_controls_polars(
            df, columns, filter_condition, order_by_columns, row_limit
        )
        return df

    elif engine.lower() == "pyspark":
        if not spark:
            raise ValueError(
                "A SparkSession object must be provided when engine='pyspark'."
            )
        df = _read_delta_table_pyspark(spark, target_uri, options)
        df = _apply_delta_read_controls_pyspark(
            df, columns, filter_condition, order_by_columns, row_limit
        )
        return df

    else:
        raise ValueError(
            f"Unknown read engine: {processing_engine}. Must be 'polars' or 'pyspark'."
        )


# ----------------------------------------------------------------------
# --- 1. ENGINE-SPECIFIC CONTROL APPLICATION ---
# ----------------------------------------------------------------------


def _apply_delta_read_controls_polars(
    df: PolarsFrame,
    columns: Optional[List[str]],
    filter_condition: Optional[str],
    order_by_columns: Optional[List[Tuple[str, str]]],
    row_limit: Optional[int],
) -> PolarsFrame:
    """Applies projection, filter, order, and limit to a Polars LazyFrame."""

    #  Column Projection (MUST be applied first in LazyFrames for efficiency)
    if columns:
        # Polars automatically optimizes column selection during scan
        df = df.select(columns)
        logger.info(f"Polars: Applied column projection for {len(columns)} columns.")

    #  Filter Condition (WHERE clause)
    if filter_condition:
        # NOTE: Assumes filter_condition is a safe Polars expression string (e.g., "pl.col('price') > 10")
        try:
            filter_expr = eval(filter_condition, {"pl": pl})
            df = df.filter(filter_expr)
            logger.info("Polars: Applied row filter.")
        except Exception as e:
            logger.error(f"Polars filter failed to execute: {e}")
            # Do not raise, as it might be complex logic; let the rest of the pipeline handle errors.

    #  Row Ordering
    if order_by_columns:
        sort_cols = [col for col, direction in order_by_columns]
        # Map 'asc' to False (0) and 'desc' to True (1)
        sort_descending = [
            direction.lower() == "desc" for col, direction in order_by_columns
        ]

        df = df.sort(sort_cols, descending=sort_descending)
        logger.info(
            f"Polars: Applied ordering by {sort_cols} (Descending: {sort_descending})."
        )

    #  Row Limit
    if row_limit is not None and row_limit > 0:
        df = df.limit(row_limit)
        logger.info(f"Polars: Applied row limit of {row_limit}.")

    return df


def _apply_delta_read_controls_pyspark(
    df: SparkFrame,
    columns: Optional[List[str]],
    filter_condition: Optional[str],
    order_by_columns: Optional[List[Tuple[str, str]]],
    row_limit: Optional[int],
) -> SparkFrame:
    """Applies projection, filter, order, and limit to a PySpark DataFrame."""

    # Imports need to be local to this function for safety, but we assume F and SparkDataFrame are available globally/conditionally.

    #  Column Projection
    if columns:
        # Spark's select automatically applies projection pushdown
        df = df.select(*columns)
        logger.info(f"PySpark: Applied column projection for {len(columns)} columns.")

    #  Filter Condition (WHERE clause)
    if filter_condition:
        # PySpark uses F.expr for SQL-like string filtering (e.g., "time_bucket > '2025-01-01'")
        df = df.filter(F.expr(filter_condition))
        logger.info("PySpark: Applied row filter.")

        #  Row Ordering (NEW LOGIC)
        if order_by_columns:
            # Build a list of Spark column expressions (F.asc(col) or F.desc(col))
            sort_expressions = []
            for col_name, direction in order_by_columns:
                if direction.lower() == "desc":
                    sort_expressions.append(F.col(col_name).desc())
                else:
                    # Default is ascending
                    sort_expressions.append(F.col(col_name).asc())

            df = df.orderBy(*sort_expressions)
            logger.info(f"PySpark: Applied ordering by {order_by_columns}.")

    #  Row Limit
    if row_limit is not None and row_limit > 0:
        # Spark's limit is applied efficiently to the distributed data set
        df = df.limit(row_limit)
        logger.info(f"PySpark: Applied row limit of {row_limit}.")

    return df


# ----------------------------------------------------------------------
# --- 2. BASE READER FUNCTIONS (Original Code) ---
# ----------------------------------------------------------------------


def _read_delta_table_polars(target_uri: str, options: Dict[str, Any]) -> PolarsFrame:
    """Reads Delta data using Polars scan_delta for base LazyFrame creation."""
    try:
        import polars as pl
    except ImportError:
        raise ImportError("The 'polars' library is required.")

    logger.info(f"Polars: Scanning Delta table at {target_uri}")
    # Base read operation
    return pl.scan_delta(target_uri, **options)


def _read_delta_table_pyspark(
    spark: Any, target_uri: str, options: Dict[str, Any]
) -> SparkFrame:
    """Reads Delta data using Spark's native read API for base DataFrame creation."""
    try:
        from pyspark.sql import DataFrame as SparkDataFrame
    except ImportError:
        raise ImportError("The 'pyspark' library is required.")

    logger.info(f"PySpark: Reading Delta table {target_uri}")
    # Base read operation
    reader = spark.read.format("delta").options(**options)
    return reader.load(target_uri)


def query_delta_with_sql(
    target_uri: str,
    sql_query: str,
    engine: str = "polars",
    temp_table_name: str = "_temp_delta_table",
    input_df: Optional[Any] = None,
    spark: Optional[SparkSession] = None,
) -> Union[pl.DataFrame, SparkFrame]:
    """
    Executes a SQL query against a Delta Lake table, dispatching the execution
    to the specified engine (DuckDB/Polars or PySpark).

    Args:
        target_uri (str): The path/URI or catalog name of the Delta table.
        sql_query (str): The SQL query to execute (must select from a table named 'my_delta_table').
        engine (str): The execution engine ('polars' uses DuckDB, 'pyspark' uses Spark SQL).
        temp_table_name (str): The temporary view name (required if engine='pyspark').
        spark (Optional[SparkSession]): The active Spark session (required if engine='pyspark').

    Returns:
        Union[pl.DataFrame, SparkFrame]: The result set as a Polars or Spark DataFrame.

    Raises:
        ValueError: If the engine is unknown or context is missing.
        ImportError: If required libraries are not installed.

    💡 Usage Example (Polars - Local Development):

    ```python
    LOCAL_PATH = "./.local-lakehouse/silver/market_ohlcv_master"
    QUERY = "SELECT internal_symbol, COUNT(*) FROM my_delta_table GROUP BY 1 LIMIT 5;"

    df_result_pl = query_delta_with_sql(
        target_uri=LOCAL_PATH,
        sql_query=QUERY,
        engine='polars',
        temp_table_name='my_temp_table'
    )
    # df_result_pl is a Polars DataFrame
    ```

    💡 Usage Example (PySpark - Cloud Execution):

    ```python
    # Assume spark_session is active
    CLOUD_TABLE = "prod_catalog.silver.market_ohlcv_master"
    QUERY = "SELECT internal_symbol, AVG(close) FROM my_delta_table GROUP BY 1;"

    df_result_spark = query_delta_with_sql(
        target_uri=CLOUD_TABLE,
        sql_query=QUERY,
        engine='pyspark',
        temp_table_name='my_temp_table',
        spark=spark_session
    )
    # df_result_spark is a Spark DataFrame
    ```
    """
    if engine.lower() == "polars":
        return _query_delta_with_duckdb(target_uri, sql_query, input_df, temp_table_name)

    elif engine.lower() == "pyspark":
        if not spark:
            raise ValueError(
                "A SparkSession object must be provided when engine='pyspark'."
            )
        return _query_delta_with_pyspark(spark, target_uri, sql_query, input_df, temp_table_name)

    else:
        raise ValueError(
            f"Unknown query engine: {engine}. Must be 'polars' or 'pyspark'."
        )


# -------------------------------------------------------------------------

### 2. Polars Implementation (DuckDB Core)

def _query_delta_with_duckdb(
    delta_table_path: Optional[str], 
    sql_query: str, 
    input_df: Optional[pl.DataFrame] = None, 
    temp_table_name: str = "my_delta_table"  # Renamed for clarity in query
) -> pl.DataFrame:
    """
    Executes a SQL query against a Delta table (via URI) OR an in-memory Polars DataFrame (input_df) using DuckDB.

    DuckDB registers the primary data source under the temporary name specified 
    by `temp_table_name` for use in the `sql_query`.

    Args:
        delta_table_path (Optional[str]): Local file path to the Delta table directory. Required if input_df is None.
        sql_query (str): The SQL query to execute (must select from the table named by temp_table_name).
        input_df (Optional[pl.DataFrame]): An optional Polars DataFrame to query directly.
        temp_table_name (str): The temporary view name DuckDB uses to hold the source data.

    Returns:
        pl.DataFrame: The result of the query.
    """
    
    # Check for valid input configuration
    if not delta_table_path and input_df is None:
        raise ValueError("Either 'delta_table_path' or 'input_df' must be provided.")

    try:
        import duckdb
    except ImportError:
        raise ImportError("The 'duckdb' library is required for the Polars query engine.")

    # 1. Connect to an in-memory DuckDB instance
    con = duckdb.connect(database=":memory:", read_only=False)

    # 2. Install/Load Delta extension (Only needed if querying the Delta path)
    if delta_table_path:
        try:
            con.sql("INSTALL delta; LOAD delta;")
        except Exception as e:
            logger.warning(f"DuckDB Warning: Could not install/load Delta extension: {e}")

    # 3. Register the Data Source
    if input_df is not None:
        # Register the in-memory Polars DataFrame (most efficient)
        con.register(temp_table_name, input_df)
        logger.info(f"DuckDB: Registered in-memory Polars DataFrame as '{temp_table_name}'.")
    elif delta_table_path:
        # Register the Delta table using delta_scan
        con.sql(
            f"CREATE OR REPLACE VIEW {temp_table_name} AS SELECT * FROM delta_scan('{delta_table_path}');"
        )
        logger.info(f"DuckDB: Registered Delta table at '{delta_table_path}' as '{temp_table_name}'.")
    
    # Execute the query using the simplified con.sql() interface
    result_relation = con.sql(sql_query)

    # --- FIX: Handle Zero Records and Fetch ---
    # We rely on fetching the Arrow Table first for schema preservation
    try:
        result_arrow = result_relation.fetch_arrow_table()
        
        # We must explicitly check for zero rows to prevent conversion errors
        if result_arrow.num_rows == 0:
            logger.warning("DuckDB: Query returned zero records. Returning empty Polars DataFrame.")
            # For zero rows, we rely on the returned Arrow table's schema
        
        df_result = pl.from_arrow(result_arrow)
        
    except Exception as e:
        logger.error(f"DuckDB query fetch failed: {e}")
        # As a final safeguard, try to fetch the schema of the query result if fetch_arrow_table failed
        # This is the last resort to avoid crashing on empty result set
        try:
             empty_arrow = con.execute(f"SELECT * FROM ({sql_query}) AS subquery LIMIT 0").fetch_arrow_table()
             df_result = pl.from_arrow(empty_arrow)
        except Exception:
            # Crash if even schema recovery fails
            con.close()
            raise

    con.close()
    return df_result


def _query_delta_with_pyspark(
    spark: Any, # SparkSession type
    target_uri: Optional[str],
    sql_query: str,
    input_df: Optional[Any] = None, 
    temp_table_name: str = "my_delta_table",
) -> Any: # Returns SparkFrame
    """
    Private helper: Executes SQL against a cloud-based Delta table (URI) 
    or an in-memory Spark DataFrame (input_df), ensuring schema is preserved 
    even when the query returns zero records.

    Args:
        spark (Any): The active SparkSession instance.
        target_uri (Optional[str]): The path/URI or catalog name of the Delta table. Required if input_df is None.
        sql_query (str): The SQL query to execute.
        input_df (Optional[Any]): An optional Spark DataFrame to query directly (in-memory).
        temp_table_name (str): The temporary view name created from the source data.

    Returns:
        SparkFrame: The result set as a Spark DataFrame (guaranteed to have a schema).
    """
    try:
        # Conditional PySpark Imports
        from pyspark.sql import DataFrame as SparkDataFrame, functions as F
        from pyspark.sql import SparkSession # Re-importing for clarity
    except ImportError:
        raise ImportError("The 'pyspark' library is required for the PySpark query engine.")
    
    # --- Input Validation ---
    if not target_uri and input_df is None:
        raise ValueError("Either 'target_uri' or 'input_df' must be provided.")

    # 1. Register the Data Source (Unified Logic)
    
    # Check if we are querying an existing DataFrame or a file path
    if input_df is not None:
        # Register the in-memory Spark DataFrame
        df_source = input_df
        df_source.createOrReplaceTempView(temp_table_name)
        logger.info(f"PySpark: Registered in-memory DataFrame as temporary view '{temp_table_name}'.")

    elif target_uri:
        # Register the Delta table for querying from URI/Catalog
        
        # Logic to determine if URI is a full catalog name or a location path
        if "." in target_uri:
            # Assume target_uri is a catalog/table name (e.g., prod.silver.ohlcv)
            view_creation_sql = f"CREATE OR REPLACE TEMPORARY VIEW {temp_table_name} AS SELECT * FROM {target_uri}"
        else:
            # Assume target_uri is a file path (e.g., abfss://...)
            view_creation_sql = f"CREATE OR REPLACE TEMPORARY VIEW {temp_table_name} USING DELTA LOCATION '{target_uri}'"

        logger.info(f"PySpark: Creating temporary view {temp_table_name} from Delta URI.")
        spark.sql(view_creation_sql)

    # 2. Execute the user's query against the temporary view
    df_result = spark.sql(sql_query)

    # 3. Handle Zero Records (Schema Preservation)
    if df_result.isEmpty():
        logger.warning(
            "PySpark: Query returned zero records. Preserving schema and returning empty DataFrame."
        )
        # Extract the schema and recreate an empty DF to ensure schema integrity for downstream steps
        result_schema = df_result.schema
        df_result = spark.createDataFrame([], result_schema)

    return df_result