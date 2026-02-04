# from pyspark.sql import SparkSession
from typing import Dict, Any, Optional, List, Union
from loguru import logger
from .lakehouse_management import get_delta_path
from .schema_validation import validate_schema

import pathlib
import pyarrow as pa
import os
import json
import yaml
import polars as pl
import copy


# Conditional Imports/Type Hints
PolarsFrame = Union[pl.DataFrame, pl.LazyFrame]
SparkFrame = Any
SparkSession = Any 

# --- 1. Polars/Deltalake Engine Implementation ---


def _get_default_type(engine: str) -> str:
    """Returns the most appropriate default type string for a derived column based on the engine."""
    engine = engine.lower()
    if engine == "polars":
        # Polars prefers specific types, but 'STRING' is the safest default for schema inference
        return "STRING"
    elif engine == "pyspark":
        # PySpark often defaults to 'string' in transformations
        return "string"
    return "string"


def _merge_dicts(d1: Dict, d2: Dict) -> Dict:
    """Recursively merges dictionary d2 into d1."""
    merged = d1.copy()
    for k, v in d2.items():
        if isinstance(v, dict) and k in d1 and isinstance(d1[k], dict):
            merged[k] = _merge_dicts(d1[k], v)
        else:
            merged[k] = v
    return merged


def _map_yaml_type_to_pyarrow_type_object(yaml_type: str) -> pa.DataType:
    """Helper that returns the instantiated PyArrow DataType object (e.g., pa.string())."""
    mapping = {
        "STRING": pa.string(),
        "BOOLEAN": pa.bool_(),
        "FLOAT": pa.float64(),
        "INT": pa.int64(),
        "TIMESTAMP": pa.timestamp("us", tz="UTC"),
        "DATE": pa.date32(),
        "FLOAT64": pa.float64(),  # ... etc.
    }
    return mapping.get(yaml_type.upper(), pa.string())


def _create_or_update_delta_table_polars(
    schema: Dict[str, Any], target_path: str, alter_table: bool, debug_mode: bool
):
    logger.info(f"Using Polars/Deltalake engine. Target path: {target_path}")

    try:
        from deltalake import DeltaTable  # Localized import for DeltaTable
        import pyarrow as pa
    except ImportError:
        # We rely on this being available for Polars-first mode
        raise ImportError(
            "The 'deltalake' and 'pyarrow' libraries are required for the Polars engine."
        )

    # 1. DEFINE THE PYARROW SCHEMA (DRY & SIMPLIFIED)
    # Use a list comprehension to create pa.Field objects, leveraging the pa.field() constructor
    arrow_fields: List[pa.Field] = [
        pa.field(
            col["name"],
            _map_yaml_type_to_pyarrow_type_object(col["type"]),
            col.get("nullable", True),
        )
        for col in schema["model"]["columns"]
    ]

    # Create the final schema object
    target_arrow_schema = pa.schema(arrow_fields)  # Use pa.schema()

    # Extract write options and partition_by (No Change)
    write_config = schema["model"]["write"]
    partition_by = write_config.get("cluster_by", [])
    tbl_options_raw = write_config.get("options", {})

    # Ensure the directory structure exists
    os.makedirs(target_path, exist_ok=True)

    # Check if the table/path already exists
    table_exists = os.path.isdir(os.path.join(target_path, "_delta_log"))

    if not table_exists:
        logger.info(
            f"Delta table at '{target_path}' does not exist. Creating new table structure."
        )

        if debug_mode:
            logger.info(
                f"--- Debug Mode: Would create new Delta table structure with schema: {target_arrow_schema}"
            )
            return

        try:
            # Preprocess options dictionary to convert all lists to comma-separated strings
            tbl_options_processed = {}
            for key, value in tbl_options_raw.items():
                if isinstance(value, list):
                    # Convert list values (like dataSkippingStatsColumns) to a single string
                    tbl_options_processed[key] = ",".join(map(str, value))
                else:
                    # Keep non-list values (like 'true' or '30 days') as they are
                    tbl_options_processed[key] = str(
                        value
                    )  # Ensure all values are strings

            DeltaTable.create(
                table_uri=target_path,
                schema=target_arrow_schema,
                partition_by=partition_by,
                configuration=tbl_options_processed,
            )
            logger.info(
                f"✅ Polars/Deltalake table created successfully at: {target_path}"
            )

        except Exception as e:
            logger.error(
                f"Failed to create Delta table structure at {target_path}: {e}"
            )
            raise

    elif alter_table:
        logger.info("Table already exists. Checking for schema updates...")
        # NOTE: Deltalake schema evolution is implicitly handled during subsequent MERGE/APPEND.
        # This section remains for validation/reporting purposes.
        try:
            # existing_polars_df = pl.read_delta(target_path, limit=0)
            # existing_schema = existing_polars_df.to_arrow().schema

            # 1. Use the Lazy API to scan the path
            existing_schema_lazy = pl.scan_delta(target_path)
            existing_schema = existing_schema_lazy.limit(0).collect().to_arrow().schema

            # dt = DeltaTable(target_path)
            # existing_schema = dt.to_polars().schema.to_pyarrow()

            new_cols_added = 0
            for field in target_arrow_schema:
                if field.name not in existing_schema.names:
                    logger.warning(
                        f"⚠️ Column '{field.name}' not found. Will be added on next MERGE/APPEND."
                    )
                    new_cols_added += 1

            if new_cols_added == 0:
                logger.info("✅ No new columns detected. Table is up-to-date.")

        except Exception as e:
            logger.error(f"Failed to read existing Delta table at {target_path}: {e}")
            raise Exception(
                f"Failed to read existing Delta table at {target_path}: {e}"
            )

    else:
        logger.info(f"Table '{target_path}' already exists. Alterations are disabled.")


def _create_or_update_delta_table_pyspark(
    spark: Any,  # SparkSession
    schema: Dict[str, Any],
    target_table_name: str,
    location: Optional[str] = None,
    alter_table: bool = False,
    debug_mode: bool = False,
):
    """
    (Internal PySpark Logic) Dynamically creates a Delta table or updates its schema
    and properties using PySpark SQL DDL statements (original function logic).
    """
    table_exists = spark.catalog.tableExists(target_table_name)
    changes_detected = False

    if not table_exists:
        logger.info(f"Table '{target_table_name}' does not exist. Creating new table.")
        # --- 1. Build and run CREATE TABLE statement for new tables ---

        column_defs = []
        for col in schema["model"]["columns"]:
            name = col["name"]
            col_type = col["type"].upper()
            nullable = "NOT NULL" if not col.get("nullable", True) else ""
            # Escaping single quotes in comments for SQL DDL
            comment_text = col.get("description", "").replace("'", "''")
            comment = f"COMMENT '{comment_text}'"
            column_defs.append(f"`{name}` {col_type} {nullable} {comment}")

        column_sql = ",\n    ".join(column_defs)

        cluster_by_cols = schema["model"]["write"].get("cluster_by", [])
        cluster_sql = ""
        if cluster_by_cols:
            quoted_cols = [f"`{col}`" for col in cluster_by_cols]
            cluster_sql = f"CLUSTER BY ({', '.join(quoted_cols)})"

        tbl_properties = schema["model"]["write"].get("options", {})
        properties_sql = ""
        if tbl_properties:
            props_list = []
            for k, v in tbl_properties.items():
                # Format list values (like delta.dataSkippingStatsColumns) into comma-separated strings
                value_str = ",".join(map(str, v)) if isinstance(v, list) else v
                props_list.append(f"'{k}' = '{value_str}'")
            properties_sql = f"TBLPROPERTIES ({', '.join(props_list)})"

        table_comment = schema.get("description", "")
        table_comment_sql = f"COMMENT '{table_comment}'" if table_comment else ""
        location_sql = f"LOCATION '{location}'" if location else ""

        create_sql = f"""
        CREATE TABLE {target_table_name} ({column_sql})
        USING DELTA {cluster_sql} {location_sql} {table_comment_sql} {properties_sql}
        """

        if debug_mode:
            logger.info("--- Debug Mode: CREATE DDL ---")
            logger.info(create_sql)
        else:
            spark.sql(create_sql)
            logger.info(f"table: {target_table_name} created")

    elif alter_table:
        logger.info(
            f"Table '{target_table_name}' already exists. Checking for updates..."
        )

        # Use an internal utility to fetch column details cleanly
        existing_table_details = spark.sql(
            f"DESCRIBE TABLE EXTENDED {target_table_name}"
        ).collect()

        # Filter for actual columns and extract column comments
        existing_cols = {
            row["col_name"]: row["comment"]
            for row in existing_table_details
            if not row["col_name"].startswith("#")
            and row["col_name"]
            not in ["Location", "Provider", "Owner", "Table Properties", "Comment"]
        }

        # A. Check for and add new columns
        new_cols_to_add = []
        for col_config in schema["model"]["columns"]:
            if col_config["name"] not in existing_cols:
                name = col_config["name"]
                col_type = col_config["type"].upper()
                comment_text = col_config.get("description", "").replace("'", "''")
                comment = f"COMMENT '{comment_text}'"
                new_cols_to_add.append(f"`{name}` {col_type} {comment}")

        if new_cols_to_add:
            changes_detected = True
            add_cols_sql = f"ALTER TABLE {target_table_name} ADD COLUMNS ({', '.join(new_cols_to_add)})"

            if debug_mode:
                logger.info("--- Debug Mode: ADD COLUMNS DDL ---")
                logger.info(add_cols_sql)
            else:
                logger.info("--- Change detected: Adding new columns ---")
                spark.sql(add_cols_sql)
                logger.info(
                    f"✅ Added {len(new_cols_to_add)} new column(s) to {target_table_name}"
                )

        # B. Check for and update comments on existing columns (Optional for many ETL workflows)
        # Skipped for brevity, as the original provided logic was complex due to parsing DESCRIBE EXTENDED output.
        # In practice, comment updates are done separately and less frequently.

        # C. Check for and update table properties and the main table comment
        # This section is the most prone to parsing issues from DESCRIBE EXTENDED.
        # The logic below attempts to find the property rows by name.

        # Simplified parsing for table properties and comment
        table_properties_map = {
            row["col_name"]: row["data_type"]
            for row in existing_table_details
            if row["col_name"] in ["Table Properties", "Comment"]
        }

        existing_properties_str = table_properties_map.get("Table Properties", "")
        existing_comment = table_properties_map.get("Comment", "")

        # Manual parsing of TBLPROPERTIES string (e.g., '(key1=val1,key2=val2)')
        existing_properties = {}
        if existing_properties_str and existing_properties_str not in ("()", "NULL"):
            try:
                # Simple split/join logic from original
                props_list = existing_properties_str.strip("()").split(",")
                for prop in props_list:
                    key_value = prop.split("=", 1)
                    if len(key_value) == 2:
                        key = key_value[0].strip()
                        value = key_value[1].strip().strip("'\"")
                        existing_properties[key] = value
            except Exception as e:
                logger.warning(f"Could not parse existing properties string: {e}")

        # Construct target properties map
        target_properties = schema["model"]["write"].get("options", {}).copy()

        # Compare and update properties/comment
        needs_prop_update = False

        # Check if the overall table comment needs updating
        if existing_comment != schema.get("description", ""):
            needs_prop_update = True
            target_properties["comment"] = schema.get("description", "")

        # Check for property changes
        formatted_target_properties = {
            k: (",".join(map(str, v)) if isinstance(v, list) else str(v))
            for k, v in target_properties.items()
        }

        # This comparison is still imperfect due to quote and list formatting issues when comparing raw strings
        if not all(
            existing_properties.get(k) == v
            for k, v in formatted_target_properties.items()
        ) or len(formatted_target_properties) != len(existing_properties):
            needs_prop_update = True

        if needs_prop_update:
            changes_detected = True
            props_list = [
                f"'{k}' = '{v}'" for k, v in formatted_target_properties.items()
            ]
            properties_sql = f"ALTER TABLE {target_table_name} SET TBLPROPERTIES ({', '.join(props_list)})"

            if debug_mode:
                logger.info("--- Debug Mode: SET TBLPROPERTIES DDL ---")
                logger.info(properties_sql)
            else:
                logger.info(
                    "--- Change detected: Updating table properties/comment ---"
                )
                spark.sql(properties_sql)
                logger.info(
                    f"✅ Updated table properties/comment for {target_table_name}"
                )

        # Cluster/Partitioning check
        if schema["model"]["write"].get("cluster_by"):
            logger.info(
                "ℹ️ Info: Liquid clustering/Partitioning keys cannot be changed after table creation and were not checked for alteration."
            )

        if not changes_detected:
            logger.info(
                "✅ No schema or property changes detected. Table is already up-to-date."
            )
    else:
        logger.info(
            f"Table '{target_table_name}' already exists. Alterations are disabled by the 'alter_table=False' flag."
        )


def create_or_update_delta_table(
    schema: Dict[str, Any],
    target_name_or_path: str,
    engine: str = "polars",
    spark: Any = None,
    alter_table: bool = False,
    debug_mode: bool = False,
):
    """
    Dynamically creates or updates a Delta Lake table based on a schema dictionary,
    dispatching the execution based on the specified engine ('polars' or 'pyspark').

    This function ensures the Delta table structure matches the defined YAML schema
    before ETL data merge/append operations begin.

    Args:
        schema (dict): The schema definition dictionary derived from a YAML config file.
        target_name_or_path (str): The target table location.
            - If engine='pyspark': This should be the full, three-part name (e.g., catalog.schema.table).
            - If engine='polars': This should be the local file system path (e.g., ./.local-lakehouse/silver/table).
        engine (str): The processing engine to use. Defaults to 'polars' for local development.
        spark (Optional[SparkSession]): The active Spark session (required if engine='pyspark').
        alter_table (bool): If True, enables schema evolution (adding columns, updating comments) for existing tables.
        debug_mode (bool): If True, prints DDL statements/actions without executing them.

    Raises:
        ValueError: If an invalid engine is specified or if 'pyspark' is chosen without a SparkSession.

    ---

    **💡 Usage Example (Local Development - Polars)**

    ```python
    from src.etl.utils.schema_manager import create_or_update_delta_table

    # 1. Define a minimal mock schema
    MOCK_SCHEMA = {
        'description': 'Test table for polars',
        'model': {
            'columns': [
                {'name': 'id', 'type': 'INT', 'nullable': False},
                {'name': 'name', 'type': 'STRING', 'description': 'User name'},
            ],
            'write': {'options': {'comment': 'Updated Comment'}}
        }
    }

    # 2. Define the local path
    LOCAL_PATH = "./.local-lakehouse/reference/mock_table"

    # 3. Run the function (will create the Delta table structure)
    create_or_update_delta_table(
        schema=MOCK_SCHEMA,
        target_name_or_path=LOCAL_PATH,
        engine='polars',
        alter_table=True,
        debug_mode=True
    )
    ```

    **💡 Usage Example (Production - PySpark)**

    ```python
    from src.etl.utils.schema_manager import create_or_update_delta_table
    from pyspark.sql import SparkSession

    # Assume MOCK_SCHEMA is loaded
    # 1. Create Spark Session (or assume it's running in Databricks/cluster)
    spark = SparkSession.builder.appName("SchemaInit").getOrCreate()

    # 2. Run the function (will execute DDL statements against the catalog)
    create_or_update_delta_table(
        schema=MOCK_SCHEMA,
        target_name_or_path="catalog.schema.mock_table",
        engine='pyspark',
        spark=spark,
        alter_table=True,
        debug_mode=True
    )
    ```
    """
    if engine.lower() == "polars":
        return _create_or_update_delta_table_polars(
            schema=schema,
            target_path=target_name_or_path,
            alter_table=alter_table,
            debug_mode=debug_mode,
        )

    elif engine.lower() == "pyspark":
        if not spark:
            raise ValueError(
                "A SparkSession object must be provided when engine='pyspark'."
            )

        # Call the original PySpark logic function (renamed internally)
        return _create_or_update_delta_table_pyspark(
            spark=spark,
            schema=schema,
            target_table_name=target_name_or_path,
            # Pass location from target_name_or_path if needed, or handle it inside
            location=None,  # Location is typically managed by the catalog when using three-part naming
            alter_table=alter_table,
            debug_mode=debug_mode,
        )

    else:
        raise ValueError(f"Invalid engine '{engine}'. Must be 'polars' or 'pyspark'.")


def _load_schema_from_registry(schema_full_path: str) -> Dict[str, Any]:
    """
    Locates and loads a YAML schema definition file from the registry path
    using the schema_full_path provided.

    Args:
        schema_full_path (str): The full path to the schema file.

    Returns:
        Dict[str, Any]: The loaded schema dictionary.

    Raises:
        FileNotFoundError: If the YAML file cannot be found.
    """

    full_schema_path = pathlib.Path(schema_full_path).resolve()

    schema_filename = ""
    schema_filename = schema_full_path.split("/")[-1]

    if not os.path.exists(full_schema_path):
        logger.error(f"Schema file not found at expected path: {full_schema_path}")
        raise FileNotFoundError(
            f"Schema file '{schema_filename}' not found at: {full_schema_path}"
        )

    # 4. Load the YAML content (rest of the logic remains the same)
    with open(full_schema_path, "r") as f:
        schema_data = yaml.safe_load(f)

    dataset_name = schema_data.get("dataset_name", "")

    if str(full_schema_path).find("_default_schema.yaml") >= -1:
        logger.info(f"Loaded default schema from schema file: {schema_filename}")
        return schema_data
    else:
        logger.info(
            f"Loaded schema for dataset '{dataset_name}' from schema file: {schema_filename}"
        )
    return schema_data


def initialize_lakehouse_table(
    dataset_schema_full_path: str,
    dataset_schema: Optional[Dict[str, Any]],
    lakehouse_path: str,
    databricks_catalog: str,
    table_prefix: str,
    target_layer: str,
    engine: str = "polars",
    spark: Optional[Any] = None,
    debug_mode: bool = False,
    default_schema_path: str = "",
    default_schema_list: List = [],
    debug: bool = False,
) -> str:
    """
    Initializes a Delta table in the specified Lakehouse layer by loading a YAML schema
    file and using the central dispatcher.

    Args:
        dataset_schema_full_path (str): The full path to the dataset schema file.
        dataset_schema (Optional[Dict[str, Any]]): The dataset schema dictionary.
        lakehouse_path (str): The full path to the Lakehouse directory.
        databricks_catalog (str): The name of the Databricks catalog.
        table_prefix (str): The prefix to be added to the table name.
        target_layer (str): The logical layer (e.g., 'reference', 'silver', 'gold').
        engine (str): The processing engine ('polars' for local, 'pyspark' for cloud).
        spark (Optional[Any]): The active SparkSession (required if engine='pyspark').
        debug_mode (bool): If True, runs the schema manager in debug mode.
        default_schema_path (str): Path to the default schema file.
        default_schema_list (List[str], optional): List of default schemas to merge

    Returns:
        Dict[str, Any]: Dictionary containing target layer, table name, path, and the final schema dictionary.

    💡 Usage Example (Local Development - Polars):

    ```python
    # Ensure all helper imports are correct in your environment
    # NOTE: The schema file must exist in the registry path.

    table_details = initialize_lakehouse_table(
            dataset_schema_full_path='path/to/market_ohlcv_master.yaml',
            dataset_schema=None,
            lakehouse_path='D:/Github/_SaaS/SaaS_getdatalineage/.local-lakehouse',
            databricks_catalog='demox_dev_001',
            table_prefix='market_ohlcv_',
            target_layer='silver',
            engine='polars',
            default_schema_path='path/to/_default_schema.yaml',
            default_schema_list=['metadata_columns'], # Only add audit metadata
        )
        print(f"Table initialized at: {table_details['target_name_or_path']}")
    ```
    """
    try:
        # 1. Load the Schema from the Registry
        # The schema_filename should now contain the target_layer information

        if dataset_schema_full_path:
            schema_data = _load_schema_from_registry(dataset_schema_full_path)
        else:
            schema_data = dataset_schema

        # Merge Default Columns (if specified)
        if default_schema_list:
            # Load defaults from the specialized '_common' pipeline folder
            schema_data_default = _load_schema_from_registry(default_schema_path)

            # --- Perform Merging Logic ---
            for default_schema_key in default_schema_list:
                logger.info(f"Adding default schema: {default_schema_key}")

                # Retrieve the list of columns to be merged
                default_cols_to_add = schema_data_default["defaults"].get(
                    default_schema_key, []
                )

                if not default_cols_to_add:
                    logger.warning(
                        f"Default schema key '{default_schema_key}' not found or empty in _default_schema.yaml."
                    )
                    continue

                # Slow Changing Dimension (SCD) columns must be PREPENDED (put at the start)
                if "slowly_changing_dimension" in default_schema_key:
                    # Prepend the SCD columns to the user's defined columns
                    schema_data["model"]["columns"] = (
                        default_cols_to_add + schema_data["model"]["columns"]
                    )

                # Standard Metadata columns are APPENDED (put at the end)
                elif "metadata_columns" in default_schema_key:
                    # Append the Metadata columns to the user's defined columns
                    schema_data["model"]["columns"].extend(default_cols_to_add)

        try:
            logger.info("Validating Schema...")
            validate_schema(schema_data, primary_key_nullable=True)
        except Exception as e:
            logger.error(f"Schema validation failed: {str(e)}")
            raise


        # Extract the target table name
        table_key = schema_data["dataset"]

        if table_prefix:
            logger.info(f"Applying table prefix: {table_prefix}")
            table_key = f"{table_prefix}_{table_key}"

        if debug_mode:
            logger.info(
                f"--- Debug Mode: Initializing table '{table_key}' in layer '{target_layer}' using engine '{engine}' ---"
            )
            logger.info(f"Schema Data: {json.dumps(schema_data, indent=2)}")

        if engine == "polars":
            # 3A. Polars: Determine local path and ensure local directory exists
            target_path = get_delta_path(
                lakehouse_path=lakehouse_path, layer=target_layer, table_name=table_key
            )
            os.makedirs(target_path, exist_ok=True)
            target_name_or_path = target_path

        elif engine == "pyspark":
            # 3B. PySpark: Determine the three-part cloud catalog name
            # Assuming the full catalog name is passed via the 'spark' object's configuration or a global variable
            CATALOG_NAME = databricks_catalog
            target_name_or_path = f"{CATALOG_NAME}.{target_layer}.{table_key}"

        else:
            raise ValueError(f"Invalid engine '{engine}'.")

        logger.info(f"Initializing table '{table_key}' in layer '{target_layer}'...")

        if debug_mode:
            logger.info(f"Target Name/Path resolved to: {target_name_or_path}")

        # 4. Call the central dispatch function
        # This function handles the actual DDL creation or Polars write.
        create_or_update_delta_table(
            schema=schema_data,
            target_name_or_path=target_name_or_path,
            engine=engine,
            spark=spark,
            alter_table=True,  # Allow schema evolution if it already exists
            debug_mode=debug_mode,
        )

        logger.success(f"Successfully initialized table at: {target_name_or_path}")
        return {
            "target_layer": target_layer,
            "table_key": table_key,
            "target_name_or_path": target_name_or_path,
            "schema": schema_data,
        }

    except Exception as e:
        logger.error(
            f"Failed to initialize table from schema -> target_name_or_path: '{target_name_or_path}': {e}"
        )
        raise


def derive_silver_schema(
    bronze_schema: Dict[str, Any],
    transformations: List[Dict[str, Any]],
    engine: str,
    silver_overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Derives a Silver schema from a Bronze schema and a list of transformations.

    This function simulates the effects of the ETL transformations on the schema
    structure (column renaming, casting, adding new columns) to predict the
    final schema state.

    Args:
        bronze_schema (Dict[str, Any]): The source schema dictionary (e.g., from a YAML file).
        transformations (List[Dict[str, Any]]): A list of transformation definitions.
        engine (str): The target processing engine ('polars' or 'pyspark').
        silver_overrides (Optional[Dict[str, Any]]): A dictionary containing Silver-specific
            configurations, such as a 'write' block to override Bronze properties.

    Returns:
        Dict[str, Any]: The derived Silver schema dictionary.

    Example:
        # Example is conceptual, relying on external definition of transformations.
        # The key takeaway is the accurate prediction of the schema structure.

        # Example Call:
        # silver_schema = derive_silver_schema(bronze_schema, trans_config, engine='polars', silver_overrides)
    """
    if "model" not in bronze_schema or "columns" not in bronze_schema["model"]:
        raise ValueError(
            "Invalid bronze_schema: must contain 'model' and 'model.columns'."
        )

    # Start with a deep copy of the bronze schema to avoid modifying the original
    silver_schema = copy.deepcopy(bronze_schema)

    # Use a dictionary for easier column lookups by name
    columns_dict = {col["name"]: col for col in silver_schema["model"]["columns"]}

    logger.info(
        f"Deriving Silver schema from {len(columns_dict)} Bronze columns using {engine.upper()} engine..."
    )

    for transform in transformations:
        transform_name = next(iter(transform))
        transform_config = transform[transform_name]

        if transform_name == "rename_column":
            col_from = transform_config["from"]
            col_to = transform_config["to"]
            if col_from in columns_dict:
                # logger.info(f"  - Renaming column '{col_from}' to '{col_to}'")
                col_data = columns_dict.pop(col_from)
                col_data["name"] = col_to
                columns_dict[col_to] = col_data

        elif transform_name == "cast_column":
            col_name = transform_config["column"]
            target_type = transform_config["type"]
            if col_name in columns_dict:
                # logger.info(f"  - Updating type for column '{col_name}' to '{target_type}'")
                columns_dict[col_name]["type"] = target_type

        elif transform_name == "derive_column":
            col_name = transform_config["name"]

            # Use engine to set a sensible default type if not explicitly provided in the config
            default_type = transform_config.get("type", _get_default_type(engine))

            # logger.info(f"  - Deriving new column '{col_name}' with default type '{default_type}'")
            new_col = {
                "name": col_name,
                "type": default_type,  # Use the determined type
                "nullable": transform_config.get("nullable", True),
                "description": transform_config.get("description", "Derived column."),
            }
            columns_dict[col_name] = new_col

        elif transform_name == "drop_column":
            cols_to_drop = transform_config
            for col_name in cols_to_drop:
                if col_name in columns_dict:
                    # logger.info(f"  - Dropping column '{col_name}'")
                    columns_dict.pop(col_name)

        # NOTE: Other transforms like 'replace_column_names' and 'promote_header'
        # do not change the number or type of columns, so they are not necessary here.

    # Convert the dictionary back to a list of columns
    silver_schema["model"]["columns"] = list(columns_dict.values())

    # Merge Silver-specific write properties over the inherited Bronze properties.
    if silver_overrides and "write" in silver_overrides:
        logger.info("  - Merging Silver-specific write properties...")
        if "write" not in silver_schema["model"]:
            silver_schema["model"]["write"] = {}

        bronze_write_props = silver_schema["model"]["write"]
        silver_write_props = silver_overrides["write"]

        # Use the provided recursive merge helper
        silver_schema["model"]["write"] = _merge_dicts(
            bronze_write_props, silver_write_props
        )

    # Update other metadata for the Silver table
    # Assume the Bronze schema had a 'dataset' key
    bronze_dataset_name = bronze_schema.get("dataset", "unknown_dataset")
    silver_schema["dataset"] = f"{bronze_dataset_name}"
    silver_schema["description"] = (
        f"Cleansed and transformed Silver layer for {bronze_dataset_name}."
    )

    logger.info("✅ Silver schema derived successfully.")
    return silver_schema


def _get_existing_column_names(target_uri: str, engine: str, spark: Optional[SparkSession] = None) -> List[str]:
    """
    Private helper to fetch the column names of the existing Delta table on disk.
    """
    if engine.lower() == 'polars':
        try:
            # Use Polars Lazy API to scan the schema (efficient)
            existing_schema = pl.scan_delta(target_uri).collect_schema()
            return existing_schema.names()
        except Exception:
            # If the table doesn't exist, return an empty list
            return []
            
    elif engine.lower() == 'pyspark':
        if not spark: raise ValueError("Spark session required for PySpark schema check.")
        try:
            # Read the schema of the existing table
            df_existing = spark.read.format("delta").load(target_uri)
            return df_existing.columns
        except Exception:
            # If table/path doesn't exist, return empty list
            return []
            
    return []


def get_newly_added_columns(
    target_uri: str,
    final_schema: Dict[str, Any],
    engine: str,
    spark: Optional[SparkSession] = None
) -> List[str]:
    """
    Compares the intended final schema (from YAML config) against the schema 
    of the existing Delta table to identify and return a list of new column names.

    Args:
        target_uri (str): The path/URI to the Delta table (target of the write).
        final_schema (Dict[str, Any]): The full, derived schema dictionary containing
            the 'model.columns' list.
        engine (str): The execution engine ('polars' or 'pyspark').
        spark (Optional[SparkSession]): The active Spark session (required if engine='pyspark').

    Returns:
        List[str]: A list of column names that exist in final_schema but not in the Delta table.
        
    💡 Usage Example:
    
    ```python
    # Assume FULL_SILVER_SCHEMA includes new_column_X
    new_cols = get_newly_added_columns(
        target_uri='local_lakehouse/silver/ohlcv',
        final_schema=FULL_SILVER_SCHEMA,
        engine='polars'
    )
    if new_cols:
        logger.warning(f"Schema Evolution detected: {new_cols}")
    ```
    """
    
    # 1. Get the list of columns in the currently existing Delta table
    existing_columns = _get_existing_column_names(target_uri, engine, spark)
    
    # Check if the table is brand new
    if not existing_columns:
        logger.info("Target Delta table does not exist yet. All columns are 'new'.")
        return [] # Return empty list if table is brand new; no downstream flag needed yet.

    # 2. Get the list of columns the application intends to write
    intended_columns = {col['name'] for col in final_schema['model']['columns']}
    
    # 3. Perform the difference check
    existing_set = set(existing_columns)
    
    # Find columns that are IN the intended set BUT NOT IN the existing set
    new_columns = sorted(list(intended_columns - existing_set))
    
    if new_columns:
        logger.warning(f"Schema change detected: {new_columns} will be added to the target table.")
    
    return new_columns
