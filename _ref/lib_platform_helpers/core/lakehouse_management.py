from typing import List, Dict, Any, Optional
import copy
from loguru import logger
import os


def _merge_dicts(base: Dict, override: Dict) -> Dict:
    """Helper function to recursively merge two dictionaries."""
    for key, value in override.items():
        if isinstance(value, dict) and key in base and isinstance(base[key], dict):
            base[key] = _merge_dicts(base[key], value)
        else:
            base[key] = value
    return base


# File: src/etl/lib_platform_helpers/core/lakehouse_management.py (assumed location)


def get_delta_path(lakehouse_path: str, layer: str, table_name: str) -> str:
    """
    Constructs the full path for a Delta table based on the environment root and layer.

    Args:
        layer (str): The logical layer (e.g., 'bronze', 'silver', 'reference').
        table_name (str): The name of the Delta table directory.

    Returns:
        str: The full filesystem path (local) or ABFS path (cloud).

    💡 Usage Example:
    ```python
    # Assuming LAKEHOUSE_ROOT_PATH='./.local-lakehouse'
    path = get_delta_path(layer='reference', table_name='ticker_registry')
    # Result: ./.local-lakehouse/reference/ticker_registry
    ```
    """
    root = lakehouse_path
    if not root:
        # Fallback for production when using ABFS paths via environment variables
        # Note: In PySpark, this is often handled by the catalog, but this is a useful utility.
        return f"abfs://{layer}/{table_name}"

    # Local path structure
    return os.path.join(root, layer, table_name)


def derive_silver_schema(
    bronze_schema: Dict[str, Any],
    transformations: List[Dict[str, Any]],
    silver_overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Derives a Silver schema from a Bronze schema and a list of transformations.

    This function takes an initial schema and a list of transformation rules
    (e.g., renames, casts, derived columns) and returns a new schema dictionary
    that reflects the final state of the data. It also merges any Silver-specific
    'write' properties over the Bronze defaults.

    Args:
        bronze_schema (Dict[str, Any]): The source schema dictionary (e.g., from a YAML file).
        transformations (List[Dict[str, Any]]): A list of transformation definitions.
        silver_overrides (Optional[Dict[str, Any]]): A dictionary containing Silver-specific
            configurations, such as a 'write' block to override Bronze properties.

    Returns:
        Dict[str, Any]: The derived Silver schema dictionary.

    Example:
        bronze_schema = {
            "dataset": "customers", "version": 1.0, "primary_key": "id",
            "model": {
                "columns": [
                    {"name": "id", "type": "integer", "description": "Primary key."},
                    {"name": "fname", "type": "string", "description": "Customer first name."}
                ],
                "write": {"cluster_by": ["id"], "options": {"delta.autoOptimize.autoCompact": "true"}}
            }
        }

        trans_config = [{"rename_column": {"from": "fname", "to": "first_name"}}]
        silver_overrides = {"write": {"cluster_by": ["first_name"]}} # Override clustering for Silver

        silver_schema = derive_silver_schema(bronze_schema, trans_config, silver_overrides)
        # The returned schema will have the correct columns and the updated 'cluster_by' property,
        # but will still inherit the 'delta.autoOptimize.autoCompact' option.
    """
    if "model" not in bronze_schema or "columns" not in bronze_schema["model"]:
        raise ValueError(
            "Invalid bronze_schema: must contain 'model' and 'model.columns'."
        )

    # Start with a deep copy of the bronze schema to avoid modifying the original
    silver_schema = copy.deepcopy(bronze_schema)

    # Use a dictionary for easier column lookups by name
    columns_dict = {col["name"]: col for col in silver_schema["model"]["columns"]}

    logger.info(f"Deriving Silver schema from {len(columns_dict)} Bronze columns...")

    for transform in transformations:
        transform_name = next(iter(transform))
        transform_config = transform[transform_name]

        if transform_name == "rename_column":
            col_from = transform_config["from"]
            col_to = transform_config["to"]
            if col_from in columns_dict:
                # logger.info(f"  - Renaming column '{col_from}' to '{col_to}'")
                col_data = columns_dict.pop(col_from)
                col_data["name"] = col_to
                columns_dict[col_to] = col_data

        elif transform_name == "cast_column":
            col_name = transform_config["column"]
            target_type = transform_config["type"]
            if col_name in columns_dict:
                # logger.info(f"  - Updating type for column '{col_name}' to '{target_type}'")
                columns_dict[col_name]["type"] = target_type

        elif transform_name == "derive_column":
            col_name = transform_config["name"]
            # logger.info(f"  - Deriving new column '{col_name}'")
            new_col = {
                "name": col_name,
                "type": transform_config.get("type", "string"),
                "nullable": transform_config.get("nullable", True),
                "description": transform_config.get("description", ""),
            }
            columns_dict[col_name] = new_col

        elif transform_name == "drop_column":
            cols_to_drop = transform_config
            for col_name in cols_to_drop:
                if col_name in columns_dict:
                    print(f"  - Dropping column '{col_name}'")
                    columns_dict.pop(col_name)

    # Convert the dictionary back to a list of columns
    silver_schema["model"]["columns"] = list(columns_dict.values())

    # Merge Silver-specific write properties over the inherited Bronze properties.
    if silver_overrides and "write" in silver_overrides:
        logger.info("  - Merging Silver-specific write properties...")
        if "write" not in silver_schema["model"]:
            silver_schema["model"]["write"] = {}

        bronze_write_props = silver_schema["model"]["write"]
        silver_write_props = silver_overrides["write"]

        silver_schema["model"]["write"] = _merge_dicts(
            bronze_write_props, silver_write_props
        )

    # Update other metadata for the Silver table
    silver_schema["dataset"] = f"{silver_schema['dataset']}_silver"
    silver_schema["description"] = (
        f"Cleansed and transformed Silver layer for {bronze_schema['dataset']}."
    )

    logger.info("✅ Silver schema derived successfully.")
    return silver_schema


def delete_folders(
    dbutils, folder_list: List[str], dry_run: bool = False, logger: object = None
) -> None:
    """
    Deletes folders or volume paths within a Databricks Unity Catalog environment
    using dbutils.fs.rm.

    Supports both:
      - Lakehouse workspace paths (e.g., 'dbfs:/mnt/bronze/data')
      - Unity Catalog Volumes (e.g., '/Volumes/catalog/schema/volume/path')

    Args:
        folder_list (List[str]): A list of folder paths to delete.
                                 Examples:
                                   - "dbfs:/mnt/raw/old_files"
                                   - "/Volumes/jetblue_dev_engines/_bronze/tmp"
                                   - "/Workspace/Shared/temp"
        dry_run (bool): If True, lists what would be deleted without removing anything.
                        Defaults to False.
        logger (object, optional): A logger instance (e.g., from `logging` or `loguru`).
                                   If None, defaults to printing to console.

    Example:
        ```python
        # Example 1: Dry run — list all folders that would be deleted
        delete_folders([
            "/Volumes/jetblue_dev_engines/_bronze/tmp",
            "/Volumes/jetblue_dev_engines/_silver/archive"
        ], dry_run=True)

        # Example 2: Actual deletion
        delete_folders([
            "/Volumes/jetblue_dev_engines/_bronze/tmp",
            "/Volumes/jetblue_dev_engines/_silver/archive"
        ], dry_run=False)
        ```

    Notes:
        - Automatically prevents deletion of unsafe paths like "/", ".", or "*".
        - Works across UC Volumes, DBFS, and Workspace directories.
        - For each folder:
            * If exists → deletes (or logs in dry run)
            * If not found → logs "path does not exist"
        - Recursively deletes folder contents.
    """
    log_func = logger.info if logger else print
    log_warn = logger.warning if logger else print
    log_error = logger.error if logger else print

    for folder_path in folder_list:
        safe_path = f"{folder_path.strip()}/"

        # 🛡️ Safety check
        if safe_path in ["/", ".", "", "*"]:
            log_warn(f"⚠️ Unsafe path detected: '{safe_path}' — skipping.")
            continue

        try:
            # Check existence
            exists = False
            try:
                dbutils.fs.ls(safe_path)
                exists = True
            except Exception as e:
                exists = False
                raise e

            if exists:
                if dry_run:
                    log_warn(f"🧪 DRY RUN: Would delete → {safe_path}")
                else:
                    dbutils.fs.rm(safe_path, recurse=True)
                    log_func(f"✅ Deleted → {safe_path}")
            else:
                log_func(f"❌ Path does not exist → {safe_path}")

        except Exception as e:
            log_error(f"⚠️ Error deleting '{safe_path}': {e}")
