from typing import List, Dict, Any, Tuple
import json


def validate_schema(schema: Dict[str, Any], primary_key_nullable: bool = True) -> Tuple[bool, List[str]]:
    """
    Validates the structure and rules of a dataset schema dictionary,
    including validation of data types against Polars/Arrow standards.

    Args:
        schema (Dict[str, Any]): The dataset schema dictionary to validate.
        primary_key_nullable (bool): Whether the primary key can be nullable.

    Returns:
        Tuple[bool, List[str]]: A tuple containing a boolean indicating validity
                                 and a list of validation error messages.

    Raises:
        Exception: If the schema is found to be invalid.

    Example:
        sample_schema = {
            'dataset': 'market_ohlcv_master',
            'version': 1.0,
            'primary_key': ['time_bucket', 'internal_symbol', 'timeframe'],
            'model': {
                'columns': [
                    {'name': 'time_bucket', 'type': 'TIMESTAMP', 'nullable': False},
                    {'name': 'internal_symbol', 'type': 'STRING', 'nullable': False},
                    {'name': 'volume', 'type': 'FLOAT64', 'nullable': False} # Polars type
                ],
                'write': {'format': 'delta', 'cluster_by': ['internal_symbol'], 'options': {}}
            }
        }
        try:
            is_valid, errors = validate_schema(sample_schema, primary_key_nullable=False)
            print(f"Schema is valid: {is_valid}")
        except Exception as e:
            print(f"Validation failed: {e}")
    """
    errors: List[str] = []

    # --- Polars/Arrow Standardized Types (Extended List) ---
    # We use the common uppercase convention for consistency across Spark and Polars DDL inputs.
    allowed_types = {
        "STRING",
        "BOOLEAN",
        "DATE",
        "TIMESTAMP",
        "DATETIME",
        "INT32",
        "INT64",
        "FLOAT",
        "FLOAT32",
        "FLOAT64",
        "DECIMAL",
        "DOUBLE",
        "INT",
        "BIGINT",
    }

    # Top-level checks
    if not isinstance(schema, dict):
        return False, ["schema must be a dict"]

    # dataset
    if "dataset" not in schema:
        errors.append("missing top-level key: 'dataset'")
    elif not isinstance(schema["dataset"], str) or not schema["dataset"]:
        errors.append("'dataset' must be a non-empty string")

    # version
    if "version" not in schema:
        errors.append("missing top-level key: 'version'")
    elif not isinstance(schema["version"], (int, float)):
        errors.append("'version' must be an int or float")

    # primary_key (Allow list of strings for composite primary keys, e.g., market_ohlcv_master)
    if "primary_key" not in schema:
        errors.append("missing top-level key: 'primary_key'")
    elif not isinstance(schema["primary_key"], (str, list)) or (
        isinstance(schema["primary_key"], str) and not schema["primary_key"]
    ):
        errors.append(
            "'primary_key' must be a non-empty string (single) or a non-empty list of strings (composite)"
        )

    # model
    model = schema.get("model")
    if model is None:
        errors.append("missing top-level key: 'model'")
        return False, errors

    if not isinstance(model, dict):
        errors.append("'model' must be a dict")
        return False, errors

    # columns
    columns = model.get("columns")
    if columns is None:
        errors.append("model missing 'columns' key")
        return False, errors

    if not isinstance(columns, list) or not columns:
        errors.append("'model.columns' must be a non-empty list")
        return False, errors

    seen_names = set()
    column_names = []

    # --- Start Column-Level Validation ---
    for idx, col in enumerate(columns):
        prefix = f"model.columns[{idx}]"
        if not isinstance(col, dict):
            errors.append(f"{prefix} must be a dict")
            continue

        # name
        name = col.get("name")
        if not isinstance(name, str) or not name:
            errors.append(f"{prefix} missing or invalid 'name'")
            continue
        if name in seen_names:
            errors.append(f"duplicate column name: '{name}'")
        seen_names.add(name)
        column_names.append(name)

        # type (UPDATED CHECK)
        ctype = col.get("type")
        if ctype is None:
            errors.append(f"{prefix} ('{name}') missing 'type'")
        # Normalize type to uppercase for checking against the allowed set
        elif not isinstance(ctype, str) or ctype.upper() not in allowed_types:
            errors.append(
                f"{prefix} ('{name}') has invalid 'type': {ctype!r}. Allowed: {sorted(allowed_types)}"
            )

        # nullable
        if "nullable" not in col:
            errors.append(
                f"{prefix} ('{name}') missing 'nullable' (must be True/False)"
            )
        elif not isinstance(col["nullable"], bool):
            errors.append(f"{prefix} ('{name}') 'nullable' must be a boolean")

        # maxLength (Only applicable to STRING)
        if "maxLength" in col:
            if not isinstance(col["maxLength"], int) or col["maxLength"] <= 0:
                errors.append(f"{prefix} ('{name}') 'maxLength' must be a positive int")
            # Check if the type is a string variant (case-insensitive)
            elif col.get("type", "").upper() not in {"STRING", "VARCHAR"}:
                errors.append(
                    f"{prefix} ('{name}') 'maxLength' is only applicable to type 'STRING'"
                )

        # description
        if "description" in col and not isinstance(col["description"], str):
            errors.append(f"{prefix} ('{name}') 'description' must be a string")

    # --- Start Cross-Reference Validation ---

    # primary_key existence and not nullable
    pks = schema.get("primary_key")
    if isinstance(pks, str):
        pks = [pks]  # Convert single key to list

    if isinstance(pks, list):
        for pk in pks:
            if pk not in column_names:
                errors.append(f"primary_key column '{pk}' not found in model.columns")
            else:
                # Find the column definition and ensure not nullable
                col_def = next((col for col in columns if col.get("name") == pk), None)
                
                if primary_key_nullable == False and col_def and col_def.get("nullable") is True:
                    errors.append(f"primary_key column '{pk}' must not be nullable")

    # write section
    write = model.get("write")
    if write is None:
        errors.append("model missing 'write' key")
    elif not isinstance(write, dict):
        errors.append("model.write must be a dict")
    else:
        fmt = write.get("format")
        if fmt is None:
            errors.append("model.write missing 'format'")
        elif not isinstance(fmt, str):
            errors.append("model.write.format must be a string")
        else:
            allowed_formats = {"delta", "parquet", "csv", "json"}
            if fmt not in allowed_formats:
                errors.append(
                    f"model.write.format '{fmt}' not in allowed formats {sorted(allowed_formats)}"
                )

        # cluster_by (Checked against column_names)
        cluster_by = write.get("cluster_by", [])
        if not isinstance(cluster_by, list):
            errors.append("model.write.cluster_by must be a list")
        else:
            for p in cluster_by:
                if not isinstance(p, str):
                    errors.append(f"cluster_by entry must be a string, got {p!r}")
                elif p not in column_names:
                    errors.append(f"cluster_by column '{p}' not found in model.columns")

        # options (Checked against data_skipping_stats_columns)
        options = write.get("options", {})
        if not isinstance(options, dict):
            errors.append("model.write.options must be a dict")

        data_skipping_stats_columns = options.get("delta.dataSkippingStatsColumns", [])
        if not isinstance(data_skipping_stats_columns, list):
            errors.append(
                "model.write.options['delta.dataSkippingStatsColumns'] must be a list"
            )
        else:
            for p in data_skipping_stats_columns:
                if not isinstance(p, str):
                    errors.append(
                        f"data_skipping_stats_columns entry must be a string, got {p!r}"
                    )
                elif p not in column_names:
                    errors.append(
                        f"data_skipping_stats_columns column '{p}' not found in model.columns"
                    )

    # --- Final Result ---
    is_valid = len(errors) == 0
    if not is_valid:
        raise Exception(
            f"Schema provided is not valid. Errors: {json.dumps(errors, indent=2)}"
        )

    return is_valid, errors
