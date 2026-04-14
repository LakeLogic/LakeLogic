"""
DDL generation for LakeLogic.

Generates CREATE TABLE, ALTER TABLE, and DROP TABLE statements from
DataContract schema definitions, targeting multiple backends.

Supports: Spark/Databricks, DuckDB, SQLite, Snowflake, BigQuery, PostgreSQL.

Usage (Python API):
    from lakelogic.core.ddl import generate_ddl, create_table

    ddl = generate_ddl(contract, backend="spark")
    create_table(contract, backend="duckdb", db_path="warehouse.duckdb")

Usage (CLI):
    lakelogic init-tables ./contracts/ --backend spark --dry-run
    lakelogic init-tables ./contracts/orders.yaml --backend duckdb
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

from lakelogic.core.models import DataContract, FieldDefinition, Materialization


# ── Type Mappings ────────────────────────────────────────────────────────────
# Contract types → backend-specific SQL types.
# Contract types are intentionally simple (string, int, float, boolean, etc.)
# and mapped to the most appropriate backend type.

_TYPE_MAP: Dict[str, Dict[str, str]] = {
    # contract_type → {backend: sql_type}
    "string": {
        "spark": "STRING",
        "databricks": "STRING",
        "duckdb": "VARCHAR",
        "sqlite": "TEXT",
        "snowflake": "VARCHAR",
        "bigquery": "STRING",
        "postgresql": "TEXT",
    },
    "varchar": {
        "spark": "STRING",
        "databricks": "STRING",
        "duckdb": "VARCHAR",
        "sqlite": "TEXT",
        "snowflake": "VARCHAR",
        "bigquery": "STRING",
        "postgresql": "VARCHAR",
    },
    "text": {
        "spark": "STRING",
        "databricks": "STRING",
        "duckdb": "VARCHAR",
        "sqlite": "TEXT",
        "snowflake": "VARCHAR",
        "bigquery": "STRING",
        "postgresql": "TEXT",
    },
    "int": {
        "spark": "INT",
        "databricks": "INT",
        "duckdb": "INTEGER",
        "sqlite": "INTEGER",
        "snowflake": "INTEGER",
        "bigquery": "INT64",
        "postgresql": "INTEGER",
    },
    "integer": {
        "spark": "INT",
        "databricks": "INT",
        "duckdb": "INTEGER",
        "sqlite": "INTEGER",
        "snowflake": "INTEGER",
        "bigquery": "INT64",
        "postgresql": "INTEGER",
    },
    "bigint": {
        "spark": "BIGINT",
        "databricks": "BIGINT",
        "duckdb": "BIGINT",
        "sqlite": "INTEGER",
        "snowflake": "BIGINT",
        "bigquery": "INT64",
        "postgresql": "BIGINT",
    },
    "long": {
        "spark": "BIGINT",
        "databricks": "BIGINT",
        "duckdb": "BIGINT",
        "sqlite": "INTEGER",
        "snowflake": "BIGINT",
        "bigquery": "INT64",
        "postgresql": "BIGINT",
    },
    "smallint": {
        "spark": "SMALLINT",
        "databricks": "SMALLINT",
        "duckdb": "SMALLINT",
        "sqlite": "INTEGER",
        "snowflake": "SMALLINT",
        "bigquery": "INT64",
        "postgresql": "SMALLINT",
    },
    "tinyint": {
        "spark": "TINYINT",
        "databricks": "TINYINT",
        "duckdb": "TINYINT",
        "sqlite": "INTEGER",
        "snowflake": "TINYINT",
        "bigquery": "INT64",
        "postgresql": "SMALLINT",
    },
    "float": {
        "spark": "FLOAT",
        "databricks": "FLOAT",
        "duckdb": "FLOAT",
        "sqlite": "REAL",
        "snowflake": "FLOAT",
        "bigquery": "FLOAT64",
        "postgresql": "REAL",
    },
    "double": {
        "spark": "DOUBLE",
        "databricks": "DOUBLE",
        "duckdb": "DOUBLE",
        "sqlite": "REAL",
        "snowflake": "DOUBLE",
        "bigquery": "FLOAT64",
        "postgresql": "DOUBLE PRECISION",
    },
    "boolean": {
        "spark": "BOOLEAN",
        "databricks": "BOOLEAN",
        "duckdb": "BOOLEAN",
        "sqlite": "INTEGER",
        "snowflake": "BOOLEAN",
        "bigquery": "BOOL",
        "postgresql": "BOOLEAN",
    },
    "bool": {
        "spark": "BOOLEAN",
        "databricks": "BOOLEAN",
        "duckdb": "BOOLEAN",
        "sqlite": "INTEGER",
        "snowflake": "BOOLEAN",
        "bigquery": "BOOL",
        "postgresql": "BOOLEAN",
    },
    "date": {
        "spark": "DATE",
        "databricks": "DATE",
        "duckdb": "DATE",
        "sqlite": "TEXT",
        "snowflake": "DATE",
        "bigquery": "DATE",
        "postgresql": "DATE",
    },
    "timestamp": {
        "spark": "TIMESTAMP",
        "databricks": "TIMESTAMP",
        "duckdb": "TIMESTAMP",
        "sqlite": "TEXT",
        "snowflake": "TIMESTAMP_NTZ",
        "bigquery": "TIMESTAMP",
        "postgresql": "TIMESTAMP",
    },
    "timestamp_ntz": {
        "spark": "TIMESTAMP_NTZ",
        "databricks": "TIMESTAMP_NTZ",
        "duckdb": "TIMESTAMP",
        "sqlite": "TEXT",
        "snowflake": "TIMESTAMP_NTZ",
        "bigquery": "TIMESTAMP",
        "postgresql": "TIMESTAMP WITHOUT TIME ZONE",
    },
    "timestamp_tz": {
        "spark": "TIMESTAMP",
        "databricks": "TIMESTAMP",
        "duckdb": "TIMESTAMPTZ",
        "sqlite": "TEXT",
        "snowflake": "TIMESTAMP_TZ",
        "bigquery": "TIMESTAMP",
        "postgresql": "TIMESTAMP WITH TIME ZONE",
    },
    "binary": {
        "spark": "BINARY",
        "databricks": "BINARY",
        "duckdb": "BLOB",
        "sqlite": "BLOB",
        "snowflake": "BINARY",
        "bigquery": "BYTES",
        "postgresql": "BYTEA",
    },
    "json": {
        "spark": "STRING",
        "databricks": "STRING",
        "duckdb": "JSON",
        "sqlite": "TEXT",
        "snowflake": "VARIANT",
        "bigquery": "JSON",
        "postgresql": "JSONB",
    },
    "array": {
        "spark": "ARRAY<STRING>",
        "databricks": "ARRAY<STRING>",
        "duckdb": "VARCHAR[]",
        "sqlite": "TEXT",
        "snowflake": "ARRAY",
        "bigquery": "ARRAY<STRING>",
        "postgresql": "TEXT[]",
    },
}

# Backends that support PARTITIONED BY / PARTITION BY
_PARTITION_BACKENDS = {"spark", "databricks", "bigquery"}

# Backends that support CLUSTER BY
_CLUSTER_BACKENDS = {"spark", "databricks", "bigquery", "snowflake"}

# Table formats per backend
_DEFAULT_FORMATS = {
    "spark": "DELTA",
    "databricks": "DELTA",
}

# ── Safe Type Widening ───────────────────────────────────────────────────────
# Defines lossless type promotions that can be auto-applied without data loss.
# Keys are normalised base types; values are sets of safe target types.

_SAFE_WIDENINGS: Dict[str, set] = {
    "tinyint": {"smallint", "int", "integer", "bigint", "long", "float", "double"},
    "smallint": {"int", "integer", "bigint", "long", "float", "double"},
    "int": {"bigint", "long", "double"},
    "integer": {"bigint", "long", "double"},
    "bigint": {"double"},
    "long": {"double"},
    "float": {"double"},
    "boolean": {"int", "integer", "bigint", "long", "string", "varchar", "text"},
    "bool": {"int", "integer", "bigint", "long", "string", "varchar", "text"},
    "date": {"timestamp", "timestamp_ntz", "timestamp_tz"},
    "varchar": {"text", "string"},
    "char": {"varchar", "text", "string"},
}


def _normalize_base_type(sql_type: str) -> str:
    """
    Extract the base type name from a potentially parameterised SQL type
    and map it to a canonical synonym for comparison.

    Examples:
        VARCHAR(255) → varchar
        STRING → varchar
        DECIMAL(10,2) → decimal
        INT → integer
    """
    import re
    m = re.match(r"^(\w+)", sql_type.strip())
    base = m.group(1).lower() if m else sql_type.strip().lower()

    _synonyms = {
        "string": "varchar",
        "text": "varchar",
        "int": "integer",
        "int4": "integer",
        "int8": "bigint",
        "long": "bigint",
        "float4": "float",
        "float8": "double",
        "bool": "boolean",
    }
    return _synonyms.get(base, base)


def _extract_varchar_length(sql_type: str) -> Optional[int]:
    """Extract length from VARCHAR(N) / CHAR(N). Returns None if unbounded."""
    import re
    m = re.match(r"^(?:var)?char\s*\((\d+)\)", sql_type.strip(), re.IGNORECASE)
    return int(m.group(1)) if m else None


def is_safe_widening(from_type: str, to_type: str) -> bool:
    """
    Check whether a type change is a safe (lossless) widening.

    Handles both base type promotions (int → bigint) and
    parameterised widenings (varchar(50) → varchar(255)).

    Args:
        from_type: Current SQL type (e.g. 'INT', 'VARCHAR(50)').
        to_type: Desired SQL type (e.g. 'BIGINT', 'VARCHAR(255)').

    Returns:
        True if the change is lossless.
    """
    from_base = _normalize_base_type(from_type)
    to_base = _normalize_base_type(to_type)

    # Same base type — check parameterised widening (varchar(50) → varchar(255))
    if from_base == to_base:
        if from_base in ("varchar", "char"):
            from_len = _extract_varchar_length(from_type)
            to_len = _extract_varchar_length(to_type)
            # Unbounded → unbounded is a no-op; bounded → unbounded is safe
            if from_len is None and to_len is None:
                return True  # no-op
            if from_len is None and to_len is not None:
                return False  # restricting an unbounded string is a narrowing!
            if to_len is None:
                return True  # removing length constraint is safe
            return to_len >= from_len
        # Same base, same type — no change needed
        return True

    # Check the widening map
    allowed = _SAFE_WIDENINGS.get(from_base, set())
    return to_base in allowed


def _resolve_type(contract_type: str, backend: str) -> str:
    """
    Resolve a contract field type to a backend-specific SQL type.

    Handles parameterised types like decimal(10,2) and varchar(255).

    Args:
        contract_type: Type from the contract FieldDefinition.
        backend: Target backend name.

    Returns:
        SQL type string for the backend.
    """
    raw = contract_type.strip()
    lower = raw.lower()

    # Handle parameterised types: decimal(10,2), numeric(p,s), varchar(n)
    import re

    param_match = re.match(r"^(\w+)\((.+)\)$", lower)
    if param_match:
        base_type = param_match.group(1)
        params = param_match.group(2)

        if base_type in ("decimal", "numeric"):
            backend_base = {
                "spark": "DECIMAL",
                "databricks": "DECIMAL",
                "duckdb": "DECIMAL",
                "sqlite": "REAL",
                "snowflake": "NUMBER",
                "bigquery": "NUMERIC",
                "postgresql": "NUMERIC",
            }.get(backend, "DECIMAL")
            if backend == "sqlite":
                return backend_base  # SQLite ignores precision
            return f"{backend_base}({params})"

        if base_type in ("varchar", "char"):
            backend_base = {
                "spark": "STRING",
                "databricks": "STRING",
                "duckdb": "VARCHAR",
                "sqlite": "TEXT",
                "snowflake": "VARCHAR",
                "bigquery": "STRING",
                "postgresql": "VARCHAR",
            }.get(backend, "VARCHAR")
            if backend in ("spark", "databricks", "sqlite", "bigquery", "polars", "pandas", "python"):
                return backend_base  # These ignore length constraints
            return f"{backend_base}({params})"

        # Pass through unknown parameterised types
        return raw.upper()

    # Exact match in type map
    mapping = _TYPE_MAP.get(lower)
    if mapping:
        return mapping.get(backend, raw.upper())

    # Unknown type — pass through as-is (user may have used a native type)
    logger.debug(f"Unknown contract type '{contract_type}' for backend '{backend}', passing through as-is.")
    return raw.upper()


def _resolve_table_name(contract: DataContract) -> Optional[str]:
    """
    Extract the target table name from the contract.

    Resolution order:
      1. materialization.target_path (table:...)
      2. server.path (table:...)
      3. info.table_name (explicit table name from contract)
      4. dataset (contract dataset identifier)
      5. info.title (sanitised fallback)

    Args:
        contract: DataContract instance.

    Returns:
        Table name string or None.
    """
    mat = contract.materialization
    if mat and mat.target_path:
        target = str(mat.target_path)
        if target.startswith("table:"):
            return target[6:]

    if contract.server and contract.server.path:
        target = str(contract.server.path)
        if target.startswith("table:"):
            return target[6:]

    # info.table_name — explicit table name (may have been template-resolved)
    if contract.info and getattr(contract.info, "table_name", None):
        return contract.info.table_name

    # Fallback: use dataset name
    if contract.dataset:
        return contract.dataset

    # Last resort: sanitize title for table name
    if contract.info and contract.info.title:
        import re
        return re.sub(r"[^a-zA-Z0-9_]", "_", contract.info.title).lower()

    return None


def _get_fields(contract: DataContract) -> List[FieldDefinition]:
    """Extract field definitions from the contract model."""
    if contract.model and contract.model.fields:
        return contract.model.fields
    return []


# ── Public API ───────────────────────────────────────────────────────────────


def generate_ddl(
    contract: DataContract,
    backend: str,
    *,
    table_name: Optional[str] = None,
    if_not_exists: bool = True,
    include_comments: bool = True,
) -> str:
    """
    Generate a CREATE TABLE DDL statement from a DataContract.

    Args:
        contract: DataContract with schema model.
        backend: Target backend (spark, databricks, duckdb, sqlite,
                 snowflake, bigquery, postgresql).
        table_name: Override table name (defaults to contract target).
        if_not_exists: Include IF NOT EXISTS clause.
        include_comments: Include column comments/descriptions.

    Returns:
        SQL DDL string.

    Raises:
        ValueError: If no fields or table name can be resolved.
    """
    backend = backend.lower()
    fields = _get_fields(contract)
    if not fields:
        raise ValueError(
            "Cannot generate DDL: contract has no model.fields defined. "
            "Add a 'model' section with field definitions to your contract."
        )

    resolved_table = table_name or _resolve_table_name(contract)
    if not resolved_table:
        raise ValueError(
            "Cannot generate DDL: no table name resolved. "
            "Set materialization.target_path to 'table:schema.table_name' or provide table_name parameter."
        )

    primary_key = list(contract.primary_key or [])
    mat = contract.materialization or Materialization()
    partition_by = list(mat.partition_by or [])
    cluster_by = list(mat.cluster_by or [])

    # Build column definitions
    col_defs = []
    for field in fields:
        sql_type = _resolve_type(field.type, backend)
        nullable = "" if not field.required else " NOT NULL"

        # Column comment
        comment = ""
        if (
            include_comments
            and field.description
            and backend in ("spark", "databricks", "snowflake", "bigquery", "postgresql")
        ):
            # Spark's parser does NOT handle '' escaping inside inline
            # COMMENT clauses, so strip single quotes for Spark/Databricks.
            if backend in ("spark", "databricks"):
                escaped = field.description.replace("'", "")
            else:
                escaped = field.description.replace("'", "''")
            if backend in ("spark", "databricks"):
                comment = f" COMMENT '{escaped}'"
            # For other backends, comments are added post-CREATE

        col_def = f"  {field.name} {sql_type}{nullable}{comment}"

        # PII marker as comment for documentation
        if field.pii and include_comments:
            if backend in ("spark", "databricks") and not comment:
                col_def += " COMMENT 'PII'"
            elif backend in ("spark", "databricks"):
                pass  # Already has a comment
            else:
                col_def += " /* PII */"

        col_defs.append(col_def)

    # ── LakeLogic system columns (when lineage is enabled) ──────────────────
    lineage_cfg = getattr(contract, "lineage", None)
    if lineage_cfg and getattr(lineage_cfg, "enabled", False):
        _sys_cols = []
        if getattr(lineage_cfg, "capture_source_path", True):
            _sys_cols.append((getattr(lineage_cfg, "source_column_name", "_lakelogic_source"), "string"))
        if getattr(lineage_cfg, "capture_timestamp", True):
            _sys_cols.append((getattr(lineage_cfg, "timestamp_column_name", "_lakelogic_processed_at"), "timestamp"))
        if getattr(lineage_cfg, "capture_run_id", True):
            _sys_cols.append((getattr(lineage_cfg, "run_id_column_name", "_lakelogic_run_id"), "string"))
        if getattr(lineage_cfg, "capture_contract_name", False):
            _sys_cols.append((getattr(lineage_cfg, "contract_name_column_name", "_lakelogic_contract_name"), "string"))
        if getattr(lineage_cfg, "capture_domain", True):
            _sys_cols.append((getattr(lineage_cfg, "domain_column_name", "_lakelogic_domain"), "string"))
        if getattr(lineage_cfg, "capture_system", True):
            _sys_cols.append((getattr(lineage_cfg, "system_column_name", "_lakelogic_system"), "string"))
        if getattr(lineage_cfg, "capture_created_at", True):
            _sys_cols.append((getattr(lineage_cfg, "created_at_column_name", "_lakelogic_created_at"), "timestamp"))
        if getattr(lineage_cfg, "capture_created_by", True):
            _sys_cols.append((getattr(lineage_cfg, "created_by_column_name", "_lakelogic_created_by"), "string"))

        # Deduplicate against user-defined fields
        existing_names = {f.name for f in fields}
        for col_name, col_type in _sys_cols:
            if col_name not in existing_names:
                sql_type = _resolve_type(col_type, backend)
                col_defs.append(f"  {col_name} {sql_type}")

    # ── Soft-delete system columns ──────────────────────────────────────────
    if mat:
        existing_names = {f.name for f in fields}
        _sd_cols = []
        sd_col = getattr(mat, "soft_delete_column", None)
        sd_time_col = getattr(mat, "soft_delete_time_column", None)
        sd_reason_col = getattr(mat, "soft_delete_reason_column", None)
        if sd_col:
            _sd_cols.append((sd_col, "boolean"))
        if sd_time_col:
            _sd_cols.append((sd_time_col, "string"))
        if sd_reason_col:
            _sd_cols.append((sd_reason_col, "string"))
        for col_name, col_type in _sd_cols:
            if col_name not in existing_names:
                sql_type = _resolve_type(col_type, backend)
                col_defs.append(f"  {col_name} {sql_type}")

    # Primary key constraint
    if primary_key:
        if backend in ("duckdb", "postgresql", "sqlite"):
            pk_name = f"pk_{resolved_table.replace('.', '_')}"
            pk_cols = ", ".join(primary_key)
            col_defs.append(f"  CONSTRAINT {pk_name} PRIMARY KEY ({pk_cols})")
        elif backend == "snowflake":
            pk_cols = ", ".join(primary_key)
            col_defs.append(f"  PRIMARY KEY ({pk_cols})")
        # Spark/Databricks/BigQuery don't enforce PK constraints in DDL

    columns_sql = ",\n".join(col_defs)

    # IF NOT EXISTS
    exists_clause = " IF NOT EXISTS" if if_not_exists else ""

    # Build CREATE TABLE
    ddl = f"CREATE TABLE{exists_clause} {resolved_table} (\n{columns_sql}\n)"

    # Table format (Spark/Databricks: USING DELTA)
    table_format = None
    if mat.format:
        table_format = mat.format.upper()
    elif backend in _DEFAULT_FORMATS:
        table_format = _DEFAULT_FORMATS[backend]

    if table_format and backend in ("spark", "databricks"):
        ddl += f"\nUSING {table_format}"

    # PARTITIONED BY
    if partition_by and backend in _PARTITION_BACKENDS:
        part_cols = ", ".join(partition_by)
        if backend == "bigquery":
            # BigQuery uses PARTITION BY with expressions
            if len(partition_by) == 1:
                ddl += f"\nPARTITION BY {partition_by[0]}"
            else:
                ddl += f"\nPARTITION BY {part_cols}"
        else:
            ddl += f"\nPARTITIONED BY ({part_cols})"

    # CLUSTER BY / CLUSTERED BY
    if cluster_by and backend in _CLUSTER_BACKENDS:
        cluster_cols = ", ".join(cluster_by)
        if backend == "bigquery":
            ddl += f"\nCLUSTER BY {cluster_cols}"
        elif backend in ("spark", "databricks"):
            ddl += f"\nCLUSTERED BY ({cluster_cols}) INTO 32 BUCKETS"
        elif backend == "snowflake":
            ddl += f"\nCLUSTER BY ({cluster_cols})"

    # TBLPROPERTIES (Spark/Databricks)
    table_props = getattr(mat, "table_properties", None) or {}
    if backend in ("spark", "databricks"):
        props_dict = dict(table_props)
        if "delta.enableDeletionVectors" not in props_dict:
            props_dict["delta.enableDeletionVectors"] = "false"
        props = ", ".join(
            f"'{k}' = {v if str(v).lower() in ('true', 'false') else f'{v}'}" for k, v in props_dict.items()
        )
        ddl += f"\nTBLPROPERTIES ({props})"

    # LOCATION (Spark/Databricks external tables)
    ext_location = getattr(mat, "location", None)
    if ext_location and backend in ("spark", "databricks"):
        ddl += f"\nLOCATION '{ext_location}'"

    ddl += ";"

    # Add table-level comment for Snowflake / PostgreSQL
    if include_comments and contract.info and contract.info.description:
        escaped = contract.info.description.replace("'", "''")
        if backend == "snowflake":
            ddl += f"\n\nCOMMENT ON TABLE {resolved_table} IS '{escaped}';"
        elif backend == "postgresql":
            ddl += f"\n\nCOMMENT ON TABLE {resolved_table} IS '{escaped}';"
        elif backend == "bigquery":
            # BigQuery uses OPTIONS in ALTER
            ddl += f"\n\nALTER TABLE {resolved_table} SET OPTIONS (description='{escaped}');"

    # Add column-level comments for backends that need separate statements
    if include_comments and backend in ("snowflake", "postgresql"):
        for field in fields:
            if field.description:
                escaped = field.description.replace("'", "''")
                ddl += f"\nCOMMENT ON COLUMN {resolved_table}.{field.name} IS '{escaped}';"

    return ddl


def generate_drop_ddl(
    contract: DataContract,
    backend: str,
    *,
    table_name: Optional[str] = None,
    if_exists: bool = True,
) -> str:
    """
    Generate a DROP TABLE DDL statement.

    Args:
        contract: DataContract instance.
        backend: Target backend.
        table_name: Override table name.
        if_exists: Include IF EXISTS clause.

    Returns:
        SQL DROP TABLE string.
    """
    backend = backend.lower()
    resolved_table = table_name or _resolve_table_name(contract)
    if not resolved_table:
        raise ValueError("Cannot generate DROP DDL: no table name resolved.")

    exists_clause = " IF EXISTS" if if_exists else ""
    return f"DROP TABLE{exists_clause} {resolved_table};"


def generate_alter_ddl(
    contract: DataContract,
    backend: str,
    existing_columns: List[str],
    *,
    existing_column_types: Optional[Dict[str, str]] = None,
    table_name: Optional[str] = None,
) -> List[str]:
    """
    Generate ALTER TABLE statements for schema evolution.

    Handles three categories of schema change:

    1. **New columns** — emits ``ALTER TABLE ADD COLUMN``.
    2. **Safe type widenings** — emits ``ALTER COLUMN TYPE`` for lossless
       promotions (e.g. INT → BIGINT, VARCHAR(50) → VARCHAR(255)).
    3. **Unsafe type changes** — logs a WARNING and skips.
    4. **Removed columns** — logs an INFO and skips (physical column stays;
       runtime ``schema_policy`` handles pruning).

    Args:
        contract: DataContract with updated schema.
        backend: Target backend.
        existing_columns: List of column names already in the table.
        existing_column_types: Optional mapping of column name → current SQL
            type string. When provided, enables type-change detection.
        table_name: Override table name.

    Returns:
        List of ALTER TABLE SQL statements (safe operations only).
    """
    backend = backend.lower()
    fields = _get_fields(contract)
    resolved_table = table_name or _resolve_table_name(contract)
    if not resolved_table:
        raise ValueError("Cannot generate ALTER DDL: no table name resolved.")

    existing_set = {c.lower() for c in existing_columns}
    expected_set = {f.name.lower() for f in fields}
    existing_types_lower = (
        {k.lower(): v for k, v in existing_column_types.items()}
        if existing_column_types
        else {}
    )
    statements: List[str] = []

    # ── Resolve Evolution Policy ──────────────────────────────────────────
    server = contract.effective_server() if hasattr(contract, "effective_server") else None
    from lakelogic.core.models import SchemaPolicy as _SP
    _default_evo = _SP().evolution
    evolution = _default_evo
    if server and getattr(server, "schema_policy", None):
        evolution = getattr(server.schema_policy, "evolution", _default_evo) or _default_evo
    evolution = str(evolution).lower()

    # ── 1. New columns ────────────────────────────────────────────────────
    for field in fields:
        if field.name.lower() not in existing_set:
            if evolution == "strict":
                raise ValueError(
                    f"Schema evolution error: New column '{field.name}' detected in contract "
                    f"but not in target table '{resolved_table}'. "
                    f"Schema evolution policy is 'strict'."
                )
            
            sql_type = _resolve_type(field.type, backend)
            if backend in ("duckdb",):
                statements.append(
                    f"ALTER TABLE {resolved_table} ADD COLUMN IF NOT EXISTS {field.name} {sql_type};"
                )
            else:
                statements.append(
                    f"ALTER TABLE {resolved_table} ADD COLUMN {field.name} {sql_type};"
                )
            logger.info(
                f"Schema evolution: ADD COLUMN {field.name} {sql_type} → {resolved_table}"
            )

    # ── 2. Type changes (requires existing_column_types) ──────────────────
    if existing_types_lower:
        for field in fields:
            col_lower = field.name.lower()
            if col_lower not in existing_types_lower:
                continue  # new column — already handled above

            current_sql_type = existing_types_lower[col_lower]
            desired_sql_type = _resolve_type(field.type, backend)

            # Intelligent type equivalence check
            # Treat synonyms with identical parameters (like VARCHAR / STRING) as equal
            base_cur = _normalize_base_type(current_sql_type)
            base_des = _normalize_base_type(desired_sql_type)

            # Strip base to compare parameters, e.g. "(10,2)" vs "(10,2)"
            import re
            p_cur = re.sub(r"^[a-zA-Z0-9_]+", "", current_sql_type.strip()).replace(" ", "")
            p_des = re.sub(r"^[a-zA-Z0-9_]+", "", desired_sql_type.strip()).replace(" ", "")

            is_identical = False
            if current_sql_type.upper().strip() == desired_sql_type.upper().strip():
                is_identical = True
            elif base_cur == base_des and p_cur == p_des:
                is_identical = True
            # Delta/PyArrow backends don't enforce string lengths — treat
            # VARCHAR vs VARCHAR(N) as identical when targeting Delta tables.
            elif base_cur == base_des and base_cur in ("varchar", "char", "string", "text"):
                if backend in ("polars", "pandas", "python", "duckdb"):
                    is_identical = True

            if is_identical:
                continue  # Types are identical (or synonymous) — skip DDL generation

            if evolution == "strict":
                raise ValueError(
                    f"Schema evolution error: Type mismatch for '{field.name}' "
                    f"({current_sql_type} → {desired_sql_type}) in target table '{resolved_table}'. "
                    f"Schema evolution policy is 'strict'."
                )

            if is_safe_widening(current_sql_type, desired_sql_type):
                # Generate ALTER COLUMN TYPE for safe widenings
                if backend in ("spark", "databricks"):
                    # Spark/Databricks: ALTER TABLE ... ALTER COLUMN ... TYPE ...
                    statements.append(
                        f"ALTER TABLE {resolved_table} ALTER COLUMN {field.name} TYPE {desired_sql_type};"
                    )
                elif backend == "snowflake":
                    statements.append(
                        f"ALTER TABLE {resolved_table} MODIFY COLUMN {field.name} {desired_sql_type};"
                    )
                elif backend == "bigquery":
                    # BigQuery doesn't support ALTER COLUMN TYPE directly;
                    # widening happens automatically for compatible types.
                    logger.info(
                        f"Schema evolution: BigQuery auto-widens {field.name} "
                        f"{current_sql_type} → {desired_sql_type} (no DDL needed)"
                    )
                    continue
                elif backend in ("duckdb",):
                    statements.append(
                        f"ALTER TABLE {resolved_table} ALTER COLUMN {field.name} TYPE {desired_sql_type};"
                    )
                elif backend == "postgresql":
                    statements.append(
                        f"ALTER TABLE {resolved_table} ALTER COLUMN {field.name} TYPE {desired_sql_type};"
                    )
                else:
                    statements.append(
                        f"ALTER TABLE {resolved_table} ALTER COLUMN {field.name} TYPE {desired_sql_type};"
                    )
                logger.info(
                    f"Schema evolution: SAFE WIDENING {field.name} "
                    f"{current_sql_type} → {desired_sql_type} → {resolved_table}"
                )
            else:
                # Unsafe type change — warn but do NOT generate DDL
                logger.warning(
                    f"Schema evolution: UNSAFE type change detected for "
                    f"{resolved_table}.{field.name}: {current_sql_type} → {desired_sql_type}. "
                    f"This change may cause data loss and must be applied manually. "
                    f"Consider using a migration script or 'overwriteSchema' option."
                )

    # ── 3. Removed columns (in table but no longer in contract) ───────────
    # Exclude lineage/system columns from the removed check
    _system_prefixes = ("_lakelogic_", "_rule_", "quarantine_")
    for col in sorted(existing_set - expected_set):
        if any(col.startswith(p) for p in _system_prefixes):
            continue
        logger.info(
            f"Schema evolution: Column '{col}' exists in {resolved_table} but is "
            f"no longer in the contract. Physical column retained; runtime "
            f"schema_policy controls whether it is pruned from output."
        )

    return statements


# ── Execution helpers ────────────────────────────────────────────────────────

# Contract type → PyArrow type mapping for Delta table initialization
_CONTRACT_TO_ARROW: Dict[str, str] = {
    "string": "string", "varchar": "string", "text": "string", "char": "string",
    "int": "int32", "integer": "int32",
    "bigint": "int64", "long": "int64",
    "smallint": "int16", "tinyint": "int8",
    "float": "float32", "double": "float64",
    "boolean": "bool", "bool": "bool",
    "date": "date32", "timestamp": "timestamp[us]",
    "timestamp_ntz": "timestamp[us]", "timestamp_tz": "timestamp[us, tz=UTC]",
    "binary": "binary",
    "json": "string", "array": "string",
}


def _resolve_arrow_type(contract_type: str):
    """Map a contract field type string to a PyArrow data type."""
    import re
    import pyarrow as pa

    lower = contract_type.strip().lower()

    # Parameterised types: decimal(10,2), varchar(255)
    param_match = re.match(r"^(\w+)\((.+)\)$", lower)
    if param_match:
        base = param_match.group(1)
        params = param_match.group(2)
        if base in ("decimal", "numeric"):
            parts = [int(p.strip()) for p in params.split(",")]
            return pa.decimal128(parts[0], parts[1] if len(parts) > 1 else 0)
        if base in ("varchar", "char"):
            return pa.string()  # Arrow strings are unbounded

    # Direct lookup
    arrow_key = _CONTRACT_TO_ARROW.get(lower, "string")
    if arrow_key == "string":
        return pa.string()
    elif arrow_key == "int8":
        return pa.int8()
    elif arrow_key == "int16":
        return pa.int16()
    elif arrow_key == "int32":
        return pa.int32()
    elif arrow_key == "int64":
        return pa.int64()
    elif arrow_key == "float32":
        return pa.float32()
    elif arrow_key == "float64":
        return pa.float64()
    elif arrow_key == "bool":
        return pa.bool_()
    elif arrow_key == "date32":
        return pa.date32()
    elif arrow_key.startswith("timestamp"):
        return pa.timestamp("us")
    elif arrow_key == "binary":
        return pa.binary()
    else:
        return pa.string()


def _init_delta_table_from_contract(contract: DataContract) -> None:
    """Initialize a Delta table with correct schema using an empty write.

    Builds a PyArrow schema from the contract model fields and writes an
    empty (zero-row) table via ``write_deltalake``.  This creates the
    ``_delta_log/`` metadata folder and records the schema without
    materializing any data rows.

    If the Delta table already exists at the target path, this is a no-op.

    Args:
        contract: DataContract with model fields and materialization target.
    """
    fields = _get_fields(contract)
    if not fields:
        logger.warning(
            "Cannot initialize Delta table: contract has no model.fields. "
            "Table will be created on first data write."
        )
        return

    mat = contract.materialization
    if not mat or not mat.target_path:
        logger.info(
            "No materialization.target_path configured — "
            "Delta table will be created on first data write."
        )
        return

    target = str(mat.target_path)
    if target.startswith("table:"):
        # Catalog-managed table — Delta init not applicable
        logger.info(
            f"Target '{target}' is a catalog table reference — "
            "DDL must be applied via the catalog engine (Spark/Databricks)."
        )
        return

    try:
        import pyarrow as pa
        from deltalake import write_deltalake, DeltaTable
    except ImportError:
        logger.warning(
            "deltalake and pyarrow are required for Delta DDL init. "
            "Install them: pip install deltalake pyarrow"
        )
        return

    # Build storage options for cloud paths
    storage_opts = None
    if any(target.startswith(p) for p in ("abfss://", "abfs://", "s3://", "s3a://", "gs://", "gcs://")):
        from lakelogic.core.materialization import _build_storage_options
        storage_opts = _build_storage_options()
        if not storage_opts:
            logger.warning(
                f"Cloud path detected ({target[:30]}...) but no storage credentials "
                "found in environment. Set AZURE_*/AWS_*/GOOGLE_* env vars."
            )
            return

    # Check if already initialized
    try:
        DeltaTable(target, storage_options=storage_opts)
        table_label = _resolve_table_name(contract) or target
        logger.info(
            f"Delta table already exists at {table_label} — schema evolution "
            "will be applied during the next data write."
        )
        return
    except Exception:
        pass  # Table doesn't exist yet — proceed with creation

    # Build PyArrow schema from contract fields
    pa_fields = []
    for field in fields:
        arrow_type = _resolve_arrow_type(field.type)
        pa_fields.append(pa.field(field.name, arrow_type, nullable=not field.required))

    # Add lineage system columns if enabled
    lineage_cfg = getattr(contract, "lineage", None)
    if lineage_cfg and getattr(lineage_cfg, "enabled", False):
        _sys_defs = [
            ("capture_source_path", "source_column_name", "_lakelogic_source", pa.string()),
            ("capture_timestamp", "timestamp_column_name", "_lakelogic_processed_at", pa.timestamp("us")),
            ("capture_run_id", "run_id_column_name", "_lakelogic_run_id", pa.string()),
            ("capture_domain", "domain_column_name", "_lakelogic_domain", pa.string()),
            ("capture_system", "system_column_name", "_lakelogic_system", pa.string()),
            ("capture_created_at", "created_at_column_name", "_lakelogic_created_at", pa.timestamp("us")),
            ("capture_created_by", "created_by_column_name", "_lakelogic_created_by", pa.string()),
        ]
        existing_names = {f.name for f in fields}
        for flag_attr, name_attr, default_name, arrow_t in _sys_defs:
            if getattr(lineage_cfg, flag_attr, True):
                col_name = getattr(lineage_cfg, name_attr, default_name)
                if col_name not in existing_names:
                    pa_fields.append(pa.field(col_name, arrow_t, nullable=True))

    schema = pa.schema(pa_fields)

    # Resolve partition columns
    partition_by = list(mat.partition_by or [])
    # Prune partition columns not in the schema
    schema_names = set(schema.names)
    partition_by = [c for c in partition_by if c in schema_names]

    table_label = _resolve_table_name(contract) or target

    try:
        # Prefer DeltaTable.create() — purpose-built for schema-only init
        create_kwargs: Dict[str, Any] = {
            "table_uri": target,
            "schema": schema,
        }
        if partition_by:
            create_kwargs["partition_by"] = partition_by
        if storage_opts:
            create_kwargs["storage_options"] = storage_opts

        DeltaTable.create(**create_kwargs)

        logger.info(
            f"Initialized Delta table schema for {table_label} "
            f"({len(fields)} columns, 0 rows) at {target}"
        )
    except TypeError:
        # Older deltalake versions may not have DeltaTable.create()
        # Fall back to write_deltalake with an empty table
        try:
            empty_table = pa.table(
                {f.name: pa.array([], type=f.type) for f in schema},
                schema=schema,
            )
            wdl_kwargs: Dict[str, Any] = {"mode": "overwrite"}
            if partition_by:
                wdl_kwargs["partition_by"] = partition_by
            if storage_opts:
                wdl_kwargs["storage_options"] = storage_opts

            write_deltalake(target, empty_table, **wdl_kwargs)

            logger.info(
                f"Initialized Delta table schema for {table_label} "
                f"({len(fields)} columns, 0 rows) at {target}"
            )
        except Exception as fallback_err:
            logger.warning(
                f"Could not initialize Delta table at {target}: {fallback_err}. "
                "Table will be created on first data write."
            )
    except Exception as e:
        logger.warning(
            f"Could not initialize Delta table at {target}: {e}. "
            "Table will be created on first data write."
        )

def create_table(
    contract: DataContract,
    backend: str,
    *,
    table_name: Optional[str] = None,
    db_path: Optional[str] = None,
    connection: Any = None,
    dry_run: bool = False,
) -> str:
    """
    Generate and optionally execute CREATE TABLE DDL.

    Args:
        contract: DataContract with schema model.
        backend: Target backend.
        table_name: Override table name.
        db_path: Database file path (for DuckDB/SQLite).
        connection: Existing database connection to use.
        dry_run: If True, only return the DDL without executing.

    Returns:
        The generated DDL string.
    """
    ddl = generate_ddl(contract, backend, table_name=table_name)

    if dry_run:
        logger.info(f"[DRY RUN] DDL for {backend}:\n{ddl}")
        return ddl

    backend = backend.lower()

    if backend in ("duckdb",):
        mat = contract.materialization
        is_delta = mat and str(mat.format or "").lower() == "delta"
        if is_delta:
            _init_delta_table_from_contract(contract)
            return ddl

        import duckdb

        con = connection or duckdb.connect(database=str(db_path or ":memory:"))
        try:
            for statement in ddl.split(";"):
                statement = statement.strip()
                if statement:
                    con.execute(statement)
            # Print a cleaner summary instead of just the first line
            table_lbl = table_name or _resolve_table_name(contract)
            logger.info(f"Created table {table_lbl} via DuckDB")
        finally:
            if not connection:
                con.close()

    elif backend == "sqlite":
        import sqlite3

        con = connection or sqlite3.connect(str(db_path or ":memory:"))
        try:
            for statement in ddl.split(";"):
                statement = statement.strip()
                if statement:
                    con.execute(statement)
            con.commit()
            logger.info(f"Created table via SQLite: {ddl.splitlines()[0]}")
        finally:
            if not connection:
                con.close()

    elif backend in ("spark", "databricks"):
        # If Spark is targeting direct storage (not a catalog table), use
        # the empty-DataFrame Delta init — spark.sql("CREATE TABLE ...") would
        # register a managed table in the default catalog, not at the path.
        mat = contract.materialization
        target_str = str(mat.target_path) if mat and mat.target_path else ""
        is_direct_storage = target_str and not target_str.startswith("table:")
        if is_direct_storage:
            _init_delta_table_from_contract(contract)
            return ddl

        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()

            # Ensure the target schema exists before CREATE TABLE
            _resolved = table_name or _resolve_table_name(contract)
            if _resolved:
                _parts = _resolved.replace("`", "").split(".")
                if len(_parts) >= 2:
                    _schema_ref = ".".join([f"`{p}`" if "-" in p else p for p in _parts[:-1]])
                    try:
                        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {_schema_ref}")
                    except Exception as schema_exc:
                        logger.warning(
                            f"Could not create schema {_schema_ref}: {schema_exc}. "
                            f"CREATE TABLE may fail if schema does not exist."
                        )

            for statement in ddl.split(";"):
                statement = statement.strip()
                if statement:
                    spark.sql(statement)

            logger.info(f"Created table via Spark: {ddl.splitlines()[0]}")
        except ImportError:
            raise ValueError("Spark backend requires pyspark installed.")

    elif backend == "snowflake":
        if not connection:
            raise ValueError("Snowflake backend requires a connection object.")
        cursor = connection.cursor()
        try:
            for statement in ddl.split(";"):
                statement = statement.strip()
                if statement:
                    cursor.execute(statement)
            logger.info(f"Created table via Snowflake: {ddl.splitlines()[0]}")
        finally:
            cursor.close()

    elif backend == "bigquery":
        if not connection:
            raise ValueError("BigQuery backend requires a client object as connection.")
        for statement in ddl.split(";"):
            statement = statement.strip()
            if statement:
                connection.query(statement).result()
        logger.info(f"Created table via BigQuery: {ddl.splitlines()[0]}")

    elif backend == "postgresql":
        if not connection:
            raise ValueError("PostgreSQL backend requires a connection object.")
        cursor = connection.cursor()
        try:
            for statement in ddl.split(";"):
                statement = statement.strip()
                if statement:
                    cursor.execute(statement)
            connection.commit()
            logger.info(f"Created table via PostgreSQL: {ddl.splitlines()[0]}")
        finally:
            cursor.close()

    elif backend in ("polars", "pandas", "python"):
        _init_delta_table_from_contract(contract)

    else:
        raise ValueError(f"Unsupported backend for table creation: {backend}")

    return ddl


def init_tables_from_directory(
    contracts_dir: Path,
    backend: str,
    *,
    db_path: Optional[str] = None,
    connection: Any = None,
    dry_run: bool = False,
    pattern: str = "*.yaml",
) -> Dict[str, str]:
    """
    Scan a directory for contracts and generate/execute DDL for each.

    Args:
        contracts_dir: Directory containing contract YAML files.
        backend: Target backend.
        db_path: Database file path (for DuckDB/SQLite).
        connection: Existing database connection.
        dry_run: If True, only return DDL without executing.
        pattern: Glob pattern for contract files.

    Returns:
        Dict mapping contract file path to generated DDL.
    """
    import yaml

    contracts_dir = Path(contracts_dir)
    if not contracts_dir.is_dir():
        raise ValueError(f"Not a directory: {contracts_dir}")

    results = {}
    for yaml_file in sorted(contracts_dir.glob(pattern)):
        try:
            with open(yaml_file, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
            if not isinstance(data, dict):
                logger.debug(f"Skipping {yaml_file}: not a valid contract")
                continue

            contract = DataContract(**data)
            fields = _get_fields(contract)
            if not fields:
                logger.debug(f"Skipping {yaml_file}: no model.fields defined")
                continue

            ddl = create_table(
                contract,
                backend,
                db_path=db_path,
                connection=connection,
                dry_run=dry_run,
            )
            results[str(yaml_file)] = ddl
            logger.info(f"{'[DRY RUN] ' if dry_run else ''}Processed {yaml_file.name}")

        except Exception as exc:
            logger.warning(f"Failed to process {yaml_file}: {exc}")
            results[str(yaml_file)] = f"-- ERROR: {exc}"

    # Also scan for .yml files
    for yaml_file in sorted(contracts_dir.glob("*.yml")):
        if str(yaml_file) not in results:
            try:
                with open(yaml_file, "r", encoding="utf-8") as f:
                    data = yaml.safe_load(f)
                if not isinstance(data, dict):
                    continue
                contract = DataContract(**data)
                fields = _get_fields(contract)
                if not fields:
                    continue
                ddl = create_table(
                    contract,
                    backend,
                    db_path=db_path,
                    connection=connection,
                    dry_run=dry_run,
                )
                results[str(yaml_file)] = ddl
            except Exception as exc:
                logger.warning(f"Failed to process {yaml_file}: {exc}")
                results[str(yaml_file)] = f"-- ERROR: {exc}"

    return results
