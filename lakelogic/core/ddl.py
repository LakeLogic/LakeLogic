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
            if backend in ("spark", "databricks", "sqlite", "bigquery"):
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

    Looks at materialization.target_path (table:...) then server.path.

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

    # Fallback: use dataset name or info title
    if contract.dataset:
        return contract.dataset
    if contract.info and contract.info.title:
        # Sanitize title for table name
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
                col_def += "  -- PII"

        col_defs.append(col_def)

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
    table_name: Optional[str] = None,
) -> List[str]:
    """
    Generate ALTER TABLE statements for schema evolution (adding new columns).

    Args:
        contract: DataContract with updated schema.
        backend: Target backend.
        existing_columns: List of column names already in the table.
        table_name: Override table name.

    Returns:
        List of ALTER TABLE SQL statements.
    """
    backend = backend.lower()
    fields = _get_fields(contract)
    resolved_table = table_name or _resolve_table_name(contract)
    if not resolved_table:
        raise ValueError("Cannot generate ALTER DDL: no table name resolved.")

    existing_set = {c.lower() for c in existing_columns}
    statements = []

    for field in fields:
        if field.name.lower() not in existing_set:
            sql_type = _resolve_type(field.type, backend)
            if backend in ("duckdb",):
                statements.append(f"ALTER TABLE {resolved_table} ADD COLUMN IF NOT EXISTS {field.name} {sql_type};")
            elif backend == "bigquery":
                statements.append(f"ALTER TABLE {resolved_table} ADD COLUMN {field.name} {sql_type};")
            else:
                statements.append(f"ALTER TABLE {resolved_table} ADD COLUMN {field.name} {sql_type};")

    return statements


# ── Execution helpers ────────────────────────────────────────────────────────


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
        import duckdb

        con = connection or duckdb.connect(database=str(db_path or ":memory:"))
        try:
            for statement in ddl.split(";"):
                statement = statement.strip()
                if statement:
                    con.execute(statement)
            logger.info(f"Created table via DuckDB: {ddl.splitlines()[0]}")
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
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.getOrCreate()
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
