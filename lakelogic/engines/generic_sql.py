"""
GenericSQLAdapter — One adapter, any SQL database.

Executes LakeLogic quality rules directly inside the target database
by transpiling Standard SQL (DuckDB dialect) to the target's native dialect
using sqlglot.

Supports any DB-API 2.0 compatible connection (psycopg2, pyodbc, mysql-connector, etc.).

Usage:
    import psycopg2

    from lakelogic import DataContract
    from lakelogic.engines.generic_sql import GenericSQLAdapter

    conn = psycopg2.connect("dbname=analytics user=reader")
    contract = DataContract.from_yaml("contracts/gold_orders.yaml")

    adapter = GenericSQLAdapter(
        contract=contract,
        connection=conn,
        dialect="postgres",
        source_table="gold.orders",
    )
    good_df, bad_df = adapter.execute()
"""

import re
import time
from typing import Any, Dict, List, Optional, Tuple

from loguru import logger

from lakelogic.core.models import DataContract
from lakelogic.engines.base import ENGINE_DIALECT_MAP, EngineAdapter

from ..core import types as _types


class GenericSQLAdapter(EngineAdapter):
    """
    Generic SQL execution engine for LakeLogic.

    Pushes quality rule evaluation down to any SQL database by transpiling
    rules from DuckDB dialect to the target dialect using sqlglot.

    Supported databases (via sqlglot dialects):
        postgres, mysql, redshift, bigquery, snowflake, clickhouse,
        sqlserver/tsql, oracle, trino, presto, databricks, and more.

    Args:
        contract: DataContract instance.
        connection: DB-API 2.0 compatible database connection.
        dialect: Target SQL dialect name (e.g. "postgres", "mysql", "redshift").
        source_table: Fully-qualified table name to validate (e.g. "schema.table").
        trace: Optional trace steps.
    """

    def __init__(
        self,
        contract: DataContract,
        connection: Any,
        dialect: str = "postgres",
        source_table: str = "source",
        trace: Optional[List[Any]] = None,
    ):
        super().__init__(contract, trace)
        self.conn = connection
        self.source_table = source_table
        self.engine_name = dialect
        self.engine_dialect = ENGINE_DIALECT_MAP.get(dialect.lower(), dialect)

    def _execute_sql(self, sql: str) -> Any:
        """Execute a SQL statement and return the cursor."""
        cursor = self.conn.cursor()
        cursor.execute(sql)
        return cursor

    def _fetch_scalar(self, sql: str) -> Any:
        """Execute SQL and return a single scalar value."""
        cursor = self._execute_sql(sql)
        row = cursor.fetchone()
        cursor.close()
        return row[0] if row else None

    def _fetch_count(self, sql: str) -> int:
        """Execute a COUNT query and return the integer result."""
        result = self._fetch_scalar(sql)
        return int(result) if result is not None else 0

    def execute(self, df: Any = None) -> Tuple[Any, Any]:
        """
        Execute quality rules against the source table in the database.

        Unlike Polars/Spark adapters which process in-memory DataFrames,
        this adapter pushes ALL rule evaluation to the database.

        Args:
            df: Ignored. The source is the database table specified in __init__.

        Returns:
            Tuple of (good_results, bad_results) as dicts with counts and details.
        """
        # Refuse ONLY the ops this adapter genuinely cannot express (join/lookup need
        # a second relation; explode/pivot/unpivot reshape the set). Everything else
        # is applied below as chained derived tables. Refusing up front, by name,
        # keeps the original guarantee: rules never run against data that silently
        # skipped a transformation.
        self._assert_transformations_expressible()

        start = time.time()
        table = self._transformation_source()

        # ── 1. Get source row count ──────────────────────────────────────
        count_sql = self._transpile(f"SELECT COUNT(*) FROM {table}")
        source_count = self._fetch_count(count_sql)
        self._add_trace("source_count", input_rows=source_count)
        logger.info(f"GenericSQL[{self.engine_dialect}] source rows: {source_count}")

        # ── 2. Evaluate row-level rules ──────────────────────────────────
        row_rules = self.get_row_rules()
        rule_failures: Dict[str, int] = {}
        rule_conditions: List[str] = []

        for rule in row_rules:
            # Transpile each rule's SQL to target dialect
            rule_sql = self._transpile(rule.sql)
            rule_conditions.append(rule_sql)

            # Count failures: rows where the rule is NOT satisfied
            check_sql = self._transpile(f"SELECT COUNT(*) FROM {table} WHERE NOT ({rule.sql})")
            try:
                fail_count = self._fetch_count(check_sql)
                rule_failures[rule.name] = fail_count
                status = "✅" if fail_count == 0 else f"❌ {fail_count} failures"
                logger.info(f"  Rule '{rule.name}': {status}")
                self._add_trace(
                    f"rule:{rule.name}",
                    details={"failures": fail_count, "sql": rule_sql},
                    status="ok" if fail_count == 0 else "fail",
                )
            except Exception as e:
                logger.warning(f"  Rule '{rule.name}' failed to execute: {e}")
                rule_failures[rule.name] = -1
                self._add_trace(
                    f"rule:{rule.name}",
                    details={"error": str(e)},
                    status="error",
                )

        # ── 3. Evaluate dataset-level rules ──────────────────────────────
        dataset_rules = self.get_dataset_rules()
        dataset_results: List[Dict[str, Any]] = []

        for rule in dataset_rules:
            rule_sql = self._transpile(rule.sql)
            try:
                result = self._fetch_scalar(rule_sql)
                passed = True

                if rule.must_be_less_than is not None:
                    passed = result < rule.must_be_less_than
                elif rule.must_be_greater_than is not None:
                    passed = result > rule.must_be_greater_than
                elif getattr(rule, "must_be_between", None) is not None:
                    lo, hi = rule.must_be_between
                    passed = lo <= result <= hi

                dataset_results.append(
                    {
                        "rule": rule.name,
                        "value": result,
                        "passed": passed,
                    }
                )
                status = "✅" if passed else f"❌ value={result}"
                logger.info(f"  Dataset rule '{rule.name}': {status}")
            except Exception as e:
                logger.warning(f"  Dataset rule '{rule.name}' failed: {e}")
                dataset_results.append(
                    {
                        "rule": rule.name,
                        "value": None,
                        "passed": False,
                        "error": str(e),
                    }
                )

        self.dataset_rule_results = dataset_results

        # ── 4. Calculate good/bad counts ─────────────────────────────────
        if rule_conditions:
            # Build a combined WHERE clause: all rules must pass
            combined_pass = " AND ".join(f"({c})" for c in rule_conditions)
            combined_pass_sql = self._transpile(f"SELECT COUNT(*) FROM {table} WHERE {combined_pass}")
            try:
                good_count = self._fetch_count(combined_pass_sql)
            except Exception:
                good_count = source_count - sum(v for v in rule_failures.values() if v > 0)
        else:
            good_count = source_count

        bad_count = source_count - good_count

        elapsed = (time.time() - start) * 1000
        self._add_trace(
            "complete",
            input_rows=source_count,
            output_rows=good_count,
            duration_ms=elapsed,
            details={
                "bad_count": bad_count,
                "rule_failures": rule_failures,
            },
        )

        logger.info(f"GenericSQL[{self.engine_dialect}] complete: {good_count} good, {bad_count} bad ({elapsed:.0f}ms)")

        # ── 5. Return results ────────────────────────────────────────────
        # Unlike in-memory engines, we return dicts with counts + metadata
        # rather than DataFrames (the data stays in the database).
        good_result = {
            "count": good_count,
            "source_table": table,
            "dialect": self.engine_dialect,
        }
        bad_result = {
            "count": bad_count,
            "rule_failures": rule_failures,
            "source_table": table,
            "dialect": self.engine_dialect,
        }

        return good_result, bad_result

    def generate_ddl(self, backend: Optional[str] = None) -> str:
        """
        Generate CREATE TABLE DDL for the contract schema.

        Transpiles DuckDB-dialect DDL to the target database dialect.
        """
        if not self.contract.model or not self.contract.model.fields:
            return ""

        table_name = self.contract.dataset or "output_table"
        columns: List[str] = []

        for field in self.contract.model.fields:
            col_type = field.type.upper() if field.type else "TEXT"
            nullable = "" if not field.required else " NOT NULL"
            columns.append(f"  {field.name} {col_type}{nullable}")

        ddl = f"CREATE TABLE {table_name} (\n{','.join(chr(10) + c for c in columns)}\n)"

        return self._transpile(ddl, read_dialect="duckdb")

    def validate_connection(self) -> Dict[str, Any]:
        """
        Quick health check — verifies the connection and table are accessible.

        Returns:
            Dict with status, row_count, and column_count.
        """
        try:
            count = self._fetch_count(self._transpile(f"SELECT COUNT(*) FROM {self.source_table}"))
            return {
                "status": "ok",
                "dialect": self.engine_dialect,
                "table": self.source_table,
                "row_count": count,
            }
        except Exception as e:
            return {
                "status": "error",
                "dialect": self.engine_dialect,
                "table": self.source_table,
                "error": str(e),
            }

    # ═════════════════════════════════════════════════════════════════════
    # Schema Evolution — ALTER TABLE for contract changes
    # ═════════════════════════════════════════════════════════════════════

    def _get_table_columns(self, table: Optional[str] = None) -> List[str]:
        """
        Query the database to get current column names for a table.

        Uses INFORMATION_SCHEMA which is supported by Postgres, MySQL,
        Redshift, Snowflake, BigQuery, SQL Server, and most others.
        """
        target = table or self.source_table
        # Parse schema.table
        parts = target.split(".")
        if len(parts) == 2:
            schema_name, table_name = parts
        elif len(parts) == 3:
            # catalog.schema.table (e.g. BigQuery project.dataset.table)
            schema_name, table_name = parts[1], parts[2]
        else:
            schema_name, table_name = None, parts[0]

        if schema_name:
            query = self._transpile(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_schema = '{schema_name}' AND table_name = '{table_name}' "
                f"ORDER BY ordinal_position"
            )
        else:
            query = self._transpile(
                f"SELECT column_name FROM information_schema.columns "
                f"WHERE table_name = '{table_name}' "
                f"ORDER BY ordinal_position"
            )

        try:
            cursor = self._execute_sql(query)
            return [row[0].lower() for row in cursor.fetchall()]
        except Exception as e:
            logger.warning(f"Could not fetch columns for {target}: {e}")
            return []

    def sync_schema(
        self,
        target_table: Optional[str] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Sync a table's schema to match the contract definition.

        Compares the contract's model.fields against the actual table columns.
        Generates ALTER TABLE ADD COLUMN for any missing columns.

        Does NOT drop columns or change types — those are destructive operations
        that should be handled manually.

        Args:
            target_table: Table to sync. Defaults to source_table.
            dry_run: If True, return the ALTER statements without executing them.

        Returns:
            Dict with added columns, skipped columns, and SQL statements.
        """
        table = target_table or self.source_table

        if not self.contract.model or not self.contract.model.fields:
            return {"status": "ok", "message": "No fields in contract", "added": []}

        # Get current table columns
        existing_columns = self._get_table_columns(table)
        if not existing_columns:
            return {
                "status": "warning",
                "message": f"Could not read columns from {table}. Table may not exist.",
                "added": [],
            }

        # Compare against contract
        contract_fields = {f.name.lower(): f for f in self.contract.model.fields}
        missing = [f for name, f in contract_fields.items() if name not in existing_columns]
        extra_in_table = [c for c in existing_columns if c not in contract_fields]

        alter_statements: List[str] = []
        added: List[str] = []

        for field in missing:
            col_type = field.type.upper() if field.type else "TEXT"
            has_default = getattr(field, "default", None) is not None
            default = f" DEFAULT {self._format_literal(field.default)}" if has_default else ""

            alter_sql = self._transpile(f"ALTER TABLE {table} ADD COLUMN {field.name} {col_type}{default}")
            alter_statements.append(alter_sql)
            added.append(field.name)

        # Execute or dry-run
        if not dry_run and alter_statements:
            for stmt in alter_statements:
                try:
                    self._execute_sql(stmt)
                    logger.info(f"  ✅ Added column: {stmt}")
                except Exception as e:
                    logger.error(f"  ❌ Failed: {stmt} — {e}")
            try:
                self.conn.commit()
            except Exception:
                self.conn.rollback()

        result = {
            "status": "ok",
            "table": table,
            "added": added,
            "extra_in_table": extra_in_table,
            "alter_statements": alter_statements,
            "dry_run": dry_run,
        }

        if added:
            logger.info(
                f"GenericSQL[{self.engine_dialect}] sync_schema: added {len(added)} column(s) to {table}: {added}"
            )
        else:
            logger.info(f"GenericSQL[{self.engine_dialect}] sync_schema: {table} is up to date")

        if extra_in_table:
            logger.info(f"  ℹ️  {len(extra_in_table)} column(s) in table but not in contract: {extra_in_table}")

        self._add_trace(
            "sync_schema",
            details={
                "added": added,
                "extra_in_table": extra_in_table,
                "dry_run": dry_run,
            },
        )
        return result

    def alter_add_column(
        self,
        target_table: str,
        column_name: str,
        column_type: str = "TEXT",
        default: Optional[str] = None,
        not_null: bool = False,
    ) -> Dict[str, Any]:
        """
        Add a single column to a table.

        Args:
            target_table: Table to alter.
            column_name: New column name.
            column_type: SQL type (e.g. "VARCHAR(255)", "INTEGER", "TIMESTAMP").
            default: Default value expression.
            not_null: If True, add NOT NULL constraint (requires default).

        Returns:
            Dict with alter status.
        """
        parts = [f"ALTER TABLE {target_table} ADD COLUMN {column_name} {column_type}"]
        if default is not None:
            parts.append(f"DEFAULT {default}")
        if not_null:
            parts.append("NOT NULL")

        alter_sql = self._transpile(" ".join(parts))

        try:
            self._execute_sql(alter_sql)
            self.conn.commit()
            logger.info(f"GenericSQL[{self.engine_dialect}] ALTER TABLE: added {column_name} to {target_table}")
            return {"status": "ok", "table": target_table, "column": column_name, "sql": alter_sql}
        except Exception as e:
            self.conn.rollback()
            logger.error(f"GenericSQL ALTER TABLE failed: {e}")
            return {"status": "error", "error": str(e), "sql": alter_sql}

    def alter_drop_column(
        self,
        target_table: str,
        column_name: str,
    ) -> Dict[str, Any]:
        """
        Drop a column from a table.

        ⚠️ Destructive operation — use with caution.

        Args:
            target_table: Table to alter.
            column_name: Column to drop.

        Returns:
            Dict with alter status.
        """
        alter_sql = self._transpile(f"ALTER TABLE {target_table} DROP COLUMN {column_name}")

        try:
            self._execute_sql(alter_sql)
            self.conn.commit()
            logger.info(f"GenericSQL[{self.engine_dialect}] ALTER TABLE: dropped {column_name} from {target_table}")
            return {"status": "ok", "table": target_table, "column_dropped": column_name, "sql": alter_sql}
        except Exception as e:
            self.conn.rollback()
            logger.error(f"GenericSQL ALTER TABLE failed: {e}")
            return {"status": "error", "error": str(e), "sql": alter_sql}

    # ═════════════════════════════════════════════════════════════════════
    # Phase 2: CTAS Materialization — good / bad / quarantine tables
    # ═════════════════════════════════════════════════════════════════════

    # Structured transformations this adapter can express. Ops NOT listed here need
    # a second relation (join/lookup) or set-reshaping (explode/pivot/unpivot) and are
    # REFUSED by name rather than skipped — the whole reason this guard exists.
    _SUPPORTED_TRANSFORMS = {
        "rename",
        "select",
        "drop",
        "cast",
        "filter",
        "derive",
        "coalesce",
        "trim",
        "lower",
        "upper",
        "deduplicate",
        "sql",
    }

    def _assert_transformations_expressible(self) -> None:
        """Fail loudly on any transformation this adapter cannot render as SQL.

        Named ops, not a blanket ban. The blanket refusal that preceded this rejected
        every contract with a `transformations:` block — safe, but it made the adapter
        unusable for the many contracts whose transformations are ordinary projections.
        Only genuinely inexpressible ops are refused now, and they are refused BY NAME
        so the message says what to do about it.
        """
        unsupported: list[str] = []
        for trans in getattr(self.contract, "transformations", None) or []:
            declared = (
                [f for f in type(trans).model_fields if f != "phase" and getattr(trans, f, None) is not None]
                if hasattr(type(trans), "model_fields")
                else []
            )
            for op in declared:
                if op not in self._SUPPORTED_TRANSFORMS:
                    unsupported.append(op)

        if unsupported:
            raise NotImplementedError(
                f"The '{self.engine_name}' engine cannot apply these transformations: "
                f"{sorted(set(unsupported))}. They need a second relation or reshape the "
                "row set, which this adapter does not express. Running anyway would "
                "evaluate every quality rule against data that skipped them and report "
                "success, so the run is refused. Either pre-materialise them as a view "
                "and point source_table at it, or use an engine that applies them "
                "(polars, duckdb, spark, snowflake, bigquery)."
            )

    def _quote(self, name: str) -> str:
        """Quote an identifier for the target dialect (sqlglot normalises on transpile)."""
        return f'"{name}"'

    def _transformation_source(self) -> str:
        """The FROM target for rule evaluation: the source table, or a derived table
        wrapping the contract's transformations.

        Chained subqueries, deliberately, rather than the temp tables the warehouse
        engines use. Temp-table syntax and permissions vary across the dialects this
        adapter claims (``CREATE TEMP`` vs ``CREATE TEMPORARY``; Trino barely has
        them), whereas a derived table is plain ANSI SQL that needs no DDL rights at
        all. It also composes with the existing rule SQL untouched: every
        ``FROM {table}`` in execute() keeps working when *table* becomes
        ``(SELECT ...) AS lakelogic_src``.
        """
        transformations = getattr(self.contract, "transformations", None) or []
        if not transformations:
            return self.source_table

        current = self.source_table
        for trans in transformations:
            sql = self._transformation_to_sql(trans, current)
            if sql:
                current = f"({sql}) AS lakelogic_t"
        return current

    def _relation_columns(self, source: str) -> List[str]:
        """Column names of any relation — a table OR a derived table.

        ``_get_table_columns`` uses INFORMATION_SCHEMA, which cannot describe a
        subquery, so it returns nothing once transformations start chaining. A
        zero-row probe works on every DB-API driver and for every relation shape,
        and returns no data.
        """
        cursor = self.conn.cursor()
        try:
            cursor.execute(self._transpile(f"SELECT * FROM {source} WHERE 1=0"))
            return [d[0] for d in (cursor.description or [])]
        except Exception as exc:
            logger.warning(f"Could not resolve columns for {source}: {exc}")
            return []
        finally:
            try:
                cursor.close()
            except Exception:
                pass

    def _transformation_to_sql(self, trans, source: str) -> Optional[str]:
        """Render ONE transformation as a SELECT over *source*, or None if it is a no-op."""
        cols = self._relation_columns(source)
        if not cols:
            # Without the column list a projection would be built as `SELECT  FROM`,
            # which is a parser error rather than a wrong answer — but refuse
            # explicitly so the reason is the missing schema, not the broken SQL.
            raise RuntimeError(
                f"Cannot apply transformations: no columns resolved for {source}. "
                "The source relation must be readable by the supplied connection."
            )

        if getattr(trans, "sql", None):
            # A raw SQL step names the contract dataset (or `source`) as its FROM.
            body = str(trans.sql)
            for alias in filter(None, [self.contract.dataset, "source"]):
                body = re.sub(rf"\b{re.escape(alias)}\b", source, body)
            return body

        if getattr(trans, "rename", None):
            pairs = dict(trans.rename.iter_pairs())
            if not pairs:
                return None
            projected = [f"{self._quote(c)} AS {self._quote(pairs[c])}" if c in pairs else self._quote(c) for c in cols]
            return f"SELECT {', '.join(projected)} FROM {source}"

        if getattr(trans, "select", None):
            keep = list(trans.select.columns or [])
            return f"SELECT {', '.join(self._quote(c) for c in keep)} FROM {source}" if keep else None

        if getattr(trans, "drop", None):
            drop = set(trans.drop.columns or [])
            keep = [c for c in cols if c not in drop]
            return f"SELECT {', '.join(self._quote(c) for c in keep)} FROM {source}" if keep else None

        if getattr(trans, "filter", None):
            cond = trans.filter.sql
            return f"SELECT * FROM {source} WHERE {cond}" if cond else None

        if getattr(trans, "derive", None):
            return f"SELECT *, ({trans.derive.sql}) AS {self._quote(trans.derive.field)} FROM {source}"

        if getattr(trans, "cast", None):
            casts = dict(trans.cast.columns or {})
            if not casts:
                return None
            projected = [
                f"CAST({self._quote(c)} AS {self._sql_cast_type(casts[c])}) AS {self._quote(c)}"
                if c in casts
                else self._quote(c)
                for c in cols
            ]
            return f"SELECT {', '.join(projected)} FROM {source}"

        for op, fn in (
            ("trim", "TRIM"),
            ("lower", "LOWER"),
            ("upper", "UPPER"),
        ):
            cfg = getattr(trans, op, None)
            if cfg:
                targets = set(cfg.fields or [])
                projected = [
                    f"{fn}({self._quote(c)}) AS {self._quote(c)}" if c in targets else self._quote(c) for c in cols
                ]
                return f"SELECT {', '.join(projected)} FROM {source}"

        if getattr(trans, "coalesce", None):
            cfg = trans.coalesce
            srcs = [self._quote(c) for c in (cfg.sources or [])]
            if not srcs:
                return None
            if cfg.default is not None:
                srcs.append(repr(cfg.default) if isinstance(cfg.default, str) else str(cfg.default))
            out = cfg.output or cfg.field
            return f"SELECT *, COALESCE({', '.join(srcs)}) AS {self._quote(out)} FROM {source}"

        if getattr(trans, "deduplicate", None):
            cfg = trans.deduplicate
            on = getattr(cfg, "on", None) or []
            if not on:
                return None
            sort_by = getattr(cfg, "sort_by", None) or []
            if not sort_by:
                # Mirrors the runtime rule: an unordered dedup is non-deterministic,
                # so it is refused rather than silently picking an arbitrary row.
                raise NotImplementedError(
                    "deduplicate requires sort_by — without it the surviving row is "
                    "arbitrary and the result is not reproducible."
                )
            direction = "DESC" if str(getattr(cfg, "order", "desc")).lower() == "desc" else "ASC"
            part = ", ".join(self._quote(c) for c in on)
            order = ", ".join(f"{self._quote(c)} {direction}" for c in sort_by)
            inner = f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {part} ORDER BY {order}) AS lakelogic_rn FROM {source}"
            keep = ", ".join(self._quote(c) for c in cols) if cols else "*"
            return f"SELECT {keep} FROM ({inner}) AS lakelogic_d WHERE lakelogic_rn = 1"

        return None

    # Rendered for the PostgreSQL dialect, the same one core/ddl uses to CREATE
    # these columns. Previously `float` cast to DOUBLE PRECISION against a REAL
    # column.
    _SQL_CAST_TYPES = _types.as_cast_map("postgresql")

    def _sql_cast_type(self, type_name: str) -> str:
        key = str(type_name).lower()
        if key not in self._SQL_CAST_TYPES:
            raise NotImplementedError(
                f"cast to '{type_name}' is not supported on the {self.engine_name} engine. "
                f"Supported: {sorted(self._SQL_CAST_TYPES)}."
            )
        return self._SQL_CAST_TYPES[key]

    def _build_pass_condition(self) -> str:
        """Build the combined WHERE clause for all row rules passing."""
        row_rules = self.get_row_rules()
        if not row_rules:
            return "1=1"
        conditions = [self._transpile(rule.sql) for rule in row_rules]
        return " AND ".join(f"({c})" for c in conditions)

    def _build_fail_condition(self) -> str:
        """Build the WHERE clause for any row rule failing."""
        row_rules = self.get_row_rules()
        if not row_rules:
            return "1=0"
        conditions = [self._transpile(rule.sql) for rule in row_rules]
        return " OR ".join(f"NOT ({c})" for c in conditions)

    def _build_error_columns_sql(self) -> str:
        """
        Build SQL CASE expressions that produce error detail columns.

        Generates:
          - _lakelogic_errors:      array/concat of failed rule names
          - _lakelogic_categories:  array/concat of failed rule categories
        """
        row_rules = self.get_row_rules()
        if not row_rules:
            return ""

        # Build CONCAT_WS of failed rule names (portable across dialects)
        error_parts: List[str] = []
        category_parts: List[str] = []

        for rule in row_rules:
            rule_sql = self._transpile(rule.sql)
            name = rule.name.replace("'", "''")
            category = (rule.category or "unknown").replace("'", "''")
            error_parts.append(f"CASE WHEN NOT ({rule_sql}) THEN '{name}' END")
            category_parts.append(f"CASE WHEN NOT ({rule_sql}) THEN '{category}' END")

        # Use CONCAT_WS for portability — sqlglot will transpile
        errors_expr = f"CONCAT_WS('|', {', '.join(error_parts)})"
        categories_expr = f"CONCAT_WS('|', {', '.join(category_parts)})"

        return (
            f", {self._transpile(errors_expr)} AS {self.ERROR_COLUMN}"
            f", {self._transpile(categories_expr)} AS {self.CATEGORY_COLUMN}"
        )

    def materialize_good(
        self,
        target_table: str,
        if_exists: str = "replace",
    ) -> Dict[str, Any]:
        """
        Materialize good (passing) rows into a target table.

        Creates a new table containing only rows that pass all quality rules.

        Args:
            target_table: Fully-qualified target table name (e.g. "gold.orders").
            if_exists: "replace" (DROP + CREATE), "append" (INSERT INTO), "fail" (error if exists).

        Returns:
            Dict with status and row count.
        """
        pass_where = self._build_pass_condition()

        if if_exists == "replace":
            drop_sql = self._transpile(f"DROP TABLE IF EXISTS {target_table}")
            create_sql = self._transpile(
                f"CREATE TABLE {target_table} AS SELECT * FROM {self.source_table} WHERE {pass_where}"
            )
            try:
                self._execute_sql(drop_sql)
                self._execute_sql(create_sql)
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

        elif if_exists == "append":
            insert_sql = self._transpile(
                f"INSERT INTO {target_table} SELECT * FROM {self.source_table} WHERE {pass_where}"
            )
            try:
                self._execute_sql(insert_sql)
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

        elif if_exists == "fail":
            create_sql = self._transpile(
                f"CREATE TABLE {target_table} AS SELECT * FROM {self.source_table} WHERE {pass_where}"
            )
            self._execute_sql(create_sql)
            self.conn.commit()

        count = self._fetch_count(self._transpile(f"SELECT COUNT(*) FROM {target_table}"))
        logger.info(f"GenericSQL[{self.engine_dialect}] materialized {count} good rows → {target_table}")

        self._add_trace("materialize_good", output_rows=count, details={"target": target_table})
        return {"status": "ok", "table": target_table, "rows": count}

    def materialize_bad(
        self,
        target_table: str,
        if_exists: str = "replace",
        include_error_columns: bool = True,
    ) -> Dict[str, Any]:
        """
        Materialize bad (quarantined) rows into a target table.

        Creates a quarantine table with rows that fail any quality rule,
        optionally including error detail columns (_lakelogic_errors, _lakelogic_categories).

        Args:
            target_table: Target quarantine table name (e.g. "quarantine.orders").
            if_exists: "replace", "append", or "fail".
            include_error_columns: If True, add _lakelogic_errors and _lakelogic_categories columns.

        Returns:
            Dict with status and row count.
        """
        fail_where = self._build_fail_condition()
        error_cols = self._build_error_columns_sql() if include_error_columns else ""

        select_sql = f"SELECT *{error_cols} FROM {self.source_table} WHERE {fail_where}"

        if if_exists == "replace":
            drop_sql = self._transpile(f"DROP TABLE IF EXISTS {target_table}")
            create_sql = self._transpile(f"CREATE TABLE {target_table} AS {select_sql}")
            try:
                self._execute_sql(drop_sql)
                self._execute_sql(create_sql)
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

        elif if_exists == "append":
            insert_sql = self._transpile(f"INSERT INTO {target_table} {select_sql}")
            try:
                self._execute_sql(insert_sql)
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise

        elif if_exists == "fail":
            create_sql = self._transpile(f"CREATE TABLE {target_table} AS {select_sql}")
            self._execute_sql(create_sql)
            self.conn.commit()

        count = self._fetch_count(self._transpile(f"SELECT COUNT(*) FROM {target_table}"))
        logger.info(f"GenericSQL[{self.engine_dialect}] materialized {count} bad rows → {target_table}")

        self._add_trace("materialize_bad", output_rows=count, details={"target": target_table})
        return {"status": "ok", "table": target_table, "rows": count}

    def create_tables(
        self,
        good_table: str,
        bad_table: Optional[str] = None,
        if_exists: str = "replace",
    ) -> Dict[str, Any]:
        """
        Convenience method: materialize both good and bad tables in one call.

        Args:
            good_table: Target table for passing rows.
            bad_table: Target table for quarantined rows (optional).
            if_exists: "replace", "append", or "fail".

        Returns:
            Dict with good and bad table results.
        """
        result: Dict[str, Any] = {}
        result["good"] = self.materialize_good(good_table, if_exists=if_exists)
        if bad_table:
            result["bad"] = self.materialize_bad(bad_table, if_exists=if_exists)
        return result

    # ═════════════════════════════════════════════════════════════════════
    # Phase 3: MERGE / Upsert / INSERT / UPDATE / DELETE
    # ═════════════════════════════════════════════════════════════════════

    def merge(
        self,
        target_table: str,
        merge_keys: List[str],
        update_columns: Optional[List[str]] = None,
        insert_only: bool = False,
        validate_before_merge: bool = True,
    ) -> Dict[str, Any]:
        """
        MERGE (upsert) validated good rows into a target table.

        Performs a SQL MERGE: inserts new rows and updates existing ones
        based on merge_keys. Automatically transpiled to the target dialect.

        Args:
            target_table: Destination table to merge into.
            merge_keys: List of columns to match on (e.g. ["order_id"]).
            update_columns: Columns to update on match. None = all non-key columns.
            insert_only: If True, only INSERT new rows (skip UPDATE on match).
            validate_before_merge: If True, only merge rows passing quality rules.

        Returns:
            Dict with merge status and row counts.
        """
        source = self.source_table
        pass_where = self._build_pass_condition() if validate_before_merge else "1=1"

        # Determine columns from contract
        if self.contract.model and self.contract.model.fields:
            all_columns = [f.name for f in self.contract.model.fields]
        else:
            # Fallback: query table metadata
            all_columns = update_columns or merge_keys

        if update_columns is None:
            update_columns = [c for c in all_columns if c not in merge_keys]

        # ── Build MERGE statement ────────────────────────────────────────
        on_clause = " AND ".join(f"target.{self._quote_ident(k)} = source.{self._quote_ident(k)}" for k in merge_keys)

        # WHEN MATCHED → UPDATE SET
        if not insert_only and update_columns:
            update_set = ", ".join(f"{self._quote_ident(c)} = source.{self._quote_ident(c)}" for c in update_columns)
            matched_clause = f"WHEN MATCHED THEN UPDATE SET {update_set}"
        else:
            matched_clause = ""

        # WHEN NOT MATCHED → INSERT
        insert_cols = ", ".join(self._quote_ident(c) for c in all_columns)
        insert_vals = ", ".join(f"source.{self._quote_ident(c)}" for c in all_columns)
        not_matched_clause = f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})"

        merge_sql = (
            f"MERGE INTO {target_table} AS target "
            f"USING (SELECT * FROM {source} WHERE {pass_where}) AS source "
            f"ON {on_clause} "
            f"{matched_clause} "
            f"{not_matched_clause}"
        )

        # Transpile to target dialect
        merge_sql = self._transpile(merge_sql)

        try:
            start = time.time()
            self._execute_sql(merge_sql)
            self.conn.commit()
            elapsed = (time.time() - start) * 1000

            # Get final count
            target_count = self._fetch_count(self._transpile(f"SELECT COUNT(*) FROM {target_table}"))

            logger.info(
                f"GenericSQL[{self.engine_dialect}] MERGE → {target_table}: {target_count} total rows ({elapsed:.0f}ms)"
            )
            self._add_trace(
                "merge",
                output_rows=target_count,
                duration_ms=elapsed,
                details={
                    "target": target_table,
                    "merge_keys": merge_keys,
                    "insert_only": insert_only,
                },
            )
            return {
                "status": "ok",
                "table": target_table,
                "rows": target_count,
                "duration_ms": elapsed,
            }

        except Exception as e:
            self.conn.rollback()
            logger.error(f"GenericSQL MERGE failed: {e}")
            return {"status": "error", "error": str(e)}

    def insert_validated(
        self,
        target_table: str,
        validate_before_insert: bool = True,
    ) -> Dict[str, Any]:
        """
        INSERT validated good rows into a target table (append-only).

        Args:
            target_table: Destination table.
            validate_before_insert: If True, only insert rows passing rules.

        Returns:
            Dict with insert status and row count.
        """
        pass_where = self._build_pass_condition() if validate_before_insert else "1=1"

        insert_sql = self._transpile(f"INSERT INTO {target_table} SELECT * FROM {self.source_table} WHERE {pass_where}")

        try:
            start = time.time()
            self._execute_sql(insert_sql)
            self.conn.commit()
            elapsed = (time.time() - start) * 1000

            count = self._fetch_count(self._transpile(f"SELECT COUNT(*) FROM {self.source_table} WHERE {pass_where}"))
            logger.info(f"GenericSQL[{self.engine_dialect}] INSERT {count} rows → {target_table}")
            self._add_trace("insert", output_rows=count, duration_ms=elapsed)
            return {"status": "ok", "table": target_table, "rows_inserted": count}

        except Exception as e:
            self.conn.rollback()
            logger.error(f"GenericSQL INSERT failed: {e}")
            return {"status": "error", "error": str(e)}

    def update_where(
        self,
        target_table: str,
        set_columns: Dict[str, str],
        where: str,
    ) -> Dict[str, Any]:
        """
        Execute an UPDATE statement with transpilation.

        Args:
            target_table: Table to update.
            set_columns: Dict of {column: expression} to set.
            where: WHERE clause (in DuckDB SQL dialect).

        Returns:
            Dict with update status.
        """
        set_clause = ", ".join(f"{self._quote_ident(col)} = {expr}" for col, expr in set_columns.items())

        update_sql = self._transpile(f"UPDATE {target_table} SET {set_clause} WHERE {where}")

        try:
            start = time.time()
            cursor = self._execute_sql(update_sql)
            self.conn.commit()
            elapsed = (time.time() - start) * 1000

            affected = cursor.rowcount if hasattr(cursor, "rowcount") else -1
            cursor.close()

            logger.info(f"GenericSQL[{self.engine_dialect}] UPDATE {affected} rows in {target_table}")
            self._add_trace("update", output_rows=affected, duration_ms=elapsed)
            return {"status": "ok", "table": target_table, "rows_affected": affected}

        except Exception as e:
            self.conn.rollback()
            logger.error(f"GenericSQL UPDATE failed: {e}")
            return {"status": "error", "error": str(e)}

    def delete_where(
        self,
        target_table: str,
        where: str,
    ) -> Dict[str, Any]:
        """
        Execute a DELETE statement with transpilation.

        Args:
            target_table: Table to delete from.
            where: WHERE clause (in DuckDB SQL dialect).

        Returns:
            Dict with delete status.
        """
        delete_sql = self._transpile(f"DELETE FROM {target_table} WHERE {where}")

        try:
            start = time.time()
            cursor = self._execute_sql(delete_sql)
            self.conn.commit()
            elapsed = (time.time() - start) * 1000

            affected = cursor.rowcount if hasattr(cursor, "rowcount") else -1
            cursor.close()

            logger.info(f"GenericSQL[{self.engine_dialect}] DELETE {affected} rows from {target_table}")
            self._add_trace("delete", output_rows=affected, duration_ms=elapsed)
            return {"status": "ok", "table": target_table, "rows_deleted": affected}

        except Exception as e:
            self.conn.rollback()
            logger.error(f"GenericSQL DELETE failed: {e}")
            return {"status": "error", "error": str(e)}

    def reprocess_quarantine(
        self,
        quarantine_table: str,
        target_table: str,
        merge_keys: List[str],
    ) -> Dict[str, Any]:
        """
        Re-validate quarantined rows and merge any that now pass back into the target.

        Useful after fixing data quality issues upstream — rows that previously
        failed rules may now pass and should be promoted from quarantine.

        Args:
            quarantine_table: Source quarantine table containing bad rows.
            target_table: Target good table to merge recovered rows into.
            merge_keys: Columns to match on during MERGE.

        Returns:
            Dict with recovery status and counts.
        """
        # Temporarily change source to quarantine table
        original_source = self.source_table
        self.source_table = quarantine_table

        try:
            # Re-validate quarantined rows
            good_result, bad_result = self.execute()
            recovered = good_result["count"]
            still_bad = bad_result["count"]

            if recovered > 0:
                # MERGE recovered rows into the target
                merge_result = self.merge(
                    target_table=target_table,
                    merge_keys=merge_keys,
                    validate_before_merge=True,
                )

                # Remove recovered rows from quarantine
                pass_where = self._build_pass_condition()
                self._execute_sql(self._transpile(f"DELETE FROM {quarantine_table} WHERE {pass_where}"))
                self.conn.commit()

                logger.info(
                    f"GenericSQL[{self.engine_dialect}] reprocessed quarantine: "
                    f"{recovered} recovered, {still_bad} still quarantined"
                )
            else:
                merge_result = None
                logger.info(f"GenericSQL[{self.engine_dialect}] no recoverable rows in quarantine")

            return {
                "status": "ok",
                "recovered": recovered,
                "still_quarantined": still_bad,
                "merge_result": merge_result,
            }

        finally:
            self.source_table = original_source
