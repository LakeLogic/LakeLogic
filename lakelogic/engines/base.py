from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

import sqlglot
from loguru import logger

from lakelogic.core.models import (
    DataContract,
    DatasetRuleNullRatio,
    DatasetRuleRowCountBetween,
    DatasetRuleUnique,
    QualityRule,
    RowRuleAcceptedValues,
    RowRuleLifecycleWindow,
    RowRuleNotNull,
    RowRuleRange,
    RowRuleReferentialIntegrity,
    RowRuleRegexMatch,
)

# ── Dialect map: engine_name -> sqlglot dialect ──────────────────────────────
ENGINE_DIALECT_MAP: Dict[str, str] = {
    "duckdb": "duckdb",
    "spark": "spark",
    "polars": "duckdb",  # Polars SQL is DuckDB-compatible
    "pandas": "duckdb",
    "bigquery": "bigquery",
    "snowflake": "snowflake",
    "postgres": "postgres",
    "postgresql": "postgres",
    "mysql": "mysql",
    "redshift": "redshift",
    "clickhouse": "clickhouse",
    "sqlserver": "tsql",
    "mssql": "tsql",
    "oracle": "oracle",
    "trino": "trino",
    "presto": "presto",
    "databricks": "databricks",
}


class EngineAdapter(ABC):
    """
    Abstract Base Class for all execution engines.
    """

    ERROR_COLUMN = "_lakelogic_errors"
    CATEGORY_COLUMN = "_lakelogic_categories"

    def __init__(self, contract: DataContract, trace: Optional[List[Any]] = None):
        """
        Initialize adapter with a contract.

        Args:
            contract: DataContract instance.
            trace: Optional list of trace steps to pre-populate.
        """
        self.contract = contract
        self.dataset_rule_results: List[Dict[str, Any]] = []
        self.schema_drift: Dict[str, List[str]] = {}
        self.engine_name: str = ""
        self.engine_dialect: str = ""  # set by subclass or _resolve_dialect()
        self.trace: List[Any] = []  # Avoid circular import of TraceStep here if needed, or import at runtime

    def _add_trace(
        self,
        step: str,
        input_rows: Optional[int] = None,
        output_rows: Optional[int] = None,
        duration_ms: Optional[float] = None,
        details: Optional[Dict[str, Any]] = None,
        status: str = "ok",
    ):
        import time

        from lakelogic.core.models import TraceStep

        self.trace.append(
            TraceStep(
                step=step,
                timestamp=time.time(),
                input_rows=input_rows,
                output_rows=output_rows,
                duration_ms=duration_ms,
                details=details or {},
                status=status,
            )
        )

    def _get_row_count(self, df: Any) -> Optional[int]:
        """Helper to get row count safely across engines."""
        if df is None:
            return 0
        try:
            # Polars LazyFrame — must collect to count
            try:
                import polars as pl

                if isinstance(df, pl.LazyFrame):
                    return df.select(pl.len()).collect().item()
            except ImportError:
                pass
            if hasattr(df, "height"):
                return int(df.height)
            if hasattr(df, "count"):
                # DuckDB/Spark relation
                try:
                    res = df.count()
                    if hasattr(res, "fetchone"):
                        return res.fetchone()[0]
                    if isinstance(res, int):
                        return res
                except Exception as e:
                    logger.debug(f"`count()` failed natively, falling back to len(): {e}")
                    pass
            return int(len(df))
        except Exception:
            return None

    @abstractmethod
    def execute(self, df: Any) -> Tuple[Any, Any]:
        """
        Primary execution method: Validates and Transforms data.

        Args:
            df: Input dataframe.

        Returns:
            Tuple of (good_df, bad_df).
        """
        pass

    def _scd2_injected_columns(self) -> set:
        """
        Names of columns the SCD2 materializer injects (surrogate key, effective
        from/to, current flag, version, change reason). These won't exist on the
        source dataframe at validation time, so checks that run BEFORE
        materialization (auto-`_required` row rules, strict "missing fields"
        schema enforcement) must treat them as derived — not missing.

        Mirrors the suppression in
        lakelogic.core.processor:_handle_drift (the drift warning already
        skips these); this extends the same awareness to the engine.
        """
        mat = getattr(self.contract, "materialization", None)
        strategy = getattr(mat, "strategy", None) if mat else None
        if strategy != "scd2":
            return set()
        scd2_cfg = getattr(mat, "scd2", None)
        if scd2_cfg is None:
            return set()
        cfg = (
            scd2_cfg
            if isinstance(scd2_cfg, dict)
            else (scd2_cfg.model_dump() if hasattr(scd2_cfg, "model_dump") else {})
        )
        injected = set()
        for key, default in (
            ("surrogate_key", "_sk"),
            ("effective_from_field", "effective_from"),
            ("effective_to_field", "effective_to"),
            ("current_flag_field", "is_current"),
            ("version_column", "_version"),
            ("change_reason_column", "_change_reason"),
        ):
            val = cfg.get(key, default)
            if val:
                injected.add(val)
        return injected

    def get_row_rules(self) -> List[QualityRule]:
        """
        Returns all rules that should trigger row-level quarantine.
        """
        rules: List[QualityRule] = []

        enforce_required = True
        if self.contract.quality is not None:
            enforce_required = bool(getattr(self.contract.quality, "enforce_required", True))

        # SCD2 mechanics columns are injected AFTER validation, so auto-generated
        # `_required` (IS NOT NULL) rules on them would always fail. Skip them.
        scd2_injected = self._scd2_injected_columns()

        if enforce_required and self.contract.model and self.contract.model.fields:
            for field in self.contract.model.fields:
                if field.required and field.name not in scd2_injected:
                    rules.append(
                        QualityRule(
                            name=f"{field.name}_required",
                            sql=f"{self._quote_ident(field.name)} IS NOT NULL",
                            category="completeness",
                            description=f"{field.name} is required",
                        )
                    )
                if field.rules:
                    rules.extend(field.rules)

        if self.contract.quality and self.contract.quality.row_rules:
            for spec in self.contract.quality.row_rules:
                expanded = self._expand_row_rule(spec)
                if not expanded:
                    continue
                if isinstance(expanded, list):
                    rules.extend(expanded)
                else:
                    rules.append(expanded)

        return rules

    def get_dataset_rules(self) -> List[QualityRule]:
        """
        Returns all rules that are aggregate/metric based.
        """
        if not self.contract.quality or not self.contract.quality.dataset_rules:
            return []
        rules: List[QualityRule] = []
        for spec in self.contract.quality.dataset_rules:
            expanded = self._expand_dataset_rule(spec)
            if expanded:
                rules.append(expanded)
        return rules

    def _normalize_engine(self) -> str:
        """
        Normalize engine name for helper logic.

        Returns:
            Normalized engine name.
        """
        engine = (self.engine_name or "").lower()
        if engine == "pandas":
            return "duckdb"
        return engine

    def _resolve_dialect(self) -> str:
        """
        Resolve the sqlglot dialect for the current engine.

        Priority: explicit engine_dialect > engine_name lookup > "duckdb" default.
        """
        if self.engine_dialect:
            return self.engine_dialect
        engine = self._normalize_engine()
        return ENGINE_DIALECT_MAP.get(engine, "duckdb")

    def _transpile(self, sql: str, read_dialect: str = "duckdb") -> str:
        """
        Transpile a SQL expression from a source dialect to the target engine dialect.

        Standard SQL in contracts is written in DuckDB dialect (the default "read").
        This method converts it to the active engine's dialect.

        Args:
            sql: SQL expression string (e.g. a quality rule or transformation).
            read_dialect: Source dialect of the input SQL. Defaults to "duckdb".

        Returns:
            Transpiled SQL for the current engine.
        """
        target = self._resolve_dialect()
        if read_dialect == target:
            return sql
        try:
            result = sqlglot.transpile(sql, read=read_dialect, write=target)
            return result[0] if result else sql
        except sqlglot.errors.ErrorLevel:
            logger.warning(f"sqlglot transpile failed for: {sql!r} -> {target}; using original")
            return sql
        except Exception:
            # Fallback: return original SQL if transpilation fails
            return sql

    def _transpile_derive_sql(self, derive) -> str:
        """
        Resolve the best SQL expression for a derive transform.

        Resolution order:
          1. Engine-specific override (``sql_duckdb`` / ``sql_spark``) — always wins.
          2. Auto-transpile ``sql`` from Spark dialect → engine dialect via sqlglot.
          3. Fall back to raw ``sql`` if transpilation fails.

        Contracts are authored in **Spark SQL** (the most widely known dialect).
        When running on Polars or DuckDB, sqlglot auto-converts standard functions.
        For Spark-specific functions missing in sqlglot (e.g. try_to_date, timestamp_micros),
        we apply regex polyfills before transpilation.

        Args:
            derive: TransformationDerive instance with .sql, .sql_duckdb, .sql_spark.

        Returns:
            SQL expression string ready for the current engine.
        """
        target = self._resolve_dialect()

        # 1. Explicit engine override wins
        if target in ("duckdb",) and getattr(derive, "sql_duckdb", None):
            return derive.sql_duckdb
        if target in ("spark", "databricks") and getattr(derive, "sql_spark", None):
            return derive.sql_spark

        # 2. Auto-transpile from Spark → target dialect
        raw_sql = derive.sql
        if target not in ("spark", "databricks"):
            try:
                import re

                # Polyfill: Spark-specific functions to DuckDB equivalents
                if target == "duckdb":
                    # timestamp_micros(x) -> make_timestamp(CAST(x AS BIGINT))
                    raw_sql = re.sub(
                        r"timestamp_micros\s*\((.*?)\)",
                        r"make_timestamp(CAST(\1 AS BIGINT))",
                        raw_sql,
                        flags=re.IGNORECASE,
                    )

                    # try_to_date(x, 'yyyyMMdd') -> strptime(x, '%Y%m%d')::date
                    def _date_repl(m):
                        arg1, fmt = m.group(1), m.group(2)
                        fmt = fmt.replace("yyyy", "%Y").replace("MM", "%m").replace("dd", "%d")
                        return f"strptime({arg1}, '{fmt}')::DATE"

                    raw_sql = re.sub(
                        r"try_to_date\s*\(\s*(.*?)\s*,\s*'(.*?)'\s*\)",
                        _date_repl,
                        raw_sql,
                        flags=re.IGNORECASE,
                    )

                transpiled = sqlglot.transpile(raw_sql, read="spark", write=target)
                if transpiled and transpiled[0] != derive.sql:
                    logger.debug(f"Auto-transpiled derive SQL: {derive.sql!r} → {transpiled[0]!r}")
                    return transpiled[0]
            except Exception as e:
                logger.debug(f"sqlglot transpile failed for derive: {derive.sql!r} → {target}: {e}")

        # 3. Fallback: return raw SQL as-is
        return raw_sql

    def _format_literal(self, value: Any) -> str:
        """
        Format a literal for SQL.

        Args:
            value: Python value.

        Returns:
            SQL literal string.
        """
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        text = str(value).replace("'", "''")
        return f"'{text}'"

    def _regex_sql(self, field: str, pattern: str) -> str:
        """
        Build an engine-specific regex SQL clause.

        Uses sqlglot transpilation from a DuckDB-dialect REGEXP_MATCHES
        expression to the target engine's equivalent.

        Args:
            field: Column name.
            pattern: Regex pattern.

        Returns:
            SQL boolean expression.
        """
        qfield = self._quote_ident(field)
        # Write in DuckDB dialect, let sqlglot transpile to target
        duckdb_sql = f"REGEXP_MATCHES({qfield}, '{pattern}')"
        return self._transpile(duckdb_sql, read_dialect="duckdb")

    def _quote_ident(self, name: str) -> str:
        """
        Quote an identifier for SQL generation in rule expressions.

        Default uses ANSI double quotes which are supported by DuckDB/Snowflake
        and accepted by Polars SQL. Engines can override if needed.
        """
        text = str(name)
        if text.startswith('"') and text.endswith('"'):
            return text
        escaped = text.replace('"', '""')
        return f'"{escaped}"'

    def _lineage_columns(self) -> set[str]:
        """
        Return lineage column names to ignore during schema drift checks.
        """
        lineage = getattr(self.contract, "lineage", None)
        if not lineage or not getattr(lineage, "enabled", False):
            return set()

        cols: set[str] = set()
        if getattr(lineage, "capture_source_path", False):
            cols.add(lineage.source_column_name)
        if getattr(lineage, "capture_timestamp", False):
            cols.add(lineage.timestamp_column_name)
        if getattr(lineage, "capture_run_id", False):
            cols.add(lineage.run_id_column_name)
        if getattr(lineage, "capture_contract_name", False):
            cols.add(lineage.contract_name_column_name)
        if getattr(lineage, "capture_domain", False):
            cols.add(lineage.domain_column_name)
        if getattr(lineage, "capture_system", False):
            cols.add(lineage.system_column_name)
        return cols

    def _build_rollup_sql(self, rollup: Any, source_table: str = "source") -> str:
        """
        Build SQL for a rollup transformation.

        Args:
            rollup: TransformationRollup config.
            source_table: Source table/view name.

        Returns:
            SQL query string.
        """
        group_by = list(getattr(rollup, "group_by", []) or [])
        aggregations = dict(getattr(rollup, "aggregations", {}) or {})
        keys = getattr(rollup, "keys", None)
        key_expr = getattr(rollup, "key_expr", None)
        rollup_keys_col = getattr(rollup, "rollup_keys_column", None)
        rollup_keys_count_col = getattr(rollup, "rollup_keys_count_column", None)
        upstream_run_id_col = getattr(rollup, "upstream_run_id_column", None)
        upstream_run_ids_col = getattr(rollup, "upstream_run_ids_column", None)
        distinct = bool(getattr(rollup, "distinct", True))

        select_parts: List[str] = []
        group_exprs = [self._quote_ident(col) for col in group_by]
        select_parts.extend(group_exprs)

        for output_name, expr in aggregations.items():
            if not expr:
                continue
            select_parts.append(f"{expr} AS {self._quote_ident(output_name)}")

        if not key_expr:
            if isinstance(keys, str):
                keys = [keys]
            if keys:
                key_cols = [self._quote_ident(k) for k in keys]
                if len(key_cols) == 1:
                    key_expr = key_cols[0]
                else:
                    key_expr = f"CONCAT_WS('||', {', '.join(key_cols)})"

        distinct_sql = "DISTINCT " if distinct else ""

        if key_expr and rollup_keys_col:
            select_parts.append(f"ARRAY_AGG({distinct_sql}{key_expr}) AS {self._quote_ident(rollup_keys_col)}")
            if rollup_keys_count_col:
                select_parts.append(f"COUNT({distinct_sql}{key_expr}) AS {self._quote_ident(rollup_keys_count_col)}")

        if upstream_run_id_col and upstream_run_ids_col:
            uid_q = self._quote_ident(upstream_run_id_col)
            uids_q = self._quote_ident(upstream_run_ids_col)
            select_parts.append(f"ARRAY_AGG({distinct_sql}{uid_q}) AS {uids_q}")

        if not select_parts:
            select_parts = ["*"]

        sql = f"SELECT {', '.join(select_parts)} FROM {source_table}"
        if group_exprs:
            sql += f" GROUP BY {', '.join(group_exprs)}"
        return sql

    def _pivot_agg_expr(self, agg: str, expr: str) -> str:
        """
        Build an aggregate expression for pivot operations.

        Args:
            agg: Aggregate function name.
            expr: SQL expression to aggregate.

        Returns:
            SQL aggregate expression.
        """
        agg_norm = (agg or "first").lower().strip()
        engine = self._normalize_engine()

        if agg_norm in {"first", "any_value"}:
            func = "ANY_VALUE" if engine in {"bigquery", "snowflake"} else "FIRST"
            return f"{func}({expr})"
        if agg_norm == "last":
            func = "ANY_VALUE" if engine in {"bigquery", "snowflake"} else "LAST"
            return f"{func}({expr})"
        if agg_norm in {"count_distinct", "distinct_count"}:
            return f"COUNT(DISTINCT {expr})"
        if agg_norm == "count":
            return f"COUNT({expr})"
        if agg_norm == "sum":
            return f"SUM({expr})"
        if agg_norm in {"avg", "mean"}:
            return f"AVG({expr})"
        if agg_norm == "min":
            return f"MIN({expr})"
        if agg_norm == "max":
            return f"MAX({expr})"

        func = "ANY_VALUE" if engine in {"bigquery", "snowflake"} else "FIRST"
        return f"{func}({expr})"

    def _build_pivot_sql(self, pivot: Any, source_table: str = "source") -> Optional[str]:
        """
        Build SQL for a pivot transformation using conditional aggregation.

        Args:
            pivot: TransformationPivot config.
            source_table: Source table/view name.

        Returns:
            SQL query string or None.
        """
        if not pivot:
            return None

        pivot_col = getattr(pivot, "pivot_col", None)
        pivot_cols = getattr(pivot, "pivot_cols", None) or []
        if not pivot_col and pivot_cols:
            if len(pivot_cols) == 1:
                pivot_col = pivot_cols[0]
        if not pivot_col:
            logger.warning("Pivot transformation skipped; pivot_col is required.")
            return None

        value_cols = getattr(pivot, "value_cols", None) or []
        if not value_cols and getattr(pivot, "value_col", None):
            value_cols = [pivot.value_col]
        if not value_cols:
            logger.warning("Pivot transformation skipped; value_cols is required.")
            return None

        values = list(getattr(pivot, "values", None) or [])
        if not values and getattr(pivot, "pivot_values", None):
            values = list(pivot.pivot_values)
        if not values:
            logger.warning("Pivot transformation skipped; values list is required for portable SQL pivot.")
            return None

        id_vars = list(getattr(pivot, "id_vars", None) or [])
        value_aliases = dict(getattr(pivot, "value_aliases", None) or {})
        aggs = dict(getattr(pivot, "aggs", None) or {})
        default_agg = getattr(pivot, "agg", None) or "first"
        separator = getattr(pivot, "separator", None) or "_"
        name_template = getattr(pivot, "name_template", None)
        fill_value = getattr(pivot, "fill_value", None)

        pivot_qcol = self._quote_ident(pivot_col)
        select_parts: List[str] = [self._quote_ident(col) for col in id_vars]

        for value_col in value_cols:
            agg_name = aggs.get(value_col, default_agg)
            value_expr = self._quote_ident(value_col)
            for value in values:
                alias_key = value
                alias = value_aliases.get(value) or value_aliases.get(str(value)) or value
                case_expr = f"CASE WHEN {pivot_qcol} = {self._format_literal(value)} THEN {value_expr} END"
                agg_expr = self._pivot_agg_expr(agg_name, case_expr)
                if fill_value is not None:
                    agg_expr = f"COALESCE({agg_expr}, {self._format_literal(fill_value)})"
                if name_template:
                    alias_text = str(name_template).format(
                        value_col=value_col,
                        pivot_value=alias_key,
                        pivot_alias=alias,
                    )
                else:
                    alias_text = f"{value_col}{separator}{alias}"
                select_parts.append(f"{agg_expr} AS {self._quote_ident(alias_text)}")

        if not select_parts:
            return None

        sql = f"SELECT {', '.join(select_parts)} FROM {source_table}"
        if id_vars:
            group_by = ", ".join(self._quote_ident(col) for col in id_vars)
            sql += f" GROUP BY {group_by}"
        return sql

    def _build_unpivot_sql(self, unpivot: Any, source_table: str = "source") -> Optional[str]:
        """
        Build SQL for an unpivot transformation using UNION ALL.

        Args:
            unpivot: TransformationUnpivot config.
            source_table: Source table/view name.

        Returns:
            SQL query string or None.
        """
        if not unpivot:
            return None

        id_vars = list(getattr(unpivot, "id_vars", None) or [])
        value_vars = list(getattr(unpivot, "value_vars", None) or [])
        if not value_vars and getattr(unpivot, "value_cols", None):
            value_vars = list(unpivot.value_cols)
        if not value_vars:
            logger.warning("Unpivot transformation skipped; value_vars is required.")
            return None

        key_field = getattr(unpivot, "key_field", None) or "key"
        value_field = getattr(unpivot, "value_field", None) or "value"
        include_nulls = bool(getattr(unpivot, "include_nulls", False))
        value_aliases = dict(getattr(unpivot, "value_aliases", None) or {})

        select_rows: List[str] = []
        for col in value_vars:
            alias = value_aliases.get(col) or value_aliases.get(str(col)) or col
            id_exprs = [self._quote_ident(c) for c in id_vars]
            select_exprs = id_exprs + [
                f"{self._format_literal(alias)} AS {self._quote_ident(key_field)}",
                f"{self._quote_ident(col)} AS {self._quote_ident(value_field)}",
            ]
            sql = f"SELECT {', '.join(select_exprs)} FROM {source_table}"
            if not include_nulls:
                sql += f" WHERE {self._quote_ident(col)} IS NOT NULL"
            select_rows.append(sql)

        return " UNION ALL ".join(select_rows) if select_rows else None

    def _build_bucket_sql(self, bucket: Any, source_table: str = "source") -> Optional[str]:
        """
        Build a CASE expression for bucket/bin transformations.

        Uses only CASE/WHEN/THEN/ELSE -- valid on Polars, DuckDB, Spark, Snowflake, BigQuery.
        """
        if not bucket:
            return None
        field = getattr(bucket, "field", None)
        source = getattr(bucket, "source", None)
        bins = list(getattr(bucket, "bins", None) or [])
        default = getattr(bucket, "default", None)
        if not field or not source:
            logger.warning("bucket transform skipped: 'field' and 'source' are required.")
            return None
        qsource = self._quote_ident(source)
        when_clauses: List[str] = []
        for b in bins:
            conditions: List[str] = []
            if getattr(b, "eq", None) is not None:
                conditions.append(f"{qsource} = {self._format_literal(b.eq)}")
            if getattr(b, "gte", None) is not None:
                conditions.append(f"{qsource} >= {self._format_literal(b.gte)}")
            if getattr(b, "gt", None) is not None:
                conditions.append(f"{qsource} > {self._format_literal(b.gt)}")
            if getattr(b, "lte", None) is not None:
                conditions.append(f"{qsource} <= {self._format_literal(b.lte)}")
            if getattr(b, "lt", None) is not None:
                conditions.append(f"{qsource} < {self._format_literal(b.lt)}")
            if not conditions:
                continue
            when_clauses.append(f"WHEN {' AND '.join(conditions)} THEN {self._format_literal(b.label)}")
        if not when_clauses:
            logger.warning(f"bucket '{field}': no valid bins defined.")
            return None
        else_clause = f"ELSE {self._format_literal(default)}" if default is not None else "ELSE NULL"
        case_expr = f"CASE {' '.join(when_clauses)} {else_clause} END"
        return f"SELECT *, ({case_expr}) AS {self._quote_ident(field)} FROM {source_table}"

    def _build_date_diff_sql(self, date_diff: Any, source_table: str = "source") -> Optional[str]:
        """
        Build an engine-specific date-difference expression.

        Dispatches on self.engine_name:
          polars    -> integer division of duration (STRPTIME subtraction)
          spark     -> DATEDIFF(to, from)              (reversed args, days only)
          duckdb / snowflake / bigquery -> DATEDIFF('day', from, to)
        """
        if not date_diff:
            return None
        field = getattr(date_diff, "field", None)
        from_col = getattr(date_diff, "from_col", None)
        to_col = getattr(date_diff, "to_col", None)
        unit = (getattr(date_diff, "unit", None) or "days").lower().rstrip("s")
        if not field or not from_col or not to_col:
            logger.warning("date_diff transform skipped: 'field', 'from_col', and 'to_col' are required.")
            return None
        qfrom = self._quote_ident(from_col)
        qto = self._quote_ident(to_col)
        engine = self._normalize_engine()
        if engine == "spark":
            diff_expr = f"DATEDIFF({qto}, {qfrom})"
        elif engine == "polars":
            # Polars SQL: subtracting two STRPTIME values yields duration[μs]
            # but neither DATE_PART nor EXTRACT(EPOCH FROM ...) work on
            # durations.  Instead, extract epoch seconds from each timestamp
            # *individually* (which works) and subtract the two integers.
            fmt = "%Y-%m-%d"
            epoch_to = f"EXTRACT(EPOCH FROM STRPTIME(SUBSTR({qto}, 1, 10), '{fmt}'))"
            epoch_from = f"EXTRACT(EPOCH FROM STRPTIME(SUBSTR({qfrom}, 1, 10), '{fmt}'))"
            seconds_expr = f"({epoch_to} - {epoch_from})"
            if unit == "day":
                diff_expr = f"CAST({seconds_expr} / 86400 AS INTEGER)"
            elif unit == "hour":
                diff_expr = f"CAST({seconds_expr} / 3600 AS INTEGER)"
            elif unit == "minute":
                diff_expr = f"CAST({seconds_expr} / 60 AS INTEGER)"
            elif unit == "second":
                diff_expr = f"CAST({seconds_expr} AS INTEGER)"
            else:
                diff_expr = f"CAST({seconds_expr} / 86400 AS INTEGER)"
        else:
            diff_expr = f"DATEDIFF('{unit}', {qfrom}, {qto})"
        return f"SELECT *, ({diff_expr}) AS {self._quote_ident(field)} FROM {source_table}"

    def _expand_row_rule(self, spec: Any) -> Optional[Any]:
        """
        Expand structured row rule specs into QualityRule objects.

        Args:
            spec: Rule spec or QualityRule.

        Returns:
            QualityRule, list of QualityRule, or None.
        """
        if isinstance(spec, QualityRule):
            return spec

        def _parse_payload(payload: Any) -> Tuple[Optional[str], Dict[str, Any]]:
            if isinstance(payload, str):
                return payload, {}
            if isinstance(payload, dict):
                return payload.get("field") or payload.get("column"), payload
            return None, {}

        def _build_not_null_rule(field: str, cfg: Dict[str, Any]) -> QualityRule:
            name = cfg.get("name") or f"{field}_not_null"
            qfield = self._quote_ident(field)
            return QualityRule(
                name=name,
                sql=f"{qfield} IS NOT NULL",
                category=cfg.get("category", "completeness"),
                description=cfg.get("description"),
                severity=cfg.get("severity", "error"),
            )

        if isinstance(spec, RowRuleNotNull):
            payload = spec.not_null
            rules: List[QualityRule] = []

            if isinstance(payload, list):
                for item in payload:
                    field, cfg = _parse_payload(item)
                    if not field:
                        continue
                    rules.append(_build_not_null_rule(field, cfg))
                return rules or None

            if isinstance(payload, dict):
                fields = payload.get("fields") or payload.get("columns")
                if isinstance(fields, list):
                    base_cfg = {k: v for k, v in payload.items() if k not in {"fields", "columns", "field", "column"}}
                    for item in fields:
                        if isinstance(item, dict):
                            field, cfg = _parse_payload(item)
                            if not field:
                                continue
                            merged = {**base_cfg, **cfg}
                            rules.append(_build_not_null_rule(field, merged))
                        else:
                            field = str(item)
                            rules.append(_build_not_null_rule(field, base_cfg))
                    return rules or None

            field, cfg = _parse_payload(payload)
            if not field:
                return None
            return _build_not_null_rule(field, cfg)

        if isinstance(spec, RowRuleAcceptedValues):
            cfg = spec.accepted_values or {}
            field = cfg.get("field") or cfg.get("column")
            values = cfg.get("values")
            if not field or not values:
                return None
            value_sql = ", ".join(self._format_literal(v) for v in values)
            name = cfg.get("name") or f"{field}_accepted_values"
            qfield = self._quote_ident(field)
            return QualityRule(
                name=name,
                sql=f"{qfield} IN ({value_sql})",
                category=cfg.get("category", "consistency"),
                description=cfg.get("description"),
                severity=cfg.get("severity", "error"),
            )

        if isinstance(spec, RowRuleRegexMatch):
            cfg = spec.regex_match or {}
            field = cfg.get("field") or cfg.get("column")
            pattern = cfg.get("pattern")
            if not field or pattern is None:
                return None
            name = cfg.get("name") or f"{field}_regex_match"
            return QualityRule(
                name=name,
                sql=self._regex_sql(field, pattern),
                category=cfg.get("category", "correctness"),
                description=cfg.get("description"),
                severity=cfg.get("severity", "error"),
            )

        if isinstance(spec, RowRuleRange):
            cfg = spec.range or {}
            field = cfg.get("field") or cfg.get("column")
            if not field:
                return None
            minimum = cfg.get("min")
            maximum = cfg.get("max")
            inclusive = cfg.get("inclusive", True)
            clauses = []
            qfield = self._quote_ident(field)
            if minimum is not None:
                op = ">=" if inclusive else ">"
                clauses.append(f"{qfield} {op} {self._format_literal(minimum)}")
            if maximum is not None:
                op = "<=" if inclusive else "<"
                clauses.append(f"{qfield} {op} {self._format_literal(maximum)}")
            if not clauses:
                return None
            name = cfg.get("name") or f"{field}_range"
            sql = " AND ".join(clauses)
            return QualityRule(
                name=name,
                sql=sql,
                category=cfg.get("category", "correctness"),
                description=cfg.get("description"),
                severity=cfg.get("severity", "error"),
            )

        if isinstance(spec, RowRuleReferentialIntegrity):
            cfg = spec.referential_integrity or {}
            field = cfg.get("field") or cfg.get("column")
            reference = cfg.get("reference")
            key = cfg.get("key")
            if not field or not reference or not key:
                return None
            name = cfg.get("name") or f"{field}_referential_integrity"
            qfield = self._quote_ident(field)
            qkey = self._quote_ident(key)
            return QualityRule(
                name=name,
                sql=f"{qfield} IN (SELECT {qkey} FROM {reference})",
                category=cfg.get("category", "consistency"),
                description=cfg.get("description"),
                severity=cfg.get("severity", "error"),
            )

        if isinstance(spec, RowRuleLifecycleWindow):
            cfg = spec.lifecycle_window or {}
            event_ts = cfg.get("event_ts") or cfg.get("timestamp_field")
            event_key = cfg.get("event_key") or cfg.get("field")
            reference = cfg.get("reference") or cfg.get("dim_table")
            reference_key = cfg.get("reference_key") or cfg.get("dim_key")
            start_field = cfg.get("start_field") or "start_date"
            end_field = cfg.get("end_field") or "end_date"
            end_default = cfg.get("end_default") or "9999-12-31"
            if not event_ts or not event_key or not reference or not reference_key:
                return None
            name = cfg.get("name") or f"{event_key}_lifecycle_window"
            q_event_ts = self._quote_ident(event_ts)
            q_event_key = self._quote_ident(event_key)
            q_ref_key = self._quote_ident(reference_key)
            q_start = self._quote_ident(start_field)
            q_end = self._quote_ident(end_field)
            end_literal = self._format_literal(end_default)
            sql = (
                f"{q_event_ts} >= (SELECT r.{q_start} FROM {reference} r "
                f"WHERE r.{q_ref_key} = {q_event_key}) AND "
                f"COALESCE((SELECT r.{q_end} FROM {reference} r "
                f"WHERE r.{q_ref_key} = {q_event_key}), {end_literal}) >= {q_event_ts}"
            )
            return QualityRule(
                name=name,
                sql=sql,
                category=cfg.get("category", "consistency"),
                description=cfg.get("description"),
                severity=cfg.get("severity", "error"),
            )

        return None

    def _expand_dataset_rule(self, spec: Any) -> Optional[QualityRule]:
        """
        Expand structured dataset rule specs into QualityRule objects.

        Args:
            spec: Rule spec or QualityRule.

        Returns:
            QualityRule or None.
        """
        if isinstance(spec, QualityRule):
            return spec

        dataset = self.contract.dataset or "source"

        if isinstance(spec, DatasetRuleUnique):
            payload = spec.unique
            field = payload if isinstance(payload, str) else payload.get("field")
            cfg = payload if isinstance(payload, dict) else {}
            if not field:
                return None
            name = cfg.get("name") or f"{field}_unique"
            qfield = self._quote_ident(field)
            return QualityRule(
                name=name,
                sql=f"SELECT COUNT(*) - COUNT(DISTINCT {qfield}) FROM {dataset}",
                category=cfg.get("category", "consistency"),
                description=cfg.get("description"),
                severity=cfg.get("severity", "error"),
                must_be_less_than=1,
            )

        if isinstance(spec, DatasetRuleNullRatio):
            cfg = spec.null_ratio or {}
            field = cfg.get("field") or cfg.get("column")
            threshold = cfg.get("max") or cfg.get("threshold")
            if not field or threshold is None:
                return None
            threshold_val = float(threshold)
            if threshold_val > 1:
                threshold_val = threshold_val / 100.0
            name = cfg.get("name") or f"{field}_null_ratio"
            qfield = self._quote_ident(field)
            sql = f"SELECT SUM(CASE WHEN {qfield} IS NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0) FROM {dataset}"
            return QualityRule(
                name=name,
                sql=sql,
                category=cfg.get("category", "completeness"),
                description=cfg.get("description"),
                severity=cfg.get("severity", "error"),
                must_be_less_than=threshold_val,
            )

        if isinstance(spec, DatasetRuleRowCountBetween):
            cfg = spec.row_count_between or {}
            minimum = cfg.get("min")
            maximum = cfg.get("max")
            if minimum is None and maximum is None:
                return None
            name = cfg.get("name") or "row_count_between"
            rule = QualityRule(
                name=name,
                sql=f"SELECT COUNT(*) FROM {dataset}",
                category=cfg.get("category", "completeness"),
                description=cfg.get("description"),
                severity=cfg.get("severity", "error"),
            )
            if minimum is not None and maximum is not None:
                rule.must_be_between = [minimum, maximum]
            elif minimum is not None:
                rule.must_be_greater_than = minimum
            elif maximum is not None:
                rule.must_be_less_than = maximum
            return rule

        return None
