from abc import ABC, abstractmethod
from typing import Any, Tuple, List, Dict, Optional
from lakelogic.core.models import (
    DataContract,
    QualityRule,
    RowRuleNotNull,
    RowRuleAcceptedValues,
    RowRuleRegexMatch,
    RowRuleRange,
    RowRuleReferentialIntegrity,
    RowRuleLifecycleWindow,
    DatasetRuleUnique,
    DatasetRuleNullRatio,
    DatasetRuleRowCountBetween,
)

class EngineAdapter(ABC):
    """
    Abstract Base Class for all execution engines.
    """
    
    ERROR_COLUMN = "_lakelogic_errors"
    CATEGORY_COLUMN = "_lakelogic_categories"

    def __init__(self, contract: DataContract):
        """
        Initialize adapter with a contract.

        Args:
            contract: DataContract instance.
        """
        self.contract = contract
        self.dataset_rule_results: List[Dict[str, Any]] = []
        self.schema_drift: Dict[str, List[str]] = {}
        self.engine_name: str = ""

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

    def get_row_rules(self) -> List[QualityRule]:
        """
        Returns all rules that should trigger row-level quarantine.
        """
        rules: List[QualityRule] = []

        enforce_required = True
        if self.contract.quality is not None:
            enforce_required = bool(getattr(self.contract.quality, "enforce_required", True))

        if enforce_required and self.contract.model and self.contract.model.fields:
            for field in self.contract.model.fields:
                if field.required:
                    rules.append(
                        QualityRule(
                            name=f"{field.name}_required",
                            sql=f"{self._quote_ident(field.name)} IS NOT NULL",
                            category="completeness",
                            description=f"{field.name} is required"
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

        Args:
            field: Column name.
            pattern: Regex pattern.

        Returns:
            SQL boolean expression.
        """
        engine = self._normalize_engine()
        qfield = self._quote_ident(field)
        if engine == "spark":
            return f"{qfield} RLIKE '{pattern}'"
        if engine == "bigquery":
            return f"REGEXP_CONTAINS({qfield}, r'{pattern}')"
        if engine == "snowflake":
            return f"REGEXP_LIKE({qfield}, '{pattern}')"
        if engine == "duckdb":
            return f"REGEXP_MATCHES({qfield}, '{pattern}')"
        return f"REGEXP_LIKE({qfield}, '{pattern}')"

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
                    key_expr = f\"CONCAT_WS('||', {', '.join(key_cols)})\"

        distinct_sql = "DISTINCT " if distinct else ""

        if key_expr and rollup_keys_col:
            select_parts.append(
                f\"ARRAY_AGG({distinct_sql}{key_expr}) AS {self._quote_ident(rollup_keys_col)}\"
            )
            if rollup_keys_count_col:
                select_parts.append(
                    f\"COUNT({distinct_sql}{key_expr}) AS {self._quote_ident(rollup_keys_count_col)}\"
                )

        if upstream_run_id_col and upstream_run_ids_col:
            select_parts.append(
                f\"ARRAY_AGG({distinct_sql}{self._quote_ident(upstream_run_id_col)}) AS {self._quote_ident(upstream_run_ids_col)}\"
            )

        if not select_parts:
            select_parts = ["*"]

        sql = f\"SELECT {', '.join(select_parts)} FROM {source_table}\"
        if group_exprs:
            sql += f\" GROUP BY {', '.join(group_exprs)}\"
        return sql

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
            sql = (
                f"SELECT SUM(CASE WHEN {qfield} IS NULL THEN 1 ELSE 0 END) "
                f"/ NULLIF(COUNT(*), 0) FROM {dataset}"
            )
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
