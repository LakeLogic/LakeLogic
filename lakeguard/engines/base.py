from abc import ABC, abstractmethod
from typing import Any, Tuple, List, Dict, Optional
from lakeguard.core.models import (
    DataContract,
    QualityRule,
    RowRuleNotNull,
    RowRuleAcceptedValues,
    RowRuleRegexMatch,
    RowRuleRange,
    RowRuleReferentialIntegrity,
    DatasetRuleUnique,
    DatasetRuleNullRatio,
    DatasetRuleRowCountBetween,
)

class EngineAdapter(ABC):
    """
    Abstract Base Class for all execution engines.
    """
    
    ERROR_COLUMN = "_lakeguard_errors"
    CATEGORY_COLUMN = "_lakeguard_categories"

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

        if self.contract.model and self.contract.model.fields:
            for field in self.contract.model.fields:
                if field.required:
                    rules.append(
                        QualityRule(
                            name=f"{field.name}_required",
                            sql=f"{field.name} IS NOT NULL",
                            category="completeness",
                            description=f"{field.name} is required"
                        )
                    )
                if field.rules:
                    rules.extend(field.rules)

        if self.contract.quality and self.contract.quality.row_rules:
            for spec in self.contract.quality.row_rules:
                expanded = self._expand_row_rule(spec)
                if expanded:
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
        if engine == "spark":
            return f"{field} RLIKE '{pattern}'"
        if engine == "bigquery":
            return f"REGEXP_CONTAINS({field}, r'{pattern}')"
        if engine == "snowflake":
            return f"REGEXP_LIKE({field}, '{pattern}')"
        if engine == "duckdb":
            return f"REGEXP_MATCHES({field}, '{pattern}')"
        return f"REGEXP_LIKE({field}, '{pattern}')"

    def _expand_row_rule(self, spec: Any) -> Optional[QualityRule]:
        """
        Expand structured row rule specs into QualityRule objects.

        Args:
            spec: Rule spec or QualityRule.

        Returns:
            QualityRule or None.
        """
        if isinstance(spec, QualityRule):
            return spec

        def _parse_payload(payload: Any) -> Tuple[Optional[str], Dict[str, Any]]:
            if isinstance(payload, str):
                return payload, {}
            if isinstance(payload, dict):
                return payload.get("field") or payload.get("column"), payload
            return None, {}

        if isinstance(spec, RowRuleNotNull):
            field, cfg = _parse_payload(spec.not_null)
            if not field:
                return None
            name = cfg.get("name") or f"{field}_not_null"
            return QualityRule(
                name=name,
                sql=f"{field} IS NOT NULL",
                category=cfg.get("category", "completeness"),
                description=cfg.get("description"),
                severity=cfg.get("severity", "error"),
            )

        if isinstance(spec, RowRuleAcceptedValues):
            cfg = spec.accepted_values or {}
            field = cfg.get("field") or cfg.get("column")
            values = cfg.get("values")
            if not field or not values:
                return None
            value_sql = ", ".join(self._format_literal(v) for v in values)
            name = cfg.get("name") or f"{field}_accepted_values"
            return QualityRule(
                name=name,
                sql=f"{field} IN ({value_sql})",
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
            if minimum is not None:
                op = ">=" if inclusive else ">"
                clauses.append(f"{field} {op} {self._format_literal(minimum)}")
            if maximum is not None:
                op = "<=" if inclusive else "<"
                clauses.append(f"{field} {op} {self._format_literal(maximum)}")
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
            return QualityRule(
                name=name,
                sql=f"{field} IN (SELECT {key} FROM {reference})",
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
            return QualityRule(
                name=name,
                sql=f"SELECT COUNT(*) - COUNT(DISTINCT {field}) FROM {dataset}",
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
            sql = (
                f"SELECT SUM(CASE WHEN {field} IS NULL THEN 1 ELSE 0 END) "
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
