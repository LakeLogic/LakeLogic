from pydantic import (
    BaseModel,
    Field,
    ConfigDict,
    AliasChoices,
    field_validator,
    model_validator,
)
from pathlib import Path
from typing import List, Optional, Dict, Any, Union
from loguru import logger


def _warn_unknown_extra_keys(
    instance: Any,
    known_keys: set,
    block_name: str,
) -> None:
    """Log a warning for each unrecognised key in a Pydantic model's extras.

    Pydantic models with ``extra="allow"`` accept unknown keys silently.
    This helper inspects ``__pydantic_extra__`` and uses fuzzy matching
    (``difflib.get_close_matches``) to suggest corrections for likely
    typos.  Called from ``model_validator(mode="after")`` on models
    where mistypes are especially dangerous.

    Parameters
    ----------
    instance : BaseModel
        The constructed Pydantic model instance.
    known_keys : set[str]
        Set of valid key names for this model.
    block_name : str
        Human-readable block name for the log message (e.g. "source",
        "materialization").
    """
    import os as _os

    if _os.environ.get("LAKELOGIC_SKIP_KEY_WARNINGS", "").strip() not in ("", "0"):
        return

    extras = getattr(instance, "__pydantic_extra__", None) or {}
    if not extras:
        return

    from difflib import get_close_matches

    for key in extras:
        if key.startswith("_"):
            continue
        if key in known_keys:
            continue

        matches = get_close_matches(key, known_keys, n=1, cutoff=0.6)
        if matches:
            logger.warning(
                f"Unknown key '{key}' in '{block_name}' block — did you mean "
                f"'{matches[0]}'? This key will be ignored by LakeLogic."
            )
        else:
            logger.warning(
                f"Unknown key '{key}' in '{block_name}' block — this key "
                f"will be ignored by LakeLogic. "
                f"Known keys: {', '.join(sorted(known_keys))}"
            )


_QUALITY_CATEGORIES = {
    "correctness",
    "completeness",
    "consistency",
    "validity",
    "accuracy",
    "timeliness",
    "uniqueness",
    "integrity",
    "schema",
    "rule",
}

_QUALITY_CATEGORY_SYNONYMS = {
    "complete": "completeness",
    "consistant": "consistency",
    "consistent": "consistency",
    "valid": "validity",
    "accurate": "accuracy",
    "timely": "timeliness",
    "unique": "uniqueness",
    "referential_integrity": "integrity",
    "referential": "integrity",
}

# ── Tier normalization ──────────────────────────────────────────────────────
# Maps common naming conventions to canonical medallion tiers.
# Users can write any of these in their contract and it will be normalized.
TIER_CANONICAL_MAP = {
    # Canonical names
    "bronze": "bronze",
    "silver": "silver",
    "gold": "gold",
    # Alternative naming: raw / stage / curated
    "raw": "bronze",
    "stage": "silver",
    "staging": "silver",
    "curated": "gold",
    # Alternative naming: landing / cleansed / refined
    "landing": "bronze",
    "cleansed": "silver",
    "refined": "gold",
    # Alternative naming: ingestion / transform / presentation
    "ingestion": "bronze",
    "ingest": "bronze",
    "transform": "silver",
    "presentation": "gold",
    "consumption": "gold",
    # Reference data (cross-layer)
    "reference": "reference",
    "ref": "reference",
    "seed": "reference",
    "lookup": "reference",
    "masterdata": "reference",
    "master_data": "reference",
}

TIER_VALID_CANONICAL = {"bronze", "silver", "gold", "reference"}


# -- OLC-authored spec shapes (single source of truth) --------------------
# These field-shapes are authored ONCE in the Open Lakehouse Contract
# (olc.models._nested) and re-exported here, so adding/changing a field
# happens in OLC only. Only shapes with a fully behavior-free subtree are
# inverted here; shapes carrying runtime validators/methods stay below
# and will invert via subclassing in a later phase.
from olc.models import _nested as _olcn  # noqa: E402  (base classes for runtime subclasses)
from olc.models._nested import (  # noqa: E402,F401
    TransformationDeduplicateByLatest,
    DltEndpointConfig,
    SourcePartition,
    PreprocessingConfig,
    TransformationDeduplicate,
    DownstreamConsumer,
    Quarantine,
    UpstreamContractRef,
    UpstreamSource,
    Notification,
    ConfidenceConfig,
    DatasetRuleNullRatio,
    DatasetRuleRowCountBetween,
    DatasetRuleUnique,
    Environment,
    ExternalLogic,
    FactConfig,
    ForeignKeyRef,
    Info,
    Link,
    PostIngestionConfig,
    RetryConfig,
    RowCountSLO,
    RowRuleAcceptedValues,
    RowRuleLifecycleWindow,
    RowRuleNotNull,
    RowRuleRange,
    RowRuleReferentialIntegrity,
    RowRuleRegexMatch,
    SchemaPolicy,
    ServiceLevel,
    ServiceLevelObjective,
    TransformationBucket,
    TransformationBucketBin,
    TransformationCast,
    TransformationCoalesce,
    TransformationDateDiff,
    TransformationDateRangeExplode,
    TransformationDerive,
    TransformationDrop,
    TransformationExplode,
    TransformationJoin,
    TransformationJsonExtract,
    TransformationLookup,
    TransformationLower,
    TransformationMapValues,
    TransformationRollup,
    TransformationSelect,
    TransformationSplit,
    TransformationTrim,
    TransformationUpper,
)


class Server(BaseModel):
    """Storage and ingestion settings for a contract."""

    type: str  # s3, gcs, adls, azure, local, glue
    format: str = "parquet"  # parquet, delta, iceberg, csv, json
    path: str  # e.g. s3://bucket/path, gs://bucket/path, abfss://container@account...

    # Ingestion Controls
    mode: str = "validate"  # 'validate' for Quality Gate, 'ingest' for Raw-to-Bronze movement
    schema_policy: Optional[SchemaPolicy] = Field(default_factory=SchemaPolicy)
    cast_to_string: bool = False

    # Landing zone lifecycle (Bronze layer only)
    post_ingestion: Optional[PostIngestionConfig] = None


class DltSourceConfig(_olcn.DltSourceConfig):
    """DLT-specific source configuration, embedded in SourceConfig.

    Supports two modes:

    Mode 1 — Verified Source::

        source:
          type: dlt
          dlt:
            source: stripe_analytics
            resource: charges
            credentials:
              api_key: ${STRIPE_API_KEY}

    Mode 2 — Declarative REST API::

        source:
          type: dlt
          dlt:
            base_url: https://api.example.com/v1/
            credentials:
              api_key: ${API_KEY}
            endpoints:
              - name: users
                path: users
                params:
                  limit: 100
    """

    model_config = ConfigDict(extra="allow")

    @model_validator(mode="after")
    def _validate_mode(self) -> "DltSourceConfig":
        if not self.source and not self.base_url:
            raise ValueError("dlt source must specify either 'source' (verified source) or 'base_url' (REST API mode)")
        return self


class SourceConfig(_olcn.SourceConfig):
    """Source acquisition settings for landing/stream/table/dlt inputs."""

    model_config = ConfigDict(extra="allow")
    dlt: Optional[DltSourceConfig] = None
    _SOURCE_KNOWN_KEYS: set = {
        "type",
        "query",
        "path",
        "format",
        "load_mode",
        "pattern",
        "watermark_field",
        "cdc_op_field",
        "cdc_delete_values",
        "cdc_timestamp_field",
        "partition",
        "options",
        "watermark_strategy",
        "target_path",
        "lookback",
        "from_date",
        "to_date",
        "pipeline_log_table",
        "pipeline_name",
        "manifest_path",
        "watermark_date_parts",
        "partition_filters",
        "flatten_nested",
        "dlt",
        "post_ingestion",
    }

    @model_validator(mode="after")
    def _warn_unknown_keys(self) -> "SourceConfig":
        _warn_unknown_extra_keys(self, self._SOURCE_KNOWN_KEYS, "source")
        return self


class TransformationRename(_olcn.TransformationRename):
    """Rename a column prior to validation."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    # Keys that carry rename configuration rather than a rename PAIR. Anything else
    # in extras is treated as the `{old: new}` shorthand.
    _RENAME_CONFIG_KEYS = {"from_name", "to_name", "mappings", "from", "to", "phase"}

    def iter_pairs(self) -> List[tuple[str, str]]:
        if self.mappings:
            return [(src, dst) for src, dst in self.mappings.items() if src and dst]
        if self.from_name and self.to_name:
            return [(self.from_name, self.to_name)]

        # Bare shorthand: `rename: {old_status: status}`.
        #
        # The model allows extra keys, so this form VALIDATED cleanly and then did
        # nothing at all — every engine calls iter_pairs(), which only knew about
        # `mappings` and `from_name`/`to_name`. The column was never renamed, and any
        # rule referencing the new name failed on a column that did not exist. A
        # contract that silently does nothing is worse than one that is rejected.
        extras = self.model_extra or {}
        pairs = [
            (src, dst)
            for src, dst in extras.items()
            if src and isinstance(dst, str) and dst and src not in self._RENAME_CONFIG_KEYS
        ]
        if pairs:
            # Applied, but NOT canonical OLC: the strict model rejects it outright
            # ("unknown key(s) not permitted ... transformations.rename.old_status").
            # So this form works on the lenient runtime and fails the spec gate —
            # exactly the kind of divergence a user should hear about at the point of
            # use, not at CI. Doing what they meant AND naming the constraint beats
            # both silently ignoring it and silently accepting it.
            logger.warning(
                f"rename uses the non-canonical shorthand {{{', '.join(f'{s}: {d}' for s, d in pairs)}}}. "
                "It is applied here, but strict/canonical OLC validation REJECTS it. "
                "Use `rename: {mappings: {old: new}}` (or from_name/to_name) to be "
                "portable."
            )
        return pairs


class TransformationFilter(_olcn.TransformationFilter):
    """Row-level filter expressed in SQL."""

    @model_validator(mode="before")
    @classmethod
    def _accept_string_shorthand(cls, data: Any) -> Any:
        """Accept ``filter: 'SQL'`` as shorthand for ``filter: {sql: 'SQL'}``."""
        if isinstance(data, str):
            return {"sql": data}
        return data


class TransformationPivot(_olcn.TransformationPivot):
    """Pivot rows into columns using conditional aggregation."""

    model_config = ConfigDict(extra="allow")

    @model_validator(mode="after")
    def _normalize(self):
        if not self.pivot_col and self.pivot_cols:
            if len(self.pivot_cols) == 1:
                self.pivot_col = self.pivot_cols[0]
        if not self.value_cols and self.value_col:
            self.value_cols = [self.value_col]
        if not self.values and self.pivot_values:
            self.values = list(self.pivot_values)
        return self


class TransformationUnpivot(_olcn.TransformationUnpivot):
    """Unpivot columns into rows."""

    model_config = ConfigDict(extra="allow")

    @model_validator(mode="after")
    def _normalize(self):
        if not self.value_vars and self.value_cols:
            self.value_vars = list(self.value_cols)
        return self


class Transformation(_olcn.Transformation):
    """Transformation step (SQL or structured)."""

    model_config = ConfigDict(extra="allow")
    rename: Optional[TransformationRename] = None
    filter: Optional[TransformationFilter] = None
    pivot: Optional[TransformationPivot] = None
    unpivot: Optional[TransformationUnpivot] = None


class QualityRule(_olcn.QualityRule):
    """Row-level or dataset-level quality rule."""

    @field_validator("category", mode="before")
    @classmethod
    def _normalize_category(cls, value: Any) -> str:
        if value is None:
            return "correctness"
        text = str(value).strip().lower()
        if not text:
            return "correctness"
        text = _QUALITY_CATEGORY_SYNONYMS.get(text, text)
        if text not in _QUALITY_CATEGORIES:
            logger.warning(
                f"Unknown quality rule category '{value}'. Expected one of: {', '.join(sorted(_QUALITY_CATEGORIES))}."
            )
        return text


class Quality(_olcn.Quality):
    """Quality rule groups for row and dataset checks."""

    row_rules: List[
        Union[
            QualityRule,
            RowRuleNotNull,
            RowRuleAcceptedValues,
            RowRuleRegexMatch,
            RowRuleRange,
            RowRuleReferentialIntegrity,
            RowRuleLifecycleWindow,
        ]
    ] = Field(default_factory=list)
    dataset_rules: List[
        Union[
            QualityRule,
            DatasetRuleUnique,
            DatasetRuleNullRatio,
            DatasetRuleRowCountBetween,
        ]
    ] = Field(default_factory=list)


class FieldDefinition(_olcn.FieldDefinition):
    """Schema field definition."""

    rules: List[QualityRule] = Field(default_factory=list)


class Model(_olcn.Model):
    """Schema model definition."""

    fields: List[FieldDefinition] = Field(default_factory=list)


class Materialization(_olcn.Materialization):
    """Materialization settings for writing outputs."""

    model_config = ConfigDict(extra="allow")
    _MAT_KNOWN_KEYS: set = {
        "strategy",
        "partition_by",
        "cluster_by",
        "reprocess_policy",
        "reprocess_date_column",
        "target_path",
        "format",
        "location",
        "scd2",
        "scd1",
        "fact",
        "soft_delete_column",
        "soft_delete_value",
        "soft_delete_time_column",
        "soft_delete_reason_column",
        "table_properties",
        "compaction",
        "unknown_member",
        "merge_dedup_guard",
        "secondary_targets",
        "dlt_destination",
        "dlt_credentials",
        "dlt_dataset_name",
    }

    @model_validator(mode="after")
    def _validate_strategy_alignment(self) -> "Materialization":
        """Warn when strategy and sub-config blocks are mismatched."""
        if self.strategy == "scd2" and not self.scd2:
            logger.warning(
                "materialization.strategy is 'scd2' but no 'scd2:' config block is defined. "
                "LakeLogic requires track_columns, timestamp_field, etc. in the scd2 block."
            )
        if self.scd2 and self.strategy != "scd2":
            logger.warning(
                f"materialization.scd2 block is defined but strategy is '{self.strategy}', not 'scd2'. "
                "The scd2 block will be ignored."
            )
        return self

    @model_validator(mode="after")
    def _warn_unknown_keys(self) -> "Materialization":
        _warn_unknown_extra_keys(self, self._MAT_KNOWN_KEYS, "materialization")
        return self


# ── LLM Extraction Models ────────────────────────────────────────────────────


# ExtractionField is intentionally NOT a separate class.
# output_schema reuses FieldDefinition for consistency with model.fields.
# Extraction-specific hints (extraction_task, extraction_examples, max_length)
# are optional fields on FieldDefinition itself.


class ExtractionConfig(_olcn.ExtractionConfig):
    """
    LLM extraction configuration for unstructured data processing.

    Turns raw unstructured content (text, PDFs, images, audio, video)
    into structured rows via LLM, governed by the data contract.

    Example YAML:
        extraction:
          provider: openai
          model: gpt-4o-mini
          temperature: 0.1
          prompt_template: |
            Extract the following from this support ticket:
            {{ ticket_body }}
          output_schema:
            - name: sentiment
              type: string
              enum: [positive, neutral, negative]
          source:
            text_column: ticket_body
          confidence:
            min_threshold: 0.8
    """

    model_config = ConfigDict(extra="allow")
    output_schema: List[FieldDefinition] = Field(default_factory=list)


class LineageConfig(_olcn.LineageConfig):
    """Lineage capture settings."""

    model_config = ConfigDict(extra="allow")
    _LINEAGE_KNOWN_KEYS: set = {
        "enabled",
        "capture_source_path",
        "capture_timestamp",
        "capture_run_id",
        "source_column_name",
        "timestamp_column_name",
        "run_id_column_name",
        "capture_contract_name",
        "contract_name_column_name",
        "capture_domain",
        "capture_system",
        "domain_column_name",
        "system_column_name",
        "capture_created_at",
        "created_at_column_name",
        "capture_created_by",
        "created_by_column_name",
        "created_by_override",
        "preserve_upstream",
        "upstream_prefix",
        "run_id_source",
    }

    @model_validator(mode="after")
    def _warn_unknown_keys(self) -> "LineageConfig":
        _warn_unknown_extra_keys(self, self._LINEAGE_KNOWN_KEYS, "lineage")
        return self


# ─────────────────────────────────────────────────────────────────────────────
# Open Data Contract Standard (ODCS v3.x) ⇄ LakeLogic mapping
#
# Reference: https://github.com/bitol-io/open-data-contract-standard
#
# The converter below understands the *full* ODCS v3.x document shape while
# staying backward-compatible with:
#   • the legacy simplified LakeLogic-flavoured ODCS (flat ``schema:`` columns),
#   • native LakeLogic contracts (which never carry ``kind: DataContract``).
#
# Detection is unchanged: ``kind == "DataContract"`` AND ``apiVersion`` present.
# ─────────────────────────────────────────────────────────────────────────────

# ODCS logicalType → LakeLogic field.type
_ODCS_LOGICAL_TO_LL: Dict[str, str] = {
    "string": "string",
    "integer": "integer",
    "number": "double",
    "boolean": "boolean",
    "date": "timestamp",
    "object": "string",
    "array": "string",
}

# LakeLogic field.type → ODCS logicalType (reverse, for export)
_LL_TYPE_TO_LOGICAL: Dict[str, str] = {
    "string": "string",
    "varchar": "string",
    "text": "string",
    "char": "string",
    "uuid": "string",
    "integer": "integer",
    "int": "integer",
    "bigint": "integer",
    "long": "integer",
    "smallint": "integer",
    "tinyint": "integer",
    "double": "number",
    "float": "number",
    "decimal": "number",
    "numeric": "number",
    "number": "number",
    "real": "number",
    "boolean": "boolean",
    "bool": "boolean",
    "date": "date",
    "timestamp": "date",
    "timestamp_ntz": "date",
    "datetime": "date",
}

# ODCS quality operator keys, most-specific first
_ODCS_OPERATORS = (
    "mustBeGreaterOrEqualTo",
    "mustBeLessOrEqualTo",
    "mustBeGreaterThan",
    "mustBeLessThan",
    "mustBeBetween",
    "mustNotBe",
    "mustBe",
)


def _odcs_map_logical_type(raw: Any) -> str:
    """Map an ODCS ``logicalType``/``physicalType``/legacy ``type`` to a LakeLogic type.

    Known ODCS logical types are mapped; anything else (SQL-native types used by
    legacy simplified contracts, e.g. ``timestamp``/``bigint``) passes through as-is.
    """
    if not raw:
        return "string"
    key = str(raw).strip().lower()
    return _ODCS_LOGICAL_TO_LL.get(key, str(raw).strip())


def _ll_type_to_logical(raw: Any) -> str:
    """Reverse map a LakeLogic field.type to an ODCS logicalType (export)."""
    if not raw:
        return "string"
    return _LL_TYPE_TO_LOGICAL.get(str(raw).strip().lower(), "string")


def _odcs_slug(text: Any, fallback: str = "quality_rule") -> str:
    """Turn a description/rule name into a safe LakeLogic rule name."""
    if not text:
        return fallback
    out = []
    for ch in str(text).strip().lower():
        if ch.isalnum():
            out.append(ch)
        elif ch in " -/._":
            out.append("_")
    slug = "".join(out).strip("_")
    while "__" in slug:
        slug = slug.replace("__", "_")
    return slug or fallback


def _odcs_sql_literal(value: Any) -> str:
    """Render a Python value as a SQL literal for generated rules."""
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return str(value)
    return "'" + str(value).replace("'", "''") + "'"


def _odcs_extract_operator(q: Dict[str, Any]):
    """Return (operator_key, value) for the first ODCS comparison operator present."""
    for op in _ODCS_OPERATORS:
        if op in q:
            return op, q[op]
    return None, None


def _odcs_apply_operator(rule: Dict[str, Any], op: str, value: Any) -> bool:
    """Populate must_be_* thresholds on a dataset QualityRule dict.

    Returns True if a threshold was applied, False if the operator is not
    representable with LakeLogic's must_be_* thresholds (caller should skip).
    """
    if op == "mustBeBetween" and isinstance(value, (list, tuple)) and len(value) == 2:
        rule["must_be_between"] = [value[0], value[1]]
        return True
    if op == "mustBe":
        rule["must_be_between"] = [value, value]
        return True
    if op in ("mustBeGreaterThan", "mustBeGreaterOrEqualTo"):
        rule["must_be_greater_than"] = value
        return True
    if op in ("mustBeLessThan", "mustBeLessOrEqualTo"):
        rule["must_be_less_than"] = value
        return True
    # mustNotBe has no must_be_* representation — deferred.
    return False


def _odcs_library_aggregate_sql(rule_name: str, column: Optional[str], table_ref: str) -> Optional[str]:
    """Build a scalar-returning SELECT for a common ODCS library aggregate rule."""
    rn = (rule_name or "").lower()
    col = column or "*"
    agg = None
    if rn in ("rowcount", "count", "recordcount"):
        agg = "COUNT(*)"
    elif rn in ("avg", "average", "mean"):
        agg = f"AVG({col})"
    elif rn in ("sum", "total"):
        agg = f"SUM({col})"
    elif rn in ("min", "minimum"):
        agg = f"MIN({col})"
    elif rn in ("max", "maximum"):
        agg = f"MAX({col})"
    elif rn in ("nullproportion", "nullratio"):
        agg = f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*), 0)"
    elif column:
        # Unknown library rule with an explicit column and operator — optimistic.
        agg = f"{rule_name}({col})"
    if agg is None:
        return None
    return f"SELECT {agg} FROM {table_ref}"


def _odcs_process_quality_list(
    quality_list: Any,
    column: Optional[str],
    table_ref: str,
    row_rules: List[Dict[str, Any]],
    dataset_rules: List[Dict[str, Any]],
) -> None:
    """Translate an ODCS ``quality[]`` array (schema- or property-level) into
    LakeLogic row/dataset rule dicts, appending to the provided lists.
    """
    if not isinstance(quality_list, list):
        return
    for q in quality_list:
        if not isinstance(q, dict):
            continue
        qtype = str(q.get("type") or "").strip().lower()
        col = q.get("column") or q.get("property") or column
        desc = q.get("description") or q.get("name") or q.get("rule")
        base_name = _odcs_slug(
            desc or (f"{col}_quality" if col else "quality_rule"),
            fallback=(f"{col}_quality" if col else "quality_rule"),
        )
        severity = q.get("severity") or "error"

        # type: text — documentation only, no execution
        if qtype == "text":
            continue

        # type: sql / custom — a raw SQL query
        if qtype in ("sql", "custom") or (not qtype and (q.get("query") or q.get("sql"))):
            query = q.get("query") or q.get("sql")
            if not query:
                continue
            op, val = _odcs_extract_operator(q)
            if op:
                rule = {"name": base_name, "sql": query, "description": desc, "severity": severity}
                if _odcs_apply_operator(rule, op, val):
                    dataset_rules.append(rule)
                else:
                    logger.warning(
                        "ODCS quality operator '%s' is not representable; skipping rule '%s'.", op, base_name
                    )
            else:
                row_rules.append({"name": base_name, "sql": query, "description": desc, "severity": severity})
            continue

        # type: library (or any rule-bearing entry)
        if qtype == "library" or q.get("rule"):
            rule_name = str(q.get("rule") or "").strip().lower()
            op, val = _odcs_extract_operator(q)
            valid_values = q.get("validValues", q.get("valid_values"))

            if rule_name in ("nullcount", "nullcheck", "notnull", "not_null", "null"):
                if col:
                    row_rules.append(
                        {
                            "name": base_name or f"{col}_not_null",
                            "sql": f"{col} IS NOT NULL",
                            "description": desc,
                            "severity": severity,
                            "category": "completeness",
                        }
                    )
                continue
            if rule_name in ("duplicatecount", "uniquecheck", "unique", "distinctcheck", "duplicate"):
                if col:
                    dataset_rules.append({"unique": {"field": col, "name": base_name, "severity": severity}})
                continue
            if valid_values is not None and col:
                values_sql = ", ".join(_odcs_sql_literal(v) for v in valid_values)
                row_rules.append(
                    {
                        "name": base_name or f"{col}_valid_values",
                        "sql": f"{col} IN ({values_sql})",
                        "description": desc,
                        "severity": severity,
                        "category": "consistency",
                    }
                )
                continue
            if op:
                sql = _odcs_library_aggregate_sql(rule_name, col, table_ref)
                if sql:
                    rule = {"name": base_name, "sql": sql, "description": desc, "severity": severity}
                    if _odcs_apply_operator(rule, op, val):
                        dataset_rules.append(rule)
                        continue
                logger.warning(
                    "ODCS library rule '%s' with operator '%s' could not be mapped; skipping.", rule_name, op
                )
                continue
            # Rule with neither recognised name nor operator — nothing executable.
            logger.warning(
                "ODCS library quality rule '%s' has no mappable operator/values; documented only.", rule_name
            )


def _odcs_select_schema_table(schema: List[Any], data: Dict[str, Any]):
    """Choose the single schema/table object LakeLogic will execute.

    LakeLogic is one-dataset-per-contract. Prefer the table whose ``name``
    matches the contract ``name``/``id``; otherwise take the first. Returns
    ``(selected, skipped_names)``.
    """
    tables = [e for e in schema if isinstance(e, dict)]
    if not tables:
        return None, []
    match_keys = {str(data.get("name") or "").lower(), str(data.get("id") or "").lower()} - {""}
    selected = None
    for t in tables:
        if str(t.get("name") or "").lower() in match_keys:
            selected = t
            break
    if selected is None:
        selected = tables[0]
    skipped = [t.get("name") for t in tables if t is not selected]
    return selected, skipped


def _odcs_map_server_type(server_type: Any) -> str:
    """Best-effort map an ODCS server ``type`` to a LakeLogic source.type."""
    st = str(server_type or "").strip().lower()
    table_like = {
        "databricks",
        "snowflake",
        "bigquery",
        "redshift",
        "postgres",
        "postgresql",
        "mysql",
        "sqlserver",
        "synapse",
        "oracle",
        "trino",
        "presto",
        "athena",
        "glue",
    }
    if st in table_like:
        return "table"
    if st in ("kafka", "kinesis", "pubsub", "eventhub"):
        return "stream"
    # object stores / files / anything else → landing
    return "landing"


def _convert_odcs_to_lakelogic(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert an Open Data Contract Standard (ODCS v3.x) dict into LakeLogic format.

    Handles the full ODCS v3.x document (fundamentals, ``schema[].properties[]``,
    schema- and property-level ``quality[]``, ``slaProperties[]``, ``team[]``/
    ``roles[]``, ``servers[]``, ``description`` object, ``customProperties``) while
    remaining backward-compatible with the legacy simplified flat-``schema`` form.
    """
    # Detection is unchanged: requires kind == DataContract and apiVersion.
    if data.get("kind") != "DataContract" or "apiVersion" not in data:
        return data  # Not ODCS or already LakeLogic

    ll: Dict[str, Any] = {}
    consumed: set = {"kind", "apiVersion"}

    # ── Fundamentals ─────────────────────────────────────────────────────────
    spec_version = data.get("apiVersion")
    # LakeLogic `version` is the CONTRACT version (top-level `version`); the ODCS
    # spec version lives in apiVersion and is preserved in metadata.
    ll["version"] = str(data.get("version") or spec_version or "1.0")
    consumed.add("version")

    info: Dict[str, Any] = {}
    title = data.get("name") or data.get("id") or data.get("dataset")
    if title:
        info["title"] = str(title)
    consumed.update({"name", "id", "dataset"})

    # description object: {purpose, usage, limitations}
    desc = data.get("description")
    if isinstance(desc, dict):
        parts = [str(desc[k]) for k in ("purpose", "usage", "limitations") if desc.get(k)]
        if parts:
            info["description"] = " — ".join(parts)
    elif isinstance(desc, str) and desc:
        info["description"] = desc
    consumed.add("description")

    if data.get("domain"):
        info["domain"] = data["domain"]
    if data.get("status"):
        info["status"] = data["status"]
    consumed.update({"domain", "status"})

    # metadata carries everything that has no first-class LakeLogic home so
    # nothing is silently dropped, plus the ODCS round-trip anchors.
    metadata: Dict[str, Any] = {"odcs_api_version": spec_version}
    if data.get("id"):
        metadata["odcs_id"] = data["id"]
    if data.get("name"):
        metadata["odcs_name"] = data["name"]
    for k in ("tenant", "dataProduct", "tags"):
        if data.get(k) is not None:
            metadata[k] = data[k]
    consumed.update({"tenant", "dataProduct", "tags"})

    # ── Ownership: team[] (v3) / stakeholders[] (v2) / roles[] ───────────────
    owner = None
    team = data.get("team")
    if isinstance(team, list):
        for member in team:
            if isinstance(member, dict) and (member.get("username") or member.get("name") or member.get("email")):
                owner = member.get("username") or member.get("name") or member.get("email")
                break
    if owner is None:
        stake = data.get("stakeholders")
        if isinstance(stake, list):
            for s in stake:
                if isinstance(s, dict) and (s.get("username") or s.get("name")):
                    owner = s.get("username") or s.get("name")
                    break
    if owner is None:
        roles = data.get("roles")
        if isinstance(roles, list) and roles and isinstance(roles[0], dict):
            owner = roles[0].get("role") or roles[0].get("approvers") or roles[0].get("access")
    if owner:
        info["owner"] = str(owner)
    consumed.update({"team", "stakeholders", "roles", "slaDefaultColumn"})

    # ── Schema → model.fields (+ primary_key, partitioning, quality) ─────────
    schema = data.get("schema")
    consumed.add("schema")
    fields: List[Dict[str, Any]] = []
    primary_key: List = []  # (position, name)
    partition_cols: List = []  # (position, name)
    row_rules: List[Dict[str, Any]] = []
    dataset_rules: List[Dict[str, Any]] = []
    table_ref = str(title or "source")

    if isinstance(schema, list) and schema:
        has_tables = any(isinstance(e, dict) and isinstance(e.get("properties"), list) for e in schema)
        if has_tables:
            selected, skipped = _odcs_select_schema_table(schema, data)
            if skipped:
                logger.warning(
                    "ODCS contract defines multiple schema objects; LakeLogic is "
                    "one-dataset-per-contract. Using '%s'; skipping: %s.",
                    (selected or {}).get("name"),
                    ", ".join(str(s) for s in skipped),
                )
            props = (selected or {}).get("properties") or []
            phys = (selected or {}).get("physicalName")
            if phys:
                info.setdefault("table_name", phys)
                table_ref = str(phys)
            elif selected and selected.get("name"):
                table_ref = str(selected["name"])
            # schema-level quality applies to the dataset
            _odcs_process_quality_list((selected or {}).get("quality"), None, table_ref, row_rules, dataset_rules)
        else:
            # Legacy simplified form: the schema list *is* the column list.
            props = schema

        for idx, col in enumerate(props):
            if not isinstance(col, dict) or not col.get("name"):
                continue
            fname = col["name"]
            field: Dict[str, Any] = {"name": fname}
            field["type"] = _odcs_map_logical_type(col.get("logicalType") or col.get("physicalType") or col.get("type"))
            if col.get("required"):
                field["required"] = True
            if col.get("description"):
                field["description"] = col["description"]

            # primaryKey (+ position) → ordered primary_key + required
            if col.get("primaryKey"):
                pos = col.get("primaryKeyPosition", idx + 1)
                primary_key.append((pos, fname))
                field["required"] = True

            # Security classification → pii / sensitive (+ masking default)
            classification = col.get("classification")
            if classification:
                field["classification"] = classification
                cl = str(classification).lower()
                if "pii" in cl or "personal" in cl:
                    field["pii"] = True
                    field.setdefault("masking", "redact")
                elif cl in ("confidential", "restricted", "sensitive", "secret"):
                    field["sensitive"] = True
                    field.setdefault("masking", "redact")
            if col.get("criticalDataElement") and not field.get("pii"):
                field["sensitive"] = True
                field.setdefault("masking", "redact")

            # Direct LakeLogic-style flags (legacy simplified form)
            if col.get("pii"):
                field["pii"] = True
            if col.get("sensitive"):
                field["sensitive"] = True
            if col.get("masking"):
                field["masking"] = col["masking"]

            # uniqueness → dataset unique rule (FieldDefinition has no `unique` flag)
            if col.get("unique"):
                dataset_rules.append({"unique": {"field": fname, "name": f"{_odcs_slug(fname)}_unique"}})

            # partitioning (best-effort)
            if col.get("partitioned"):
                ppos = col.get("partitionKeyPosition", idx + 1)
                partition_cols.append((ppos, fname))

            # property-level quality
            _odcs_process_quality_list(col.get("quality"), fname, table_ref, row_rules, dataset_rules)

            fields.append(field)

    if fields:
        ll["model"] = {"fields": fields}
    if primary_key:
        ll["primary_key"] = [n for _, n in sorted(primary_key, key=lambda x: x[0])]
    if partition_cols:
        ll["materialization"] = {"partition_by": [n for _, n in sorted(partition_cols, key=lambda x: x[0])]}

    quality_block: Dict[str, Any] = {}
    if row_rules:
        quality_block["row_rules"] = row_rules
    if dataset_rules:
        quality_block["dataset_rules"] = dataset_rules
    if quality_block:
        ll["quality"] = quality_block

    # ── slaProperties[] → service_levels ─────────────────────────────────────
    sla = data.get("slaProperties")
    consumed.add("slaProperties")
    if isinstance(sla, list) and sla:
        svc: Dict[str, Any] = {}
        extra_sla: List[Any] = []
        for item in sla:
            if not isinstance(item, dict):
                continue
            prop = str(item.get("property") or "").strip().lower()
            value = item.get("value")
            unit = item.get("unit")
            if prop in ("frequency", "latency", "freshness"):
                svc["freshness"] = f"{value}{unit}" if unit else str(value)
            else:
                extra_sla.append(item)
        if extra_sla:
            metadata["odcs_sla_properties"] = extra_sla
        if svc:
            ll["service_levels"] = svc

    # ── servers[] → source (best-effort; customProperties.lakelogic wins) ────
    servers = data.get("servers")
    consumed.add("servers")
    if isinstance(servers, list) and servers:
        chosen = None
        for s in servers:
            if isinstance(s, dict) and str(s.get("environment", "")).lower() in ("prod", "production"):
                chosen = s
                break
        if chosen is None:
            chosen = next((s for s in servers if isinstance(s, dict)), None)
        if isinstance(chosen, dict):
            source: Dict[str, Any] = {"type": _odcs_map_server_type(chosen.get("type"))}
            path = (
                chosen.get("location")
                or chosen.get("path")
                or chosen.get("dataset")
                or chosen.get("catalog")
                or chosen.get("project")
            )
            if path:
                source["path"] = str(path)
            if chosen.get("format"):
                source["format"] = chosen["format"]
            ll["source"] = source
            metadata["odcs_server_type"] = chosen.get("type")

    if metadata:
        ll["metadata"] = metadata

    # ── customProperties.lakelogic overrides — applied LAST (they win) ───────
    custom = data.get("customProperties")
    consumed.add("customProperties")
    ll_custom = custom.get("lakelogic") if isinstance(custom, dict) else None
    if isinstance(custom, dict):
        others = {k: v for k, v in custom.items() if k != "lakelogic"}
        if others:
            metadata.setdefault("odcs_custom_properties", others)
    if isinstance(ll_custom, dict):
        for k, v in ll_custom.items():
            if k == "quality" and isinstance(v, dict) and isinstance(ll.get("quality"), dict):
                merged = dict(ll["quality"])
                for rk in ("row_rules", "dataset_rules"):
                    if isinstance(v.get(rk), list):
                        merged[rk] = list(merged.get(rk, [])) + list(v[rk])
                for qk, qv in v.items():
                    if qk not in ("row_rules", "dataset_rules"):
                        merged[qk] = qv
                ll["quality"] = merged
            elif k in ("materialization", "metadata") and isinstance(v, dict) and isinstance(ll.get(k), dict):
                merged = dict(ll[k])
                merged.update(v)
                ll[k] = merged
            else:
                ll[k] = v

    if info:
        ll["info"] = info

    # ── Final pass: copy any still-unmapped top-level ODCS keys so nothing is
    # silently lost (without clobbering anything already mapped). ─────────────
    for k, v in data.items():
        if k in consumed or k in ll:
            continue
        ll[k] = v

    return ll


def to_odcs(contract: "DataContract") -> Dict[str, Any]:
    """Export a LakeLogic ``DataContract`` to a valid ODCS v3.x dict.

    The emitted document round-trips: re-importing it via ``DataContract(**doc)``
    yields an equivalent, executable LakeLogic contract because the execution
    context (source, materialization, tier, target_layer) is carried in
    ``customProperties.lakelogic``.
    """
    info = getattr(contract, "info", None)
    meta = dict(getattr(contract, "metadata", {}) or {})

    name = (info.title if info and info.title else None) or meta.get("odcs_name") or meta.get("odcs_id")
    odcs_id = meta.get("odcs_id") or name or "lakelogic-contract"
    name = name or odcs_id

    odcs: Dict[str, Any] = {
        "apiVersion": meta.get("odcs_api_version") or "v3.0.2",
        "kind": "DataContract",
        "id": odcs_id,
        "name": name,
        "version": str(getattr(contract, "version", "1.0")),
        "status": (info.status if info and info.status else None) or "active",
    }
    if info and info.domain:
        odcs["domain"] = info.domain
    if meta.get("tenant"):
        odcs["tenant"] = meta["tenant"]
    if meta.get("dataProduct"):
        odcs["dataProduct"] = meta["dataProduct"]
    if meta.get("tags"):
        odcs["tags"] = meta["tags"]
    if info and info.description:
        odcs["description"] = {"purpose": info.description}
    if info and info.owner:
        odcs["team"] = [{"username": info.owner, "role": "owner"}]

    # ── schema[].properties[] ────────────────────────────────────────────────
    pk = list(getattr(contract, "primary_key", []) or [])
    partition_by = []
    mat = getattr(contract, "materialization", None)
    if mat is not None:
        partition_by = list(getattr(mat, "partition_by", []) or [])

    properties: List[Dict[str, Any]] = []
    model = getattr(contract, "model", None)
    fields = list(getattr(model, "fields", []) or []) if model else []
    for f in fields:
        prop: Dict[str, Any] = {
            "name": f.name,
            "logicalType": _ll_type_to_logical(f.type),
            "physicalType": f.type,
        }
        if f.required:
            prop["required"] = True
        if f.description:
            prop["description"] = f.description
        if f.name in pk:
            prop["primaryKey"] = True
            prop["primaryKeyPosition"] = pk.index(f.name) + 1
        if f.name in partition_by:
            prop["partitioned"] = True
            prop["partitionKeyPosition"] = partition_by.index(f.name) + 1
        if getattr(f, "pii", False):
            prop["classification"] = f.classification or "PII"
            prop["criticalDataElement"] = True
        elif getattr(f, "sensitive", False):
            prop["classification"] = f.classification or "confidential"
            prop["criticalDataElement"] = True
        elif f.classification:
            prop["classification"] = f.classification
        properties.append(prop)

    schema_obj: Dict[str, Any] = {
        "name": name,
        "physicalType": "table",
        "properties": properties,
    }
    if info and getattr(info, "table_name", None):
        schema_obj["physicalName"] = info.table_name

    # ── quality[] (reverse of the import mapping) ────────────────────────────
    quality_items: List[Dict[str, Any]] = []
    quality = getattr(contract, "quality", None)
    if quality is not None:
        for r in list(getattr(quality, "row_rules", []) or []):
            item = _rule_to_odcs_quality(r, is_dataset=False)
            if item:
                quality_items.append(item)
        for r in list(getattr(quality, "dataset_rules", []) or []):
            item = _rule_to_odcs_quality(r, is_dataset=True)
            if item:
                quality_items.append(item)
    if quality_items:
        schema_obj["quality"] = quality_items

    odcs["schema"] = [schema_obj]

    # ── slaProperties[] (from service_levels) ────────────────────────────────
    sla_props: List[Dict[str, Any]] = []
    svc = getattr(contract, "service_levels", None)
    if svc is not None and getattr(svc, "freshness", None):
        fresh = svc.freshness
        if not isinstance(fresh, str):
            fresh = getattr(fresh, "threshold", None)
        if fresh is not None:
            sla_props.append({"property": "frequency", "value": fresh})
    for extra in meta.get("odcs_sla_properties", []) or []:
        if isinstance(extra, dict):
            sla_props.append(extra)
    if sla_props:
        odcs["slaProperties"] = sla_props

    # ── customProperties.lakelogic — the execution context (round-trip) ──────
    lakelogic_ctx: Dict[str, Any] = {}
    if getattr(contract, "tier", None):
        lakelogic_ctx["tier"] = contract.tier
    source = getattr(contract, "source", None)
    if source is not None:
        lakelogic_ctx["source"] = source.model_dump(exclude_none=True, exclude_defaults=True)
    if mat is not None:
        # NB: primary_key is NOT re-emitted here — it round-trips via the
        # schema properties' primaryKey flag (top-level primary_key is the
        # authoritative field; materialization.primary_key would warn).
        mat_dump = mat.model_dump(exclude_none=True, exclude_defaults=True)
        if mat_dump:
            lakelogic_ctx["materialization"] = mat_dump
    if info and getattr(info, "target_layer", None):
        lakelogic_ctx["target_layer"] = info.target_layer
    if lakelogic_ctx:
        odcs["customProperties"] = {"lakelogic": lakelogic_ctx}

    return odcs


def _rule_to_odcs_quality(rule: Any, is_dataset: bool) -> Optional[Dict[str, Any]]:
    """Reverse-map a LakeLogic quality rule object to an ODCS quality entry."""
    # Structured dataset unique rule
    if isinstance(rule, DatasetRuleUnique):
        payload = rule.unique
        field = payload if isinstance(payload, str) else (payload or {}).get("field")
        item: Dict[str, Any] = {"type": "library", "rule": "uniqueCheck", "mustBe": 0}
        if field:
            item["column"] = field
        return item

    # Plain QualityRule (row predicate or dataset aggregate)
    if isinstance(rule, QualityRule):
        item = {"type": "sql", "query": rule.sql, "description": rule.description or rule.name}
        if is_dataset:
            if rule.must_be_between is not None:
                item["mustBeBetween"] = rule.must_be_between
            elif rule.must_be_greater_than is not None:
                item["mustBeGreaterThan"] = rule.must_be_greater_than
            elif rule.must_be_less_than is not None:
                item["mustBeLessThan"] = rule.must_be_less_than
        return item

    # Other structured business rules — emit a descriptive sql entry best-effort.
    try:
        dumped = rule.model_dump(exclude_none=True)
    except Exception:
        return None
    return {"type": "text", "description": str(dumped)}


class DataContract(BaseModel):
    """
    Finalized SQL-First Data Contract Model.
    Supports ODCS-style metadata and consolidated 'sql' keywords.
    """

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    @model_validator(mode="before")
    @classmethod
    def _odcs_interceptor(cls, data: Any) -> Any:
        """Intercept and convert ODCS YAML into LakeLogic dict before Pydantic parses it."""
        if isinstance(data, dict):
            return _convert_odcs_to_lakelogic(data)
        return data

    @model_validator(mode="before")
    @classmethod
    def _soft_deletes_interceptor(cls, data: Any) -> Any:
        """Map the top-level ``soft_deletes:`` YAML block into materialization fields.

        The contract YAML supports a user-friendly shorthand::

            soft_deletes:
              enabled: true
              flag_field: "_is_deleted"
              reason_field: "_delete_reason"
              timestamp_field: "_deleted_at"

        Internally the soft-delete behaviour is driven by
        ``materialization.soft_delete_column``, ``soft_delete_time_column``,
        and ``soft_delete_reason_column``.  This validator bridges the two
        representations so users don't have to write the verbose form.
        """
        if not isinstance(data, dict):
            return data

        sd = data.get("soft_deletes")
        if not isinstance(sd, dict) or not sd.get("enabled", False):
            return data

        # Ensure materialization dict exists
        mat = data.setdefault("materialization", {})
        if not isinstance(mat, dict):
            return data

        # Map soft_deletes fields → materialization fields (user values take priority)
        if sd.get("flag_field") and not mat.get("soft_delete_column"):
            mat["soft_delete_column"] = sd["flag_field"]
        if sd.get("timestamp_field") and not mat.get("soft_delete_time_column"):
            mat["soft_delete_time_column"] = sd["timestamp_field"]
        if sd.get("reason_field") and not mat.get("soft_delete_reason_column"):
            mat["soft_delete_reason_column"] = sd["reason_field"]

        # If enabled but no flag_field specified, set a sensible default
        if not mat.get("soft_delete_column"):
            mat["soft_delete_column"] = "_lakelogic_is_deleted"
        if not mat.get("soft_delete_time_column"):
            mat["soft_delete_time_column"] = "_lakelogic_deleted_at"
        if not mat.get("soft_delete_reason_column"):
            mat["soft_delete_reason_column"] = "_lakelogic_delete_reason"

        return data

    @model_validator(mode="before")
    @classmethod
    def _schema_policy_migrator(cls, data: Any) -> Any:
        """Migrate legacy root-level schema_policy or server.schema_evolution -> server.schema_policy."""
        if not isinstance(data, dict):
            return data

        server_block = data.get("server")
        if not isinstance(server_block, dict):
            # If there's no server block at all, but there is root policy, we instantiate it
            if (
                data.get("schema_policy") is not None
                or data.get("schema_evolution") is not None
                or data.get("allow_schema_drift") is not None
            ):
                server_block = {}
                data["server"] = server_block
            else:
                return data

        policy = server_block.setdefault("schema_policy", {})

        # 1. Migrate root-level schema_policy
        root_policy = data.pop("schema_policy", None)
        if isinstance(root_policy, dict):
            if "evolution" in root_policy and "evolution" not in policy:
                policy["evolution"] = root_policy["evolution"]
            if "unknown_fields" in root_policy and "unknown_fields" not in policy:
                policy["unknown_fields"] = root_policy["unknown_fields"]

        # 2. Migrate server.schema_evolution
        legacy_evo = server_block.pop("schema_evolution", None)
        if legacy_evo:
            if "evolution" not in policy:
                policy["evolution"] = legacy_evo
            if legacy_evo == "strict" and "unknown_fields" not in policy:
                policy["unknown_fields"] = "quarantine"

        # 3. Migrate server.allow_schema_drift
        legacy_drift = server_block.pop("allow_schema_drift", None)
        if legacy_drift is not None and "unknown_fields" not in policy:
            policy["unknown_fields"] = "allow" if legacy_drift else "quarantine"

        return data

    version: str
    info: Optional[Info] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)  # For generic tagging (status, classification)
    server: Optional[Server] = None
    source: Optional[SourceConfig] = None
    environments: Dict[str, Environment] = Field(default_factory=dict)
    links: List[Link] = Field(default_factory=list)

    dataset: Optional[str] = None
    primary_key: List[str] = Field(default_factory=list)
    natural_key: List[str] = Field(default_factory=list)  # Business key for SCD2 (repeated across versions)

    # LINEAGE & OBSERVABILITY
    lineage: Optional[LineageConfig] = Field(default_factory=LineageConfig)

    # MATERIALIZATION LAYER (Gold/Silver)
    materialization: Optional[Materialization] = Field(default_factory=Materialization)
    logic: Optional[str] = None  # Full SQL for materialization

    # EXTERNAL LOGIC
    external_logic: Optional[ExternalLogic] = None
    observatory: Optional[Dict[str, Any]] = None

    # LLM EXTRACTION (unstructured → structured)
    extraction: Optional[ExtractionConfig] = None

    # ORCHESTRATION & DEPENDENCIES
    upstream: List[str] = Field(default_factory=list)
    # Structured upstream refs to products that HAVE their own contract, and nested
    # non-contract origins (source system -> landing -> this). Mirrors `downstream`
    # (whose DownstreamConsumer nests `consumers`); both come from olc.models._nested.
    upstream_contracts: List[UpstreamContractRef] = Field(default_factory=list)
    upstream_sources: List[UpstreamSource] = Field(default_factory=list)
    downstream: List[DownstreamConsumer] = Field(default_factory=list)
    schedule: Optional[str] = None

    schema_policy: Optional[SchemaPolicy] = None
    model: Optional[Model] = None
    quality: Optional[Quality] = Field(default_factory=Quality)
    transformations: List[Transformation] = Field(default_factory=list)
    service_levels: Optional[ServiceLevel] = None
    quarantine: Optional[Quarantine] = Field(default_factory=Quarantine)
    compliance: Optional[Dict[str, Any]] = Field(default_factory=dict)

    # TIER / LAYER ── mandatory for single-contract mode
    tier: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("tier", "layer", "target_layer"),
    )
    contract_file_name: Optional[str] = None

    @field_validator("tier", mode="before")
    @classmethod
    def _normalize_tier(cls, v: Any) -> Optional[str]:
        """Normalize tier values to canonical medallion names."""
        if v is None:
            return None
        raw = str(v).strip().lower()
        canonical = TIER_CANONICAL_MAP.get(raw)
        if canonical:
            return canonical
        # Allow passthrough for custom tiers (e.g. "platinum", "archive")
        return raw

    # ── Cross-field validation ─────────────────────────────────────────────────

    # Known top-level contract keys — union of Pydantic fields + recognised
    # shorthand blocks (soft_deletes) + ODCS aliases that the interceptors
    # handle.  Keys in ``_PRIVATE_EXTRA_KEYS`` are silently allowed because
    # they are injected at runtime by the processor / pipeline runner.
    _KNOWN_KEYS: set = {
        # Pydantic-declared fields
        "version",
        "info",
        "metadata",
        "server",
        "source",
        "environments",
        "links",
        "dataset",
        "primary_key",
        "natural_key",
        "lineage",
        "materialization",
        "logic",
        "external_logic",
        "extraction",
        "upstream",
        "downstream",
        "schedule",
        "schema_policy",
        "model",
        "quality",
        "transformations",
        "service_levels",
        "quarantine",
        "tier",
        "layer",
        "target_layer",
        # Recognised shorthand blocks (handled by interceptors)
        "soft_deletes",
        # ODCS / alternative schema keys accepted by _odcs_interceptor
        "schema",
        "tables",
        "columns",
        "properties",
        "fields",
        "kind",
        "apiVersion",
        "type",
        "status",
        "description",
        "datasetDomain",
        "quantumName",
        "datasetName",
        "driver",
        "driverVersion",
        "servers",
        "price",
        "stakeholders",
        "roles",
        "slaDefaultColumn",
        "slaProperties",
        "tags",
        "customProperties",
        "observatory",
        "compliance",
    }
    _PRIVATE_EXTRA_KEYS: set = {
        "_base_path",
        "_contract_path",
        "_resolved_by_pipeline",
    }

    @model_validator(mode="after")
    def _warn_unknown_keys(self) -> "DataContract":
        """Warn about unrecognised top-level contract keys."""
        _warn_unknown_extra_keys(
            self,
            self._KNOWN_KEYS | self._PRIVATE_EXTRA_KEYS,
            "contract",
        )
        return self

    @model_validator(mode="after")
    def _validate_incremental_requires_run_log(self) -> "DataContract":
        """
        Raise a clear error when ``source.load_mode: incremental`` is set but
        no run-log backend is configured.

        Without a run log, ``get_last_run_watermark()`` always returns ``None``
        and every ``run_source()`` call re-reads every file in the landing zone,
        making the incremental setting a silent no-op.

        Required: at least one of these ``metadata`` keys must be present:

        ============================================================================
        Key                  Storage                       Notes
        ============================================================================
        ``run_log_dir``      Per-run JSON files (dir)      Lightest; local/ADLS
        ``run_log_path``     Single JSON file              Overwritten each run
        ``run_log_table``    DuckDB / Spark / SQLite table Queryable history
        ============================================================================
        """
        source = getattr(self, "source", None)
        if source is None:
            return self

        load_mode = getattr(source, "load_mode", None)
        if load_mode != "incremental":
            return self

        # delta_version can store state in table properties, so it's exempt from mandatory run_log
        if getattr(source, "watermark_strategy", None) == "delta_version":
            return self

        metadata = self.metadata or {}
        has_run_log = any(metadata.get(key) for key in ("run_log_dir", "run_log_path", "run_log_table"))
        if not has_run_log:
            import os as _os

            if _os.environ.get("LAKELOGIC_SKIP_INCREMENTAL_CHECK", "").strip() not in (
                "",
                "0",
            ):
                return self
            raise ValueError(
                "source.load_mode is 'incremental' but no run-log backend is "
                "configured in metadata. Without a run log the watermark is never "
                "persisted, so every run re-processes ALL files in the landing zone.\n\n"
                "Add at least ONE of the following to your contract's metadata block:\n\n"
                "  metadata:\n"
                "    run_log_dir: logs/runs/           # lightweight — one JSON file per run\n"
                "    # OR\n"
                "    run_log_path: logs/run_log.json   # single overwritten JSON file\n"
                "    # OR\n"
                "    run_log_table: bronze.run_logs    # queryable DuckDB/Spark/SQLite table\n\n"
                "To suppress this error while testing pass "
                "LAKELOGIC_SKIP_INCREMENTAL_CHECK=1 as an environment variable."
            )
        return self

    @model_validator(mode="after")
    def _validate_load_mode_properties(self) -> "DataContract":
        """Validate that required properties are present for each load_mode.

        load_mode: incremental
           - watermark_field recommended (defaults to _lakelogic_processed_at)
        load_mode: cdc
           - cdc_op_field required
        """

        source = getattr(self, "source", None)
        if source is None:
            return self

        load_mode = getattr(source, "load_mode", "full")

        if load_mode == "incremental":
            wm_field = getattr(source, "watermark_field", None)
            wm_strategy = getattr(source, "watermark_strategy", None)
            if not wm_field and not wm_strategy:
                logger.warning(
                    "source.load_mode is 'incremental' but no watermark_field is set. "
                    "Defaulting to '_lakelogic_processed_at'. To silence this warning, "
                    "add 'watermark_field: _lakelogic_processed_at' to the source block."
                )
            # pipeline_log_table defaults to "pipeline_runs" at runtime
            # (see incremental.py), so no warning needed here.
            if wm_strategy == "lookback" and not getattr(source, "lookback", None):
                logger.warning(
                    "watermark_strategy is 'lookback' but lookback duration is not set. Defaulting to '7 days'."
                )

        elif load_mode == "cdc":
            has_op_field = bool(getattr(source, "cdc_op_field", None))
            has_ts_field = bool(getattr(source, "cdc_timestamp_field", None))
            if not has_op_field and not has_ts_field:
                raise ValueError(
                    "source.load_mode is 'cdc' but neither cdc_op_field nor "
                    "cdc_timestamp_field is set. At least one is required.\n\n"
                    "  For full CDC (with operation flags):\n"
                    "    source:\n"
                    "      load_mode: cdc\n"
                    "      cdc_op_field: _operation\n"
                    '      cdc_delete_values: ["D", "DELETE"]\n'
                    "      cdc_timestamp_field: _lakelogic_processed_at\n\n"
                    "  For timestamp-only incremental (table-to-table):\n"
                    "    source:\n"
                    "      load_mode: cdc\n"
                    "      cdc_timestamp_field: _lakelogic_processed_at"
                )

        return self

    # ── Convenience constructors ───────────────────────────────────────────────

    @classmethod
    def from_yaml(cls, path: Union[str, "Path"]) -> "DataContract":
        """
        Load a ``DataContract`` directly from a YAML file.

        This mirrors the loading logic in ``DataProcessor._load_contract`` —
        ``on``/``off``/``yes``/``no`` are NOT treated as booleans, while
        ``true``/``false`` are.  The contract's ``_base_path`` and
        ``_contract_path`` attributes are set to the parent directory and
        file path respectively, so relative paths in the contract (quarantine
        target, watermark, etc.) resolve correctly.

        Parameters
        ----------
        path : str or Path
            Absolute or relative path to the contract YAML file.

        Returns
        -------
        DataContract

        Examples
        --------
        ::

            contract = DataContract.from_yaml("contracts/bronze_zoopla.yaml")
            contract.reset(dry_run=True)   # preview managed paths
            contract.reset()               # wipe quarantine + output + watermark
        """
        import re as _re
        import yaml as _yaml
        from pathlib import Path as _P

        _path = _P(path)
        if not _path.exists():
            raise FileNotFoundError(f"Contract file not found: {_path}")

        class _Loader(_yaml.SafeLoader):
            pass

        # Strip 'on/off/yes/no' → not booleans; keep true/false.
        for _key, _mappings in list(_Loader.yaml_implicit_resolvers.items()):
            _Loader.yaml_implicit_resolvers[_key] = [
                (tag, regex) for tag, regex in _mappings if tag != "tag:yaml.org,2002:bool"
            ]
        _Loader.add_implicit_resolver(
            "tag:yaml.org,2002:bool",
            _re.compile(r"^(?:true|false)$", _re.IGNORECASE),
            list("tTfF"),
        )

        with open(_path, "r") as _f:
            data = _yaml.load(_f, Loader=_Loader)

        instance = cls(**data)
        try:
            instance._base_path = _path.parent
            instance._contract_path = _path
        except Exception:
            pass
        return instance

    def to_odcs(self) -> Dict[str, Any]:
        """Export this contract to a valid Open Data Contract Standard (v3.x) dict.

        The document round-trips: ``DataContract(**contract.to_odcs())`` yields an
        equivalent, executable contract (the execution context is carried in
        ``customProperties.lakelogic``).
        """
        return to_odcs(self)

    # ── Environment resolution ─────────────────────────────────────────────────

    @property
    def active_env(self) -> Optional[str]:
        """Return the active environment name from the LAKELOGIC_ENV env-var (if set)."""
        import os

        return os.environ.get("LAKELOGIC_ENV") or None

    # ── Reset / reload ─────────────────────────────────────────────────────────

    def reset(
        self,
        *,
        targets: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Delete all data managed by this contract without touching the contract YAML.

        Removes:
        - ``materialization.target_path`` — the Good-records output (Delta/Parquet/CSV)
        - ``quarantine.target``           — the quarantined bad-records store
        - ``.lakelogic/watermark_*``      — incremental watermark file for this contract
        - ``metadata.run_log_dir``        — JSON run-log files that hold the source mtime
          watermark used by ``load_mode: incremental`` (also ``run_log_path`` /
          ``run_log_database``).

        Parameters
        ----------
        targets : list of str, optional
            Subset of targets to reset. Any combination of:
            ``"materialization"``, ``"quarantine"``, ``"watermark"``,
            ``"run_log"``.
            Defaults to all four.
        dry_run : bool
            When ``True`` nothing is deleted — returns a dict describing what
            *would* be removed so you can inspect before committing.

        Returns
        -------
        dict
            ``{target_name: {"path": ..., "deleted": bool, "dry_run": bool}}``

        Examples
        --------
        ::

            contract = DataContract.from_yaml("contracts/bronze_zoopla.yaml")

            # Preview everything that would be removed
            contract.reset(dry_run=True)

            # Delete everything (incl. run log so incremental sees all files)
            contract.reset()

            # Delete only the quarantine store
            contract.reset(targets=["quarantine"])

            # Full reload: clear outputs + run-log watermark then re-run
            contract.reset()
            DataProcessor(contract).run_source()
        """
        import shutil

        _all = {"materialization", "quarantine", "watermark", "run_log"}
        _targets = set(targets) if targets else _all
        _base = getattr(self, "_base_path", None)

        def _resolve(raw: str) -> "Path":
            from pathlib import Path as _P

            p = _P(raw)
            if not p.is_absolute() and _base:
                p = _P(_base) / p
            return p

        def _delete(p: "Path", raw: Optional[str] = None) -> bool:
            """Delete file or directory tree. Returns True if something was removed."""
            if raw and "://" in raw and not raw.startswith("file://"):
                try:
                    import fsspec

                    fs, fs_path = fsspec.core.url_to_fs(raw)
                    if fs.exists(fs_path):
                        fs.rm(fs_path, recursive=True)
                        return True
                    return False
                except ImportError:
                    pass  # Fall back to local pathlib if fsspec missing
                except Exception as e:
                    import logging

                    logging.getLogger(__name__).warning(f"Could not delete cloud URI {raw}: {e}")
                    return False

            if not p.exists():
                return False
            if p.is_dir():
                shutil.rmtree(p)
            else:
                p.unlink()
            return True

        report: Dict[str, Any] = {}

        # ── 1. Materialization target + external location ─────────────────────
        if "materialization" in _targets:
            mat_path = self.materialization.target_path if self.materialization else None
            mat_location = getattr(self.materialization, "location", None) if self.materialization else None

            # Clean up target_path
            if mat_path:
                p = _resolve(mat_path)
                if dry_run:
                    report["materialization"] = {
                        "path": str(p),
                        "exists": p.exists(),
                        "dry_run": True,
                    }
                else:
                    deleted = _delete(p, mat_path)
                    report["materialization"] = {
                        "path": str(mat_path),
                        "deleted": deleted,
                        "dry_run": False,
                    }
            else:
                report["materialization"] = {
                    "path": None,
                    "note": "no target_path configured",
                }

            # Clean up external location (e.g. abfss:// Delta table storage)
            if mat_location:
                loc_str = str(mat_location)
                _is_cloud = any(
                    loc_str.startswith(pfx)
                    for pfx in (
                        "abfss://",
                        "abfs://",
                        "s3://",
                        "s3a://",
                        "gs://",
                        "gcs://",
                    )
                )
                if _is_cloud:
                    if dry_run:
                        report["materialization_location"] = {
                            "path": loc_str,
                            "dry_run": True,
                            "note": "cloud location would be deleted",
                        }
                    else:
                        _deleted = False
                        # Strategy 1: Databricks dbutils (natively handles abfss://, s3://, etc.)
                        try:
                            from pyspark.sql import SparkSession

                            _spark = SparkSession.getActiveSession()
                            if _spark:
                                _dbutils = None
                                try:
                                    # Databricks runtime injects dbutils
                                    _dbutils = _spark._jvm.com.databricks.service.DBUtils(_spark._jsc.sc())
                                except Exception:
                                    try:
                                        import IPython

                                        _dbutils = IPython.get_ipython().user_ns.get("dbutils")
                                    except Exception:
                                        pass
                                if _dbutils:
                                    _dbutils.fs.rm(loc_str, True)
                                    _deleted = True
                                    logger.info(f"Reset: deleted cloud location {loc_str} via dbutils")
                        except Exception as _db_exc:
                            logger.debug(f"Reset: dbutils.fs.rm failed: {_db_exc}")

                        # Strategy 2: fsspec (for non-Databricks environments)
                        if not _deleted:
                            try:
                                import fsspec
                                import os as _os2

                                _opts: dict = {}
                                if loc_str.startswith(("abfss://", "abfs://")):
                                    for _ek, _ok in [
                                        ("AZURE_STORAGE_ACCOUNT_NAME", "account_name"),
                                        ("AZURE_STORAGE_ACCOUNT", "account_name"),
                                        ("AZURE_STORAGE_ACCOUNT_KEY", "account_key"),
                                        ("AZURE_TENANT_ID", "tenant_id"),
                                        ("AZURE_CLIENT_ID", "client_id"),
                                        ("AZURE_CLIENT_SECRET", "client_secret"),
                                    ]:
                                        _v = _os2.getenv(_ek)
                                        if _v and _ok not in _opts:
                                            _opts[_ok] = _v
                                elif loc_str.startswith(("s3://", "s3a://")):
                                    for _ek, _ok in [
                                        ("AWS_ACCESS_KEY_ID", "key"),
                                        ("AWS_SECRET_ACCESS_KEY", "secret"),
                                        ("AWS_SESSION_TOKEN", "token"),
                                    ]:
                                        _v = _os2.getenv(_ek)
                                        if _v and _ok not in _opts:
                                            _opts[_ok] = _v
                                elif loc_str.startswith(("gs://", "gcs://")):
                                    for _ek, _ok in [
                                        ("GOOGLE_APPLICATION_CREDENTIALS", "token"),
                                    ]:
                                        _v = _os2.getenv(_ek)
                                        if _v and _ok not in _opts:
                                            _opts[_ok] = _v

                                fs, path_part = fsspec.core.url_to_fs(loc_str, **_opts)
                                logger.info(f"Reset: deleting cloud location {loc_str} (resolved path: {path_part})")
                                if fs.exists(path_part):
                                    fs.rm(path_part, recursive=True)
                                    _deleted = True
                                    logger.info(f"Reset: deleted {path_part}")
                                else:
                                    _deleted = True
                                    logger.info(f"Reset: cloud path {path_part} does not exist")
                            except Exception as _fs_exc:
                                logger.debug(f"Reset: fsspec delete failed: {_fs_exc}")

                        if _deleted:
                            report["materialization_location"] = {
                                "path": loc_str,
                                "deleted": True,
                                "dry_run": False,
                            }
                        else:
                            logger.warning(
                                f"Reset: could not delete cloud location {loc_str}. "
                                "This is non-fatal — DROP TABLE handles storage cleanup."
                            )
                            report["materialization_location"] = {
                                "path": loc_str,
                                "deleted": False,
                                "note": "non-fatal — table DROP handles storage cleanup",
                            }
                else:
                    # Local location path
                    loc_p = _resolve(loc_str)
                    if dry_run:
                        report["materialization_location"] = {
                            "path": str(loc_p),
                            "exists": loc_p.exists(),
                            "dry_run": True,
                        }
                    else:
                        deleted = _delete(loc_p)
                        report["materialization_location"] = {
                            "path": str(loc_p),
                            "deleted": deleted,
                            "dry_run": False,
                        }

        # ── 2. Quarantine target ───────────────────────────────────────────────
        if "quarantine" in _targets:
            q_target = self.quarantine.target if self.quarantine else None
            if q_target and not q_target.startswith("table:"):
                p = _resolve(q_target)
                if dry_run:
                    report["quarantine"] = {
                        "path": str(p),
                        "exists": p.exists(),
                        "dry_run": True,
                    }
                else:
                    deleted = _delete(p, q_target)
                    report["quarantine"] = {
                        "path": str(q_target),
                        "deleted": deleted,
                        "dry_run": False,
                    }
            elif q_target and q_target.startswith("table:"):
                report["quarantine"] = {
                    "path": q_target,
                    "note": "table targets are not auto-deleted by reset(); drop the table manually",
                }
            else:
                report["quarantine"] = {
                    "path": None,
                    "note": "no quarantine target configured",
                }

        # ── 3. Incremental watermark ───────────────────────────────────────────
        if "watermark" in _targets:
            from pathlib import Path as _P

            # Watermark lives at <base>/.lakelogic/watermark_<title_slug>.json
            title_slug = ""
            if self.info and self.info.title:
                import re as _re

                title_slug = _re.sub(r"[^\w]+", "_", self.info.title.lower()).strip("_")
            base_dir = _P(_base) if _base else _P.cwd()
            wm_dir = base_dir / ".lakelogic"
            wm_candidates = (
                list(wm_dir.glob(f"watermark_{title_slug}*.json")) + list(wm_dir.glob("watermark*.json"))
                if wm_dir.exists()
                else []
            )
            # Deduplicate
            wm_candidates = list(dict.fromkeys(wm_candidates))
            if wm_candidates:
                if dry_run:
                    report["watermark"] = {
                        "paths": [str(p) for p in wm_candidates],
                        "dry_run": True,
                    }
                else:
                    deleted_paths = []
                    for wp in wm_candidates:
                        if wp.exists():
                            wp.unlink()
                            deleted_paths.append(str(wp))
                    report["watermark"] = {"deleted": deleted_paths, "dry_run": False}
            else:
                report["watermark"] = {"paths": [], "note": "no watermark file found"}

        # ── 4. Run log (source-mtime watermark for incremental loads) ──────────
        if "run_log" in _targets:
            from pathlib import Path as _P

            metadata = self.metadata or {}
            _base_p = _P(_base) if _base else _P.cwd()
            run_log_targets = []

            # run_log_dir  → directory of per-run JSON files
            log_dir_val = metadata.get("run_log_dir")
            if log_dir_val:
                log_dir = _P(log_dir_val) if _P(log_dir_val).is_absolute() else _base_p / log_dir_val
                run_log_targets.append(("dir", log_dir))

            # run_log_path → single fixed JSON file
            log_path_val = metadata.get("run_log_path")
            if log_path_val:
                log_path = _P(log_path_val) if _P(log_path_val).is_absolute() else _base_p / log_path_val
                run_log_targets.append(("file", log_path))

            # run_log_database → DuckDB/SQLite file
            log_db_val = metadata.get("run_log_database")
            if log_db_val:
                log_db = _P(log_db_val) if _P(log_db_val).is_absolute() else _base_p / log_db_val
                run_log_targets.append(("file", log_db))
            elif not log_dir_val and not log_path_val:
                # Default DuckDB path used when run_log_table is configured
                # but run_log_database is not explicit
                default_db = _base_p / "logs" / "lakelogic_run_logs.duckdb"
                if default_db.exists():
                    run_log_targets.append(("file", default_db))

            if not run_log_targets:
                report["run_log"] = {
                    "note": "run log not configured in metadata (run_log_dir / run_log_path / run_log_table)"
                }
            else:
                run_log_report = []
                for kind, p in run_log_targets:
                    if dry_run:
                        run_log_report.append({"path": str(p), "exists": p.exists(), "dry_run": True})
                    else:
                        deleted = _delete(p)
                        run_log_report.append({"path": str(p), "deleted": deleted, "dry_run": False})
                report["run_log"] = run_log_report

        return report

    def effective_server(self, env: Optional[str] = None) -> Optional["Server"]:
        """
        Return a ``Server`` instance with environment-specific overrides applied.

        Resolution order
        ----------------
        1. ``env`` argument (explicit)
        2. ``LAKELOGIC_ENV`` environment variable
        3. No override — returns ``self.server`` unchanged (may be ``None``)

        Only ``path`` and ``format`` are overridable per environment.  All other
        ``Server`` attributes (mode, schema_evolution, …) come from the base
        ``server`` block.

        Examples
        --------
        ::

            # Contract YAML:
            #   server:
            #     type: adls
            #     path: abfss://container@account.dfs.core.windows.net/prod/customers/
            #     format: parquet
            #   environments:
            #     dev:
            #       path: abfss://container@account.dfs.core.windows.net/dev/customers/
            #     test:
            #       path: abfss://container@account.dfs.core.windows.net/test/customers/

            s = contract.effective_server(env="dev")
            # s.path → "abfss://container@account.dfs.core.windows.net/dev/customers/"

            # Or set LAKELOGIC_ENV=test before running and call with no arg:
            import os; os.environ["LAKELOGIC_ENV"] = "test"
            s = contract.effective_server()
            # s.path → ".../test/customers/"
        """
        active = env or self.active_env
        if not active or not self.environments:
            return self.server

        override = self.environments.get(active)
        if override is None:
            return self.server

        if self.server is None:
            # Build a minimal Server from the override alone
            return Server(
                type="local",
                path=override.path,
                format=override.format or "parquet",
            )

        # Shallow-copy server and apply overrides
        data = self.server.model_dump()
        data["path"] = override.path
        if override.format is not None:
            data["format"] = override.format
        return Server(**data)


class TraceStep(BaseModel):
    """Execution step metadata for debugging/visualization."""

    step: str
    timestamp: float
    input_rows: Optional[int] = None
    output_rows: Optional[int] = None
    duration_ms: Optional[float] = None
    details: Dict[str, Any] = Field(default_factory=dict)
    status: str = "ok"  # ok, warning, error


class ExecutionTrace(BaseModel):
    """Collection of execution steps for a single run."""

    run_id: Optional[str] = None
    steps: List[TraceStep] = Field(default_factory=list)
    total_duration_ms: Optional[float] = None
