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


class Info(BaseModel):
    """Contract metadata such as title, version, and ownership."""

    title: str
    table_name: Optional[str] = None  # canonical table name (e.g. "bronze_ga_events")
    version: str = "1.0.0"
    description: Optional[str] = None
    owner: Optional[str] = None
    contact: Optional[Union[str, Dict[str, str]]] = None
    target_layer: Optional[str] = None
    status: Optional[str] = None
    classification: Optional[str] = None
    domain: Optional[str] = None  # e.g. "real-estate", "finance", "logistics"
    system: Optional[str] = None  # e.g. "zoopla", "salesforce", "sap"


class Server(BaseModel):
    """Storage and ingestion settings for a contract."""

    type: str  # s3, gcs, adls, azure, local, glue
    format: str = "parquet"  # parquet, delta, iceberg, csv, json
    path: str  # e.g. s3://bucket/path, gs://bucket/path, abfss://container@account...

    # Ingestion Controls
    mode: str = "validate"  # 'validate' for Quality Gate, 'ingest' for Raw-to-Bronze movement
    schema_evolution: str = "strict"  # strict, append, merge, overwrite
    allow_schema_drift: bool = False
    cast_to_string: bool = False


class Environment(BaseModel):
    """Environment-specific path/format overrides."""

    path: str
    format: Optional[str] = None


class SourcePartition(BaseModel):
    """Date-partitioned landing directory configuration.

    Limits file globbing to only the relevant date partitions instead
    of scanning the entire landing directory.

    Example YAML::

        source:
          path: "{landing_root}/events"
          partition:
            format: "y_%Y/m_%m/d_%d"   # strftime tokens
            lookback_days: 3
    """

    model_config = ConfigDict(extra="allow")

    format: str  # strftime format, e.g. "y_%Y/m_%m/d_%d"
    lookback_days: Optional[int] = None  # how many days back to scan; None = all partitions
    start_date: Optional[str] = None  # ISO date override for backfills
    end_date: Optional[str] = None  # ISO date override for backfills
    file_pattern: Optional[str] = None  # glob pattern; auto-derives from source.format


class SourceConfig(BaseModel):
    """Source acquisition settings for landing/stream/table inputs."""

    model_config = ConfigDict(extra="allow")

    type: str  # landing | stream | table
    path: Optional[str] = None
    format: Optional[str] = None
    load_mode: str = "full"  # full | incremental | cdc
    pattern: Optional[str] = None
    watermark_field: Optional[str] = None
    cdc_op_field: Optional[str] = None
    cdc_delete_values: List[str] = Field(default_factory=list)
    cdc_timestamp_field: Optional[str] = None

    # Date-partitioned landing support
    partition: Optional[SourcePartition] = None

    # ── Incremental processing strategy ──────────────────────────────────────
    # Declares HOW the pipeline resolves the (from_dt, to_dt) window for
    # incremental reads.  Consumed by IncrementalBoundary.from_contract().
    #
    # Strategies:
    #   max_target    — MAX(watermark_field) on target Delta table (default)
    #   pipeline_log  — last successful run in a pipeline audit log table
    #   manifest      — JSON manifest file listing processed partition values
    #   lookback      — sliding window back from NOW (e.g. "7 days", "3 hours")
    #   date_range    — explicit from_date / to_date (useful for backfills)
    #
    # Example contract YAML:
    #   source:
    #     load_mode: incremental
    #     watermark_field: _snapshot_date
    #     watermark_strategy: max_target
    #     target_path: abfss://silver@acct.dfs.core.windows.net/zoopla/listings
    #
    # OR (lookback):
    #   source:
    #     load_mode: incremental
    #     watermark_field: _snapshot_date
    #     watermark_strategy: lookback
    #     lookback: "7 days"          # "3 hours" | "30 mins" | "1 month" etc.
    #
    # OR (pipeline log):
    #   source:
    #     load_mode: incremental
    #     watermark_field: _snapshot_date
    #     watermark_strategy: pipeline_log
    #     pipeline_log_table: meta.pipeline_runs
    #     pipeline_name: bronze_to_silver_zoopla_listings
    #
    # OR (manifest):
    #   source:
    #     load_mode: incremental
    #     watermark_field: _snapshot_date
    #     watermark_strategy: manifest
    #     manifest_path: /dbfs/mnt/meta/manifests/bronze_to_silver_zoopla.json
    #   delta_version — Snapshot version tracking for Spark Delta tables
    #
    # Example contract YAML:
    # max_target | pipeline_log | manifest | lookback | date_range | delta_version
    watermark_strategy: Optional[str] = "max_target"
    target_path: Optional[str] = None  # required when strategy == max_target
    lookback: Optional[str] = None  # e.g. "7 days", "3 hours" — strategy == lookback
    from_date: Optional[str] = None  # ISO date — strategy == date_range
    to_date: Optional[str] = None  # ISO date — strategy == date_range
    pipeline_log_table: Optional[str] = None  # Spark table name — strategy == pipeline_log
    pipeline_name: Optional[str] = None  # pipeline identifier — strategy == pipeline_log
    manifest_path: Optional[str] = None  # JSON file path — strategy == manifest

    # ── Multi-column partition support ────────────────────────────────────────────
    # watermark_date_parts: maps logical date roles to actual column names.
    # Used when the temporal boundary is spread across multiple columns rather
    # than a single date/timestamp field.
    #
    # YAML form (positional list — [year_col, month_col, day_col]):
    #   watermark_date_parts: [year, month, day]
    #
    # YAML form (named dict — when column names differ from year/month/day):
    #   watermark_date_parts:
    #     year:  partition_year
    #     month: partition_month
    #     day:   partition_day
    #
    # partition_filters: static (non-temporal) partition values ANDed into
    # every filter expression alongside the temporal range.
    #
    #   partition_filters:
    #     country: GB
    #     region:  south
    watermark_date_parts: Optional[Union[List[str], Dict[str, str]]] = None
    partition_filters: Dict[str, Any] = Field(default_factory=dict)

    # ── JSON flattening for silver/gold reading bronze tables ──────────────────
    # When the upstream table stores nested objects as JSON strings (e.g. bronze
    # written with preserve_nested=True), set flatten_nested to expand them into
    # flat ``parent_child`` columns before schema validation runs.
    #
    # Values (same semantics as infer_contract's preserve_nested flag, inverted):
    #   false (default) — no flattening; load as-is
    #   true            — flatten ALL JSON-string columns automatically
    #   [col, col, ...]  — flatten only the named columns
    #
    # Example (silver contract):
    #   source:
    #     type: table
    #     path: .../bronze/bronze_zoopla_listing
    #     flatten_nested: [derived, pricing, location]
    flatten_nested: Union[bool, List[str]] = False

    _SOURCE_KNOWN_KEYS: set = {
        "type", "path", "format", "load_mode", "pattern",
        "watermark_field", "cdc_op_field", "cdc_delete_values",
        "cdc_timestamp_field", "partition", "options",
        "watermark_strategy", "target_path", "lookback",
        "from_date", "to_date", "pipeline_log_table",
        "pipeline_name", "manifest_path",
        "watermark_date_parts", "partition_filters",
        "flatten_nested",
    }

    @model_validator(mode="after")
    def _warn_unknown_keys(self) -> "SourceConfig":
        _warn_unknown_extra_keys(self, self._SOURCE_KNOWN_KEYS, "source")
        return self


class SchemaPolicy(BaseModel):
    """Schema enforcement rules for unknown and evolving fields."""

    evolution: str = "strict"  # strict, compatible, allow
    unknown_fields: str = "quarantine"  # quarantine, drop, allow


class Link(BaseModel):
    """Reference dataset link (file path or table name)."""

    name: str
    path: Optional[str] = None
    type: str = "parquet"  # parquet, csv, table
    table: Optional[str] = None
    broadcast: bool = False
    # Column projection — only load these columns from the linked table.
    # Reduces DataFrame footprint by avoiding loading unused columns.
    # If empty, all columns are loaded (default behavior).
    columns: List[str] = Field(default_factory=list)


class TransformationRename(BaseModel):
    """Rename a column prior to validation."""

    model_config = ConfigDict(populate_by_name=True, extra="allow")
    from_name: Optional[str] = Field(default=None, alias="from")
    to_name: Optional[str] = Field(default=None, alias="to")
    mappings: Optional[Dict[str, str]] = None

    def iter_pairs(self) -> List[tuple[str, str]]:
        if self.mappings:
            return [(src, dst) for src, dst in self.mappings.items() if src and dst]
        if self.from_name and self.to_name:
            return [(self.from_name, self.to_name)]
        return []


class TransformationDerive(BaseModel):
    """Derive a new field from a SQL expression.

    ``sql`` is the default/Spark expression.  When running on a different
    engine you can supply an engine-specific override:

    * ``sql_duckdb`` — used by the Polars and DuckDB adapters.
    * ``sql_spark`` — explicit Spark override (falls back to ``sql``).
    """

    field: str
    sql: str
    sql_duckdb: Optional[str] = None
    sql_spark: Optional[str] = None


class TransformationLookup(BaseModel):
    """Lookup/join enrichment configuration."""

    field: str
    reference: str
    on: str
    key: str
    value: str
    default_value: Optional[Any] = None  # Handles orphaned keys (-1, 'Unknown')


class TransformationFilter(BaseModel):
    """Row-level filter expressed in SQL."""

    sql: str

    @model_validator(mode="before")
    @classmethod
    def _accept_string_shorthand(cls, data: Any) -> Any:
        """Accept ``filter: 'SQL'`` as shorthand for ``filter: {sql: 'SQL'}``."""
        if isinstance(data, str):
            return {"sql": data}
        return data


class TransformationDeduplicate(BaseModel):
    """Deduplication rule configuration."""

    model_config = ConfigDict(populate_by_name=True)

    on: List[str] = Field(validation_alias=AliasChoices("on", "by"))
    sort_by: Optional[List[str]] = None
    order: str = "desc"


class TransformationSelect(BaseModel):
    """Select a subset of columns."""

    columns: List[str]


class TransformationDrop(BaseModel):
    """Drop columns by name."""

    columns: List[str]


class TransformationCast(BaseModel):
    """Cast columns to specific types."""

    columns: Dict[str, str]


class TransformationTrim(BaseModel):
    """Trim whitespace from fields."""

    fields: List[str]
    side: str = "both"  # both | left | right


class TransformationLower(BaseModel):
    """Lower-case string fields."""

    fields: List[str]


class TransformationUpper(BaseModel):
    """Upper-case string fields."""

    fields: List[str]


class TransformationCoalesce(BaseModel):
    """Coalesce multiple fields into a single output."""

    field: str
    sources: List[str] = Field(default_factory=list)
    default: Optional[Any] = None
    output: Optional[str] = None


class TransformationSplit(BaseModel):
    """Split a string field into an array."""

    field: str
    delimiter: str = ","
    output: Optional[str] = None


class TransformationExplode(BaseModel):
    """Explode an array field into multiple rows."""

    field: str
    output: Optional[str] = None


class TransformationMapValues(BaseModel):
    """Map input values to output values."""

    field: str
    mapping: Dict[str, Any]
    default: Optional[Any] = None
    output: Optional[str] = None


class TransformationRollup(BaseModel):
    """Aggregate data and retain rollup lineage keys."""

    group_by: List[str] = Field(default_factory=list)
    aggregations: Dict[str, str] = Field(default_factory=dict)  # output_name -> SQL expression
    keys: Optional[Union[str, List[str]]] = None
    key_expr: Optional[str] = None
    rollup_keys_column: Optional[str] = "_lakelogic_rollup_keys"
    rollup_keys_count_column: Optional[str] = "_lakelogic_rollup_keys_count"
    upstream_run_id_column: Optional[str] = "_upstream_run_id"
    upstream_run_ids_column: Optional[str] = "_upstream_lakelogic_run_ids"
    distinct: bool = True


class TransformationJoin(BaseModel):
    """Join a reference table to enrich multiple fields."""

    reference: str
    on: str
    key: str
    fields: List[str]
    type: str = "left"  # left | inner | right | full
    prefix: Optional[str] = None
    defaults: Dict[str, Any] = Field(default_factory=dict)


class TransformationPivot(BaseModel):
    """Pivot rows into columns using conditional aggregation."""

    model_config = ConfigDict(extra="allow")
    id_vars: List[str] = Field(default_factory=list)
    pivot_col: Optional[str] = None
    pivot_cols: Optional[List[str]] = None
    value_col: Optional[str] = None
    value_cols: Optional[List[str]] = None
    values: List[Any] = Field(default_factory=list)
    pivot_values: Optional[List[Any]] = None
    agg: str = "first"
    aggs: Dict[str, str] = Field(default_factory=dict)
    fill_value: Optional[Any] = None
    separator: str = "_"
    name_template: Optional[str] = None
    value_aliases: Dict[str, str] = Field(default_factory=dict)

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


class TransformationUnpivot(BaseModel):
    """Unpivot columns into rows."""

    model_config = ConfigDict(extra="allow")
    id_vars: List[str] = Field(default_factory=list)
    value_vars: List[str] = Field(default_factory=list)
    value_cols: Optional[List[str]] = None
    key_field: str = "key"
    value_field: str = "value"
    include_nulls: bool = False
    value_aliases: Dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _normalize(self):
        if not self.value_vars and self.value_cols:
            self.value_vars = list(self.value_cols)
        return self


class TransformationBucketBin(BaseModel):
    """A single range/equality bin for TransformationBucket."""

    label: str
    lt: Optional[float] = None  # source < lt  → label
    lte: Optional[float] = None  # source <= lte → label
    gt: Optional[float] = None  # source > gt  → label
    gte: Optional[float] = None  # source >= gte → label
    eq: Optional[Any] = None  # source == eq  → label (exact match)


class TransformationBucket(BaseModel):
    """
    Map a numeric (or string) column into labelled bands.

    Compiles to a standard SQL CASE expression — identical across all engines.

    YAML example::

        - phase: post
          bucket:
            field: price_band
            source: pricing_price
            bins:
              - lt: 250000
                label: sub_250k
              - lt: 500000
                label: 250k_500k
              - lt: 1000000
                label: 500k_1m
            default: 1m_plus
    """

    field: str  # output column name
    source: str  # input column to evaluate
    bins: List[TransformationBucketBin] = Field(default_factory=list)
    default: Optional[Any] = None  # ELSE value; NULL when omitted


class TransformationJsonExtract(BaseModel):
    """
    Extract a scalar value from a JSON string column.

    Engine-agnostic: Polars uses str.json_path_match, DuckDB uses ->> operator,
    Spark uses get_json_object.

    YAML example::

        - phase: post
          json_extract:
            field: location_latitude
            source: location_coordinates
            path: "$.latitude"
            cast: float
    """

    field: str  # output column name
    source: str  # source JSON string column
    path: str  # JSONPath expression, e.g. "$.latitude"
    cast: Optional[str] = None  # optional type cast: float, integer, string


class TransformationDateRangeExplode(BaseModel):
    """
    Explode each row into one row per calendar day in [start_col, end_col].

    The output column receives successive date values. If end_col is omitted
    or null the current date is used as the upper bound.

    Engine-agnostic: Polars uses pl.date_range + explode,
    DuckDB uses generate_series + unnest.

    YAML example::

        - phase: post
          date_range_explode:
            output: snapshot_date
            start_col: creation_date
            end_col: deleted_at       # nullable — defaults to today when null
    """

    output: str  # name of the new date column
    start_col: str  # column holding range start date
    end_col: Optional[str] = None  # column holding range end date (nullable)
    interval: str = "1d"  # Polars duration string: '1d', '7d', etc.


class TransformationDateDiff(BaseModel):
    """
    Compute the integer difference between two date/timestamp columns.

    The YAML spec is engine-agnostic; each adapter emits the dialect-correct
    SQL (DATEDIFF, DATE_PART, etc.).

    YAML example::

        - phase: post
          date_diff:
            field: listing_age_days
            from_col: creation_date
            to_col: event_date
            unit: days
    """

    field: str  # output column name
    from_col: str  # earlier date column (start)
    to_col: str  # later date column (end)
    unit: str = "days"  # days | hours | months


class Transformation(BaseModel):
    """Transformation step (SQL or structured)."""

    model_config = ConfigDict(extra="allow")
    rename: Optional[TransformationRename] = None
    derive: Optional[TransformationDerive] = None
    lookup: Optional[TransformationLookup] = None
    filter: Optional[TransformationFilter] = None
    deduplicate: Optional[TransformationDeduplicate] = None
    select: Optional[TransformationSelect] = None
    drop: Optional[TransformationDrop] = None
    cast: Optional[TransformationCast] = None
    trim: Optional[TransformationTrim] = None
    lower: Optional[TransformationLower] = None
    upper: Optional[TransformationUpper] = None
    coalesce: Optional[TransformationCoalesce] = None
    split: Optional[TransformationSplit] = None
    explode: Optional[TransformationExplode] = None
    map_values: Optional[TransformationMapValues] = None
    rollup: Optional[TransformationRollup] = None
    join: Optional[TransformationJoin] = None
    pivot: Optional[TransformationPivot] = None
    unpivot: Optional[TransformationUnpivot] = None
    json_extract: Optional[TransformationJsonExtract] = None
    date_range_explode: Optional[TransformationDateRangeExplode] = None
    bucket: Optional[TransformationBucket] = None
    date_diff: Optional[TransformationDateDiff] = None
    sql: Optional[str] = None
    phase: str = "post"  # pre | post


class RowRuleNotNull(BaseModel):
    """Business-friendly not-null rule."""

    not_null: Union[str, Dict[str, Any], List[Union[str, Dict[str, Any]]]]


class RowRuleAcceptedValues(BaseModel):
    """Business-friendly accepted values rule."""

    accepted_values: Dict[str, Any]


class RowRuleRegexMatch(BaseModel):
    """Business-friendly regex match rule."""

    regex_match: Dict[str, Any]


class RowRuleRange(BaseModel):
    """Business-friendly range rule."""

    range: Dict[str, Any]


class ForeignKeyRef(BaseModel):
    """
    Declaration of a foreign-key relationship on a field.

    Used in two places:
      1. ``FieldDefinition.foreign_key`` — field-level documentation + generator hint.
         The generator samples FK column values from the PK pool of the referenced contract.
      2. ``RowRuleReferentialIntegrity.referential_integrity`` — quality-rule payload
         that the DataProcessor evaluates at validation time.

    Contract YAML example
    ---------------------
    # Field-level (documentation + generator hint)
    schema:
      columns:
        - name: agent_id
          type: BIGINT
          foreign_key:
            contract: silver_agents   # LakeLogic contract name
            column:   agent_id        # PK column in that contract

    # Quality rule (validation)
    quality:
      row_rules:
        - referential_integrity:
            field:    agent_id
            contract: silver_agents
            column:   agent_id
            severity: critical

    dbt equivalent
    ---------------
    - name: agent_id
      tests:
        - relationships:
            to:    ref('agents')
            field: agent_id
    """

    contract: str  # LakeLogic contract name (e.g. 'silver_agents')
    column: str  # PK column in the referenced contract
    severity: str = "error"  # error | warning | info


class RowRuleReferentialIntegrity(BaseModel):
    """Business-friendly referential integrity rule."""

    referential_integrity: Dict[str, Any]  # keys: field, contract, column, severity


class RowRuleLifecycleWindow(BaseModel):
    """Business-friendly lifecycle window rule."""

    lifecycle_window: Dict[str, Any]


class DatasetRuleUnique(BaseModel):
    """Business-friendly unique rule."""

    unique: Union[str, Dict[str, Any]]


class DatasetRuleNullRatio(BaseModel):
    """Business-friendly null ratio rule."""

    null_ratio: Dict[str, Any]


class DatasetRuleRowCountBetween(BaseModel):
    """Business-friendly row count rule."""

    row_count_between: Dict[str, Any]


class QualityRule(BaseModel):
    """Row-level or dataset-level quality rule."""

    name: str
    sql: str
    category: str = "correctness"
    description: Optional[str] = None
    severity: str = "error"  # error, warning, info
    phase: str = "pre"  # pre | post — post-phase rules run after transforms

    # Thresholds for dataset-level rules
    must_be_between: Optional[List[float]] = None
    must_be_less_than: Optional[float] = None
    must_be_greater_than: Optional[float] = None

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


class Quality(BaseModel):
    """Quality rule groups for row and dataset checks."""

    enforce_required: bool = True
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


class Notification(BaseModel):
    """
    Notification channel configuration.

    Minimal usage — just ``target`` and ``on_events``::

        notifications:
          - target: "env:TEAMS_WEBHOOK"
            on_events: [quarantine, failure]

    The ``type`` field defaults to ``apprise`` which auto-detects the
    channel from the target URL scheme (``mailto://``, ``slack://``,
    ``msteams://``, etc.).  Set ``type`` explicitly only when using the
    legacy built-in adapters (``smtp``, ``sendgrid``, ``slack``,
    ``teams``, ``webhook``).
    """

    model_config = ConfigDict(populate_by_name=True, extra="allow")
    type: str = "apprise"  # apprise (default) | slack | teams | email/smtp | sendgrid | webhook
    target: Optional[str] = Field(default=None, alias=AliasChoices("target", "to", "channel", "url"))
    targets: Optional[List[str]] = None  # Apprise multi-channel fan-out
    on_events: List[str] = Field(default_factory=lambda: ["quarantine", "failure"])
    subject_template: Optional[str] = None
    subject_template_file: Optional[str] = None
    message_template: Optional[str] = None
    message_template_file: Optional[str] = None
    # Alias support for message template naming.
    body_template: Optional[str] = None
    body_template_file: Optional[str] = None
    template_context: Dict[str, Any] = Field(default_factory=dict)


class Quarantine(BaseModel):
    """Quarantine settings and notification routing."""

    target: Optional[str] = None
    table: Optional[str] = None  # shared quarantine table (e.g. "{domain_catalog}._quarantine")
    enabled: bool = True
    include_error_reason: bool = True
    strict_notifications: bool = True
    # Output format for file-based quarantine targets.
    # Supported: parquet (default), csv, delta, json.
    # delta requires the deltalake package for Polars/DuckDB engines;
    # Spark accepts delta, iceberg, parquet, csv, json natively.
    format: Optional[str] = None
    # Write mode for the quarantine target.
    # append (default) — adds new bad records to existing data.
    # overwrite        — replaces the quarantine target on every run.
    write_mode: str = "append"
    notifications: List[Notification] = Field(default_factory=list)


class ServiceLevelObjective(BaseModel):
    """Service-level objective definition."""

    description: Optional[str] = None
    threshold: Optional[Union[str, float]] = None
    field: Optional[str] = None


class RowCountSLO(BaseModel):
    """Row count SLO for individual contracts.

    Validates that the output row count falls within expected bounds.
    Checked against the field specified by check_field (default: counts_good).

    When skip_reprocess_days is set and the reprocess date range exceeds that
    threshold, SLO checks and counts_source computation are skipped entirely
    to avoid expensive Spark wide-transformation actions on large backfills.
    """

    min_rows: Optional[int] = None  # Minimum expected rows (fail if below)
    max_rows: Optional[int] = None  # Maximum expected rows (fail if above)
    check_field: str = "counts_good"  # counts_good | counts_source | counts_total
    skip_reprocess_days: int = 3  # Skip SLO + source count when reprocess range > N days (0 = never skip)
    description: Optional[str] = None


class ServiceLevel(BaseModel):
    """Service-level settings for freshness, availability, and row counts."""

    freshness: Optional[Union[str, ServiceLevelObjective]] = None
    availability: Optional[Union[float, ServiceLevelObjective]] = None  # field completeness percentage e.g. 99.9
    row_count: Optional[RowCountSLO] = None  # min/max row count bounds per contract


class FieldDefinition(BaseModel):
    """Schema field definition."""

    name: str
    type: str
    required: bool = False
    pii: bool = False
    phi: bool = False
    classification: Optional[str] = None
    description: Optional[str] = None
    rules: List[QualityRule] = Field(default_factory=list)
    # Generator hints — persisted so DataGenerator.from_dbt() round-trips correctly.
    # accepted_values: generator picks from this list; validator checks the IN rule.
    # min / max: generator stays within range; validator checks >=/< expression rules.
    accepted_values: Optional[List[Any]] = None
    min: Optional[float] = None
    max: Optional[float] = None
    # Foreign-key hint — generator samples from the PK pool of the referenced contract.
    # At validation time, DataProcessor evaluates the corresponding referential_integrity rule.
    foreign_key: Optional[ForeignKeyRef] = None

    # ── Kimball Dimensional Modelling ─────────────────────────────────────────
    nullable: Optional[bool] = None  # Explicit nullability (critical for accumulating snapshot milestones)
    milestone: bool = False  # Flag for accumulating snapshot milestone date columns
    generated: bool = False  # Flag for auto-generated fields (surrogate keys, SCD2 validity columns)

    # ── PII Security Group Mapping ───────────────────────────────────────────
    # security_groups: list of group names that are allowed to see unmasked values.
    # If the current user is NOT in any of these groups, the masking strategy is applied.
    # Groups map to IAM/AD groups, Databricks UC groups, or custom group providers.
    security_groups: List[str] = Field(default_factory=list)
    # Per-field default masking strategy: nullify | hash | redact | partial | encrypt
    masking: Optional[str] = None
    # Custom format template for 'partial' masking.
    # Tokens: {first1}-{first9}, {last1}-{last9}, {domain} (email)
    # Example: "{first1}***@{domain}" → j***@company.com
    # Example: "***-***-{last4}"      → ***-***-1234
    # Example: "{first2}** ***"       → SW** *** (postcode)
    # If omitted, auto-detects email/phone/generic patterns.
    masking_format: Optional[str] = None
    # Optional reference to an encrypted PII vault table for reversible unmasking.
    # URI format: keyvault://vault-name/secret-name | databricks://scope/key | env://VAR
    pii_vault: Optional[str] = None

    # LLM extraction hints (used when field appears in extraction.output_schema)
    extraction_task: Optional[str] = None  # ner | classification | summarization | local_llm
    extraction_examples: List[str] = Field(default_factory=list)  # few-shot examples
    max_length: Optional[int] = None  # max string length for extracted text


class Model(BaseModel):
    """Schema model definition."""

    fields: List[FieldDefinition] = Field(default_factory=list)
    grain: Optional[str] = None  # Human-readable grain description (e.g. "one row per order lifecycle")
    grain_key: List[str] = Field(default_factory=list)  # Conceptual grain columns (subset of primary_key)


class FactConfig(BaseModel):
    """
    Kimball Fact Table Automated Governance.

    Automatically injects pipeline constraints based on the defined Fact Table architecture:
    - accumulating_snapshot: ensures milestone dates are monotonically increasing
    - transaction: lock strategy to append
    - factless: asserts no metric columns exist

    YAML example::
        materialization:
          strategy: merge
          fact:
            type: accumulating_snapshot
            milestone_dates:
              - placed_date
              - shipped_date
    """

    type: str  # transaction | periodic_snapshot | accumulating_snapshot | factless | aggregate
    milestone_dates: List[str] = Field(default_factory=list)


class Materialization(BaseModel):
    """Materialization settings for writing outputs."""

    model_config = ConfigDict(extra="allow")
    strategy: str = "append"  # append, merge, scd2, overwrite
    partition_by: List[str] = Field(default_factory=list)
    cluster_by: List[str] = Field(default_factory=list)
    reprocess_policy: str = "overwrite_partition"  # how to handle re-runs
    reprocess_date_column: Optional[str] = None  # column for date-range reprocessing (defaults to first partition_by)
    target_path: Optional[str] = None
    format: Optional[str] = None
    location: Optional[str] = (
        None  # External storage location for UC tables (e.g. abfss://container@account.dfs.core.windows.net/path/)
    )
    scd2: Optional[Dict[str, Any]] = None
    fact: Optional[FactConfig] = None
    soft_delete_column: Optional[str] = None  # e.g. '_lakelogic_is_deleted'
    soft_delete_value: Any = True  # Value to set when deleted
    soft_delete_time_column: Optional[str] = None  # e.g. '_lakelogic_deleted_at'
    soft_delete_reason_column: Optional[str] = None  # e.g. '_lakelogic_delete_reason'
    table_properties: Optional[Dict[str, str]] = None  # e.g. {'delta.autoOptimize.optimizeWrite': 'true'}
    compaction: Optional[Dict[str, Any]] = None  # e.g. {'auto': True, 'vacuum_retention_hours': 168}
    unknown_member: Optional[Dict[str, Any]] = None  # Kimball unknown member row for dimensions

    _MAT_KNOWN_KEYS: set = {
        "strategy", "partition_by", "cluster_by", "reprocess_policy",
        "reprocess_date_column", "target_path", "format", "location",
        "scd2", "scd1", "fact",
        "soft_delete_column", "soft_delete_value",
        "soft_delete_time_column", "soft_delete_reason_column",
        "table_properties", "compaction", "unknown_member",
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


class DownstreamConsumer(BaseModel):
    """
    A downstream consumer of a contract's output.

    Enables end-to-end lineage tracking from source → gold → dashboard/report.
    Declared on gold-layer contracts to capture what uses the data.

    Example YAML:
        downstream:
          - type: dashboard
            name: Monthly Revenue Dashboard
            platform: power_bi
            url: https://app.powerbi.com/groups/.../dashboards/...
            owner: analytics-team
            refresh: "daily 06:00 UTC"

          - type: report
            name: Weekly Sales Report
            platform: databricks_sql

          - type: api
            name: Customer Lookup API
            platform: internal
            url: https://api.internal.com/v1/customers

          - type: ml_model
            name: Churn Prediction
            platform: mlflow
            owner: data-science
    """

    model_config = ConfigDict(extra="allow")

    type: str  # dashboard | report | api | ml_model | application | notebook | export
    name: str  # human-readable consumer name
    platform: Optional[str] = None  # power_bi | tableau | looker | databricks_sql | metabase | grafana | custom
    url: Optional[str] = None  # link to the dashboard/report/API
    owner: Optional[str] = None  # team or person responsible
    description: Optional[str] = None  # what this consumer does
    refresh: Optional[str] = None  # refresh schedule (e.g. "daily 06:00 UTC")
    columns_used: List[str] = Field(default_factory=list)  # which columns from this contract are used
    sla: Optional[str] = None  # expected data freshness (e.g. "< 4 hours")


# ── LLM Extraction Models ────────────────────────────────────────────────────


# ExtractionField is intentionally NOT a separate class.
# output_schema reuses FieldDefinition for consistency with model.fields.
# Extraction-specific hints (extraction_task, extraction_examples, max_length)
# are optional fields on FieldDefinition itself.


class ConfidenceConfig(BaseModel):
    """
    Configuration for HOW extraction confidence is scored.

    The confidence THRESHOLD belongs in quality.row_rules, not here::

        quality:
          row_rules:
            - name: confidence_gate
              sql: "_lakelogic_extraction_confidence >= 0.7"
              action: quarantine

    Methods:
      - log_probs: use token-level log probabilities (OpenAI, etc.)
      - self_assessment: ask the LLM to rate its own confidence
      - consistency: run extraction N times, measure field-level agreement
      - field_completeness: % of non-nullable fields that are present
    """

    enabled: bool = True
    method: str = "field_completeness"  # log_probs | self_assessment | consistency | field_completeness
    column: str = "_lakelogic_extraction_confidence"
    consistency_runs: int = 3  # for consistency method only


class RetryConfig(BaseModel):
    """Retry configuration for LLM API calls."""

    max_attempts: int = 3
    backoff: str = "exponential"  # fixed | linear | exponential
    initial_delay: float = 1.0  # seconds


class PreprocessingConfig(BaseModel):
    """
    Preprocessing pipeline for raw unstructured files before LLM extraction.

    Bronze holds raw files (PDFs, images, videos, audio). Before the LLM
    can extract structured data, we need to convert them to text.

    Example YAML:
        preprocessing:
          content_type: pdf
          ocr:
            enabled: true
            engine: tesseract     # tesseract | azure_di | textract | google_vision
            language: eng
          chunking:
            strategy: page        # page | paragraph | sentence | fixed_size
            max_chunk_tokens: 4000
            overlap_tokens: 200

    For video:
        preprocessing:
          content_type: video
          transcription:
            engine: whisper       # whisper | azure_speech | google_speech
            language: en
          frame_extraction:
            enabled: true
            interval_seconds: 30
            engine: gpt-4o        # vision model for frame analysis
    """

    model_config = ConfigDict(extra="allow")

    content_type: str  # pdf | image | video | audio | html | email | text

    # Text extraction (PDFs, images)
    ocr: Optional[Dict[str, Any]] = None  # {engine, language, dpi, ...}

    # Audio/video transcription
    transcription: Optional[Dict[str, Any]] = None  # {engine, language, ...}

    # Video frame extraction
    frame_extraction: Optional[Dict[str, Any]] = None  # {interval_seconds, engine}

    # Text chunking for long documents
    chunking: Optional[Dict[str, Any]] = None  # {strategy, max_chunk_tokens, overlap}

    # File path settings
    file_column: Optional[str] = None  # column containing file path/URL
    text_output_column: str = "_extracted_text"  # where extracted text goes


class ExtractionConfig(BaseModel):
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

    # LLM provider and model
    provider: str  # openai | anthropic | azure_openai | ollama | bedrock | google
    model: str  # gpt-4o-mini | claude-3.5-sonnet | llama-3-70b | gemini-2.0-flash
    temperature: float = 0.1  # low for deterministic extraction
    max_tokens: int = 1000
    response_format: str = "json"  # json | text

    # Prompt
    prompt_template: str  # Jinja2 template with access to row columns
    system_prompt: Optional[str] = None  # system message for the LLM

    # Source text configuration
    text_column: Optional[str] = None  # column containing text to extract from
    context_columns: List[str] = Field(default_factory=list)  # extra columns for context

    # Preprocessing pipeline (for raw files: PDF, image, audio, video)
    preprocessing: Optional[PreprocessingConfig] = None

    # Output schema — what the LLM should extract (same format as model.fields)
    output_schema: List[FieldDefinition] = Field(default_factory=list)

    # Processing
    batch_size: int = 50  # rows per API call
    concurrency: int = 5  # parallel API calls
    retry: Optional[RetryConfig] = Field(default_factory=RetryConfig)

    # Confidence scoring
    confidence: Optional[ConfidenceConfig] = Field(default_factory=ConfidenceConfig)

    # Cost controls
    max_cost_per_run: Optional[float] = None  # USD limit — stop if exceeded
    max_rows_per_run: Optional[int] = None  # row limit for safety

    # Fallback
    fallback_model: Optional[str] = None  # cheaper/faster model if primary fails
    fallback_provider: Optional[str] = None

    # PII safety
    redact_pii_before_llm: bool = False  # run PII masking before sending to LLM
    pii_fields: List[str] = Field(default_factory=list)  # fields to redact


class LineageConfig(BaseModel):
    """Lineage capture settings."""

    model_config = ConfigDict(extra="allow")
    enabled: bool = False
    capture_source_path: bool = True
    capture_timestamp: bool = True
    capture_run_id: bool = True
    source_column_name: str = "_lakelogic_source"
    timestamp_column_name: str = "_lakelogic_processed_at"
    run_id_column_name: str = "_lakelogic_run_id"
    capture_contract_name: bool = True
    contract_name_column_name: str = "_lakelogic_contract_name"
    capture_domain: bool = True
    capture_system: bool = True
    domain_column_name: str = "_lakelogic_domain"
    system_column_name: str = "_lakelogic_system"
    capture_created_at: bool = True

    created_at_column_name: str = "_lakelogic_created_at"

    capture_created_by: bool = True
    created_by_column_name: str = "_lakelogic_created_by"

    created_by_override: Optional[str] = None  # Static value; bypasses auto-detection
    preserve_upstream: List[str] = Field(default_factory=list)
    upstream_prefix: str = "_upstream"
    run_id_source: str = "run_id"  # run_id | pipeline_run_id

    _LINEAGE_KNOWN_KEYS: set = {
        "enabled", "capture_source_path", "capture_timestamp",
        "capture_run_id", "source_column_name", "timestamp_column_name",
        "run_id_column_name", "capture_contract_name",
        "contract_name_column_name", "capture_domain", "capture_system",
        "domain_column_name", "system_column_name",
        "capture_created_at", "created_at_column_name",
        "capture_created_by", "created_by_column_name",
        "created_by_override", "preserve_upstream",
        "upstream_prefix", "run_id_source",
    }

    @model_validator(mode="after")
    def _warn_unknown_keys(self) -> "LineageConfig":
        _warn_unknown_extra_keys(self, self._LINEAGE_KNOWN_KEYS, "lineage")
        return self


class ExternalLogic(BaseModel):
    """External logic hook for advanced processing."""

    type: str  # python | notebook
    path: str
    entrypoint: str = "run"
    args: Dict[str, Any] = Field(default_factory=dict)
    output_path: Optional[str] = None
    output_format: Optional[str] = None  # csv | parquet
    handles_output: Optional[bool] = None  # if True, skip built-in materialize
    kernel_name: Optional[str] = None  # notebook kernel override


def _convert_odcs_to_lakelogic(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert an Open Data Contract Standard (ODCS) dict into LakeLogic format."""
    # Check if this is an ODCS contract (requires kind and apiVersion)
    if data.get("kind") != "DataContract" or "apiVersion" not in data:
        return data  # Not ODCS or already LakeLogic

    lakelogic_data = {}


    # 1. Map root properties
    # LakeLogic requires a version. We'll use the ODCS apiVersion.
    lakelogic_data["version"] = data.get("apiVersion", "v1")
    if "dataset" in data:
        lakelogic_data["info"] = {"title": data["dataset"]}

    # 2. Map schema to model.fields
    if "schema" in data and isinstance(data["schema"], list):
        fields = []
        for col in data["schema"]:
            field = {
                "name": col.get("name"),
                "type": col.get("type", "string"),
            }
            if "description" in col:
                field["description"] = col["description"]
            if "required" in col:
                field["required"] = col["required"]
            if "pii" in col:
                field["pii"] = col["pii"]
            fields.append(field)
        lakelogic_data["model"] = {"fields": fields}

    # 3. Apply customProperties.lakelogic overrides (the execution instructions)
    custom_props = data.get("customProperties", {}).get("lakelogic", {})
    if isinstance(custom_props, dict):
        for k, v in custom_props.items():
            lakelogic_data[k] = v

    # 4. Copy any missing properties for a best-effort merge
    # This allows direct pass-through of things like `metadata`, `servers` if they happen to align.
    for k, v in data.items():
        if k not in ["kind", "apiVersion", "dataset", "schema", "customProperties"] and k not in lakelogic_data:
            lakelogic_data[k] = v

    return lakelogic_data


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

    # LLM EXTRACTION (unstructured → structured)
    extraction: Optional[ExtractionConfig] = None

    # ORCHESTRATION & DEPENDENCIES
    upstream: List[str] = Field(default_factory=list)
    downstream: List[DownstreamConsumer] = Field(default_factory=list)
    schedule: Optional[str] = None

    schema_policy: Optional[SchemaPolicy] = None
    model: Optional[Model] = None
    quality: Optional[Quality] = Field(default_factory=Quality)
    transformations: List[Transformation] = Field(default_factory=list)
    service_levels: Optional[ServiceLevel] = None
    quarantine: Optional[Quarantine] = Field(default_factory=Quarantine)

    # TIER / LAYER ── mandatory for single-contract mode
    tier: Optional[str] = Field(
        default=None,
        validation_alias=AliasChoices("tier", "layer", "target_layer"),
    )

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
        "version", "info", "metadata", "server", "source", "environments",
        "links", "dataset", "primary_key", "natural_key", "lineage",
        "materialization", "logic", "external_logic", "extraction",
        "upstream", "downstream", "schedule", "schema_policy", "model",
        "quality", "transformations", "service_levels", "quarantine",
        "tier", "layer", "target_layer",
        # Recognised shorthand blocks (handled by interceptors)
        "soft_deletes",
        # ODCS / alternative schema keys accepted by _odcs_interceptor
        "schema", "tables", "columns", "properties", "fields",
        "kind", "apiVersion", "type", "status", "description",
        "datasetDomain", "quantumName", "datasetName",
        "driver", "driverVersion", "servers",
        "price", "stakeholders", "roles", "slaDefaultColumn",
        "slaProperties", "tags", "customProperties",
    }
    _PRIVATE_EXTRA_KEYS: set = {
        "_base_path", "_contract_path", "_resolved_by_pipeline",
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
            if not getattr(source, "cdc_op_field", None):
                raise ValueError(
                    "source.load_mode is 'cdc' but cdc_op_field is not set. "
                    "This field must specify the column indicating the CDC operation type.\n\n"
                    "  source:\n"
                    "    load_mode: cdc\n"
                    "    cdc_op_field: _operation\n"
                    '    cdc_delete_values: ["D", "DELETE"]'
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

        def _delete(p: "Path") -> bool:
            """Delete file or directory tree. Returns True if something was removed."""
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
                    deleted = _delete(p)
                    report["materialization"] = {
                        "path": str(p),
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
                    deleted = _delete(p)
                    report["quarantine"] = {
                        "path": str(p),
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
