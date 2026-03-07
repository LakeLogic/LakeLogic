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
import warnings

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


class Info(BaseModel):
    """Contract metadata such as title, version, and ownership."""

    title: str
    version: str
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


class SourceConfig(BaseModel):
    """Source acquisition settings for landing/stream/table inputs."""

    type: str  # landing | stream | table
    path: Optional[str] = None
    load_mode: str = "full"  # full | incremental | cdc
    pattern: Optional[str] = None
    watermark_field: Optional[str] = None
    cdc_op_field: Optional[str] = None
    cdc_delete_values: List[str] = Field(default_factory=list)

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
    watermark_strategy: str = "max_target"  # max_target | pipeline_log | manifest | lookback | date_range
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
    """Derive a new field from a SQL expression."""

    field: str
    sql: str


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


class TransformationDeduplicate(BaseModel):
    """Deduplication rule configuration."""

    on: List[str]
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
            warnings.warn(
                f"Unknown quality rule category '{value}'. Expected one of: {', '.join(sorted(_QUALITY_CATEGORIES))}.",
                UserWarning,
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


class ServiceLevel(BaseModel):
    """Service-level settings for freshness and availability."""

    freshness: Optional[Union[str, ServiceLevelObjective]] = None
    availability: Optional[Union[float, ServiceLevelObjective]] = None  # percentage e.g. 99.9


class FieldDefinition(BaseModel):
    """Schema field definition."""

    name: str
    type: str
    required: bool = False
    pii: bool = False
    classification: Optional[str] = None
    description: Optional[str] = None
    rules: List[QualityRule] = Field(default_factory=list)
    # Generator hints — persisted so DataGenerator.from_dbt() round-trips correctly.
    # accepted_values: generator picks from this list; validator checks the IN rule.
    # min / max: generator stays within range; validator checks >=/<= expression rules.
    accepted_values: Optional[List[Any]] = None
    min: Optional[float] = None
    max: Optional[float] = None
    # Foreign-key hint — generator samples from the PK pool of the referenced contract.
    # At validation time, DataProcessor evaluates the corresponding referential_integrity rule.
    foreign_key: Optional[ForeignKeyRef] = None


class Model(BaseModel):
    """Schema model definition."""

    fields: List[FieldDefinition] = Field(default_factory=list)


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
    soft_delete_column: Optional[str] = None  # e.g. '_lakelogic_is_deleted'
    soft_delete_value: Any = True  # Value to set when deleted
    soft_delete_time_column: Optional[str] = None  # e.g. '_lakelogic_deleted_at'
    soft_delete_reason_column: Optional[str] = None  # e.g. '_lakelogic_delete_reason'
    table_properties: Optional[Dict[str, str]] = None  # e.g. {'delta.autoOptimize.optimizeWrite': 'true'}
    compaction: Optional[Dict[str, Any]] = None  # e.g. {'auto': True, 'vacuum_retention_hours': 168}


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
    capture_contract_name: bool = False
    contract_name_column_name: str = "_lakelogic_contract_name"
    capture_domain: bool = True
    capture_system: bool = True
    domain_column_name: str = "_lakelogic_domain"
    system_column_name: str = "_lakelogic_system"
    preserve_upstream: List[str] = Field(default_factory=list)
    upstream_prefix: str = "_upstream"
    run_id_source: str = "run_id"  # run_id | pipeline_run_id


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


class DataContract(BaseModel):
    """
    Finalized SQL-First Data Contract Model.
    Supports ODCS-style metadata and consolidated 'sql' keywords.
    """

    model_config = ConfigDict(populate_by_name=True, extra="allow")

    version: str
    info: Optional[Info] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)  # For generic tagging (status, classification)
    server: Optional[Server] = None
    source: Optional[SourceConfig] = None
    environments: Dict[str, Environment] = Field(default_factory=dict)
    links: List[Link] = Field(default_factory=list)

    dataset: Optional[str] = None
    primary_key: List[str] = Field(default_factory=list)

    # LINEAGE & OBSERVABILITY
    lineage: Optional[LineageConfig] = Field(default_factory=LineageConfig)

    # MATERIALIZATION LAYER (Gold/Silver)
    materialization: Optional[Materialization] = Field(default_factory=Materialization)
    logic: Optional[str] = None  # Full SQL for materialization

    # EXTERNAL LOGIC
    external_logic: Optional[ExternalLogic] = None

    # ORCHESTRATION & DEPENDENCIES
    upstream: List[str] = Field(default_factory=list)
    schedule: Optional[str] = None

    schema_policy: Optional[SchemaPolicy] = None
    model: Optional[Model] = None
    quality: Optional[Quality] = Field(default_factory=Quality)
    transformations: List[Transformation] = Field(default_factory=list)
    service_levels: Optional[ServiceLevel] = None
    quarantine: Optional[Quarantine] = Field(default_factory=Quarantine)

    # ── Cross-field validation ─────────────────────────────────────────────────

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

        # ── 1. Materialization target ──────────────────────────────────────────
        if "materialization" in _targets:
            mat_path = self.materialization.target_path if self.materialization else None
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
