from pydantic import BaseModel, Field, ConfigDict, AliasChoices, field_validator
from typing import List, Optional, Dict, Any, Union
from enum import Enum
from datetime import datetime
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

class Server(BaseModel):
    """Storage and ingestion settings for a contract."""
    type: str # s3, gcs, adls, azure, local, glue
    format: str = "parquet" # parquet, delta, iceberg, csv, json
    path: str # e.g. s3://bucket/path, gs://bucket/path, abfss://container@account...
    
    # Ingestion Controls
    mode: str = "validate" # 'validate' for Quality Gate, 'ingest' for Raw-to-Bronze movement
    schema_evolution: str = "strict" # strict, append, merge, overwrite
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

class SchemaPolicy(BaseModel):
    """Schema enforcement rules for unknown and evolving fields."""
    evolution: str = "strict" # strict, compatible, allow
    unknown_fields: str = "quarantine" # quarantine, drop, allow

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
    default_value: Optional[Any] = None # Handles orphaned keys (-1, 'Unknown')

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

class RowRuleReferentialIntegrity(BaseModel):
    """Business-friendly referential integrity rule."""
    referential_integrity: Dict[str, Any]

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
    severity: str = "error" # error, warning, info
    
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
                f"Unknown quality rule category '{value}'. "
                f"Expected one of: {', '.join(sorted(_QUALITY_CATEGORIES))}.",
                UserWarning,
            )
        return text

class Quality(BaseModel):
    """Quality rule groups for row and dataset checks."""
    enforce_required: bool = True
    row_rules: List[Union[QualityRule, RowRuleNotNull, RowRuleAcceptedValues, RowRuleRegexMatch, RowRuleRange, RowRuleReferentialIntegrity, RowRuleLifecycleWindow]] = Field(default_factory=list)
    dataset_rules: List[Union[QualityRule, DatasetRuleUnique, DatasetRuleNullRatio, DatasetRuleRowCountBetween]] = Field(default_factory=list)

class Notification(BaseModel):
    """Notification channel configuration."""
    model_config = ConfigDict(populate_by_name=True, extra="allow")
    type: str # slack, teams, email, webhook
    target: str = Field(alias=AliasChoices("target", "to", "channel", "url"))
    on_events: List[str] = Field(default_factory=lambda: ["quarantine", "failure"])

class Quarantine(BaseModel):
    """Quarantine settings and notification routing."""
    target: Optional[str] = None
    enabled: bool = True
    include_error_reason: bool = True
    strict_notifications: bool = True
    notifications: List[Notification] = Field(default_factory=list)

class ServiceLevelObjective(BaseModel):
    """Service-level objective definition."""
    description: Optional[str] = None
    threshold: Optional[Union[str, float]] = None
    field: Optional[str] = None

class ServiceLevel(BaseModel):
    """Service-level settings for freshness and availability."""
    freshness: Optional[Union[str, ServiceLevelObjective]] = None
    availability: Optional[Union[float, ServiceLevelObjective]] = None # percentage e.g. 99.9

class FieldDefinition(BaseModel):
    """Schema field definition."""
    name: str
    type: str
    required: bool = False
    pii: bool = False
    classification: Optional[str] = None
    description: Optional[str] = None
    rules: List[QualityRule] = Field(default_factory=list)

class Model(BaseModel):
    """Schema model definition."""
    fields: List[FieldDefinition] = Field(default_factory=list)

class Materialization(BaseModel):
    """Materialization settings for writing outputs."""
    model_config = ConfigDict(extra="allow")
    strategy: str = "append" # append, merge, scd2, overwrite
    partition_by: List[str] = Field(default_factory=list)
    cluster_by: List[str] = Field(default_factory=list)
    reprocess_policy: str = "overwrite_partition" # how to handle re-runs
    target_path: Optional[str] = None
    format: Optional[str] = None
    scd2: Optional[Dict[str, Any]] = None
    soft_delete_column: Optional[str] = None # e.g. '_lakelogic_is_deleted'
    soft_delete_value: Any = True # Value to set when deleted
    soft_delete_time_column: Optional[str] = None # e.g. '_lakelogic_deleted_at'
    soft_delete_reason_column: Optional[str] = None # e.g. '_lakelogic_delete_reason'

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
    metadata: Dict[str, Any] = Field(default_factory=dict) # For generic tagging (status, classification)
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
    logic: Optional[str] = None # Full SQL for materialization

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
