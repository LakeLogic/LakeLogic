from pydantic import BaseModel, Field, ConfigDict, AliasChoices
from typing import List, Optional, Dict, Any, Union
from enum import Enum
from datetime import datetime

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
    from_name: str = Field(alias="from")
    to_name: str = Field(alias="to")

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
    join: Optional[TransformationJoin] = None
    sql: Optional[str] = None
    phase: str = "post"  # pre | post

class RowRuleNotNull(BaseModel):
    """Business-friendly not-null rule."""
    not_null: Union[str, Dict[str, Any]]

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

class Quality(BaseModel):
    """Quality rule groups for row and dataset checks."""
    row_rules: List[Union[QualityRule, RowRuleNotNull, RowRuleAcceptedValues, RowRuleRegexMatch, RowRuleRange, RowRuleReferentialIntegrity]] = Field(default_factory=list)
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

class LineageConfig(BaseModel):
    """Lineage capture settings."""
    model_config = ConfigDict(extra="allow")
    enabled: bool = False
    capture_source_path: bool = True
    capture_timestamp: bool = True
    capture_run_id: bool = True
    source_column_name: str = "_lakeguard_source"
    timestamp_column_name: str = "_lakeguard_processed_at"
    run_id_column_name: str = "_lakeguard_run_id"


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
