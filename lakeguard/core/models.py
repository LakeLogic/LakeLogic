from pydantic import BaseModel, Field, ConfigDict, AliasChoices
from typing import List, Optional, Dict, Any, Union
from enum import Enum
from datetime import datetime

class Info(BaseModel):
    title: str
    version: str
    description: Optional[str] = None
    owner: Optional[str] = None
    contact: Optional[Union[str, Dict[str, str]]] = None

class Server(BaseModel):
    type: str # s3, gcs, adls, azure, local, glue
    format: str = "parquet" # parquet, delta, iceberg, csv, json
    path: str # e.g. s3://bucket/path, gs://bucket/path, abfss://container@account...
    
    # Ingestion Controls
    mode: str = "validate" # 'validate' for Quality Gate, 'ingest' for Raw-to-Bronze movement
    schema_evolution: str = "strict" # strict, append, merge, overwrite
    allow_schema_drift: bool = False

class Environment(BaseModel):
    path: str
    format: Optional[str] = None

class SchemaPolicy(BaseModel):
    evolution: str = "strict" # strict, compatible, allow
    unknown_fields: str = "quarantine" # quarantine, drop, allow

class Link(BaseModel):
    name: str
    path: str
    type: str = "parquet"

class TransformationRename(BaseModel):
    from_name: str = Field(alias="from")
    to_name: str = Field(alias="to")

class TransformationDerive(BaseModel):
    field: str
    sql: str

class TransformationLookup(BaseModel):
    field: str
    reference: str
    on: str
    key: str
    value: str
    default_value: Optional[Any] = None # Handles orphaned keys (-1, 'Unknown')

class TransformationFilter(BaseModel):
    sql: str

class TransformationDeduplicate(BaseModel):
    on: List[str]
    sort_by: Optional[List[str]] = None
    order: str = "desc"

class Transformation(BaseModel):
    model_config = ConfigDict(extra="allow")
    rename: Optional[TransformationRename] = None
    derive: Optional[TransformationDerive] = None
    lookup: Optional[TransformationLookup] = None
    filter: Optional[TransformationFilter] = None
    deduplicate: Optional[TransformationDeduplicate] = None

class QualityRule(BaseModel):
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
    row_rules: List[QualityRule] = Field(default_factory=list)
    dataset_rules: List[QualityRule] = Field(default_factory=list)

class Notification(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    type: str # slack, teams, email, webhook
    target: str = Field(alias=AliasChoices("target", "to", "channel", "url"))
    on_events: List[str] = Field(default_factory=lambda: ["quarantine", "failure"])

class Quarantine(BaseModel):
    target: Optional[str] = None
    enabled: bool = True
    include_error_reason: bool = True
    notifications: List[Notification] = Field(default_factory=list)

class ServiceLevelObjective(BaseModel):
    description: Optional[str] = None
    threshold: Optional[Union[str, float]] = None
    field: Optional[str] = None

class ServiceLevel(BaseModel):
    freshness: Optional[Union[str, ServiceLevelObjective]] = None
    availability: Optional[Union[float, ServiceLevelObjective]] = None # percentage e.g. 99.9

class FieldDefinition(BaseModel):
    name: str
    type: str
    required: bool = False
    pii: bool = False
    classification: Optional[str] = None
    description: Optional[str] = None
    rules: List[QualityRule] = Field(default_factory=list)

class Model(BaseModel):
    fields: List[FieldDefinition] = Field(default_factory=list)

class Materialization(BaseModel):
    model_config = ConfigDict(extra="allow")
    strategy: str = "append" # append, merge, scd2, overwrite
    partition_by: List[str] = Field(default_factory=list)
    cluster_by: List[str] = Field(default_factory=list)
    reprocess_policy: str = "overwrite_partition" # how to handle re-runs

class LineageConfig(BaseModel):
    model_config = ConfigDict(extra="allow")
    capture_source_path: bool = True
    capture_timestamp: bool = True
    capture_run_id: bool = True
    source_column_name: str = "_lakeguard_source"
    timestamp_column_name: str = "_lakeguard_processed_at"
    run_id_column_name: str = "_lakeguard_run_id"

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
    
    # ORCHESTRATION & DEPENDENCIES
    upstream: List[str] = Field(default_factory=list)
    schedule: Optional[str] = None
    
    schema_policy: Optional[SchemaPolicy] = None
    model: Optional[Model] = None
    quality: Optional[Quality] = Field(default_factory=Quality)
    transformations: List[Transformation] = Field(default_factory=list)
    service_levels: Optional[ServiceLevel] = None
    quarantine: Optional[Quarantine] = Field(default_factory=Quarantine)
