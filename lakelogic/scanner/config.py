"""
Scanner configuration — loaded from scanner.yaml.

Intentionally minimal: connection + discovery + SLO defaults + observatory.
No scheduling (Observatory owns that), no notifications (Observatory owns that).
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field, model_validator


def _resolve_env(val: Optional[str]) -> Optional[str]:
    """Resolve ${ENV_VAR} references in string values."""
    if not val or not isinstance(val, str):
        return val
    if val.startswith("${") and val.endswith("}"):
        return os.environ.get(val[2:-1], "")
    return val


# ── Connection ────────────────────────────────────────────────────────────────


class ConnectionConfig(BaseModel):
    type: str  # delta | unity_catalog | duckdb | snowflake | bigquery

    # Delta / ADLS / S3 / GCS
    storage_root: Optional[str] = None
    credentials: Optional[str] = None  # managed_identity | service_principal | env

    # Unity Catalog / Databricks
    host: Optional[str] = None
    token: Optional[str] = None
    catalog: Optional[str] = None

    # DuckDB
    path: Optional[str] = None  # file path or :memory:

    # Snowflake
    account: Optional[str] = None
    warehouse: Optional[str] = None
    database: Optional[str] = None
    role: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

    # BigQuery
    project: Optional[str] = None
    credentials_path: Optional[str] = None

    @model_validator(mode="after")
    def resolve_secrets(self) -> "ConnectionConfig":
        for field in ("token", "password", "storage_root", "host"):
            val = getattr(self, field, None)
            if val:
                setattr(self, field, _resolve_env(val))
        return self


# ── Discovery ─────────────────────────────────────────────────────────────────


class DiscoveryConfig(BaseModel):
    mode: str = "catalog"  # catalog | storage | manifest
    include_schemas: List[str] = Field(default_factory=list)  # empty = all schemas
    exclude_patterns: List[str] = Field(default_factory=lambda: ["_*", "tmp_*", "test_*"])
    timestamp_columns: List[str] = Field(
        default_factory=lambda: [
            "updated_at",
            "created_at",
            "event_timestamp",
            "_lakelogic_loaded_at",
            "_lakelogic_processed_at",
            "last_modified",
        ]
    )
    max_tables: Optional[int] = None  # safety cap for large catalogs


# ── SLO defaults ──────────────────────────────────────────────────────────────


class FreshnessDefaults(BaseModel):
    max_delay_minutes: int = 120
    warn_at_minutes: Optional[int] = None


class SchemaDriftDefaults(BaseModel):
    on_breaking_change: str = "fail"  # fail | warn | ignore
    on_column_added: str = "warn"
    on_column_removed: str = "fail"
    on_type_change: str = "fail"
    on_nullable_change: str = "warn"


class VolumeDefaults(BaseModel):
    anomaly_enabled: bool = True
    lookback_runs: int = 14
    min_ratio: float = 0.5
    max_ratio: float = 2.0


class RetentionDefaults(BaseModel):
    default: Optional[str] = None  # ISO 8601 period e.g. P90D — applied to all tables


class SLODefaults(BaseModel):
    freshness: FreshnessDefaults = Field(default_factory=FreshnessDefaults)
    schema_drift: SchemaDriftDefaults = Field(default_factory=SchemaDriftDefaults)
    volume: VolumeDefaults = Field(default_factory=VolumeDefaults)
    retention: RetentionDefaults = Field(default_factory=RetentionDefaults)


# ── Output ────────────────────────────────────────────────────────────────────


class OutputConfig(BaseModel):
    """Where to write _slo_checks results locally (optional — Observatory push is separate)."""

    slo_checks_table: Optional[str] = None  # Delta path or UC table name
    slo_checks_backend: str = "delta"  # delta | duckdb | sqlite


# ── Observatory ───────────────────────────────────────────────────────────────


class ObservatoryConfig(BaseModel):
    endpoint: Optional[str] = None
    api_key: Optional[str] = None
    emit_on: List[str] = Field(default_factory=lambda: ["success", "failed", "warning"])
    include_schema_snapshot: bool = True

    @model_validator(mode="after")
    def resolve_secrets(self) -> "ObservatoryConfig":
        self.endpoint = _resolve_env(self.endpoint)
        self.api_key = _resolve_env(self.api_key)
        return self


# ── Root config ───────────────────────────────────────────────────────────────


class ScannerConfig(BaseModel):
    connection: ConnectionConfig
    discovery: DiscoveryConfig = Field(default_factory=DiscoveryConfig)
    slo_defaults: SLODefaults = Field(default_factory=SLODefaults)
    output: OutputConfig = Field(default_factory=OutputConfig)
    observatory: ObservatoryConfig = Field(default_factory=ObservatoryConfig)

    # Convenience: catalog / domain name used in SLOReport
    @property
    def domain(self) -> str:
        return self.connection.catalog or self.connection.database or "scanner"

    @classmethod
    def from_yaml(cls, path: str) -> "ScannerConfig":
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f"Scanner config not found: {path}")
        with open(p, "r", encoding="utf-8") as f:
            raw: Dict[str, Any] = yaml.safe_load(f) or {}
        return cls.model_validate(raw)

    @classmethod
    def from_args(
        cls,
        connection_type: str,
        path: Optional[str] = None,
        host: Optional[str] = None,
        catalog: Optional[str] = None,
        token: Optional[str] = None,
    ) -> "ScannerConfig":
        """Build a minimal config from CLI args for quick one-shot scans."""
        conn: Dict[str, Any] = {"type": connection_type}
        if path:
            conn["storage_root"] = path
            conn["path"] = path
        if host:
            conn["host"] = host
        if catalog:
            conn["catalog"] = catalog
        if token:
            conn["token"] = token
        return cls(connection=ConnectionConfig(**conn))
