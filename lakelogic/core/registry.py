"""
Registry Management for Lakehouse Domains.

Parses and validates domain-level `_registry.yaml` files.
Provides typed access to storage paths, SLOs, and active contracts.
Can also scaffold a registry from a directory of loose contracts.
"""

from __future__ import annotations

import os
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from loguru import logger
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class SLOFreshnessConfig(BaseModel):
    max_delay_minutes: int
    check_column: Union[str, List[str]] = "_lakelogic_loaded_at"
    max_source_delay_minutes: Optional[int] = None  # source-time freshness
    exclude_tables: List[str] = Field(default_factory=list)


class SLORowCountAnomalyConfig(BaseModel):
    """Anomaly detection against historical run log baselines."""

    enabled: bool = False
    lookback_runs: int = 14
    min_ratio: float = 0.5
    max_ratio: float = 2.0
    method: str = "median"  # "median" | "rolling_average"
    min_runs_before_enforcement: int = 5


class SLORowCountConfig(BaseModel):
    """Per-layer row count thresholds checked against run log entries."""

    min_rows: Optional[int] = None
    max_rows: Optional[int] = None
    check_field: str = "counts_good"  # run log column to check
    warn_only: bool = False  # true = log warning instead of failing
    anomaly: Optional[SLORowCountAnomalyConfig] = None
    exclude_tables: List[str] = Field(default_factory=list)


class SLOQualitySeverityThreshold(BaseModel):
    min_good_ratio: float = 0.95


class SLOQualityConfig(BaseModel):
    min_good_ratio: float = 0.95
    max_quarantine_ratio: float = 0.05
    by_severity: Dict[str, SLOQualitySeverityThreshold] = Field(default_factory=dict)


class SLOScheduleConfig(BaseModel):
    expected_completion_utc: str = "06:00"
    expected_start_utc: Optional[str] = None
    expected_duration_minutes: Optional[int] = None
    warn_if_duration_exceeds_minutes: Optional[int] = None
    timezone: str = "UTC"
    environments: List[str] = Field(default_factory=list)  # empty = all environments
    pipeline_cron: Optional[str] = None


class SLOAlertingConfig(BaseModel):
    """SLO alerting — emits events through the existing notifications system."""

    emit_events: bool = True
    min_severity: str = "medium"  # only emit events for this severity and above
    suppress_duplicate_alerts_minutes: int = 30
    notify_on_recovery: bool = True


class RegistrySLO(BaseModel):
    freshness: Dict[str, SLOFreshnessConfig] = Field(default_factory=dict)
    row_count: Dict[str, SLORowCountConfig] = Field(default_factory=dict)
    quality: Optional[SLOQualityConfig] = None
    schedule: Optional[SLOScheduleConfig] = None
    alerting: Optional[SLOAlertingConfig] = None


class RegistryStorage(BaseModel):
    # Unity Catalog table config — pipeline derives targets from info.table_name
    domain_catalog: Optional[str] = None  # e.g. "`catalog`.domain"
    quarantine_root: Optional[str] = None  # e.g. "`catalog`.quarantine"
    run_log_table: Optional[str] = None  # e.g. "`catalog`.domain._run_logs"
    external_location_root: Optional[str] = None  # e.g. "abfss://domain@acct.dfs.core.windows.net"
    # Databricks Volume / operational roots
    landing_root: Optional[str] = None
    contract_root: Optional[str] = None
    bronze_root: Optional[str] = None
    silver_root: Optional[str] = None
    gold_root: Optional[str] = None
    log_root: Optional[str] = None
    # Cloud storage paths (e.g. abfss://, s3://, gs://)
    landing_path: Optional[str] = None
    contract_path: Optional[str] = None
    bronze_path: Optional[str] = None
    silver_path: Optional[str] = None
    gold_path: Optional[str] = None
    log_path: Optional[str] = None
    quarantine_path: Optional[str] = None


class RegistryContract(BaseModel):
    model_config = ConfigDict(extra="allow")

    layer: str
    entity: str
    path: str
    depends_on: List[str] = Field(default_factory=list)  # entity names within the same layer
    contract_version: str = "1.0"
    enabled: bool = True
    # Injected fields after resolution
    resolved_path: Optional[str] = None
    contract_dict: Optional[Dict[str, Any]] = None


class CloudReporting(BaseModel):
    enabled: bool = False
    report_url: Optional[str] = None
    api_key: Optional[str] = None

    @field_validator("api_key", "report_url", mode="before")
    def resolve_env_vars(cls, v: Optional[str]) -> Optional[str]:
        if not v:
            return v
        if v.startswith("${") and v.endswith("}"):
            env_var = v[2:-1]
            return os.environ.get(env_var, "")
        return v


class EnvironmentConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    catalog: str
    storage_account: Optional[str] = None
    region: Optional[str] = None


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """
    Recursively merge *override* into *base*.  Override values win on conflicts.
    Lists are replaced (not concatenated) — the override list takes precedence.

    Used to layer ``_domain.yaml`` defaults underneath ``_system.yaml`` values
    so that the system always wins while inheriting unset domain-level config.
    """
    merged = dict(base)
    for key, val in override.items():
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(val, dict)
        ):
            merged[key] = _deep_merge(merged[key], val)
        else:
            merged[key] = val
    return merged


def _resolve_placeholders(obj: Any, vars_map: Dict[str, str]) -> Any:
    """
    Recursively walk a dict/list structure and resolve ``{key}`` placeholders
    in any string values using *vars_map*.

    Unresolvable placeholders (no matching key) are left as-is.
    """
    if isinstance(obj, str) and "{" in obj:
        # Partial format — replace only known keys, leave unknown {placeholders} as-is
        for k, v in vars_map.items():
            obj = obj.replace(f"{{{k}}}", str(v))
        return obj
    elif isinstance(obj, dict):
        return {k: _resolve_placeholders(v, vars_map) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_resolve_placeholders(item, vars_map) for item in obj]
    return obj


class DomainRegistry(BaseModel):
    """
    Typed representation of a Data Mesh Domain Registry (e.g., _registry.yaml).
    """

    domain: str
    system: str
    storage_mode: str = "uc"  # "uc" (Unity Catalog) | "direct" (ADLS / cloud paths)
    # Layer aliases — override to rename medallion layers (e.g. bronze_layer: "raw")
    bronze_layer: str = "bronze"
    silver_layer: str = "silver"
    gold_layer: str = "gold"
    ownership: Dict[str, Any] = Field(default_factory=dict)
    slo: RegistrySLO = Field(default_factory=RegistrySLO)
    storage: RegistryStorage = Field(default_factory=RegistryStorage)
    contracts: List[RegistryContract] = Field(default_factory=list)
    cloud: CloudReporting = Field(default_factory=CloudReporting)
    environments: Dict[str, EnvironmentConfig] = Field(default_factory=dict)
    # System-level defaults — inherited by contracts that don't define their own
    compliance: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    lineage: Dict[str, Any] = Field(default_factory=dict)
    quarantine: Dict[str, Any] = Field(default_factory=dict)
    materialization: Dict[str, Any] = Field(default_factory=dict)  # per-layer defaults
    notifications_enabled: bool = True  # global switch to disable all system/domain/contract notifications
    notifications: List[Dict[str, Any]] = Field(default_factory=list)  # system-wide notification channels
    server: Dict[str, Any] = Field(default_factory=dict)  # per-layer server config
    cost: Dict[str, Any] = Field(default_factory=dict)  # cost observability config
    # Cross-domain lineage: upstream tables not managed by this registry
    external_sources: List[Dict[str, Any]] = Field(default_factory=list)

    # Internal state
    _registry_dir: Optional[Path] = None

    @model_validator(mode="after")
    def validate_unique_entities(self) -> "DomainRegistry":
        entity_layers = {}
        for c in self.contracts:
            if c.entity in entity_layers:
                raise ValueError(
                    f"Duplicate contract entity found: '{c.entity}'. "
                    f"Entities must be globally unique across the contract list. "
                    f"Found in layer '{entity_layers[c.entity]}' and '{c.layer}'."
                )
            entity_layers[c.entity] = c.layer
        return self

    @classmethod
    def from_yaml(cls, path: str, environment: str = "dev", storage_mode: str = "uc") -> "DomainRegistry":
        """
        Load a registry from a YAML file, resolving environment tokens and contract paths.

        Parameters
        ----------
        path : str
            Path to the registry YAML file.
        environment : str
            Environment name to resolve (e.g. "dev", "staging", "prod").
        storage_mode : str
            ``"uc"``  — resolve paths using Unity Catalog Volumes / table names
            (Databricks pipelines).  ``"direct"`` — resolve paths using cloud
            storage URIs such as ``abfss://`` (Azure Functions, non-Spark runtimes).
        """
        yaml_path = Path(path)
        if not yaml_path.exists():
            raise FileNotFoundError(f"Registry not found: {yaml_path}")

        with open(yaml_path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)

        # ── Domain-level inheritance ──────────────────────────────────────
        # Walk up from the _system.yaml directory to discover a sibling
        # _domain.yaml in the parent.  Domain-level keys provide defaults;
        # system-level keys override (child wins).
        #
        # Inheritance hierarchy:
        #   _domain.yaml  (domain defaults)  ← base
        #   _system.yaml  (system overrides) ← wins on conflict
        #
        # Inheritable keys: slo, ownership, notifications, quarantine,
        #                   lineage, materialization, server
        _DOMAIN_INHERITABLE_KEYS = [
            "slo", "ownership", "notifications", "quarantine",
            "compliance", "lineage", "materialization", "server",
            "cost",
        ]
        # Scalar keys: inherit only if the system doesn't define them
        _DOMAIN_SCALAR_KEYS = [
            "domain", "bronze_layer", "silver_layer", "gold_layer",
            "notifications_enabled",
        ]
        domain_yaml_path = yaml_path.parent.parent / "_domain.yaml"
        if domain_yaml_path.exists():
            with open(domain_yaml_path, "r", encoding="utf-8") as df:
                domain_raw = yaml.safe_load(df) or {}
            logger.info(f"Domain config inherited from {domain_yaml_path}")
            for key in _DOMAIN_INHERITABLE_KEYS:
                if key not in domain_raw:
                    continue
                domain_val = domain_raw[key]
                system_val = raw.get(key)
                if system_val is None:
                    # System didn't define this key — inherit wholesale
                    merged_val = domain_val
                elif isinstance(system_val, dict) and isinstance(domain_val, dict):
                    # Deep merge: domain provides base, system overrides
                    merged_val = _deep_merge(domain_val, system_val)
                elif isinstance(system_val, list) and isinstance(domain_val, list):
                    # Lists: concatenate domain + system (system appends)
                    merged_val = domain_val + system_val
                else:
                    # system value wins outright (scalar override)
                    continue

                # Validate: trial-parse the merged value through Pydantic
                # to ensure the domain schema shape is compatible.
                _trial = dict(raw)
                _trial[key] = merged_val
                try:
                    cls.model_validate(_trial)
                    raw[key] = merged_val
                    logger.trace(f"  Inherited domain key '{key}'")
                except Exception as exc:
                    logger.warning(
                        f"  Skipped domain key '{key}': incompatible schema "
                        f"({exc.__class__.__name__}). System value retained."
                    )

            # Scalar keys: inherit if system doesn't define them,
            # warn on mismatches when both define the same key.
            for key in _DOMAIN_SCALAR_KEYS:
                domain_val = domain_raw.get(key)
                if domain_val is None:
                    continue
                system_val = raw.get(key)
                if system_val is None:
                    # System didn't define it — inherit from domain
                    raw[key] = domain_val
                    logger.trace(f"  Inherited domain scalar '{key}' = {domain_val}")
                elif system_val != domain_val:
                    # Both defined but different — flag the mismatch
                    logger.warning(
                        f"  ⚠ Config mismatch: _system.yaml has {key}='{system_val}' "
                        f"but _domain.yaml has {key}='{domain_val}'. "
                        f"Domain value takes precedence for consistency."
                    )
                    raw[key] = domain_val

            # ── Cost currency mismatch check ──────────────────────────────
            # The domain's cost.currency is the authoritative reporting
            # currency for budget enforcement and Observatory roll-ups.
            # If the system defines a different currency, warn and enforce
            # the domain value to prevent mixed-currency aggregations.
            domain_cost = domain_raw.get("cost") or {}
            system_cost = raw.get("cost") or {}
            domain_currency = domain_cost.get("currency")
            system_currency = system_cost.get("currency")
            if domain_currency and system_currency and domain_currency != system_currency:
                logger.warning(
                    f"  ⚠ Cost currency mismatch: _system.yaml has cost.currency='{system_currency}' "
                    f"but _domain.yaml has cost.currency='{domain_currency}'. "
                    f"Domain currency '{domain_currency}' will be used for roll-ups and budget enforcement. "
                    f"System rates will still be applied, but reported in {domain_currency}."
                )
                if "cost" in raw and isinstance(raw["cost"], dict):
                    raw["cost"]["currency"] = domain_currency

        # Parse into typed model
        registry = cls.model_validate(raw)
        registry.storage_mode = storage_mode
        registry._registry_dir = yaml_path.parent

        # 1. Resolve environment bindings
        env_config = registry.environments.get(environment)
        if not env_config:
            logger.warning(f"Environment '{environment}' not defined in registry; skipping dynamic substitutions.")
            sub_map = {}
        else:
            sub_map = env_config.model_dump(exclude_none=True)

        # 2. Inject environment tokens + registry-level vars into storage paths
        sub_map.setdefault("domain", registry.domain)
        sub_map.setdefault("system", registry.system)
        sub_map.setdefault("bronze_layer", registry.bronze_layer)
        sub_map.setdefault("silver_layer", registry.silver_layer)
        sub_map.setdefault("gold_layer", registry.gold_layer)

        # Self-resolve: allow env values to reference each other,
        # e.g. storage_root: "abfss://nondelta@{storage_account}.dfs..."
        #      data_root:    "abfss://{domain}@{storage_account}.dfs..."
        for _pass in range(2):  # two passes handles chained refs
            sub_map = {
                k: v.format_map(defaultdict(lambda: None, **sub_map)) if isinstance(v, str) and "{" in v else v
                for k, v in sub_map.items()
            }
        if sub_map:
            for field, val in registry.storage.model_dump().items():
                if isinstance(val, str) and "{" in val:
                    try:
                        new_val = val.format(**sub_map)
                        setattr(registry.storage, field, new_val)
                    except KeyError as e:
                        logger.warning(f"Could not resolve template var {e} in storage.{field}: {val}")

        # 2b. Resolve template vars in system-level quarantine, metadata, lineage
        #     These may reference resolved storage fields (e.g. {quarantine_path}).
        _full_vars = dict(sub_map)
        for field, val in registry.storage.model_dump().items():
            if val is not None:
                _full_vars[field] = val
        for section_name in ("quarantine", "metadata", "lineage", "compliance"):
            section = getattr(registry, section_name, None)
            if section and isinstance(section, dict):
                setattr(registry, section_name, _resolve_placeholders(section, _full_vars))
        # Resolve notifications list (URLs may contain placeholders)
        if registry.notifications:
            registry.notifications = _resolve_placeholders(registry.notifications, _full_vars)

        # 3. Resolve actual DataContracts and absolute paths
        for c in registry.contracts:
            if not c.enabled:
                continue

            # Resolve {domain}/{system}/{*_layer} placeholders in the contract path
            resolved_contract_path = c.path
            if "{" in resolved_contract_path:
                resolved_contract_path = resolved_contract_path.replace("{domain}", registry.domain)
                resolved_contract_path = resolved_contract_path.replace("{system}", registry.system)
                resolved_contract_path = resolved_contract_path.replace("{bronze_layer}", registry.bronze_layer)
                resolved_contract_path = resolved_contract_path.replace("{silver_layer}", registry.silver_layer)
                resolved_contract_path = resolved_contract_path.replace("{gold_layer}", registry.gold_layer)

            # Resolve the absolute path relative to the registry file
            c_path = Path(resolved_contract_path)
            if not c_path.is_absolute() and registry._registry_dir:
                c_path = registry._registry_dir / c_path

            if not c_path.exists():
                logger.warning(f"Contract file not found: {c_path}")
                c.enabled = False
                continue

            c.resolved_path = str(c_path)

            # Load the actual contract content
            with open(c_path, "r", encoding="utf-8") as rf:
                c_dict = yaml.safe_load(rf)
                c_dict["__file__"] = str(c_path)  # Inject original path

                # Resolve storage placeholders ({landing_root}, {bronze_root}, etc.)
                # in all string values within the contract dict.
                # Prefer _path (cloud) over _root (Databricks Volume) when available.
                storage_vars = {}
                for field, val in registry.storage.model_dump().items():
                    if val is not None:
                        storage_vars[field] = val
                # Include registry-level vars so contract content
                # (e.g. info.table_name: "{silver_layer}_{system}_sessions")
                # gets fully resolved.
                storage_vars["domain"] = registry.domain
                storage_vars["system"] = registry.system
                storage_vars["bronze_layer"] = registry.bronze_layer
                storage_vars["silver_layer"] = registry.silver_layer
                storage_vars["gold_layer"] = registry.gold_layer
                # Inject lineage column names so contracts can reference them
                # as {timestamp_column_name}, {source_column_name}, etc.
                if registry.lineage:
                    for lk, lv in registry.lineage.items():
                        if isinstance(lv, str):
                            storage_vars.setdefault(lk, lv)
                storage_vars.update(sub_map)  # env vars (catalog, storage_account, etc.)
                # In "direct" mode (Azure Functions / non-Spark), override UC
                # _root variables with their ADLS _path equivalents so that
                # contracts resolve to cloud storage URIs.
                # In "uc" mode (Databricks), keep _root values as-is.
                if storage_mode == "direct":
                    _root_to_path = {
                        "landing_root": "landing_path",
                        "contract_root": "contract_path",
                        "bronze_root": "bronze_path",
                        "silver_root": "silver_path",
                        "gold_root": "gold_path",
                        "log_root": "log_path",
                    }
                    for root_key, path_key in _root_to_path.items():
                        path_val = storage_vars.get(path_key)
                        if path_val:
                            storage_vars[root_key] = path_val

                c_dict = _resolve_placeholders(c_dict, storage_vars)

                # Inject system-level compliance defaults and validate residency
                if registry.compliance:
                    if not c_dict.get("compliance"):
                        c_dict["compliance"] = dict(registry.compliance)
                    else:
                        c_dict["compliance"] = _deep_merge(dict(registry.compliance), c_dict["compliance"])
                
                # Check data residency against environment region
                c_compliance = c_dict.get("compliance", {})
                c_residency = c_compliance.get("data_residency")
                if c_residency and env_config and getattr(env_config, "region", None):
                    if c_residency.upper() != env_config.region.upper():
                        logger.warning(
                            f"⚠ Compliance Violation [{c.entity}]: Requires '{c_residency.upper()}' data residency, "
                            f"but target environment '{environment}' is '{env_config.region.upper()}'."
                        )

                # Inject system-level metadata defaults (e.g. run_log_dir)
                if registry.metadata:
                    if not c_dict.get("metadata"):
                        c_dict["metadata"] = dict(registry.metadata)
                    else:
                        for k, v in registry.metadata.items():
                            c_dict["metadata"].setdefault(k, v)
                    # Resolve any remaining placeholders in metadata values
                    c_dict["metadata"] = _resolve_placeholders(c_dict["metadata"], storage_vars)

                # Inject system-level materialization defaults
                if registry.materialization and c.layer in registry.materialization:
                    if not c_dict.get("materialization"):
                        c_dict["materialization"] = registry.materialization[c.layer]
                    else:
                        for k, v in registry.materialization[c.layer].items():
                            c_dict["materialization"].setdefault(k, v)

                # Inject arbitrary extra fields from _system.yaml's contract array
                # (e.g. pipeline configs like schedule/frequency)
                extras = c.model_extra or {}
                for k, v in extras.items():
                    if k not in c_dict:
                        c_dict[k] = _resolve_placeholders(v, storage_vars)
                    elif isinstance(v, dict) and isinstance(c_dict[k], dict):
                        for sub_k, sub_v in v.items():
                            c_dict[k].setdefault(sub_k, _resolve_placeholders(sub_v, storage_vars))

                c.contract_dict = c_dict

        return registry

    def get_active_contracts(self, layer: Optional[str] = None) -> List[RegistryContract]:
        """
        Return the list of enabled contracts, optionally filtered by medallion layer.
        """
        active = [c for c in self.contracts if c.enabled and c.contract_dict]
        if layer:
            active = [c for c in active if c.layer == layer]
        return active
