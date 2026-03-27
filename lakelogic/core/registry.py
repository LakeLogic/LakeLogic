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
from pydantic import BaseModel, ConfigDict, Field, field_validator


class SLOFreshnessConfig(BaseModel):
    max_delay_minutes: int
    check_column: Union[str, List[str]] = "_lakelogic_loaded_at"
    exclude_tables: List[str] = Field(default_factory=list)


class SLORowCountConfig(BaseModel):
    """Per-layer row count thresholds checked against run log entries."""
    min_rows: Optional[int] = None
    max_rows: Optional[int] = None
    check_field: str = "counts_good"  # run log column to check
    exclude_tables: List[str] = Field(default_factory=list)


class SLOQualityConfig(BaseModel):
    min_good_ratio: float = 0.95
    max_quarantine_ratio: float = 0.05


class SLOScheduleConfig(BaseModel):
    expected_completion_utc: str = "06:00"
    pipeline_cron: Optional[str] = None


class RegistrySLO(BaseModel):
    freshness: Dict[str, SLOFreshnessConfig] = Field(default_factory=dict)
    row_count: Dict[str, SLORowCountConfig] = Field(default_factory=dict)
    quality: Optional[SLOQualityConfig] = None
    schedule: Optional[SLOScheduleConfig] = None


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
    catalog: str
    storage_account: Optional[str] = None


def _resolve_placeholders(obj: Any, vars_map: Dict[str, str]) -> Any:
    """
    Recursively walk a dict/list structure and resolve ``{key}`` placeholders
    in any string values using *vars_map*.

    Unresolvable placeholders (no matching key) are left as-is.
    """
    if isinstance(obj, str) and "{" in obj:
        try:
            return obj.format_map(defaultdict(lambda: None, **vars_map)) if "{" in obj else obj
        except (KeyError, ValueError, IndexError):
            # Partial format — replace what we can, leave the rest
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
    lineage: Dict[str, Any] = Field(default_factory=dict)
    quarantine: Dict[str, Any] = Field(default_factory=dict)
    materialization: Dict[str, Any] = Field(default_factory=dict)  # per-layer defaults
    server_defaults: Dict[str, Any] = Field(default_factory=dict)  # per-layer server config
    # Cross-domain lineage: upstream tables not managed by this registry
    external_sources: List[Dict[str, Any]] = Field(default_factory=list)

    # Internal state
    _registry_dir: Optional[Path] = None

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
        sub_map["domain"] = registry.domain
        sub_map["system"] = registry.system
        sub_map["bronze_layer"] = registry.bronze_layer
        sub_map["silver_layer"] = registry.silver_layer
        sub_map["gold_layer"] = registry.gold_layer
        if sub_map:
            for field, val in registry.storage.model_dump().items():
                if isinstance(val, str) and "{" in val:
                    try:
                        new_val = val.format(**sub_map)
                        setattr(registry.storage, field, new_val)
                    except KeyError as e:
                        logger.warning(f"Could not resolve template var {e} in storage.{field}: {val}")

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
