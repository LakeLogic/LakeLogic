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
from typing import Any, Dict, List, Optional

import yaml
from pydantic import BaseModel, Field, field_validator
from loguru import logger

from lakelogic.core.models import DataContract


class SLOFreshnessConfig(BaseModel):
    max_delay_minutes: int
    check_column: str = "_lakelogic_loaded_at"


class SLOQualityConfig(BaseModel):
    min_good_ratio: float = 0.95
    max_quarantine_ratio: float = 0.05


class SLOScheduleConfig(BaseModel):
    expected_completion_utc: str = "06:00"
    pipeline_cron: Optional[str] = None


class RegistrySLO(BaseModel):
    freshness: Dict[str, SLOFreshnessConfig] = Field(default_factory=dict)
    quality: Optional[SLOQualityConfig] = None
    schedule: Optional[SLOScheduleConfig] = None


class RegistryStorage(BaseModel):
    # Databricks Volume / Unity Catalog roots
    landing_root: Optional[str] = None
    contract_root: Optional[str] = None
    bronze_root: Optional[str] = None
    silver_root: Optional[str] = None
    gold_root: Optional[str] = None
    quarantine_root: Optional[str] = None
    log_root: Optional[str] = None
    # Cloud storage paths (e.g. abfss://, s3://, gs://)
    landing_path: Optional[str] = None
    contract_path: Optional[str] = None
    bronze_path: Optional[str] = None
    silver_path: Optional[str] = None
    gold_path: Optional[str] = None
    log_path: Optional[str] = None


class RegistryContract(BaseModel):
    layer: str
    entity: str
    path: str
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
    ownership: Dict[str, Any] = Field(default_factory=dict)
    slo: RegistrySLO = Field(default_factory=RegistrySLO)
    storage: RegistryStorage = Field(default_factory=RegistryStorage)
    contracts: List[RegistryContract] = Field(default_factory=list)
    cloud: CloudReporting = Field(default_factory=CloudReporting)
    environments: Dict[str, EnvironmentConfig] = Field(default_factory=dict)
    
    # Internal state
    _registry_dir: Optional[Path] = None

    @classmethod
    def from_yaml(cls, path: str, environment: str = "dev") -> "DomainRegistry":
        """
        Load a registry from a YAML file, resolving environment tokens and contract paths.
        """
        yaml_path = Path(path)
        if not yaml_path.exists():
            raise FileNotFoundError(f"Registry not found: {yaml_path}")
            
        with open(yaml_path, "r", encoding="utf-8") as f:
            raw = yaml.safe_load(f)
            
        # Parse into typed model
        registry = cls.model_validate(raw)
        registry._registry_dir = yaml_path.parent
        
        # 1. Resolve environment bindings
        env_config = registry.environments.get(environment)
        if not env_config:
            logger.warning(f"Environment '{environment}' not defined in registry; skipping dynamic substitutions.")
            sub_map = {}
        else:
            sub_map = env_config.model_dump(exclude_none=True)
            
        # 2. Inject environment tokens into storage paths
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
                
            # Resolve the absolute path relative to the registry file
            c_path = copy_path = Path(c.path)
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
                c_dict["__file__"] = str(c_path) # Inject original path

                # Resolve storage placeholders ({landing_root}, {bronze_root}, etc.)
                # in all string values within the contract dict.
                # Prefer _path (cloud) over _root (Databricks Volume) when available.
                storage_vars = {}
                for field, val in registry.storage.model_dump().items():
                    if val is not None:
                        storage_vars[field] = val
                # Also map _root keys to _path values when _path is available
                # e.g. {landing_root} -> landing_path value (for non-Databricks)
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
                        # When _path (cloud) exists, use it as the value for _root placeholders
                        storage_vars[root_key] = path_val

                c_dict = _resolve_placeholders(c_dict, storage_vars)
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
