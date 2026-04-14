"""
Centralized path resolution utilities for LakeLogic.

This module is the single source of truth for:
- URI detection (cloud vs local paths)
- Table naming conventions ({layer}_{system}_{entity})
- Engine-specific SQL table references (Spark, DuckDB, Polars)
- Materialization target path resolution
- Run log and quarantine path resolution
- Azure storage option enrichment for Polars' Rust object_store

All consumer modules (slo.py, processor.py, materialization.py, runner.py,
run_log.py) should import from this module instead of reimplementing
path logic inline.
"""

from __future__ import annotations

import os
import re
from typing import Any, Dict, Optional


# ── Compiled patterns ─────────────────────────────────────────────────────

# Matches standard URI schemes: abfss://, s3://, gs://, file://, etc.
_URI_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]+://")
# Matches Windows-mangled URIs where pathlib replaced :// with :\
_URI_WINDOWS_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9+.-]+:\\\\")


# ═══════════════════════════════════════════════════════════════════════════
# URI Detection
# ═══════════════════════════════════════════════════════════════════════════

def is_uri_path(path: str) -> bool:
    r"""Check if a path is a cloud URI (abfss://, s3://, gs://, file://, etc.).

    Also handles Windows-mangled URIs where ``pathlib.Path`` has replaced
    ``://`` with ``:\`` (e.g. ``abfss:\container@account``).

    Examples
    --------
    >>> is_uri_path("abfss://marketing@sa.dfs.core.windows.net/bronze")
    True
    >>> is_uri_path("s3://bucket/prefix")
    True
    >>> is_uri_path("./lakehouse/marketing")
    False
    >>> is_uri_path("C:\\Users\\data\\lake")
    False
    """
    p = str(path)
    return bool(_URI_PATTERN.match(p)) or bool(_URI_WINDOWS_PATTERN.match(p))


# ═══════════════════════════════════════════════════════════════════════════
# Table Name Convention
# ═══════════════════════════════════════════════════════════════════════════

def make_table_name(layer: str, system: str, entity: str) -> str:
    """Build the canonical LakeLogic table name.

    Convention: ``{layer}_{system}_{entity}``

    Examples
    --------
    >>> make_table_name("bronze", "google_analytics", "events")
    'bronze_google_analytics_events'
    """
    return f"{layer}_{system}_{entity}"


# ═══════════════════════════════════════════════════════════════════════════
# Engine-Specific SQL Table References
# ═══════════════════════════════════════════════════════════════════════════

def to_sql_table_ref(path_or_table: str, engine: str) -> str:
    """Convert a path/URI or catalog table name to an engine-specific SQL
    ``FROM`` clause reference.

    Parameters
    ----------
    path_or_table : str
        Either a cloud URI (e.g. ``abfss://...``) or a catalog table name
        (e.g. ``catalog.schema.table``).
    engine : str
        One of ``"spark"``, ``"duckdb"``, ``"polars"``.

    Returns
    -------
    str
        Engine-compatible SQL table reference.

    Examples
    --------
    >>> to_sql_table_ref("abfss://container@acct.dfs.core.windows.net/tbl", "spark")
    "delta.`abfss://container@acct.dfs.core.windows.net/tbl`"
    >>> to_sql_table_ref("abfss://container@acct.dfs.core.windows.net/tbl", "duckdb")
    "delta_scan('abfss://container@acct.dfs.core.windows.net/tbl')"
    >>> to_sql_table_ref("catalog.schema.table", "spark")
    'catalog.schema.table'
    >>> to_sql_table_ref("catalog.schema.table", "duckdb")
    'catalog.schema.table'
    """
    clean = path_or_table.replace("`", "")

    if is_uri_path(clean):
        if engine == "spark":
            return f"delta.`{clean}`"
        elif engine == "duckdb":
            return f"delta_scan('{clean}')"
        else:
            # Polars doesn't use SQL references — return raw path
            return clean
    else:
        # Catalog table name — usable directly by all engines
        return clean


# ═══════════════════════════════════════════════════════════════════════════
# Materialization Target Resolution
# ═══════════════════════════════════════════════════════════════════════════

def resolve_materialization_path(
    contract: Any = None,
    registry_storage: Any = None,
    layer: str = "",
    system: str = "",
    entity: str = "",
    override_path: Optional[str] = None,
) -> Optional[str]:
    """Resolve the physical storage path for a materialized table.

    This is the single source of truth for where a contract's data lives.
    The resolution priority mirrors ``materialization.py._resolve_target()``:

    1. ``override_path`` (explicit caller override)
    2. ``contract.materialization.path`` or ``target_path``
    3. ``contract.materialization.table`` (may be a UC name or URI)
    4. ``contract.effective_server().path`` (environment-aware)
    5. ``storage.external_location_root/{layer}_{system}_{entity}``
    6. ``storage.{layer}_path/{layer}_{system}_{entity}``
    7. ``storage.{layer}_root/{entity}``

    Parameters
    ----------
    contract : DataContract or dict, optional
        The contract object or raw contract dict.
    registry_storage : RegistryStorage, optional
        The registry's ``storage`` block.
    layer, system, entity : str
        Medallion layer, system name, and entity name.
    override_path : str, optional
        Explicit override that takes highest priority.

    Returns
    -------
    str or None
        Resolved path, or None if no path could be determined.
    """
    # 1. Explicit override
    if override_path:
        return str(override_path)

    # Extract materialization config from contract
    mat_config: dict = {}
    if contract is not None:
        if isinstance(contract, dict):
            mat_config = contract.get("materialization", {}) or {}
        else:
            # contract_dict attribute (from RegistryContract)
            cdict = getattr(contract, "contract_dict", None)
            if cdict and isinstance(cdict, dict):
                mat_config = cdict.get("materialization", {}) or {}
            # Also check direct materialization attribute
            mat_obj = getattr(contract, "materialization", None)
            if mat_obj and not mat_config:
                mat_config = {
                    "path": getattr(mat_obj, "path", None),
                    "target_path": getattr(mat_obj, "target_path", None),
                    "table": getattr(mat_obj, "table", None),
                }

    # 2. Explicit materialization path/target_path
    result = mat_config.get("path") or mat_config.get("target_path")
    if result:
        return str(result)

    # 3. Materialization table (may be URI or UC catalog name)
    table_val = mat_config.get("table")
    if table_val:
        return str(table_val)

    # 4. effective_server().path (environment-aware)
    contract_obj = contract
    if hasattr(contract, "contract"):
        contract_obj = contract.contract
    if contract_obj and hasattr(contract_obj, "effective_server"):
        try:
            eff_server = contract_obj.effective_server()
            if eff_server and getattr(eff_server, "path", None):
                return str(eff_server.path)
        except Exception:
            pass

    # 5–7. Registry storage fallbacks
    if registry_storage:
        table_name = make_table_name(layer, system, entity) if (layer and system and entity) else entity

        # 5. external_location_root
        ext_root = getattr(registry_storage, "external_location_root", None)
        if ext_root:
            return f"{ext_root}/{table_name}"

        # 6. Layer-specific path (e.g. storage.bronze_path)
        layer_path = getattr(registry_storage, f"{layer}_path", None)
        if layer_path:
            return f"{layer_path}/{table_name}"

        # 7. Layer-specific root (e.g. storage.bronze_root)
        layer_root = getattr(registry_storage, f"{layer}_root", None)
        if layer_root:
            return f"{layer_root}/{entity}"

    return None


# ═══════════════════════════════════════════════════════════════════════════
# Run Log Table Reference
# ═══════════════════════════════════════════════════════════════════════════

def resolve_run_log_ref(run_log_table: str, engine: str) -> str:
    """Resolve a run_log_table value to an engine-compatible SQL reference.

    Convenience wrapper around :func:`to_sql_table_ref`.

    Parameters
    ----------
    run_log_table : str
        The ``metadata.run_log_table`` value (may be a cloud URI or
        catalog table name).
    engine : str
        One of ``"spark"``, ``"duckdb"``, ``"polars"``.

    Returns
    -------
    str
        Engine-compatible SQL table reference.
    """
    clean = run_log_table.replace("`", "")
    return to_sql_table_ref(clean, engine)


# ═══════════════════════════════════════════════════════════════════════════
# Quarantine Path Resolution
# ═══════════════════════════════════════════════════════════════════════════

def resolve_quarantine_path(
    contract: Any = None,
    registry_storage: Any = None,
    system: str = "",
    entity: str = "",
    layer: str = "",
) -> Optional[str]:
    """Resolve the quarantine target path from contract or registry defaults.

    Priority:
    1. Contract-level ``quarantine.target``
    2. Registry ``storage.quarantine_path/{layer}_{system}_{entity}``
    3. Registry ``storage.quarantine_root/{layer}_{system}_{entity}``
    4. Registry ``storage.external_location_root/_quarantine/{table_name}``

    Parameters
    ----------
    contract : DataContract or dict, optional
        The contract object or raw contract dict.
    registry_storage : RegistryStorage, optional
        The registry's ``storage`` block.
    system, entity, layer : str
        System name, entity name, and medallion layer.

    Returns
    -------
    str or None
        Resolved quarantine path, or None if not configured.
    """
    # 1. Contract-level quarantine target
    if contract is not None:
        if isinstance(contract, dict):
            q = contract.get("quarantine", {}) or {}
            if q.get("target"):
                return str(q["target"])
        else:
            q_obj = getattr(contract, "quarantine", None)
            if q_obj and getattr(q_obj, "target", None):
                return str(q_obj.target)

    table_name = make_table_name(layer, system, entity) if (layer and system and entity) else entity

    if registry_storage:
        # 2. quarantine_path
        q_path = getattr(registry_storage, "quarantine_path", None)
        if q_path:
            return f"{q_path}/{table_name}"

        # 3. quarantine_root
        q_root = getattr(registry_storage, "quarantine_root", None)
        if q_root:
            return f"{q_root}/{table_name}"

        # 4. external_location_root/_quarantine
        ext_root = getattr(registry_storage, "external_location_root", None)
        if ext_root:
            return f"{ext_root}/_quarantine/{table_name}"

    return None


# ═══════════════════════════════════════════════════════════════════════════
# Azure Storage Option Enrichment
# ═══════════════════════════════════════════════════════════════════════════

def enrich_azure_storage_options(storage_opts: Dict[str, str]) -> Dict[str, str]:
    """Add ``azure_storage_*`` prefixed keys required by Polars' native Rust
    ``object_store`` backend.

    The ``deltalake`` Python library accepts generic keys like ``account_key``,
    but Polars' internal Rust reader strictly requires the ``azure_storage_``
    prefix. This function bridges the gap by duplicating the keys.

    Also injects ``azure_storage_use_cli`` from the ``AZURE_USE_AZURE_CLI``
    environment variable.

    Parameters
    ----------
    storage_opts : dict
        Storage options dict (typically from ``cloud_credentials.resolve_storage_options``).

    Returns
    -------
    dict
        The same dict, mutated in-place with additional keys.
    """
    if "account_key" in storage_opts:
        storage_opts["azure_storage_account_key"] = storage_opts["account_key"]
    if "account_name" in storage_opts:
        storage_opts["azure_storage_account_name"] = storage_opts["account_name"]
    if "bearer_token" in storage_opts:
        storage_opts["azure_storage_bearer_token"] = storage_opts["bearer_token"]
    storage_opts["azure_storage_use_cli"] = os.environ.get("AZURE_USE_AZURE_CLI", "false")
    return storage_opts
