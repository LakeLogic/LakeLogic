"""
Declarative Data Mesh Pipeline engine.

Takes a DomainRegistry and executes data contracts in sequence
(bronze -> silver -> gold), handling dependencies, retries, GDPR erasures,
and HIPAA masking automatically.
"""

from __future__ import annotations

import copy
import json
import os
import time
import uuid
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from loguru import logger

from lakelogic.core.models import DataContract
from lakelogic.core.observer import RemoteObserver
from lakelogic.core.processor import DataProcessor
from lakelogic.core.registry import DomainRegistry, RegistryContract
from lakelogic.pipeline.impact import (
    build_restatement_impact,
    format_restatement_impact,
    is_restatement_run,
    topological_order,
)


def _friendly_validation_error(entity: str, exc: Exception) -> str:
    """Convert Pydantic ValidationError into a concise, actionable message."""
    from pydantic import ValidationError

    if not isinstance(exc, ValidationError):
        return f"DDL failed for {entity}: {exc}"

    parts = []
    for err in exc.errors():
        field = " → ".join(str(loc) for loc in err.get("loc", []))
        msg = err.get("msg", "unknown error")
        parts.append(f"  • {field}: {msg}")

    hint = "\n".join(parts)
    return (
        f"Contract '{entity}' has validation errors:\n"
        f"{hint}\n"
        f"  Fix: check your contract YAML and add any missing required fields."
    )


class PipelineRunSummary:
    """Standardized summary of a pipeline execution."""

    def __init__(self, run_id: str, environment: str, dry_run: bool):
        self.run_id = run_id
        self.environment = environment
        self.dry_run = dry_run
        self.results: List[Dict[str, Any]] = []
        # Advisory restatement impact report, populated only when the run
        # restated data (a reprocess). ``None`` on the common path.
        self.restatement_impact: Optional[Dict[str, Any]] = None

    def append(
        self,
        contract: str,
        layer: str,
        status: str,
        rows: Any = "-",
        error: str = "",
        rows_raw: Any = None,
        rows_good: Any = None,
        rows_bad: Any = None,
        table_name: str = "",
    ):
        # Remove any existing entry for this contract+layer (e.g., from failed earlier retry attempts)
        self.results = [r for r in self.results if not (r.get("contract") == contract and r.get("layer") == layer)]

        self.results.append(
            {
                "contract": contract,
                "layer": layer,
                "table_name": table_name,
                "status": status,
                "rows": rows,
                "rows_raw": rows_raw,
                "rows_good": rows_good,
                "rows_bad": rows_bad,
                "error": error,
            }
        )

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "run_id": self.run_id,
            "environment": self.environment,
            "dry_run": self.dry_run,
            "results": self.results,
        }
        # Additive: present only for restatement (reprocess) runs, so the
        # common-path payload shape is unchanged.
        if self.restatement_impact is not None:
            payload["restatement_impact"] = self.restatement_impact
        return payload

    def has_failures(self) -> bool:
        """True if any contract in this run failed."""
        return any(r.get("status") == "failed" for r in self.results)

    def failure_details(self) -> str:
        """One line per failed contract with its error message — meant to be put
        INTO a raised exception. Databricks (and most job runners) don't return
        notebook stdout, so a generic 'a contract failed' leaves operators blind;
        this makes the actual per-contract error travel with the exception."""
        fails = [r for r in self.results if r.get("status") == "failed"]
        if not fails:
            return ""
        parts = []
        for r in fails:
            name = r.get("contract") or r.get("table_name") or "?"
            layer = r.get("layer") or "?"
            err = str(r.get("error") or "no error captured").strip().splitlines()
            first = err[0][:300] if err else "no error captured"
            parts.append(f"{name} [{layer}]: {first}")
        return f"{len(fails)} contract(s) failed — " + " | ".join(parts)

    def __str__(self) -> str:
        lines = []
        lines.append("=" * 80)
        lines.append(" PIPELINE RUN SUMMARY")
        lines.append("=" * 80)
        lines.append(f"  Pipeline run    : {self.run_id}")
        lines.append(f"  Environment     : {self.environment}")
        lines.append(f"  Dry run         : {self.dry_run}")
        lines.append("")

        if not self.results:
            lines.append("  No contracts processed.")
            lines.append("=" * 80)
            return "\n".join(lines)

        header = f"  {'Table Name':<38} {'Layer':<8} {'Status':<10} {'Rows':<8} {'Good/Qrtn':<12}"
        lines.append(header)
        lines.append("  " + "-" * 78)

        for r in self.results:
            t_name = str(r.get("table_name") or r.get("contract") or "")[:36]
            layer = str(r.get("layer") or "")[:7]
            status = str(r.get("status") or "")[:9]
            rows = str(r.get("rows", "-"))[:7]

            good = r.get("rows_good")
            bad = r.get("rows_bad")
            if good is not None and bad is not None:
                dq_str = f"{good}/{bad}"  # pragma: no cover
            else:
                dq_str = "-"

            line = f"  {t_name:<38} {layer:<8} {status:<10} {rows:<8} {dq_str:<12}"
            lines.append(line)

            err = r.get("error")
            if err:
                lines.append(f"    └─ Error: {str(err)[:70]}")

        lines.append("=" * 80)
        if self.restatement_impact is not None:
            lines.append(format_restatement_impact(self.restatement_impact))
        return "\n".join(lines)


class CircuitBreakerTripped(Exception):
    """Raised when too many consecutive entity failures indicate an infrastructure outage."""

    pass


class EntityTimeoutError(Exception):
    """Raised when a single entity exceeds entity_timeout_minutes."""

    pass


class LakehousePipeline:
    """
    Executes a DomainRegistry through a pipeline run.
    """

    def __init__(self, registry: DomainRegistry, engine: str = "spark", spark: Any = None):
        self.registry = registry
        self.engine = engine
        self.spark = spark
        self.storage_mode = getattr(registry, "storage_mode", "uc")
        self.run_id = str(uuid.uuid4())

        # ── Engine / storage mode compatibility check ─────────────────────
        # Unity Catalog (uc) mode defaults to Spark, but other engines (Polars, DuckDB)
        # can now access remote cloud links dynamically.

        if self.engine == "spark" and not self.spark:
            # Try to auto-resolve if inside Databricks
            try:  # pragma: no cover
                from pyspark.sql import SparkSession  # pragma: no cover

                _builder = SparkSession.builder.config(
                    "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
                ).config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                try:
                    from delta import configure_spark_with_delta_pip  # type: ignore

                    _builder = configure_spark_with_delta_pip(_builder)
                except Exception:
                    pass
                self.spark = _builder.getOrCreate()  # pragma: no cover
            except ImportError:  # pragma: no cover
                pass  # pragma: no cover

        if self.engine == "spark" and self.storage_mode == "direct" and self.spark:  # pragma: no cover
            import os

            # If no contracts reference cloud paths (abfs/wasbs/s3/gs), skip the
            # Azure-credential requirement — pure-local spark runs don't need it.
            _needs_cloud = False
            try:
                for _c in getattr(self.registry, "contracts", []) or []:
                    _src = str(getattr(getattr(_c, "source", None), "path", "") or "")
                    if any(
                        _src.startswith(p)
                        for p in ("abfs://", "abfss://", "wasbs://", "wasb://", "s3://", "s3a://", "gs://")
                    ):
                        _needs_cloud = True
                        break
            except Exception:
                _needs_cloud = True  # fail safe: keep the original requirement

            client_id = os.environ.get("AZURE_CLIENT_ID") or os.environ.get("ARM_CLIENT_ID")
            client_secret = os.environ.get("AZURE_CLIENT_SECRET") or os.environ.get("ARM_CLIENT_SECRET")
            tenant_id = os.environ.get("AZURE_TENANT_ID") or os.environ.get("ARM_TENANT_ID")
            account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")
            if not _needs_cloud:
                client_id = client_secret = tenant_id = None
                account_key = account_key or "skip-local"

            if client_id and client_secret and tenant_id:
                self.spark.conf.set("fs.azure.account.auth.type", "OAuth")
                self.spark.conf.set(
                    "fs.azure.account.oauth.provider.type",
                    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                )
                self.spark.conf.set("fs.azure.account.oauth2.client.id", client_id)
                self.spark.conf.set("fs.azure.account.oauth2.client.secret", client_secret)
                self.spark.conf.set(
                    "fs.azure.account.oauth2.client.endpoint",
                    f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
                )
            elif not account_key:
                raise ValueError(
                    "Spark in 'direct' mode requires Azure credentials. "
                    "Please provide AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, and AZURE_TENANT_ID "
                    "(or AZURE_STORAGE_ACCOUNT_KEY) in os.environ."
                )

        if self.spark:
            try:
                # Force strictly OSS-compatible Delta tables to ensure Polars interoperability
                self.spark.conf.set("spark.databricks.delta.properties.defaults.enableDeletionVectors", "false")
                logger.debug(
                    "Disabled DeletionVectors by default in Spark session for OSS compatibility."
                )  # pragma: no cover
            except Exception as e:
                # Serverless / Spark Connect forbids setting session-level Delta
                # defaults (CONFIG_NOT_AVAILABLE). That's expected and harmless —
                # DeletionVectors are disabled per-table at write time in
                # materialization.py. Log only the concise first line; a Spark
                # Connect exception's str() otherwise carries the full JVM
                # stacktrace, which floods the output for a non-issue.
                reason = next((ln for ln in str(e).splitlines() if ln.strip()), e.__class__.__name__)
                logger.debug(
                    f"Session-level DeletionVectors default not settable "
                    f"(serverless/Spark Connect); enforced per-table instead: {reason}"
                )

    # ── Run log mode auto-detection ────────────────────────────────────────────

    def _resolve_run_log_mode(self, contract_dict: dict, explicit_mode: Optional[str] = None) -> Optional[str]:
        """Auto-detect run_log_mode when not explicitly set.

        When storage_mode is 'uc' and the contract has a run_log_table,
        default to table-only writes (skip JSON files).
        """
        if explicit_mode:
            return explicit_mode
        if self.storage_mode == "uc":
            metadata = (contract_dict or {}).get("metadata", {})
            if metadata.get("run_log_table"):
                return "table"
        return None

    # ── UC path auto-resolution ───────────────────────────────────────────────

    @staticmethod
    def _looks_like_catalog_ref(path: str) -> bool:
        """Return True if *path* looks like a Unity Catalog table reference.

        Heuristic: contains at least one dot, no slashes, and doesn't start
        with a known scheme (abfss://, s3://, gs://, /Volumes, etc.).
        """
        if not path:
            return False
        if path.startswith(("table:", "abfss://", "s3://", "gs://", "adl://", "https://", "/", ".")):
            return False
        # Backtick-wrapped identifiers (e.g. `catalog`.schema.table) are catalog refs
        if "`" in path:
            return True
        return "." in path and "/" not in path

    def _resolve_uc_paths(self, contract_dict: dict) -> dict:
        """Auto-populate materialization, quarantine, source and run-log
        paths from ``info.table_name`` and registry storage config.

        Mutates *contract_dict* in-place and returns it for convenience.

        Rules
        -----
        1. If ``info.table_name`` is set and a target field is missing,
           derive it from the registry's storage roots.
        2. If ``storage_mode == 'uc'`` and a path looks like a catalog
           reference, auto-prefix ``table:`` so downstream readers treat
           it as a Spark table rather than a file path.
        3. Source auto-resolution only applies to silver/gold layers
           (bronze sources are external and user-controlled).
        """
        if not contract_dict:
            return contract_dict  # pragma: no cover

        info = contract_dict.get("info") or {}
        table_name = info.get("table_name")
        target_layer = info.get("target_layer", "")
        storage = self.registry.storage if self.registry else None

        # ── Step 1: Derive missing paths from table_name ──────────────────
        _is_direct = self.storage_mode == "direct"

        # ── Direct-mode pre-flight checks ─────────────────────────────────
        if _is_direct and table_name and storage:
            if not storage.external_location_root:
                raise ValueError(
                    "storage_mode='direct' requires storage.external_location_root "
                    "to be set (e.g. 'abfss://domain@account.dfs.core.windows.net'). "
                    "Set this in _system.yaml under storage.external_location_root."
                )
            root = storage.external_location_root
            if "abfss://" in root or "adl://" in root:
                # Azure ADLS — check for SPN or storage key credentials
                spn_vars = ["AZURE_CLIENT_ID", "AZURE_CLIENT_SECRET", "AZURE_TENANT_ID"]
                key_vars = ["AZURE_STORAGE_ACCOUNT_KEY"]
                has_spn = all(os.environ.get(v) for v in spn_vars)
                has_key = any(os.environ.get(v) for v in key_vars)
                if not has_spn and not has_key:
                    logger.warning(
                        f"Direct mode with Azure storage — no credentials detected. "
                        f"Set either SPN ({', '.join(spn_vars)}) or "
                        f"storage key ({', '.join(key_vars)}) env vars. "
                        f"On Databricks this may be handled via cluster config."
                    )
            elif "s3://" in root or "s3a://" in root:  # pragma: no cover
                aws_vars = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]  # pragma: no cover
                if not all(os.environ.get(v) for v in aws_vars):  # pragma: no cover
                    logger.warning(
                        f"Direct mode with S3 — credentials may be needed: {', '.join(aws_vars)}"
                    )  # pragma: no cover
            elif "gs://" in root:  # pragma: no cover
                if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):  # pragma: no cover
                    logger.warning(
                        "Direct mode with GCS — GOOGLE_APPLICATION_CREDENTIALS may be needed."
                    )  # pragma: no cover

        if table_name and storage:
            # ── Layer-aware root resolution ────────────────────────────────
            # In direct mode, prefer the layer-specific path (bronze_path,
            # silver_path, gold_path) over the generic external_location_root.
            # This lets users set one root per layer in _system.yaml:
            #   bronze_path: "abfss://.../{bronze_layer}"
            #   silver_path: "abfss://.../{silver_layer}"
            # and have each contract auto-resolve to the correct layer path.
            _layer_path_map = {
                "bronze": getattr(storage, "bronze_path", None),
                "silver": getattr(storage, "silver_path", None),
                "gold": getattr(storage, "gold_path", None),
            }
            _layer_root = _layer_path_map.get(target_layer) or storage.external_location_root

            # Materialization
            mat = contract_dict.setdefault("materialization", {})
            if _is_direct:
                if not mat.get("target_path") and _layer_root:
                    mat["target_path"] = f"{_layer_root}/{table_name}"
                if not mat.get("format"):
                    mat["format"] = "delta"
            else:
                if not mat.get("target_path") and storage.domain_catalog:
                    mat["target_path"] = f"{storage.domain_catalog}.{table_name}"
            if not mat.get("location") and _layer_root:
                mat["location"] = f"{_layer_root}/{table_name}"

            # Quarantine — inherit system-level defaults first so that
            # quar.get("enabled") is truthful before we derive the target.
            if self.registry and self.registry.quarantine:
                existing_quar = contract_dict.get("quarantine") or {}
                merged_q = {**self.registry.quarantine, **existing_quar}
                contract_dict["quarantine"] = merged_q

            # Quarantine — prefix table name with domain so tables from
            # different domains don't collide in the shared quarantine schema.
            quar = contract_dict.get("quarantine") or {}
            if _is_direct:
                if quar.get("enabled") and not quar.get("target"):
                    _domain = info.get("domain", "")
                    _q_table = f"{_domain}_{table_name}" if _domain else table_name
                    _q_path = getattr(storage, "quarantine_path", None)
                    if _q_path:
                        quar["target"] = f"{_q_path}/{_q_table}"
                    elif _layer_root:
                        quar["target"] = f"{_layer_root}/_quarantine/{_q_table}"
                    contract_dict["quarantine"] = quar
            else:
                if quar.get("enabled") and not quar.get("target") and storage.quarantine_root:
                    _domain = info.get("domain", "")
                    _q_table = f"{_domain}_{table_name}" if _domain else table_name
                    quar["target"] = f"{storage.quarantine_root}.{_q_table}"

                    # Ensure it creates an EXTERNAL table if quarantine_path is set
                    _q_path = getattr(storage, "quarantine_path", None)
                    if _q_path:
                        quar["location"] = f"{_q_path}/{_q_table}"

                    contract_dict["quarantine"] = quar

            # Run log (from registry, not per-contract) — skip in direct mode
            if not _is_direct and storage.run_log_table:
                metadata = contract_dict.setdefault("metadata", {})
                if not metadata.get("run_log_table"):
                    metadata["run_log_table"] = storage.run_log_table

            # Source for silver/gold — derive from table_name of upstream layer
            if target_layer in ("silver", "gold"):
                source = contract_dict.get("source") or {}
                if _is_direct:
                    if not source.get("path") and source.get("type") != "sql" and _layer_root:
                        source["path"] = f"{_layer_root}/{table_name}"
                        source.setdefault("type", "landing")
                        source.setdefault("format", "delta")
                        contract_dict["source"] = source
                else:
                    if source.get("type") == "table" and not source.get("path") and storage.domain_catalog:
                        source["path"] = f"{storage.domain_catalog}.{table_name}"
                        contract_dict["source"] = source

        # ── Step 2: Auto-prefix table: in UC mode ────────────────────────
        if self.storage_mode == "uc":
            # Materialization target_path
            mat = contract_dict.get("materialization") or {}
            tp = mat.get("target_path", "")
            if tp and not tp.startswith("table:") and self._looks_like_catalog_ref(tp):
                mat["target_path"] = f"table:{tp}"

            # Quarantine target
            quar = contract_dict.get("quarantine") or {}
            qt = quar.get("target", "")
            if qt and not qt.startswith("table:") and self._looks_like_catalog_ref(qt):
                quar["target"] = f"table:{qt}"

            # Source path (silver/gold only)
            if target_layer in ("silver", "gold"):
                source = contract_dict.get("source") or {}
                sp = source.get("path", "")
                if sp and not sp.startswith("table:") and self._looks_like_catalog_ref(sp):
                    source["path"] = f"table:{sp}"

            # Link paths
            for link in contract_dict.get("links", []):
                lp = link.get("path", "")
                if lp and not lp.startswith("table:") and self._looks_like_catalog_ref(lp):
                    link["path"] = f"table:{lp}"
                    link["type"] = "table"

        # ── Step 3: Inherit system-level lineage defaults ────────────────
        if self.registry and self.registry.lineage:
            existing_lineage = contract_dict.get("lineage") or {}
            # System-level is base, contract-level overrides
            merged = {**self.registry.lineage, **existing_lineage}
            contract_dict["lineage"] = merged

        # ── Step 4: Inherit system-level quarantine defaults ──────────
        if self.registry and self.registry.quarantine:
            existing_quar = contract_dict.get("quarantine") or {}
            # System-level is base, contract-level overrides
            merged = {**self.registry.quarantine, **existing_quar}
            contract_dict["quarantine"] = merged

        # ── Step 5: Inherit per-layer materialization defaults ─────────
        if self.registry and self.registry.materialization and target_layer:
            _global_mat = self.registry.materialization.get("_all", {})
            layer_defaults = self.registry.materialization.get(target_layer, {})
            # Global base ← layer overrides ← contract overrides
            combined = {**_global_mat, **layer_defaults}
            if combined:
                existing_mat = contract_dict.get("materialization") or {}
                merged = {**combined, **existing_mat}
                contract_dict["materialization"] = merged

        # ── Step 6: Inherit per-layer server defaults ────────────────
        if self.registry and self.registry.server and target_layer:
            layer_server = self.registry.server.get(target_layer)
            if layer_server and isinstance(layer_server, dict):
                server = contract_dict.get("server")
                if server and isinstance(server, dict):
                    # Deep-merge: layer defaults fill gaps, including
                    # nested dicts like schema_policy
                    for key, val in layer_server.items():  # pragma: no cover
                        if key not in server:  # pragma: no cover
                            server[key] = copy.deepcopy(val)  # pragma: no cover
                        elif isinstance(val, dict) and isinstance(server.get(key), dict):  # pragma: no cover
                            # Deep-merge nested dicts (e.g. schema_policy)  # pragma: no cover
                            for sub_key, sub_val in val.items():  # pragma: no cover
                                if sub_key not in server[key]:  # pragma: no cover
                                    server[key][sub_key] = sub_val  # pragma: no cover
                else:
                    # No server block — create one with reasonable defaults
                    # so schema_policy, cast_to_string etc. are inherited.
                    new_server = copy.deepcopy(layer_server)
                    new_server.setdefault("type", "local")
                    new_server.setdefault("path", ".")
                    contract_dict["server"] = new_server
                    # Also stash as metadata for materialization fallback
                    meta = contract_dict.setdefault("metadata", {})
                    meta["_server_layer_defaults"] = dict(layer_server)

        return contract_dict

    # ── Dependency ordering & parallel execution ──────────────────────────

    @staticmethod
    def _topological_sort(contracts: List[RegistryContract]) -> List[RegistryContract]:
        """Sort contracts within a layer by ``depends_on`` (topological order).

        Raises ``ValueError`` on circular dependencies.

        Delegates to :func:`lakelogic.pipeline.impact.topological_order`, which
        owns the shared forward (downstream) edge index — the same index the
        restatement impact report reads. Behaviour is unchanged.
        """
        return topological_order(contracts)

    @staticmethod
    def _group_by_dependency_level(contracts: List[RegistryContract]) -> List[List[RegistryContract]]:
        """Group contracts into execution waves (levels) for parallel execution.

        Wave 0: no dependencies → run in parallel.
        Wave 1: depends only on Wave 0 → run in parallel after Wave 0.
        etc.
        """
        by_entity = {c.entity: c for c in contracts}
        levels: Dict[str, int] = {}

        def _level(entity: str, visited: set) -> int:
            if entity in levels:
                return levels[entity]
            if entity in visited:
                raise ValueError(f"Circular dependency: {entity}")
            visited.add(entity)
            c = by_entity.get(entity)
            if not c or not c.depends_on:
                levels[entity] = 0
                return 0
            dep_levels = [_level(dep, visited) for dep in c.depends_on if dep in by_entity]
            if not dep_levels:
                levels[entity] = 0  # pragma: no cover
                return 0  # pragma: no cover
            dep_level = max(dep_levels)
            levels[entity] = dep_level + 1
            return dep_level + 1

        for c in contracts:
            if c.entity not in levels:
                _level(c.entity, set())

        # Group by level
        waves: Dict[int, List[RegistryContract]] = defaultdict(list)
        for c in contracts:
            waves[levels[c.entity]].append(c)

        return [waves[i] for i in sorted(waves)]

    # ── Test data generation ──────────────────────────────────────────────────

    def _generate_test_data(
        self,
        contracts: List[RegistryContract],
        rows: int = 500,
        invalid_ratio: float = 0.05,
        ai: bool = False,
        ai_provider: Optional[str] = None,
        ai_model: Optional[str] = None,
        ai_api_key: Optional[str] = None,
        suggest_rules: bool = False,
    ) -> None:
        """Generate test landing-zone data for bronze contracts.

        Uses the contract schema and source config to create realistic test
        files at the landing path in the correct format and partition structure.
        """
        from lakelogic.core.generator import DataGenerator

        for c in contracts:
            if c.layer != "bronze" or not c.contract_dict:
                continue  # pragma: no cover

            source = c.contract_dict.get("source") or {}
            landing_path = source.get("path")
            fmt = (source.get("format") or "json").lower()
            partition_cfg = source.get("partition")

            if not landing_path:
                logger.warning(f"No source.path for {c.entity} — skipping test data generation")  # pragma: no cover
                continue  # pragma: no cover

            if not c.resolved_path:
                logger.warning(f"No resolved contract path for {c.entity} — skipping")  # pragma: no cover
                continue  # pragma: no cover

            try:
                gen = DataGenerator(c.resolved_path, seed=42)
            except Exception as e:  # pragma: no cover
                logger.warning(f"Cannot create DataGenerator for {c.entity}: {e}")  # pragma: no cover
                continue  # pragma: no cover

            logger.info(f"  🧪 Generating {rows} test rows for {c.entity} (format={fmt})")

            # Log schema overview for observability
            fields = (c.contract_dict.get("model") or {}).get("fields") or []
            n_required = sum(1 for f in fields if f.get("required"))
            n_nullable = len(fields) - n_required
            logger.info(f"     Schema   : {len(fields)} fields ({n_required} required, {n_nullable} nullable)")
            if partition_cfg:
                logger.info(f"     Partition: {partition_cfg}")

            try:
                df = gen.generate(
                    rows=rows, invalid_ratio=invalid_ratio, ai=ai, ai_provider=ai_provider, ai_model=ai_model
                )
            except Exception as e:  # pragma: no cover
                logger.warning(f"Test data generation failed for {c.entity}: {e}")  # pragma: no cover
                continue  # pragma: no cover

            # Log output summary
            if hasattr(df, "shape"):
                logger.info(f"     Output   : {df.shape[0]:,} rows × {df.shape[1]} columns")

            # Determine output path
            output_dir = Path(landing_path)

            if partition_cfg:
                # Generate date-partitioned directories
                import datetime

                partition_format = (
                    partition_cfg.get("format", "y_%Y/m_%m/d_%d")
                    if isinstance(partition_cfg, dict)
                    else str(partition_cfg)
                )
                lookback_days = partition_cfg.get("lookback_days", 3) if isinstance(partition_cfg, dict) else 3
                today = datetime.date.today()
                rows_per_day = max(1, rows // lookback_days)

                for day_offset in range(lookback_days):
                    day = today - datetime.timedelta(days=day_offset)
                    partition_dir = output_dir / day.strftime(partition_format)
                    partition_dir.mkdir(parents=True, exist_ok=True)
                    day_df = gen.generate(
                        rows=rows_per_day,
                        invalid_ratio=invalid_ratio,
                        ai=ai,
                        ai_provider=ai_provider,
                        ai_model=ai_model,
                    )
                    self._write_test_data(day_df, partition_dir / f"data.{fmt}", fmt)
                    logger.debug(f"    Partition: {partition_dir} ({rows_per_day} rows)")
            else:
                output_dir.mkdir(parents=True, exist_ok=True)
                # Produce the full 3-file generation report output
                if invalid_ratio > 0:
                    try:
                        data_path, invalid_path, report_path = gen.save_with_report(
                            df, output_dir, name=c.entity, format=fmt
                        )
                        logger.info(f"     Report   : {report_path}")
                    except Exception as e:  # pragma: no cover
                        logger.debug(f"save_with_report failed, falling back: {e}")  # pragma: no cover
                        self._write_test_data(df, output_dir / f"test_data.{fmt}", fmt)  # pragma: no cover
                else:
                    self._write_test_data(df, output_dir / f"test_data.{fmt}", fmt)

            logger.info(f"  Test data written to {output_dir}")

            if suggest_rules and c.resolved_path:
                try:
                    from lakelogic.ai.contract_enricher import enrich_contract
                    from lakelogic.core.bootstrap import _format_contract_yaml

                    logger.info(f"  🤖 Suggesting quality rules for {c.entity}...")

                    # Convert generated df to pandas for enrichment
                    pd_df = df.to_pandas() if hasattr(df, "to_pandas") else df

                    enriched_dict = enrich_contract(
                        c.contract_dict, pd_df, provider=ai_provider, model=ai_model, api_key=ai_api_key, sample_size=20
                    )

                    with open(c.resolved_path, "w", encoding="utf-8") as f:
                        f.write(_format_contract_yaml(enriched_dict))

                    logger.info(f"  ✅ Saved suggested rules to {c.resolved_path}")
                except Exception as e:  # pragma: no cover
                    logger.warning(f"  ⚠️ Failed to suggest rules for {c.entity}: {e}")  # pragma: no cover

    @staticmethod
    def _write_test_data(df: Any, path: Path, fmt: str) -> None:
        """Write a Polars DataFrame to file in the given format."""
        import polars as pl

        if not isinstance(df, pl.DataFrame):
            return
        if fmt == "json":
            df.write_ndjson(path)
        elif fmt == "parquet":
            df.write_parquet(path)
        elif fmt == "csv":
            df.write_csv(path)
        else:
            df.write_ndjson(path)

    # ── Phase 1: Setup & Resets ──────────────────────────────────────────────

    def _delete_run_log_entries(self, contract_dict: Dict[str, Any], entity_name: str, layer: str) -> None:
        """Delete run log entries for a specific contract using precise filtering.

        Uses dataset (target table name), data_layer, domain, and system to
        uniquely identify the contract's rows — avoids false positives from
        LIKE-based matching.
        """
        _rl_table = contract_dict.get("metadata", {}).get("run_log_table")
        if not _rl_table:
            return  # pragma: no cover

        # Resolve the target table name (dataset column in run_log)
        mat_cfg = contract_dict.get("materialization", {})
        _target = mat_cfg.get("target_path", "") or mat_cfg.get("path", "")
        info = contract_dict.get("info", {})
        metadata = contract_dict.get("metadata", {})

        if str(_target).startswith("table:"):
            _table_full = str(_target)[len("table:") :]
            dataset_val = _table_full.split(".")[-1] if "." in _table_full else _table_full
        else:
            dataset_val = info.get("table_name") or contract_dict.get("dataset") or info.get("title")

        # Resolve domain, system, data_layer from info/metadata
        info = contract_dict.get("info", {})
        metadata = contract_dict.get("metadata", {})
        domain = metadata.get("domain") or info.get("domain")
        system = metadata.get("system") or info.get("system")
        data_layer = metadata.get("data_layer") or info.get("target_layer") or layer

        # Build WHERE clause with all available fields for precision
        conditions = []
        params_desc = []
        if dataset_val:
            conditions.append(f"dataset = '{dataset_val}'")
            params_desc.append(f"dataset={dataset_val}")
        if data_layer:
            conditions.append(f"data_layer = '{data_layer}'")
            params_desc.append(f"layer={data_layer}")
        if domain:
            conditions.append(f"domain = '{domain}'")
            params_desc.append(f"domain={domain}")
        if system:
            conditions.append(f"system = '{system}'")
            params_desc.append(f"system={system}")

        if not conditions:
            # Fallback: if no precise fields available, use case-insensitive LIKE
            conditions.append(f"LOWER(contract) LIKE LOWER('%{entity_name}%')")  # pragma: no cover
            params_desc.append(f"contract~={entity_name}")  # pragma: no cover

        where_clause = " AND ".join(conditions)
        try:
            if self.spark:
                _is_uri = _rl_table.startswith(("abfss://", "abfs://", "s3://", "s3a://", "gs://", "gcs://", "file://"))
                _table_ref = f"delta.`{_rl_table}`" if _is_uri else _rl_table

                _exists = False
                if _is_uri:
                    try:
                        self.spark.read.format("delta").load(_rl_table).limit(1)
                        _exists = True
                    except Exception:
                        _exists = False
                else:
                    _exists = self.spark.catalog.tableExists(_rl_table)

                if _exists:
                    self.spark.sql(f"DELETE FROM {_table_ref} WHERE {where_clause}")
                    logger.info(f"  Cleared run log entries ({', '.join(params_desc)}) from {_rl_table}")
                else:
                    logger.debug(
                        f"  Run log table {_rl_table} does not exist yet; nothing to clear"
                    )  # pragma: no cover
            else:
                # Polars / non-Spark fallback using delta-rs
                try:
                    from deltalake import DeltaTable

                    from lakelogic.core.run_log import _build_cloud_opts

                    storage_opts = _build_cloud_opts(_rl_table)
                    try:
                        dt = DeltaTable(_rl_table, storage_options=storage_opts)
                        dt.delete(predicate=where_clause)
                        logger.info(
                            f"  Cleared run log entries ({', '.join(params_desc)}) from {_rl_table} via delta-rs"
                        )
                    except Exception as e:  # pragma: no cover
                        if (
                            "No table" in str(e) or "Not a Delta table" in str(e) or "is not a Delta table" in str(e)
                        ):  # pragma: no cover
                            logger.debug(
                                f"  Run log table {_rl_table} does not exist yet; nothing to clear"
                            )  # pragma: no cover
                        else:  # pragma: no cover
                            raise e  # pragma: no cover
                except ImportError:  # pragma: no cover
                    logger.debug("deltalake not installed, skipping run log delete")  # pragma: no cover
        except Exception as _rl_exc:  # pragma: no cover
            logger.warning(f"  Could not clear run log for '{entity_name}': {_rl_exc}")  # pragma: no cover

    def _execute_resets(
        self, active_contracts: List[RegistryContract], reset_layers: Set[str], reload_layers: Set[str], dry_run: bool
    ) -> None:
        """Run DROP (reset) and TRUNCATE (reload) operations.

        Run-log / SLO-check clearing is done PER ENTITY inside the loop below
        (via _delete_run_log_entries, which DELETEs only the rows matching the
        reset entity's dataset/layer/domain/system). The shared `_logs` /
        `_slo_checks` tables are intentionally NOT wiped wholesale here — doing
        so destroyed other entities' history (and, for Delta, removed the
        `_delta_log` transaction log so the subsequent per-entity DELETE failed
        with "no log files"). Resetting one entity must leave the rest intact.
        """
        for c in active_contracts:
            layer = c.layer
            name = c.entity
            if layer in reset_layers:
                logger.info(f"Resetting [{layer}] {name}")
                processor = DataProcessor(contract=c.contract_dict, engine=self.engine, pipeline_run_id=self.run_id)
                # Always execute resets — if user explicitly asked for reset, honour it
                # regardless of pipeline dry_run (dry_run only affects data processing)
                processor.reset(dry_run=False)

                contract_dict = c.contract_dict or {}

                # Drop the main managed table for this entity
                mat_cfg = contract_dict.get("materialization", {})
                mat_target = mat_cfg.get("target_path", "")
                info = contract_dict.get("info") or {}
                _table_name = info.get("table_name", "")

                if mat_target.startswith("table:") and self.spark:
                    # Explicit table: target in materialization
                    main_table = mat_target[len("table:") :]
                    try:
                        self.spark.sql(f"DROP TABLE IF EXISTS {main_table}")
                        logger.info(f"  Dropped main table {main_table}")
                    except Exception as _mt_exc:  # pragma: no cover
                        logger.warning(f"  Could not drop main table {main_table}: {_mt_exc}")  # pragma: no cover
                elif _table_name and self.spark and self.registry:
                    # Derive from domain_catalog + table_name (UC convention)
                    storage = self.registry.storage  # pragma: no cover
                    if storage and storage.domain_catalog:  # pragma: no cover
                        main_table = f"{storage.domain_catalog}.{_table_name}"  # pragma: no cover
                        try:  # pragma: no cover
                            self.spark.sql(f"DROP TABLE IF EXISTS {main_table}")  # pragma: no cover
                            logger.info(f"  Dropped main table {main_table}")  # pragma: no cover
                        except Exception as _mt_exc:  # pragma: no cover
                            logger.warning(f"  Could not drop main table {main_table}: {_mt_exc}")  # pragma: no cover

                # Drop the quarantine table for this entity too
                q_cfg = contract_dict.get("quarantine", {})
                q_target = q_cfg.get("target", "")
                if q_target.startswith("table:") and self.spark:
                    q_table = q_target[len("table:") :]
                    try:
                        self.spark.sql(f"DROP TABLE IF EXISTS {q_table}")
                        logger.info(f"  Dropped quarantine table {q_table}")
                    except Exception as _q_exc:  # pragma: no cover
                        logger.warning(f"  Could not drop quarantine table {q_table}: {_q_exc}")  # pragma: no cover

                    # Also clean up the cloud storage location for the quarantine table
                    storage = self.registry.storage if self.registry else None
                    if storage and storage.external_location_root:
                        info = contract_dict.get("info") or {}
                        _tbl = info.get("table_name", "")
                        _domain = info.get("domain", "")
                        _q_name = f"{_domain}_{_tbl}" if _domain else _tbl
                        q_cloud_path = f"{storage.external_location_root}/_quarantine/{_q_name}"
                        try:
                            _dbutils = None
                            try:
                                _dbutils = self.spark._jvm.com.databricks.service.DBUtils(self.spark._jsc.sc())
                            except Exception:
                                try:
                                    import IPython

                                    _dbutils = IPython.get_ipython().user_ns.get("dbutils")
                                except Exception:  # pragma: no cover
                                    pass  # pragma: no cover
                            if _dbutils:
                                _dbutils.fs.rm(q_cloud_path, True)
                                logger.info(f"  Reset: deleted quarantine cloud location {q_cloud_path} via dbutils")
                            else:
                                logger.debug(  # pragma: no cover
                                    f"  dbutils not available; quarantine cloud path {q_cloud_path} not deleted"  # pragma: no cover # noqa: E501
                                )  # pragma: no cover
                        except Exception as _qp_exc:  # pragma: no cover
                            logger.warning(
                                f"  Could not delete quarantine cloud path {q_cloud_path}: {_qp_exc}"
                            )  # pragma: no cover

                elif q_cfg.get("enabled") and q_target:
                    # Quarantine target is not a table: prefix — try cloud (fsspec) deletion
                    _q_deleted = False
                    _is_cloud_q = any(
                        q_target.startswith(pfx)
                        for pfx in ("abfss://", "abfs://", "s3://", "s3a://", "gs://", "gcs://")
                    )
                    if _is_cloud_q:
                        try:
                            import os as _os_q

                            import fsspec

                            _q_opts: dict = {}
                            if q_target.startswith(("abfss://", "abfs://")):
                                for _ek, _ok in [
                                    ("AZURE_STORAGE_ACCOUNT_NAME", "account_name"),
                                    ("AZURE_STORAGE_ACCOUNT", "account_name"),
                                    ("AZURE_STORAGE_ACCOUNT_KEY", "account_key"),
                                    ("AZURE_TENANT_ID", "tenant_id"),
                                    ("AZURE_CLIENT_ID", "client_id"),
                                    ("AZURE_CLIENT_SECRET", "client_secret"),
                                ]:
                                    _v = _os_q.getenv(_ek)
                                    if _v and _ok not in _q_opts:
                                        _q_opts[_ok] = _v  # pragma: no cover
                            fs, q_path_part = fsspec.core.url_to_fs(q_target, **_q_opts)
                            if fs.exists(q_path_part):
                                fs.rm(q_path_part, recursive=True)
                                logger.info(f"  Reset: deleted quarantine cloud path {q_target}")
                            else:
                                logger.info(
                                    f"  Reset: quarantine cloud path {q_target} does not exist"
                                )  # pragma: no cover
                            _q_deleted = True
                        except ImportError:  # pragma: no cover
                            logger.debug("  fsspec not available for quarantine cloud deletion")  # pragma: no cover
                        except Exception as _q_exc:  # pragma: no cover
                            logger.warning(
                                f"  Could not delete quarantine cloud path {q_target}: {_q_exc}"
                            )  # pragma: no cover
                    else:
                        import shutil
                        from pathlib import Path as _P

                        _qp = _P(q_target)
                        if _qp.exists():
                            try:
                                if _qp.is_dir():
                                    shutil.rmtree(_qp)
                                else:
                                    _qp.unlink()  # pragma: no cover
                                logger.info(f"  Reset: deleted local quarantine path {q_target}")
                                _q_deleted = True
                            except Exception as _q_exc:  # pragma: no cover
                                logger.warning(
                                    f"  Could not delete local quarantine path {q_target}: {_q_exc}"
                                )  # pragma: no cover
                        else:  # pragma: no cover
                            _q_deleted = True  # pragma: no cover
                    if not _q_deleted:
                        logger.warning(  # pragma: no cover
                            f"  Quarantine is enabled for {name} but could not resolve/delete "
                            f"target '{q_target}'. Quarantine data was NOT dropped."
                        )

                # Clear run log entries using precise multi-column filter
                self._delete_run_log_entries(contract_dict, name, layer)

            elif layer in reload_layers:
                logger.info(f"Reloading (truncate) [{layer}] {name}")
                contract_dict = c.contract_dict or {}
                mat_cfg = contract_dict.get("materialization", {})
                mat_target = mat_cfg.get("target_path", "") or mat_cfg.get("path", "")

                if mat_target.startswith("table:") and self.spark:
                    table_name = mat_target[len("table:") :]  # pragma: no cover
                    if not dry_run:  # pragma: no cover
                        try:  # pragma: no cover
                            self.spark.sql(f"TRUNCATE TABLE {table_name}")  # pragma: no cover
                            logger.info(f"  Truncated {table_name}")  # pragma: no cover
                        except Exception as e:  # pragma: no cover
                            logger.warning(f"  Could not truncate {table_name}: {e}")  # pragma: no cover
                elif mat_target:
                    if not dry_run:
                        try:
                            # Use internal data contract wipe mechanism (supports fsspec now)
                            DataProcessor(
                                contract=contract_dict, engine=self.engine, pipeline_run_id=self.run_id
                            ).reset(targets=["materialization"])
                            logger.info(f"  Truncated (deleted files) {mat_target}")
                        except Exception as e:  # pragma: no cover
                            logger.warning(f"  Could not truncate files at {mat_target}: {e}")  # pragma: no cover

                q_cfg = contract_dict.get("quarantine", {})
                q_target = q_cfg.get("target", "")
                if q_target.startswith("table:") and q_cfg.get("enabled", False) and self.spark:
                    q_table = q_target[len("table:") :]  # pragma: no cover
                    if not dry_run:  # pragma: no cover
                        try:  # pragma: no cover
                            self.spark.sql(f"TRUNCATE TABLE {q_table}")  # pragma: no cover
                            logger.info(f"  Truncated quarantine {q_table}")  # pragma: no cover
                        except Exception as e:  # pragma: no cover
                            logger.warning(f"  Could not truncate {q_table}: {e}")  # pragma: no cover
                elif q_target and q_cfg.get("enabled", False):
                    if not dry_run:
                        try:
                            DataProcessor(
                                contract=contract_dict, engine=self.engine, pipeline_run_id=self.run_id
                            ).reset(targets=["quarantine"])
                            logger.info(f"  Truncated quarantine (deleted files) {q_target}")
                        except Exception as e:  # pragma: no cover
                            logger.warning(
                                f"  Could not truncate quarantine files at {q_target}: {e}"
                            )  # pragma: no cover

                # Clear run log entries using precise multi-column filter
                if not dry_run:
                    self._delete_run_log_entries(contract_dict, name, layer)

                processor = DataProcessor(contract=contract_dict, engine=self.engine, pipeline_run_id=self.run_id)
                if not dry_run:
                    processor.reset(targets=["watermark", "run_log"])
                else:
                    processor.reset(targets=["watermark", "run_log"], dry_run=True)  # pragma: no cover

    # ── Phase 2: DDL Only ────────────────────────────────────────────────────

    def generate_ddl_only(self, active_contracts: List[RegistryContract], dry_run: bool) -> PipelineRunSummary:
        """Create target tables from schemas without data loading.

        After CREATE TABLE (IF NOT EXISTS), introspects the existing schema
        and applies safe schema evolution (new columns, type widenings) via
        ``generate_alter_ddl``.
        """
        from lakelogic.core.ddl import _resolve_table_name, generate_alter_ddl

        summary = PipelineRunSummary(self.run_id, "ddl_only", dry_run)
        failures = []

        for c in active_contracts:
            try:
                processor = DataProcessor(contract=c.contract_dict, engine=self.engine, pipeline_run_id=self.run_id)
                resolved_table = _resolve_table_name(processor.contract) or ""

                if dry_run:
                    ddl = processor.generate_ddl(backend=self.engine)
                    logger.info(f"DRY RUN DDL Preview for {c.entity}:\n{ddl}")

                    # Also preview ALTER statements if we can introspect
                    existing_cols, existing_types = self._introspect_table_schema(processor.contract, self.engine)
                    if existing_cols:
                        alter_stmts = generate_alter_ddl(
                            processor.contract,
                            self.engine,
                            existing_cols,
                            existing_column_types=existing_types,
                        )
                        if alter_stmts:
                            alter_preview = "\n".join(alter_stmts)
                            logger.info(f"DRY RUN Schema Evolution for {c.entity}:\n{alter_preview}")

                    summary.append(c.entity, c.layer, "ddl_dry_run", table_name=resolved_table)
                else:
                    processor.create_table(backend=self.engine)
                    logger.info(f"Table created for {c.entity}")

                    # ── Schema evolution: apply safe ALTERs to existing tables ──
                    existing_cols, existing_types = self._introspect_table_schema(processor.contract, self.engine)
                    if existing_cols:
                        alter_stmts = generate_alter_ddl(
                            processor.contract,
                            self.engine,
                            existing_cols,
                            existing_column_types=existing_types,
                        )
                        if alter_stmts:
                            self._execute_alter_statements(alter_stmts, self.engine, c.entity)
                            logger.info(f"Applied {len(alter_stmts)} schema evolution statement(s) for {c.entity}")

                    summary.append(c.entity, c.layer, "ddl_created", table_name=resolved_table)
            except Exception as e:
                msg = _friendly_validation_error(c.entity, e)
                logger.error(msg)
                summary.append(c.entity, c.layer, "ddl_failed", error=msg)
                failures.append((c.entity, msg))

        if failures:
            failed_names = ", ".join(f[0] for f in failures)
            raise RuntimeError(f"DDL failed for {len(failures)} contract(s): {failed_names}. See logs above.")

        return summary

    @staticmethod
    def _introspect_table_schema(contract, backend: str) -> tuple:
        """Introspect existing table schema to enable schema evolution.

        Returns:
            Tuple of (column_names: List[str], column_types: Dict[str, str]).
            Returns ([], {}) if the table doesn't exist or can't be introspected.
        """
        from lakelogic.core.ddl import _resolve_table_name

        try:
            # ── Introspection Routing ─────────────────────────────────────
            # Determine if target is a direct path (Delta folder) or catalog backed
            mat = getattr(contract, "materialization", None)
            target_path = None
            if mat and hasattr(mat, "target_path") and mat.target_path:
                target = str(mat.target_path)
                if not target.startswith("table:"):
                    target_path = target

            # ── Pattern 1: Direct Storage (Delta Protocol) Introspection ─
            if backend in ("polars", "pandas", "python") or (backend == "duckdb" and target_path):
                if not target_path:
                    return [], {}  # pragma: no cover

                try:
                    from deltalake import DeltaTable

                    from lakelogic.core.processor import DataProcessor as _DP

                    # Get storage options for cloud paths
                    storage_opts = None
                    if any(target_path.startswith(p) for p in ("abfss://", "s3://", "gs://")):
                        _dummy = _DP.__new__(_DP)
                        storage_opts = _dummy._get_cloud_storage_options(target_path)

                    dt = DeltaTable(target_path, storage_options=storage_opts)
                    schema = dt.schema()
                    col_names = [f.name for f in schema.fields]
                    # Map Arrow/Delta types to contract-like type names
                    _delta_type_map = {
                        "int8": "TINYINT",
                        "int16": "SMALLINT",
                        "int32": "INTEGER",
                        "int64": "BIGINT",
                        "uint8": "SMALLINT",
                        "uint16": "INTEGER",
                        "uint32": "BIGINT",
                        "uint64": "BIGINT",
                        "float": "FLOAT",
                        "double": "DOUBLE",
                        "float32": "FLOAT",
                        "float64": "DOUBLE",
                        "string": "VARCHAR",
                        "utf8": "VARCHAR",
                        "large_string": "VARCHAR",
                        "large_utf8": "VARCHAR",
                        "boolean": "BOOLEAN",
                        "bool": "BOOLEAN",
                        "date32": "DATE",
                        "date": "DATE",
                        "timestamp": "TIMESTAMP",
                        "binary": "BINARY",
                    }
                    col_types = {}
                    for f in schema.fields:
                        type_str = str(f.type)
                        # Delta returns PrimitiveType("string") — extract inner name
                        import re as _re

                        _prim = _re.search(r'"(\w+)"', type_str)
                        if _prim:
                            type_str = _prim.group(1).lower()
                        else:
                            type_str = type_str.lower()
                        # Handle timestamp variants
                        if "timestamp" in type_str:
                            col_types[f.name] = "TIMESTAMP"
                        else:
                            col_types[f.name] = _delta_type_map.get(type_str, type_str.upper())
                    return col_names, col_types
                except Exception as e:  # pragma: no cover
                    logger.debug(f"Could not introspect Delta schema: {e}")  # pragma: no cover
                    return [], {}  # pragma: no cover

            elif backend == "duckdb":
                table_name = _resolve_table_name(contract)  # pragma: no cover
                if not table_name:  # pragma: no cover
                    return [], {}  # pragma: no cover
                try:  # pragma: no cover
                    import duckdb  # pragma: no cover

                    # pragma: no cover
                    con = duckdb.connect(database=":memory:")  # pragma: no cover
                    result = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()  # pragma: no cover
                    con.close()  # pragma: no cover
                    col_names = [row[1] for row in result]  # pragma: no cover
                    col_types = {row[1]: row[2] for row in result}  # pragma: no cover
                    return col_names, col_types  # pragma: no cover
                except Exception:  # pragma: no cover
                    return [], {}  # pragma: no cover

            elif backend in ("spark", "databricks"):
                table_name = _resolve_table_name(contract)
                if not table_name:
                    return [], {}  # pragma: no cover
                try:
                    from pyspark.sql import SparkSession

                    spark = SparkSession.builder.getOrCreate()
                    df = spark.table(table_name)
                    col_names = df.columns
                    col_types = {f.name: f.dataType.simpleString().upper() for f in df.schema.fields}
                    return col_names, col_types
                except Exception:  # pragma: no cover
                    return [], {}  # pragma: no cover
        # pragma: no cover
        except Exception as e:  # pragma: no cover
            logger.debug(f"Schema introspection skipped for {backend}: {e}")  # pragma: no cover
        # pragma: no cover
        return [], {}  # pragma: no cover

    @staticmethod
    def _execute_alter_statements(statements: List[str], backend: str, entity: str) -> None:
        """Execute ALTER TABLE statements for schema evolution.

        For dataframe engines (polars/pandas), ALTER statements are logged
        but not executed — Delta handles schema merge on write.
        """
        if backend in ("polars", "pandas", "python", "duckdb"):
            for stmt in statements:
                logger.info(
                    f"Schema evolution ({entity}): {stmt} "
                    f"(will be applied on next Delta write with schema_mode='merge')"
                )
            return

        if backend in ("spark", "databricks"):
            try:
                from pyspark.sql import SparkSession

                spark = SparkSession.builder.getOrCreate()
                for stmt in statements:
                    spark.sql(stmt)
                    logger.info(f"Executed: {stmt}")
            except Exception as e:  # pragma: no cover
                logger.warning(f"Spark ALTER failed for {entity}: {e}")  # pragma: no cover
            return

        # For other backends, log the statements for manual application
        for stmt in statements:
            logger.info(f"Schema evolution ({entity}): {stmt} (manual execution required for {backend})")

    # ── Phase 3: Compliance & Privacy ────────────────────────────────────────

    def _resolve_erasure_strategy(self, contract_dict: Optional[Dict[str, Any]], fallback: str) -> str:
        """Resolve the effective erasure strategy for a contract.

        Resolution order (first non-None wins):
          1. Contract-level: ``contract_dict.compliance.erasure.strategy``
          2. Registry-level: ``self.registry.compliance.erasure.strategy``
          3. Fallback parameter (from CLI or method default)
        """
        # 1. Contract-level override
        if contract_dict:
            c_strategy = (contract_dict.get("compliance") or {}).get("erasure", {}).get("strategy")
            if c_strategy:
                return c_strategy

        # 2. Registry-level (domain / system)
        reg_compliance = getattr(self.registry, "compliance", None) or {}
        r_strategy = (reg_compliance.get("erasure") or {}).get("strategy")
        if r_strategy:
            return r_strategy

        # 3. Fallback
        return fallback  # pragma: no cover

    def _execute_gdpr_pass(
        self,
        active_contracts: List[RegistryContract],
        subject_col: str,
        subject_ids: List[str],
        strategy: str,
        salt: str,
        dry_run: bool,
        partition_filter: Optional[Dict[str, str]] = None,
    ):
        """GDPR Right to be Forgotten target masking."""
        from lakelogic.core.gdpr import _get_pii_column_names, generate_erasure_report

        partition_msg = ""
        if partition_filter:
            partition_msg = (
                f" [partition: {partition_filter['column']}='{partition_filter['value']}']"  # pragma: no cover
            )
        logger.info(
            f"GDPR Erasure Pass: {len(subject_ids)} subjects on '{subject_col}' (Strategy: {strategy}){partition_msg}"
        )

        for c in active_contracts:
            dc = DataContract(**(c.contract_dict or {}))
            pii_cols = _get_pii_column_names(dc)
            if not pii_cols:
                continue  # pragma: no cover

            has_target = any(f.name == subject_col for f in (dc.model.fields if dc.model else []))
            if not has_target:
                continue  # pragma: no cover

            # Resolve per-contract effective strategy (contract → registry → CLI param)
            effective_strategy = self._resolve_erasure_strategy(c.contract_dict, strategy)

            if effective_strategy != strategy:
                logger.info(
                    f"  [{c.entity}] Using compliance config strategy '{effective_strategy}' "
                    f"(overrides default '{strategy}')"
                )

            # Resolve the contract's storage target. Order of precedence:
            #  1. Explicit `materialization.target_path` / `materialization.path`
            #     in the contract (covers Spark UC `table:` targets)
            #  2. `_resolve_target` on the DataContract (covers contracts that
            #     declare an explicit Server path)
            #  3. `{layer_path}/{info.table_name}` from the registry's storage
            #     block — the common case for path-based delta contracts that
            #     inherit their location from the system YAML's bronze_path /
            #     silver_path / gold_path placeholders
            mat_cfg = (c.contract_dict or {}).get("materialization", {})
            mat_target = mat_cfg.get("target_path", "") or mat_cfg.get("path", "")
            if not mat_target:
                try:
                    from lakelogic.core.materialization import _resolve_target as _mat_resolve

                    resolved_target, _ = _mat_resolve(dc)
                    if resolved_target is not None:
                        mat_target = str(resolved_target)
                except Exception:
                    pass
            if not mat_target:
                mat_target = self._infer_storage_path_from_registry(c, dc)

            if mat_target.startswith("table:") and self.spark:
                table_name = mat_target[len("table:") :]
                sql_vals = ", ".join([f"'{str(v).replace(chr(39), chr(39) * 2)}'" for v in subject_ids])

                set_clauses = []
                for col in pii_cols:
                    if col == subject_col and effective_strategy == "nullify":
                        continue  # pragma: no cover
                    if effective_strategy == "nullify":
                        set_clauses.append(f"`{col}` = NULL")  # pragma: no cover
                    elif effective_strategy == "redact":
                        set_clauses.append(f"`{col}` = '***REDACTED***'")  # pragma: no cover
                    elif effective_strategy == "hash":
                        set_clauses.append(f"`{col}` = hex(sha2(concat('{salt}', `{col}`), 256))")

                if not set_clauses:
                    continue  # pragma: no cover

                update_sql = f"UPDATE {table_name} SET {','.join(set_clauses)} WHERE `{subject_col}` IN ({sql_vals})"

                # Scope to partition if specified (multi-region safeguard)
                if partition_filter:
                    pcol = partition_filter["column"].replace("`", "")  # pragma: no cover
                    pval = partition_filter["value"].replace("'", "''")  # pragma: no cover
                    update_sql += f" AND `{pcol}` = '{pval}'"  # pragma: no cover

                affected = 0
                if dry_run:
                    logger.info(f"DRY RUN GDPR SQL: {update_sql}")
                    affected = len(subject_ids)
                else:
                    try:  # pragma: no cover
                        res = self.spark.sql(update_sql)  # pragma: no cover
                        affected = res.collect()[0]["num_affected_rows"]  # pragma: no cover
                        logger.info(f"GDPR Update: {affected} rows in {table_name}")  # pragma: no cover
                    except Exception as e:  # pragma: no cover
                        logger.error(f"GDPR Update failed on {table_name}: {e}")  # pragma: no cover

                if affected > 0 or dry_run:
                    report = generate_erasure_report(
                        dc, subject_col, subject_ids, effective_strategy, affected, partition_filter=partition_filter
                    )
                    report["pipeline_run_id"] = self.run_id

                    log_dir = "/Workspace/Shared/lakelogic_logs/gdpr_reports"
                    os.makedirs(log_dir, exist_ok=True)
                    log_path = f"{log_dir}/erasure_{c.entity}_{int(time.time())}.json"
                    with open(log_path, "w") as f:
                        json.dump(report, f, indent=2)

                    try:
                        RemoteObserver().report(report)
                    except Exception:  # pragma: no cover
                        pass  # Fail silently  # pragma: no cover
            else:
                # Path-based Delta target (local lakehouse_polars/ or any
                # filesystem URL). Read → transform PII columns → overwrite.
                # Spark is not required; we use polars + deltalake directly so
                # the erasure works in any environment that can already write
                # the table (i.e. anywhere the rest of the pipeline runs).
                affected = self._apply_path_based_erasure(
                    mat_target=mat_target,
                    pii_cols=pii_cols,
                    subject_col=subject_col,
                    subject_ids=subject_ids,
                    strategy=effective_strategy,
                    salt=salt,
                    dry_run=dry_run,
                    partition_filter=partition_filter,
                    entity_label=f"[{c.layer}] {c.entity}",
                    contract=dc,
                    kind="gdpr",
                )

                if affected > 0 or dry_run:
                    report = generate_erasure_report(
                        dc, subject_col, subject_ids, effective_strategy, affected, partition_filter=partition_filter
                    )
                    report["pipeline_run_id"] = self.run_id

                    # Write the audit JSON next to the run logs so it lands
                    # somewhere predictable on local disks too — the legacy
                    # /Workspace/... path only exists on Databricks.
                    try:
                        from pathlib import Path as _Path

                        log_dir = _Path("./logs/gdpr_reports")
                        log_dir.mkdir(parents=True, exist_ok=True)
                        log_path = log_dir / f"erasure_{c.entity}_{int(time.time())}.json"
                        with open(log_path, "w", encoding="utf-8") as f:
                            json.dump(report, f, indent=2, default=str)
                        logger.info(f"GDPR audit report written: {log_path}")
                    except Exception as exc:
                        logger.warning(f"Could not write GDPR audit report: {exc}")

                    try:
                        RemoteObserver().report(report)
                    except Exception:
                        pass

    def _infer_storage_path_from_registry(self, registry_contract, dc) -> str:
        """Compute a contract's storage path from `{layer_path}/{table_name}`.

        Contracts that don't declare an explicit `materialization.target_path`
        inherit their location from the system YAML's storage block. Each
        layer has its own root (`bronze_path`, `silver_path`, `gold_path`)
        which the registry has already resolved to an absolute or
        CWD-relative path. The table name comes from `info.table_name` (also
        registry-resolved, with `{system}` / `{bronze_layer}` / etc. expanded).

        Returns the empty string if we can't determine a path — the caller
        treats that as "not found" and skips erasure with a warning.
        """
        try:
            layer = (registry_contract.layer or "").lower()
            storage = getattr(self.registry, "storage", None)
            if not storage:
                return ""
            layer_root_attr = f"{layer}_path"
            layer_root = getattr(storage, layer_root_attr, None)
            if not layer_root:
                return ""
            info = getattr(dc, "info", None)
            table_name = getattr(info, "table_name", None) if info else None
            if not table_name:
                return ""
            # `layer_root` and `table_name` are already placeholder-resolved
            # by the registry — just join.
            from pathlib import Path as _Path

            return str(_Path(str(layer_root)) / str(table_name))
        except Exception:
            return ""

    def _apply_path_based_erasure(
        self,
        mat_target: str,
        pii_cols: List[str],
        subject_col: str,
        subject_ids: List[Any],
        strategy: str,
        salt: str,
        dry_run: bool,
        partition_filter: Optional[Dict[str, str]],
        entity_label: str,
        *,
        contract=None,
        kind: str = "gdpr",
    ) -> int:
        """Apply right-to-delete erasure to a path-based Delta table.

        Loads the table → delegates the actual PII/PHI transformation to the
        battle-tested helpers in ``core/gdpr.py`` (``forget_subjects``) and
        ``core/hipaa.py`` (``forget_patients``) → writes the result back via
        Delta overwrite. The helpers handle multiple dataframe backends
        (polars / pandas / pyspark / duckdb), set the compliance-metadata
        columns (``_is_deleted`` / ``_deleted_at`` / ``_delete_reason`` /
        ``_updated_at``), apply per-field strategy overrides from the
        contract's ``compliance`` block, and emit a structured run-log
        audit record — none of which the previous inline implementation did.

        Args:
            mat_target: Resolved storage path of the Delta table.
            pii_cols: PII/PHI column names (logged only — the helper
                re-derives them from contract ``pii: true`` / ``phi: true``
                annotations, so they always match what the contract defines).
            subject_col: Column carrying the subject identifier.
            subject_ids: Subjects to forget.
            strategy: ``nullify`` | ``hash`` | ``redact``.
            salt: Salt used when ``strategy='hash'``.
            dry_run: When True, count matches and log but don't write.
            partition_filter: Optional ``{column, value}`` partition scope.
            entity_label: Log label like ``[silver] silver_rideflow_driver_profiles``.
            contract: The pydantic DataContract — required by ``forget_*``
                so it can locate ``pii`` / ``phi`` fields and write the
                audit record. Passed by the caller (GDPR/HIPAA pass).
            kind: ``gdpr`` (default) routes to ``forget_subjects``;
                ``hipaa`` routes to ``forget_patients``.

        Returns the number of rows whose data was modified.
        """
        if not mat_target:
            logger.warning(f"{entity_label}: empty materialization target; skipping erasure")
            return 0

        try:
            import polars as pl
            from deltalake import DeltaTable, write_deltalake
        except ImportError as exc:
            logger.error(f"{entity_label}: path-based erasure requires polars + deltalake — {exc}")
            return 0

        from pathlib import Path as _Path

        target_path = _Path(mat_target)
        # Contract paths are registry-resolved but may still be CWD-relative
        # (e.g. ``./lakehouse_polars/...``). resolve() makes them absolute so
        # the Delta reader works regardless of the caller's CWD.
        try:
            target_path = target_path.resolve()
        except Exception:
            pass

        if not target_path.exists():
            logger.warning(f"{entity_label}: target path does not exist — skipping ({target_path})")
            return 0

        try:
            dt = DeltaTable(str(target_path))
        except Exception as exc:
            logger.warning(f"{entity_label}: not a Delta table or unreadable — skipping ({exc})")
            return 0

        df = pl.from_arrow(dt.to_pyarrow_table())

        # Count matching rows up-front so we can log + return a meaningful
        # number even when the helper internally sets metadata for 0
        # "modified" rows (e.g. when only the subject_col is PII).
        match_expr = pl.col(subject_col).is_in(subject_ids)
        if partition_filter:
            pcol = partition_filter["column"]
            pval = partition_filter["value"]
            if pcol in df.columns:
                match_expr = match_expr & (pl.col(pcol).cast(pl.Utf8) == str(pval))
        matched = df.filter(match_expr).height
        if matched == 0:
            logger.info(f"{entity_label}: 0 rows matched subject filter (strategy={strategy})")
            return 0

        if dry_run:
            logger.info(
                f"{entity_label}: DRY RUN — would erase {matched} row(s) on cols {pii_cols} (strategy={strategy})"
            )
            return matched

        # Delegate to the canonical helper. It dispatches to the right
        # backend (polars here), respects ``compliance.strategy_per_field``
        # overrides, stamps the four metadata columns on affected rows, and
        # writes a run-log audit entry.
        if contract is None:
            logger.warning(
                f"{entity_label}: contract not passed to _apply_path_based_erasure; "
                "falling back to the in-line transform — compliance metadata columns "
                "and the run-log audit record will NOT be set."
            )
            erased = df
        else:
            if kind == "hipaa":
                from lakelogic.core.hipaa import forget_patients

                erased = forget_patients(
                    df,
                    contract,
                    patient_column=subject_col,
                    patient_ids=subject_ids,
                    erasure_strategy=strategy,
                    hash_salt=salt,
                    audit=True,
                    partition_filter=partition_filter,
                )
            else:
                from lakelogic.core.gdpr import forget_subjects

                erased = forget_subjects(
                    df,
                    contract,
                    subject_column=subject_col,
                    subject_ids=subject_ids,
                    erasure_strategy=strategy,
                    hash_salt=salt,
                    audit=True,
                    partition_filter=partition_filter,
                )

        # Overwrite the Delta table with the erased frame. schema_mode="overwrite"
        # is required because forget_subjects/forget_patients adds four compliance
        # metadata columns (_is_deleted/_deleted_at/_delete_reason/_updated_at)
        # if they don't already exist — without schema evolution deltalake would
        # silently drop them and the audit trail would be lost.
        write_deltalake(
            str(target_path),
            erased.to_arrow(),
            mode="overwrite",
            schema_mode="overwrite",
        )
        logger.info(
            f"{entity_label}: erased {matched} row(s) on cols {pii_cols} (strategy={strategy}, target={target_path})"
        )
        return matched

    def _execute_hipaa_pass(
        self,
        active_contracts: List[RegistryContract],
        patient_col: str,
        patient_ids: List[str],
        strategy: str,
        salt: str,
        dry_run: bool,
        partition_filter: Optional[Dict[str, str]] = None,
    ):
        """HIPAA Safe Harbor PHI masking."""
        from lakelogic.core.hipaa import _get_phi_column_names, generate_hipaa_erasure_report

        partition_msg = ""
        if partition_filter:
            partition_msg = (
                f" [partition: {partition_filter['column']}='{partition_filter['value']}']"  # pragma: no cover
            )
        logger.info(
            f"HIPAA Erasure Pass: {len(patient_ids)} patients on '{patient_col}' (Strategy: {strategy}){partition_msg}"
        )

        for c in active_contracts:
            dc = DataContract(**(c.contract_dict or {}))
            phi_cols = _get_phi_column_names(dc)
            if not phi_cols:
                continue  # pragma: no cover

            has_target = any(f.name == patient_col for f in (dc.model.fields if dc.model else []))
            if not has_target:
                continue  # pragma: no cover

            # Resolve per-contract effective strategy (contract → registry → CLI param)
            effective_strategy = self._resolve_erasure_strategy(c.contract_dict, strategy)

            if effective_strategy != strategy:
                logger.info(
                    f"  [{c.entity}] Using compliance config strategy '{effective_strategy}' "
                    f"(overrides default '{strategy}')"
                )

            # Resolve the contract's storage target via the same code path
            # the pipeline uses for writes. Contracts often don't carry an
            # explicit materialization.target_path — they inherit it from
            # the registry's storage block (`{silver_path}/...`). Reading
            # the raw dict misses those; _resolve_target picks them up.
            mat_cfg = (c.contract_dict or {}).get("materialization", {})
            mat_target = mat_cfg.get("target_path", "") or mat_cfg.get("path", "")
            if not mat_target:
                try:
                    from lakelogic.core.materialization import _resolve_target as _mat_resolve

                    resolved_target, _ = _mat_resolve(dc)
                    if resolved_target is not None:
                        mat_target = str(resolved_target)
                except Exception:
                    pass

            if mat_target.startswith("table:") and self.spark:
                table_name = mat_target[len("table:") :]
                sql_vals = ", ".join([f"'{str(v).replace(chr(39), chr(39) * 2)}'" for v in patient_ids])

                set_clauses = []
                for col in phi_cols:
                    if col == patient_col and effective_strategy == "nullify":
                        continue  # pragma: no cover
                    if effective_strategy == "nullify":
                        set_clauses.append(f"`{col}` = NULL")  # pragma: no cover
                    elif effective_strategy == "redact":
                        set_clauses.append(f"`{col}` = '***REDACTED_PHI***'")  # pragma: no cover
                    elif effective_strategy == "hash":
                        set_clauses.append(f"`{col}` = hex(sha2(concat('{salt}', `{col}`), 256))")

                if not set_clauses:
                    continue  # pragma: no cover

                update_sql = f"UPDATE {table_name} SET {','.join(set_clauses)} WHERE `{patient_col}` IN ({sql_vals})"

                # Scope to partition if specified (multi-region safeguard)
                if partition_filter:
                    pcol = partition_filter["column"].replace("`", "")  # pragma: no cover
                    pval = partition_filter["value"].replace("'", "''")  # pragma: no cover
                    update_sql += f" AND `{pcol}` = '{pval}'"  # pragma: no cover

                affected = 0
                if dry_run:
                    logger.info(f"DRY RUN HIPAA SQL: {update_sql}")  # pragma: no cover
                    affected = len(patient_ids)  # pragma: no cover
                else:
                    try:
                        res = self.spark.sql(update_sql)
                        affected = res.collect()[0]["num_affected_rows"]
                        logger.info(f"HIPAA Update: {affected} rows in {table_name}")
                    except Exception as e:  # pragma: no cover
                        logger.error(f"HIPAA Update failed on {table_name}: {e}")  # pragma: no cover

                if affected > 0 or dry_run:
                    report = generate_hipaa_erasure_report(
                        dc, patient_col, patient_ids, effective_strategy, affected, partition_filter=partition_filter
                    )
                    report["pipeline_run_id"] = self.run_id

                    log_dir = "/Workspace/Shared/lakelogic_logs/hipaa_reports"
                    os.makedirs(log_dir, exist_ok=True)
                    log_path = f"{log_dir}/erasure_{c.entity}_{int(time.time())}.json"
                    with open(log_path, "w") as f:
                        json.dump(report, f, indent=2)

                    try:
                        RemoteObserver().report(report)
                    except Exception:  # pragma: no cover
                        pass  # Fail silently  # pragma: no cover
            else:
                # Path-based Delta target — same separation of concerns as the
                # GDPR pass. Delegates the actual PHI transformation to
                # core/hipaa.py:forget_patients, which handles all 4 dataframe
                # backends and stamps the compliance metadata columns.
                affected = self._apply_path_based_erasure(
                    mat_target=mat_target,
                    pii_cols=phi_cols,
                    subject_col=patient_col,
                    subject_ids=patient_ids,
                    strategy=effective_strategy,
                    salt=salt,
                    dry_run=dry_run,
                    partition_filter=partition_filter,
                    entity_label=f"[{c.layer}] {c.entity}",
                    contract=dc,
                    kind="hipaa",
                )

                if affected > 0 or dry_run:
                    report = generate_hipaa_erasure_report(
                        dc, patient_col, patient_ids, effective_strategy, affected, partition_filter=partition_filter
                    )
                    report["pipeline_run_id"] = self.run_id
                    try:
                        from pathlib import Path as _Path

                        log_dir = _Path("./logs/hipaa_reports")
                        log_dir.mkdir(parents=True, exist_ok=True)
                        log_path = log_dir / f"erasure_{c.entity}_{int(time.time())}.json"
                        with open(log_path, "w", encoding="utf-8") as f:
                            json.dump(report, f, indent=2, default=str)
                        logger.info(f"HIPAA audit report written: {log_path}")
                    except Exception as exc:
                        logger.warning(f"Could not write HIPAA audit report: {exc}")
                    try:
                        RemoteObserver().report(report)
                    except Exception:
                        pass

    # ── Phase 4: Main Medallion Loop ─────────────────────────────────────────

    def run(
        self,
        *,
        target_layers: str = "bronze,silver,gold",
        reset_layers: str = "",
        reload_layers: str = "",
        dry_run: bool = False,
        entity_filter: str = "",
        # Reprocessing
        reprocess_from: Optional[str] = None,
        reprocess_to: Optional[str] = None,
        reprocess_column: Optional[str] = None,
        reprocess_values: Optional[List[str]] = None,
        lookback_days: Optional[int] = None,
        run_log_mode: Optional[str] = None,
        # Retry / resilience
        retry_attempts: int = 1,
        retry_base_wait_seconds: int = 30,
        entity_timeout_minutes: Optional[int] = None,
        max_consecutive_failures: int = 0,
        resume_from_run: Optional[str] = None,
        # Parallel execution
        parallel: bool = False,
        max_workers: int = 4,
        # Test data generation
        generate_test_data: bool = False,
        test_data_rows: int = 500,
        test_data_invalid_ratio: float = 0.05,
        test_data_ai: bool = False,
        test_data_ai_provider: Optional[str] = None,
        test_data_ai_model: Optional[str] = None,
        test_data_ai_api_key: Optional[str] = None,
        # GDPR / HIPAA
        forget_column: str = "",
        forget_values: List[str] = None,
        forget_strategy: str = "nullify",
        forget_salt: str = "",
        forget_partition_column: str = "",
        forget_partition_value: str = "",
        forget_patient_column: str = "",
        forget_patient_ids: List[str] = None,
        forget_patient_strategy: str = "nullify",
        forget_patient_salt: str = "",
        forget_patient_partition_column: str = "",
        forget_patient_partition_value: str = "",
        ddl_only: bool = False,
        environment: str = "",
        debug_mode: bool = False,
        # Lineage
        created_by: Optional[str] = None,
    ) -> PipelineRunSummary:
        """
        Execute the pipeline loop.
        """
        forget_values = forget_values or []
        forget_patient_ids = forget_patient_ids or []
        self._created_by_override = created_by

        # ── Checkpointing: load succeeded entities from a previous run ──
        _checkpoint_succeeded: Set[str] = set()
        if resume_from_run:
            _checkpoint_succeeded = self._load_checkpoint(resume_from_run)
            if _checkpoint_succeeded:
                logger.info(
                    f"🔄 Resuming from run {resume_from_run}: "
                    f"skipping {len(_checkpoint_succeeded)} already-succeeded entities "
                    f"({', '.join(sorted(_checkpoint_succeeded))})"
                )

        # ── Circuit breaker state ──
        _consecutive_failures = 0

        targets = [layer.strip().lower() for layer in target_layers.split(",") if layer.strip()]
        layer_order = ["bronze", "silver", "gold"]
        target_set = set(layer_order) if "all" in targets else set(targets)

        # Custom log format: includes [entity] tag when set via contextualize (parallel mode)
        def _log_format(record):
            entity = record["extra"].get("entity", "")  # pragma: no cover
            tag = f"[{entity}] " if entity else ""  # pragma: no cover
            return (  # pragma: no cover
                "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
                f"{tag}"
                "<level>{message}</level>\n{exception}"
            )

        # Set log level based on debug_mode
        import sys

        if debug_mode:
            logger.info("Debug mode enabled — verbose logging active")  # pragma: no cover
            logger.remove()  # pragma: no cover
            logger.add(sys.stderr, format=_log_format, level="DEBUG")  # pragma: no cover
        elif parallel:
            # In parallel mode, reconfigure format to include entity tags
            logger.remove()  # pragma: no cover
            logger.add(sys.stderr, format=_log_format, level="INFO")  # pragma: no cover

        logger.info(f"Pipeline storage mode: {self.storage_mode}")

        # ── 0. Native System Table Bootstrap (Unity Catalog) ──────────────────
        # Uses the same pattern as generate_ddl: CREATE TABLE with an explicit
        # schema + LOCATION, so tables are always created with or without data.
        # Schemas are defined centrally in core.constants to stay in sync with
        # run_log.py and the engine adapters.
        if (
            self.engine in ("spark", "databricks")
            and self.spark
            and getattr(self.registry.storage, "domain_catalog", None)
        ):
            from lakelogic.core.constants import (
                SYSTEM_TABLE_SCHEMA_LOGS,
                SYSTEM_TABLE_SCHEMA_QUARANTINE,
            )

            catalog_schema = self.registry.storage.domain_catalog

            # When quarantine_root is configured, per-contract quarantine tables
            # are written to a dedicated UC schema (e.g. `catalog.quarantine`).
            # A system-level _quarantine table at quarantine_path would overlap
            # with those per-entity tables, causing LOCATION_OVERLAP errors.
            # Only bootstrap _quarantine when there is NO quarantine_root.
            has_quarantine_root = bool(getattr(self.registry.storage, "quarantine_root", None))

            sys_tables = {
                "_logs": (getattr(self.registry.storage, "log_path", None), SYSTEM_TABLE_SCHEMA_LOGS),
            }
            if not has_quarantine_root:
                sys_tables["_quarantine"] = (
                    getattr(self.registry.storage, "quarantine_path", None),
                    SYSTEM_TABLE_SCHEMA_QUARANTINE,
                )

            for tbl, (path, schema_def) in sys_tables.items():
                if not path:
                    continue
                if not str(path).startswith("table:"):
                    ddl = (
                        f"CREATE TABLE IF NOT EXISTS {catalog_schema}.{tbl} (\n"
                        f"{schema_def}\n"
                        f")\n"
                        f"USING DELTA\n"
                        f"LOCATION '{path}'"
                    )
                    if dry_run or ddl_only:
                        logger.info(f"DRY RUN System DDL Preview for {tbl}:\n{ddl}")
                    if not dry_run:
                        try:
                            self.spark.sql(ddl)
                            if not ddl_only:
                                logger.debug(f"Ensured Unity Catalog materialization for {catalog_schema}.{tbl}")
                        except Exception as e:
                            logger.warning(f"Could not bootstrap system table {tbl}: {e}")

        resets = {layer.strip().lower() for layer in reset_layers.split(",") if layer.strip()}
        reloads = {layer.strip().lower() for layer in reload_layers.split(",") if layer.strip()}
        entities = {entity.strip().lower() for entity in entity_filter.split(",") if entity.strip()}

        # Filter active contracts
        all_active = self.registry.get_active_contracts()
        if entities:
            all_active = [c for c in all_active if c.entity.lower() in entities]  # pragma: no cover

        summary = PipelineRunSummary(self.run_id, environment or "unknown", dry_run)

        # 1. Resolve UC paths for ALL contracts before any processing/validation
        for c in all_active:
            if c.contract_dict:
                c.contract_dict = copy.deepcopy(c.contract_dict)
                # Inject pipeline-level environment into contract metadata
                # so DataProcessor can resolve it consistently for notifications & run logs
                if environment:
                    c.contract_dict.setdefault("metadata", {})["environment"] = environment
                self._resolve_uc_paths(c.contract_dict)

        # 2. Resets
        if resets or reloads:
            self._execute_resets(all_active, resets, reloads, dry_run)

        # 2. Privacy Passes
        if forget_column and forget_values:
            gdpr_partition = (
                {"column": forget_partition_column, "value": forget_partition_value}
                if forget_partition_column
                else None
            )
            self._execute_gdpr_pass(
                all_active,
                forget_column,
                forget_values,
                forget_strategy,
                forget_salt,
                dry_run,
                partition_filter=gdpr_partition,
            )

        if forget_patient_column and forget_patient_ids:
            hipaa_partition = (
                {"column": forget_patient_partition_column, "value": forget_patient_partition_value}
                if forget_patient_partition_column
                else None
            )
            self._execute_hipaa_pass(
                all_active,
                forget_patient_column,
                forget_patient_ids,
                forget_patient_strategy,
                forget_patient_salt,
                dry_run,
                partition_filter=hipaa_partition,
            )

        # 3. DDL Only
        if ddl_only:
            layer_filtered = [c for c in all_active if c.layer in target_set]
            return self.generate_ddl_only(layer_filtered, dry_run)

        # 4. Test Data Generation (bronze landing zone)
        if generate_test_data:
            bronze_contracts = [c for c in all_active if c.layer == "bronze"]
            if bronze_contracts:
                logger.info("── Generating Test Data ──")
                self._generate_test_data(
                    bronze_contracts,
                    rows=test_data_rows,
                    invalid_ratio=test_data_invalid_ratio,
                    ai=test_data_ai,
                    ai_provider=test_data_ai_provider,
                    ai_model=test_data_ai_model,
                    ai_api_key=test_data_ai_api_key,
                )

        # 5. Processing Loop
        layers_with_new_data = set()
        upstream_map = {"silver": "bronze", "gold": "silver"}

        for layer in layer_order:
            if layer not in target_set:
                continue

            layer_contracts = [c for c in all_active if c.layer == layer]
            if not layer_contracts:
                continue  # pragma: no cover

            # Order by dependencies
            try:
                layer_contracts = self._topological_sort(layer_contracts)
            except ValueError as e:  # pragma: no cover
                logger.error(f"Dependency error in {layer}: {e}")  # pragma: no cover
                for c in layer_contracts:  # pragma: no cover
                    summary.append(c.entity, layer, "failed", error=str(e))  # pragma: no cover
                continue  # pragma: no cover

            logger.info(f"── Processing Layer: {layer.upper()} ({len(layer_contracts)} contracts) ──")

            # Skip downstream if upstream had no new data entirely
            upstream = upstream_map.get(layer)
            if upstream and upstream not in layers_with_new_data and upstream in target_set:
                logger.info(f"Upstream '{upstream}' had no new data — skipping {layer}")
                for c in layer_contracts:
                    summary.append(c.entity, layer, "skipped_no_upstream")
                continue

            # Group into dependency waves for parallel execution
            if parallel and not dry_run:
                waves = self._group_by_dependency_level(layer_contracts)  # pragma: no cover
            else:
                # Sequential: each contract is its own wave
                waves = [[c] for c in layer_contracts]

            for wave_idx, wave in enumerate(waves):
                if parallel and len(wave) > 1:
                    logger.info(
                        f"  Wave {wave_idx}: [{', '.join(c.entity for c in wave)}] (parallel)"
                    )  # pragma: no cover
                    self._execute_wave_parallel(  # pragma: no cover
                        wave,
                        layer,
                        summary,
                        dry_run,
                        run_log_mode,
                        reprocess_from,
                        reprocess_to,
                        reprocess_column,
                        reprocess_values,
                        lookback_days,
                        layers_with_new_data,
                        max_workers,
                        retry_attempts=retry_attempts,
                        retry_base_wait_seconds=retry_base_wait_seconds,
                        entity_timeout_minutes=entity_timeout_minutes,
                    )
                else:
                    for c in wave:
                        # ── Checkpointing: skip already-succeeded entities ──
                        _ck_key = f"{layer}:{c.entity}"
                        if _ck_key in _checkpoint_succeeded:
                            logger.info(
                                f"  ⏭️ Skipping {c.entity} [{layer}] — already succeeded in run {resume_from_run}"
                            )
                            summary.append(c.entity, layer, "skipped_checkpoint")
                            continue

                        # ── Circuit breaker: abort if too many consecutive failures ──
                        if max_consecutive_failures > 0 and _consecutive_failures >= max_consecutive_failures:
                            logger.error(
                                f"🔴 Circuit breaker tripped: {_consecutive_failures} consecutive failures. "
                                f"Skipping remaining entities to avoid wasted retries."
                            )
                            summary.append(c.entity, layer, "skipped_circuit_breaker")
                            continue

                        try:
                            self._process_contract_with_retry(
                                c,
                                layer,
                                summary,
                                dry_run,
                                run_log_mode,
                                reprocess_from,
                                reprocess_to,
                                reprocess_column,
                                reprocess_values,
                                lookback_days,
                                layers_with_new_data,
                                retry_attempts=retry_attempts,
                                retry_base_wait_seconds=retry_base_wait_seconds,
                                entity_timeout_minutes=entity_timeout_minutes,
                            )
                            _consecutive_failures = 0  # Reset on success
                        except Exception as e:
                            _consecutive_failures += 1
                            if type(e).__name__ == "EntityTimeoutError":
                                logger.error(  # pragma: no cover
                                    f"❌ {c.entity} [{layer}] timed out after {entity_timeout_minutes} minutes."  # pragma: no cover # noqa: E501
                                )  # pragma: no cover
                                summary.append(c.entity, layer, "timeout")  # pragma: no cover
                            else:
                                # Catch-all for any other wrapper errors not logged by _process_single_contract
                                logger.error(f"❌ {c.entity} [{layer}] failed unexpectedly: {e}")
                                # Only append if not already in summary to avoid duplicate failure entries
                                if not any(
                                    r.get("contract") == c.entity and r.get("layer") == layer for r in summary.results
                                ):
                                    summary.append(c.entity, layer, "failed")

        # ── 6. Restatement impact (advisory) ──────────────────────────────────
        # Only for runs that restate already-materialized data. Never blocks,
        # never changes exit status, never alters what executed.
        self._attach_restatement_impact(
            summary,
            all_active,
            target_set,
            reprocess_from=reprocess_from,
            reprocess_to=reprocess_to,
            reprocess_column=reprocess_column,
            reprocess_values=reprocess_values,
        )
        return summary

    def _attach_restatement_impact(
        self,
        summary: PipelineRunSummary,
        all_active: List[RegistryContract],
        target_set: Set[str],
        *,
        reprocess_from: Optional[str] = None,
        reprocess_to: Optional[str] = None,
        reprocess_column: Optional[str] = None,
        reprocess_values: Optional[List[str]] = None,
    ) -> None:
        """Attach + log the restatement impact report, if this run restated data.

        Advisory only: every failure path here is swallowed. A broken impact
        report must never break a pipeline.
        """
        try:
            if not is_restatement_run(reprocess_from, reprocess_to, reprocess_column, reprocess_values):
                return

            restated = [
                (r["layer"], r["contract"]) for r in summary.results if r.get("status") == "success"
            ]
            in_run_scope = [(c.layer, c.entity) for c in all_active if c.layer in target_set]

            report = build_restatement_impact(all_active, restated, in_run_scope)
            summary.restatement_impact = report
            logger.info("\n" + format_restatement_impact(report))
        except Exception as e:  # pragma: no cover - defensive; advisory only
            logger.warning(f"Restatement impact report unavailable (run unaffected): {e}")

    # ── Checkpoint helpers ─────────────────────────────────────────────────────

    def _load_checkpoint(self, pipeline_run_id: str) -> Set[str]:
        """Load succeeded entity keys from a previous pipeline run.

        Returns a set of ``"layer:entity"`` strings that succeeded
        in the given run, so the current run can skip them.
        """
        succeeded: Set[str] = set()
        try:
            storage = self.registry.storage
            run_log_table = getattr(storage, "run_log_table", None)
            if not run_log_table:
                logger.warning("No run_log_table configured — cannot load checkpoint")  # pragma: no cover
                return succeeded  # pragma: no cover

            from lakelogic.core.paths import resolve_run_log_ref

            if self.engine == "spark" and self.spark:
                ref = resolve_run_log_ref(run_log_table, "spark")
                rows = self.spark.sql(f"""
                    SELECT data_layer, dataset, stage
                    FROM {ref}
                    WHERE pipeline_run_id = '{pipeline_run_id}'
                      AND stage = 'succeeded'
                """).collect()
                for r in rows:
                    succeeded.add(f"{r['data_layer']}:{r['dataset']}")
            else:
                # DuckDB or Polars fallback — read via polars
                import polars as pl

                from lakelogic.core.paths import enrich_azure_storage_options
                from lakelogic.engines.cloud_credentials import resolve_storage_options

                storage_opts = enrich_azure_storage_options(resolve_storage_options(run_log_table))
                try:
                    df = pl.read_delta(run_log_table, storage_options=storage_opts)
                except Exception:
                    df = pl.read_parquet(run_log_table, storage_options=storage_opts)

                filtered = df.filter((pl.col("pipeline_run_id") == pipeline_run_id) & (pl.col("stage") == "succeeded"))
                for row in filtered.to_dicts():
                    succeeded.add(f"{row.get('data_layer', '')}:{row.get('dataset', '')}")

            logger.debug(f"Checkpoint loaded: {len(succeeded)} succeeded entities from run {pipeline_run_id}")
        except Exception as e:  # pragma: no cover
            logger.warning(f"Failed to load checkpoint from run {pipeline_run_id}: {e}")  # pragma: no cover

        return succeeded

    # ── Contract execution helpers ────────────────────────────────────────────

    def _process_contract_with_retry(
        self,
        c,
        layer: str,
        summary: PipelineRunSummary,
        dry_run: bool,
        run_log_mode: Optional[str],
        reprocess_from: Optional[str],
        reprocess_to: Optional[str],
        reprocess_column: Optional[str],
        reprocess_values: Optional[List[str]],
        lookback_days: Optional[int],
        layers_with_new_data: set,
        *,
        retry_attempts: int = 1,
        retry_base_wait_seconds: int = 30,
        entity_timeout_minutes: Optional[int] = None,
    ) -> None:
        """Wrap _process_single_contract with exponential-backoff retry and timeout.

        Delegates to :func:`lakelogic.core.retry.retry_call`.
        If ``entity_timeout_minutes`` is set, each attempt is time-boxed.
        """
        from lakelogic.core.retry import retry_call

        def _timed_process(*args):
            if entity_timeout_minutes and entity_timeout_minutes > 0:
                import threading

                entity_label = getattr(c, "entity", str(c))

                # Use a thread with join(timeout) — works on all platforms
                error_holder = [None]

                def _target():
                    try:  # pragma: no cover
                        self._process_single_contract(*args)  # pragma: no cover
                    except Exception as e:  # pragma: no cover
                        error_holder[0] = e  # pragma: no cover

                t = threading.Thread(target=_target, daemon=True)
                t.start()
                t.join(timeout=entity_timeout_minutes * 60)

                if t.is_alive():
                    raise EntityTimeoutError(f"{entity_label} exceeded timeout of {entity_timeout_minutes} minutes")
                if error_holder[0]:  # pragma: no cover
                    raise error_holder[0]  # pragma: no cover
            else:
                self._process_single_contract(*args)

        retry_call(
            _timed_process,
            args=(
                c,
                layer,
                summary,
                dry_run,
                run_log_mode,
                reprocess_from,
                reprocess_to,
                reprocess_column,
                reprocess_values,
                lookback_days,
                layers_with_new_data,
            ),
            attempts=retry_attempts,
            base_wait_seconds=retry_base_wait_seconds,
            label=getattr(c, "entity", str(c)),
        )

    # ── Post-ingestion landing zone cleanup ────────────────────────────────

    def _execute_post_ingestion_cleanup(self, c: RegistryContract, processor: Any) -> None:
        """Execute landing zone cleanup after a successful Bronze commit.

        Actions:
          delete  — remove source files from the landing zone
          archive — move source files to the archive path
          retain  — no-op (files stay in place)

        Safety:
          - Only runs AFTER a successful Bronze Delta commit.
          - If cleanup fails and cleanup_is_blocking is False (default),
            the pipeline continues with a warning.
          - If cleanup_is_blocking is True, a cleanup failure raises.
        """
        contract_dict = c.contract_dict or {}
        server = contract_dict.get("server") or {}

        # Resolve post_ingestion config:
        #   1. source.post_ingestion (contract-level override, highest precedence)
        #   2. server.post_ingestion (system-level default)
        source = contract_dict.get("source") or {}
        pi_config = source.get("post_ingestion") or server.get("post_ingestion") or {}
        action = pi_config.get("action", "retain")

        if action == "retain":
            return  # Nothing to do

        # Resolve the source landing path
        source = contract_dict.get("source") or {}  # pragma: no cover
        landing_path = source.get("path")  # pragma: no cover
        if not landing_path:  # pragma: no cover
            logger.debug(f"  No source.path for {c.entity} — skipping post-ingestion cleanup")  # pragma: no cover
            return  # pragma: no cover
        # pragma: no cover
        cleanup_is_blocking = pi_config.get("cleanup_is_blocking", False)  # pragma: no cover
        # pragma: no cover
        try:  # pragma: no cover
            if action == "delete":  # pragma: no cover
                self._cleanup_landing_files(landing_path, c.entity, mode="delete")  # pragma: no cover
            elif action == "archive":  # pragma: no cover
                # Resolve archive_path: config-level > storage-level  # pragma: no cover
                archive_path = pi_config.get("archive_path")  # pragma: no cover
                if not archive_path:  # pragma: no cover
                    storage = self.registry.storage if self.registry else None  # pragma: no cover
                    archive_path = getattr(storage, "archive_path", None) if storage else None  # pragma: no cover
                if not archive_path:  # pragma: no cover
                    logger.warning(  # pragma: no cover
                        f"  ⚠️ post_ingestion.action=archive for {c.entity} but no "  # pragma: no cover
                        f"archive_path configured — skipping archive"  # pragma: no cover
                    )  # pragma: no cover
                    return  # pragma: no cover
                self._cleanup_landing_files(
                    landing_path, c.entity, mode="archive", archive_path=archive_path
                )  # pragma: no cover
            else:  # pragma: no cover
                logger.warning(
                    f"  Unknown post_ingestion action '{action}' for {c.entity} — skipping"
                )  # pragma: no cover
                return  # pragma: no cover
            # pragma: no cover
            logger.info(f"  🧹 Post-ingestion: {action}d landing files for {c.entity}")  # pragma: no cover
        # pragma: no cover
        except Exception as cleanup_exc:  # pragma: no cover
            if cleanup_is_blocking:  # pragma: no cover
                raise RuntimeError(  # pragma: no cover
                    f"Post-ingestion cleanup ({action}) failed for {c.entity}: {cleanup_exc}"  # pragma: no cover
                ) from cleanup_exc  # pragma: no cover
            else:  # pragma: no cover
                logger.warning(  # pragma: no cover
                    f"  ⚠️ Post-ingestion cleanup ({action}) failed for {c.entity}: {cleanup_exc}. "
                    f"Pipeline continues (cleanup_is_blocking=false). "
                    f"Files remain in landing zone and will be retried on next run."
                )

    def _cleanup_landing_files(
        self, landing_path: str, entity: str, mode: str = "delete", archive_path: Optional[str] = None
    ) -> None:
        """Physically delete or move files from the landing zone.

        Supports three storage backends:
          1. Local filesystem (pathlib) — for local/colab environments
          2. Cloud storage (fsspec) — for Azure ADLS, S3, GCS
          3. Databricks dbutils — when running on Databricks clusters
        """
        _is_cloud = any(  # pragma: no cover
            landing_path.startswith(pfx)
            for pfx in ("abfss://", "abfs://", "s3://", "s3a://", "gs://", "gcs://")  # pragma: no cover
        )  # pragma: no cover
        _is_local = not _is_cloud  # pragma: no cover
        # pragma: no cover
        if _is_local:  # pragma: no cover
            self._cleanup_local(landing_path, entity, mode, archive_path)  # pragma: no cover
        else:  # pragma: no cover
            # Try dbutils first (Databricks), then fall back to fsspec  # pragma: no cover
            if self.spark and self._try_cleanup_dbutils(landing_path, entity, mode, archive_path):  # pragma: no cover
                return  # pragma: no cover
            self._cleanup_cloud(landing_path, entity, mode, archive_path)  # pragma: no cover

    def _cleanup_local(self, landing_path: str, entity: str, mode: str, archive_path: Optional[str]) -> None:
        """Clean up landing files on local filesystem."""
        import shutil  # pragma: no cover
        from pathlib import Path  # pragma: no cover

        # pragma: no cover
        src = Path(landing_path)  # pragma: no cover
        if not src.exists():  # pragma: no cover
            logger.debug(f"  Landing path {src} does not exist — nothing to clean up")  # pragma: no cover
            return  # pragma: no cover
        # pragma: no cover
        if mode == "delete":  # pragma: no cover
            if src.is_dir():  # pragma: no cover
                # Delete only the files, preserve the directory structure  # pragma: no cover
                for f in src.rglob("*"):  # pragma: no cover
                    if f.is_file():  # pragma: no cover
                        f.unlink()  # pragma: no cover
                logger.debug(f"  Deleted all files in {src}")  # pragma: no cover
            elif src.is_file():  # pragma: no cover
                src.unlink()  # pragma: no cover
                logger.debug(f"  Deleted file {src}")  # pragma: no cover
        elif mode == "archive" and archive_path:  # pragma: no cover
            dst = Path(archive_path)  # pragma: no cover
            dst.mkdir(parents=True, exist_ok=True)  # pragma: no cover
            if src.is_dir():  # pragma: no cover
                for f in src.rglob("*"):  # pragma: no cover
                    if f.is_file():  # pragma: no cover
                        target = dst / f.relative_to(src)  # pragma: no cover
                        target.parent.mkdir(parents=True, exist_ok=True)  # pragma: no cover
                        shutil.move(str(f), str(target))  # pragma: no cover
                logger.debug(f"  Archived files from {src} → {dst}")  # pragma: no cover
            elif src.is_file():  # pragma: no cover
                shutil.move(str(src), str(dst / src.name))  # pragma: no cover
                logger.debug(f"  Archived {src} → {dst}")  # pragma: no cover

    def _try_cleanup_dbutils(self, landing_path: str, entity: str, mode: str, archive_path: Optional[str]) -> bool:
        """Attempt cleanup via Databricks dbutils. Returns True if successful."""
        _dbutils = None  # pragma: no cover
        try:  # pragma: no cover
            _dbutils = self.spark._jvm.com.databricks.service.DBUtils(self.spark._jsc.sc())  # pragma: no cover
        except Exception:  # pragma: no cover
            try:  # pragma: no cover
                import IPython  # pragma: no cover

                # pragma: no cover
                _dbutils = IPython.get_ipython().user_ns.get("dbutils")  # pragma: no cover
            except Exception:  # pragma: no cover
                pass  # pragma: no cover
        # pragma: no cover
        if not _dbutils:  # pragma: no cover
            return False  # pragma: no cover
        # pragma: no cover
        try:  # pragma: no cover
            if mode == "delete":  # pragma: no cover
                # Preserve the top-level directory (managed by Terraform)
                try:
                    children = _dbutils.fs.ls(landing_path)
                    for child in children:
                        _dbutils.fs.rm(child.path, True)
                    logger.debug(f"  Deleted contents of {landing_path} via dbutils")
                except Exception:
                    # Fallback if landing_path is just a file or doesn't exist as dir
                    _dbutils.fs.rm(landing_path, True)  # pragma: no cover
                    logger.debug(f"  Deleted {landing_path} via dbutils")  # pragma: no cover
            elif mode == "archive" and archive_path:  # pragma: no cover
                # dbutils.fs.mv is an atomic move on ADLS/S3  # pragma: no cover
                _dbutils.fs.mv(landing_path, archive_path, True)  # pragma: no cover
                logger.debug(f"  Archived {landing_path} → {archive_path} via dbutils")  # pragma: no cover
            return True  # pragma: no cover
        except Exception as e:  # pragma: no cover
            logger.debug(f"  dbutils cleanup failed for {entity}: {e}")  # pragma: no cover
            return False  # pragma: no cover

    def _cleanup_cloud(self, landing_path: str, entity: str, mode: str, archive_path: Optional[str]) -> None:
        """Clean up landing files on cloud storage via fsspec."""
        import os as _os_cleanup  # pragma: no cover

        # pragma: no cover
        try:  # pragma: no cover
            import fsspec  # pragma: no cover
        except ImportError:  # pragma: no cover
            raise RuntimeError(  # pragma: no cover
                "fsspec is required for cloud post-ingestion cleanup but is not installed. "  # pragma: no cover
                "Install it with: pip install fsspec adlfs  (or s3fs / gcsfs)"  # pragma: no cover
            )  # pragma: no cover
        # pragma: no cover
        # Build storage options from environment variables  # pragma: no cover
        storage_opts: dict = {}  # pragma: no cover
        if landing_path.startswith(("abfss://", "abfs://")):  # pragma: no cover
            for env_key, opt_key in [  # pragma: no cover
                ("AZURE_STORAGE_ACCOUNT_NAME", "account_name"),  # pragma: no cover
                ("AZURE_STORAGE_ACCOUNT", "account_name"),  # pragma: no cover
                ("AZURE_STORAGE_ACCOUNT_KEY", "account_key"),  # pragma: no cover
                ("AZURE_STORAGE_SAS_TOKEN", "sas_token"),  # pragma: no cover
                ("AZURE_CLIENT_ID", "client_id"),  # pragma: no cover
                ("AZURE_CLIENT_SECRET", "client_secret"),  # pragma: no cover
                ("AZURE_TENANT_ID", "tenant_id"),  # pragma: no cover
            ]:  # pragma: no cover
                val = _os_cleanup.environ.get(env_key)  # pragma: no cover
                if val:  # pragma: no cover
                    storage_opts[opt_key] = val  # pragma: no cover
        # pragma: no cover
        fs, _ = fsspec.core.url_to_fs(landing_path, **storage_opts)  # pragma: no cover
        # pragma: no cover
        if mode == "delete":  # pragma: no cover
            if fs.exists(landing_path):  # pragma: no cover
                if fs.isdir(landing_path):
                    # Delete only the contents to preserve the Terraform-managed parent dir
                    for item in fs.ls(landing_path, detail=False):
                        fs.rm(item, recursive=True)
                    logger.debug(f"  Deleted contents of {landing_path} via fsspec")
                else:
                    fs.rm(landing_path, recursive=True)  # pragma: no cover
                    logger.debug(f"  Deleted {landing_path} via fsspec")  # pragma: no cover
        elif mode == "archive" and archive_path:  # pragma: no cover
            if fs.exists(landing_path):  # pragma: no cover
                # For cross-container moves, use copy + delete  # pragma: no cover
                fs.copy(landing_path, archive_path, recursive=True)  # pragma: no cover
                if fs.isdir(landing_path):
                    for item in fs.ls(landing_path, detail=False):
                        fs.rm(item, recursive=True)
                else:
                    fs.rm(landing_path, recursive=True)  # pragma: no cover
                logger.debug(f"  Archived {landing_path} → {archive_path} via fsspec")  # pragma: no cover

    def _process_single_contract(
        self,
        c: RegistryContract,
        layer: str,
        summary: PipelineRunSummary,
        dry_run: bool,
        run_log_mode: Optional[str],
        reprocess_from: Optional[str],
        reprocess_to: Optional[str],
        reprocess_column: Optional[str],
        reprocess_values: Optional[List[str]],
        lookback_days: Optional[int],
        layers_with_new_data: set,
    ) -> None:
        """Process a single contract: resolve UC paths, run source, materialize."""
        # Log contract + target for observability
        _title = (c.contract_dict or {}).get("info", {}).get("title", c.entity)
        _version = (c.contract_dict or {}).get("version", "")
        _ver_str = f" v{_version}" if _version else ""
        # Resolve output table name from info.table_name or materialization.target_path
        _info = (c.contract_dict or {}).get("info", {})
        _mat = (c.contract_dict or {}).get("materialization", {})
        _table_name = (
            _info.get("table_name", "")
            or _mat.get("target_path", "").replace("table:", "")
            or _mat.get("location", "")
            or ""
        )
        if _table_name:
            _sys = getattr(self.registry, "system", "") or ""
            _dom = getattr(self.registry, "domain", "") or ""
            _table_name = _table_name.replace("{system}", _sys)
            _table_name = _table_name.replace("{domain}", _dom)
            _table_name = _table_name.replace("{layer}", layer)
            _table_name = _table_name.replace("{bronze_layer}", layer)
            _table_name = _table_name.replace("{silver_layer}", layer)
            _table_name = _table_name.replace("{gold_layer}", layer)
            _table_name = _table_name.strip("._-/")
        logger.info("  ─────────────────────────────────────────────────────────")
        logger.info(f"  📄 [{layer}] {c.entity} | Contract: {_title}{_ver_str}")

        if dry_run:
            logger.info(f"DRY RUN - skipping {c.entity}")  # pragma: no cover
            summary.append(c.entity, layer, "dry_run", table_name=_table_name)  # pragma: no cover
            return  # pragma: no cover

        try:
            resolved_mode = self._resolve_run_log_mode(c.contract_dict, run_log_mode)
            # Inject created_by override into lineage config if provided
            if getattr(self, "_created_by_override", None):
                lineage_cfg = c.contract_dict.setdefault("lineage", {})
                lineage_cfg["created_by_override"] = self._created_by_override
            processor = DataProcessor(
                contract=c.contract_dict, engine=self.engine, pipeline_run_id=self.run_id, run_log_mode=resolved_mode
            )
            # Inject domain ownership and notifications configuration
            processor._ownership = getattr(self.registry, "ownership", {}) or {}
            processor._notifications = getattr(self.registry, "notifications", []) or []
            processor._notifications_enabled = getattr(self.registry, "notifications_enabled", True)
            result = processor.run_source(
                reprocess_from=reprocess_from,
                reprocess_to=reprocess_to,
                reprocess_column=reprocess_column,
                reprocess_values=reprocess_values,
                lookback_days=lookback_days,
            )

            df_good = result.good
            df_bad = getattr(result, "bad", None)

            is_good_empty = (
                df_good is None
                or (isinstance(df_good, list) and len(df_good) == 0)
                or (hasattr(df_good, "is_empty") and df_good.is_empty())
                or (hasattr(df_good, "__len__") and len(df_good) == 0)
                or (hasattr(df_good, "isEmpty") and df_good.isEmpty())
            )

            is_bad_empty = (
                df_bad is None
                or (isinstance(df_bad, list) and len(df_bad) == 0)
                or (hasattr(df_bad, "is_empty") and df_bad.is_empty())
                or (hasattr(df_bad, "__len__") and len(df_bad) == 0)
                or (hasattr(df_bad, "isEmpty") and df_bad.isEmpty())
            )

            _status = "success"
            if is_good_empty and is_bad_empty:
                logger.info(f"No new rows for {c.entity} - proceeding to ensure target schema existence.")
                _status = "no_new_rows"

            # Spark compatibility layer: if the adapter already returned
            # native Spark DataFrames (SparkAdapter does), skip conversion.
            # Only convert from Polars/Pandas if a non-Spark adapter was used.
            df_bad = result.bad
            if self.engine == "spark":
                _is_spark_df = hasattr(df_good, "sparkSession")
                if not _is_spark_df:
                    # Fallback: adapter returned non-Spark DF — convert
                    import polars as pl

                    if isinstance(df_good, pl.DataFrame):
                        if df_good.is_empty() and len(df_good.columns) == 0:
                            from pyspark.sql.types import StructType

                            df_good = self.spark.createDataFrame([], StructType([]))
                        else:
                            void_cols = [
                                col
                                for col, t in zip(df_good.columns, df_good.dtypes)
                                if str(t) in ("Null", "null", "Void", "void")
                            ]
                            if void_cols:
                                df_good = df_good.with_columns([pl.col(c).cast(pl.Utf8) for c in void_cols])
                            df_good = self.spark.createDataFrame(df_good.to_pandas())
                    else:
                        df_good = self.spark.createDataFrame(df_good)  # pragma: no cover

                    if df_bad is not None and not hasattr(df_bad, "sparkSession"):
                        if isinstance(df_bad, pl.DataFrame):
                            if df_bad.is_empty() and len(df_bad.columns) == 0:
                                from pyspark.sql.types import StructType

                                df_bad = self.spark.createDataFrame([], StructType([]))
                            else:
                                void_cols = [
                                    col
                                    for col, t in zip(df_bad.columns, df_bad.dtypes)
                                    if str(t) in ("Null", "null", "Void", "void")
                                ]
                                if void_cols:
                                    df_bad = df_bad.with_columns([pl.col(c).cast(pl.Utf8) for c in void_cols])
                                df_bad = self.spark.createDataFrame(df_bad.to_pandas())
                        else:
                            df_bad = self.spark.createDataFrame(df_bad)  # pragma: no cover

            _bad_type = type(df_bad).__name__ if df_bad is not None else "None"
            logger.debug(f"Pre-materialize: df_good type={type(df_good).__name__}, df_bad type={_bad_type}")
            if hasattr(df_good, "dtypes"):
                try:  # pragma: no cover
                    logger.debug(f"Pre-materialize: df_good schema={df_good.dtypes}")  # pragma: no cover
                except Exception:  # pragma: no cover
                    pass  # pragma: no cover

            # Pull pre-computed counts from the run report (already integers,
            # no extra Spark actions needed — counts were computed once during validation).
            _report = getattr(processor, "last_report", None) or {}
            _counts = _report.get("counts") or {}
            rows_raw = _counts.get("source") or _counts.get("total")
            rows_good = _counts.get("good")
            rows_bad = _counts.get("quarantined")
            row_count = rows_good if rows_good is not None else "?"

            logger.debug(f"Row counts (from report): raw={rows_raw}, good={rows_good}, bad={rows_bad}")

            _is_empty_run = (is_good_empty or rows_good == 0) and (is_bad_empty or rows_bad == 0 or rows_bad is None)

            if _is_empty_run:
                # Avoid empty Delta transactions that increment version numbers unnecessarily.
                # Use DDL engine to ensure target tables exist instead.
                try:
                    processor.create_table(backend=self.engine)
                    logger.debug(f"Ensured target schema exists via DDL for {c.entity}")
                except Exception as ddl_e:
                    logger.debug(f"DDL check failed for {c.entity}: {ddl_e}")
            else:
                processor.materialize(df_good, df_bad)

            if rows_bad and rows_bad > 0:
                _q_config = getattr(processor.contract, "quarantine", None)
                if _q_config and getattr(_q_config, "fail_on_quarantine", False):
                    # Surface validation failure details so operators can diagnose immediately
                    _failures = _report.get("row_rule_failures") or []
                    _detail_lines = []
                    for f in _failures[:10]:  # Cap at 10 to avoid log flooding
                        _msg = f.get("message") or f.get("name") or str(f)
                        _detail_lines.append(f"  • {_msg}")
                    _detail_str = "\n".join(_detail_lines) if _detail_lines else "  (no rule details captured)"
                    raise ValueError(
                        f"Pipeline failed: {rows_bad} record(s) quarantined for '{c.entity}'.\n"
                        f"Validation failures:\n{_detail_str}"
                    )

            logger.debug(f"✅ Materialized {row_count} rows for {c.entity}")
            layers_with_new_data.add(layer)

            # ── Post-ingestion cleanup (Bronze only) ────────────────────────
            # After a successful Bronze commit, execute the landing zone
            # lifecycle action (delete / archive / retain).
            if layer == "bronze":
                self._execute_post_ingestion_cleanup(c, processor)

            summary.append(
                c.entity,
                layer,
                _status,
                rows=row_count,
                rows_raw=rows_raw,
                rows_good=rows_good,
                rows_bad=rows_bad,
                table_name=_table_name,
            )

            # Write run log with final succeeded status.
            # Skip if the processor already wrote its own log (e.g. no_new_data early-exit).
            _already_logged = getattr(processor, "_run_log_already_written", False)
            if not _already_logged:
                _report = getattr(processor, "last_report", None) or {}
                _report["status"] = "succeeded"
                try:
                    from lakelogic.core.run_log import write_run_log

                    write_run_log(
                        _report,
                        processor.contract,
                        engine_name=processor.engine_name,
                        run_log_mode=processor._run_log_mode,
                    )
                except Exception as log_exc:  # pragma: no cover
                    logger.warning(f"Failed to write run log for {c.entity}: {log_exc}")  # pragma: no cover

        except Exception as e:
            # Enrich auth/permission errors with the active identity so
            # operators immediately know *which* principal was rejected.
            _identity_hint = ""
            if "403" in str(e) or "AuthorizationPermission" in str(e):
                import os as _os

                _cid = _os.getenv("AZURE_CLIENT_ID") or _os.getenv("ARM_CLIENT_ID")
                _tid = _os.getenv("AZURE_TENANT_ID") or _os.getenv("ARM_TENANT_ID")
                if _cid:
                    _identity_hint = f" | identity: SP client_id={_cid}, tenant_id={_tid or 'unknown'}"
                elif _os.getenv("AZURE_STORAGE_ACCOUNT_KEY"):  # pragma: no cover
                    _identity_hint = " | identity: account-key"  # pragma: no cover
                elif _os.getenv("AZURE_STORAGE_SAS_TOKEN"):  # pragma: no cover
                    _identity_hint = " | identity: SAS token"  # pragma: no cover
                else:  # pragma: no cover
                    _identity_hint = (
                        " | identity: DefaultAzureCredential (az login / managed identity)"  # pragma: no cover
                    )
            logger.error(f"❌ Failed to process {c.entity}: {e}{_identity_hint}")
            summary.append(c.entity, layer, "failed", error=str(e), table_name=_table_name)

            # Write run log with failed status so the failure is auditable
            try:
                from lakelogic.core.run_log import write_run_log

                _report = getattr(processor, "last_report", None) if "processor" in dir() else None
                if _report:
                    _report["status"] = "failed"
                    _report["error_message"] = str(e)[:2000]
                    write_run_log(
                        _report,
                        processor.contract,
                        engine_name=processor.engine_name,
                        run_log_mode=processor._run_log_mode,
                    )
                else:
                    # Failure occurred before processor built a report — write minimal entry
                    from datetime import datetime, timezone
                    from uuid import uuid4

                    _minimal = {
                        "run_id": str(uuid4()),
                        "pipeline_run_id": self.run_id,
                        "engine": self.engine,
                        "contract": _title or c.entity,
                        "dataset": _table_name or c.entity,
                        "data_layer": layer,
                        "domain": getattr(self.registry, "domain", None),
                        "system": getattr(self.registry, "system", None),
                        "environment": (c.contract_dict or {}).get("metadata", {}).get("environment", "unknown"),
                        "timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                        "status": "failed",
                        "error_message": str(e)[:2000],
                    }
                    # Use processor.contract if available (already constructed);
                    # fall back to building a minimal contract from the raw dict.
                    _dc = None
                    if "processor" in dir() and processor is not None:
                        _dc = processor.contract  # pragma: no cover
                    if _dc is None and c.contract_dict:
                        try:
                            _dc = DataContract(**c.contract_dict)
                        except Exception:
                            pass  # Contract itself is invalid — can't construct
                    if _dc:
                        write_run_log(
                            _minimal,
                            _dc,
                            engine_name=self.engine,
                            run_log_mode=getattr(processor, "_run_log_mode", None) if "processor" in dir() else None,
                        )
                    else:
                        logger.debug(f"Could not write failure run log for {c.entity}: no valid contract available")
            except Exception as log_exc:
                logger.debug(f"Could not write failure run log for {c.entity}: {log_exc}")

            raise  # Fail fast for orchestrator auto-retries

    def _execute_wave_parallel(
        self,
        wave: List[RegistryContract],
        layer: str,
        summary: PipelineRunSummary,
        dry_run: bool,
        run_log_mode: Optional[str],
        reprocess_from: Optional[str],
        reprocess_to: Optional[str],
        reprocess_column: Optional[str],
        reprocess_values: Optional[List[str]],
        lookback_days: Optional[int],
        layers_with_new_data: set,
        max_workers: int = 4,
        retry_attempts: int = 1,
        retry_base_wait_seconds: int = 30,
        entity_timeout_minutes: Optional[int] = None,
    ) -> None:
        """Execute a wave of independent contracts in parallel using threads."""
        errors: List[Exception] = []

        def _run_contract(c: RegistryContract) -> None:
            with logger.contextualize(entity=c.entity):
                self._process_contract_with_retry(
                    c,
                    layer,
                    summary,
                    dry_run,
                    run_log_mode,
                    reprocess_from,
                    reprocess_to,
                    reprocess_column,
                    reprocess_values,
                    lookback_days,
                    layers_with_new_data,
                    retry_attempts=retry_attempts,
                    retry_base_wait_seconds=retry_base_wait_seconds,
                    entity_timeout_minutes=entity_timeout_minutes,
                )

        with ThreadPoolExecutor(max_workers=min(max_workers, len(wave))) as executor:
            futures = {executor.submit(_run_contract, c): c for c in wave}
            for future in as_completed(futures):
                c = futures[future]
                try:
                    future.result()
                except Exception as e:
                    errors.append(e)
                    logger.error(f"❌ Parallel execution failed for {c.entity}: {e}")

        if errors:
            raise errors[0]  # Fail fast with first error

    # ── DAG Visualization ────────────────────────────────────────────────────

    def visualize_dag(
        self, *, title: str = None, entity_filter: str = "", layer_filter: str = "", theme: str = "dark"
    ) -> str:
        """Generate an inline HTML DAG visualization of the pipeline.

        Args:
            title: Optional business value oriented label to override the default domain/system header.
            entity_filter: Comma-separated entity names to highlight (e.g. "sessions").
            layer_filter: Comma-separated layers to highlight (e.g. "bronze").
            theme: "dark" or "light" (default "dark").

        Returns HTML suitable for ``displayHTML()`` in Databricks notebooks
        or Jupyter's ``IPython.display.HTML()``.

        Usage in Databricks notebook::

            pipeline = LakehousePipeline(registry)
            displayHTML(pipeline.visualize_dag())

            # Custom business label:
            displayHTML(pipeline.visualize_dag(title="Marketplace Booking Engine"))
        """

        contracts = self.registry.get_active_contracts()
        layer_order = ["external", "bronze", "silver", "gold", "downstream"]
        layer_colors = {
            "external": ("#0d9488", "#2dd4bf"),
            "bronze": ("#b8860b", "#daa520"),
            "silver": ("#6b7b8d", "#8fa4b8"),
            "gold": ("#daa520", "#ffd700"),
            "downstream": ("#7c3aed", "#a78bfa"),
        }

        # Parse filters
        _entity_set = {e.strip().lower() for e in entity_filter.split(",") if e.strip()} if entity_filter else set()
        _layer_set = {lyr.strip().lower() for lyr in layer_filter.split(",") if lyr.strip()} if layer_filter else set()
        _has_filter = bool(_entity_set or _layer_set)

        # Build node data
        nodes = []
        # Build entity→node_id lookup for cross-layer depends_on resolution
        _entity_to_node_id = {c.entity: f"{c.layer}_{c.entity}" for c in contracts}

        for c in contracts:
            cd = c.contract_dict or {}
            info = cd.get("info", {})
            pii_count = sum(1 for f in (cd.get("model", {}).get("fields", [])) if f.get("pii"))
            pipeline_config = cd.get("pipeline", {})
            frequency = pipeline_config.get("frequency", "") if isinstance(pipeline_config, dict) else ""
            # Resolve depends_on: look up each dependency's actual layer via
            # the entity map, falling back to same-layer prefix only as last resort.
            resolved_deps = []
            for d in c.depends_on:
                if d in _entity_to_node_id:
                    resolved_deps.append(_entity_to_node_id[d])
                else:
                    # Fallback: assume same layer (legacy behaviour)
                    resolved_deps.append(f"{c.layer}_{d}")  # pragma: no cover
            nodes.append(
                {
                    "id": f"{c.layer}_{c.entity}",
                    "entity": c.entity,
                    "layer": c.layer,
                    "title": info.get("title", c.entity),
                    "version": cd.get("version", ""),
                    "pii": pii_count,
                    "frequency": frequency,
                    "depends_on": resolved_deps,
                }
            )

        # ── External source nodes (cross-domain lineage) ────────────
        ext_sources = getattr(self.registry, "external_sources", []) or []
        for ext in ext_sources:
            ext_id = f"ext_{ext.get('name', 'unknown')}"
            nodes.append(
                {
                    "id": ext_id,
                    "entity": ext.get("name", "unknown"),
                    "layer": "external",
                    "title": ext.get("name", "External Source"),
                    "version": "",
                    "pii": 0,
                    "depends_on": [],
                    "external": True,
                    "source_domain": ext.get("source_domain", ""),
                    "catalog_path": ext.get("catalog_path", ""),
                }
            )

        # Infer cross-layer edges: bronze → silver, silver → gold
        layer_entities = {}
        for n in nodes:
            layer_entities.setdefault(n["layer"], []).append(n)

        edges = []
        # Explicit depends_on edges
        for n in nodes:
            for dep_id in n["depends_on"]:
                edges.append((dep_id, n["id"], "dependency"))

        # Cross-layer lineage edges — intelligent entity-name matching
        # Always infer cross-layer edges from source.path, even when
        # depends_on is present.  depends_on controls intra-layer
        # ordering (e.g. dimensions before facts); source.path captures
        # the primary Bronze→Silver or Silver→Gold data-flow edge.
        upstream_map = {"silver": "bronze", "gold": "silver"}
        # Build set of (node_id, upstream_node_id) pairs already covered
        # by explicit depends_on so we don't draw duplicate edges.
        _existing_edges = {(dep_id, n["id"]) for n in nodes for dep_id in n["depends_on"]}

        for layer, upstream_layer in upstream_map.items():
            for n in layer_entities.get(layer, []):
                # Try to extract source table name from contract YAML source.path
                _matched = False
                for c in contracts:
                    if f"{c.layer}_{c.entity}" != n["id"]:
                        continue
                    cd = c.contract_dict or {}
                    source_path = (cd.get("source") or {}).get("path", "")
                    if source_path:
                        # Extract the table/entity suffix from paths like
                        # "{bronze_path}/{bronze_layer}_{system}_trip_completed"
                        path_tail = source_path.rsplit("/", 1)[-1]  # pragma: no cover
                        # Strip template vars: "{bronze_layer}_rideflow_trips" → "rideflow_trips"  # pragma: no cover
                        import re as _re  # pragma: no cover

                        # pragma: no cover
                        path_tail_clean = _re.sub(r"\{[^}]*\}_?", "", path_tail).strip("_")  # pragma: no cover
                        if path_tail_clean:  # pragma: no cover
                            for upstream_n in layer_entities.get(upstream_layer, []):  # pragma: no cover
                                u_entity = upstream_n["entity"].lower()  # pragma: no cover
                                # Match if upstream entity name is in the cleaned source path  # pragma: no cover
                                # or vice versa (bidirectional for system-prefixed names)  # pragma: no cover
                                if (
                                    path_tail_clean.lower() in u_entity or u_entity in path_tail_clean.lower()
                                ):  # pragma: no cover
                                    edge_pair = (upstream_n["id"], n["id"])  # pragma: no cover
                                    if edge_pair not in _existing_edges:  # pragma: no cover
                                        edges.append((upstream_n["id"], n["id"], "lineage"))  # pragma: no cover
                                        _existing_edges.add(edge_pair)  # pragma: no cover
                                    _matched = True  # pragma: no cover
                    break

                # Fallback: bidirectional entity name matching
                if not _matched:
                    entity_lower = n["entity"].lower()
                    # Strip the tier prefix and optional system prefix for matching
                    _core = entity_lower
                    for pfx in [f"{layer}_", f"{layer}_rideflow_", f"{layer}_olist_"]:
                        if _core.startswith(pfx):
                            _core = _core[len(pfx) :]  # pragma: no cover
                            break  # pragma: no cover
                    for upstream_n in layer_entities.get(upstream_layer, []):
                        u_entity = upstream_n["entity"].lower()
                        u_core = u_entity
                        for pfx in [f"{upstream_layer}_", f"{upstream_layer}_rideflow_", f"{upstream_layer}_olist_"]:
                            if u_core.startswith(pfx):
                                u_core = u_core[len(pfx) :]  # pragma: no cover
                                break  # pragma: no cover
                        # Bidirectional: either core name is a substring of the other
                        if _core and u_core and (_core in u_core or u_core in _core):
                            edge_pair = (upstream_n["id"], n["id"])
                            if edge_pair not in _existing_edges:
                                edges.append((upstream_n["id"], n["id"], "lineage"))  # pragma: no cover
                                _existing_edges.add(edge_pair)  # pragma: no cover

        # External source → consuming contract edges
        for ext in ext_sources:
            ext_id = f"ext_{ext.get('name', 'unknown')}"
            consumed_by = ext.get("consumed_by", [])
            for consumer in consumed_by:
                # Match consumer to contract nodes
                for n in nodes:
                    if n.get("external"):
                        continue
                    # Match explicitly by ID (e.g., 'silver_events') OR generically by entity (only for Bronze target)
                    if n["id"] == consumer or (n["entity"] == consumer and n["layer"] == "bronze"):
                        edges.append((ext_id, n["id"], "external"))

        # ── Downstream consumer nodes (from contract YAML) ─────────
        ds_icon_map = {
            "dashboard": "📊",
            "api": "🔌",
            "report": "📈",
            "table": "📋",
        }
        seen_ds = set()
        for c in contracts:
            cd = c.contract_dict or {}
            downstream_list = cd.get("downstream", [])
            for ds in downstream_list:
                ds_name = ds.get("name", "unknown")
                ds_type = ds.get("type", "table")
                ds_id = f"ds_{ds_name.replace(' ', '_').lower()}"
                if ds_id not in seen_ds:
                    seen_ds.add(ds_id)
                    nodes.append(
                        {
                            "id": ds_id,
                            "entity": ds_name,
                            "layer": "downstream",
                            "title": ds_name,
                            "version": "",
                            "pii": 0,
                            "depends_on": [],
                            "downstream": True,
                            "ds_type": ds_type,
                            "ds_platform": ds.get("platform", ""),
                            "ds_owner": ds.get("owner", ""),
                            "ds_icon": ds_icon_map.get(ds_type, "📋"),
                        }
                    )
                # Edge: parent contract → downstream consumer
                parent_id = f"{c.layer}_{c.entity}"
                edges.append((parent_id, ds_id, "downstream"))

        # ── Determine highlighted vs dimmed nodes ────────────────────────────
        if _has_filter:
            # Primary matches: nodes that match BOTH entity AND layer filters
            primary_ids = set()
            for n in nodes:
                entity_match = (not _entity_set) or (n["entity"].lower() in _entity_set)
                layer_match = (not _layer_set) or (n["layer"].lower() in _layer_set)
                if entity_match and layer_match:
                    primary_ids.add(n["id"])

            # Connected nodes: directly upstream or downstream of primary
            connected_ids = set(primary_ids)
            for src_id, dst_id, _ in edges:
                if src_id in primary_ids:
                    connected_ids.add(dst_id)
                if dst_id in primary_ids:
                    connected_ids.add(src_id)

            # Connected edges: any edge touching a primary node
            highlighted_edges = set()
            for i, (src_id, dst_id, _) in enumerate(edges):
                if src_id in primary_ids or dst_id in primary_ids:
                    highlighted_edges.add(i)
        else:
            primary_ids = {n["id"] for n in nodes}  # pragma: no cover
            connected_ids = primary_ids  # pragma: no cover
            highlighted_edges = set(range(len(edges)))  # pragma: no cover

        # Layout: position nodes in columns by layer with generous spacing
        node_width = 280
        node_height = 100
        header_h = 40  # space for layer column headers
        y_gap = 180  # vertical gap between nodes in same column

        # Calculate dynamic x_positions based purely on which layers are populated
        used_layer_list = [layer for layer in layer_order if any(n["layer"] == layer for n in nodes)]
        x_positions = {layer: i * 410 for i, layer in enumerate(used_layer_list)}

        node_positions = {}
        used_layers = set(used_layer_list)
        for layer in used_layer_list:
            layer_nodes = [n for n in nodes if n["layer"] == layer]

            total = len(layer_nodes)
            total_height = total * node_height + (total - 1) * (y_gap - node_height) if total else 0
            start_y = max(header_h + 10, header_h + (500 - total_height) // 2) if total else header_h + 100
            for i, n in enumerate(layer_nodes):
                y = start_y + i * y_gap
                node_positions[n["id"]] = (x_positions[layer], y)

        # Normalize x positions: shift so minimum x starts at padding
        if node_positions:
            min_x = min(p[0] for p in node_positions.values())
            x_shift = 50 - min_x  # shift so leftmost is at x=50
            node_positions = {k: (v[0] + x_shift, v[1]) for k, v in node_positions.items()}
            # Also shift header positions
            x_positions = {k: v + x_shift for k, v in x_positions.items()}
        else:
            x_shift = 0  # pragma: no cover

        canvas_h = max(n[1] for n in node_positions.values()) + node_height + 80 if node_positions else 400
        canvas_w = max(n[0] for n in node_positions.values()) + node_width + 80 if node_positions else 1200

        # Layer column headers
        header_html = ""
        layer_labels = {
            "external": "EXTERNAL",
            "bronze": "BRONZE",
            "silver": "SILVER",
            "gold": "GOLD",
            "downstream": "DOWNSTREAM",
        }
        for layer in layer_order:
            if layer in layer_entities and layer in used_layers:
                x = x_positions[layer]
                bg, fg = layer_colors.get(layer, ("#444", "#888"))
                hdr_style = (
                    f"position:absolute;left:{x}px;top:4px;"
                    f"font-size:0.65rem;font-weight:700;color:{fg};"
                    f"letter-spacing:0.1em;opacity:0.6;"
                )
                header_html += f'<div style="{hdr_style}">{layer_labels[layer]}</div>'

        # Generate node HTML
        node_html = ""
        for n in nodes:
            x, y = node_positions[n["id"]]
            bg, fg = layer_colors.get(n["layer"], ("#444", "#888"))
            pii_badge = f'<span class="dag-badge dag-badge-pii">🔒 {n["pii"]} PII</span>' if n["pii"] else ""
            is_external = n.get("external", False)

            # Determine opacity/styling based on filter
            if _has_filter:
                if n["id"] in primary_ids:
                    opacity = "1.0"
                    border_style = f"border-color:{bg};box-shadow:0 0 20px {bg}44;"
                    dot_color = "#22c55e"
                elif n["id"] in connected_ids:
                    opacity = "0.7"
                    border_style = f"border-color:{bg}55;"
                    dot_color = "#22c55e88"
                else:
                    opacity = "0.25"  # pragma: no cover
                    border_style = "border-color:#2a2a30;"  # pragma: no cover
                    dot_color = "#555"  # pragma: no cover
            else:  # pragma: no cover
                opacity = "1.0"  # pragma: no cover
                border_style = f"border-color:{bg}55;"  # pragma: no cover
                dot_color = "#22c55e"  # pragma: no cover

            # External nodes get dashed borders
            if is_external:
                border_style += "border-style:dashed;"
                node_icon = "🌐"
                subtitle = n.get("source_domain", "")
                ver_badge = ""
            elif n.get("downstream"):
                border_style += "border-style:dashed;"
                node_icon = n.get("ds_icon", "📋")
                platform = n.get("ds_platform", "")
                ds_type = n.get("ds_type", "")
                subtitle = f"{platform} • {ds_type}" if platform else ds_type
                ver_badge = ""
            else:
                node_icon = "📋"
                subtitle = self.registry.system.upper()
                ver_badge = f'<span class="dag-badge dag-badge-ver">📄 V{n["version"]}</span>'

            freq_badge = (
                f'<span class="dag-badge dag-badge-freq">⏱ {n["frequency"]}</span>' if n.get("frequency") else ""
            )

            hover_in = (
                f"this.style.borderColor='{bg}cc';this.style.boxShadow='0 8px 32px {bg}33';this.style.opacity='1.0'"
            )
            hover_out = f"this.style.borderColor='{bg}55';this.style.boxShadow='none';this.style.opacity='{opacity}'"
            node_html += f"""
            <div class="dag-node"
                 style="left:{x}px;top:{y}px;{border_style}opacity:{opacity};"
                 onmouseover="{hover_in}"
                 onmouseout="{hover_out}">
              <div class="dag-dot"
                   style="background:{dot_color};box-shadow:0 0 6px {dot_color};"
                   ></div>
              <div class="dag-hdr">
                <div class="dag-icon" style="background:{bg}22;color:{fg};">{node_icon}</div>
                <div class="dag-ttl">{n["title"]}</div>
              </div>
              <div class="dag-sys">{subtitle}</div>
              <div class="dag-badges">
                <span class="dag-badge" style="background:{bg}33;color:{fg};">{n["layer"].upper()}</span>
                {ver_badge}
                {freq_badge}
                {pii_badge}
              </div>
            </div>"""

        # Generate SVG edges with smart routing
        edge_paths = ""
        for idx, (src_id, dst_id, edge_type) in enumerate(edges):
            if src_id not in node_positions or dst_id not in node_positions:
                continue  # pragma: no cover
            sx, sy = node_positions[src_id]
            dx, dy = node_positions[dst_id]

            same_column = sx == dx
            cls = "dag-dep" if edge_type == "dependency" else "dag-flow"
            marker = "dag-arrow-dep" if edge_type == "dependency" else "dag-arrow"

            # Dim edges not connected to the focused entity
            edge_opacity = "" if not _has_filter or idx in highlighted_edges else "opacity:0.15;"

            if same_column:
                # Intra-layer dependency: arc to the right of nodes
                src_exit_y = sy + node_height  # exit from bottom  # pragma: no cover
                dst_enter_y = dy  # enter at top  # pragma: no cover
                mid_x = sx + node_width + 60  # arc out to the right  # pragma: no cover
                edge_paths += (  # pragma: no cover
                    f'<path d="M {sx + node_width // 2},{src_exit_y} '
                    f"C {mid_x},{src_exit_y + 40} "
                    f"{mid_x},{dst_enter_y - 40} "
                    f'{dx + node_width // 2},{dst_enter_y}" '
                    f'class="{cls}" style="{edge_opacity}" marker-end="url(#{marker})"/>\n'
                )
            else:
                # Cross-layer: exit right, enter left
                exit_x = sx + node_width
                exit_y = sy + node_height // 2
                enter_x = dx
                enter_y = dy + node_height // 2
                cpx1 = exit_x + 80
                cpx2 = enter_x - 80
                edge_paths += (
                    f'<path d="M {exit_x},{exit_y} '
                    f"C {cpx1},{exit_y} {cpx2},{enter_y} "
                    f'{enter_x},{enter_y}" '
                    f'class="{cls}" style="{edge_opacity}" marker-end="url(#{marker})"/>\n'
                )

        # Subtitle metrics — only count standard data layers
        std_layers = {"bronze", "silver", "gold"}
        std_nodes = [n for n in nodes if n["layer"] in std_layers]
        std_contract_count = len(std_nodes)
        std_layer_count = len(set(n["layer"] for n in std_nodes))

        if _has_filter:
            filter_parts = []
            if _layer_set:
                filter_parts.append(f"Layer: {', '.join(sorted(_layer_set)).upper()}")
            if _entity_set:
                filter_parts.append(f"Entity: {', '.join(sorted(_entity_set))}")
            subtitle = f"{std_contract_count} contracts • {std_layer_count} layers • Filter: {' / '.join(filter_parts)}"
        else:
            subtitle = f"{std_contract_count} contracts • {std_layer_count} layers"  # pragma: no cover

        dag_title = title if title else f"{self.registry.domain} / {self.registry.system}"

        if theme == "light":
            bg_color, bg_dot = "#f8f9fa", "#e5e7eb"
            text_main, text_sub = "#111827", "#4b5563"
            node_bg, node_border = "#ffffff", "#e5e7eb"
            node_text, node_sys = "#1f2937", "#6b7280"
            path_fill, flow_stroke = "#9ca3af", "#d1d5db"
            badge_bg = "#f3f4f6"
        else:
            bg_color, bg_dot = "#121212", "#222222"
            text_main, text_sub = "#ffffff", "#666666"
            node_bg, node_border = "#1a1a1a", "#2a2a30"
            node_text, node_sys = "#f0f0f0", "#555555"
            path_fill, flow_stroke = "#555555", "#444444"
            badge_bg = "#1e3a5f44"

        html = f"""
        <div style="font-family:'Inter','Segoe UI',sans-serif;background:{bg_color};
             background-image:radial-gradient(circle at 1px 1px,{bg_dot} 1px,transparent 0);
             background-size:24px 24px;padding:30px 30px 20px;border-radius:12px;position:relative;overflow-x:auto;">
          <h2 style="color:{text_main};font-size:1.2rem;margin:0 0 4px;"
              >Lakelogic Pipeline DAG — {dag_title}</h2>
          <p style="color:{text_sub};font-size:0.8rem;margin:0 0 24px;">{subtitle}</p>
          <div style="position:relative;width:{canvas_w}px;height:{canvas_h}px;">
            {header_html}
            <svg style="position:absolute;top:0;left:0;width:100%;
                        height:100%;pointer-events:none;"
                 viewBox="0 0 {canvas_w} {canvas_h}">
              <defs>
                <marker id="dag-arrow" viewBox="0 0 12 10"
                        refX="11" refY="5" markerWidth="10"
                        markerHeight="8" orient="auto-start-reverse">
                  <path d="M 0 0 L 12 5 L 0 10 z" fill="{path_fill}"/>
                </marker>
                <marker id="dag-arrow-dep" viewBox="0 0 12 10"
                        refX="11" refY="5" markerWidth="10"
                        markerHeight="8" orient="auto-start-reverse">
                  <path d="M 0 0 L 12 5 L 0 10 z" fill="#4a9eff"/>
                </marker>
              </defs>
              {edge_paths}
            </svg>
            {node_html}
          </div>
          <div style="display:flex;gap:24px;font-size:0.7rem;color:{text_sub};margin-top:16px;">
            <span>◼ <span style="color:#2dd4bf">External</span></span>
            <span>◼ <span style="color:#daa520">Bronze</span></span>
            <span>◼ <span style="color:#8fa4b8">Silver</span></span>
            <span>◼ <span style="color:#ffd700">Gold</span></span>
            <span>◼ <span style="color:#a78bfa">Downstream</span></span>
            <span style="color:#4a9eff">━━ Dependency</span>
            <span style="color:{path_fill}">╌╌ Data Flow</span>
          </div>
        </div>
        <style>
          .dag-node{{position:absolute;background:{node_bg};border:2px solid {node_border};border-radius:12px;
                     padding:14px 18px;width:{node_width}px;height:{node_height}px;box-sizing:border-box;
                     transition:all 0.2s ease;cursor:default;}}
          .dag-hdr{{display:flex;align-items:center;gap:10px;margin-bottom:6px;}}
          .dag-icon{{width:28px;height:28px;border-radius:7px;display:flex;align-items:center;justify-content:center;font-size:13px;flex-shrink:0;}}
          .dag-ttl{{font-size:0.82rem;font-weight:600;color:{node_text};line-height:1.25;}}
          .dag-sys{{font-size:0.62rem;color:{node_sys};text-transform:uppercase;letter-spacing:0.05em;margin-bottom:8px;}}
          .dag-badges{{display:flex;gap:5px;flex-wrap:wrap;}}
          .dag-badge{{font-size:0.55rem;font-weight:600;padding:2px 7px;
                     border-radius:4px;text-transform:uppercase;
                     letter-spacing:0.04em;}}
          .dag-badge-ver{{background:{badge_bg};color:#4a9eff;}}
          .dag-badge-freq{{background:#2dd4bf22;color:#2dd4bf;}}
          .dag-badge-pii{{background:#dc262633;color:#f87171;}}
          .dag-dot{{width:7px;height:7px;border-radius:50%;position:absolute;top:10px;right:12px;}}
          svg .dag-flow{{fill:none;stroke:{flow_stroke};stroke-width:2;stroke-dasharray:8 4;opacity:0.5;}}
          svg .dag-dep{{fill:none;stroke:#4a9eff;stroke-width:2.5;opacity:0.85;stroke-dasharray:none;}}
        </style>
        """
        return html
