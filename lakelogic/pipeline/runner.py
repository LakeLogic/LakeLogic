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
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

from loguru import logger

from lakelogic.core.models import DataContract
from lakelogic.core.observer import RemoteObserver
from lakelogic.core.processor import DataProcessor
from lakelogic.core.registry import DomainRegistry, RegistryContract


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
        return {
            "run_id": self.run_id,
            "environment": self.environment,
            "dry_run": self.dry_run,
            "results": self.results,
        }


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

        if self.engine == "spark" and not self.spark:
            # Try to auto-resolve if inside Databricks
            try:
                from pyspark.sql import SparkSession

                self.spark = SparkSession.builder.getOrCreate()
            except ImportError:
                pass

        if self.spark:
            try:
                # Force strictly OSS-compatible Delta tables to ensure Polars interoperability
                self.spark.conf.set("spark.databricks.delta.properties.defaults.enableDeletionVectors", "false")
                logger.debug("Disabled DeletionVectors by default in Spark session for OSS compatibility.")
            except Exception as e:
                logger.debug(f"Could not disable DeletionVectors on Spark session: {e}")

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
            return contract_dict

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
            elif "s3://" in root or "s3a://" in root:
                aws_vars = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
                if not all(os.environ.get(v) for v in aws_vars):
                    logger.warning(f"Direct mode with S3 — credentials may be needed: {', '.join(aws_vars)}")
            elif "gs://" in root:
                if not os.environ.get("GOOGLE_APPLICATION_CREDENTIALS"):
                    logger.warning("Direct mode with GCS — GOOGLE_APPLICATION_CREDENTIALS may be needed.")

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
                if quar.get("enabled") and not quar.get("target") and _layer_root:
                    _domain = info.get("domain", "")
                    _q_table = f"{_domain}_{table_name}" if _domain else table_name
                    quar["target"] = f"{_layer_root}/_quarantine/{_q_table}"
                    contract_dict["quarantine"] = quar
            else:
                if quar.get("enabled") and not quar.get("target") and storage.quarantine_root:
                    _domain = info.get("domain", "")
                    _q_table = f"{_domain}_{table_name}" if _domain else table_name
                    quar["target"] = f"{storage.quarantine_root}.{_q_table}"
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
                    if not source.get("path") and _layer_root:
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
            layer_defaults = self.registry.materialization.get(target_layer, {})
            if layer_defaults:
                existing_mat = contract_dict.get("materialization") or {}
                # Layer defaults are base, contract-level overrides
                merged = {**layer_defaults, **existing_mat}
                contract_dict["materialization"] = merged

        # ── Step 6: Inherit per-layer server defaults ────────────────
        if self.registry and self.registry.server and target_layer:
            layer_server = self.registry.server.get(target_layer)
            if layer_server and isinstance(layer_server, dict):
                server = contract_dict.get("server")
                if server and isinstance(server, dict):
                    # Deep-merge: layer defaults fill gaps, including
                    # nested dicts like schema_policy
                    for key, val in layer_server.items():
                        if key not in server:
                            server[key] = copy.deepcopy(val)
                        elif isinstance(val, dict) and isinstance(server.get(key), dict):
                            # Deep-merge nested dicts (e.g. schema_policy)
                            for sub_key, sub_val in val.items():
                                if sub_key not in server[key]:
                                    server[key][sub_key] = sub_val
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
        """
        by_entity = {c.entity: c for c in contracts}
        in_degree: Dict[str, int] = {c.entity: 0 for c in contracts}
        graph: Dict[str, List[str]] = defaultdict(list)

        for c in contracts:
            for dep in c.depends_on:
                if dep in by_entity:
                    graph[dep].append(c.entity)
                    in_degree[c.entity] += 1

        queue = deque(e for e, d in in_degree.items() if d == 0)
        ordered: List[RegistryContract] = []

        while queue:
            entity = queue.popleft()
            ordered.append(by_entity[entity])
            for downstream in graph[entity]:
                in_degree[downstream] -= 1
                if in_degree[downstream] == 0:
                    queue.append(downstream)

        if len(ordered) != len(contracts):
            remaining = set(in_degree) - {c.entity for c in ordered}
            raise ValueError(f"Circular dependency detected among contracts: {remaining}")

        return ordered

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
            dep_level = max(_level(dep, visited) for dep in c.depends_on if dep in by_entity)
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
                continue

            source = c.contract_dict.get("source") or {}
            landing_path = source.get("path")
            fmt = (source.get("format") or "json").lower()
            partition_cfg = source.get("partition")

            if not landing_path:
                logger.warning(f"No source.path for {c.entity} — skipping test data generation")
                continue

            if not c.resolved_path:
                logger.warning(f"No resolved contract path for {c.entity} — skipping")
                continue

            try:
                gen = DataGenerator(c.resolved_path, seed=42)
            except Exception as e:
                logger.warning(f"Cannot create DataGenerator for {c.entity}: {e}")
                continue

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
            except Exception as e:
                logger.warning(f"Test data generation failed for {c.entity}: {e}")
                continue

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
                    except Exception as e:
                        logger.debug(f"save_with_report failed, falling back: {e}")
                        self._write_test_data(df, output_dir / f"test_data.{fmt}", fmt)
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
                except Exception as e:
                    logger.warning(f"  ⚠️ Failed to suggest rules for {c.entity}: {e}")

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
            return

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
            conditions.append(f"LOWER(contract) LIKE LOWER('%{entity_name}%')")
            params_desc.append(f"contract~={entity_name}")

        where_clause = " AND ".join(conditions)
        try:
            if self.spark:
                # Check if the run_log table exists before attempting DELETE
                if self.spark.catalog.tableExists(_rl_table):
                    self.spark.sql(f"DELETE FROM {_rl_table} WHERE {where_clause}")
                    logger.info(f"  Cleared run log entries ({', '.join(params_desc)}) from {_rl_table}")
                else:
                    logger.debug(f"  Run log table {_rl_table} does not exist yet; nothing to clear")
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
                    except Exception as e:
                        if "No table" in str(e) or "Not a Delta table" in str(e) or "is not a Delta table" in str(e):
                            logger.debug(f"  Run log table {_rl_table} does not exist yet; nothing to clear")
                        else:
                            raise e
                except ImportError:
                    logger.debug("deltalake not installed, skipping run log delete")
        except Exception as _rl_exc:
            logger.warning(f"  Could not clear run log for '{entity_name}': {_rl_exc}")

    def _execute_resets(
        self, active_contracts: List[RegistryContract], reset_layers: Set[str], reload_layers: Set[str], dry_run: bool
    ) -> None:
        """Run DROP (reset) and TRUNCATE (reload) operations."""
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
                    except Exception as _mt_exc:
                        logger.warning(f"  Could not drop main table {main_table}: {_mt_exc}")
                elif _table_name and self.spark and self.registry:
                    # Derive from domain_catalog + table_name (UC convention)
                    storage = self.registry.storage
                    if storage and storage.domain_catalog:
                        main_table = f"{storage.domain_catalog}.{_table_name}"
                        try:
                            self.spark.sql(f"DROP TABLE IF EXISTS {main_table}")
                            logger.info(f"  Dropped main table {main_table}")
                        except Exception as _mt_exc:
                            logger.warning(f"  Could not drop main table {main_table}: {_mt_exc}")

                # Drop the quarantine table for this entity too
                q_cfg = contract_dict.get("quarantine", {})
                q_target = q_cfg.get("target", "")
                if q_target.startswith("table:") and self.spark:
                    q_table = q_target[len("table:") :]
                    try:
                        self.spark.sql(f"DROP TABLE IF EXISTS {q_table}")
                        logger.info(f"  Dropped quarantine table {q_table}")
                    except Exception as _q_exc:
                        logger.warning(f"  Could not drop quarantine table {q_table}: {_q_exc}")

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
                                except Exception:
                                    pass
                            if _dbutils:
                                _dbutils.fs.rm(q_cloud_path, True)
                                logger.info(f"  Reset: deleted quarantine cloud location {q_cloud_path} via dbutils")
                            else:
                                logger.debug(
                                    f"  dbutils not available; quarantine cloud path {q_cloud_path} not deleted"
                                )
                        except Exception as _qp_exc:
                            logger.warning(f"  Could not delete quarantine cloud path {q_cloud_path}: {_qp_exc}")

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
                                        _q_opts[_ok] = _v
                            fs, q_path_part = fsspec.core.url_to_fs(q_target, **_q_opts)
                            if fs.exists(q_path_part):
                                fs.rm(q_path_part, recursive=True)
                                logger.info(f"  Reset: deleted quarantine cloud path {q_target}")
                            else:
                                logger.info(f"  Reset: quarantine cloud path {q_target} does not exist")
                            _q_deleted = True
                        except ImportError:
                            logger.debug("  fsspec not available for quarantine cloud deletion")
                        except Exception as _q_exc:
                            logger.warning(f"  Could not delete quarantine cloud path {q_target}: {_q_exc}")
                    else:
                        import shutil
                        from pathlib import Path as _P

                        _qp = _P(q_target)
                        if _qp.exists():
                            try:
                                if _qp.is_dir():
                                    shutil.rmtree(_qp)
                                else:
                                    _qp.unlink()
                                logger.info(f"  Reset: deleted local quarantine path {q_target}")
                                _q_deleted = True
                            except Exception as _q_exc:
                                logger.warning(f"  Could not delete local quarantine path {q_target}: {_q_exc}")
                        else:
                            _q_deleted = True
                    if not _q_deleted:
                        logger.warning(
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
                    table_name = mat_target[len("table:") :]
                    if not dry_run:
                        try:
                            self.spark.sql(f"TRUNCATE TABLE {table_name}")
                            logger.info(f"  Truncated {table_name}")
                        except Exception as e:
                            logger.warning(f"  Could not truncate {table_name}: {e}")
                elif mat_target:
                    if not dry_run:
                        try:
                            # Use internal data contract wipe mechanism (supports fsspec now)
                            DataProcessor(
                                contract=contract_dict, engine=self.engine, pipeline_run_id=self.run_id
                            ).reset(targets=["materialization"])
                            logger.info(f"  Truncated (deleted files) {mat_target}")
                        except Exception as e:
                            logger.warning(f"  Could not truncate files at {mat_target}: {e}")

                q_cfg = contract_dict.get("quarantine", {})
                q_target = q_cfg.get("target", "")
                if q_target.startswith("table:") and q_cfg.get("enabled", False) and self.spark:
                    q_table = q_target[len("table:") :]
                    if not dry_run:
                        try:
                            self.spark.sql(f"TRUNCATE TABLE {q_table}")
                            logger.info(f"  Truncated quarantine {q_table}")
                        except Exception as e:
                            logger.warning(f"  Could not truncate {q_table}: {e}")
                elif q_target and q_cfg.get("enabled", False):
                    if not dry_run:
                        try:
                            DataProcessor(
                                contract=contract_dict, engine=self.engine, pipeline_run_id=self.run_id
                            ).reset(targets=["quarantine"])
                            logger.info(f"  Truncated quarantine (deleted files) {q_target}")
                        except Exception as e:
                            logger.warning(f"  Could not truncate quarantine files at {q_target}: {e}")

                # Clear run log entries using precise multi-column filter
                if not dry_run:
                    self._delete_run_log_entries(contract_dict, name, layer)

                processor = DataProcessor(contract=contract_dict, engine=self.engine, pipeline_run_id=self.run_id)
                if not dry_run:
                    processor.reset(targets=["watermark", "run_log"])
                else:
                    processor.reset(targets=["watermark", "run_log"], dry_run=True)

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
                    return [], {}

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
                except Exception as e:
                    logger.debug(f"Could not introspect Delta schema: {e}")
                    return [], {}

            elif backend == "duckdb":
                table_name = _resolve_table_name(contract)
                if not table_name:
                    return [], {}
                try:
                    import duckdb

                    con = duckdb.connect(database=":memory:")
                    result = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
                    con.close()
                    col_names = [row[1] for row in result]
                    col_types = {row[1]: row[2] for row in result}
                    return col_names, col_types
                except Exception:
                    return [], {}

            elif backend in ("spark", "databricks"):
                table_name = _resolve_table_name(contract)
                if not table_name:
                    return [], {}
                try:
                    from pyspark.sql import SparkSession

                    spark = SparkSession.builder.getOrCreate()
                    df = spark.table(table_name)
                    col_names = df.columns
                    col_types = {f.name: f.dataType.simpleString().upper() for f in df.schema.fields}
                    return col_names, col_types
                except Exception:
                    return [], {}

        except Exception as e:
            logger.debug(f"Schema introspection skipped for {backend}: {e}")

        return [], {}

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
            except Exception as e:
                logger.warning(f"Spark ALTER failed for {entity}: {e}")
            return

        # For other backends, log the statements for manual application
        for stmt in statements:
            logger.info(f"Schema evolution ({entity}): {stmt} (manual execution required for {backend})")

    # ── Phase 3: Compliance & Privacy ────────────────────────────────────────

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
            partition_msg = f" [partition: {partition_filter['column']}='{partition_filter['value']}']"
        logger.info(
            f"GDPR Erasure Pass: {len(subject_ids)} subjects on '{subject_col}' (Strategy: {strategy}){partition_msg}"
        )

        for c in active_contracts:
            dc = DataContract(**(c.contract_dict or {}))
            pii_cols = _get_pii_column_names(dc)
            if not pii_cols:
                continue

            has_target = any(f.name == subject_col for f in (dc.model.fields if dc.model else []))
            if not has_target:
                continue

            mat_cfg = (c.contract_dict or {}).get("materialization", {})
            mat_target = mat_cfg.get("target_path", "") or mat_cfg.get("path", "")

            if mat_target.startswith("table:") and self.spark:
                table_name = mat_target[len("table:") :]
                sql_vals = ", ".join([f"'{str(v).replace(chr(39), chr(39) * 2)}'" for v in subject_ids])

                set_clauses = []
                for col in pii_cols:
                    if col == subject_col and strategy == "nullify":
                        continue
                    if strategy == "nullify":
                        set_clauses.append(f"`{col}` = NULL")
                    elif strategy == "redact":
                        set_clauses.append(f"`{col}` = '***REDACTED***'")
                    elif strategy == "hash":
                        set_clauses.append(f"`{col}` = hex(sha2(concat('{salt}', `{col}`), 256))")

                if not set_clauses:
                    continue

                update_sql = f"UPDATE {table_name} SET {','.join(set_clauses)} WHERE `{subject_col}` IN ({sql_vals})"

                # Scope to partition if specified (multi-region safeguard)
                if partition_filter:
                    pcol = partition_filter["column"].replace("`", "")
                    pval = partition_filter["value"].replace("'", "''")
                    update_sql += f" AND `{pcol}` = '{pval}'"

                affected = 0
                if dry_run:
                    logger.info(f"DRY RUN GDPR SQL: {update_sql}")
                    affected = len(subject_ids)
                else:
                    try:
                        res = self.spark.sql(update_sql)
                        affected = res.collect()[0]["num_affected_rows"]
                        logger.info(f"GDPR Update: {affected} rows in {table_name}")
                    except Exception as e:
                        logger.error(f"GDPR Update failed on {table_name}: {e}")

                if affected > 0 or dry_run:
                    report = generate_erasure_report(
                        dc, subject_col, subject_ids, strategy, affected, partition_filter=partition_filter
                    )
                    report["pipeline_run_id"] = self.run_id

                    log_dir = "/Workspace/Shared/lakelogic_logs/gdpr_reports"
                    os.makedirs(log_dir, exist_ok=True)
                    log_path = f"{log_dir}/erasure_{c.entity}_{int(time.time())}.json"
                    with open(log_path, "w") as f:
                        json.dump(report, f, indent=2)

                    try:
                        RemoteObserver().report(report)
                    except Exception:
                        pass  # Fail silently

            else:
                logger.warning(
                    f"Path-based target for [{c.layer}] {c.entity} not yet supported in native driver erasure pass."
                )

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
            partition_msg = f" [partition: {partition_filter['column']}='{partition_filter['value']}']"
        logger.info(
            f"HIPAA Erasure Pass: {len(patient_ids)} patients on '{patient_col}' (Strategy: {strategy}){partition_msg}"
        )

        for c in active_contracts:
            dc = DataContract(**(c.contract_dict or {}))
            phi_cols = _get_phi_column_names(dc)
            if not phi_cols:
                continue

            has_target = any(f.name == patient_col for f in (dc.model.fields if dc.model else []))
            if not has_target:
                continue

            mat_cfg = (c.contract_dict or {}).get("materialization", {})
            mat_target = mat_cfg.get("target_path", "") or mat_cfg.get("path", "")

            if mat_target.startswith("table:") and self.spark:
                table_name = mat_target[len("table:") :]
                sql_vals = ", ".join([f"'{str(v).replace(chr(39), chr(39) * 2)}'" for v in patient_ids])

                set_clauses = []
                for col in phi_cols:
                    if col == patient_col and strategy == "nullify":
                        continue
                    if strategy == "nullify":
                        set_clauses.append(f"`{col}` = NULL")
                    elif strategy == "redact":
                        set_clauses.append(f"`{col}` = '***REDACTED_PHI***'")
                    elif strategy == "hash":
                        set_clauses.append(f"`{col}` = hex(sha2(concat('{salt}', `{col}`), 256))")

                if not set_clauses:
                    continue

                update_sql = f"UPDATE {table_name} SET {','.join(set_clauses)} WHERE `{patient_col}` IN ({sql_vals})"

                # Scope to partition if specified (multi-region safeguard)
                if partition_filter:
                    pcol = partition_filter["column"].replace("`", "")
                    pval = partition_filter["value"].replace("'", "''")
                    update_sql += f" AND `{pcol}` = '{pval}'"

                affected = 0
                if dry_run:
                    logger.info(f"DRY RUN HIPAA SQL: {update_sql}")
                    affected = len(patient_ids)
                else:
                    try:
                        res = self.spark.sql(update_sql)
                        affected = res.collect()[0]["num_affected_rows"]
                        logger.info(f"HIPAA Update: {affected} rows in {table_name}")
                    except Exception as e:
                        logger.error(f"HIPAA Update failed on {table_name}: {e}")

                if affected > 0 or dry_run:
                    report = generate_hipaa_erasure_report(
                        dc, patient_col, patient_ids, strategy, affected, partition_filter=partition_filter
                    )
                    report["pipeline_run_id"] = self.run_id

                    log_dir = "/Workspace/Shared/lakelogic_logs/hipaa_reports"
                    os.makedirs(log_dir, exist_ok=True)
                    log_path = f"{log_dir}/erasure_{c.entity}_{int(time.time())}.json"
                    with open(log_path, "w") as f:
                        json.dump(report, f, indent=2)

                    try:
                        RemoteObserver().report(report)
                    except Exception:
                        pass  # Fail silently

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
            entity = record["extra"].get("entity", "")
            tag = f"[{entity}] " if entity else ""
            return (
                "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
                "<level>{level: <8}</level> | "
                "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
                f"{tag}"
                "<level>{message}</level>\n{exception}"
            )

        # Set log level based on debug_mode
        import sys

        if debug_mode:
            logger.info("Debug mode enabled — verbose logging active")
            logger.remove()
            logger.add(sys.stderr, format=_log_format, level="DEBUG")
        elif parallel:
            # In parallel mode, reconfigure format to include entity tags
            logger.remove()
            logger.add(sys.stderr, format=_log_format, level="INFO")

        logger.info(f"Pipeline storage mode: {self.storage_mode}")

        resets = {layer.strip().lower() for layer in reset_layers.split(",") if layer.strip()}
        reloads = {layer.strip().lower() for layer in reload_layers.split(",") if layer.strip()}
        entities = {entity.strip().lower() for entity in entity_filter.split(",") if entity.strip()}

        # Filter active contracts
        all_active = self.registry.get_active_contracts()
        if entities:
            all_active = [c for c in all_active if c.entity.lower() in entities]

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
                continue

            # Order by dependencies
            try:
                layer_contracts = self._topological_sort(layer_contracts)
            except ValueError as e:
                logger.error(f"Dependency error in {layer}: {e}")
                for c in layer_contracts:
                    summary.append(c.entity, layer, "failed", error=str(e))
                continue

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
                waves = self._group_by_dependency_level(layer_contracts)
            else:
                # Sequential: each contract is its own wave
                waves = [[c] for c in layer_contracts]

            for wave_idx, wave in enumerate(waves):
                if parallel and len(wave) > 1:
                    logger.info(f"  Wave {wave_idx}: [{', '.join(c.entity for c in wave)}] (parallel)")
                    self._execute_wave_parallel(
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
                                logger.error(
                                    f"❌ {c.entity} [{layer}] timed out after {entity_timeout_minutes} minutes."
                                )
                                summary.append(c.entity, layer, "timeout")
                            else:
                                # Catch-all for any other wrapper errors not logged by _process_single_contract
                                logger.error(f"❌ {c.entity} [{layer}] failed unexpectedly: {e}")
                                # Only append if not already in summary to avoid duplicate failure entries
                                if not any(
                                    r.get("contract") == c.entity and r.get("layer") == layer for r in summary.results
                                ):
                                    summary.append(c.entity, layer, "failed")
        return summary

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
                logger.warning("No run_log_table configured — cannot load checkpoint")
                return succeeded

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
        except Exception as e:
            logger.warning(f"Failed to load checkpoint from run {pipeline_run_id}: {e}")

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
                    try:
                        self._process_single_contract(*args)
                    except Exception as e:
                        error_holder[0] = e

                t = threading.Thread(target=_target, daemon=True)
                t.start()
                t.join(timeout=entity_timeout_minutes * 60)

                if t.is_alive():
                    raise EntityTimeoutError(f"{entity_label} exceeded timeout of {entity_timeout_minutes} minutes")
                if error_holder[0]:
                    raise error_holder[0]
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
            logger.info(f"DRY RUN - skipping {c.entity}")
            summary.append(c.entity, layer, "dry_run", table_name=_table_name)
            return

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
            )

            is_bad_empty = (
                df_bad is None
                or (isinstance(df_bad, list) and len(df_bad) == 0)
                or (hasattr(df_bad, "is_empty") and df_bad.is_empty())
                or (hasattr(df_bad, "__len__") and len(df_bad) == 0)
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
                        void_cols = [
                            col
                            for col, t in zip(df_good.columns, df_good.dtypes)
                            if str(t) in ("Null", "null", "Void", "void")
                        ]
                        if void_cols:
                            df_good = df_good.with_columns([pl.col(c).cast(pl.Utf8) for c in void_cols])
                        df_good = self.spark.createDataFrame(df_good.to_pandas())
                    else:
                        df_good = self.spark.createDataFrame(df_good)

                    if df_bad is not None and not hasattr(df_bad, "sparkSession"):
                        if isinstance(df_bad, pl.DataFrame):
                            void_cols = [
                                col
                                for col, t in zip(df_bad.columns, df_bad.dtypes)
                                if str(t) in ("Null", "null", "Void", "void")
                            ]
                            if void_cols:
                                df_bad = df_bad.with_columns([pl.col(c).cast(pl.Utf8) for c in void_cols])
                            df_bad = self.spark.createDataFrame(df_bad.to_pandas())
                        else:
                            df_bad = self.spark.createDataFrame(df_bad)

            _bad_type = type(df_bad).__name__ if df_bad is not None else "None"
            logger.debug(f"Pre-materialize: df_good type={type(df_good).__name__}, df_bad type={_bad_type}")
            if hasattr(df_good, "dtypes"):
                try:
                    logger.debug(f"Pre-materialize: df_good schema={df_good.dtypes}")
                except Exception:
                    pass

            # Pull pre-computed counts from the run report (already integers,
            # no extra Spark actions needed — counts were computed once during validation).
            _report = getattr(processor, "last_report", None) or {}
            _counts = _report.get("counts") or {}
            rows_raw = _counts.get("source") or _counts.get("total")
            rows_good = _counts.get("good")
            rows_bad = _counts.get("quarantined")
            row_count = rows_good if rows_good is not None else "?"

            logger.debug(f"Row counts (from report): raw={rows_raw}, good={rows_good}, bad={rows_bad}")
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

            # Write run log with final succeeded status
            _report = getattr(processor, "last_report", None) or {}
            _report["status"] = "succeeded"
            try:
                from lakelogic.core.run_log import write_run_log

                write_run_log(
                    _report, processor.contract, engine_name=processor.engine_name, run_log_mode=processor._run_log_mode
                )
            except Exception as log_exc:
                logger.warning(f"Failed to write run log for {c.entity}: {log_exc}")

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
                elif _os.getenv("AZURE_STORAGE_ACCOUNT_KEY"):
                    _identity_hint = " | identity: account-key"
                elif _os.getenv("AZURE_STORAGE_SAS_TOKEN"):
                    _identity_hint = " | identity: SAS token"
                else:
                    _identity_hint = " | identity: DefaultAzureCredential (az login / managed identity)"
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
                        "timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
                        "status": "failed",
                        "error_message": str(e)[:2000],
                    }
                    # Use processor.contract if available (already constructed);
                    # fall back to building a minimal contract from the raw dict.
                    _dc = None
                    if "processor" in dir() and processor is not None:
                        _dc = processor.contract
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

    def visualize_dag(self, *, entity_filter: str = "", layer_filter: str = "") -> str:
        """Generate an inline HTML DAG visualization of the pipeline.

        Args:
            entity_filter: Comma-separated entity names to highlight (e.g. "sessions").
            layer_filter: Comma-separated layers to highlight (e.g. "bronze").

        Returns HTML suitable for ``displayHTML()`` in Databricks notebooks
        or Jupyter's ``IPython.display.HTML()``.

        Usage in Databricks notebook::

            pipeline = LakehousePipeline(registry)
            displayHTML(pipeline.visualize_dag())

            # Filtered view — highlight bronze sessions and its connections:
            displayHTML(pipeline.visualize_dag(entity_filter="sessions", layer_filter="bronze"))
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
        for c in contracts:
            cd = c.contract_dict or {}
            info = cd.get("info", {})
            pii_count = sum(1 for f in (cd.get("model", {}).get("fields", [])) if f.get("pii"))
            pipeline_config = cd.get("pipeline", {})
            frequency = pipeline_config.get("frequency", "") if isinstance(pipeline_config, dict) else ""
            nodes.append(
                {
                    "id": f"{c.layer}_{c.entity}",
                    "entity": c.entity,
                    "layer": c.layer,
                    "title": info.get("title", c.entity),
                    "version": cd.get("version", ""),
                    "pii": pii_count,
                    "frequency": frequency,
                    "depends_on": [f"{c.layer}_{d}" for d in c.depends_on],
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

        # Cross-layer lineage edges
        upstream_map = {"silver": "bronze", "gold": "silver"}
        for layer, upstream_layer in upstream_map.items():
            for n in layer_entities.get(layer, []):
                for upstream_n in layer_entities.get(upstream_layer, []):
                    edges.append((upstream_n["id"], n["id"], "lineage"))

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
            primary_ids = {n["id"] for n in nodes}
            connected_ids = primary_ids
            highlighted_edges = set(range(len(edges)))

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
            x_shift = 0

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
                    opacity = "0.25"
                    border_style = "border-color:#2a2a30;"
                    dot_color = "#555"
            else:
                opacity = "1.0"
                border_style = f"border-color:{bg}55;"
                dot_color = "#22c55e"

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
                continue
            sx, sy = node_positions[src_id]
            dx, dy = node_positions[dst_id]

            same_column = sx == dx
            cls = "dag-dep" if edge_type == "dependency" else "dag-flow"
            marker = "dag-arrow-dep" if edge_type == "dependency" else "dag-arrow"

            # Dim edges not connected to the focused entity
            edge_opacity = "" if not _has_filter or idx in highlighted_edges else "opacity:0.15;"

            if same_column:
                # Intra-layer dependency: arc to the right of nodes
                src_exit_y = sy + node_height  # exit from bottom
                dst_enter_y = dy  # enter at top
                mid_x = sx + node_width + 60  # arc out to the right
                edge_paths += (
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
            subtitle = f"{std_contract_count} contracts • {std_layer_count} layers"

        html = f"""
        <div style="font-family:'Inter','Segoe UI',sans-serif;background:#0d0d0f;
             background-image:radial-gradient(circle at 1px 1px,#1a1a1f 1px,transparent 0);
             background-size:24px 24px;padding:30px 30px 20px;border-radius:12px;position:relative;overflow-x:auto;">
          <h2 style="color:#fff;font-size:1.2rem;margin:0 0 4px;"
              >📊 Pipeline DAG — {self.registry.domain} / {self.registry.system}</h2>
          <p style="color:#666;font-size:0.8rem;margin:0 0 24px;">{subtitle}</p>
          <div style="position:relative;width:{canvas_w}px;height:{canvas_h}px;">
            {header_html}
            <svg style="position:absolute;top:0;left:0;width:100%;
                        height:100%;pointer-events:none;"
                 viewBox="0 0 {canvas_w} {canvas_h}">
              <defs>
                <marker id="dag-arrow" viewBox="0 0 12 10"
                        refX="11" refY="5" markerWidth="10"
                        markerHeight="8" orient="auto-start-reverse">
                  <path d="M 0 0 L 12 5 L 0 10 z" fill="#555"/>
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
          <div style="display:flex;gap:24px;font-size:0.7rem;color:#666;margin-top:16px;">
            <span>◼ <span style="color:#2dd4bf">External</span></span>
            <span>◼ <span style="color:#daa520">Bronze</span></span>
            <span>◼ <span style="color:#8fa4b8">Silver</span></span>
            <span>◼ <span style="color:#ffd700">Gold</span></span>
            <span>◼ <span style="color:#a78bfa">Downstream</span></span>
            <span style="color:#4a9eff">━━ Dependency</span>
            <span style="color:#555">╌╌ Data Flow</span>
          </div>
        </div>
        <style>
          .dag-node{{position:absolute;background:#16161a;border:2px solid #2a2a30;border-radius:12px;
                     padding:14px 18px;width:{node_width}px;height:{node_height}px;box-sizing:border-box;
                     transition:all 0.2s ease;cursor:default;}}
          .dag-hdr{{display:flex;align-items:center;gap:10px;margin-bottom:6px;}}
          .dag-icon{{width:28px;height:28px;border-radius:7px;display:flex;align-items:center;justify-content:center;font-size:13px;flex-shrink:0;}}
          .dag-ttl{{font-size:0.82rem;font-weight:600;color:#f0f0f0;line-height:1.25;}}
          .dag-sys{{font-size:0.62rem;color:#555;text-transform:uppercase;letter-spacing:0.05em;margin-bottom:8px;}}
          .dag-badges{{display:flex;gap:5px;flex-wrap:wrap;}}
          .dag-badge{{font-size:0.55rem;font-weight:600;padding:2px 7px;
                     border-radius:4px;text-transform:uppercase;
                     letter-spacing:0.04em;}}
          .dag-badge-ver{{background:#1e3a5f44;color:#4a9eff;}}
          .dag-badge-freq{{background:#2dd4bf22;color:#2dd4bf;}}
          .dag-badge-pii{{background:#dc262633;color:#f87171;}}
          .dag-dot{{width:7px;height:7px;border-radius:50%;position:absolute;top:10px;right:12px;}}
          svg .dag-flow{{fill:none;stroke:#444;stroke-width:2;stroke-dasharray:8 4;opacity:0.5;}}
          svg .dag-dep{{fill:none;stroke:#4a9eff;stroke-width:2.5;opacity:0.85;stroke-dasharray:none;}}
        </style>
        """
        return html
