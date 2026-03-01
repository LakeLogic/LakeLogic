"""
Registry-driven pipeline driver for LakeLogic.

This module orchestrates bronze/silver/gold runs from contract registries, supports
incremental windows (last_success), and reprocessing of late-arriving data.
"""

import argparse
import json
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from http.server import HTTPServer
from pathlib import Path
from threading import Lock, Thread
from typing import Any, Dict, List, Optional, Tuple
from uuid import uuid4

import yaml

from lakelogic import DataProcessor
from lakelogic.core.models import DataContract, Quality
from loguru import logger


@dataclass
class Window:
    """Window boundaries used for incremental and reprocessing runs."""

    start: Optional[datetime]
    end: Optional[datetime]
    label: str


class ContractLoader:
    """Load DataContract YAML files while preserving 'on' keyword semantics."""

    def __init__(self) -> None:
        class Loader(yaml.SafeLoader):
            pass

        for key, mappings in list(Loader.yaml_implicit_resolvers.items()):
            Loader.yaml_implicit_resolvers[key] = [
                (tag, regex) for tag, regex in mappings if tag != "tag:yaml.org,2002:bool"
            ]

        bool_regex = re.compile(r"^(?:true|false)$", re.IGNORECASE)
        Loader.add_implicit_resolver("tag:yaml.org,2002:bool", bool_regex, list("tTfF"))
        self._loader = Loader

    def load(self, path: Path) -> DataContract:
        """
        Load a DataContract from disk and annotate with base path.

        Args:
            path: YAML file path.

        Returns:
            DataContract instance.
        """
        data = yaml.load(path.read_text(encoding="utf-8"), Loader=self._loader)
        contract = DataContract(**data)
        contract._base_path = path.parent
        contract._contract_path = path
        return contract


# Backwards-compatible re-export: RunLogReader was extracted to its own module
from lakelogic.cli.run_log_reader import RunLogReader  # noqa: F401


class PipelineDriver:
    """Registry-driven pipeline orchestrator for bronze/silver/gold layers."""

    def __init__(
        self,
        engine: str,
        max_workers: int,
        *,
        summary_path: Optional[Path] = None,
        summary_table: Optional[str] = None,
        summary_backend: Optional[str] = None,
        summary_database: Optional[str] = None,
        summary_table_format: Optional[str] = None,
        summary_merge_on_run_id: bool = True,
        metrics_path: Optional[Path] = None,
        metrics_backend: Optional[str] = None,
        metrics_host: Optional[str] = None,
        metrics_port: Optional[int] = None,
        metrics_prefix: Optional[str] = None,
        metrics_tags: Optional[Dict[str, str]] = None,
        overrides: Optional[Dict[str, Any]] = None,
        policy_pack: Optional[str] = None,
        policy_pack_dir: Optional[Path] = None,
        state_path: Optional[Path] = None,
        resume: bool = False,
        retries: int = 0,
        retry_backoff: float = 1.0,
        retry_max_delay: float = 60.0,
        approval_required: bool = False,
        approval_file: Optional[Path] = None,
        cache_references: bool = False,
        fail_fast: bool = True,
    ) -> None:
        """
        Initialize a pipeline driver.

        Args:
            engine: Execution engine (polars/pandas/duckdb/spark).
            max_workers: Maximum parallel tasks per layer.
            summary_path: Optional path to write a run summary JSON.
            fail_fast: Whether to stop on first error.
        """
        self.engine = engine
        self.max_workers = self._resolve_max_workers(max_workers)
        self.loader = ContractLoader()
        self.completed_lock = Lock()
        self.completed: set[str] = set()
        self.summary_lock = Lock()
        self.summary_path = summary_path
        self.summary_table = summary_table
        self.summary_backend = summary_backend
        self.summary_database = summary_database
        self.summary_table_format = summary_table_format
        self.summary_merge_on_run_id = summary_merge_on_run_id
        self.metrics_path = metrics_path
        self.metrics_backend = metrics_backend
        self.metrics_host = metrics_host
        self.metrics_port = metrics_port
        self.metrics_prefix = metrics_prefix or "lakelogic"
        self.metrics_tags = metrics_tags or {}
        self.metrics_snapshot: Dict[str, object] = {}
        self.prometheus_server: Optional[HTTPServer] = None
        self.prometheus_thread: Optional[Thread] = None
        self.overrides = overrides or {}
        self.policy_pack = policy_pack
        self.policy_pack_dir = policy_pack_dir
        self.state_path = state_path
        self.resume = resume
        self.retries = max(0, int(retries))
        self.retry_backoff = max(0.1, float(retry_backoff))
        self.retry_max_delay = max(1.0, float(retry_max_delay))
        self.approval_required = approval_required
        self.approval_file = approval_file
        self.cache_references = cache_references
        self.fail_fast = fail_fast
        self.pipeline_run_id = uuid4().hex
        self.summary: Dict[str, object] = {
            "run_id": self.pipeline_run_id,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "engine": engine,
            "metrics": {
                "total_contracts": 0,
                "successful": 0,
                "skipped_missing_upstream": 0,
                "missing_upstreams": 0,
                "skipped_no_sources": 0,
                "failed": 0,
                "full_loads": 0,
                "full_loads_due_to_missing_logs": 0,
            },
            "runs": [],
        }
        if (self.metrics_backend or "").lower() == "prometheus":
            self._start_prometheus_server()
        self.state = self._load_state()

    def run(
        self,
        registry_paths: Dict[str, Path],
        layers: List[str],
        window: Window,
        reprocess: bool,
        *,
        entity_filter: Optional[set[str]] = None,
        contract_filter: Optional[set[Path]] = None,
    ) -> None:
        """
        Run selected layers using the provided registries.

        Args:
            registry_paths: Mapping of layer keys to registry paths.
            layers: Layers to run in order.
            window: Window definition for incremental processing.
            reprocess: Whether to force reprocess semantics.
            entity_filter: Optional set of entity names to include.
            contract_filter: Optional set of contract paths to include.
        """
        registry_index = self._build_index(registry_paths, entity_filter, contract_filter)

        if "reference" in layers and registry_paths.get("reference"):
            self._run_layer(
                "reference",
                registry_paths["reference"],
                window,
                reprocess,
                registry_index,
                entity_filter,
                contract_filter,
            )

        if "bronze" in layers and registry_paths.get("system"):
            self._run_layer(
                "bronze",
                registry_paths["system"],
                window,
                reprocess,
                registry_index,
                entity_filter,
                contract_filter,
            )

        if "silver" in layers and registry_paths.get("system"):
            self._run_layer(
                "silver",
                registry_paths["system"],
                window,
                reprocess,
                registry_index,
                entity_filter,
                contract_filter,
            )

        if "gold" in layers and registry_paths.get("gold"):
            self._run_layer(
                "gold",
                registry_paths["gold"],
                window,
                reprocess,
                registry_index,
                entity_filter,
                contract_filter,
            )

        self._finalize_summary()

    def _run_layer(
        self,
        stage: str,
        registry_path: Path,
        window: Window,
        reprocess: bool,
        registry_index: Dict[str, Path],
        entity_filter: Optional[set[str]],
        contract_filter: Optional[set[Path]],
    ) -> None:
        """
        Execute all contracts for a given layer with dependency ordering.

        Args:
            stage: Layer name.
            registry_path: Registry YAML path.
            window: Window definition for incremental processing.
            reprocess: Whether to force reprocess semantics.
            registry_index: Mapping of dataset to contract path.
        """
        entries = self._load_registry(registry_path, stage, entity_filter, contract_filter)
        if not entries:
            return

        contracts = [(path, self.loader.load(path)) for path in entries]
        self._increment_metric("total_contracts", len(contracts))
        graph = self._build_graph(contracts)

        completed: Dict[str, bool] = {}
        in_progress: Dict[str, bool] = {}

        while graph:
            ready = [name for name, deps in graph.items() if all(d in completed for d in deps)]
            if not ready:
                remaining = ", ".join(graph.keys())
                raise RuntimeError(f"Unresolvable dependencies for stage {stage}: {remaining}")

            with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
                futures = {}
                for name in ready:
                    if in_progress.get(name):
                        continue
                    in_progress[name] = True
                    path = next(p for p, c in contracts if c.dataset == name)
                    contract = next(c for p, c in contracts if c.dataset == name)
                    futures[
                        pool.submit(
                            self._run_contract,
                            path,
                            contract,
                            stage,
                            window,
                            reprocess,
                            registry_index,
                        )
                    ] = name

                for future in as_completed(futures):
                    name = futures[future]
                    future.result()
                    completed[name] = True
                    graph.pop(name, None)

    def _run_contract(
        self,
        path: Path,
        contract: DataContract,
        stage: str,
        window: Window,
        reprocess: bool,
        registry_index: Dict[str, Path],
    ) -> None:
        """
        Execute a single contract with upstream checks and materialization.

        Args:
            path: Contract path.
            contract: DataContract instance.
            stage: Layer name.
            window: Window definition for incremental processing.
            reprocess: Whether to force reprocess semantics.
            registry_index: Mapping of dataset to contract path.
        """
        dataset = contract.dataset or path.stem
        run_key = self._state_key(dataset, stage, window)
        if self.resume and self._state_completed(run_key):
            logger.info(f"Skipping {dataset}: already completed in state.")
            self._increment_metric("successful", 1)
            return
        run_record = {
            "pipeline_run_id": self.pipeline_run_id,
            "dataset": dataset,
            "stage": stage,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "status": "started",
            "reason": None,
        }
        upstreams = contract.upstream or []

        upstream_ok, upstream_details = self._upstreams_fresh(contract, upstreams, registry_index, window)
        if not upstream_ok:
            detail_str = ", ".join(f"{item['upstream']}({item['reason']})" for item in upstream_details)
            logger.warning(f"Skipping {dataset}: upstream not fresh. {detail_str}")
            run_record["status"] = "skipped"
            run_record["reason"] = "missing_upstream"
            run_record["missing_upstreams"] = upstream_details
            self._increment_metric("skipped_missing_upstream", 1)
            self._increment_metric("missing_upstreams", len(upstream_details))
            self._record_run(run_record)
            return

        contract = self._prepare_contract_for_stage(contract, stage, reprocess)
        contract = self._apply_policy_pack(contract, stage)
        contract = self._apply_overrides(contract)
        if self.cache_references:
            try:
                contract.metadata["cache_reference_links"] = True
            except Exception:
                pass
        sources, effective_window, window_reason = self._resolve_sources(contract, window)
        run_record["window"] = {
            "label": effective_window.label,
            "start": effective_window.start.isoformat() if effective_window.start else None,
            "end": effective_window.end.isoformat() if effective_window.end else None,
        }
        run_record["window_reason"] = window_reason
        run_record["source_count"] = len(sources)

        if not sources:
            logger.warning(f"Skipping {dataset}: no sources resolved")
            run_record["status"] = "skipped"
            run_record["reason"] = "no_sources"
            self._increment_metric("skipped_no_sources", 1)
            self._record_run(run_record)
            return

        try:
            for source in sources:
                processor = DataProcessor(
                    engine=self.engine,
                    contract=contract,
                    stage=stage,
                    pipeline_run_id=self.pipeline_run_id,
                )
                good_df, bad_df = self._execute_with_retries(processor, source)
                processor.materialize(good_df, bad_df)
                self._evaluate_approvals(processor.last_report, contract, dataset)
            self._record_success(dataset)
            run_record["status"] = "success"
            if effective_window.label == "full":
                self._increment_metric("full_loads", 1)
                logger.info(f"{dataset}: full load executed")
            self._increment_metric("successful", 1)
            self._state_mark_completed(run_key)
        except Exception as exc:
            run_record["status"] = "failed"
            run_record["reason"] = str(exc)
            self._increment_metric("failed", 1)
            if self.fail_fast:
                raise
            logger.exception(f"Contract failed for {dataset}: {exc}")
            return
        finally:
            self._record_run(run_record)

    def _prepare_contract_for_stage(self, contract: DataContract, stage: str, reprocess: bool) -> DataContract:
        """
        Prepare a contract for a specific layer.

        Args:
            contract: DataContract instance.
            stage: Layer name.
            reprocess: Whether to force reprocess semantics.

        Returns:
            Prepared DataContract.
        """
        if stage == "bronze":
            clone = contract.model_copy(deep=True)
            clone.transformations = []
            clone.quality = Quality(row_rules=[], dataset_rules=[])
            return clone

        clone = contract.model_copy(deep=True)
        if reprocess and clone.materialization:
            clone.materialization.reprocess_policy = "overwrite_partition_safe"
        return clone

    def _resolve_sources(self, contract: DataContract, window: Window) -> Tuple[List[str], Window, Optional[str]]:
        """
        Resolve source paths for a contract and window.

        Args:
            contract: DataContract instance.
            window: Window definition.

        Returns:
            Tuple of (source list, effective window, window reason).
        """
        source_cfg = contract.source
        effective_window = window
        window_reason: Optional[str] = None

        if window.label == "last_success":
            last_success, reason = self._get_last_success(contract)
            if last_success:
                effective_window = Window(last_success, None, "incremental")
            else:
                window_reason = reason
                if reason in [
                    "run_log_table_missing",
                    "run_log_entry_missing",
                    "run_log_db_missing",
                ]:
                    logger.warning(f"{contract.dataset or contract.info.title}: {reason}; forcing full load.")
                elif reason == "no_run_log_table":
                    logger.warning(f"{contract.dataset or contract.info.title}: no run_log_table; forcing full load.")
                else:
                    logger.warning(
                        f"{contract.dataset or contract.info.title}: {reason or 'no_last_success'}; forcing full load."
                    )
                if reason:
                    self._increment_metric("full_loads_due_to_missing_logs", 1)
                effective_window = Window(None, None, "full")

        if not source_cfg:
            # Use environment-aware server path (respects LAKELOGIC_ENV / environments block)
            eff_server = contract.effective_server()
            if eff_server and eff_server.path:
                return [str(eff_server.path)], effective_window, window_reason
            return [], effective_window, window_reason

        raw_path = source_cfg.path
        if not raw_path:
            return [], effective_window, window_reason

        if str(raw_path).startswith("table:"):
            return [str(raw_path)], effective_window, window_reason

        base = getattr(contract, "_base_path", None)
        path = Path(raw_path)
        if not path.is_absolute() and base:
            path = base / path

        if source_cfg.type == "landing":
            if source_cfg.pattern:
                files = sorted(path.glob(source_cfg.pattern))
            else:
                files = [path]

            if source_cfg.load_mode in ["incremental", "cdc"]:
                start, end = effective_window.start, effective_window.end
                if start:
                    filtered = [p for p in files if self._file_in_window(p, start, end)]
                    if filtered:
                        files = filtered
                    else:
                        return (
                            [str(p) for p in files],
                            Window(None, None, "full"),
                            window_reason,
                        )

            return [str(p) for p in files], effective_window, window_reason

        return [str(path)], effective_window, window_reason

    def _upstreams_fresh(
        self,
        contract: DataContract,
        upstreams: List[str],
        registry_index: Dict[str, Path],
        window: Window,
    ) -> Tuple[bool, List[Dict[str, str]]]:
        """
        Check whether all upstream datasets are fresh enough.

        Args:
            upstreams: Upstream dataset names.
            registry_index: Mapping of dataset to contract path.
            window: Window definition.

        Returns:
            Tuple of (is_fresh, missing_details).
        """
        if not upstreams:
            return True, []

        metadata = contract.metadata or {}
        policy = (metadata.get("upstream_policy") or "strict").lower()
        grace_hours = metadata.get("upstream_grace_hours")
        grace_delta = None
        try:
            if grace_hours is not None:
                grace_delta = timedelta(hours=float(grace_hours))
        except Exception:
            grace_delta = None

        log_reader = RunLogReader(self.engine)
        missing = []
        for upstream in upstreams:
            with self.completed_lock:
                if upstream in self.completed:
                    continue
            path = registry_index.get(upstream)
            if not path:
                missing.append({"upstream": upstream, "reason": "missing_contract"})
                continue
            contract = self.loader.load(path)
            last_success, reason = log_reader.last_success_info(contract)
            if not last_success:
                missing.append({"upstream": upstream, "reason": reason or "missing_last_success"})
                continue
            if window.start and last_success < window.start:
                if grace_delta and last_success >= (window.start - grace_delta):
                    missing.append({"upstream": upstream, "reason": "stale_within_grace"})
                else:
                    missing.append({"upstream": upstream, "reason": "stale_last_success"})
        if missing:
            if policy == "warn":
                logger.warning(f"Upstream policy=warn; proceeding with stale/missing upstreams: {missing}")
                return True, missing
            return False, missing
        return True, []

    def _build_index(
        self,
        registry_paths: Dict[str, Path],
        entity_filter: Optional[set[str]],
        contract_filter: Optional[set[Path]],
    ) -> Dict[str, Path]:
        """
        Build a dataset -> contract path index from registries.

        Args:
            registry_paths: Mapping of registry paths.
            entity_filter: Optional set of entity names to include.
            contract_filter: Optional set of contract paths to include.

        Returns:
            Dict of dataset name to contract path.
        """
        index: Dict[str, Path] = {}
        for registry in registry_paths.values():
            if not registry:
                continue
            for path in self._load_registry(registry, None, entity_filter, contract_filter):
                contract = self.loader.load(path)
                if contract.dataset:
                    index[contract.dataset] = path
        return index

    def _build_graph(self, contracts: List[Tuple[Path, DataContract]]) -> Dict[str, List[str]]:
        """
        Build a dependency graph for contracts in a layer.

        Args:
            contracts: List of (path, contract).

        Returns:
            Dict of dataset -> list of upstream dataset names.
        """
        graph: Dict[str, List[str]] = {}
        dataset_names = {c.dataset for _, c in contracts if c.dataset}
        for _, contract in contracts:
            if not contract.dataset:
                continue
            deps = [d for d in (contract.upstream or []) if d in dataset_names]
            graph[contract.dataset] = deps
        return graph

    def _load_registry(
        self,
        registry_path: Path,
        stage: Optional[str],
        entity_filter: Optional[set[str]],
        contract_filter: Optional[set[Path]],
    ) -> List[Path]:
        """
        Load enabled contract paths from a registry for a given stage.

        Args:
            registry_path: Registry YAML path.
            stage: Optional layer stage (bronze/silver).
            entity_filter: Optional set of entity names to include.
            contract_filter: Optional set of contract paths to include.

        Returns:
            List of contract paths.
        """
        data = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
        entries = data.get("entries", [])
        paths: List[Path] = []
        for entry in entries:
            if entry.get("enabled") is False:
                continue
            entity_name = str(entry.get("entity") or "").strip()
            if entity_filter and entity_name not in entity_filter:
                continue
            contract_paths: List[str] = []
            contracts_block = entry.get("contracts")
            if stage and isinstance(contracts_block, dict):
                if contracts_block.get(stage):
                    contract_paths.append(contracts_block.get(stage))
            elif stage is None and isinstance(contracts_block, dict):
                contract_paths.extend([val for val in contracts_block.values() if val])

            if entry.get("contract_path"):
                contract_paths.append(entry.get("contract_path"))

            for contract_path in contract_paths:
                resolved = (registry_path.parent / contract_path).resolve()
                if contract_filter and resolved not in contract_filter:
                    continue
                paths.append(resolved)
        return paths

    def _file_in_window(self, path: Path, start: datetime, end: Optional[datetime]) -> bool:
        """
        Determine if a file belongs to a given window.

        Args:
            path: File path.
            start: Window start.
            end: Window end.

        Returns:
            True if file is within the window.
        """
        name = path.name
        date_match = re.search(r"\d{4}-\d{2}-\d{2}", name)
        if date_match:
            try:
                file_date = datetime.strptime(date_match.group(0), "%Y-%m-%d").replace(tzinfo=timezone.utc)
                if end:
                    return start <= file_date < end
                return file_date >= start
            except Exception:
                pass
        try:
            mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
        except Exception:
            return True
        if end:
            return start <= mtime < end
        return mtime >= start

    @staticmethod
    def _resolve_max_workers(max_workers: int) -> int:
        """
        Resolve adaptive concurrency based on CPU when max_workers <= 0.

        Args:
            max_workers: Requested max workers (<=0 means auto).

        Returns:
            Resolved worker count.
        """
        if max_workers and max_workers > 0:
            return max_workers
        cpu = os.cpu_count() or 4
        return min(32, max(1, cpu * 2))

    def _execute_with_retries(self, processor: DataProcessor, source: str) -> Tuple[Any, Any]:
        """
        Execute a contract with retry and exponential backoff.

        Args:
            processor: DataProcessor instance.
            source: Source path.

        Returns:
            Tuple of (good_df, bad_df).
        """
        attempt = 0
        delay = self.retry_backoff
        while True:
            try:
                result = processor.run_source(source)
                return result.good, result.bad
            except Exception as exc:
                attempt += 1
                if attempt > self.retries:
                    raise
                logger.warning(f"Retry {attempt}/{self.retries} for source {source} after error: {exc}")
                time.sleep(min(delay, self.retry_max_delay))
                delay = min(delay * 2, self.retry_max_delay)

    def _load_state(self) -> Dict[str, Any]:
        """
        Load state from disk if resume mode is enabled.
        """
        if not self.state_path:
            return {}
        if not self.state_path.exists():
            return {}
        try:
            return json.loads(self.state_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _save_state(self) -> None:
        """
        Persist state to disk.
        """
        if not self.state_path:
            return
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps(self.state, indent=2), encoding="utf-8")

    def _state_key(self, dataset: str, stage: str, window: Window) -> str:
        """
        Build a unique state key for a dataset/layer/window.
        """
        return f"{stage}:{dataset}:{window.label}:{window.start}:{window.end}"

    def _state_completed(self, key: str) -> bool:
        """
        Check if a state key is marked completed.
        """
        return bool(self.state.get("completed", {}).get(key))

    def _state_mark_completed(self, key: str) -> None:
        """
        Mark a state key as completed.
        """
        completed = self.state.setdefault("completed", {})
        completed[key] = True
        self._save_state()

    def _apply_overrides(self, contract: DataContract) -> DataContract:
        """
        Apply run-level overrides to a contract.
        """
        if not self.overrides:
            return contract
        data = contract.model_dump(by_alias=True)
        for key, value in self.overrides.items():
            self._set_nested_value(data, key, value)
        new_contract = DataContract(**data)
        new_contract._base_path = getattr(contract, "_base_path", None)
        return new_contract

    def _apply_policy_pack(self, contract: DataContract, stage: str) -> DataContract:
        """
        Merge a policy pack into a contract if configured.
        """
        policy_name = self.policy_pack
        if contract.metadata and contract.metadata.get("policy_pack"):
            policy_name = contract.metadata.get("policy_pack")
        if not policy_name:
            return contract

        pack_dir = self.policy_pack_dir or Path("policy_packs")
        pack_path = Path(policy_name)
        if not pack_path.is_absolute():
            pack_path = pack_dir / f"{policy_name}.yaml"
        if not pack_path.exists():
            logger.warning(f"Policy pack not found: {pack_path}")
            return contract

        pack_data = yaml.safe_load(pack_path.read_text(encoding="utf-8")) or {}
        stage_key = f"{stage}_defaults"
        defaults = pack_data.get("defaults", {}) or {}
        stage_defaults = pack_data.get(stage_key, {}) or {}

        # Extract transformations from defaults so list replacement doesn't clobber contract steps.
        defaults_transforms = None
        stage_transforms = None
        if isinstance(defaults, dict):
            defaults_transforms = defaults.pop("transformations", None)
            defaults_mode = defaults.pop("transformations_mode", None)
        else:
            defaults_mode = None
        if isinstance(stage_defaults, dict):
            stage_transforms = stage_defaults.pop("transformations", None)
            stage_mode = stage_defaults.pop("transformations_mode", None)
        else:
            stage_mode = None

        merged = contract.model_dump(by_alias=True)
        self._deep_merge(merged, defaults)
        self._deep_merge(merged, stage_defaults)
        if pack_data.get("quality"):
            merged_quality = merged.get("quality") or {}
            pack_quality = pack_data.get("quality") or {}
            for key, items in pack_quality.items():
                merged_quality.setdefault(key, [])
                if isinstance(merged_quality[key], list) and isinstance(items, list):
                    merged_quality[key].extend(items)
            merged["quality"] = merged_quality
        if pack_data.get("service_levels"):
            merged["service_levels"] = pack_data.get("service_levels")

        def _normalize_steps(raw: Any) -> List[Dict[str, Any]]:
            return raw if isinstance(raw, list) else []

        def _merge_steps(current: List[Dict[str, Any]], steps: Any, mode: Optional[str]) -> List[Dict[str, Any]]:
            new_steps = _normalize_steps(steps)
            if not new_steps:
                return current
            merge_mode = (mode or "prepend").lower()
            if merge_mode == "replace":
                return list(new_steps)
            if merge_mode == "append":
                return list(current) + list(new_steps)
            # default/prepend
            return list(new_steps) + list(current)

        # Merge policy-pack transformations (root and stage-specific).
        current_steps = merged.get("transformations")
        current_steps = current_steps if isinstance(current_steps, list) else []

        base_steps = (
            pack_data.get("transformations")
            if isinstance(pack_data.get("transformations"), list)
            else defaults_transforms
        )
        base_mode = pack_data.get("transformations_mode") or defaults_mode

        stage_steps_override = pack_data.get(f"{stage}_transformations")
        stage_steps = stage_steps_override if isinstance(stage_steps_override, list) else stage_transforms
        stage_mode = pack_data.get(f"{stage}_transformations_mode") or stage_mode or base_mode

        current_steps = _merge_steps(current_steps, base_steps, base_mode)
        current_steps = _merge_steps(current_steps, stage_steps, stage_mode)

        if current_steps:
            merged["transformations"] = current_steps

        new_contract = DataContract(**merged)
        new_contract._base_path = getattr(contract, "_base_path", None)
        return new_contract

    def _evaluate_approvals(self, report: Optional[Dict[str, Any]], contract: DataContract, dataset: str) -> None:
        """
        Enforce approval gates for schema drift or quarantine ratio.
        """
        if not report:
            return

        metadata = contract.metadata or {}
        approval_required = bool(metadata.get("approval_required", self.approval_required))
        approval_file = metadata.get("approval_file") or (str(self.approval_file) if self.approval_file else None)
        quarantine_threshold = metadata.get("approval_quarantine_ratio_threshold")
        drift_gate = metadata.get("approval_schema_drift", False)

        if not approval_required:
            return

        violations = []
        counts = report.get("counts") or {}
        ratio = counts.get("quarantine_ratio")
        if quarantine_threshold is not None and ratio is not None:
            try:
                threshold_val = float(quarantine_threshold)
                if threshold_val > 1:
                    threshold_val = threshold_val / 100.0
                if ratio > threshold_val:
                    violations.append(f"quarantine_ratio {ratio:.2f} > {threshold_val:.2f}")
            except Exception:
                pass

        drift = report.get("schema_drift") or {}
        if drift_gate and (drift.get("missing_fields") or drift.get("unknown_fields")):
            violations.append("schema_drift")

        if violations and approval_required:
            if approval_file and Path(approval_file).exists():
                logger.warning(f"Approval file found; proceeding despite: {', '.join(violations)}")
                return
            raise RuntimeError(f"Approval required for {dataset}: {', '.join(violations)}")

    @staticmethod
    def _set_nested_value(data: Dict[str, Any], path: str, value: Any) -> None:
        """
        Set a dotted-path value in a dict.
        """
        parts = path.split(".")
        cursor = data
        for part in parts[:-1]:
            if part not in cursor or not isinstance(cursor[part], dict):
                cursor[part] = {}
            cursor = cursor[part]
        cursor[parts[-1]] = value

    @staticmethod
    def _deep_merge(target: Dict[str, Any], source: Dict[str, Any]) -> None:
        """
        Deep merge source into target dict.
        """
        for key, value in source.items():
            if isinstance(value, dict) and isinstance(target.get(key), dict):
                PipelineDriver._deep_merge(target[key], value)
            else:
                target[key] = value

    def _record_success(self, dataset: str) -> None:
        """
        Mark a dataset as completed in the current run.

        Args:
            dataset: Dataset name.
        """
        with self.completed_lock:
            self.completed.add(dataset)

    def _get_last_success(self, contract: DataContract) -> Tuple[Optional[datetime], Optional[str]]:
        """
        Get last-success timestamp for a contract from run logs.

        Args:
            contract: DataContract instance.

        Returns:
            Timestamp or None.
        """
        log_reader = RunLogReader(self.engine)
        return log_reader.last_success_info(contract)

    def _record_run(self, record: Dict[str, object]) -> None:
        """
        Append a per-contract run record to the summary.

        Args:
            record: Run record data.
        """
        with self.summary_lock:
            record["ended_at"] = datetime.now(timezone.utc).isoformat()
            started_at = record.get("started_at")
            if started_at:
                try:
                    start_dt = datetime.fromisoformat(str(started_at))
                    end_dt = datetime.fromisoformat(str(record["ended_at"]))
                    record["duration_seconds"] = max(0.0, (end_dt - start_dt).total_seconds())
                except Exception:
                    record["duration_seconds"] = None
            self.summary["runs"].append(record)

    def _increment_metric(self, name: str, value: int) -> None:
        """
        Increment a summary metric.

        Args:
            name: Metric name.
            value: Increment value.
        """
        with self.summary_lock:
            metrics = self.summary["metrics"]
            metrics[name] = int(metrics.get(name, 0)) + value

    def _finalize_summary(self) -> None:
        """Finalize and optionally persist the run summary."""
        from lakelogic.cli.observability import finalize_summary

        finalize_summary(self.summary, self.summary_path)
        self._write_summary_table()
        self._emit_metrics()
        self._stop_prometheus_server()

    def _write_summary_table(self) -> None:
        """Write a pipeline summary row to a table backend."""
        from lakelogic.cli.observability import write_summary_table

        write_summary_table(
            self.summary,
            self.summary_table,
            self.summary_backend,
            self.summary_database,
            self.summary_table_format,
            self.summary_merge_on_run_id,
            self.engine,
        )

    def _flatten_summary(self) -> Dict[str, object]:
        """Flatten summary data into a table-oriented record."""
        from lakelogic.cli.observability import flatten_summary

        return flatten_summary(self.summary)

    def _emit_metrics(self) -> None:
        """Emit metrics to a JSON file or StatsD endpoint."""
        from lakelogic.cli.observability import emit_metrics

        self.metrics_snapshot = emit_metrics(
            self.summary,
            self.metrics_path,
            self.metrics_backend,
            self.metrics_host,
            self.metrics_port,
            self.metrics_prefix,
            self.metrics_tags,
        )

    def _format_prometheus(self) -> str:
        """Format metrics in Prometheus exposition format."""
        from lakelogic.cli.observability import format_prometheus

        return format_prometheus(self.metrics_snapshot, self.metrics_prefix or "lakelogic")

    def _start_prometheus_server(self) -> None:
        """Start a lightweight Prometheus /metrics HTTP server."""
        from lakelogic.cli.observability import start_prometheus_server

        server, thread = start_prometheus_server(
            self.metrics_host,
            self.metrics_port,
            lambda: self.metrics_snapshot,
            self.metrics_prefix or "lakelogic",
        )
        self.prometheus_server = server
        self.prometheus_thread = thread

    def _stop_prometheus_server(self) -> None:
        """Stop the Prometheus HTTP server if running."""
        from lakelogic.cli.observability import stop_prometheus_server

        stop_prometheus_server(self.prometheus_server, self.prometheus_thread)
        self.prometheus_server = None
        self.prometheus_thread = None


# Backwards-compatible re-exports: parser functions extracted to cli_parsers.py
from lakelogic.cli.cli_parsers import (  # noqa: F401, E402
    parse_layers,
    parse_entities,
    parse_contracts,
    parse_metrics_tags,
    parse_overrides,
    build_backfill_windows,
    parse_window,
)


def main() -> None:
    """CLI entrypoint for the registry-driven pipeline driver."""
    parser = argparse.ArgumentParser(description="LakeLogic pipeline driver (registry-based).")
    parser.add_argument(
        "--registry",
        required=True,
        help="Path to system registry (bronze/silver contracts).",
    )
    parser.add_argument("--reference-registry", help="Path to reference registry (optional).")
    parser.add_argument("--gold-registry", help="Path to gold registry (optional).")
    parser.add_argument(
        "--layers",
        default="bronze,silver,gold",
        help="Layers to run (comma-separated).",
    )
    parser.add_argument("--entities", help="Limit execution to specific entities (comma-separated).")
    parser.add_argument(
        "--contracts",
        help="Limit execution to specific contract paths (comma-separated).",
    )
    parser.add_argument(
        "--strict-layer-order",
        action="store_true",
        help="Validate the order of layers strictly.",
    )
    parser.add_argument(
        "--window",
        default="last_success",
        help="Window: last_success | yesterday | none | range",
    )
    parser.add_argument("--window-start-date", help="Window start date (YYYY-MM-DD) for --window range.")
    parser.add_argument("--window-end-date", help="Window end date (YYYY-MM-DD) for --window range.")
    parser.add_argument("--reprocess-date", help="Reprocess a specific date (YYYY-MM-DD).")
    parser.add_argument("--reprocess-start-date", help="Reprocess start date (YYYY-MM-DD).")
    parser.add_argument("--reprocess-end-date", help="Reprocess end date (YYYY-MM-DD).")
    parser.add_argument("--engine", default="polars")
    parser.add_argument("--max-workers", type=int, default=4, help="Max parallel workers (0 = auto).")
    parser.add_argument("--summary-path", help="Write a run summary JSON to this path.")
    parser.add_argument("--summary-table", help="Write a pipeline summary row to a table backend.")
    parser.add_argument("--summary-backend", help="Summary backend: spark | duckdb | sqlite.")
    parser.add_argument("--summary-database", help="Database path for duckdb/sqlite summary tables.")
    parser.add_argument(
        "--summary-table-format",
        help="Table format for Spark summary tables (default delta).",
    )
    parser.add_argument(
        "--summary-merge-on-run-id",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Merge summary rows on run_id (Spark only).",
    )
    parser.add_argument("--metrics-path", help="Write a metrics JSON payload to this path.")
    parser.add_argument("--metrics-backend", help="Metrics backend: statsd | prometheus.")
    parser.add_argument("--metrics-host", help="StatsD host (default 127.0.0.1).")
    parser.add_argument("--metrics-port", type=int, help="StatsD port (default 8125).")
    parser.add_argument("--metrics-prefix", help="StatsD metric prefix (default lakelogic).")
    parser.add_argument("--metrics-tags", help="Comma-separated tags (key=value) for metrics.")
    parser.add_argument(
        "--set",
        dest="overrides",
        action="append",
        help="Override contract fields (key=value).",
    )
    parser.add_argument("--policy-pack", help="Apply a policy pack by name.")
    parser.add_argument("--policy-pack-dir", help="Directory containing policy packs.")
    parser.add_argument("--state-path", help="State file for partial resume.")
    parser.add_argument("--resume", action="store_true", help="Resume from last successful state.")
    parser.add_argument("--retries", type=int, default=0, help="Retry count for transient failures.")
    parser.add_argument(
        "--retry-backoff",
        type=float,
        default=1.0,
        help="Initial retry backoff in seconds.",
    )
    parser.add_argument(
        "--retry-max-delay",
        type=float,
        default=60.0,
        help="Max retry delay in seconds.",
    )
    parser.add_argument(
        "--approval-required",
        action="store_true",
        help="Require approvals on drift/quarantine thresholds.",
    )
    parser.add_argument("--approval-file", help="Approval file path to bypass approval gates.")
    parser.add_argument(
        "--cache-references",
        action="store_true",
        help="Cache reference datasets across runs.",
    )
    parser.add_argument("--backfill-start-date", help="Backfill start date (YYYY-MM-DD).")
    parser.add_argument("--backfill-end-date", help="Backfill end date (YYYY-MM-DD).")
    parser.add_argument(
        "--backfill-granularity",
        default="day",
        help="Backfill granularity: day | week.",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continue running after a contract failure.",
    )
    args = parser.parse_args()

    layers = parse_layers(args.layers, strict=args.strict_layer_order)
    entity_filter = parse_entities(args.entities)
    contract_filter = parse_contracts(args.contracts)
    metrics_tags = parse_metrics_tags(args.metrics_tags)
    window, reprocess = parse_window(
        args.window,
        args.window_start_date,
        args.window_end_date,
        args.reprocess_date,
        args.reprocess_start_date,
        args.reprocess_end_date,
    )
    overrides = parse_overrides(args.overrides)
    policy_pack_dir = Path(args.policy_pack_dir) if args.policy_pack_dir else None
    state_path = Path(args.state_path) if args.state_path else None
    if args.resume and not state_path:
        state_path = Path("logs/driver_state.json")

    driver = PipelineDriver(
        args.engine,
        args.max_workers,
        summary_path=Path(args.summary_path) if args.summary_path else None,
        summary_table=args.summary_table,
        summary_backend=args.summary_backend,
        summary_database=args.summary_database,
        summary_table_format=args.summary_table_format,
        summary_merge_on_run_id=args.summary_merge_on_run_id,
        metrics_path=Path(args.metrics_path) if args.metrics_path else None,
        metrics_backend=args.metrics_backend,
        metrics_host=args.metrics_host,
        metrics_port=args.metrics_port,
        metrics_prefix=args.metrics_prefix,
        metrics_tags=metrics_tags,
        overrides=overrides,
        policy_pack=args.policy_pack,
        policy_pack_dir=policy_pack_dir,
        state_path=state_path,
        resume=args.resume,
        retries=args.retries,
        retry_backoff=args.retry_backoff,
        retry_max_delay=args.retry_max_delay,
        approval_required=args.approval_required,
        approval_file=Path(args.approval_file) if args.approval_file else None,
        cache_references=args.cache_references,
        fail_fast=not args.continue_on_error,
    )

    registry_paths = {
        "system": Path(args.registry),
        "reference": Path(args.reference_registry) if args.reference_registry else None,
        "gold": Path(args.gold_registry) if args.gold_registry else None,
    }

    if window.label == "last_success":
        print("Window=last_success: using run log tables (if available).")

    if args.backfill_start_date and args.backfill_end_date:
        windows = build_backfill_windows(args.backfill_start_date, args.backfill_end_date, args.backfill_granularity)
        for backfill_window in windows:
            driver.run(
                registry_paths,
                layers,
                backfill_window,
                reprocess=False,
                entity_filter=entity_filter,
                contract_filter=contract_filter,
            )
    else:
        driver.run(
            registry_paths,
            layers,
            window,
            reprocess,
            entity_filter=entity_filter,
            contract_filter=contract_filter,
        )


if __name__ == "__main__":
    main()
