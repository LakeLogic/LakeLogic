"""
Registry-driven pipeline driver for LakeGuard.

This module orchestrates bronze/silver/gold runs from contract registries, supports
incremental windows (last_success), and reprocessing of late-arriving data.
"""

import argparse
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from http.server import BaseHTTPRequestHandler, HTTPServer
from threading import Thread
from pathlib import Path
from threading import Lock
from typing import Dict, List, Optional, Tuple

import yaml

from lakeguard import DataProcessor
from lakeguard.core.models import DataContract, Quality
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
        return contract


class RunLogReader:
    """Read last-success timestamps from configured run log tables."""

    def __init__(self, engine: str) -> None:
        self.engine = engine

    def last_success(self, contract: DataContract) -> Optional[datetime]:
        """
        Return the last successful run timestamp for a contract from run log tables.

        Args:
            contract: DataContract instance.

        Returns:
            Timestamp of last run or None if not found.
        """
        timestamp, _ = self.last_success_info(contract)
        return timestamp

    def last_success_info(self, contract: DataContract) -> Tuple[Optional[datetime], Optional[str]]:
        """
        Return last-success timestamp with a reason code when missing.

        Args:
            contract: DataContract instance.

        Returns:
            Tuple of (timestamp, reason).
        """
        metadata = contract.metadata or {}
        table_name = metadata.get("run_log_table")
        if not table_name:
            return None, "no_run_log_table"

        backend = (metadata.get("run_log_backend") or ("spark" if self.engine == "spark" else "duckdb")).lower()

        if backend == "spark":
            return self._read_spark(table_name, contract)
        if backend == "duckdb":
            return self._read_duckdb(table_name, contract, metadata)
        if backend == "sqlite":
            return self._read_sqlite(table_name, contract, metadata)
        return None, "unsupported_backend"

    def _contract_key(self, contract: DataContract) -> str:
        """
        Resolve the run-log contract key.

        Args:
            contract: DataContract instance.

        Returns:
            Identifier used in run log tables.
        """
        if contract.info and contract.info.title:
            return contract.info.title
        if contract.dataset:
            return contract.dataset
        return "unknown"

    def _read_spark(self, table_name: str, contract: DataContract) -> Tuple[Optional[datetime], Optional[str]]:
        """
        Read last-success timestamp from a Spark run log table.

        Args:
            table_name: Spark table name.
            contract: DataContract instance.

        Returns:
            Timestamp or None.
        """
        try:
            from pyspark.sql import SparkSession
        except Exception:
            return None, "spark_unavailable"
        spark = SparkSession.builder.getOrCreate()
        if not spark.catalog.tableExists(table_name):
            return None, "run_log_table_missing"
        key = self._contract_key(contract)
        df = spark.sql(f"SELECT MAX(timestamp) AS last_ts FROM {table_name} WHERE contract = '{key}'")
        rows = df.collect()
        if not rows:
            return None, "run_log_entry_missing"
        value = rows[0]["last_ts"]
        if not value:
            return None, "run_log_entry_missing"
        return self._parse_timestamp(value), None

    def _read_duckdb(self, table_name: str, contract: DataContract, metadata: Dict[str, str]) -> Tuple[Optional[datetime], Optional[str]]:
        """
        Read last-success timestamp from a DuckDB run log table.

        Args:
            table_name: Table name.
            contract: DataContract instance.
            metadata: Contract metadata.

        Returns:
            Timestamp or None.
        """
        try:
            import duckdb
        except Exception:
            return None, "duckdb_unavailable"

        base_path = getattr(contract, "_base_path", None)
        db_path = metadata.get("run_log_database") or "logs/lakeguard_run_logs.duckdb"
        db_path = self._resolve_path(db_path, base_path)
        if not db_path.exists():
            return None, "run_log_db_missing"

        key = self._contract_key(contract)
        con = duckdb.connect(database=str(db_path))
        try:
            try:
                result = con.execute(
                    f"SELECT MAX(timestamp) FROM {table_name} WHERE contract = ?",
                    [key],
                ).fetchone()
            except Exception:
                return None, "run_log_table_missing"
        finally:
            con.close()
        if not result or not result[0]:
            return None, "run_log_entry_missing"
        return self._parse_timestamp(result[0]), None

    def _read_sqlite(self, table_name: str, contract: DataContract, metadata: Dict[str, str]) -> Tuple[Optional[datetime], Optional[str]]:
        """
        Read last-success timestamp from a SQLite run log table.

        Args:
            table_name: Table name.
            contract: DataContract instance.
            metadata: Contract metadata.

        Returns:
            Timestamp or None.
        """
        import sqlite3

        base_path = getattr(contract, "_base_path", None)
        db_path = metadata.get("run_log_database") or "logs/lakeguard_run_logs.sqlite"
        db_path = self._resolve_path(db_path, base_path)
        if not db_path.exists():
            return None, "run_log_db_missing"

        key = self._contract_key(contract)
        conn = sqlite3.connect(str(db_path))
        try:
            try:
                cur = conn.execute(
                    f"SELECT MAX(timestamp) FROM {table_name} WHERE contract = ?",
                    (key,),
                )
                result = cur.fetchone()
            except Exception:
                return None, "run_log_table_missing"
        finally:
            conn.close()
        if not result or not result[0]:
            return None, "run_log_entry_missing"
        return self._parse_timestamp(result[0]), None

    @staticmethod
    def _parse_timestamp(value) -> Optional[datetime]:
        """
        Parse a timestamp value into a datetime.

        Args:
            value: Timestamp-like value.

        Returns:
            datetime or None.
        """
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value))
        except Exception:
            return None

    @staticmethod
    def _resolve_path(raw: str, base: Optional[Path]) -> Path:
        """
        Resolve a raw path against a base directory.

        Args:
            raw: Raw path string.
            base: Base directory.

        Returns:
            Resolved Path.
        """
        path = Path(raw)
        if not path.is_absolute() and base:
            path = base / path
        return path


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
        self.max_workers = max_workers
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
        self.metrics_prefix = metrics_prefix or "lakeguard"
        self.metrics_tags = metrics_tags or {}
        self.metrics_snapshot: Dict[str, object] = {}
        self.prometheus_server: Optional[HTTPServer] = None
        self.prometheus_thread: Optional[Thread] = None
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
                    futures[pool.submit(
                        self._run_contract,
                        path,
                        contract,
                        stage,
                        window,
                        reprocess,
                        registry_index
                    )] = name

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
        run_record = {
            "pipeline_run_id": self.pipeline_run_id,
            "dataset": dataset,
            "stage": stage,
            "started_at": datetime.now(timezone.utc).isoformat(),
            "status": "started",
            "reason": None,
        }
        upstreams = contract.upstream or []

        upstream_ok, upstream_details = self._upstreams_fresh(upstreams, registry_index, window)
        if not upstream_ok:
            detail_str = ", ".join(
                f"{item['upstream']}({item['reason']})" for item in upstream_details
            )
            logger.warning(f"Skipping {dataset}: upstream not fresh. {detail_str}")
            run_record["status"] = "skipped"
            run_record["reason"] = "missing_upstream"
            run_record["missing_upstreams"] = upstream_details
            self._increment_metric("skipped_missing_upstream", 1)
            self._increment_metric("missing_upstreams", len(upstream_details))
            self._record_run(run_record)
            return

        contract = self._prepare_contract_for_stage(contract, stage, reprocess)
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
                processor = DataProcessor(engine=self.engine, contract=contract, pipeline_run_id=self.pipeline_run_id)
                good_df, bad_df = processor.run_source(source)
                processor.materialize(good_df, bad_df)
            self._record_success(dataset)
            run_record["status"] = "success"
            if effective_window.label == "full":
                self._increment_metric("full_loads", 1)
                logger.info(f"{dataset}: full load executed")
            self._increment_metric("successful", 1)
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
                if reason in ["run_log_table_missing", "run_log_entry_missing", "run_log_db_missing"]:
                    logger.warning(f"{contract.dataset or contract.info.title}: {reason}; forcing full load.")
                elif reason == "no_run_log_table":
                    logger.warning(f"{contract.dataset or contract.info.title}: no run_log_table; forcing full load.")
                else:
                    logger.warning(f"{contract.dataset or contract.info.title}: {reason or 'no_last_success'}; forcing full load.")
                if reason:
                    self._increment_metric("full_loads_due_to_missing_logs", 1)
                effective_window = Window(None, None, "full")

        if not source_cfg:
            if contract.server and contract.server.path:
                return [str(contract.server.path)], effective_window, window_reason
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
                        return [str(p) for p in files], Window(None, None, "full"), window_reason

            return [str(p) for p in files], effective_window, window_reason

        return [str(path)], effective_window, window_reason

    def _upstreams_fresh(
        self,
        upstreams: List[str],
        registry_index: Dict[str, Path],
        window: Window
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
                missing.append({"upstream": upstream, "reason": "stale_last_success"})
        if missing:
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
        """
        Finalize and optionally persist the run summary.
        """
        with self.summary_lock:
            self.summary["finished_at"] = datetime.now(timezone.utc).isoformat()
            try:
                start_dt = datetime.fromisoformat(str(self.summary["started_at"]))
                end_dt = datetime.fromisoformat(str(self.summary["finished_at"]))
                self.summary["duration_seconds"] = max(0.0, (end_dt - start_dt).total_seconds())
            except Exception:
                self.summary["duration_seconds"] = None
        if self.summary_path:
            self.summary_path.parent.mkdir(parents=True, exist_ok=True)
            self.summary_path.write_text(json.dumps(self.summary, indent=2), encoding="utf-8")
            logger.info(f"Wrote run summary to {self.summary_path}")
        self._write_summary_table()
        self._emit_metrics()
        self._stop_prometheus_server()

    def _write_summary_table(self) -> None:
        """
        Write a pipeline summary row to a table backend.
        """
        if not self.summary_table:
            return

        backend = (self.summary_backend or ("spark" if self.engine == "spark" else "duckdb")).lower()
        record = self._flatten_summary()

        if backend == "spark":
            try:
                from pyspark.sql import SparkSession
            except Exception as exc:
                logger.warning(f"Summary table backend 'spark' unavailable: {exc}")
                return

            spark = SparkSession.builder.getOrCreate()
            table_name = self.summary_table
            parts = table_name.split(".")
            if len(parts) == 2:
                spark.sql(f"CREATE DATABASE IF NOT EXISTS {parts[0]}")
            elif len(parts) >= 3:
                schema = ".".join(parts[:-1])
                spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

            df = spark.createDataFrame([record])
            if spark.catalog.tableExists(table_name):
                try:
                    existing_cols = set(spark.table(table_name).columns)
                    if "summary_json" not in existing_cols:
                        spark.sql(f"ALTER TABLE {table_name} ADD COLUMNS (summary_json STRING)")
                except Exception as exc:
                    logger.warning(f"Failed to align summary table schema for {table_name}: {exc}")

                if self.summary_merge_on_run_id:
                    view_name = f"lakeguard_summary_updates_{uuid4().hex}"
                    df.createOrReplaceTempView(view_name)
                    try:
                        spark.sql(f"""
                            MERGE INTO {table_name} AS target
                            USING {view_name} AS source
                            ON target.run_id = source.run_id
                            WHEN MATCHED THEN UPDATE SET *
                            WHEN NOT MATCHED THEN INSERT *
                        """)
                    except Exception as exc:
                        logger.warning(f"Summary table merge failed for {table_name}: {exc}")
                        return
                    finally:
                        try:
                            spark.catalog.dropTempView(view_name)
                        except Exception:
                            pass
                else:
                    fmt = self.summary_table_format or "delta"
                    df.write.mode("append").format(fmt).saveAsTable(table_name)
            else:
                fmt = self.summary_table_format or "delta"
                df.write.mode("overwrite").format(fmt).saveAsTable(table_name)
            logger.info(f"Wrote pipeline summary to Spark table {table_name}")
            return

        if backend == "duckdb":
            try:
                import duckdb
            except Exception as exc:
                logger.warning(f"Summary table backend 'duckdb' unavailable: {exc}")
                return

            db_path = Path(self.summary_database or "logs/lakeguard_pipeline_runs.duckdb")
            db_path.parent.mkdir(parents=True, exist_ok=True)
            table_name = self.summary_table
            con = duckdb.connect(database=str(db_path))
            try:
                con.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        run_id VARCHAR,
                        started_at VARCHAR,
                        finished_at VARCHAR,
                        duration_seconds DOUBLE,
                        engine VARCHAR,
                        total_contracts BIGINT,
                        successful BIGINT,
                        failed BIGINT,
                        skipped_missing_upstream BIGINT,
                        skipped_no_sources BIGINT,
                        full_loads BIGINT,
                        full_loads_due_to_missing_logs BIGINT,
                        missing_upstreams BIGINT,
                        summary_json VARCHAR
                    )
                """)
                con.execute(f"ALTER TABLE {table_name} ADD COLUMN IF NOT EXISTS summary_json VARCHAR")
                con.execute(
                    f"""
                    INSERT INTO {table_name} (
                        run_id,
                        started_at,
                        finished_at,
                        duration_seconds,
                        engine,
                        total_contracts,
                        successful,
                        failed,
                        skipped_missing_upstream,
                        skipped_no_sources,
                        full_loads,
                        full_loads_due_to_missing_logs,
                        missing_upstreams,
                        summary_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        record["run_id"],
                        record["started_at"],
                        record["finished_at"],
                        record["duration_seconds"],
                        record["engine"],
                        record["total_contracts"],
                        record["successful"],
                        record["failed"],
                        record["skipped_missing_upstream"],
                        record["skipped_no_sources"],
                        record["full_loads"],
                        record["full_loads_due_to_missing_logs"],
                        record["missing_upstreams"],
                        record["summary_json"],
                    ],
                )
            finally:
                con.close()
            logger.info(f"Wrote pipeline summary to DuckDB table {table_name} ({db_path})")
            return

        if backend == "sqlite":
            import sqlite3

            db_path = Path(self.summary_database or "logs/lakeguard_pipeline_runs.sqlite")
            db_path.parent.mkdir(parents=True, exist_ok=True)
            table_name = self.summary_table.replace(".", "_")
            if table_name != self.summary_table:
                logger.warning(f"SQLite does not support schemas. Using table name '{table_name}' instead of '{self.summary_table}'.")
            con = sqlite3.connect(str(db_path))
            try:
                con.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name} (
                        run_id TEXT,
                        started_at TEXT,
                        finished_at TEXT,
                        duration_seconds REAL,
                        engine TEXT,
                        total_contracts INTEGER,
                        successful INTEGER,
                        failed INTEGER,
                        skipped_missing_upstream INTEGER,
                        skipped_no_sources INTEGER,
                        full_loads INTEGER,
                        full_loads_due_to_missing_logs INTEGER,
                        missing_upstreams INTEGER,
                        summary_json TEXT
                    )
                """)
                try:
                    cols = [row[1] for row in con.execute(f"PRAGMA table_info({table_name})").fetchall()]
                    if "summary_json" not in cols:
                        con.execute(f"ALTER TABLE {table_name} ADD COLUMN summary_json TEXT")
                except Exception:
                    pass
                con.execute(
                    f"""
                    INSERT INTO {table_name} (
                        run_id,
                        started_at,
                        finished_at,
                        duration_seconds,
                        engine,
                        total_contracts,
                        successful,
                        failed,
                        skipped_missing_upstream,
                        skipped_no_sources,
                        full_loads,
                        full_loads_due_to_missing_logs,
                        missing_upstreams,
                        summary_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    [
                        record["run_id"],
                        record["started_at"],
                        record["finished_at"],
                        record["duration_seconds"],
                        record["engine"],
                        record["total_contracts"],
                        record["successful"],
                        record["failed"],
                        record["skipped_missing_upstream"],
                        record["skipped_no_sources"],
                        record["full_loads"],
                        record["full_loads_due_to_missing_logs"],
                        record["missing_upstreams"],
                        record["summary_json"],
                    ],
                )
                con.commit()
            finally:
                con.close()
            logger.info(f"Wrote pipeline summary to SQLite table {table_name} ({db_path})")
            return

        if backend == "snowflake":
            try:
                import snowflake.connector
                from snowflake.connector.pandas_tools import write_pandas
            except Exception as exc:
                logger.warning(f"Summary table backend 'snowflake' unavailable: {exc}")
                return

            params = {
                "account": os.getenv("SNOWFLAKE_ACCOUNT"),
                "user": os.getenv("SNOWFLAKE_USER"),
                "password": os.getenv("SNOWFLAKE_PASSWORD"),
                "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
                "database": os.getenv("SNOWFLAKE_DATABASE"),
                "schema": os.getenv("SNOWFLAKE_SCHEMA"),
                "role": os.getenv("SNOWFLAKE_ROLE"),
            }
            missing = [k for k, v in params.items() if k in ["account", "user", "password"] and not v]
            if missing:
                logger.warning(f"Snowflake summary write missing required fields: {', '.join(missing)}")
                return

            table_name = self.summary_table
            parts = table_name.split(".")
            if len(parts) >= 3:
                params["database"] = parts[-3]
                params["schema"] = parts[-2]
                table_only = parts[-1]
            elif len(parts) == 2:
                params["schema"] = parts[-2]
                table_only = parts[-1]
            else:
                table_only = table_name

            try:
                import pandas as pd
            except Exception as exc:
                logger.warning(f"Snowflake summary write requires pandas: {exc}")
                return

            pdf = pd.DataFrame([record])
            conn = snowflake.connector.connect(**{k: v for k, v in params.items() if v})
            try:
                ddl_columns = [
                    ("run_id", "STRING"),
                    ("started_at", "STRING"),
                    ("finished_at", "STRING"),
                    ("duration_seconds", "FLOAT"),
                    ("engine", "STRING"),
                    ("total_contracts", "NUMBER"),
                    ("successful", "NUMBER"),
                    ("failed", "NUMBER"),
                    ("skipped_missing_upstream", "NUMBER"),
                    ("skipped_no_sources", "NUMBER"),
                    ("full_loads", "NUMBER"),
                    ("full_loads_due_to_missing_logs", "NUMBER"),
                    ("missing_upstreams", "NUMBER"),
                    ("summary_json", "VARIANT"),
                ]
                column_ddl = ", ".join(f"{name} {dtype}" for name, dtype in ddl_columns)
                conn.cursor().execute(f"CREATE TABLE IF NOT EXISTS {table_only} ({column_ddl})")
                for name, dtype in ddl_columns:
                    conn.cursor().execute(f"ALTER TABLE {table_only} ADD COLUMN IF NOT EXISTS {name} {dtype}")
                write_pandas(
                    conn,
                    pdf,
                    table_name=table_only,
                    database=params.get("database"),
                    schema=params.get("schema"),
                    auto_create_table=True,
                    overwrite=False,
                )
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
            logger.info(f"Wrote pipeline summary to Snowflake table {table_only}")
            return

        if backend == "bigquery":
            try:
                from google.cloud import bigquery  # type: ignore
            except Exception as exc:
                logger.warning(f"Summary table backend 'bigquery' unavailable: {exc}")
                return

            table_name = self.summary_table
            parts = table_name.split(".")
            project = os.getenv("BIGQUERY_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
            if len(parts) == 3:
                project = parts[0]
                dataset = parts[1]
                table_only = parts[2]
            elif len(parts) == 2:
                dataset = parts[0]
                table_only = parts[1]
            else:
                logger.warning("BigQuery summary table name must be dataset.table or project.dataset.table")
                return

            if not project:
                logger.warning("BigQuery summary write missing project (bigquery_project or GOOGLE_CLOUD_PROJECT).")
                return

            try:
                import pandas as pd
            except Exception as exc:
                logger.warning(f"BigQuery summary write requires pandas: {exc}")
                return

            client = bigquery.Client(project=project)
            pdf = pd.DataFrame([record])
            table_id = f"{project}.{dataset}.{table_only}"
            desired_schema = [
                bigquery.SchemaField("run_id", "STRING"),
                bigquery.SchemaField("started_at", "STRING"),
                bigquery.SchemaField("finished_at", "STRING"),
                bigquery.SchemaField("duration_seconds", "FLOAT"),
                bigquery.SchemaField("engine", "STRING"),
                bigquery.SchemaField("total_contracts", "INTEGER"),
                bigquery.SchemaField("successful", "INTEGER"),
                bigquery.SchemaField("failed", "INTEGER"),
                bigquery.SchemaField("skipped_missing_upstream", "INTEGER"),
                bigquery.SchemaField("skipped_no_sources", "INTEGER"),
                bigquery.SchemaField("full_loads", "INTEGER"),
                bigquery.SchemaField("full_loads_due_to_missing_logs", "INTEGER"),
                bigquery.SchemaField("missing_upstreams", "INTEGER"),
                bigquery.SchemaField("summary_json", "STRING"),
            ]
            try:
                table = client.get_table(table_id)
                existing = {field.name for field in table.schema}
                updates = [field for field in desired_schema if field.name not in existing]
                if updates:
                    table.schema = list(table.schema) + updates
                    client.update_table(table, ["schema"])
            except Exception:
                table = bigquery.Table(table_id, schema=desired_schema)
                client.create_table(table, exists_ok=True)

            job_config = bigquery.LoadJobConfig(
                write_disposition="WRITE_APPEND",
                create_disposition="CREATE_IF_NEEDED",
                schema=desired_schema,
            )
            job = client.load_table_from_dataframe(pdf, table_id, job_config=job_config)
            job.result()
            logger.info(f"Wrote pipeline summary to BigQuery table {table_id}")
            return

        logger.warning(f"Unsupported summary backend: {backend}")

    def _flatten_summary(self) -> Dict[str, object]:
        """
        Flatten summary data into a table-oriented record.
        """
        metrics = self.summary.get("metrics", {})
        return {
            "run_id": self.summary.get("run_id"),
            "started_at": self.summary.get("started_at"),
            "finished_at": self.summary.get("finished_at"),
            "duration_seconds": self.summary.get("duration_seconds"),
            "engine": self.summary.get("engine"),
            "total_contracts": metrics.get("total_contracts"),
            "successful": metrics.get("successful"),
            "failed": metrics.get("failed"),
            "skipped_missing_upstream": metrics.get("skipped_missing_upstream"),
            "skipped_no_sources": metrics.get("skipped_no_sources"),
            "full_loads": metrics.get("full_loads"),
            "full_loads_due_to_missing_logs": metrics.get("full_loads_due_to_missing_logs"),
            "missing_upstreams": metrics.get("missing_upstreams"),
            "summary_json": json.dumps(self.summary, default=str),
        }

    def _emit_metrics(self) -> None:
        """
        Emit metrics to a JSON file or StatsD endpoint.
        """
        record = self._flatten_summary()
        metrics = {
            "run_id": record.get("run_id"),
            "engine": record.get("engine"),
            "duration_seconds": record.get("duration_seconds"),
            "total_contracts": record.get("total_contracts"),
            "successful": record.get("successful"),
            "failed": record.get("failed"),
            "skipped_missing_upstream": record.get("skipped_missing_upstream"),
            "skipped_no_sources": record.get("skipped_no_sources"),
            "full_loads": record.get("full_loads"),
            "full_loads_due_to_missing_logs": record.get("full_loads_due_to_missing_logs"),
            "missing_upstreams": record.get("missing_upstreams"),
        }
        self.metrics_snapshot = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "tags": self.metrics_tags,
            "metrics": metrics,
        }

        if self.metrics_path:
            self.metrics_path.parent.mkdir(parents=True, exist_ok=True)
            self.metrics_path.write_text(json.dumps(self.metrics_snapshot, indent=2), encoding="utf-8")
            logger.info(f"Wrote metrics payload to {self.metrics_path}")

        backend = (self.metrics_backend or "").lower()
        if backend not in ["statsd", "prometheus"]:
            return

        host = self.metrics_host or "127.0.0.1"
        port = int(self.metrics_port or 8125)
        prefix = self.metrics_prefix or "lakeguard"

        tag_str = ""
        if self.metrics_tags:
            tag_str = "|#" + ",".join(f"{k}:{v}" for k, v in self.metrics_tags.items())

        lines = []
        for name, value in metrics.items():
            if value is None:
                continue
            metric_name = f"{prefix}.{name}"
            lines.append(f"{metric_name}:{value}|g{tag_str}")
        if not lines:
            return

        message = "\n".join(lines).encode("utf-8")
        try:
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.sendto(message, (host, port))
            sock.close()
            logger.info(f"Emitted metrics to StatsD at {host}:{port}")
        except Exception as exc:
            logger.warning(f"Failed to emit metrics to StatsD: {exc}")

    def _format_prometheus(self) -> str:
        """
        Format metrics in Prometheus exposition format.
        """
        snapshot = self.metrics_snapshot or {}
        metrics = snapshot.get("metrics") or {}
        tags = snapshot.get("tags") or {}

        def _labels(extra: Optional[Dict[str, str]] = None) -> str:
            label_items = dict(tags)
            if extra:
                label_items.update(extra)
            if not label_items:
                return ""
            pairs = ",".join(f'{k}="{v}"' for k, v in label_items.items())
            return "{" + pairs + "}"

        lines = []
        for key, value in metrics.items():
            if value is None:
                continue
            name = f"{self.metrics_prefix}_{key}"
            lines.append(f"{name}{_labels() } {value}")
        return "\n".join(lines) + "\n"

    def _start_prometheus_server(self) -> None:
        """
        Start a lightweight Prometheus /metrics HTTP server.
        """
        host = self.metrics_host or "0.0.0.0"
        port = int(self.metrics_port or 9100)

        driver_ref = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802
                if self.path not in ["/metrics", "/metrics/"]:
                    self.send_response(404)
                    self.end_headers()
                    return
                payload = driver_ref._format_prometheus().encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; version=0.0.4")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, format, *args):  # noqa: A003
                return

        try:
            server = HTTPServer((host, port), Handler)
        except Exception as exc:
            logger.warning(f"Failed to start Prometheus server on {host}:{port}: {exc}")
            return

        self.prometheus_server = server
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.prometheus_thread = thread
        logger.info(f"Prometheus metrics server running at http://{host}:{port}/metrics")

    def _stop_prometheus_server(self) -> None:
        """
        Stop the Prometheus HTTP server if running.
        """
        if not self.prometheus_server:
            return
        try:
            self.prometheus_server.shutdown()
            self.prometheus_server.server_close()
        except Exception:
            pass
        self.prometheus_server = None
        self.prometheus_thread = None


def parse_layers(raw: str, strict: bool = False) -> List[str]:
    """
    Parse a comma-separated layer list.

    Args:
        raw: Raw layer string.
        strict: Whether to enforce a strict allowed ordering.

    Returns:
        List of layers.
    """
    if not raw:
        return ["bronze", "silver", "gold"]
    raw_layers = [layer.strip() for layer in raw.split(",") if layer.strip()]
    alias_map = {
        "ref": "reference",
        "refs": "reference",
    }
    layers = [alias_map.get(layer, layer) for layer in raw_layers]
    if strict:
        valid_orders = {
            ("bronze",),
            ("bronze", "silver"),
            ("silver", "gold"),
            ("gold",),
            ("reference", "bronze"),
            ("reference", "bronze", "silver", "gold"),
            ("reference",),
        }
        if tuple(layers) not in valid_orders:
            raise ValueError(
                "Invalid layer order. Valid orders are: "
                "bronze | bronze,silver | silver,gold | gold | reference,bronze | "
                "reference | reference,bronze,silver,gold."
            )
    return layers


def parse_entities(raw: Optional[str]) -> Optional[set[str]]:
    """
    Parse comma-separated entity filters.

    Args:
        raw: Raw entity string.

    Returns:
        Set of entity names or None.
    """
    if not raw:
        return None
    return {item.strip() for item in raw.split(",") if item.strip()}


def parse_contracts(raw: Optional[str]) -> Optional[set[Path]]:
    """
    Parse comma-separated contract paths into absolute paths.

    Args:
        raw: Raw contract path string.

    Returns:
        Set of resolved Paths or None.
    """
    if not raw:
        return None
    return {Path(item.strip()).resolve() for item in raw.split(",") if item.strip()}


def parse_metrics_tags(raw: Optional[str]) -> Dict[str, str]:
    """
    Parse comma-separated key=value tags for metrics.

    Args:
        raw: Raw tag string.

    Returns:
        Dict of tag keys to values.
    """
    tags: Dict[str, str] = {}
    if not raw:
        return tags
    items = [item.strip() for item in raw.split(",") if item.strip()]
    for item in items:
        if "=" in item:
            key, value = item.split("=", 1)
            tags[key.strip()] = value.strip()
    return tags


def parse_window(
    raw: str,
    window_start_date: Optional[str],
    window_end_date: Optional[str],
    reprocess_date: Optional[str],
    reprocess_start_date: Optional[str],
    reprocess_end_date: Optional[str],
) -> Tuple[Window, bool]:
    """
    Parse window and reprocess parameters.

    Args:
        raw: Window selector.
        window_start_date: Optional start date string.
        window_end_date: Optional end date string.
        reprocess_date: Optional date string.
        reprocess_start_date: Optional start date string.
        reprocess_end_date: Optional end date string.

    Returns:
        Tuple of (Window, reprocess flag).
    """
    if reprocess_date or reprocess_start_date or reprocess_end_date:
        if reprocess_date:
            start = datetime.strptime(reprocess_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            end = start + timedelta(days=1)
        else:
            if not reprocess_start_date or not reprocess_end_date:
                raise ValueError("Both --reprocess-start-date and --reprocess-end-date are required.")
            start = datetime.strptime(reprocess_start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            end = datetime.strptime(reprocess_end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
            if end <= start:
                raise ValueError("Reprocess end date must be on or after start date.")
        return Window(start, end, "reprocess"), True

    if raw == "none":
        return Window(None, None, "full"), False
    if raw == "yesterday":
        now = datetime.now(timezone.utc)
        start = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        return Window(start, end, "yesterday"), False
    if raw == "range":
        if not window_start_date or not window_end_date:
            raise ValueError("Both --window-start-date and --window-end-date are required for window=range.")
        start = datetime.strptime(window_start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end = datetime.strptime(window_end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
        if end <= start:
            raise ValueError("Window end date must be on or after start date.")
        return Window(start, end, "range"), False
    return Window(None, None, "last_success"), False


def main() -> None:
    """CLI entrypoint for the registry-driven pipeline driver."""
    parser = argparse.ArgumentParser(description="LakeGuard pipeline driver (registry-based).")
    parser.add_argument("--registry", required=True, help="Path to system registry (bronze/silver contracts).")
    parser.add_argument("--reference-registry", help="Path to reference registry (optional).")
    parser.add_argument("--gold-registry", help="Path to gold registry (optional).")
    parser.add_argument("--layers", default="bronze,silver,gold", help="Layers to run (comma-separated).")
    parser.add_argument("--entities", help="Limit execution to specific entities (comma-separated).")
    parser.add_argument("--contracts", help="Limit execution to specific contract paths (comma-separated).")
    parser.add_argument("--strict-layer-order", action="store_true", help="Validate the order of layers strictly.")
    parser.add_argument("--window", default="last_success", help="Window: last_success | yesterday | none | range")
    parser.add_argument("--window-start-date", help="Window start date (YYYY-MM-DD) for --window range.")
    parser.add_argument("--window-end-date", help="Window end date (YYYY-MM-DD) for --window range.")
    parser.add_argument("--reprocess-date", help="Reprocess a specific date (YYYY-MM-DD).")
    parser.add_argument("--reprocess-start-date", help="Reprocess start date (YYYY-MM-DD).")
    parser.add_argument("--reprocess-end-date", help="Reprocess end date (YYYY-MM-DD).")
    parser.add_argument("--engine", default="polars")
    parser.add_argument("--max-workers", type=int, default=4)
    parser.add_argument("--summary-path", help="Write a run summary JSON to this path.")
    parser.add_argument("--summary-table", help="Write a pipeline summary row to a table backend.")
    parser.add_argument("--summary-backend", help="Summary backend: spark | duckdb | sqlite.")
    parser.add_argument("--summary-database", help="Database path for duckdb/sqlite summary tables.")
    parser.add_argument("--summary-table-format", help="Table format for Spark summary tables (default delta).")
    parser.add_argument(
        "--summary-merge-on-run-id",
        default=True,
        action=argparse.BooleanOptionalAction,
        help="Merge summary rows on run_id (Spark only).",
    )
    parser.add_argument("--metrics-path", help="Write a metrics JSON payload to this path.")
    parser.add_argument("--metrics-backend", help="Metrics backend: statsd.")
    parser.add_argument("--metrics-host", help="StatsD host (default 127.0.0.1).")
    parser.add_argument("--metrics-port", type=int, help="StatsD port (default 8125).")
    parser.add_argument("--metrics-prefix", help="StatsD metric prefix (default lakeguard).")
    parser.add_argument("--metrics-tags", help="Comma-separated tags (key=value) for metrics.")
    parser.add_argument("--continue-on-error", action="store_true", help="Continue running after a contract failure.")
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
        fail_fast=not args.continue_on_error,
    )

    registry_paths = {
        "system": Path(args.registry),
        "reference": Path(args.reference_registry) if args.reference_registry else None,
        "gold": Path(args.gold_registry) if args.gold_registry else None,
    }

    if window.label == "last_success":
        print("Window=last_success: using run log tables (if available).")

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
