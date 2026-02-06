"""
Registry-driven pipeline driver for LakeGuard.

This module orchestrates bronze/silver/gold runs from contract registries, supports
incremental windows (last_success), and reprocessing of late-arriving data.
"""

import argparse
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Lock
from typing import Dict, List, Optional, Tuple

import yaml

from lakeguard import DataProcessor
from lakeguard.core.models import DataContract, Quality


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
        metadata = contract.metadata or {}
        table_name = metadata.get("run_log_table")
        if not table_name:
            return None

        backend = (metadata.get("run_log_backend") or ("spark" if self.engine == "spark" else "duckdb")).lower()

        if backend == "spark":
            return self._read_spark(table_name, contract)
        if backend == "duckdb":
            return self._read_duckdb(table_name, contract, metadata)
        if backend == "sqlite":
            return self._read_sqlite(table_name, contract, metadata)
        return None

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

    def _read_spark(self, table_name: str, contract: DataContract) -> Optional[datetime]:
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
            return None
        spark = SparkSession.builder.getOrCreate()
        if not spark.catalog.tableExists(table_name):
            return None
        key = self._contract_key(contract)
        df = spark.sql(f"SELECT MAX(timestamp) AS last_ts FROM {table_name} WHERE contract = '{key}'")
        rows = df.collect()
        if not rows:
            return None
        value = rows[0]["last_ts"]
        if not value:
            return None
        return self._parse_timestamp(value)

    def _read_duckdb(self, table_name: str, contract: DataContract, metadata: Dict[str, str]) -> Optional[datetime]:
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
            return None

        base_path = getattr(contract, "_base_path", None)
        db_path = metadata.get("run_log_database") or "logs/lakeguard_run_logs.duckdb"
        db_path = self._resolve_path(db_path, base_path)
        if not db_path.exists():
            return None

        key = self._contract_key(contract)
        con = duckdb.connect(database=str(db_path))
        try:
            try:
                result = con.execute(
                    f"SELECT MAX(timestamp) FROM {table_name} WHERE contract = ?",
                    [key],
                ).fetchone()
            except Exception:
                return None
        finally:
            con.close()
        if not result or not result[0]:
            return None
        return self._parse_timestamp(result[0])

    def _read_sqlite(self, table_name: str, contract: DataContract, metadata: Dict[str, str]) -> Optional[datetime]:
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
            return None

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
                return None
        finally:
            conn.close()
        if not result or not result[0]:
            return None
        return self._parse_timestamp(result[0])

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

    def __init__(self, engine: str, max_workers: int) -> None:
        """
        Initialize a pipeline driver.

        Args:
            engine: Execution engine (polars/pandas/duckdb/spark).
            max_workers: Maximum parallel tasks per layer.
        """
        self.engine = engine
        self.max_workers = max_workers
        self.loader = ContractLoader()
        self.completed_lock = Lock()
        self.completed: set[str] = set()

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
        upstreams = contract.upstream or []

        if not self._upstreams_fresh(upstreams, registry_index, window):
            print(f"Skipping {dataset}: upstream not fresh")
            return

        contract = self._prepare_contract_for_stage(contract, stage, reprocess)
        sources, effective_window = self._resolve_sources(contract, window)

        if not sources:
            print(f"Skipping {dataset}: no sources resolved")
            return

        for source in sources:
            processor = DataProcessor(engine=self.engine, contract=contract)
            good_df, bad_df = processor.run_source(source)
            processor.materialize(good_df, bad_df)

        self._record_success(dataset)
        if effective_window.label == "full":
            print(f"{dataset}: full load executed")

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

    def _resolve_sources(self, contract: DataContract, window: Window) -> Tuple[List[str], Window]:
        """
        Resolve source paths for a contract and window.

        Args:
            contract: DataContract instance.
            window: Window definition.

        Returns:
            Tuple of (source list, effective window).
        """
        source_cfg = contract.source
        effective_window = window

        if window.label == "last_success":
            last_success = self._get_last_success(contract)
            if last_success:
                effective_window = Window(last_success, None, "incremental")
            else:
                effective_window = Window(None, None, "full")

        if not source_cfg:
            if contract.server and contract.server.path:
                return [str(contract.server.path)], effective_window
            return [], effective_window

        raw_path = source_cfg.path
        if not raw_path:
            return [], effective_window

        if str(raw_path).startswith("table:"):
            return [str(raw_path)], effective_window

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
                        return [str(p) for p in files], Window(None, None, "full")

            return [str(p) for p in files], effective_window

        return [str(path)], effective_window

    def _upstreams_fresh(self, upstreams: List[str], registry_index: Dict[str, Path], window: Window) -> bool:
        """
        Check whether all upstream datasets are fresh enough.

        Args:
            upstreams: Upstream dataset names.
            registry_index: Mapping of dataset to contract path.
            window: Window definition.

        Returns:
            True if upstreams are fresh, else False.
        """
        if not upstreams:
            return True

        log_reader = RunLogReader(self.engine)
        for upstream in upstreams:
            with self.completed_lock:
                if upstream in self.completed:
                    continue
            path = registry_index.get(upstream)
            if not path:
                return False
            contract = self.loader.load(path)
            last_success = log_reader.last_success(contract)
            if not last_success:
                return False
            if window.start and last_success < window.start:
                return False
        return True

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
            contract_path = None
            if stage and isinstance(entry.get("contracts"), dict):
                contract_path = entry["contracts"].get(stage)
            if not contract_path:
                contract_path = entry.get("contract_path")
            if contract_path:
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

    def _get_last_success(self, contract: DataContract) -> Optional[datetime]:
        """
        Get last-success timestamp for a contract from run logs.

        Args:
            contract: DataContract instance.

        Returns:
            Timestamp or None.
        """
        log_reader = RunLogReader(self.engine)
        return log_reader.last_success(contract)


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
    args = parser.parse_args()

    layers = parse_layers(args.layers, strict=args.strict_layer_order)
    entity_filter = parse_entities(args.entities)
    contract_filter = parse_contracts(args.contracts)
    window, reprocess = parse_window(
        args.window,
        args.window_start_date,
        args.window_end_date,
        args.reprocess_date,
        args.reprocess_start_date,
        args.reprocess_end_date,
    )

    driver = PipelineDriver(args.engine, args.max_workers)

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
