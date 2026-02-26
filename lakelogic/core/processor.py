import os
import sys
import yaml
import re
import warnings
from typing import Any, Tuple, Union, Dict, Optional, List
from pathlib import Path
from datetime import datetime, timezone
import uuid

from lakelogic.core.models import DataContract
from lakelogic.engines.base import EngineAdapter
from lakelogic.notifications.base import get_notification_adapter
from lakelogic.core.materialization import materialize_dataframe, materialize_quarantine, write_run_log
from lakelogic.core.observer import RemoteObserver
from loguru import logger


class ValidationResult:
    """
    Result object for LakeLogic runs.

    Unpacks as ``good_df, bad_df = processor.run(df)`` for the common
    two-variable pattern.  The raw (pre-validation) frame and the
    execution trace are available via ``.raw`` and ``.trace`` attributes.
    """
    def __init__(self, good, bad, raw=None, trace=None):
        self.good = good
        self.bad = bad
        self.raw = raw
        self.trace = trace

    def __iter__(self):
        yield self.good
        yield self.bad

    def __getitem__(self, idx):
        return [self.good, self.bad][idx]

    def __len__(self):
        return 2

    def __repr__(self):
        def _count(obj):
            if obj is None: return 0
            if hasattr(obj, "height"): return obj.height
            if hasattr(obj, "count"):
                try: return obj.count().fetchone()[0]
                except Exception: return "?"
            try: return len(obj)
            except Exception: return "?"
        return f"ValidationResult(good={_count(self.good)}, bad={_count(self.bad)})"

class DataProcessor:
    """
    The main entry point for running LakeLogic contracts.
    
    This class handles contract loading, engine selection, and dispatches 
    processing to the appropriate engine adapter.
    """

    def __init__(
        self,
        contract: Union[str, Path, dict, DataContract],
        engine: Optional[str] = None,
        *,
        stage: Optional[str] = None,
        pipeline_run_id: Optional[str] = None,
        trace: bool = False,
    ):
        """
        Initialize the DataProcessor.
        
        Args:
            contract: The Data Contract definition (path to YAML, dict, or DataContract object).
            engine: The execution engine to use. If None, it uses the auto-discovery logic.
            stage: Optional stage override (e.g., bronze/silver) applied from contract "stages".
            pipeline_run_id: Optional pipeline-level run id for correlation across contracts.
            trace: Enable detailed execution tracing and row debugging.
        """
        self._configure_logging()
        self.engine_name = (engine or self._discover_engine()).lower()
        self.stage = stage
        self.contract = self._load_contract(contract)
        # Support trace=True or trace="enabled"
        self.trace_enabled = trace is True or str(trace).lower() == "enabled"
        self.adapter = self._get_adapter()
        self.adapter.engine_name = self.engine_name
        self.adapter.trace_enabled = self.trace_enabled
        self.last_report: Optional[Dict[str, Any]] = None
        self.last_run_id: Optional[str] = None
        self.pipeline_run_id: Optional[str] = pipeline_run_id
        self.last_source_path: Optional[str] = None
        self._source_files: List[Dict[str, Any]] = []
        self._source_max_mtime: Optional[float] = None

    # ------------------------------------------------------------------
    # Alternative constructors
    # ------------------------------------------------------------------

    @classmethod
    def from_dbt(
        cls,
        schema_path: Union[str, "Path"],
        *,
        model: Optional[str] = None,
        source_name: Optional[str] = None,
        source_table: Optional[str] = None,
        engine: Optional[str] = None,
        stage: Optional[str] = None,
    ) -> "DataProcessor":
        """
        Create a ``DataProcessor`` from a dbt ``schema.yml`` or ``sources.yml``.

        Reads the dbt file, converts the specified model/source to a
        ``DataContract``, and returns a fully initialised ``DataProcessor``.
        All downstream LakeLogic APIs (``run``, ``run_source``,
        ``run_source_streaming``, GDPR tools, etc.) work identically.

        Parameters
        ----------
        schema_path
            Path to the dbt schema YAML file.
        model
            Name of the dbt model to import.  If the file contains exactly
            one model, this may be omitted.
        source_name
            dbt source name (for ``sources.yml`` files).
        source_table
            dbt source table name (for ``sources.yml`` files).
        engine
            Execution engine override. Defaults to auto-discovery.
        stage
            Stage override (``bronze``/``silver``/``gold``).

        Examples
        --------
        >>> proc = DataProcessor.from_dbt("models/schema.yml", model="customers")
        >>> good, bad = proc.run(df)

        >>> proc = DataProcessor.from_dbt(
        ...     "models/sources.yml",
        ...     source_name="raw",
        ...     source_table="orders",
        ... )
        """
        from lakelogic.adapters.dbt import load_contract_from_dbt
        contract = load_contract_from_dbt(
            schema_path,
            model=model,
            source_name=source_name,
            source_table=source_table,
        )
        return cls(contract, engine, stage=stage)

    def reset(
        self,
        *,
        targets: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """
        Delete all data managed by this contract's outputs.

        Delegates to ``DataContract.reset()``.  See that method for full
        documentation on ``targets`` and ``dry_run``.

        Examples
        --------
        ::

            proc = DataProcessor("contracts/bronze_zoopla.yaml")

            # Preview what would be removed
            proc.reset(dry_run=True)

            # Wipe everything, then re-run from scratch
            proc.reset()
            proc.run_source()

            # Quarantine only
            proc.reset(targets=["quarantine"])
        """
        return self.contract.reset(targets=targets, dry_run=dry_run)



    def _discover_engine(self) -> str:
        """
        Automatically discovers the best available engine.
        Priority:
        1. LAKELOGIC_ENGINE env var
        2. Spark (if running in Databricks/Spark environment)
        3. Polars (if installed)
        4. DuckDB (if installed)
        5. Pandas (fallback)
        """
        # 1. Check Env Var
        env_engine = os.getenv("LAKELOGIC_ENGINE")
        if env_engine:
            return env_engine

        # 2. Check for Spark (Databricks/Synapse detection)
        if "pyspark" in sys.modules or "spark" in globals():
            return "spark"

        # 3. Check for Polars (Preferred local engine)
        try:
            import polars
            return "polars"
        except ImportError:
            pass

        # 4. Check for DuckDB
        try:
            import duckdb
            return "duckdb"
        except ImportError:
            pass

        # 5. Default Fallback
        return "pandas"

    def _get_adapter(self) -> EngineAdapter:
        """
        Instantiates the correct adapter based on the engine name.
        """
        if self.engine_name == "polars":
            from lakelogic.engines.polars import PolarsAdapter
            return PolarsAdapter(self.contract)
        elif self.engine_name == "pandas":
            from lakelogic.engines.pandas import PandasAdapter
            return PandasAdapter(self.contract)
        elif self.engine_name == "duckdb":
            from lakelogic.engines.duckdb import DuckDBAdapter
            return DuckDBAdapter(self.contract)
        elif self.engine_name in ["spark", "pyspark"]:
            from lakelogic.engines.spark import SparkAdapter
            return SparkAdapter(self.contract)
        elif self.engine_name == "snowflake":
            from lakelogic.engines.snowflake import SnowflakeAdapter
            return SnowflakeAdapter(self.contract)
        elif self.engine_name == "bigquery":
            from lakelogic.engines.bigquery import BigQueryAdapter
            return BigQueryAdapter(self.contract)
        else:
            raise ValueError(f"Unsupported engine: {self.engine_name}")

    def _load_contract(self, contract: Union[str, Path, dict, DataContract]) -> DataContract:
        """
        Loads the contract from various formats.

        Args:
            contract: YAML path, dict, or DataContract instance.

        Returns:
            Loaded DataContract.
        """
        if isinstance(contract, DataContract):
            loaded = contract
            return self._apply_stage_overrides(loaded)
        if isinstance(contract, dict):
            loaded = DataContract(**contract)
            return self._apply_stage_overrides(loaded)
        
        path = Path(contract)
        if not path.exists():
            raise FileNotFoundError(f"Contract file not found: {path}")

        def _load_yaml_no_on_bool(handle):
            """
            Load YAML without treating 'on/off/yes/no' as booleans.
            Keeps true/false boolean parsing intact.
            """
            class Loader(yaml.SafeLoader):
                pass

            # Remove default bool resolver
            for key, mappings in list(Loader.yaml_implicit_resolvers.items()):
                Loader.yaml_implicit_resolvers[key] = [
                    (tag, regex) for tag, regex in mappings if tag != "tag:yaml.org,2002:bool"
                ]

            # Re-add bool resolver for true/false only
            bool_regex = re.compile(r"^(?:true|false)$", re.IGNORECASE)
            Loader.add_implicit_resolver("tag:yaml.org,2002:bool", bool_regex, list("tTfF"))

            return yaml.load(handle, Loader=Loader)

        with open(path, "r") as f:
            data = _load_yaml_no_on_bool(f)
            contract = DataContract(**data)
            try:
                contract._base_path = path.parent
                contract._contract_path = path
            except Exception:
                pass
            return self._apply_stage_overrides(contract)

    def _apply_stage_overrides(self, contract: DataContract) -> DataContract:
        """
        Apply stage-specific overrides from the contract "stages" block.

        Args:
            contract: Loaded DataContract.

        Returns:
            DataContract with stage overrides applied.
        """
        if not self.stage:
            return contract
        stage_key = str(self.stage).strip()
        if not stage_key:
            return contract

        stage_map = getattr(contract, "stages", None)
        if not isinstance(stage_map, dict):
            data = contract.model_dump()
            stage_map = data.get("stages")
        if not isinstance(stage_map, dict):
            return contract

        overrides = None
        if stage_key in stage_map:
            overrides = stage_map.get(stage_key)
        else:
            lower = stage_key.lower()
            for key, value in stage_map.items():
                if isinstance(key, str) and key.lower() == lower:
                    overrides = value
                    break
        if not isinstance(overrides, dict):
            return contract

        base_data = contract.model_dump(by_alias=True)
        merged = self._deep_merge(base_data, overrides)
        new_contract = DataContract(**merged)
        try:
            new_contract._base_path = getattr(contract, "_base_path", None)
            new_contract._contract_path = getattr(contract, "_contract_path", None)
        except Exception:
            pass
        return new_contract

    def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """
        Recursively merge override into base (dicts only). Lists and scalars replace.
        """
        merged = dict(base)
        for key, value in override.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = self._deep_merge(merged[key], value)
            else:
                merged[key] = value
        return merged

    def run(
        self,
        df: Any,
        source_path: Optional[Union[str, Path]] = None,
        materialize: bool = False,
        materialize_target: Optional[Union[str, Path]] = None,
        reset_trace: bool = True,
    ) -> ValidationResult:
        """
        Runs the contract against the provided dataframe.

        Args:
            df: Input dataframe.
            source_path: Optional source path for lineage/run reporting.
            materialize: Whether to write outputs to materialization targets.
            materialize_target: Optional override target for materialization.
            reset_trace: Whether to clear the current trace before starting.

        Returns:
            ValidationResult object (unpacks to good_df, bad_df).
        """
        if reset_trace or not hasattr(self, "_active_trace_steps"):
            self._active_trace_steps = []
        contract_title = self.contract.info.title if self.contract.info else (self.contract.dataset or "unknown")
        import time
        from lakelogic.core.models import ExecutionTrace, TraceStep
        from uuid import uuid4 as _uuid4

        # Generate run_id at the very start so inject_lineage can stamp it.
        # Only reset if not already set (streaming / incremental callers pre-set it).
        if not self.last_run_id:
            self.last_run_id = str(_uuid4())

        start_time = time.perf_counter()

        # Execute via adapter
        good_df, bad_df = self.adapter.execute(df)
        
        # Merge adapter trace if present
        if hasattr(self.adapter, "trace") and self.adapter.trace:
            self._active_trace_steps.extend(self.adapter.trace)

        # Inject lineage metadata
        step_start = time.perf_counter()
        from lakelogic.core.lineage import inject_lineage
        good_df, bad_df = inject_lineage(
            good_df, bad_df, self.contract, self.engine_name,
            self.last_run_id, self.pipeline_run_id, source_path,
        )
        self._active_trace_steps.append(TraceStep(
            step="Lineage Injection",
            timestamp=time.time(),
            duration_ms=(time.perf_counter() - step_start)*1000,
            status="ok"
        ))
        
        # Summary logging
        counts = self._compute_counts(df, good_df, bad_df)
        source_total = counts.get("source")
        total = counts.get("total")
        bad = counts.get("quarantined")
        if total is not None and bad is not None:
            quality_cfg = getattr(self.contract, "quality", None)
            enforce_required = True
            has_row_rules = False
            has_dataset_rules = False
            if quality_cfg is not None:
                enforce_required = bool(getattr(quality_cfg, "enforce_required", True))
                has_row_rules = bool(getattr(quality_cfg, "row_rules", []))
                has_dataset_rules = bool(getattr(quality_cfg, "dataset_rules", []))
            quality_enabled = enforce_required or has_row_rules or has_dataset_rules
            metadata = getattr(self.contract, "metadata", {}) or {}
            domain = metadata.get("domain")
            system = metadata.get("system")
            data_layer = metadata.get("data_layer")
            tags = []
            if domain:
                tags.append(f"domain={domain}")
            if system:
                tags.append(f"system={system}")
            if data_layer:
                tags.append(f"layer={data_layer}")
            tags_display = f" [{', '.join(tags)}]" if tags else ""
            ratio = counts.get("quarantine_ratio")
            ratio_display = f"{ratio:.2%}" if ratio is not None else "n/a"
            dropped = counts.get("pre_transform_dropped")
            dropped_display = f", Pre-Transform Dropped: {dropped}" if dropped is not None else ""
            source_display = f"Source: {source_total}, " if source_total is not None else ""
            if quality_enabled:
                logger.info(
                    f"Run complete.{tags_display} {source_display}Total (post-transform): {total}, "
                    f"Good: {counts.get('good')}, Quarantined: {bad}{dropped_display}, Ratio: {ratio_display}"
                )
            else:
                logger.info(
                    f"Run complete.{tags_display} {source_display}Total: {total}{dropped_display}"
                )

            if bad > 0:
                msg = f"LakeLogic Alert: {bad} records quarantined in '{contract_title}'. Total (post-transform): {total} (ratio {ratio_display})"
                self.notify(event="quarantine", message=msg)

        # Check dataset rules
        if hasattr(self.adapter, "dataset_rule_results"):
            failures = [r for r in self.adapter.dataset_rule_results if not r.get("passed")]
            if failures:
                details = "; ".join([f"{r.get('name')}={r.get('value')}" for r in failures])
                msg = f"LakeLogic dataset rule failures in '{contract_title}': {details}"
                self.notify(event="failure", message=msg)

        # Schema drift detection (ingest mode)
        drift = getattr(self.adapter, "schema_drift", {}) or {}
        if drift.get("missing_fields") or drift.get("unknown_fields"):
            allow = drift.get("allow_schema_drift", True)
            drift_msg = f"Schema drift detected for '{contract_title}': missing={drift.get('missing_fields')}, unknown={drift.get('unknown_fields')}"
            logger.warning(drift_msg)
            if not allow:
                self.notify(event="schema_drift", message=drift_msg)

        # Fail-fast if quarantine is disabled
        if self.contract.quarantine and not self.contract.quarantine.enabled:
            if bad and bad > 0:
                raise ValueError(f"Quarantine disabled but {bad} records failed validation for '{contract_title}'.")

        # Build run report and optionally write a log
        from lakelogic.core.slo import compute_slos
        slos = compute_slos(self.contract, good_df, counts, self.engine_name)
        row_rule_failures = self._extract_row_rule_failures(bad_df)
        self.last_report = self._build_report(contract_title, counts, slos, row_rule_failures, drift)
        write_run_log(self.last_report, self.contract, engine_name=self.engine_name)

        # Optional external logic hook (python/notebook)
        from lakelogic.core.external_logic import apply_external_logic
        if self.contract.external_logic:
            pre_count = self.adapter._get_row_count(good_df)
            step_start = time.perf_counter()
            good_df, external_handled = apply_external_logic(
                self.contract, good_df, self.engine_name,
                self.last_run_id, self.last_source_path,
                add_trace_fn=self._add_current_trace,
                trace_step_fn=self.trace_step,
            )
            post_count = self.adapter._get_row_count(good_df)
            
            self._active_trace_steps.append(TraceStep(
                step=f"External Logic ({self.contract.external_logic.type})",
                timestamp=time.time(),
                input_rows=pre_count,
                output_rows=post_count,
                duration_ms=(time.perf_counter() - step_start)*1000,
                details={"path": self.contract.external_logic.path},
                status="ok"
            ))
        else:
            good_df, external_handled = apply_external_logic(
                self.contract, good_df, self.engine_name,
                self.last_run_id, self.last_source_path,
            )

        # Materialize if requested
        if materialize and not external_handled:
            step_start = time.perf_counter()
            self.materialize(good_df, bad_df, target_path=materialize_target)
            self._active_trace_steps.append(TraceStep(
                step="Materialization",
                timestamp=time.time(),
                duration_ms=(time.perf_counter() - step_start)*1000,
                status="ok"
            ))

        # Optional Remote Reporting (SaaS Bridge)
        try:
            from lakelogic.core.observer import RemoteObserver
            observer = RemoteObserver()
            observer.report(self.last_report)
        except Exception:
            pass

        # Call to action for SaaS (opt-in only)
        quarantined = counts.get("quarantined")
        if quarantined is None:
            quarantined = 0
        if quarantined > 0 and os.getenv("LAKELOGIC_SHOW_TIPS", "false").lower() == "true":
            logger.info("🛡️  View deep quarantine analysis & historical drift on Lineage Logic: https://lineagelogic.com")
        
        trace = ExecutionTrace(
            run_id=self.last_run_id,
            steps=self._active_trace_steps,
            total_duration_ms=(time.perf_counter() - start_time)*1000
        )
        # Clear active trace steps after run
        self._active_trace_steps = []
        
        result = ValidationResult(good_df, bad_df, raw=df, trace=trace)
        self.last_result = result
        
        if self.trace_enabled:
            self.show_trace(trace)
            self._log_row_samples(result)

        return result

    def show_trace(self, trace: Optional[Any] = None):
        """Manually display the execution trace for the last run."""
        from lakelogic.cli.main import _display_trace
        if trace:
            _display_trace(trace)
        elif hasattr(self, "last_result") and self.last_result and self.last_result.trace:
            _display_trace(self.last_result.trace)
        elif hasattr(self, "_active_trace_steps") and self._active_trace_steps:
             # Create a dummy trace if we are mid-run or finished without result
             from lakelogic.core.models import ExecutionTrace
             dummy = ExecutionTrace(run_id=self.last_run_id or "latest", steps=self._active_trace_steps)
             _display_trace(dummy)

    def _log_row_samples(self, result: ValidationResult):
        """Log sample rows for debugging when tracing is enabled."""
        try:
            if result.good is not None:
                count = self.adapter._get_row_count(result.good)
                if count and count > 0:
                    logger.debug(f"Row Sample (GOOD): {self._get_sample_text(result.good)}")
            if result.bad is not None:
                count = self.adapter._get_row_count(result.bad)
                if count and count > 0:
                    logger.debug(f"Row Sample (QUARANTINED): {self._get_sample_text(result.bad)}")
        except Exception as e:
            logger.debug(f"Could not log row samples: {e}")

    def _get_sample_text(self, df: Any) -> str:
        """Convert a slice of the dataframe to a readable string."""
        if df is None: return "None"
        try:
            # Polars
            if hasattr(df, "head"):
                return "\n" + str(df.head(3))
            # Pandas
            if hasattr(df, "iloc"):
                return "\n" + str(df.head(3))
            # DuckDB
            if hasattr(df, "limit"):
                return "\n" + str(df.limit(3).df())
            return str(df)
        except Exception:
            return "(sample conversion failed)"

    def run_source(self, source: Optional[Union[str, Path]] = None) -> ValidationResult:
        """
        Loads data from a source file and runs the contract in one step.
        The data is loaded using the engine's optimized reader.

        Args:
            source: Optional file path to load. If None, uses path from contract.

        Returns:
            ValidationResult object (unpacks to good_df, bad_df).
        """
        self._active_trace_steps = []
        path_val = source or (self.contract.source.path if self.contract.source else None)
        if not path_val:
            raise ValueError("No source path provided and no path found in contract.")
        path = self._resolve_source_path(path_val)
        
        # Resolve catalog table names (Unity Catalog, Fabric LakeDB, Synapse) to storage paths
        if self.engine_name != "spark":  # Spark handles catalogs natively
            from lakelogic.engines.unity_catalog import resolve_catalog_path
            original_path = path
            path = resolve_catalog_path(path)
            if path != original_path:
                logger.info(f"Resolved catalog table: {original_path} -> {path}")
        
        logger.info(f"Loading source: {path} via {self.engine_name}")

        source_files = self._expand_source_files(path)
        load_mode = getattr(self.contract.source, "load_mode", "full") if self.contract.source else "full"
        if load_mode == "incremental" and source_files:
            watermark = self._get_last_source_watermark()
            if watermark is not None:
                source_files = [f for f in source_files if f.get("mtime", 0) > watermark]
            if not source_files:
                logger.info("No new files detected for incremental load; skipping run.")
                return ValidationResult(self._empty_frame(), self._empty_frame(), self._empty_frame())

        self._source_files = source_files or []
        self._source_max_mtime = None
        if source_files:
            self._source_max_mtime = max(f.get("mtime", 0) for f in source_files)

        df = None
        file_paths = [f["path"] for f in source_files] if source_files else None
        
        with self.trace_step("Load Source", path=str(path)):
            if self.engine_name == "polars":
                import polars as pl
                if df is None:
                    # Use scan_* (lazy) by default for formats that support it.
                    # This enables predicate/projection pushdown and query
                    # optimisation.  We .collect() before returning so the
                    # user always receives a regular DataFrame.
                    _scannable = path.endswith((".parquet", ".csv", ".ndjson", ".jsonl"))

                    if file_paths:
                        if _scannable:
                            # Lazy: scan each file and concat lazily
                            if path.endswith(".parquet"):
                                lf = pl.scan_parquet(file_paths if len(file_paths) > 1 else file_paths[0])
                            elif path.endswith((".ndjson", ".jsonl")):
                                lf = pl.concat([pl.scan_ndjson(p) for p in file_paths])
                            else:  # CSV
                                if len(file_paths) == 1:
                                    lf = pl.scan_csv(file_paths[0])
                                else:
                                    lf = pl.concat([pl.scan_csv(p) for p in file_paths])
                            df = lf.collect()
                        else:
                            # Eager fallback — JSON, XML, Excel (no scan_* support)
                            import json as _json

                            def _read_json_flat(filepath: str) -> "pl.DataFrame":
                                """Read a .json file and cast any nested Struct/List columns
                                to JSON strings so they match a flat contract schema."""
                                import polars as pl
                                raw = _json.loads(Path(filepath).read_text(encoding="utf-8"))
                                rows = [raw] if isinstance(raw, dict) else (raw if isinstance(raw, list) else [])
                                # Normalise nested objects to JSON strings
                                flat = [
                                    {k: (_json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v)
                                     for k, v in row.items()}
                                    for row in rows
                                ]
                                return pl.DataFrame(flat)

                            if len(file_paths) == 1:
                                fp = file_paths[0]
                                if fp.endswith(".xml"):
                                    df = pl.read_xml(fp)
                                elif fp.endswith((".xlsx", ".xls")):
                                    df = pl.read_excel(fp)
                                elif fp.endswith(".json"):
                                    df = _read_json_flat(fp)
                                else:
                                    df = pl.read_csv(fp)
                            else:
                                frames = []
                                for fp in file_paths:
                                    if fp.endswith(".xml"):
                                        frames.append(pl.read_xml(fp))
                                    elif fp.endswith((".xlsx", ".xls")):
                                        frames.append(pl.read_excel(fp))
                                    elif fp.endswith(".json"):
                                        frames.append(_read_json_flat(fp))
                                    else:
                                        frames.append(pl.read_csv(fp))
                                df = pl.concat(frames, how="diagonal")  # diagonal tolerates missing cols

                    else:
                        # No glob expansion — single path or table directory
                        p_obj = Path(path)
                        # ── Delta table directory ──────────────────────────
                        _is_delta_dir = (
                            p_obj.is_dir()
                            and (
                                (p_obj / "_delta_log").exists()         # standard Delta
                                or getattr(getattr(self.contract, "source", None), "type", None) == "table"
                            )
                        )
                        source_fmt = (
                            getattr(self.contract.materialization, "format", None)
                            if getattr(self.contract, "materialization", None)
                            else None
                        ) or (
                            getattr(self.contract.server, "format", None)
                            if getattr(self.contract, "server", None)
                            else None
                        ) or ""

                        if _is_delta_dir or source_fmt.lower() == "delta":
                            # ── Resolve incremental watermark BEFORE reading ──
                            # Query max(watermark_col) from the target (silver)
                            # table once, then pass as a filter to the Delta read
                            # so discarded rows are never loaded into memory.
                            _wm_filter_expr = None
                            _src_cfg = getattr(self.contract, "source", None)
                            if (
                                getattr(_src_cfg, "load_mode", None) == "incremental"
                                and not os.environ.get("LAKELOGIC_SKIP_INCREMENTAL_CHECK")
                            ):
                                _wm_field = getattr(_src_cfg, "watermark_field", None)
                                _mat = getattr(self.contract, "materialization", None)
                                _tgt = getattr(_mat, "target_path", None) if _mat else None
                                if _wm_field and _tgt:
                                    # Determine target column name (may be renamed by preserve_upstream)
                                    _lin = getattr(self.contract, "lineage", None)
                                    _pres = list(getattr(_lin, "preserve_upstream", []) or [])
                                    _pfx = getattr(_lin, "upstream_prefix", "_upstream") or "_upstream"
                                    if _wm_field in _pres:
                                        _tgt_col = (
                                            f"{_pfx}{_wm_field}"
                                            if _wm_field.startswith("_lakelogic_")
                                            else f"{_pfx}_{_wm_field.lstrip('_')}"
                                        )
                                    else:
                                        _tgt_col = _wm_field
                                    _max_wm = None
                                    _tgt_delta = Path(_tgt)
                                    if (_tgt_delta / "_delta_log").exists():
                                        try:
                                            _tdf = pl.read_delta(str(_tgt_delta))
                                            if _tgt_col in _tdf.columns:
                                                _max_wm = _tdf.select(pl.col(_tgt_col).max()).item()
                                        except Exception as _wm_err:
                                            logger.debug(f"Watermark read failed (full load): {_wm_err}")
                                    if _max_wm is not None:
                                        logger.info(
                                            f"Incremental load: filtering {_wm_field} > {_max_wm!r} "
                                            f"(target column: '{_tgt_col}')"
                                        )
                                        _wm_filter_expr = pl.col(_wm_field) > _max_wm
                                    else:
                                        logger.info(
                                            "Incremental load: first run or target empty — loading all rows."
                                        )

                            df = pl.read_delta(path)

                            # Apply watermark filter immediately (before flatten/rename)
                            if _wm_filter_expr is not None:
                                df = df.filter(_wm_filter_expr)
                                if df.is_empty():
                                    logger.info(
                                        "Incremental load: no new rows since last run — "
                                        "processing empty frame to preserve contract schema."
                                    )
                                # Continue (don't return early): the empty-but-schemed df
                                # flows through transforms so result.good has correct dtypes.
                        elif _scannable:
                            if path.endswith(".parquet"): lf = pl.scan_parquet(path)
                            elif path.endswith((".ndjson", ".jsonl")): lf = pl.scan_ndjson(path)
                            else: lf = pl.scan_csv(path)
                            df = lf.collect()
                        else:
                            import json as _json
                            if path.endswith(".json"):
                                raw = _json.loads(Path(path).read_text(encoding="utf-8"))
                                rows = [raw] if isinstance(raw, dict) else (raw if isinstance(raw, list) else [])
                                flat = [{k: (_json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v) for k, v in r.items()} for r in rows]
                                df = pl.DataFrame(flat)
                            elif path.endswith(".xml"): df = pl.read_xml(path)
                            elif path.endswith((".xlsx", ".xls")): df = pl.read_excel(path)
                            else: df = pl.read_csv(path)

            elif self.engine_name == "pandas":
                import pandas as pd
                import json as _json_pd

                def _pandas_read_json_flat(filepath: str) -> "pd.DataFrame":
                    """Read a .json file, serialise nested objects to JSON
                    strings so columns stay flat — consistent with the Polars
                    and DuckDB branches."""
                    raw = _json_pd.loads(Path(filepath).read_text(encoding="utf-8"))
                    rows = [raw] if isinstance(raw, dict) else (raw if isinstance(raw, list) else [])
                    flat = [
                        {k: (_json_pd.dumps(v, ensure_ascii=False)
                             if isinstance(v, (dict, list)) else v)
                         for k, v in row.items()}
                        for row in rows
                    ]
                    return pd.DataFrame(flat)

                if df is None:
                    if file_paths:
                        frames = []
                        for file in file_paths:
                            if file.endswith(".parquet"): frames.append(pd.read_parquet(file))
                            elif file.endswith(".xml"): frames.append(pd.read_xml(file))
                            elif file.endswith((".xlsx", ".xls")): frames.append(pd.read_excel(file))
                            elif file.endswith(".json"): frames.append(_pandas_read_json_flat(file))
                            else: frames.append(pd.read_csv(file))
                        df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
                    else:
                        if path.endswith(".json"): df = _pandas_read_json_flat(path)
                        elif path.endswith(".csv"): df = pd.read_csv(path)
                        elif path.endswith(".parquet"): df = pd.read_parquet(path)
                        elif path.endswith(".xml"): df = pd.read_xml(path)
                        elif path.endswith((".xlsx", ".xls")): df = pd.read_excel(path)
                        else: df = pd.read_csv(path)

            elif self.engine_name == "duckdb":
                import duckdb
                import json as _json_duck

                def _duckdb_read_csv(paths):
                    return duckdb.read_csv(paths, header=True, strict_mode=False)

                def _duckdb_read_json(paths):
                    """Read one or more .json files via DuckDB.

                    Each file may contain a single JSON object or an array of
                    objects.  We use Polars-style manual parsing (json.loads +
                    DataFrame) so nested objects are serialised to JSON-strings
                    exactly like the Polars branch's ``_read_json_flat``.
                    This keeps bronze columns flat and matches the contract
                    schema regardless of which engine is used.
                    """
                    import polars as pl

                    if isinstance(paths, str):
                        paths = [paths]
                    frames = []
                    for fp in paths:
                        raw = _json_duck.loads(Path(fp).read_text(encoding="utf-8"))
                        rows = [raw] if isinstance(raw, dict) else (raw if isinstance(raw, list) else [])
                        flat = [
                            {k: (_json_duck.dumps(v, ensure_ascii=False)
                                 if isinstance(v, (dict, list)) else v)
                             for k, v in row.items()}
                            for row in rows
                        ]
                        if flat:
                            frames.append(pl.DataFrame(flat))
                    if frames:
                        return pl.concat(frames, how="diagonal")
                    return pl.DataFrame()

                rel = None
                if df is None:
                    if file_paths:
                        if path.endswith(".parquet"):
                            rel = duckdb.read_parquet(file_paths)
                        elif path.endswith(".json"):
                            df = _duckdb_read_json(file_paths)
                        else:
                            rel = _duckdb_read_csv(file_paths)
                    else:
                        if path.endswith(".json"):
                            df = _duckdb_read_json(path)
                        elif path.endswith(".csv"):
                            rel = _duckdb_read_csv(path)
                        elif path.endswith(".parquet"):
                            rel = duckdb.read_parquet(path)
                        else:
                            rel = _duckdb_read_csv(path)

                    if rel is not None:
                        df = rel.df()

            elif self.engine_name == "spark":
                from pyspark.sql import SparkSession
                spark = SparkSession.builder.getOrCreate()
                # ── Format detection ──────────────────────────────────────────
                # Explicit server.format wins; otherwise infer from file extension.
                fmt = (
                    self.contract.server.format.lower()
                    if self.contract.server and self.contract.server.format
                    else (
                        "json" if path.endswith(".json")
                        else "csv" if path.endswith(".csv")
                        else "excel" if path.endswith((".xlsx", ".xls"))
                        else "parquet"
                    )
                )
                
                if df is None:
                    if path.startswith("table:"):
                        df = spark.table(path[6:])
                    else:
                        reader = spark.read.format(fmt)
                        if fmt == "csv":
                            reader = reader.option("header", "true")
                        elif fmt == "json":
                            # Each .json file is a single JSON object (not NDJSON),
                            # so multiLine=true is required for Spark to parse it
                            # as one record rather than one-line-per-record.
                            reader = reader.option("multiLine", "true")
                        df = reader.load(file_paths if file_paths else path)

            elif self.engine_name in ["snowflake", "bigquery"]:
                table_name = path[6:] if path.startswith("table:") else path
                return self.run(table_name, source_path=table_name, reset_trace=False)

        if df is None:
            raise ValueError(f"Could not load data from {path} using engine {self.engine_name}")

        # ── JSON-string flattening (source.flatten_nested) ────────────────────
        # When a bronze table stores nested objects as JSON strings we need to
        # expand them into flat parent_child columns so they match the silver
        # schema before validation runs.
        flatten_nested = getattr(getattr(self.contract, "source", None), "flatten_nested", False)
        if flatten_nested:
            df = self._flatten_json_df(df, flatten_nested)

        # ── Pre-validation upstream rename (lineage.preserve_upstream) ────────
        # Schema validation runs inside self.run() BEFORE inject_lineage is
        # called.  If the contract declares preserve_upstream, the upstream
        # columns must be renamed NOW (before validation) so the schema checker
        # sees the correct _upstream_lakelogic_* names rather than marking them
        # as "missing" and the raw _lakelogic_* as "unknown".
        #
        # inject_lineage's own _preserve_upstream_lineage call is then a no-op
        # (destination already exists) and just stamps fresh _lakelogic_* values.
        lineage_cfg = getattr(self.contract, "lineage", None)
        if lineage_cfg and getattr(lineage_cfg, "enabled", False):
            preserve_cols = list(getattr(lineage_cfg, "preserve_upstream", []) or [])
            if preserve_cols:
                from lakelogic.core.lineage import _preserve_upstream_lineage
                prefix = getattr(lineage_cfg, "upstream_prefix", "_upstream") or "_upstream"
                df = _preserve_upstream_lineage(df, preserve_cols, prefix, self.engine_name)

        return self.run(df, source_path=path, reset_trace=False)

    def _flatten_json_df(self, df, flatten_nested):
        """
        Flatten JSON-string columns in *df* into ``parent_child`` columns.

        Mirrors the pre-pass logic in ``ContractInferrer._flatten_df`` so that
        a silver/gold processor reading a bronze Delta table (where nested
        objects are stored as JSON strings) gets a flat schema that matches
        the inferred silver contract.

        Parameters
        ----------
        df:
            Polars / Pandas DataFrame loaded from the source table.
        flatten_nested:
            - ``True``           — flatten every JSON-string column
            - ``list[str]``      — flatten only the named columns
        """
        import json as _json
        try:
            import polars as pl
            _is_polars = isinstance(df, pl.DataFrame)
        except Exception:
            _is_polars = False

        target_cols: set
        if isinstance(flatten_nested, list):
            target_cols = set(flatten_nested)
        else:
            target_cols = set()  # empty = all

        def _try_parse(val):
            if not isinstance(val, str):
                return val
            s = val.strip()
            if s[:1] not in ("{" , "["):
                return val
            try:
                return _json.loads(s)
            except Exception:
                return val

        def _is_json_col(rows, col, sample=20):
            hits = 0
            checked = 0
            for row in rows[:sample]:
                v = row.get(col)
                if v is None:
                    continue
                checked += 1
                if isinstance(_try_parse(v), (dict, list)):
                    hits += 1
            return checked > 0 and hits / checked >= 0.5

        # ── Convert to row-dict form ───────────────────────────────────────
        if _is_polars:
            rows = df.to_dicts()
        else:
            try:
                rows = df.to_dict(orient="records")
            except Exception:
                return df  # unknown frame type — leave as-is

        if not rows:
            # Empty DataFrame — no actual data to flatten, but we still need to
            # produce the correct OUTPUT schema (flat parent_child columns) so
            # that downstream schema validation sees the right column names.
            #
            # Strategy: for each target column that exists in the df, drop it
            # and add `parent_child` null columns using the silver contract's
            # declared field names (target_cols list).  We can't introspect
            # the JSON values (there are none), so we use the contract fields.
            if _is_polars and target_cols:
                import polars as pl
                cols_present = set(df.columns)
                cols_to_drop = [c for c in target_cols if c in cols_present]
                # Identify which flat columns the contract expects for these parents
                lin_fields = []
                contract_fields = getattr(getattr(self.contract, "model", None), "fields", None) or []
                for field in contract_fields:
                    fname = getattr(field, "name", None) or (field.get("name") if isinstance(field, dict) else None)
                    if fname and any(fname.startswith(f"{p}_") for p in target_cols):
                        lin_fields.append(fname)
                # Build new schema: remove parent cols, add flat children as Utf8 nulls
                new_df = df.drop(cols_to_drop)
                for flat_col in lin_fields:
                    if flat_col not in new_df.columns:
                        new_df = new_df.with_columns(pl.lit(None).cast(pl.Utf8).alias(flat_col))
                return new_df
            return df

        # ── Identify which columns to flatten ────────────────────────────────
        if target_cols:
            # Only flatten explicitly named columns (regardless of JSON check)
            json_cols = [c for c in target_cols if c in rows[0]]
        else:
            # Auto-detect: any string column whose values are mostly JSON
            json_cols = [
                col for col in rows[0]
                if isinstance(rows[0].get(col), str) and _is_json_col(rows, col)
            ]

        if not json_cols:
            return df  # nothing to flatten

        # ── Deserialise targeted columns ───────────────────────────────────
        rows = [
            {**{k: v for k, v in row.items() if k not in json_cols},
             **{c: _try_parse(row.get(c)) for c in json_cols}}
            for row in rows
        ]

        def _explode(rows, col):
            """Expand a dict/list column into ``col_key`` child columns."""
            all_keys = {}
            for row in rows:
                val = row.get(col)
                if isinstance(val, dict):
                    all_keys.update({k: v for k, v in val.items() if k not in all_keys})
                elif isinstance(val, list) and val and isinstance(val[0], dict):
                    all_keys.update({k: v for k, v in val[0].items() if k not in all_keys})
            if not all_keys:
                return rows
            out = []
            for row in rows:
                new = {k: v for k, v in row.items() if k != col}
                val = row.get(col)
                if isinstance(val, dict):
                    for key in all_keys:
                        child = val.get(key)
                        new[f"{col}_{key}"] = (
                            _json.dumps(child, ensure_ascii=False)
                            if isinstance(child, (dict, list)) else child
                        )
                elif isinstance(val, list):
                    new[f"{col}_values"] = _json.dumps(val, ensure_ascii=False)
                else:
                    for key in all_keys:
                        new[f"{col}_{key}"] = None
                out.append(new)
            return out

        # ── Expand: iterate until no nested dicts remain (max 5 levels) ───────
        changed = True
        depth = 0
        while changed and depth < 5:
            changed = False
            depth += 1

            # Re-detect JSON-string columns each iteration: child columns that
            # were serialised back to JSON strings (e.g. location_coordinates
            # after location was exploded) need another deserialise pass so the
            # dict/list check below can catch and further explode them.
            if rows:
                json_cols_iter = [
                    col for col in rows[0]
                    if isinstance(rows[0].get(col), str) and _is_json_col(rows, col)
                ]
                if json_cols_iter:
                    rows = [
                        {**{k: v for k, v in row.items() if k not in json_cols_iter},
                         **{c: _try_parse(row.get(c)) for c in json_cols_iter}}
                        for row in rows
                    ]

            to_explode = [
                col for col in list(rows[0].keys())
                if any(isinstance(row.get(col), (dict, list)) for row in rows)
            ]
            for col in to_explode:
                rows = _explode(rows, col)
                changed = True

        # ── Rebuild DataFrame ─────────────────────────────────────────────────
        try:
            if _is_polars:
                return pl.from_dicts(rows)
            else:
                import pandas as pd
                return pd.DataFrame(rows)
        except Exception:
            return df  # reconstruction failed — return original

    def _expand_source_files(self, path: str) -> Optional[List[Dict[str, Any]]]:
        """
        Expand local file patterns into concrete file paths and mtimes.
        """
        if self._is_uri_path(path) or path.startswith("table:"):
            return None
        # Delta table directories (and other table formats) — no glob expansion.
        source_type = getattr(getattr(self.contract, "source", None), "type", None)
        if source_type == "table":
            return None
        p = Path(path)
        if p.is_dir() and (p / "_delta_log").exists():
            return None  # Delta table directory — read as a whole, not individual files
        if not any(ch in path for ch in ["*", "?", "["]):
            return None

        from glob import glob

        pattern = path
        base = getattr(self.contract, "_base_path", None)
        if base and not Path(pattern).is_absolute():
            pattern = str(Path(base) / pattern)

        files = [f for f in glob(pattern) if Path(f).is_file()]
        results = []
        for file in sorted(files):
            try:
                results.append({"path": str(Path(file).resolve()), "mtime": Path(file).stat().st_mtime})
            except Exception:
                continue
        return results or None

    def _get_last_source_watermark(self) -> Optional[float]:
        """
        Fetch the last max_source_mtime for this contract/stage from run logs.
        """
        try:
            from lakelogic.core.materialization import get_last_run_watermark
        except Exception:
            return None
        contract_title = self.contract.info.title if self.contract.info else (self.contract.dataset or "unknown")
        stage = self.stage or "default"
        return get_last_run_watermark(self.contract, contract_title, stage, engine_name=self.engine_name)

    def _empty_frame(self) -> Any:
        """
        Return an empty frame suitable for the current engine.
        """
        if self.engine_name == "polars":
            try:
                import polars as pl
                return pl.DataFrame()
            except Exception:
                return []
        if self.engine_name == "pandas":
            try:
                import pandas as pd
                return pd.DataFrame()
            except Exception:
                return []
        if self.engine_name == "spark":
            try:
                from pyspark.sql import SparkSession
                return SparkSession.builder.getOrCreate().createDataFrame([], schema=None)
            except Exception:
                return []
        return []

    def _is_uri_path(self, path: str) -> bool:
        """
        Check if a path is a URI (s3://, gs://, file://, etc.).
        """
        return bool(re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", str(path)))

    def _resolve_source_path(self, path_val: Union[str, Path]) -> str:
        """
        Resolve a source path relative to the contract base path when appropriate.
        """
        path_str = str(path_val)
        if path_str.startswith("table:") or self._is_uri_path(path_str):
            return path_str

        path_obj = Path(path_str).expanduser()
        if path_obj.is_absolute():
            return str(path_obj)

        # Prefer explicit CWD if it exists there.
        if path_obj.exists():
            try:
                return str(path_obj.resolve())
            except Exception:
                return str(path_obj)

        base = getattr(self.contract, "_base_path", None)
        if base:
            candidate = Path(base) / path_obj
            if candidate.exists():
                try:
                    return str(candidate.resolve())
                except Exception:
                    return str(candidate)

        return str(path_obj)

    def _configure_logging(self) -> None:
        """
        Configure logging based on environment variables.
        """
        debug = os.getenv("LAKELOGIC_DEBUG", "false").lower() == "true"
        if not debug:
            try:
                logger.remove()
                logger.add(sys.stderr, level="INFO")
            except Exception:
                pass
        
        # Suppress third-party warnings
        warnings.filterwarnings("ignore", message=".*PerformanceWarning.*")
        try:
            import polars as pl
            # This is specific to polars but global for the process
            warnings.filterwarnings("ignore", category=pl.PerformanceWarning)
        except ImportError:
            pass

    def notify(self, event: str, message: str):
        """
        Dispatches notifications based on contract config.

        Args:
            event: Event name triggering the notification.
            message: Notification body.
        """
        q = self.contract.quarantine
        if not q or not q.enabled or not q.notifications:
            return

        for notif in q.notifications:
            if event in notif.on_events or (event == "failure" and "dataset_rule_failed" in notif.on_events):
                try:
                    config = notif.model_dump(by_alias=True)
                    if hasattr(self.contract, "_base_path"):
                        config["_base_path"] = str(self.contract._base_path)
                    adapter = get_notification_adapter(notif.type, config)
                    adapter.send(message, subject=f"LakeLogic {event.capitalize()} Alert")
                except Exception as e:
                    logger.error(f"Failed to send notification: {e}")
                    if getattr(q, "strict_notifications", True):
                        raise

    def materialize(
        self,
        good_df: Any,
        bad_df: Optional[Any] = None,
        target_path: Optional[Union[str, Path]] = None,
    ) -> Dict[str, Any]:
        """
        Materialize good data (and optionally quarantine) to configured targets.

        Args:
            good_df: Validated dataframe.
            bad_df: Optional quarantined dataframe.
            target_path: Optional override target path.

        Returns:
            Materialization metadata.
        """
        target = Path(target_path) if target_path else None
        result = materialize_dataframe(
            good_df,
            self.contract,
            target_path=target,
            engine_name=self.engine_name,
        )

        if bad_df is not None and self.contract.quarantine and self.contract.quarantine.target:
            materialize_quarantine(bad_df, self.contract, engine_name=self.engine_name)

        return result

    def _compute_counts(self, source_df: Any, good_df: Any, bad_df: Any) -> Dict[str, Optional[int]]:
        """
        Compute row counts and quarantine ratio for a run.

        For Spark, uses a single-pass aggregation to avoid multiple .count() calls
        which would each trigger a full DAG execution.

        Args:
            source_df: Original input dataframe.
            good_df: Validated dataframe.
            bad_df: Quarantined dataframe.

        Returns:
            Dict with total, good, quarantined, and ratio values.
        """
        # For Spark, optimize by computing counts in a single action where possible
        if self.engine_name == "spark":
            try:
                from pyspark.sql import functions as F

                # Cache good_df and bad_df if they share lineage to avoid recomputation
                # Then compute counts together
                good_count = None
                bad_count = None
                source_count = None

                # Use a union with a marker column to count source/good/bad in one action
                marked_frames = []
                if source_df is not None:
                    marked_frames.append(source_df.select(F.lit("source").alias("_count_marker")))
                if good_df is not None:
                    marked_frames.append(good_df.select(F.lit("good").alias("_count_marker")))
                if bad_df is not None:
                    marked_frames.append(bad_df.select(F.lit("bad").alias("_count_marker")))

                if marked_frames:
                    combined = marked_frames[0]
                    for frame in marked_frames[1:]:
                        combined = combined.union(frame)
                    counts_result = combined.groupBy("_count_marker").count().collect()
                    counts_map = {row["_count_marker"]: row["count"] for row in counts_result}
                    source_count = counts_map.get("source")
                    good_count = counts_map.get("good", 0)
                    bad_count = counts_map.get("bad", 0)
                else:
                    good_count = 0
                    bad_count = 0

                total = (good_count or 0) + (bad_count or 0)
                ratio = bad_count / total if total > 0 else None
                dropped = None
                if source_count is not None:
                    dropped = source_count - total
                    if dropped < 0:
                        dropped = 0
                return {
                    "source": source_count,
                    "total": total,
                    "good": good_count,
                    "quarantined": bad_count,
                    "quarantine_ratio": ratio,
                    "pre_transform_dropped": dropped,
                }
            except Exception:
                pass  # Fall back to individual counts

        # Non-Spark engines: use len() which is O(1) for most dataframe types
        def _count(obj: Any) -> Optional[int]:
            try:
                if hasattr(obj, "height"):  # Polars
                    return int(obj.height)
                return len(obj)
            except Exception:
                return None

        source_total = _count(source_df)
        good = _count(good_df)
        bad = _count(bad_df)
        total = None
        if good is not None and bad is not None:
            total = good + bad
        dropped = None
        if source_total is not None and total is not None:
            dropped = source_total - total
            if dropped < 0:
                dropped = 0
        ratio = None
        if total is not None and bad is not None and total > 0:
            ratio = bad / total
        return {
            "source": source_total,
            "total": total,
            "good": good,
            "quarantined": bad,
            "quarantine_ratio": ratio,
            "pre_transform_dropped": dropped,
        }

    def _build_report(
        self,
        contract_title: str,
        counts: Dict[str, Optional[int]],
        slos: Optional[Dict[str, Any]] = None,
        row_rule_failures: Optional[list] = None,
        schema_drift: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Build a run report structure.

        Args:
            contract_title: Human-friendly contract name.
            counts: Count metrics.
            slos: SLO results.
            row_rule_failures: Rule failures extracted from quarantine.
            schema_drift: Schema drift metadata.

        Returns:
            Run report dict.
        """
        # ── Resolve metadata fields ─────────────────────────────────────────────
        # Priority: metadata block → info block (where infer_contract stores them)
        metadata = getattr(self.contract, "metadata", {}) or {}
        info = getattr(self.contract, "info", None)

        def _meta_or_info(key: str) -> Optional[str]:
            """Check metadata first, fall back to info block."""
            val = metadata.get(key)
            if val:
                return val
            return getattr(info, key, None) if info else None

        domain = _meta_or_info("domain")
        system = _meta_or_info("system")
        data_layer = _meta_or_info("data_layer") or (
            getattr(info, "target_layer", None) if info else None
        )

        # ── Resolve dataset ─────────────────────────────────────────────────────
        # contract.dataset is a top-level field; fall back to info.title
        dataset = self.contract.dataset or (info.title if info else None)

        # ── Resolve source_path ─────────────────────────────────────────────────
        # run() callers set last_source_path; run_source() doesn't — use first
        # source file from the expanded file list instead.
        source_path = self.last_source_path
        if not source_path:
            source_files = getattr(self, "_source_files", None) or []
            if source_files:
                source_path = source_files[0].get("path")

        return {
            "run_id": self.last_run_id,
            "pipeline_run_id": self.pipeline_run_id,
            "engine": self.engine_name,
            "contract": contract_title,
            "stage": self.stage or "default",
            "dataset": dataset,
            "domain": domain,
            "system": system,
            "data_layer": data_layer,
            "source_path": source_path,
            "source_files": self._source_files or [],
            "max_source_mtime": self._source_max_mtime,
            "timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "counts": counts,
            "dataset_rules": getattr(self.adapter, "dataset_rule_results", []),
            "slos": slos or {},
            "row_rule_failures": row_rule_failures or [],
            "schema_drift": schema_drift or {},
        }

    def _inject_lineage(
        self,
        good_df: Any,
        bad_df: Any,
        source_path: Optional[Union[str, Path]] = None,
    ) -> Tuple[Any, Any]:
        """Delegate to lakelogic.core.lineage (kept for backward compat)."""
        from lakelogic.core.lineage import inject_lineage
        return inject_lineage(
            good_df, bad_df, self.contract, self.engine_name,
            self.last_run_id, self.pipeline_run_id, source_path,
        )

    def _preserve_upstream_lineage(self, df: Any, columns: List[str], prefix: str) -> Any:
        """Delegate to lakelogic.core.lineage (kept for backward compat)."""
        from lakelogic.core.lineage import _preserve_upstream_lineage
        return _preserve_upstream_lineage(df, columns, prefix, self.engine_name)

    def _add_columns(self, df: Any, columns: Dict[str, Any]) -> Any:
        """Delegate to lakelogic.core.lineage (kept for backward compat)."""
        from lakelogic.core.lineage import add_columns
        return add_columns(df, columns, self.engine_name)

    def _add_current_trace(self, step: str, **kwargs):
        """Helper to add trace steps to the active run."""
        if hasattr(self, "_active_trace_steps") and self._active_trace_steps is not None:
            from lakelogic.core.models import TraceStep
            import time
            self._active_trace_steps.append(TraceStep(
                step=step,
                timestamp=time.time(),
                **kwargs
            ))

    def trace_step(self, name: str, **details):
        """
        Public context manager for custom Python tracing.
        Usage:
            with processor.trace_step("My Calculation", detail="foo"):
                ...
        """
        from contextlib import contextmanager
        
        @contextmanager
        def _trace():
            import time
            from lakelogic.core.models import TraceStep
            start = time.perf_counter()
            # Initial step without duration
            step = TraceStep(step=name, timestamp=time.time(), details=details, status="running")
            if hasattr(self, "_active_trace_steps"):
                self._active_trace_steps.append(step)
            
            try:
                yield step
                step.status = "ok"
            except Exception as e:
                step.status = "error"
                step.details["error"] = str(e)
                raise e
            finally:
                step.duration_ms = (time.perf_counter() - start) * 1000
        
        return _trace()

    def _apply_external_logic(self, good_df: Any) -> Tuple[Any, bool]:
        """Delegate to lakelogic.core.external_logic (kept for backward compat)."""
        from lakelogic.core.external_logic import apply_external_logic
        return apply_external_logic(
            self.contract, good_df, self.engine_name,
            self.last_run_id, self.last_source_path,
            add_trace_fn=self._add_current_trace,
            trace_step_fn=self.trace_step,
        )

    def _run_python_logic(self, path: Path, logic, good_df: Any) -> Tuple[Any, bool]:
        """Delegate to lakelogic.core.external_logic (kept for backward compat)."""
        from lakelogic.core.external_logic import _run_python_logic
        return _run_python_logic(
            path, logic, good_df, self.contract, self.engine_name,
            self.last_run_id, self._add_current_trace, self.trace_step,
        )

    def _run_notebook_logic(self, path: Path, logic, good_df: Any) -> Tuple[Any, bool]:
        """Delegate to lakelogic.core.external_logic (kept for backward compat)."""
        from lakelogic.core.external_logic import _run_notebook_logic
        return _run_notebook_logic(
            path, logic, good_df, self.contract, self.engine_name,
            self.last_run_id, self.last_source_path,
        )

    def _load_output_frame(self, path: Path, fmt: Optional[str]) -> Any:
        """Delegate to lakelogic.core.external_logic (kept for backward compat)."""
        from lakelogic.core.external_logic import _load_output_frame
        return _load_output_frame(path, fmt)

    def _compute_slos(self, good_df: Any, counts: Dict[str, Optional[int]]) -> Dict[str, Any]:
        """Delegate to lakelogic.core.slo (kept for backward compat)."""
        from lakelogic.core.slo import compute_slos
        return compute_slos(self.contract, good_df, counts, self.engine_name)

    # ─── SLO helpers (delegated to lakelogic.core.slo) ────────────────────
    def _parse_duration_seconds(self, value: Any) -> Optional[float]:
        from lakelogic.core.slo import _parse_duration_seconds
        return _parse_duration_seconds(value)

    def _get_max_timestamp(self, df: Any, field: str) -> Optional[datetime]:
        from lakelogic.core.slo import _get_max_timestamp
        return _get_max_timestamp(df, field, self.engine_name)

    def _coerce_datetime(self, value: Any) -> Optional[datetime]:
        from lakelogic.core.slo import _coerce_datetime
        return _coerce_datetime(value)

    def _compute_freshness(self, good_df: Any, freshness_obj: Any) -> Dict[str, Any]:
        from lakelogic.core.slo import _compute_freshness
        return _compute_freshness(good_df, freshness_obj, self.engine_name)

    def _compute_availability(self, good_df: Any, counts: Dict[str, Optional[int]], availability_obj: Any) -> Dict[str, Any]:
        from lakelogic.core.slo import _compute_availability
        return _compute_availability(good_df, counts, availability_obj, self.engine_name)

    def _non_null_ratio(self, df: Any, field: str) -> Optional[float]:
        from lakelogic.core.slo import _non_null_ratio
        return _non_null_ratio(df, field, self.engine_name)

    def _extract_row_rule_failures(self, bad_df: Any) -> list:
        """
        Extract row-level rule failures from quarantined data.

        Args:
            bad_df: Quarantined dataframe.

        Returns:
            List of rule failure descriptors.
        """
        if bad_df is None:
            return []
        error_col = getattr(self.adapter, "ERROR_COLUMN", "_lakelogic_errors")
        errors = []

        try:
            import polars as pl
            if isinstance(bad_df, pl.DataFrame) and error_col in bad_df.columns:
                series = bad_df.select(pl.col(error_col)).to_series()
                for item in series:
                    if item:
                        errors.extend(item)
        except Exception:
            pass

        try:
            import pandas as pd
            if isinstance(bad_df, pd.DataFrame) and error_col in bad_df.columns:
                for item in bad_df[error_col].explode().dropna().tolist():
                    errors.append(item)
        except Exception:
            pass

        if self.engine_name == "spark":
            try:
                from pyspark.sql import functions as F
                if error_col in bad_df.columns:
                    rows = bad_df.select(F.explode(F.col(error_col)).alias("error")).distinct().collect()
                    errors.extend([r["error"] for r in rows if r["error"]])
            except Exception:
                pass

        failures = []
        for err in set(errors):
            if isinstance(err, str) and err.startswith("Rule failed: "):
                payload = err[len("Rule failed: "):]
                name = payload
                sql = None
                if " (" in payload and payload.endswith(")"):
                    name, sql = payload.split(" (", 1)
                    sql = sql[:-1]
                failures.append({"name": name, "sql": sql, "message": err})
            else:
                failures.append({"message": str(err)})
        return failures

    # ── DDL Generation ───────────────────────────────────────────────────

    def generate_ddl(
        self,
        backend: Optional[str] = None,
        *,
        table_name: Optional[str] = None,
        if_not_exists: bool = True,
    ) -> str:
        """
        Generate CREATE TABLE DDL from this contract's schema.

        Args:
            backend: Target backend (spark, duckdb, snowflake, etc.).
                     Defaults to the processor's engine.
            table_name: Override table name.
            if_not_exists: Include IF NOT EXISTS clause.

        Returns:
            SQL DDL string.
        """
        from lakelogic.core.ddl import generate_ddl as _generate_ddl
        return _generate_ddl(
            self.contract,
            backend or self.engine_name,
            table_name=table_name,
            if_not_exists=if_not_exists,
        )

    def create_table(
        self,
        backend: Optional[str] = None,
        *,
        table_name: Optional[str] = None,
        db_path: Optional[str] = None,
        connection: Any = None,
        dry_run: bool = False,
    ) -> str:
        """
        Generate and execute CREATE TABLE DDL from this contract's schema.

        Args:
            backend: Target backend. Defaults to the processor's engine.
            table_name: Override table name.
            db_path: Database file path (for DuckDB/SQLite).
            connection: Existing database connection.
            dry_run: If True, only return DDL without executing.

        Returns:
            The generated DDL string.
        """
        from lakelogic.core.ddl import create_table as _create_table
        return _create_table(
            self.contract,
            backend or self.engine_name,
            table_name=table_name,
            db_path=db_path,
            connection=connection,
            dry_run=dry_run,
        )

    # ── GDPR Compliance ──────────────────────────────────────────────────

    def forget(
        self,
        df: Any,
        subject_column: str,
        subject_ids: List,
        *,
        erasure_strategy: str = "nullify",
        hash_salt: str = "",
    ) -> Any:
        """
        GDPR Right-to-be-Forgotten: erase PII for specific data subjects.

        Uses the contract's ``pii: true`` field annotations to identify
        which columns contain personal data.

        Args:
            df: Input dataframe.
            subject_column: Column identifying the data subject (e.g., "customer_id").
            subject_ids: List of subject identifiers to erase.
            erasure_strategy: "nullify" (default), "hash", or "redact".
            hash_salt: Salt for hashing.

        Returns:
            DataFrame with PII erased for specified subjects.
        """
        from lakelogic.core.gdpr import forget_subjects
        return forget_subjects(
            df, self.contract, subject_column, subject_ids,
            erasure_strategy=erasure_strategy,
            hash_salt=hash_salt,
        )

    def mask_pii(
        self,
        df: Any,
        *,
        strategy: str = "nullify",
        hash_salt: str = "",
        columns: Optional[List[str]] = None,
    ) -> Any:
        """
        Mask all PII columns across all rows.

        Useful for creating anonymised copies for dev/test environments.

        Args:
            df: Input dataframe.
            strategy: "nullify" (default), "hash", or "redact".
            hash_salt: Salt for hashing.
            columns: Override column list (defaults to contract PII fields).

        Returns:
            DataFrame with PII columns masked.
        """
        from lakelogic.core.gdpr import mask_pii_columns
        return mask_pii_columns(
            df, self.contract,
            strategy=strategy,
            hash_salt=hash_salt,
            columns=columns,
        )

    # ── Polars Streaming ─────────────────────────────────────────────────

    def run_source_streaming(
        self,
        source: Optional[Union[str, Path]] = None,
        *,
        output_path: Optional[str] = None,
    ) -> Any:
        """
        Load and process data using Polars streaming (LazyFrame).

        Uses ``scan_*`` instead of ``read_*`` to handle files larger
        than available memory. The contract rules are applied lazily
        and the result is either collected or sunk to a target file.

        Args:
            source: Source file path. Defaults to contract source.
            output_path: Optional path to sink results directly to disk
                         (avoids collecting into memory).

        Returns:
            ValidationResult with LazyFrame-backed good/bad frames,
            or metadata dict if output_path is specified.

        Raises:
            ValueError: If engine is not Polars or source format is unsupported.
        """
        if self.engine_name != "polars":
            raise ValueError(
                f"Streaming mode requires the 'polars' engine, got '{self.engine_name}'."
            )

        import polars as pl

        path_val = source or (self.contract.source.path if self.contract.source else None)
        if not path_val:
            raise ValueError("No source path provided and no path in contract.")
        path = self._resolve_source_path(path_val)
        path_str = str(path)

        logger.info(f"Loading source in streaming mode: {path_str}")

        # Use scan_* for lazy evaluation
        if path_str.endswith(".parquet"):
            lf = pl.scan_parquet(path_str)
        elif path_str.endswith(".csv"):
            lf = pl.scan_csv(path_str)
        elif path_str.endswith(".ndjson") or path_str.endswith(".jsonl"):
            lf = pl.scan_ndjson(path_str)
        else:
            raise ValueError(
                f"Streaming mode supports .parquet, .csv, .ndjson/.jsonl files. "
                f"Got: {path_str}"
            )

        # Run the contract using the adapter (which works with LazyFrames)
        result = self.run(lf, source_path=path_str)

        if output_path:
            # Sink results directly to disk without collecting
            good = result.good
            if isinstance(good, pl.LazyFrame):
                if output_path.endswith(".parquet"):
                    good.sink_parquet(output_path)
                elif output_path.endswith(".csv"):
                    good.sink_csv(output_path)
                else:
                    good.collect().write_parquet(output_path)
            elif isinstance(good, pl.DataFrame):
                if output_path.endswith(".parquet"):
                    good.write_parquet(output_path)
                elif output_path.endswith(".csv"):
                    good.write_csv(output_path)
            logger.info(f"Streamed results to {output_path}")
            return {"target": output_path, "format": output_path.rsplit(".", 1)[-1]}

        return result

    # ── Date Dimension Generator ─────────────────────────────────────────

    @staticmethod
    def generate_date_dimension(
        start_date: str = "2020-01-01",
        end_date: str = "2030-12-31",
        *,
        fiscal_year_start_month: int = 1,
        holiday_calendar: str = "us",
        custom_holidays: Optional[Dict[str, str]] = None,
        include_relative_flags: bool = False,
        engine: str = "polars",
        table_name: Optional[str] = None,
        db_path: Optional[str] = None,
        connection: Any = None,
    ) -> Any:
        """
        Generate a date dimension table for the lakehouse.

        Args:
            start_date: Start date (ISO format).
            end_date: End date (ISO format).
            fiscal_year_start_month: Month when fiscal year starts (1-12).
            holiday_calendar: "us", "uk", or "none".
            custom_holidays: Dict of "YYYY-MM-DD" → "Name".
            include_relative_flags: Include is_today, is_current_month, etc.
            engine: Output engine ("polars", "pandas", "duckdb").
            table_name: For DuckDB, the table name to create.
            db_path: For DuckDB, the database path.
            connection: Existing database connection.

        Returns:
            DataFrame (Polars/Pandas) or table name (DuckDB).
        """
        from lakelogic.core.dim_date import generate_date_dimension as _gen
        return _gen(
            start_date=start_date,
            end_date=end_date,
            fiscal_year_start_month=fiscal_year_start_month,
            holiday_calendar=holiday_calendar,
            custom_holidays=custom_holidays,
            include_relative_flags=include_relative_flags,
            engine=engine,
            table_name=table_name,
            db_path=db_path,
            connection=connection,
        )
