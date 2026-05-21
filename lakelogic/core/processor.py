import os
import sys
import yaml
import re
import warnings
from typing import Any, Tuple, Union, Dict, Optional, List
from pathlib import Path
from datetime import datetime, timezone

from lakelogic.core.models import DataContract
from lakelogic.engines.base import EngineAdapter
from lakelogic.notifications.base import (
    get_notification_adapter,
    render_notification_content,
)
from lakelogic.core.materialization import (
    materialize_dataframe,
    materialize_quarantine,
    write_run_log,
)
from loguru import logger


class ValidationResult:
    """
    Result object for LakeLogic runs.

    Unpacks as ``good_df, bad_df = processor.run(df)`` for the common
    two-variable pattern.  The raw (pre-validation) frame and the
    execution trace are available via ``.raw`` and ``.trace`` attributes.

    Use ``.source_count``, ``.good_count``, and ``.bad_count`` for
    engine-agnostic row counts (works with Polars, Pandas, Spark, DuckDB).
    """

    def __init__(self, good, bad, raw=None, trace=None, auto_fix_hint: Optional[str] = None):
        self.good = good
        self.bad = bad
        self.raw = raw
        self.trace = trace
        # Populated from contract-defined remediation templates.
        # SaaS layer uses this to surface Zeus AI fix suggestions
        # without embedding LLM calls in the OSS engine.
        self.auto_fix_hint: Optional[str] = auto_fix_hint

    # ── Engine-agnostic row counting ──────────────────────────────
    @staticmethod
    def _count_rows(obj):
        """Return the row count of any DataFrame-like object (Polars, Pandas, Spark, DuckDB)."""
        if obj is None:
            return 0
        # Polars DataFrame / LazyFrame
        if hasattr(obj, "height"):
            return obj.height
        # Spark DataFrame — .count() returns an int directly
        if hasattr(obj, "count") and callable(getattr(obj, "count")):
            try:
                res = obj.count()
                if isinstance(res, int):
                    return res
                # DuckDB relation — .count() returns a cursor
                if hasattr(res, "fetchone"):
                    return res.fetchone()[0]
            except Exception:
                pass
        # Pandas / list / anything with len()
        try:
            return len(obj)
        except Exception:
            return 0

    @property
    def source_count(self) -> int:
        """Row count of the raw (pre-validation) data."""
        return self._count_rows(self.raw)

    @property
    def good_count(self) -> int:
        """Row count of records that passed all quality rules."""
        return self._count_rows(self.good)

    @property
    def bad_count(self) -> int:
        """Row count of records routed to quarantine."""
        return self._count_rows(self.bad)

    @property
    def quarantine_ratio(self) -> float:
        """Ratio of bad records to total records (0.0 to 1.0)."""
        total = self.source_count
        if total == 0:
            return 0.0
        return self.bad_count / total

    @property
    def quality_score(self) -> float:
        """Quality score as a percentage (0-100). Calculated as (1 - quarantine_ratio) * 100."""
        return (1 - self.quarantine_ratio) * 100

    # ── Standard dunder methods ───────────────────────────────────
    def __iter__(self):
        yield self.good
        yield self.bad

    def __getitem__(self, idx):
        return [self.good, self.bad][idx]

    def __len__(self):
        return 2

    def __repr__(self):
        return f"ValidationResult(good={self.good_count}, bad={self.bad_count})"


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
        run_log_mode: Optional[str] = None,
    ):
        """
        Initialize the DataProcessor.

        Args:
            contract: The Data Contract definition (path to YAML, dict, or DataContract object).
            engine: The execution engine to use. If None, it uses the auto-discovery logic.
            stage: Optional stage override (e.g., bronze/silver) applied from contract "stages".
            pipeline_run_id: Optional pipeline-level run id for correlation across contracts.
            trace: Enable detailed execution tracing and row debugging.
            run_log_mode: Which run log backends to use: "dir", "table", or "all" (default).
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
        self._run_log_mode: Optional[str] = run_log_mode

        # ── Resolve contract context once ─────────────────────────────
        # These are used by notify(), _notification_template_context(),
        # _build_report(), and run() — resolved once to avoid fragmentation.
        _metadata = getattr(self.contract, "metadata", {}) or {}
        _info = getattr(self.contract, "info", None)
        self._resolved_domain = _metadata.get("domain") or (getattr(_info, "domain", None) if _info else None)
        self._resolved_system = _metadata.get("system") or (getattr(_info, "system", None) if _info else None)
        self._resolved_environment = (
            _metadata.get("environment") or os.environ.get("ENVIRONMENT") or os.environ.get("ENV") or "local"
        )
        self._resolved_data_layer = (
            _metadata.get("data_layer")
            or (getattr(_info, "data_layer", None) if _info else None)
            or (getattr(_info, "target_layer", None) if _info else None)
        )

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
        3. Polars (if installed, default fallback)
        """
        # 1. Check Env Var
        env_engine = os.getenv("LAKELOGIC_ENGINE")
        if env_engine:
            return env_engine

        # 2. Check for Spark (Databricks/Synapse detection)
        if "pyspark" in sys.modules or "spark" in globals():
            return "spark"

        # 3. Default to Polars
        return "polars"

    def _get_adapter(self) -> EngineAdapter:
        """
        Instantiates the correct adapter based on the engine name.
        """
        if self.engine_name == "polars":
            from lakelogic.engines.polars import PolarsAdapter

            return PolarsAdapter(self.contract)
        elif self.engine_name in ["spark", "pyspark"]:  # pragma: no cover
            from lakelogic.engines.spark import SparkAdapter

            return SparkAdapter(self.contract)
        elif self.engine_name == "snowflake":
            from lakelogic.engines.snowflake import SnowflakeAdapter

            return SnowflakeAdapter(self.contract)
        elif self.engine_name == "bigquery":
            from lakelogic.engines.bigquery import BigQueryAdapter

            return BigQueryAdapter(self.contract)
        elif self.engine_name == "duckdb":
            from lakelogic.engines.duckdb import DuckDBAdapter

            return DuckDBAdapter(self.contract)
        else:
            raise ValueError(f"Unsupported engine: {self.engine_name}")

    def _load_contract(self, contract: Union[str, Path, dict, DataContract]) -> DataContract:
        """
        Loads the contract from various formats.

        Args:
            contract: YAML path, inline YAML string, dict, or DataContract instance.

        Returns:
            Loaded DataContract.
        """
        if isinstance(contract, DataContract):
            loaded = self._apply_stage_overrides(contract)
            loaded = self._apply_fact_governance(loaded)
            return self._apply_cdc_defaults(loaded)
        if isinstance(contract, dict):
            loaded = DataContract(**contract)
            loaded = self._apply_stage_overrides(loaded)
            loaded = self._apply_fact_governance(loaded)
            return self._apply_cdc_defaults(loaded)

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

        # Handle inline YAML strings
        if isinstance(contract, str) and ("\n" in contract or "version:" in contract):
            data = _load_yaml_no_on_bool(contract)
            contract_obj = DataContract(**data)
            loaded = self._apply_stage_overrides(contract_obj)
            loaded = self._apply_fact_governance(loaded)
            return self._apply_cdc_defaults(loaded)

        path = Path(contract)
        if not path.exists():
            raise FileNotFoundError(f"Contract file not found: {path}")

        with open(path, "r") as f:
            data = _load_yaml_no_on_bool(f)
            contract_obj = DataContract(**data)
            try:
                contract_obj._base_path = path.parent
                contract_obj._contract_path = path
            except Exception:  # pragma: no cover - defensive: pydantic models permit attr set
                pass
            loaded = self._apply_stage_overrides(contract_obj)
            loaded = self._apply_fact_governance(loaded)
            return self._apply_cdc_defaults(loaded)

    def _apply_fact_governance(self, contract: DataContract) -> DataContract:
        """
        Auto-injects Kimball governance rules based on `materialization.fact` config.
        """
        if not contract.materialization or not contract.materialization.fact:
            return contract

        fact_cfg = contract.materialization.fact
        fact_type = str(fact_cfg.type).strip().lower()

        # 1. Transaction Facts -> Must be immutable append-only ledgers
        if fact_type == "transaction":
            if contract.materialization.strategy != "append":
                raise ValueError(
                    f"Fact table type 'transaction' requires strategy 'append'. "
                    f"Found '{contract.materialization.strategy}'."
                )

        # 2. Factless Facts -> Must contain no metric columns (only keys)
        if fact_type == "factless" and contract.model and contract.model.fields:
            pk_cols = set(contract.primary_key or [])
            for field in contract.model.fields:
                is_key = (
                    field.name in pk_cols
                    or field.name.endswith("_sk")
                    or field.name.endswith("_id")
                    or field.foreign_key is not None
                )
                is_num = any(t in str(field.type).lower() for t in ["int", "float", "double", "decimal", "numeric"])
                if is_num and not is_key:
                    logger.warning(
                        f"Factless Fact Warning: Column '{field.name}' appears to be a metric "
                        f"but fact type is 'factless'."
                    )

        # 3. Accumulating Snapshots -> Generate timestamp sequence rules
        if fact_type == "accumulating_snapshot" and fact_cfg.milestone_dates:
            from lakelogic.core.models import Quality, QualityRule

            milestones = fact_cfg.milestone_dates
            if not contract.quality:
                contract.quality = Quality()

            for i in range(len(milestones) - 1):
                start_col = milestones[i]
                end_col = milestones[i + 1]
                rule_name = f"fact_milestone_{start_col}_to_{end_col}"

                # Check if user already defined this rule so we don't duplicate
                existing = [r for r in contract.quality.row_rules if getattr(r, "name", "") == rule_name]
                if not existing:
                    contract.quality.row_rules.insert(
                        0,  # Insert at the front so they run first!
                        QualityRule(
                            name=rule_name,
                            sql=f"({end_col} IS NULL) OR ({end_col} >= {start_col})",
                            severity="error",
                            category="correctness",
                            description=(
                                f"Auto-generated Fact milestone constraint: {end_col} must occur after {start_col}"
                            ),
                        ),
                    )

        return contract

    def _apply_cdc_defaults(self, contract: DataContract) -> DataContract:
        """
        Auto-injects soft-delete column configuration when load_mode is 'cdc'
        and the user hasn't explicitly configured a hard-delete behavior.
        """
        if contract.source and str(getattr(contract.source, "load_mode", "")).lower() == "cdc":
            # Change watermark strategy to pipeline_log to correctly utilize CDC timestamps
            c_strat = getattr(contract.source, "watermark_strategy", None)
            if c_strat in [None, "max_target"]:
                contract.source.watermark_strategy = "pipeline_log"

            # Ensure materialization exists — system defaults may not have been
            # merged yet if the contract was loaded directly (not via runner).
            if contract.materialization is None:
                from lakelogic.core.models import Materialization

                contract.materialization = Materialization()

            # CDC processing inherently requires a merge strategy
            if getattr(contract.materialization, "strategy", None) in [None, "append"]:
                contract.materialization.strategy = "merge"

            if contract.materialization.soft_delete_column is None:
                contract.materialization.soft_delete_column = "_lakelogic_is_deleted"
                if contract.materialization.soft_delete_time_column is None:
                    contract.materialization.soft_delete_time_column = "_lakelogic_deleted_at"
                if contract.materialization.soft_delete_reason_column is None:
                    contract.materialization.soft_delete_reason_column = "_lakelogic_delete_reason"
        return contract

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
        from uuid import uuid4 as _uuid4

        from lakelogic.core.models import ExecutionTrace, TraceStep

        # Generate run_id at the very start so inject_lineage can stamp it.
        # Only reset if not already set (streaming / incremental callers pre-set it).
        if not self.last_run_id:
            self.last_run_id = str(_uuid4())

        start_time = time.perf_counter()
        start_time_utc = time.time()

        # Capture source_path for run report
        if source_path and not self.last_source_path:
            self.last_source_path = str(source_path)

        # ── EXTERNAL LOGIC HOOK ──
        # We run this BEFORE adapter.execute() so that the framework's strict schema
        # drift and data quality rules natively validate the new transformed dataframe.
        from lakelogic.core.external_logic import apply_external_logic

        if self.contract.external_logic:
            pre_count = 0
            if hasattr(self.adapter, "_get_row_count"):
                try:
                    pre_count = self.adapter._get_row_count(df)
                except Exception:  # pragma: no cover - defensive: row-count tolerated to fail
                    pass

            step_start = time.perf_counter()
            df, external_handled = apply_external_logic(
                self.contract,
                df,
                self.engine_name,
                self.last_run_id,
                self.last_source_path,
                add_trace_fn=self._add_current_trace,
                trace_step_fn=self.trace_step,
            )

            post_count = 0
            if hasattr(self.adapter, "_get_row_count"):
                try:
                    post_count = self.adapter._get_row_count(df)
                except Exception:  # pragma: no cover - defensive: row-count tolerated to fail
                    pass

            self._active_trace_steps.append(
                TraceStep(
                    step=f"External Logic ({self.contract.external_logic.type})",
                    timestamp=time.time(),
                    input_rows=pre_count,
                    output_rows=post_count,
                    duration_ms=(time.perf_counter() - step_start) * 1000,
                    details={"path": self.contract.external_logic.path},
                    status="ok",
                )
            )
        else:
            df, external_handled = apply_external_logic(
                self.contract,
                df,
                self.engine_name,
                self.last_run_id,
                self.last_source_path,
            )

        # Execute via adapter (pre/post-transforms, schema bounds, and row rules)
        good_df, bad_df = self.adapter.execute(df)

        # Merge adapter trace if present
        if hasattr(self.adapter, "trace") and self.adapter.trace:
            self._active_trace_steps.extend(self.adapter.trace)

        # ── INTERNALLY ENFORCE SQL FIELD LIST ────────────────────────────────
        # If the user defined an explicit model and wants unknown_fields dropped,
        # prune undocumented columns.
        if good_df is not None and self.contract.model and self.contract.model.fields:
            # Resolve unknown_fields policy: server.schema_policy > contract.schema_policy > SchemaPolicy default
            from lakelogic.core.models import SchemaPolicy as _SP

            policy = _SP().unknown_fields  # Use the model default

            # Check root-level schema_policy first
            root_sp = getattr(self.contract, "schema_policy", None)
            if root_sp and getattr(root_sp, "unknown_fields", None):
                policy = root_sp.unknown_fields.lower()

            # Server-level schema_policy takes priority
            server = getattr(self.contract, "server", None)
            if server and getattr(server, "schema_policy", None):
                policy = (server.schema_policy.unknown_fields or policy).lower()

            # If policy is 'drop', prune unknown columns.
            if policy == "drop":
                expected_cols = [f.name for f in self.contract.model.fields]

                # Polars Engine
                if self.engine_name == "polars":
                    existing = good_df.columns
                    kepts = [c for c in expected_cols if c in existing]
                    if kepts:
                        good_df = good_df.select(kepts)

                # Spark Engine
                elif self.engine_name == "spark":  # pragma: no cover
                    existing = good_df.columns
                    kepts = [c for c in expected_cols if c in existing]
                    # Preserve _source_file for lineage injection (added during load)
                    if "_source_file" in existing and "_source_file" not in kepts:
                        kepts.append("_source_file")
                    if kepts:
                        good_df = good_df.select(*kepts)

                # DuckDB / SQLite Engines
                elif self.engine_name in ("duckdb", "sqlite"):
                    existing = list(good_df.columns) if hasattr(good_df, "columns") else []
                    kepts = [c for c in expected_cols if c in existing]
                    if kepts and hasattr(good_df, "select"):
                        try:
                            # DuckDB PyRelation supports string args in select()
                            good_df = good_df.select(*kepts)
                        except Exception as exc:  # pragma: no cover - defensive: DuckDB API variance
                            logger.warning(f"DuckDB column select failed ({exc}), schema enforcement skipped")

                # Pandas Engine
                elif self.engine_name == "pandas":
                    existing = good_df.columns.tolist()
                    kepts = [c for c in expected_cols if c in existing]
                    if kepts:
                        good_df = good_df[kepts]

        # Inject lineage metadata
        step_start = time.perf_counter()
        from lakelogic.core.lineage import inject_lineage

        good_df, bad_df = inject_lineage(
            good_df,
            bad_df,
            self.contract,
            self.engine_name,
            self.last_run_id,
            self.pipeline_run_id,
            source_path,
        )
        self._active_trace_steps.append(
            TraceStep(
                step="Lineage Injection",
                timestamp=time.time(),
                duration_ms=(time.perf_counter() - step_start) * 1000,
                status="ok",
            )
        )

        # ── PII Masking ──────────────────────────────────────────────────
        # Apply per-field masking strategies defined in the contract model.
        # This runs AFTER lineage injection so lineage columns are preserved,
        # and BEFORE materialization so masked data is what gets written.
        pii_fields = []
        if self.contract.model and self.contract.model.fields:
            pii_fields = [f for f in self.contract.model.fields if f.pii]

        if pii_fields and good_df is not None:
            step_start_mask = time.perf_counter()
            try:
                from lakelogic.core.masking_engine import MaskingEngine

                # Resolve user groups from environment or contract metadata
                user_groups_raw = os.environ.get("LAKELOGIC_USER_GROUPS", "")
                user_groups = [g.strip() for g in user_groups_raw.split(",") if g.strip()] or None

                engine = MaskingEngine(
                    self.contract,
                    encryption_key=os.environ.get("LAKELOGIC_PII_KEY", ""),
                    hash_salt=os.environ.get("LAKELOGIC_PII_SALT", ""),
                )
                good_df = engine.apply(good_df, user_groups=user_groups)

                # Extract PII vault DataFrame for dual-write if any fields
                # specify pii_vault
                vault_fields = engine.get_vault_fields()
                if vault_fields:
                    logger.info(
                        f"PII vault fields detected: {[f.name for f in vault_fields]}. "
                        f"Vault extraction available via MaskingEngine.extract_vault_df()."
                    )

                self._active_trace_steps.append(
                    TraceStep(
                        step="PII Masking",
                        timestamp=time.time(),
                        duration_ms=(time.perf_counter() - step_start_mask) * 1000,
                        status="ok",
                        details={
                            "fields_masked": len(engine.get_fields_to_mask(user_groups)),
                            "strategies": {f.name: f.masking or "redact" for f in pii_fields},
                            "user_groups": user_groups,
                        },
                    )
                )
            except Exception as mask_exc:
                logger.warning(f"PII masking failed ({mask_exc}), proceeding without masking")
                self._active_trace_steps.append(
                    TraceStep(
                        step="PII Masking",
                        timestamp=time.time(),
                        duration_ms=(time.perf_counter() - step_start_mask) * 1000,
                        status="error",
                        details={"error": str(mask_exc)},
                    )
                )

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
            domain = self._resolved_domain
            system = self._resolved_system
            data_layer = self._resolved_data_layer
            # Resolve a human-readable target identifier for parallel-mode logs
            _info = getattr(self.contract, "info", None)
            _target_name = (
                getattr(_info, "table_name", None)
                or getattr(_info, "title", None)
                or getattr(self.contract, "dataset", None)
                or "unknown"
            )
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

            # Detect whether the transformations perform aggregation
            # (GROUP BY, COUNT, SUM, AVG, etc.).  When aggregation is present,
            # the row-count reduction is intentional summarisation — not data loss.
            _is_aggregation = False
            _transforms = getattr(self.contract, "transformations", None) or []
            if not isinstance(_transforms, list):  # pragma: no cover - defensive
                try:
                    _transforms = list(_transforms)
                except Exception:
                    _transforms = []
            for _t in _transforms:
                _sql = getattr(_t, "sql", None) or ""
                if _sql and any(
                    kw in _sql.upper() for kw in ("GROUP BY", "GROUP  BY", "SUM(", "COUNT(", "AVG(", "MIN(", "MAX(")
                ):
                    _is_aggregation = True
                    break

            # Choose the appropriate label and update counts dict for run log
            if dropped is not None and dropped > 0:
                if _is_aggregation:
                    _dropped_label = "Aggregated"
                    # Reclassify in counts so the run log and telemetry
                    # correctly distinguish summarisation from data loss.
                    counts["aggregated_rows"] = dropped
                    counts["pre_transform_dropped"] = 0
                else:
                    _dropped_label = "Pre-Transform Dropped"
                _dropped_line = f" | {_dropped_label}: {dropped}"
            else:
                _dropped_line = ""

            _dropped_display = f", {_dropped_label}: {dropped}" if dropped is not None and dropped > 0 else ""  # noqa: F841
            _source_display = f"Source: {source_total}, " if source_total is not None else ""  # noqa: F841
            if quality_enabled:
                logger.info(
                    f"Run complete{tags_display} | "
                    f"Source: {source_total if source_total is not None else 'n/a'} | "
                    f"Total: {total} | Good: {counts.get('good')} | Quarantine: {bad}{_dropped_line} | "
                    f"Ratio: {ratio_display}"
                )
            else:
                logger.info(
                    f"Run complete{tags_display} | "
                    f"Source: {source_total if source_total is not None else 'n/a'} | "
                    f"Total: {total}{_dropped_line}"
                )

            if bad > 0:
                reason_summary = ""
                if bad_df is not None:
                    error_col = getattr(self.adapter, "ERROR_COLUMN", "_lakelogic_errors")
                    reasons = []
                    try:
                        import polars as pl

                        if isinstance(bad_df, pl.DataFrame) and error_col in bad_df.columns:
                            s = bad_df.get_column(error_col).explode().drop_nulls()
                            if len(s) > 0:
                                counts_df = s.value_counts(sort=True)
                                reasons = [f"{row[0]} ({row[1]})" for row in counts_df.iter_rows() if row[0]]
                    except Exception:  # pragma: no cover - defensive: polars schema variance
                        pass

                    if not reasons:
                        try:
                            import pandas as pd

                            if isinstance(bad_df, pd.DataFrame) and error_col in bad_df.columns:
                                err_counts = bad_df[error_col].explode().dropna().value_counts()
                                reasons = [f"{k} ({v})" for k, v in err_counts.items() if k]
                        except Exception:  # pragma: no cover - defensive: pandas schema variance
                            pass

                    if not reasons and hasattr(bad_df, "select") and hasattr(bad_df, "groupBy"):
                        try:
                            from pyspark.sql.functions import col, explode_outer

                            if error_col in bad_df.columns:
                                err_counts = (
                                    bad_df.select(explode_outer(col(error_col)).alias("err"))
                                    .filter(col("err").isNotNull())
                                    .groupBy("err")
                                    .count()
                                    .orderBy(col("count").desc())
                                    .limit(10)
                                    .collect()
                                )
                                reasons = [f"{row['err']} ({row['count']})" for row in err_counts if row["err"]]
                        except Exception:  # pragma: no cover - defensive: requires Spark fixture
                            pass

                    if reasons:
                        top_reasons = reasons[:10]
                        if len(reasons) > 10:
                            top_reasons.append(f"...and {len(reasons) - 10} more")
                        reason_summary = (
                            "\n\nRule Failure Breakdown (records can fail multiple rules):\n- "
                            + "\n- ".join(top_reasons)
                        )

                msg = (
                    f"LakeLogic Alert: {bad} records quarantined in '{contract_title}'. "
                    f"Total (post-transform): {total} (ratio {ratio_display}){reason_summary}"
                )
                self.notify(event="quarantine", message=msg)

        # Check dataset rules
        if hasattr(self.adapter, "dataset_rule_results"):
            failures = [r for r in self.adapter.dataset_rule_results if not r.get("passed")]
            if failures:
                details = "; ".join([f"{r.get('name')}={r.get('value')}" for r in failures])
                msg = f"LakeLogic Dataset Quality Check failed in '{contract_title}': {details}"
                self.notify(event="dataset_quality_check", message=msg)

                # Halt pipeline if configured
                halt_on_fail = getattr(self.contract.quality, "fail_pipeline_on_dataset_error", False)
                if halt_on_fail:
                    raise ValueError(
                        f"Pipeline halted: Dataset Quality rules failed in '{contract_title}'. Details: {details}"
                    )

        # Schema drift detection (ingest mode)
        drift = getattr(self.adapter, "schema_drift", {}) or {}
        if drift.get("missing_fields") or drift.get("unknown_fields"):
            # Exclude columns that will be created by transformations
            derived_fields = set()
            rename_targets = set()  # new names (will appear after rename)
            rename_sources = set()  # old names (expected in source, removed after rename)
            drop_columns = set()  # columns explicitly dropped (acknowledged, not drift)
            if self.contract.transformations:
                for t in self.contract.transformations:
                    t_dict = t if isinstance(t, dict) else (t.model_dump() if hasattr(t, "model_dump") else {})
                    # Derived columns (phase: post)
                    derive = t_dict.get("derive") or {}
                    if derive.get("field"):
                        derived_fields.add(derive["field"])
                    # Rename mappings — source → target
                    rename = t_dict.get("rename") or {}
                    mappings = rename.get("mappings") or {}
                    for src, tgt in mappings.items():
                        rename_sources.add(src)
                        rename_targets.add(tgt)
                    # Drop columns — explicitly removed by the contract
                    drop = t_dict.get("drop") or {}
                    drop_cols = drop.get("columns") or []
                    for col in drop_cols:
                        drop_columns.add(col)

            # Filter out false alarms
            missing = drift.get("missing_fields", [])
            unknown = drift.get("unknown_fields", [])
            # _source_file is a transient column from _metadata.file_path
            # capture — excluded from drift as it's dropped after lineage.
            internal_cols = {"_source_file"}
            # Check if the contract has SQL transformations (post-phase).
            # When SQL transforms exist, the source DataFrame columns are
            # intermediate inputs consumed by SQL — they are NOT drift.
            # e.g. bronze has 'event_params_json' which is transformed to
            # 'ga_session_id' in silver. Without this, every bronze column
            # that isn't in the silver model gets flagged as "unknown".
            has_sql_transforms = False
            if self.contract.transformations:
                import re

                _KNOWN_TYPES = {
                    "VARCHAR",
                    "BIGINT",
                    "SMALLINT",
                    "TINYINT",
                    "INTEGER",
                    "BOOLEAN",
                    "STRING",
                    "INT",
                    "LONG",
                    "DOUBLE",
                    "FLOAT",
                    "DATE",
                    "TIMESTAMP",
                }
                for t in self.contract.transformations:
                    t_dict = t if isinstance(t, dict) else (t.model_dump() if hasattr(t, "model_dump") else {})
                    sql_text = t_dict.get("sql")
                    if sql_text:
                        has_sql_transforms = True
                        # Best effort: parse AS aliases so they aren't flagged as missing schema drift
                        for m in re.finditer(r'\bAS\s+(["\w]+)', str(sql_text), re.IGNORECASE):
                            alias = m.group(1).strip("\"'")
                            if alias.upper() not in _KNOWN_TYPES:
                                derived_fields.add(alias)
            # ── SCD2 control columns are injected by the materializer ────────
            # They won't exist in the source DataFrame at drift-check time,
            # so suppress them from "missing" warnings.
            _mat_cfg = getattr(self.contract, "materialization", None)
            _mat_strategy = getattr(_mat_cfg, "strategy", None) if _mat_cfg else None
            if _mat_strategy == "scd2":
                _scd2_cfg = getattr(_mat_cfg, "scd2", None)
                if _scd2_cfg:
                    _scd2_dict = (
                        _scd2_cfg
                        if isinstance(_scd2_cfg, dict)
                        else (_scd2_cfg.model_dump() if hasattr(_scd2_cfg, "model_dump") else {})
                    )
                    for _scd2_key in (
                        "surrogate_key",
                        "effective_from_field",
                        "effective_to_field",
                        "current_flag_field",
                        "version_column",
                        "change_reason_column",
                    ):
                        _scd2_val = _scd2_dict.get(_scd2_key)
                        if _scd2_val:
                            derived_fields.add(_scd2_val)

            real_missing = sorted(set(missing) - derived_fields - rename_targets)

            # Remove rename sources, drop columns, internal columns, and framework lineage columns
            real_unknown = sorted(
                c
                for c in set(unknown) - rename_sources - drop_columns - internal_cols
                if not c.startswith("_lakelogic_")
            )

            # If SQL transformations exist, source columns are expected to
            # differ from model fields — suppress unknown column warnings.
            if has_sql_transforms:
                real_unknown = []

            # Update the drift dictionary to suppress these internal columns in the final run log
            if "missing_fields" in drift:
                drift["missing_fields"] = real_missing
            if "unknown_fields" in drift:
                drift["unknown_fields"] = real_unknown

            from lakelogic.core.models import SchemaPolicy as _SP

            _default_policy = _SP().unknown_fields
            policy = drift.get("policy", _default_policy)
            if real_missing or real_unknown:
                drift_msg = (
                    f"Schema drift detected for '{contract_title}': missing={real_missing}, unknown={real_unknown}"
                )
                logger.warning(drift_msg)
                if policy == "quarantine":
                    self.notify(event="schema_drift", message=drift_msg)

        # Extract validation failure details early so both fail-fast paths can surface them
        row_rule_failures = self._extract_row_rule_failures(bad_df)

        # Fail-fast if quarantine is disabled
        if self.contract.quarantine and not self.contract.quarantine.enabled:
            if bad and bad > 0:
                _detail_lines = []
                for f in row_rule_failures[:10]:
                    _msg = f.get("message") or f.get("name") or str(f)
                    _detail_lines.append(f"  • {_msg}")
                _detail_str = "\n".join(_detail_lines) if _detail_lines else "  (no rule details captured)"
                raise ValueError(
                    f"Quarantine disabled but {bad} record(s) failed validation for '{contract_title}'.\n"
                    f"Validation failures:\n{_detail_str}"
                )

        # Build run report and optionally write a log
        try:
            from lakelogic.core.slo import compute_slos

            slos = compute_slos(self.contract, good_df, counts, self.engine_name)
        except ImportError:
            # compute_slos was removed; SLOValidator uses a DomainRegistry now.
            # Per-contract SLO checks are deferred to the pipeline level.
            slos = []
        self.last_report = self._build_report(contract_title, counts, slos, row_rule_failures, drift)

        # Extract pre-transform filter string for run log
        pre_filters = []
        if self.contract.transformations:
            for t in self.contract.transformations:
                t_dict = t if isinstance(t, dict) else (t.model_dump() if hasattr(t, "model_dump") else {})
                t_phase = (t_dict.get("phase") or "post").lower()
                if t_phase == "pre" and t_dict.get("filter") and t_dict["filter"].get("sql"):
                    pre_filters.append(t_dict["filter"]["sql"])
        if pre_filters:
            self.last_report["pre_transform_filter"] = " AND ".join(f"({f})" for f in pre_filters)

        # Extract max watermark value if computed
        inc_meta = getattr(self, "_incremental_metadata", {})
        if "max_watermark_value" in inc_meta:
            self.last_report["max_watermark_value"] = inc_meta["max_watermark_value"]

        # Attach dlt extraction state for persistence in run log
        _dlt_state = getattr(self, "_pending_dlt_state_json", None)
        if _dlt_state:
            self.last_report["dlt_state_json"] = _dlt_state

        # Capture contract-level SLO row count thresholds for point-in-time auditability
        slo_cfg = getattr(self.contract, "service_levels", None)
        if slo_cfg:
            rc = (
                getattr(slo_cfg, "row_count", None)
                if hasattr(slo_cfg, "row_count")
                else (slo_cfg.get("row_count") if isinstance(slo_cfg, dict) else None)
            )
            if rc:
                min_r = (
                    getattr(rc, "min_rows", None)
                    if hasattr(rc, "min_rows")
                    else (rc.get("min_rows") if isinstance(rc, dict) else None)
                )
                max_r = (
                    getattr(rc, "max_rows", None)
                    if hasattr(rc, "max_rows")
                    else (rc.get("max_rows") if isinstance(rc, dict) else None)
                )
                if min_r is not None:
                    self.last_report["slo_row_count_min"] = min_r
                if max_r is not None:
                    self.last_report["slo_row_count_max"] = max_r

        # ── SLO breach notification dispatch ──────────────────────────
        # Check computed SLO results for failures and fire slo_breach
        # events through the existing notification system.
        # Skip config-mismatch failures (inherited SLO referencing fields
        # that don't exist on this contract's schema).
        _SLO_SKIP_REASONS = {"no_data", "no_data_or_threshold"}
        if slos and isinstance(slos, dict):
            breaches = []
            for check_name, check_result in slos.items():
                if isinstance(check_result, dict) and check_result.get("passed") is False:
                    reason = check_result.get("reason", "")
                    if reason in _SLO_SKIP_REASONS:
                        logger.debug(
                            f"SLO {check_name} skipped for '{contract_title}': "
                            f"field={check_result.get('field', '?')} ({reason})"
                        )
                        continue
                    detail_parts = []
                    if check_result.get("field"):
                        detail_parts.append(f"field={check_result['field']}")
                    if check_result.get("delay_seconds") is not None:
                        delay_min = round(check_result["delay_seconds"] / 60, 1)
                        detail_parts.append(f"delay={delay_min}min")
                    if check_result.get("threshold"):
                        detail_parts.append(f"threshold={check_result['threshold']}")
                    if check_result.get("actual_pct") is not None:
                        detail_parts.append(f"actual={check_result['actual_pct']}%")
                    if check_result.get("reason"):
                        detail_parts.append(check_result["reason"])
                    breaches.append(
                        {
                            "check": check_name,
                            "detail": ", ".join(detail_parts) if detail_parts else "failed",
                        }
                    )

            if breaches:
                breach_lines = [f"  • {b['check']}: {b['detail']}" for b in breaches]
                breach_msg = f"SLO breach detected for '{contract_title}':\n" + "\n".join(breach_lines)
                logger.warning(breach_msg)
                try:
                    self.notify(event="slo_breach", message=breach_msg)
                except Exception as e:  # pragma: no cover - defensive: notify backend optional
                    logger.debug(f"SLO breach notification failed: {e}")

        # Run log is written by the pipeline runner after materialize()
        # so it can capture the final status (succeeded/failed).

        # Materialize if requested
        if materialize and not external_handled:
            step_start = time.perf_counter()
            self.materialize(good_df, bad_df, target_path=materialize_target)
            self._active_trace_steps.append(
                TraceStep(
                    step="Materialization",
                    timestamp=time.time(),
                    duration_ms=(time.perf_counter() - step_start) * 1000,
                    status="ok",
                )
            )

        # Optional Remote Reporting (SaaS Bridge)
        try:  # pragma: no cover - defensive: RemoteObserver is opt-in SaaS feature
            from lakelogic.core.observer import RemoteObserver

            observer = RemoteObserver()
            observer.report(self.last_report)
        except Exception as exc:
            logger.debug(f"Remote observer not available or failed: {exc}")

        # Call to action for SaaS (opt-in only)
        quarantined = counts.get("quarantined")
        if quarantined is None:
            quarantined = 0
        if quarantined > 0 and os.getenv("LAKELOGIC_SHOW_TIPS", "false").lower() == "true":
            logger.info(
                "🛡️  View deep quarantine analysis & historical drift on Lineage Logic: https://lineagelogic.com"
            )

        total_duration_ms = (time.perf_counter() - start_time) * 1000
        end_time_utc = time.time()

        import datetime

        self.last_report["start_time"] = datetime.datetime.fromtimestamp(
            start_time_utc, tz=datetime.timezone.utc
        ).isoformat()
        self.last_report["end_time"] = datetime.datetime.fromtimestamp(
            end_time_utc, tz=datetime.timezone.utc
        ).isoformat()
        self.last_report["run_duration_seconds"] = total_duration_ms / 1000.0

        # ── Cost estimation ───────────────────────────────────────────
        try:
            from lakelogic.core.cost_provider import resolve_cost_provider

            cost_config = None
            metadata = getattr(self.contract, "metadata", {}) or {}
            cost_config = metadata.get("cost")
            cost_provider = resolve_cost_provider(cost_config)
            _counts = self.last_report.get("counts", {})
            cost_estimate = cost_provider.estimate(
                run_id=self.last_run_id or "",
                duration_seconds=total_duration_ms / 1000.0,
                rows=_counts.get("total", 0) if isinstance(_counts, dict) else 0,
                domain=metadata.get("domain", ""),
                system=metadata.get("system", ""),
                layer=metadata.get("data_layer", ""),
            )
            self.last_report["estimated_cost"] = cost_estimate.estimated_cost
            self.last_report["cost_currency"] = cost_estimate.currency
            self.last_report["cost_confidence"] = cost_estimate.confidence
        except Exception as _cost_exc:
            logger.debug(f"Cost estimation skipped: {_cost_exc}")
            self.last_report["estimated_cost"] = None
            self.last_report["cost_currency"] = None
            self.last_report["cost_confidence"] = "none"

        trace = ExecutionTrace(
            run_id=self.last_run_id,
            steps=self._active_trace_steps,
            total_duration_ms=total_duration_ms,
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
        try:
            from lakelogic.cli.main import _display_trace

            has_cli = True
        except ImportError:
            has_cli = False

        trace_to_show = trace
        if not trace_to_show:
            if hasattr(self, "last_result") and self.last_result and self.last_result.trace:
                trace_to_show = self.last_result.trace
            elif hasattr(self, "_active_trace_steps") and self._active_trace_steps:
                from lakelogic.core.models import ExecutionTrace

                trace_to_show = ExecutionTrace(run_id=self.last_run_id or "latest", steps=self._active_trace_steps)

        if not trace_to_show:
            return

        if has_cli:
            _display_trace(trace_to_show)
        else:
            logger.info(f"Execution Trace: run_id={trace_to_show.run_id}")
            for step in trace_to_show.steps:
                logger.info(f"[{step.status.upper()}] {step.step} ({step.duration_ms}ms)")

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
        except Exception as e:  # pragma: no cover - defensive: row sampling tolerated to fail
            logger.debug(f"Could not log row samples: {e}")

    def _get_sample_text(self, df: Any) -> str:
        """Convert a slice of the dataframe to a readable string."""
        if df is None:
            return "None"
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
        except Exception:  # pragma: no cover - defensive: sample conversion tolerated to fail
            return "(sample conversion failed)"

    def run_source(
        self,
        source: Optional[Union[str, Path]] = None,
        *,
        reprocess_from: Optional[str] = None,
        reprocess_to: Optional[str] = None,
        reprocess_column: Optional[str] = None,
        reprocess_values: Optional[List[str]] = None,
        lookback_days: Optional[int] = None,
    ) -> ValidationResult:
        """
        Loads data from a source file and runs the contract in one step.
        The data is loaded using the engine's optimized reader.

        Args:
            source: Optional file path to load. If None, uses path from contract.
            reprocess_from: Optional start date (YYYY-MM-DD) for date-range
                reprocessing.  When set, the incremental watermark is bypassed
                and only rows where the reprocess date column >= this value
                are kept.  The column is resolved from
                ``materialization.reprocess_date_column``; if not set, the
                first ``partition_by`` column is used.
            reprocess_to: Optional end date (YYYY-MM-DD) for date-range
                reprocessing.  Rows where the date column <= this value are
                kept.  Can be used alone or with ``reprocess_from``.
            reprocess_column: Optional column name for ID-based reprocessing.
            reprocess_values: Optional list of values for ID-based reprocessing.
            lookback_days: Optional runtime override for partition lookback_days.
                Overrides the contract's ``source.partition.lookback_days``.

        Returns:
            ValidationResult object (unpacks to good_df, bad_df).
        """
        self._active_trace_steps = []
        self._run_log_already_written = False

        # Store reprocess range for downstream use by materialize
        self._reprocess_from = reprocess_from
        self._reprocess_to = reprocess_to
        self._reprocess_values = reprocess_values

        # Per-contract reprocess_column: each layer can map to its own column name
        # (e.g. bronze=cus_id, silver=cus_ap_id, gold=cus_ss_id).
        # Falls back to the global reprocess_column parameter from the pipeline.
        mat = getattr(self.contract, "materialization", None)
        per_contract_col = getattr(mat, "reprocess_column", None) if mat else None
        resolved_reprocess_column = per_contract_col or reprocess_column
        self._reprocess_column = resolved_reprocess_column
        if per_contract_col and reprocess_column and per_contract_col != reprocess_column:
            logger.info(
                f"Using per-contract reprocess_column '{per_contract_col}' (overriding global '{reprocess_column}')"
            )

        self._is_reprocess = bool(reprocess_from or reprocess_to or (resolved_reprocess_column and reprocess_values))

        # ── dlt source: contract-driven API ingestion ─────────────────────────
        if self.contract.source and self.contract.source.type == "dlt":
            return self._run_dlt_source()

        # ── database source: native SQL ingestion ─────────────────────────────
        if self.contract.source and self.contract.source.type == "database":
            return self._run_database_source()

        path_val = source or (self.contract.source.path if self.contract.source else None)
        if not path_val:
            raise ValueError("No source path provided and no path found in contract.")
        path = self._resolve_source_path(path_val)

        # Resolve catalog table names (Unity Catalog, Fabric LakeDB, Synapse) to storage paths
        if self.engine_name != "spark":  # Spark handles catalogs natively
            from lakelogic.engines.catalog_resolver import resolve_catalog_path

            original_path = path
            path = resolve_catalog_path(path)
            if path != original_path:
                logger.info(f"Resolved catalog table: {original_path} -> {path}")

        logger.info(f"Loading source: {path} via {self.engine_name}")

        # ── Guard: validate watermark_strategy vs source type ─────────────────
        # pipeline_log  → compares file mtime → only valid for file sources
        # delta_version → reads Delta transaction log → only valid for table sources
        # Mismatches silently degrade to full reloads every run.
        _src_cfg = self.contract.source if self.contract.source else None
        _is_table = str(path).startswith("table:") or (
            _src_cfg and getattr(_src_cfg, "type", None) in ("table", "delta", "iceberg")
        )
        _wm_strategy = getattr(_src_cfg, "watermark_strategy", None) if _src_cfg else None
        _load_mode = getattr(_src_cfg, "load_mode", "full") if _src_cfg else "full"
        if _is_table and _wm_strategy == "pipeline_log" and getattr(_src_cfg, "type", None) == "table":
            raise ValueError(
                "Invalid configuration: source type 'table' cannot use "
                "watermark_strategy 'pipeline_log' (it relies on file modification times "
                "which don't exist for table sources). "
                "Valid strategies for table sources: "
                "(1) load_mode: 'cdc' with cdc_timestamp_field: '_lakelogic_processed_at', "
                "(2) watermark_strategy: 'delta_version'."
            )
        if not _is_table and _wm_strategy == "delta_version" and _load_mode in ("incremental",):
            raise ValueError(
                "Invalid configuration: file-based source cannot use "
                "watermark_strategy 'delta_version' (it relies on Delta transaction log "
                "versioning which doesn't exist for file sources). "
                "Valid strategies for file sources: "
                "(1) watermark_strategy: 'pipeline_log', "
                "(2) load_mode: 'cdc' with cdc_timestamp_field."
            )

        # ── Reprocessing mode: bypass incremental watermark ───────────────────
        _is_reprocess = bool(reprocess_from or reprocess_to or (resolved_reprocess_column and reprocess_values))
        if _is_reprocess:
            if resolved_reprocess_column and reprocess_values:
                logger.info(
                    f"Targeted reprocessing mode: filter on {resolved_reprocess_column} "
                    f"IN ({len(reprocess_values)} values) — incremental watermark bypassed"
                )
            else:
                logger.info(
                    f"Reprocessing mode: date range [{reprocess_from or '*'} .. "
                    f"{reprocess_to or '*'}] — incremental watermark bypassed"
                )

        # ── Date-partitioned landing: expand path into per-date globs ─────────
        partition_cfg = getattr(self.contract.source, "partition", None) if self.contract.source else None
        if partition_cfg and partition_cfg.format:
            # Auto-detect initial load: if no prior watermark exists, this is a
            # first run → skip lookback and scan ALL partitions via full glob.
            load_mode = getattr(self.contract.source, "load_mode", "full") if self.contract.source else "full"
            _is_initial_load = False
            watermark = None
            if load_mode == "incremental" and not _is_reprocess:
                watermark = self._get_last_source_watermark()
                if watermark is None:
                    _is_initial_load = True
                    # If lookback_days is explicitly set (runtime or contract),
                    # respect it even on initial load — this lets users limit
                    # the first ingest to X days instead of scanning everything.
                    _effective_lookback = lookback_days or getattr(partition_cfg, "lookback_days", None)
                    if _effective_lookback:
                        logger.info(
                            f"Initial load detected (no prior watermark) — "
                            f"using lookback_days={_effective_lookback} to limit partition scan"
                        )
                    else:
                        logger.info(
                            "Initial load detected (no prior watermark) — "
                            "scanning all partitions (set lookback_days to limit)"
                        )

            if _is_initial_load:
                _effective_lookback = lookback_days or getattr(partition_cfg, "lookback_days", None)
                if _effective_lookback:
                    # Use the partition scanner with the lookback constraint
                    effective_cfg = partition_cfg.model_copy(update={"lookback_days": _effective_lookback})
                    source_files = self._expand_partitioned_paths(path, effective_cfg)
                else:
                    # No lookback constraint — scan everything
                    source_files = self._expand_source_files(path + "/**/*")
                    if not source_files:
                        source_files = self._expand_source_files(path)
            else:
                # Apply runtime lookback_days override if provided
                effective_cfg = partition_cfg
                if lookback_days is not None:
                    effective_cfg = partition_cfg.model_copy(update={"lookback_days": lookback_days})

                eff_start = reprocess_from
                if not eff_start and watermark is not None:
                    from datetime import datetime, timezone

                    eff_start = datetime.fromtimestamp(watermark, tz=timezone.utc).date().isoformat()

                source_files = self._expand_partitioned_paths(
                    path,
                    effective_cfg,
                    override_start=eff_start,
                    override_end=reprocess_to,
                )

            # Guard: if partitioned expansion found zero files, return early
            # rather than letting the engine attempt to read an empty directory.
            if source_files is not None and len(source_files) == 0:
                logger.info("No source files found in partitioned path; skipping run.")
                self._write_empty_run_log("no_new_data")
                self._run_log_already_written = True
                return ValidationResult(self._empty_frame(), self._empty_frame(), self._empty_frame())
        else:
            # Force directory glob to calculate mtimes for incremental file reads
            # if a raw directory path was provided without an explicit glob.
            _is_table = str(path).startswith("table:") or (
                self.contract.source and self.contract.source.type in ("table", "delta", "iceberg")
            )
            if (
                self.contract.source
                and getattr(self.contract.source, "load_mode", "full") == "incremental"
                and not any(ch in str(path) for ch in ["*", "?", "["])
                and not _is_table
            ):
                source_files = self._expand_source_files(str(path).rstrip("/") + "/*")
                if not source_files:
                    source_files = self._expand_source_files(path)
            else:
                # For landing sources with a bare directory path (no glob),
                # auto-append /* so the file scanner discovers contents.
                _src_type = getattr(self.contract.source, "type", None) if self.contract.source else None
                _is_bare_dir = (
                    not any(ch in str(path) for ch in ["*", "?", "["])
                    and not _is_table
                    and _src_type not in ("delta", "iceberg")
                    and not self._is_uri_path(str(path))
                    and Path(path).is_dir()
                    and not (Path(path) / "_delta_log").exists()
                    and not ((Path(path) / "metadata").exists() and (Path(path) / "data").exists())
                )
                if _is_bare_dir:
                    source_files = self._expand_source_files(str(path).rstrip("/") + "/**/*")
                    if not source_files:
                        source_files = self._expand_source_files(str(path).rstrip("/") + "/*")
                else:
                    source_files = self._expand_source_files(path)
        load_mode = getattr(self.contract.source, "load_mode", "full") if self.contract.source else "full"
        if load_mode == "incremental" and source_files and not _is_reprocess:
            watermark = self._get_last_source_watermark()
            if watermark is not None:
                source_files = [f for f in source_files if f.get("mtime", 0) > watermark]
            if not source_files:
                logger.info("No new files detected for incremental load; skipping run.")
                self._write_empty_run_log("no_new_data")
                self._run_log_already_written = True
                return ValidationResult(self._empty_frame(), self._empty_frame(), self._empty_frame())

        self._source_files = source_files or []
        self._source_max_mtime = None
        if source_files:
            self._source_max_mtime = max(f.get("mtime", 0) for f in source_files)

        df = None
        file_paths = [f["path"] for f in source_files] if source_files else None

        with self.trace_step("Load Source", path=str(path)):
            if self.engine_name in ("polars", "duckdb"):
                import polars as pl

                if df is None:
                    source_fmt = (
                        (
                            getattr(self.contract.source, "format", None)
                            if getattr(self.contract, "source", None)
                            else None
                        )
                        or (
                            getattr(self.contract.materialization, "format", None)
                            if getattr(self.contract, "materialization", None)
                            else None
                        )
                        or (
                            getattr(self.contract.server, "format", None)
                            if getattr(self.contract, "server", None)
                            else None
                        )
                        or ""
                    ).lower()

                    # Use scan_* (lazy) by default for formats that support it.
                    # This enables predicate/projection pushdown and query
                    # optimisation.  We .collect() before returning so the
                    # user always receives a regular DataFrame.
                    _scannable = source_fmt in ("parquet", "csv", "ndjson") or path.endswith(
                        (".parquet", ".csv", ".ndjson", ".jsonl")
                    )

                    if file_paths:
                        # Build storage_options for cloud paths (Polars uses
                        # its own object-store backend, not fsspec)
                        _pl_sopts = self._get_cloud_storage_options(path) if self._is_uri_path(path) else None
                        _scan_kw: dict = {}
                        if _pl_sopts:
                            _scan_kw["storage_options"] = _pl_sopts

                        if _scannable:
                            # Lazy: scan each file and concat lazily.
                            # Use diagonal_relaxed so type-inference differences
                            # across CSV files (e.g. Int64 vs Float64) are
                            # coerced to a common supertype automatically.
                            _concat_how = "diagonal_relaxed"
                            # On Windows, Polars' internal path resolver can
                            # mangle drive-letter paths (C:) even with
                            # glob=False.  For local file paths (non-URI),
                            # use eager read to bypass path handling entirely.
                            _is_local = not self._is_uri_path(path)
                            _scan_kw["glob"] = False
                            # Tag each file with its source path so
                            # _lakelogic_source shows actual filenames
                            # rather than just the parent directory.
                            _tag_source = len(file_paths) > 1

                            _read_opts = {"storage_options": _pl_sopts} if _pl_sopts else {}
                            if source_fmt == "parquet" or path.endswith(".parquet"):
                                if _tag_source:
                                    df = pl.concat(  # pragma: no cover
                                        [
                                            pl.read_parquet(p, **_read_opts).with_columns(
                                                pl.lit(p).alias("_source_file")
                                            )
                                            for p in file_paths
                                        ],
                                        how=_concat_how,
                                    )
                                else:
                                    lf = pl.scan_parquet(file_paths[0], **_scan_kw)
                                    df = lf.collect()
                            elif source_fmt == "ndjson" or path.endswith((".ndjson", ".jsonl")):
                                if _tag_source:
                                    df = pl.concat(  # pragma: no cover
                                        [
                                            pl.read_ndjson(p, **_read_opts).with_columns(
                                                pl.lit(p).alias("_source_file")
                                            )
                                            for p in file_paths
                                        ],
                                        how=_concat_how,
                                    )
                                else:
                                    _ndjson_kw = _scan_kw.copy()
                                    _ndjson_kw.pop("glob", None)
                                    lf = pl.scan_ndjson(file_paths[0], **_ndjson_kw)
                                    df = lf.collect()
                            else:  # CSV
                                if _is_local:
                                    # Eager read — bypasses Polars' internal
                                    # path canonicalisation which breaks on
                                    # Windows drive-letter paths.
                                    if _tag_source:
                                        df = pl.concat(  # pragma: no cover
                                            [
                                                pl.read_csv(p).with_columns(pl.lit(p).alias("_source_file"))
                                                for p in file_paths
                                            ],
                                            how=_concat_how,
                                        )
                                    else:
                                        df = pl.read_csv(file_paths[0])
                                else:
                                    if _tag_source:
                                        df = pl.concat(  # pragma: no cover
                                            [
                                                pl.read_csv(p, **_read_opts).with_columns(
                                                    pl.lit(p).alias("_source_file")
                                                )
                                                for p in file_paths
                                            ],
                                            how=_concat_how,
                                        )
                                    else:
                                        lf = pl.scan_csv(file_paths[0], **_scan_kw)
                                        df = lf.collect()
                        else:
                            # Eager fallback — JSON, XML, Excel (no scan_* support)
                            import json as _json

                            def _read_json_flat(filepath: str) -> "pl.DataFrame":
                                """Read a .json file and cast any nested Struct/List columns
                                to JSON strings so they match a flat contract schema.
                                Supports cloud URIs (abfss://, s3://, gs://) via fsspec."""
                                import polars as pl

                                if self._is_uri_path(filepath):
                                    import fsspec

                                    _sopts = self._get_cloud_storage_options(filepath)
                                    with fsspec.open(filepath, "r", **_sopts) as f:
                                        text = f.read()
                                    try:
                                        raw = _json.loads(text)
                                    except _json.JSONDecodeError:
                                        # NDJSON: one JSON object per line
                                        raw = [_json.loads(line) for line in text.strip().splitlines() if line.strip()]
                                else:
                                    raw = _json.loads(Path(filepath).read_text(encoding="utf-8"))
                                rows = [raw] if isinstance(raw, dict) else (raw if isinstance(raw, list) else [])
                                # Normalise nested objects to JSON strings
                                flat = [
                                    {
                                        k: (_json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v)
                                        for k, v in row.items()
                                    }
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
                                df = pl.concat(frames, how="diagonal_relaxed")  # coerce type conflicts across files

                    else:
                        # No glob expansion — single path or table directory
                        # ── Delta table directory ──────────────────────────
                        _is_delta_dir = False
                        source_fmt = (
                            (
                                getattr(self.contract.source, "format", None)
                                if getattr(self.contract, "source", None)
                                else None
                            )
                            or (
                                getattr(self.contract.materialization, "format", None)
                                if getattr(self.contract, "materialization", None)
                                else None
                            )
                            or (
                                getattr(self.contract.server, "format", None)
                                if getattr(self.contract, "server", None)
                                else None
                            )
                            or ""
                        ).lower()

                        if not self._is_uri_path(path):
                            p_obj = Path(path)
                            _is_delta_dir = p_obj.is_dir() and (p_obj / "_delta_log").exists()

                        # Explicit check for delta format or table prefix
                        if not _is_delta_dir:
                            if path.startswith("table:") or source_fmt == "delta":
                                _is_delta_dir = True
                            elif getattr(getattr(self.contract, "source", None), "type", None) == "table":
                                _is_delta_dir = True

                        if _is_delta_dir or source_fmt.lower() == "delta":
                            # ── Resolve incremental watermark BEFORE reading ──
                            # Query max(watermark_col) from the target (silver)
                            # table once, then pass as a filter to the Delta read
                            # so discarded rows are never loaded into memory.
                            _wm_filter_expr = None
                            _src_cfg = getattr(self.contract, "source", None)
                            if getattr(_src_cfg, "load_mode", None) in ("incremental", "cdc") and not os.environ.get(
                                "LAKELOGIC_SKIP_INCREMENTAL_CHECK"
                            ):
                                _wm_strategy = getattr(_src_cfg, "watermark_strategy", None)
                                _wm_field = getattr(_src_cfg, "watermark_field", None)
                                # Default to the configured lineage timestamp when no explicit watermark is set.
                                if not _wm_field:
                                    _lin_cfg = getattr(self.contract, "lineage", None)
                                    _wm_base = (
                                        getattr(_lin_cfg, "timestamp_column_name", None) or "_lakelogic_processed_at"
                                        if _lin_cfg
                                        else "_lakelogic_processed_at"
                                    )
                                    _wm_field = getattr(_src_cfg, "cdc_timestamp_field", None) or _wm_base

                                _mat = getattr(self.contract, "materialization", None)
                                _tgt = getattr(_mat, "target_path", None) if _mat else None
                                if _wm_field:
                                    _src_col, _tgt_col = self._resolve_watermark_columns(_wm_field)
                                    _max_wm = None

                                    if _wm_strategy == "pipeline_log":
                                        _max_wm_val = self._get_last_source_watermark()
                                        if _max_wm_val is not None:
                                            try:
                                                from datetime import datetime, timezone

                                                _max_wm_dt = datetime.fromtimestamp(float(_max_wm_val), tz=timezone.utc)
                                                # Use ISO string when the source column is String-typed
                                                # (Polars can't compare datetime to string directly).
                                                _max_wm = _max_wm_dt.isoformat()
                                                logger.debug(
                                                    f"pipeline_log watermark: float={_max_wm_val} -> iso={_max_wm}"
                                                )
                                            except Exception as e:
                                                logger.debug(f"Failed to parse pipeline_log watermark: {e}")
                                                _max_wm = None
                                    elif _tgt:
                                        # Watermark checking on the target table (Delta)
                                        # Skip Path() checks for URIs to avoid OSError on Windows
                                        _tgt_is_delta_dir = False
                                        if not self._is_uri_path(_tgt):
                                            _tgt_delta = Path(_tgt)
                                            _tgt_is_delta_dir = (_tgt_delta / "_delta_log").exists()
                                        else:
                                            _tgt_is_delta_dir = True  # Assume URI target is accessible if it's delta

                                        if _tgt_is_delta_dir:
                                            try:
                                                # Use DeltaTable → Arrow → Polars to avoid
                                                # deltalake/polars Schema iteration bug
                                                from deltalake import DeltaTable as _DT

                                                _dt_opts = (
                                                    self._get_cloud_storage_options(str(_tgt))
                                                    if self._is_uri_path(str(_tgt))
                                                    else None
                                                )
                                                _dt_tgt = _DT(str(_tgt), storage_options=_dt_opts)
                                                _tdf = pl.from_arrow(_dt_tgt.to_pyarrow_table())
                                                if _tgt_col in _tdf.columns:
                                                    _max_wm = _tdf.select(pl.col(_tgt_col).max()).item()
                                            except Exception as _wm_err:
                                                logger.debug(f"Watermark read failed (full load): {_wm_err}")
                                    if _max_wm is not None:
                                        logger.info(
                                            f"Incremental load: filtering {_src_col} > {_max_wm!r} "
                                            f"(strategy: {_wm_strategy or 'max_target'})"
                                        )
                                        _wm_filter_expr = pl.col(_src_col) > _max_wm
                                    else:
                                        logger.info("Incremental load: first run or target empty — loading all rows.")

                            # Use DeltaTable → Arrow → Polars to avoid
                            # deltalake/polars Schema iteration bug and
                            # pass cloud storage_options for ADLS auth.
                            from deltalake import DeltaTable as _DT

                            _dt_opts = self._get_cloud_storage_options(path) if self._is_uri_path(path) else None
                            _dt_src = _DT(path, storage_options=_dt_opts)
                            df = pl.from_arrow(_dt_src.to_pyarrow_table())

                            # Apply watermark filter immediately (before flatten/rename)
                            if _wm_filter_expr is not None:
                                if _src_col in df.columns:
                                    df = df.filter(_wm_filter_expr)
                                    if df.is_empty():
                                        logger.info(
                                            "Incremental load: no new rows since last run — "
                                            "processing empty frame to preserve contract schema."
                                        )
                                    # Continue (don't return early): the empty-but-schemed df
                                    # flows through transforms so result.good has correct dtypes.
                                else:
                                    logger.warning(
                                        f"Watermark column '{_src_col}' not found in source — falling back to full load"
                                    )

                            # Capture actual max value from source for pipeline log
                            if self._source_max_mtime is None and not df.is_empty():
                                _src_wm_field = getattr(_src_cfg, "watermark_field", None)
                                if not _src_wm_field:
                                    _lin_cfg = getattr(self.contract, "lineage", None)
                                    _wm_base = (
                                        getattr(_lin_cfg, "timestamp_column_name", None) or "_lakelogic_processed_at"
                                        if _lin_cfg
                                        else "_lakelogic_processed_at"
                                    )
                                    _src_wm_field = getattr(_src_cfg, "cdc_timestamp_field", None) or _wm_base

                                logger.debug(
                                    f"Watermark capture: resolved field='{_src_wm_field}', "
                                    f"available={_src_wm_field in df.columns if _src_wm_field else False}"
                                )
                                if _src_wm_field and _src_wm_field in df.columns:
                                    try:
                                        _val = df.select(pl.col(_src_wm_field).max()).item()
                                        if _val is not None:
                                            from datetime import datetime

                                            if isinstance(_val, datetime):
                                                self._source_max_mtime = _val.timestamp()
                                            elif isinstance(_val, str):
                                                self._source_max_mtime = datetime.fromisoformat(
                                                    _val.replace("Z", "+00:00")
                                                ).timestamp()
                                            else:
                                                self._source_max_mtime = float(_val)
                                            logger.debug(
                                                f"Captured max_source_mtime={self._source_max_mtime} "
                                                f"from '{_src_wm_field}'"
                                            )
                                    except Exception as _cap_err:
                                        logger.debug(f"Watermark capture failed: {_cap_err}")

                        elif _scannable:
                            fmt = (
                                self.contract.source.format.lower()
                                if getattr(self.contract.source, "format", None)
                                else ""
                            )
                            # Polars needs explicit glob if it's a directory
                            _supported_globs = ["csv", "parquet", "json", "jsonl", "ndjson"]
                            if (
                                fmt in _supported_globs
                                and not any(chr in path for chr in ["*", "?", "["])
                                and not path.endswith(f".{fmt}")
                            ):
                                path = f"{path.rstrip('/')}/**/*.{fmt}"

                            # On Windows, Polars' internal glob misinterprets
                            # drive-letter colons (C:) as URI schemes, producing
                            # malformed paths like "*.csv://C:/.../data.csv".
                            # Pre-expand globs via Python's glob module and pass
                            # resolved file paths instead.
                            _needs_pre_glob = not self._is_uri_path(path) and any(ch in path for ch in ["*", "?", "["])
                            if _needs_pre_glob:
                                from glob import glob as _glob

                                _resolved = sorted(_glob(path, recursive=True))
                                _resolved = [f for f in _resolved if Path(f).is_file()]
                                if _resolved:
                                    if path.endswith(".parquet") or fmt == "parquet":
                                        lf = pl.scan_parquet(_resolved if len(_resolved) > 1 else _resolved[0])
                                        df = lf.collect()
                                    elif path.endswith((".ndjson", ".jsonl")):
                                        lf = pl.concat(
                                            [pl.scan_ndjson(p) for p in _resolved],
                                            how="diagonal_relaxed",
                                        )
                                        df = lf.collect()
                                    else:
                                        # CSV — use eager read for local paths
                                        # to bypass Polars' path canonicalisation
                                        # that breaks on Windows drive letters.
                                        if not self._is_uri_path(path):
                                            df = pl.concat(
                                                [pl.read_csv(p) for p in _resolved],
                                                how="diagonal_relaxed",
                                            )
                                        else:
                                            lf = pl.concat(
                                                [pl.scan_csv(p, glob=False) for p in _resolved],
                                                how="diagonal_relaxed",
                                            )
                                            df = lf.collect()
                                else:
                                    logger.warning(f"Glob pattern matched 0 files: {path}")
                            else:
                                try:
                                    if path.endswith(".parquet") or fmt == "parquet":
                                        lf = pl.scan_parquet(path)
                                        df = lf.collect()
                                    elif path.endswith((".ndjson", ".jsonl")):
                                        lf = pl.scan_ndjson(path)
                                        df = lf.collect()
                                    else:
                                        if not self._is_uri_path(path):
                                            df = pl.read_csv(path)
                                        else:
                                            lf = pl.scan_csv(path, glob=False)
                                            df = lf.collect()
                                except Exception as e:
                                    if (
                                        "404 Not Found" in str(e)
                                        or "not found" in str(e).lower()
                                        or "no matching files" in str(e).lower()
                                    ):
                                        logger.info(
                                            f"Source path not found or matched 0 files (Polars/DuckDB): {path}. "
                                            "Returning empty dataframe."
                                        )
                                        self._write_empty_run_log("no_new_data")
                                        self._run_log_already_written = True
                                        return ValidationResult(
                                            self._empty_frame(), self._empty_frame(), self._empty_frame()
                                        )
                                    else:
                                        raise e
                        else:
                            import json as _json

                            if path.endswith(".json"):
                                if self._is_uri_path(path):
                                    # Cloud URI — read via fsspec (pl.read_json doesn't support cloud URIs)
                                    import fsspec

                                    _sopts = self._get_cloud_storage_options(path)
                                    with fsspec.open(path, "r", **_sopts) as f:
                                        text = f.read()
                                    try:
                                        raw = _json.loads(text)
                                    except _json.JSONDecodeError:
                                        # NDJSON: one JSON object per line
                                        raw = [_json.loads(line) for line in text.strip().splitlines() if line.strip()]
                                    rows = [raw] if isinstance(raw, dict) else (raw if isinstance(raw, list) else [])
                                    flat = [
                                        {
                                            k: (
                                                _json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
                                            )
                                            for k, v in r.items()
                                        }
                                        for r in rows
                                    ]
                                    df = pl.DataFrame(flat)
                                else:
                                    raw = _json.loads(Path(path).read_text(encoding="utf-8"))
                                    rows = [raw] if isinstance(raw, dict) else (raw if isinstance(raw, list) else [])
                                    flat = [
                                        {
                                            k: (
                                                _json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else v
                                            )
                                            for k, v in r.items()
                                        }
                                        for r in rows
                                    ]
                                    df = pl.DataFrame(flat)
                            elif path.endswith(".xml"):
                                df = pl.read_xml(path)
                            elif path.endswith((".xlsx", ".xls")):
                                df = pl.read_excel(path)
                            else:
                                df = pl.read_csv(path)

            elif self.engine_name == "spark":  # pragma: no cover
                from pyspark.sql import SparkSession

                spark = SparkSession.builder.getOrCreate()
                # ── Format detection ──────────────────────────────────────────
                # Check source.format first, then materialization.format,
                # then server.format; fall back to file extension or parquet.
                fmt = (
                    (getattr(self.contract.source, "format", None) if getattr(self.contract, "source", None) else None)
                    or (
                        getattr(self.contract.materialization, "format", None)
                        if getattr(self.contract, "materialization", None)
                        else None
                    )
                    or (
                        getattr(self.contract.server, "format", None)
                        if getattr(self.contract, "server", None)
                        else None
                    )
                    or (
                        "json"
                        if path.endswith(".json")
                        else "csv"
                        if path.endswith(".csv")
                        else "excel"
                        if path.endswith((".xlsx", ".xls"))
                        else "parquet"
                    )
                ).lower()

                # Pre-calculate source overrides for incremental loads
                _src_overrides = {}
                _src_cfg = getattr(self.contract, "source", None)
                if (
                    _src_cfg
                    and getattr(_src_cfg, "load_mode", None) in ("incremental", "cdc")
                    and not os.environ.get("LAKELOGIC_SKIP_INCREMENTAL_CHECK")
                ):
                    _wm_strategy = getattr(_src_cfg, "watermark_strategy", None)
                    _info = getattr(self.contract, "info", None)
                    _meta = getattr(self.contract, "metadata", None) or {}
                    _load_mode = getattr(_src_cfg, "load_mode", None)

                    # CDC table sources should default to pipeline_log, not max_target.
                    # max_target queries the SILVER table's timestamps which differ from
                    # the BRONZE timestamps we need to compare against.
                    # pipeline_log uses max_source_mtime from _run_logs — the correct
                    # upstream boundary.
                    if _load_mode == "cdc" and _wm_strategy is None:
                        _wm_strategy = "pipeline_log"
                        _src_overrides["watermark_strategy"] = "pipeline_log"

                    # Resolve dataset identically to how report logging resolves it
                    _dataset = None
                    if _mat := getattr(self.contract, "materialization", None):
                        _tp = getattr(_mat, "target_path", "") or getattr(_mat, "path", "") or ""
                        if str(_tp).startswith("table:"):
                            _tbl_full = str(_tp)[len("table:") :]
                            _dataset = _tbl_full.split(".")[-1] if "." in _tbl_full else _tbl_full

                    if not _dataset:
                        _dataset = (
                            _meta.get("dataset")
                            or getattr(self.contract, "dataset", None)
                            or (getattr(_info, "title", None) if _info else None)
                        )

                    _data_layer = _meta.get("data_layer") or (getattr(_info, "target_layer", None) if _info else None)
                    _domain = _meta.get("domain") or (getattr(_info, "domain", None) if _info else None)
                    _system = _meta.get("system") or (getattr(_info, "system", None) if _info else None)
                    _log_table = _meta.get("run_log_table")

                    if _dataset:
                        _src_overrides["dataset"] = _dataset
                    if _data_layer:
                        _src_overrides["data_layer"] = _data_layer
                    if _domain:
                        _src_overrides["domain"] = _domain
                    if _system:
                        _src_overrides["system"] = _system
                    if _log_table:
                        _src_overrides["pipeline_log_table"] = _log_table

                    if _wm_strategy in (None, "max_target", "delta_version"):
                        if getattr(_src_cfg, "target_path", None) is None:
                            _mat = getattr(self.contract, "materialization", None)
                            _target = getattr(_mat, "target", None) if _mat else None
                            if not _target:
                                _tbl = getattr(_info, "table_name", None) if _info else None
                                if _tbl:
                                    _src_path = getattr(_src_cfg, "path", "") or ""
                                    _catalog = _src_path.split(".")[0] if "." in _src_path else ""
                                    _target = f"{_catalog}.{_tbl}" if _catalog else _tbl
                            if _target:
                                _src_overrides["target_path"] = (
                                    _target if _target.startswith("table:") else f"table:{_target}"
                                )

                if df is None:
                    if path.startswith("table:"):
                        df = spark.table(path[6:])

                        # ── Incremental watermark for Spark table sources ─────
                        if _src_overrides or getattr(_src_cfg, "load_mode", None) in ("incremental", "cdc"):
                            from lakelogic.core.incremental import IncrementalBoundary

                            try:
                                if _src_overrides:
                                    boundary = IncrementalBoundary.from_source_config(_src_cfg, **_src_overrides)
                                else:
                                    boundary = IncrementalBoundary.from_source_config(_src_cfg)

                                if boundary.strategy == "delta_version":
                                    fv = boundary.metadata.get("from_version")
                                    tv = boundary.metadata.get("to_version")
                                    skip = boundary.metadata.get("skip_sync", False)

                                    # Store metadata so materialization can write back the version
                                    self._incremental_metadata = boundary.metadata
                                    table_name = path[6:] if path.startswith("table:") else path

                                    if skip:
                                        logger.info(
                                            f"Incremental load (Delta Versions): "
                                            f"Source version is unchanged ({tv}). Skipping read."
                                        )
                                        df = spark.table(table_name).filter("1 = 0")
                                    else:
                                        logger.info(f"Incremental load (Delta Versions): {fv} -> {tv}")
                                        # Reload with version options
                                        df = (
                                            spark.read.format("delta")
                                            .option("readChangeFeed", "true")
                                            .option("startingVersion", fv)
                                            .option("endingVersion", tv)
                                            .table(table_name)
                                        )
                                else:
                                    # Standard watermark filter
                                    _wm_field = getattr(_src_cfg, "watermark_field", None)
                                    # Default to the lineage timestamp when no watermark is configured.
                                    # For CDC mode, prefer cdc_timestamp_field as the watermark
                                    # since it's the user's declared timestamp for change tracking.
                                    # Bronze records are stamped with the lineage timestamp,
                                    # making it a natural high-water mark for silver reads.
                                    if not _wm_field:
                                        _lin_cfg = getattr(self.contract, "lineage", None)
                                        _wm_field = (
                                            getattr(_lin_cfg, "timestamp_column_name", None)
                                            or "_lakelogic_processed_at"
                                            if _lin_cfg
                                            else "_lakelogic_processed_at"
                                        )
                                        logger.debug(f"No watermark_field configured — defaulting to '{_wm_field}'")
                                    df = df.filter(boundary.spark_filter(_wm_field))
                                    logger.info(
                                        f"Incremental load (Spark): applied "
                                        f"{boundary.strategy} boundary on '{_wm_field}'"
                                    )
                                    # Compute Max Watermark Value via PySpark aggregation if pipeline_log strategy
                                    if boundary.strategy == "pipeline_log":
                                        try:
                                            from pyspark.sql import functions as F

                                            _max_row = df.select(
                                                F.max(_wm_field).cast("string").alias("max_wm")
                                            ).collect()[0]
                                            _max_val = _max_row["max_wm"]
                                            if _max_val:
                                                boundary.metadata["max_watermark_value"] = str(_max_val)
                                                logger.debug(
                                                    f"Computed max_watermark_value='{_max_val}' for {_wm_field}"
                                                )
                                        except Exception as wm_exc:
                                            logger.debug(f"Failed to compute max_watermark_value: {wm_exc}")

                                self._incremental_metadata = dict(boundary.metadata)
                                self._incremental_metadata["strategy"] = boundary.strategy

                                if boundary.strategy == "delta_version":
                                    _tv = boundary.metadata.get("to_version")
                                    if _tv is not None:
                                        self._incremental_metadata["max_watermark_value"] = str(_tv)

                                # ── Capture max source timestamp for table sources ──
                                # For file sources, _source_max_mtime is set from file
                                # mtimes. For table sources we derive it from the
                                # upstream _lakelogic_processed_at so the run_log entry
                                # reflects the latest consumed source record.
                                if self._source_max_mtime is None:
                                    try:
                                        from pyspark.sql import functions as F

                                        _ts_col = "_lakelogic_processed_at"
                                        if _ts_col in df.columns:
                                            _max_ts = df.select(
                                                F.max(F.unix_timestamp(F.col(_ts_col))).alias("mx")
                                            ).collect()[0]["mx"]
                                            if _max_ts is not None:
                                                self._source_max_mtime = float(_max_ts)
                                                logger.debug(
                                                    f"Captured table source max mtime: {self._source_max_mtime}"
                                                )
                                    except Exception as _mtime_err:
                                        logger.debug(f"Failed to capture table source mtime: {_mtime_err}")

                            except Exception as _wm_err:
                                logger.debug(
                                    f"Incremental boundary resolution failed (falling back to full): {_wm_err}"
                                )

                    else:
                        # File path or explicit format
                        _src_cfg = getattr(self.contract, "source", None)
                        if (
                            _src_cfg
                            and getattr(_src_cfg, "load_mode", None) in ("incremental", "cdc")
                            and fmt == "delta"
                        ):
                            from lakelogic.core.incremental import IncrementalBoundary

                            try:
                                if _src_overrides:
                                    boundary = IncrementalBoundary.from_source_config(_src_cfg, **_src_overrides)
                                else:
                                    boundary = IncrementalBoundary.from_source_config(_src_cfg)
                                if boundary.strategy == "delta_version":
                                    fv = boundary.metadata.get("from_version")
                                    tv = boundary.metadata.get("to_version")
                                    skip = boundary.metadata.get("skip_sync", False)

                                    if skip:
                                        logger.info(
                                            f"Incremental load (Delta Versions): "
                                            f"Source version is unchanged ({tv}). Skipping read."
                                        )
                                        df = spark.read.format("delta").load(path).filter("1 = 0")
                                    else:
                                        logger.info(f"Incremental load (Delta Versions): {fv} -> {tv}")
                                        df = (
                                            spark.read.format("delta")
                                            .option("readChangeFeed", "true")
                                            .option("startingVersion", fv)
                                            .option("endingVersion", tv)
                                            .load(path)
                                        )

                                    self._incremental_metadata = dict(boundary.metadata)
                                    self._incremental_metadata["strategy"] = boundary.strategy
                                    if tv is not None:
                                        self._incremental_metadata["max_watermark_value"] = str(tv)
                                    # If using CDF, we don't need a normal load
                                    return self.run(df, source_path=path, reset_trace=False)
                            except Exception as _cdf_err:
                                logger.debug(f"Delta CDF read failed, falling back to standard read: {_cdf_err}")

                        reader = spark.read.format(fmt)
                        if fmt == "csv":
                            reader = reader.option("header", "true")
                        elif fmt == "json":
                            reader = reader.option("multiLine", "true")
                        # When reading a directory (no explicit file list), enable
                        # recursive scanning so Spark finds files in partition
                        # subdirectories (e.g. y_2026/m_03/d_21/data.csv).
                        if not file_paths:
                            reader = reader.option("recursiveFileLookup", "true")

                        # ── Contract-driven schema ────────────────────────────
                        # Build a Spark StructType from the contract's model
                        # fields so CSV/JSON reads don't need to infer schema
                        # (which fails on empty dirs or Volumes FUSE paths).
                        _contract_type = type(self.contract).__name__
                        logger.debug(
                            f"Spark read: fmt={fmt}, "
                            f"file_paths={len(file_paths) if file_paths else 'None'}, "
                            f"contract_type={_contract_type}"
                        )

                        _model = getattr(self.contract, "model", None)
                        _fields_list = getattr(_model, "fields", None) if _model else None
                        if not _fields_list and isinstance(self.contract, dict):
                            _fields_list = self.contract.get("model", {}).get("fields", [])

                        logger.debug(
                            f"Spark schema: model={_model is not None}, "
                            f"fields_list={len(_fields_list) if _fields_list else 'None'}"
                        )

                        if _fields_list and str(fmt).lower() not in ("delta", "iceberg", "hudi"):
                            from pyspark.sql.types import (
                                StructType,
                                StructField,
                                StringType,
                                LongType,
                                IntegerType,
                                DoubleType,
                                FloatType,
                                BooleanType,
                                TimestampType,
                                DateType,
                            )

                            _type_map = {
                                "string": StringType(),
                                "long": LongType(),
                                "bigint": LongType(),
                                "integer": IntegerType(),
                                "int": IntegerType(),
                                "double": DoubleType(),
                                "float": FloatType(),
                                "boolean": BooleanType(),
                                "bool": BooleanType(),
                                "timestamp": TimestampType(),
                                "date": DateType(),
                            }
                            spark_fields = []
                            for f in _fields_list:
                                fname = f.get("name") if isinstance(f, dict) else getattr(f, "name", None)
                                ftype = f.get("type", "string") if isinstance(f, dict) else getattr(f, "type", "string")
                                if isinstance(f, dict):
                                    freq = f.get("required", False)
                                else:
                                    freq = getattr(f, "required", False)
                                nullable = not freq
                                spark_type = _type_map.get((ftype or "string").lower(), StringType())
                                if fname:
                                    spark_fields.append(StructField(fname, spark_type, nullable))
                            if spark_fields:
                                schema = StructType(spark_fields)
                                reader = reader.schema(schema)
                                logger.info(f"✅ Applied contract schema ({len(spark_fields)} fields) to Spark reader")
                            else:
                                logger.warning("⚠ Contract model.fields found but produced 0 Spark fields")
                        elif _fields_list:
                            logger.info(
                                f"✅ Contract schema exists but bypassed for native '{fmt}' format schema inference."
                            )
                        else:
                            logger.warning("⚠ No contract model.fields found — Spark will infer schema")

                        _load_target = file_paths if file_paths else path
                        if isinstance(_load_target, list):
                            logger.info(f"Spark loading {len(_load_target)} file(s)")
                        else:
                            logger.info(f"Spark loading: {_load_target}")
                        df = reader.load(_load_target)

                        # Capture per-row file path from Spark's hidden
                        # _metadata column before transformations strip it.
                        # This enables per-row source traceability in
                        # _lakelogic_source (vs. a single directory path).
                        _source_captured = False
                        try:
                            from pyspark.sql import functions as F

                            # Strategy 1: _metadata.file_path (full path — preferred)
                            df = df.select("*", F.col("_metadata.file_path").alias("_source_file"))
                            _source_captured = True
                        except Exception:
                            try:
                                from pyspark.sql import functions as F

                                # Strategy 2: _metadata.file_name (just filename)
                                df = df.select("*", F.col("_metadata.file_name").alias("_source_file"))
                                _source_captured = True
                            except Exception:
                                try:
                                    from pyspark.sql import functions as F

                                    # Strategy 3: input_file_name() (works for non-UC file reads)
                                    ifn = F.input_file_name()
                                    df = df.withColumn(
                                        "_source_file", F.when(ifn != F.lit(""), ifn).otherwise(F.lit(path))
                                    )
                                    _source_captured = True
                                except Exception as exc:
                                    logger.debug(f"input_file_name() fallback failed: {exc}")
                        if not _source_captured:
                            logger.debug(
                                f"Could not capture per-row source file path"
                                f" for {path} — lineage will use directory path"
                            )

            elif self.engine_name in ["snowflake", "bigquery"]:
                table_name = path[6:] if path.startswith("table:") else path
                return self.run(table_name, source_path=table_name, reset_trace=False)

        if df is None:
            raise ValueError(f"Could not load data from {path} using engine {self.engine_name}")

        # ── Target Reprocessing filter ─────────────────────────────────────────
        # Apply strict targeted reprocessing (if supplied). We apply this here
        # so Polars/Spark lazy logic can push the predicate all the way down.
        # Uses resolved_reprocess_column which may come from the contract's
        # materialization.reprocess_column or the global pipeline parameter.
        if resolved_reprocess_column and reprocess_values:
            if self.engine_name == "polars":
                import polars as pl

                # Cast the array values to Utf8 to safely match
                string_vals = [str(x) for x in reprocess_values]
                df = df.filter(pl.col(resolved_reprocess_column).cast(pl.Utf8).is_in(string_vals))
            elif self.engine_name == "spark":  # pragma: no cover
                from pyspark.sql.functions import col

                string_vals = [str(x) for x in reprocess_values]
                df = df.filter(col(resolved_reprocess_column).cast("string").isin(string_vals))
            elif self.engine_name == "pandas":
                # Pandas is eager, but we filter to save ram on transform
                string_vals = [str(x) for x in reprocess_values]
                df = df[df[resolved_reprocess_column].astype(str).isin(string_vals)]
            elif self.engine_name == "duckdb":
                import duckdb

                # Need to run a query to enforce it
                string_vals = ", ".join([f"'{str(x).replace(chr(39), chr(39) * 2)}'" for x in reprocess_values])
                query = f"SELECT * FROM df WHERE {resolved_reprocess_column} IN ({string_vals})"
                df = duckdb.sql(query).fetchdf()

            logger.info(f"Targeted reprocessing filter pushdown applied on {resolved_reprocess_column}")

        # ── JSON-string flattening (source.flatten_nested) ────────────────────
        # When a bronze table stores nested objects as JSON strings we need to
        # expand them into flat parent_child columns so they match the silver
        # schema before validation runs.
        flatten_nested = getattr(getattr(self.contract, "source", None), "flatten_nested", False)
        if flatten_nested:
            df = self._flatten_json_df(df, flatten_nested)

        # ── Date-range reprocessing filter ────────────────────────────────────
        # When reprocess_from / reprocess_to are set, filter the loaded DataFrame
        # to only include rows within the requested date range.  The date column
        # is resolved from materialization.reprocess_date_column (explicit) or
        # the first partition_by column (implicit fallback).
        if _is_reprocess:
            df = self._apply_reprocess_date_filter(df, reprocess_from, reprocess_to)

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

        result = self.run(df, source_path=path, reset_trace=False)

        # ── Post-ingestion cleanup (source-level) ────────────────────────
        # When source.post_ingestion.action is set, clean up landing files
        # after a successful run.  This mirrors PipelineRunner's server-level
        # cleanup but works for standalone DataProcessor.run_source() calls.
        _src_cfg = self.contract.source if self.contract.source else None
        _pi = getattr(_src_cfg, "post_ingestion", None) if _src_cfg else None
        if _pi and getattr(_pi, "action", "retain") != "retain":
            _action = _pi.action
            _source_path = str(path)
            _archive_path = getattr(_pi, "archive_path", None)
            _blocking = getattr(_pi, "cleanup_is_blocking", False)
            try:
                self._post_ingestion_cleanup(_source_path, _action, archive_path=_archive_path)
            except Exception as _cleanup_exc:
                if _blocking:
                    raise RuntimeError(f"Post-ingestion cleanup ({_action}) failed: {_cleanup_exc}") from _cleanup_exc
                else:
                    logger.warning(
                        f"Post-ingestion cleanup ({_action}) failed: {_cleanup_exc}. "
                        f"Pipeline continues (cleanup_is_blocking=false)."
                    )

        return result

    def _post_ingestion_cleanup(self, source_path: str, action: str, *, archive_path: Optional[str] = None) -> None:
        """Clean up landing zone files after a successful run.

        Only cleans up files that were actually ingested in this run
        (tracked in self._source_files).  This prevents accidental
        deletion of archive subdirectories or other unrelated files.

        Supports local filesystem paths.  For cloud paths, this is
        handled by PipelineRunner which has access to fsspec/dbutils.
        """
        from pathlib import Path as _Path
        import shutil as _shutil

        # Use the tracked source files from this run, filtered to only
        # include direct children of the source directory.  This prevents
        # accidental deletion of archive subdirectories or other managed
        # directories within the landing zone.
        src = _Path(source_path).resolve()
        ingested_files = [
            _Path(f["path"])
            for f in (self._source_files or [])
            if "path" in f and _Path(f["path"]).exists() and _Path(f["path"]).resolve().parent == src
        ]

        if not ingested_files:
            logger.debug(f"Post-ingestion: no ingested files to clean up in {source_path}")
            return

        if action == "delete":
            for f in ingested_files:
                f.unlink()
            logger.info(f"Post-ingestion: deleted {len(ingested_files)} ingested file(s) in {source_path}")

        elif action == "archive":
            if not archive_path:
                raise ValueError(
                    "post_ingestion.action is 'archive' but no archive_path was provided. "
                    "Set archive_path in source.post_ingestion or use PipelineRunner "
                    "with storage.archive_path in _system.yaml."
                )
            src = _Path(source_path)
            dst = _Path(archive_path).resolve()
            dst.mkdir(parents=True, exist_ok=True)
            for f in ingested_files:
                # Preserve relative path structure in archive
                try:
                    rel = f.relative_to(src)
                except ValueError:
                    rel = _Path(f.name)
                target = dst / rel
                target.parent.mkdir(parents=True, exist_ok=True)
                _shutil.move(str(f), str(target))
            logger.info(
                f"Post-ingestion: archived {len(ingested_files)} ingested file(s) from {source_path} to {archive_path}"
            )

        else:
            logger.warning(f"Post-ingestion: unknown action '{action}', skipping cleanup")

    def _resolve_reprocess_date_column(self) -> str:
        """Resolve which column to use for date-range reprocessing.

        Resolution order:
          1. Runtime parameter ``self._reprocess_column``
          2. ``materialization.reprocess_date_column`` (explicit YAML config)
          3. First entry in ``materialization.partition_by``
          4. Raise ValueError with guidance
        """
        if getattr(self, "_reprocess_column", None):
            return self._reprocess_column

        mat = getattr(self.contract, "materialization", None)
        if mat:
            explicit = getattr(mat, "reprocess_date_column", None)
            if explicit:
                return explicit
            partition_by = getattr(mat, "partition_by", []) or []
            if partition_by:
                col = partition_by[0]
                logger.info(
                    f"reprocess_date_column not set — using first partition_by column '{col}' for date-range filtering"
                )
                return col
        raise ValueError(
            "Cannot apply date-range reprocessing: no reprocess_date_column "
            "configured in materialization and partition_by is empty.  Add "
            "one of:\n"
            "  materialization:\n"
            "    reprocess_date_column: event_date\n"
            "  # OR\n"
            "  materialization:\n"
            "    partition_by: [event_date]\n"
        )

    def _apply_reprocess_date_filter(self, df, reprocess_from, reprocess_to):
        """Filter *df* to rows within [reprocess_from, reprocess_to].

        Supports Polars, Pandas, and Spark DataFrames.  Dates are compared
        as strings (YYYY-MM-DD) for string columns, or cast to date for
        typed columns.
        """
        date_col = self._resolve_reprocess_date_column()

        # ── Polars ────────────────────────────────────────────────────────────
        try:
            import polars as pl

            if isinstance(df, pl.DataFrame):
                if date_col not in df.columns:
                    raise ValueError(
                        f"Reprocess date column '{date_col}' not found in DataFrame.  Available: {df.columns}"
                    )
                col_expr = pl.col(date_col)
                # Cast to string for comparison if column is Date/Datetime
                dtype = df.schema[date_col]
                if dtype in (pl.Date, pl.Datetime):
                    col_expr_cmp = col_expr.cast(pl.Utf8)
                else:
                    col_expr_cmp = col_expr
                pre_count = len(df)
                if reprocess_from:
                    df = df.filter(col_expr_cmp >= pl.lit(reprocess_from))
                if reprocess_to:
                    df = df.filter(
                        col_expr_cmp <= pl.lit(reprocess_to + "T23:59:59" if "T" not in reprocess_to else reprocess_to)
                    )
                post_count = len(df)
                logger.info(
                    f"Reprocess filter ({date_col}): {pre_count} → {post_count} rows "
                    f"[{reprocess_from or '*'} .. {reprocess_to or '*'}]"
                )
                return df
        except ImportError:
            pass

        # ── Spark ─────────────────────────────────────────────────────────────
        if hasattr(df, "sparkSession"):
            from pyspark.sql import functions as F

            pre_count = df.count()
            if reprocess_from:
                df = df.filter(F.col(date_col) >= F.lit(reprocess_from))
            if reprocess_to:
                _to_val = reprocess_to + "T23:59:59" if "T" not in reprocess_to else reprocess_to
                df = df.filter(F.col(date_col) <= F.lit(_to_val))
            post_count = df.count()
            logger.info(
                f"Reprocess filter ({date_col}): {pre_count} → {post_count} rows "
                f"[{reprocess_from or '*'} .. {reprocess_to or '*'}]"
            )
            return df

        logger.warning("Reprocess date filter: unsupported DataFrame type — skipping filter")
        return df

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
            if s[:1] not in ("{", "["):
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
            json_cols = [col for col in rows[0] if isinstance(rows[0].get(col), str) and _is_json_col(rows, col)]

        if not json_cols:
            return df  # nothing to flatten

        # ── Deserialise targeted columns ───────────────────────────────────
        rows = [
            {
                **{k: v for k, v in row.items() if k not in json_cols},
                **{c: _try_parse(row.get(c)) for c in json_cols},
            }
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
                            _json.dumps(child, ensure_ascii=False) if isinstance(child, (dict, list)) else child
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
                    col for col in rows[0] if isinstance(rows[0].get(col), str) and _is_json_col(rows, col)
                ]
                if json_cols_iter:
                    rows = [
                        {
                            **{k: v for k, v in row.items() if k not in json_cols_iter},
                            **{c: _try_parse(row.get(c)) for c in json_cols_iter},
                        }
                        for row in rows
                    ]

            to_explode = [
                col for col in list(rows[0].keys()) if any(isinstance(row.get(col), (dict, list)) for row in rows)
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

    def _resolve_watermark_columns(self, wm_field: str) -> tuple:
        """
        Resolve watermark column names for source filtering and target querying.

        The watermark_field in a contract can be specified as either:
          - the *source* column name (e.g. ``_lakelogic_processed_at``)
          - the *target/renamed* column name (e.g. ``_upstream_lakelogic_processed_at``)

        When ``lineage.preserve_upstream`` is configured, LakeLogic renames
        source lineage columns (e.g. ``_lakelogic_processed_at`` →
        ``_upstream_lakelogic_processed_at``).  The source DataFrame still has
        the original name whereas the target table has the renamed name.

        Returns:
            (source_col, target_col) — the column name to use for filtering
            the source DataFrame and the column name to query MAX() from the
            target table.
        """
        _lin = getattr(self.contract, "lineage", None)
        _pres = list(getattr(_lin, "preserve_upstream", []) or []) if _lin else []
        _pfx = getattr(_lin, "upstream_prefix", "_upstream") or "_upstream" if _lin else "_upstream"

        def _to_renamed(col: str) -> str:
            """Compute the preserve_upstream renamed name for a source column."""
            if col.startswith("_lakelogic_"):
                return f"{_pfx}{col}"  # _upstream_lakelogic_processed_at
            return f"{_pfx}_{col.lstrip('_')}"

        def _to_original(renamed: str) -> str:
            """Reverse a preserve_upstream rename back to the source column name."""
            for src in _pres:
                if _to_renamed(src) == renamed:
                    return src
            return renamed  # not a renamed column — return as-is

        # Case 1: wm_field is a source column that will be renamed
        if wm_field in _pres:
            return wm_field, _to_renamed(wm_field)

        # Case 2: wm_field is already the renamed (target) name
        original = _to_original(wm_field)
        if original != wm_field:
            return original, wm_field

        # Case 3: no preserve_upstream mapping — same column on both sides
        return wm_field, wm_field

    def _expand_source_files(self, path: str) -> Optional[List[Dict[str, Any]]]:
        """
        Expand file patterns into concrete file paths and mtimes.
        Supports both local globs and cloud URI globs (abfss://, s3://, gs://) via fsspec.
        """
        if path.startswith("table:"):
            return None
        if self._is_uri_path(path):
            # Cloud URI — expand globs via fsspec if pattern contains wildcards
            if not any(ch in path for ch in ["*", "?", "["]):
                return None  # no glob — let the reader handle it directly
            try:
                import fsspec

                _sopts = self._get_cloud_storage_options(path)
                fs, _, paths = fsspec.get_fs_token_paths(path, storage_options=_sopts)
                results = []
                parsed_uri_parts = path.split("://", 1)
                protocol = parsed_uri_parts[0]
                original_authority = parsed_uri_parts[1].split("/", 1)[0]

                for p in sorted(paths):
                    info = fs.info(p)
                    # Reconstruct the full URI for each matched file
                    # adlfs drops the @account suffix from paths. If original URI had it, put it back.
                    if "@" in original_authority and p.startswith(original_authority.split("@")[0]):
                        container = original_authority.split("@")[0]
                        reconstructed_path = p[len(container) :]
                        if not reconstructed_path.startswith("/"):
                            reconstructed_path = "/" + reconstructed_path
                        full_uri = f"{protocol}://{original_authority}{reconstructed_path}"
                    else:
                        full_uri = f"{protocol}://{p}"
                    mtime = info.get("last_modified", 0)
                    if hasattr(mtime, "timestamp"):
                        mtime = mtime.timestamp()
                    results.append({"path": full_uri, "mtime": mtime})
                return results or None
            except FileNotFoundError:
                # Path genuinely does not exist — no files to process
                return None
            except ImportError:
                logger.warning(
                    "Cloud glob requires 'fsspec' and a provider package (e.g. adlfs). "
                    "Install with: pip install fsspec adlfs"
                )
                return None
            except Exception as e:
                err_msg = str(e).lower()
                # Detect auth / permission / connectivity errors and let them
                # propagate so the pipeline fails loudly instead of silently
                # reporting "no new rows".
                _fatal_keywords = (
                    "forbidden",
                    "unauthorized",
                    "403",
                    "401",
                    "permission",
                    "access denied",
                    "authentication",
                    "account_name",
                    "account_key",
                    "credential",
                    "name or service not known",
                    "connection",
                    "multiple values",
                )
                if any(kw in err_msg for kw in _fatal_keywords):
                    raise RuntimeError(f"Cloud storage access failed for {path}: {e}") from e
                # For anything else (e.g. empty glob, transient issues),
                # log a warning and let the caller decide.
                logger.warning(f"Cloud glob expansion failed for {path}: {e}")
                return None
        # Delta table directories (and other table formats) — no glob expansion.
        source_type = getattr(getattr(self.contract, "source", None), "type", None)
        if source_type == "table":
            return None
        p = Path(path)
        if p.is_dir():
            if (p / "_delta_log").exists():
                return None  # Delta table directory
            if (p / "metadata").exists() and (p / "data").exists():
                return None  # Iceberg table directory
        if not any(ch in path for ch in ["*", "?", "["]):
            return None

        from glob import glob

        pattern = path
        base = getattr(self.contract, "_base_path", None)
        if base and not Path(pattern).is_absolute():
            pattern = str(Path(base) / pattern)

        files = [f for f in glob(pattern, recursive=True) if Path(f).is_file()]
        results = []
        for file in sorted(files):
            try:
                results.append(
                    {
                        "path": str(Path(file).resolve()),
                        "mtime": Path(file).stat().st_mtime,
                    }
                )
            except Exception:
                continue
        return results or None

    def _expand_partitioned_paths(
        self,
        base_path: str,
        partition_cfg,
        *,
        override_start: Optional[str] = None,
        override_end: Optional[str] = None,
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Expand a base landing path into date-partitioned sub-paths using strftime.

        Instead of globbing the entire landing directory, only scans the
        directories for [today - lookback_days ... today] (or start_date..end_date).

        Supports sub-daily partitions (hourly via ``%H``, minute via ``%M``).
        When the format string contains ``%H`` or ``%M``, iteration switches
        from ``date`` to ``datetime`` with the appropriate step size so every
        partition slot is enumerated.

        Args:
            base_path: Base source path (e.g. "/Volumes/.../events").
            partition_cfg: SourcePartition config with format, lookback_days, etc.
            override_start: Reprocessing start date (YYYY-MM-DD) — overrides partition config.
            override_end: Reprocessing end date (YYYY-MM-DD) — overrides partition config.

        Returns:
            List of {path, mtime} dicts for all files found across partitions.
        """
        from datetime import date, datetime, timedelta

        # Reprocessing dates take precedence over partition config dates
        eff_start = override_start or partition_cfg.start_date
        eff_end = override_end or partition_cfg.end_date

        fmt = partition_cfg.format

        # Detect sub-daily partition granularity from the format string.
        # %H → hourly, %M → minute-level.  When present we iterate with
        # datetime objects instead of date objects so strftime resolves
        # the hour/minute tokens correctly.
        _has_hour = "%H" in fmt
        _has_minute = "%M" in fmt
        _sub_daily = _has_hour or _has_minute

        if _sub_daily:
            # Use datetime for sub-daily iteration
            if _has_minute:
                step = timedelta(minutes=1)
            else:
                step = timedelta(hours=1)

            if eff_start and eff_end:
                start_dt = datetime.fromisoformat(eff_start)
                end_dt = datetime.fromisoformat(eff_end)
            elif eff_start:
                start_dt = datetime.fromisoformat(eff_start)
                end_dt = datetime.now()
            elif eff_end:
                end_dt = datetime.fromisoformat(eff_end)
                start_dt = end_dt - timedelta(days=partition_cfg.lookback_days or 30)
            else:
                end_dt = datetime.now()
                start_dt = end_dt - timedelta(days=partition_cfg.lookback_days or 30)

            # Ensure we cover the full day range: snap start to midnight,
            # snap end to end-of-day (23:59) so all hours are scanned.
            start_dt = start_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            end_dt = end_dt.replace(hour=23, minute=59, second=59, microsecond=0)
        else:
            step = timedelta(days=1)
            # Day-level iteration uses date objects
            if eff_start and eff_end:
                start_dt = datetime.combine(date.fromisoformat(eff_start), datetime.min.time())
                end_dt = datetime.combine(date.fromisoformat(eff_end), datetime.min.time())
            elif eff_start:
                start_dt = datetime.combine(date.fromisoformat(eff_start), datetime.min.time())
                end_dt = datetime.combine(date.today(), datetime.min.time())
            elif eff_end:
                end_dt = datetime.combine(date.fromisoformat(eff_end), datetime.min.time())
                start_dt = end_dt - timedelta(days=partition_cfg.lookback_days or 30)
            else:
                end_dt = datetime.combine(date.today(), datetime.min.time())
                start_dt = end_dt - timedelta(days=partition_cfg.lookback_days or 30)

        file_pattern = partition_cfg.file_pattern
        if not file_pattern:
            # Auto-derive from source.format (e.g. json → *.json)
            src_fmt = getattr(getattr(self.contract, "source", None), "format", None)
            file_pattern = f"*.{src_fmt}" if src_fmt else "*"

        # Strip trailing glob from base_path if present (e.g. ".../events/*.json" -> ".../events")
        # The file_pattern from partition config will be used instead.
        base_clean = base_path.rstrip("/")
        # If path ends with a glob like "/*.json", extract the file pattern
        if "*" in base_clean.split("/")[-1]:
            parts = base_clean.rsplit("/", 1)
            base_clean = parts[0]
            if file_pattern == "*":
                file_pattern = parts[1]  # e.g. "*.json"

        all_files: List[Dict[str, Any]] = []
        seen_dirs: set = set()  # Deduplicate when step < 1 day but format has no hour token
        current = start_dt
        while current <= end_dt:
            partition_dir = current.strftime(fmt)
            if partition_dir not in seen_dirs:
                seen_dirs.add(partition_dir)
                partition_path = f"{base_clean}/{partition_dir}/{file_pattern}"
                logger.debug(f"Scanning partition: {partition_path}")
                files = self._expand_source_files(partition_path)
                if files:
                    all_files.extend(files)
            current += step

        _range_days = (end_dt - start_dt).days + 1
        if all_files:
            logger.info(
                f"Date-partitioned scan: {len(all_files)} files found "
                f"across {len(seen_dirs)} partitions ({start_dt.date()} to {end_dt.date()})"
            )
        else:
            logger.info(
                f"Date-partitioned scan: no files found in "
                f"{len(seen_dirs)} partitions ({start_dt.date()} to {end_dt.date()})"
            )

        return all_files if all_files else []

    def _get_last_source_watermark(self) -> Optional[float]:
        """
        Fetch the last max_source_mtime for this contract/stage from run logs.
        Uses dataset (target table name) + data_layer for precise filtering.
        """
        try:
            from lakelogic.core.run_log import get_last_run_watermark
        except Exception:
            return None
        contract_title = self.contract.info.title if self.contract.info else (self.contract.dataset or "unknown")
        stage = self.stage or "default"

        # Resolve dataset (target table name) and data_layer for precise filtering
        mat = getattr(self.contract, "materialization", None)
        _target_path = ""
        if mat:
            _target_path = getattr(mat, "target_path", "") or getattr(mat, "path", "") or ""
        dataset = None
        getattr(self.contract, "metadata", {}) or {}
        info = getattr(self.contract, "info", None)

        if str(_target_path).startswith("table:"):
            _table_full = str(_target_path)[len("table:") :]
            dataset = _table_full.split(".")[-1] if "." in _table_full else _table_full
        else:
            _info_table = getattr(info, "table_name", None) if info else None
            dataset = _info_table or getattr(self.contract, "dataset", None) or (info.title if info else None)
        data_layer = self._resolved_data_layer

        return get_last_run_watermark(
            self.contract,
            contract_title,
            stage,
            engine_name=self.engine_name,
            dataset=dataset,
            data_layer=data_layer,
        )

    def _write_empty_run_log(self, stage: str = "no_new_data") -> None:
        """Write a minimal run log entry for runs that found no new data.

        This ensures the run_log table always has a record of the pipeline
        attempt, even when there were no new files to process.
        """
        try:
            contract_title = self.contract.info.title if self.contract.info else (self.contract.dataset or "unknown")
            empty_counts = {"source": 0, "total": 0, "good": 0, "quarantined": 0, "quarantine_ratio": None}
            report = self._build_report(contract_title, empty_counts)
            # Override stage to indicate no new data (don't affect watermark)
            report["stage"] = stage
            report["status"] = stage  # e.g. "no_new_data"
            report["max_source_mtime"] = None
            self.last_report = report
            write_run_log(report, self.contract, engine_name=self.engine_name, run_log_mode=self._run_log_mode)
        except Exception as e:
            logger.debug(f"Could not write empty run log: {e}")

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
        if self.engine_name == "spark":  # pragma: no cover
            try:
                from pyspark.sql import SparkSession
                from pyspark.sql.types import StructType

                return SparkSession.builder.getOrCreate().createDataFrame([], schema=StructType([]))
            except Exception:
                return []
        return []

    def _is_uri_path(self, path: str) -> bool:
        r"""
        Check if a path is a URI (abfss://, s3://, gs://, file://, etc.).
        Also handles URIs that have been Windows-formatted (abfss:\) by pathlib.Path.
        """
        # Delegates to centralized lakelogic.core.paths module
        from lakelogic.core.paths import is_uri_path

        return is_uri_path(str(path))

    def _get_cloud_storage_options(self, path: str) -> Dict[str, str]:
        """
        Build storage_options dict for fsspec/adlfs from environment variables.
        Supports Azure (abfss://), AWS (s3://), and GCP (gs://).

        For Azure ``abfss://container@account.dfs.core.windows.net/...`` URIs,
        adlfs automatically extracts ``account_name`` from the URL.  We therefore
        only inject ``account_name`` when it **cannot** be inferred from the URI
        (e.g. ``az://container/path``), and always pass ``account_key`` when
        available so that key-based auth works out of the box.
        """
        opts: Dict[str, str] = {}
        p = str(path).lower()
        if p.startswith("abfss://") or p.startswith("az://"):
            # Determine whether the URI already embeds the account name
            # (``abfss://container@account.dfs.core.windows.net/...``).
            netloc = p.split("//", 1)[-1].split("/", 1)[0]
            uri_has_account = "@" in netloc

            acct = os.getenv("AZURE_STORAGE_ACCOUNT_NAME") or os.getenv("AZURE_STORAGE_ACCOUNT")
            if not acct and uri_has_account:
                # Extract account name from URL: container@account.dfs.core.windows.net
                acct = netloc.split("@", 1)[-1].split(".", 1)[0]

            if acct:
                # adlfs (used by pandas) will error if account_name is passed in kwargs
                # but already present in the URI.
                # Polars (Rust object_store) *requires* account_name in kwargs when
                # using Service Principal auth, regardless of the URI format.
                if getattr(self, "engine_name", "spark") in ("polars", "duckdb") or not uri_has_account:
                    opts["account_name"] = acct

            acct_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
            if acct_key:
                opts["account_key"] = acct_key

            tenant = os.getenv("AZURE_TENANT_ID")
            if tenant:
                opts["tenant_id"] = tenant
            client_id = os.getenv("AZURE_CLIENT_ID")
            if client_id:
                opts["client_id"] = client_id
            client_secret = os.getenv("AZURE_CLIENT_SECRET")
            if client_secret:
                opts["client_secret"] = client_secret
        elif p.startswith("s3://"):
            key = os.getenv("AWS_ACCESS_KEY_ID")
            if key:
                opts["key"] = key
            secret = os.getenv("AWS_SECRET_ACCESS_KEY")
            if secret:
                opts["secret"] = secret
        return opts

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
        Dispatches notifications based on contract config, registry config,
        and domain ownership contacts.

        Notification sources (all are dispatched, in this order):

        1. **Contract-level** — ``contract.quarantine.notifications``
        2. **Registry-level** — ``_notifications`` (from ``_system.yaml`` /
           ``_domain.yaml``, injected by the pipeline runner)
        3. **Ownership contacts** — ``_ownership.contacts`` (auto-resolved
           from domain config: email → email adapter, slack → slack adapter)

        Args:
            event: Event name triggering the notification.
            message: Notification body.
        """
        global_enabled = getattr(self, "_notifications_enabled", True)
        contract_enabled = (
            getattr(self.contract.quarantine, "notifications_enabled", True)
            if getattr(self.contract, "quarantine", None)
            else True
        )

        if not global_enabled or not contract_enabled:
            logger.debug(
                f"Notifications disabled (global_enabled={global_enabled}, contract_enabled={contract_enabled}). Skipping event: {event}"  # noqa: E501
            )
            return

        from lakelogic.notifications.base import resolve_ownership_contacts

        env = self._resolved_environment.upper()
        domain = self._resolved_domain
        system = self._resolved_system

        # Build prefix — only include domain/system if known
        scope = "/".join(filter(None, [domain, system]))
        prefix = f"[{env}] {scope}:" if scope else f"[{env}]"

        _EVENT_SUBJECTS = {
            "dataset_quality_check": f"{prefix} Dataset Quality Check Alert",
            "slo_breach": f"{prefix} SLO Breach Alert",
        }
        default_subject = _EVENT_SUBJECTS.get(event, f"{prefix} {event.capitalize()} Alert")
        template_context = self._notification_template_context(
            event=event,
            message=message,
            subject=default_subject,
            notification_type="auto",
        )

        dispatched_targets = set()  # Deduplicate across sources

        # ── 1. Contract-level notifications (quarantine config) ───────
        q = self.contract.quarantine
        if q and q.enabled and q.notifications:
            for notif in q.notifications:
                if (
                    event in notif.on_events
                    or (event == "dataset_quality_check" and "failure" in notif.on_events)
                    or (event == "failure" and "dataset_rule_failed" in notif.on_events)
                ):
                    try:
                        target = getattr(notif, "target", None) or ""

                        if target in dispatched_targets:
                            continue

                        config = notif.model_dump(by_alias=True)
                        if hasattr(self.contract, "_base_path"):
                            config["_base_path"] = str(self.contract._base_path)
                        adapter = get_notification_adapter(notif.type, config)
                        rendered_message, rendered_subject = render_notification_content(
                            adapter.config,
                            message=message,
                            subject=default_subject,
                            context=template_context,
                        )
                        adapter.send(rendered_message, subject=rendered_subject)

                        # Add to deduplication set only if it's hashable
                        try:
                            dispatched_targets.add(target)
                        except TypeError:
                            pass  # If unhashable (like a list), we still successfully dispatched it, just can't dedupe it later  # noqa: E501

                    except Exception as e:
                        logger.error(f"Failed to send notification: {e}")
                        if getattr(q, "strict_notifications", True):
                            raise RuntimeError(f"Notification error: {e}") from e

        # ── 2. Registry-level notifications (system/domain config) ────
        registry_notifs = getattr(self, "_notifications", None) or []
        for notif_cfg in registry_notifs:
            on_events = notif_cfg.get("on_events", [])
            # Backward compat: dataset_quality_check fires to channels subscribed to "failure"
            _event_match = event in on_events or (event == "dataset_quality_check" and "failure" in on_events)
            if not _event_match:
                continue
            try:
                target = notif_cfg.get("target", "")

                if target in dispatched_targets:
                    continue

                notif_type = notif_cfg.get("type", "")
                # Auto-detect type from target URL when not explicitly set
                if not notif_type:
                    if "hooks.slack.com" in target:
                        notif_type = "slack"
                    elif "webhook.office.com" in target or "teams" in target.lower():
                        notif_type = "teams"
                    elif "@" in target:
                        notif_type = "email"
                    else:
                        notif_type = "webhook"
                config = dict(notif_cfg)
                config["type"] = notif_type
                adapter = get_notification_adapter(notif_type, config)
                rendered_message, rendered_subject = render_notification_content(
                    adapter.config,
                    message=message,
                    subject=default_subject,
                    context=template_context,
                )
                adapter.send(rendered_message, subject=rendered_subject)

                try:
                    dispatched_targets.add(target)
                except TypeError:
                    pass

            except Exception as e:
                logger.warning(f"Registry notification failed ({notif_cfg.get('target', 'unknown')}): {e}")

        # ── 3. Ownership contacts (domain config, auto-resolved) ──────
        ownership = getattr(self, "_ownership", None) or {}
        if ownership:
            contact_channels = resolve_ownership_contacts(ownership, event)
            for ch in contact_channels:
                on_events = ch.get("on_events", [])
                _event_match = event in on_events or (event == "dataset_quality_check" and "failure" in on_events)
                if not _event_match:
                    continue
                try:
                    target = ch.get("target", "")

                    if target in dispatched_targets:
                        continue

                    adapter = get_notification_adapter(ch["type"], ch)
                    rendered_message, rendered_subject = render_notification_content(
                        ch,
                        message=message,
                        subject=default_subject,
                        context=template_context,
                    )
                    adapter.send(rendered_message, subject=rendered_subject)

                    try:
                        dispatched_targets.add(target)
                    except TypeError:
                        pass

                    logger.debug(f"  Notified ownership contact: {ch.get('_source', target)}")
                except Exception as e:
                    # Missing infrastructure config (smtp_host, etc.) is a
                    # config gap, not a runtime error — log at DEBUG level.
                    err_str = str(e)
                    target_str = ch.get("_source", ch.get("target", "unknown"))
                    if "missing required fields" in err_str:
                        logger.debug(f"Ownership notification skipped ({target_str}): {e}")
                    else:
                        logger.warning(f"Ownership notification failed ({target_str}): {e}")

    def _notification_template_context(
        self,
        *,
        event: str,
        message: str,
        subject: str,
        notification_type: str,
    ) -> Dict[str, Any]:
        """
        Build context values available to notification templates.

        Args:
            event: Triggering event name.
            message: Default event message.
            subject: Default event subject.
            notification_type: Destination notification type.

        Returns:
            Context dictionary for template rendering.
        """
        info = getattr(self.contract, "info", None)
        metadata = getattr(self.contract, "metadata", {}) or {}
        title = getattr(info, "title", None) or getattr(self.contract, "dataset", None) or "unknown"

        # Resolve dataset using the same chain as _build_report
        mat_obj = getattr(self.contract, "materialization", None)
        _target_path = ""
        if mat_obj:
            _target_path = getattr(mat_obj, "target_path", "") or getattr(mat_obj, "path", "") or ""
        if str(_target_path).startswith("table:"):
            _table_full = str(_target_path)[len("table:") :]
            _dataset = _table_full.split(".")[-1] if "." in _table_full else _table_full
        else:
            _info_table = getattr(info, "table_name", None) if info else None
            _dataset = _info_table or getattr(self.contract, "dataset", None) or title

        return {
            "event": event,
            "message": message,
            "subject": subject,
            "notification_type": notification_type,
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "engine": self.engine_name,
            "run_id": self.last_run_id,
            "pipeline_run_id": self.pipeline_run_id,
            "source_path": self.last_source_path,
            "environment": self._resolved_environment,
            "contract": {
                "title": title,
                "version": getattr(info, "version", None),
                "owner": getattr(info, "owner", None),
                "dataset": _dataset,
                "domain": self._resolved_domain,
                "system": self._resolved_system,
                "layer": self._resolved_data_layer,
            },
            "metadata": metadata,
            "ownership": getattr(self, "_ownership", {}) or {},
            "domain": self._resolved_domain,
            "system": self._resolved_system,
        }

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
        # Avoid wrapping URIs or table: paths in Path() — it corrupts slashes on Windows
        target = target_path
        if target_path and not isinstance(target_path, Path):
            target_str = str(target_path)
            if not target_str.startswith("table:") and "://" not in target_str:
                target = Path(target_path)
        result = materialize_dataframe(
            good_df,
            self.contract,
            target_path=target,
            engine_name=self.engine_name,
            incremental_metadata=getattr(self, "_incremental_metadata", None),
            is_reprocess=getattr(self, "_is_reprocess", False),
        )

        if bad_df is not None and self.contract.quarantine and self.contract.quarantine.target:
            try:
                materialize_quarantine(bad_df, self.contract, engine_name=self.engine_name)
            except Exception as q_err:
                logger.warning(
                    f"⚠️ Quarantine write failed (pipeline continues): {q_err}. "
                    f"Bad records were validated but could not be persisted to the quarantine target. "
                    f"Consider resetting the quarantine Delta table if schema has diverged."
                )

            # ── Secondary target fan-out for quarantine ──────────────────────
            try:
                mat_cfg = getattr(self.contract, "materialization", None)
                sec_targets = getattr(mat_cfg, "secondary_targets", None) if mat_cfg else None
                if sec_targets and isinstance(sec_targets, list):
                    from lakelogic.core.materialization import write_to_secondary_targets

                    _q_table = f"_quarantine_{self.contract.dataset or 'data'}"
                    write_to_secondary_targets(
                        sec_targets,
                        bad_df,
                        _q_table,
                        strategy="append",
                    )
            except Exception as _sec_exc:
                logger.debug(f"Quarantine secondary fan-out skipped: {_sec_exc}")

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
        if self.engine_name == "spark":  # pragma: no cover
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
        # Use centralized _resolved_* attributes from __init__
        getattr(self.contract, "metadata", {}) or {}
        info = getattr(self.contract, "info", None)

        domain = self._resolved_domain
        system = self._resolved_system
        data_layer = self._resolved_data_layer

        # ── Resolve dataset ─────────────────────────────────────────────────────
        # Prefer actual target table name for easier filtering in run_log queries.
        # Falls back to contract.dataset or info.title.
        mat_obj = getattr(self.contract, "materialization", None)
        _target_path = ""
        if mat_obj:
            _target_path = getattr(mat_obj, "target_path", "") or getattr(mat_obj, "path", "") or ""
        if str(_target_path).startswith("table:"):
            # Use just the table name part (after "table:")
            _table_full = str(_target_path)[len("table:") :]
            # Use the last part (table name) for the dataset field
            dataset = _table_full.split(".")[-1] if "." in _table_full else _table_full
        else:
            _info_table = getattr(info, "table_name", None) if info else None
            dataset = _info_table or self.contract.dataset or (info.title if info else None)

        # ── Resolve source_path ─────────────────────────────────────────────────
        # run() callers set last_source_path; run_source() doesn't — use first
        # source file from the expanded file list instead.
        source_path = self.last_source_path
        if not source_path:
            source_files = getattr(self, "_source_files", None) or []
            if source_files:
                source_path = source_files[0].get("path")

        # Resolve contract version
        contract_version = None
        if info:
            contract_version = getattr(info, "version", None)
        if not contract_version:
            contract_version = self.contract.version if hasattr(self.contract, "version") else None

        # During reprocessing, mark the run log entry so the incremental
        # watermark reader ignores it (it filters on stage == "source"/"default").
        _is_reprocess = getattr(self, "_is_reprocess", False)
        _stage = "reprocess" if _is_reprocess else (self.stage or "default")
        # Don't regress the watermark with older files during reprocessing
        _max_mtime = None if _is_reprocess else self._source_max_mtime

        report = {
            "run_id": self.last_run_id,
            "pipeline_run_id": self.pipeline_run_id,
            "engine": self.engine_name,
            "contract": contract_title,
            "contract_file_name": getattr(self.contract, "contract_file_name", None),
            "contract_version": str(contract_version) if contract_version else None,
            "stage": _stage,
            "dataset": dataset,
            "domain": domain,
            "system": system,
            "environment": self._resolved_environment,
            "data_layer": data_layer,
            "source_path": source_path,
            "source_files": self._source_files or [],
            "max_source_mtime": _max_mtime,
            "timestamp": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "counts": counts,
            "dataset_rules": getattr(self.adapter, "dataset_rule_results", []),
            "slos": slos or {},
            "row_rule_failures": row_rule_failures or [],
            "schema_drift": schema_drift or {},
            "incremental_metadata": getattr(self, "_incremental_metadata", {}),
        }

        # ── Execution context (auto-captured for AI diagnosis) ──────────────
        try:
            from lakelogic.core.execution_context import capture_execution_context

            report["execution_context"] = capture_execution_context(
                self.engine_name,
                start_time=getattr(self, "_run_start_time", None),
            )
        except Exception:
            pass  # never let context capture break the pipeline

        return report

    def _inject_lineage(
        self,
        good_df: Any,
        bad_df: Any,
        source_path: Optional[Union[str, Path]] = None,
    ) -> Tuple[Any, Any]:
        """Delegate to lakelogic.core.lineage (kept for backward compat)."""
        from lakelogic.core.lineage import inject_lineage

        return inject_lineage(
            good_df,
            bad_df,
            self.contract,
            self.engine_name,
            self.last_run_id,
            self.pipeline_run_id,
            source_path,
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

            self._active_trace_steps.append(TraceStep(step=step, timestamp=time.time(), **kwargs))

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
            self.contract,
            good_df,
            self.engine_name,
            self.last_run_id,
            self.last_source_path,
            add_trace_fn=self._add_current_trace,
            trace_step_fn=self.trace_step,
        )

    def _run_python_logic(self, path: Path, logic, good_df: Any) -> Tuple[Any, bool]:
        """Delegate to lakelogic.core.external_logic (kept for backward compat)."""
        from lakelogic.core.external_logic import _run_python_logic

        return _run_python_logic(
            path,
            logic,
            good_df,
            self.contract,
            self.engine_name,
            self.last_run_id,
            self._add_current_trace,
            self.trace_step,
        )

    def _run_notebook_logic(self, path: Path, logic, good_df: Any) -> Tuple[Any, bool]:
        """Delegate to lakelogic.core.external_logic (kept for backward compat)."""
        from lakelogic.core.external_logic import _run_notebook_logic

        return _run_notebook_logic(
            path,
            logic,
            good_df,
            self.contract,
            self.engine_name,
            self.last_run_id,
            self.last_source_path,
        )

    def _load_output_frame(self, path: Path, fmt: Optional[str]) -> Any:
        """Delegate to lakelogic.core.external_logic (kept for backward compat)."""
        from lakelogic.core.external_logic import _load_output_frame

        return _load_output_frame(path, fmt)

    def _compute_slos(self, good_df: Any, counts: Dict[str, Optional[int]]) -> Dict[str, Any]:
        """Delegate to lakelogic.core.slo (kept for backward compat)."""
        try:
            from lakelogic.core.slo import compute_slos

            return compute_slos(self.contract, good_df, counts, self.engine_name)
        except ImportError:
            return []

    # ─── SLO helpers (delegated to lakelogic.core.slo) ────────────────────
    def _parse_duration_seconds(self, value: Any) -> Optional[float]:
        try:
            from lakelogic.core.slo import _parse_duration_seconds

            return _parse_duration_seconds(value)
        except ImportError:
            return None

    def _get_max_timestamp(self, df: Any, field: str) -> Optional[datetime]:
        try:
            from lakelogic.core.slo import _get_max_timestamp

            return _get_max_timestamp(df, field, self.engine_name)
        except ImportError:
            return None

    def _coerce_datetime(self, value: Any) -> Optional[datetime]:
        try:
            from lakelogic.core.slo import _coerce_datetime

            return _coerce_datetime(value)
        except ImportError:
            return None

    def _compute_freshness(self, good_df: Any, freshness_obj: Any) -> Dict[str, Any]:
        try:
            from lakelogic.core.slo import _compute_freshness

            return _compute_freshness(good_df, freshness_obj, self.engine_name)
        except ImportError:
            return {}

    def _compute_availability(
        self, good_df: Any, counts: Dict[str, Optional[int]], availability_obj: Any
    ) -> Dict[str, Any]:
        try:
            from lakelogic.core.slo import _compute_availability

            return _compute_availability(good_df, counts, availability_obj, self.engine_name)
        except ImportError:
            return {}

    def _non_null_ratio(self, df: Any, field: str) -> Optional[float]:
        try:
            from lakelogic.core.slo import _non_null_ratio

            return _non_null_ratio(df, field, self.engine_name)
        except ImportError:
            return None

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

            if isinstance(bad_df, pl.DataFrame):
                logger.debug(f"_extract_row_rule_failures: Polars DF with columns={bad_df.columns}, rows={len(bad_df)}")
                if error_col in bad_df.columns:
                    # Explode the list column into individual error strings
                    exploded = bad_df.select(pl.col(error_col)).explode(error_col).drop_nulls()
                    if not exploded.is_empty():
                        errors.extend(exploded.to_series().to_list())
                    logger.debug(f"_extract_row_rule_failures: extracted {len(errors)} error(s)")
                else:
                    logger.debug(f"_extract_row_rule_failures: '{error_col}' not in columns")
        except Exception as exc:
            logger.debug(f"Polars error extraction failed: {exc}")

        try:
            import pandas as pd

            if isinstance(bad_df, pd.DataFrame) and error_col in bad_df.columns:
                for item in bad_df[error_col].explode().dropna().tolist():
                    errors.append(item)
        except Exception as exc:
            logger.debug(f"Pandas error extraction failed: {exc}")

        if self.engine_name == "spark":  # pragma: no cover
            try:
                from pyspark.sql import functions as F

                if error_col in bad_df.columns:
                    rows = bad_df.select(F.explode(F.col(error_col)).alias("error")).groupBy("error").count().collect()
                    for r in rows:
                        if r["error"]:
                            errors.extend([r["error"]] * r["count"])
            except Exception as exc:
                logger.debug(f"Spark error extraction failed: {exc}")

        from collections import Counter

        error_counts = Counter(errors)

        failures = []
        for err, count in error_counts.items():
            if isinstance(err, str) and err.startswith("Rule failed: "):
                payload = err[len("Rule failed: ") :]
                name = payload
                sql = None
                if " (" in payload and payload.endswith(")"):
                    name, sql = payload.split(" (", 1)
                    sql = sql[:-1]
                failures.append({"name": name, "sql": sql, "message": err, "count": count})
            else:
                failures.append({"message": str(err), "count": count})
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
        compliance_event: Optional[Dict[str, Any]] = None,
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
            compliance_event: Optional compliance event metadata.

        Returns:
            DataFrame with PII erased for specified subjects.
        """
        from lakelogic.core.gdpr import forget_subjects

        report_out = []
        result = forget_subjects(
            df,
            self.contract,
            subject_column,
            subject_ids,
            erasure_strategy=erasure_strategy,
            hash_salt=hash_salt,
            compliance_event=compliance_event,
            audit_report_out=report_out,
        )
        if report_out:
            self.last_report = report_out[0]
        return result

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
            df,
            self.contract,
            strategy=strategy,
            hash_salt=hash_salt,
            columns=columns,
        )

    # ── dlt source integration ────────────────────────────────────────────

    def _run_dlt_source(self) -> "ValidationResult":
        """Extract data via dlt, convert to Polars, run validation.

        Called by :meth:`run_source` when ``source.type == "dlt"``.
        The contract's ``dlt`` block drives extraction; the contract's
        ``model.fields`` enforces the schema.
        """
        try:
            from lakelogic.adapters.dlt_adapter import DltAdapter
        except ImportError:
            raise ImportError("dlt integration requires the dlt package. Install with: pip install lakelogic[dlt]")

        from lakelogic.core.run_log import get_last_run_dlt_state

        contract_title = self.contract.info.title if self.contract.info else (self.contract.dataset or "dlt_source")
        logger.info(f"Running dlt source for contract: {contract_title}")

        # Fetch the previous state from run log
        previous_state = get_last_run_dlt_state(
            self.contract,
            contract_title=contract_title,
            stage="validate",
            engine_name=self.engine_name,
            dataset=self.contract.dataset,  # use dataset directly so we map back to the right trace
            data_layer=self._resolved_data_layer,
        )

        adapter = DltAdapter(self.contract.source, contract_title)
        arrow_table = adapter.extract(previous_state=previous_state)

        # Save state so run() can embed it into the log and the materialize phase
        self._pending_dlt_state_json = getattr(adapter, "dlt_state_json", None)

        import polars as pl

        df = pl.from_arrow(arrow_table)
        logger.info(f"dlt: loaded {df.height} rows, {df.width} columns into Polars")

        # Feed into the existing validation pipeline
        return self.run(
            df,
            source_path=f"dlt://{self.contract.source.dlt.source or self.contract.source.dlt.base_url}",
        )

    # ── database source integration ────────────────────────────────────────────

    def _run_database_source(self) -> "ValidationResult":
        """Extract data natively via Polars or DuckDB, run validation.

        Called by :meth:`run_source` when ``source.type == "database"``.

        Supports:
          - PostgreSQL, MySQL, SQL Server, Oracle, SQLite via URI auto-detection.
          - Incremental CDC via ``load_mode: incremental`` + ``watermark_field``.
          - Batch chunking via ``options.fetch_size`` (SQLAlchemy iterator).
          - Parallel partitioned reads via ``options.partition_column`` + ``partition_num``.
        """
        import polars as pl

        contract_title = (
            self.contract.info.title if self.contract.info else (self.contract.dataset or "database_source")
        )
        logger.info(f"Running database source for contract: {contract_title} via engine={self.engine_name}")

        dataset = self.contract.dataset
        if not dataset:
            raise ValueError("Database source requires 'dataset' to be defined in contract to use as table name")

        uri = self.contract.source.path
        if not uri:
            raise ValueError("Database source requires 'source.path' connection URI")

        # ── Options extraction ───────────────────────────────
        options = getattr(self.contract.source, "options", {}) or {}
        partition_column = options.get("partition_column")
        partition_num = options.get("partition_num")
        fetch_size = options.get("fetch_size")

        # ── Column projection from contract model ────────────
        # Push column selection down to the database query so we only
        # transfer the fields declared in the contract over the wire.
        columns = "*"
        model = getattr(self.contract, "model", None)
        if model and getattr(model, "fields", None):
            col_names = [f.name for f in model.fields if f.name]
            # Ensure the watermark field is always fetched even if
            # it was omitted from the model (needed for CDC tracking).
            wf = getattr(self.contract.source, "watermark_field", None)
            if wf and wf not in col_names:
                col_names.append(wf)
            if col_names:
                columns = ", ".join(f'"{c}"' for c in col_names)
                logger.info(f"Column projection: selecting {len(col_names)} fields from contract model")

        custom_query = getattr(self.contract.source, "query", None)
        if custom_query:
            query = f"SELECT {columns} FROM ({custom_query}) AS _lakelogic_src"
        else:
            query = f'SELECT {columns} FROM "{dataset}"'

        load_mode = getattr(self.contract.source, "load_mode", "full")
        watermark_field = getattr(self.contract.source, "watermark_field", None)

        watermark = None
        if load_mode == "incremental":
            if not watermark_field:
                raise ValueError("Incremental load mode requires 'source.watermark_field' in contract config")

            watermark = self._get_last_source_watermark()
            if watermark is not None:
                # _get_last_source_watermark returns a float (Unix epoch).
                # Convert to ISO-8601 so the SQL WHERE clause is valid for
                # TIMESTAMP / DATETIME columns in all database dialects.
                from datetime import datetime, timezone

                watermark_iso = datetime.fromtimestamp(watermark, tz=timezone.utc).isoformat()
                query += f" WHERE {watermark_field} > '{watermark_iso}'"
                logger.info(f"Incremental mode active. Appending filter: WHERE {watermark_field} > '{watermark_iso}'")
            else:
                logger.info("Incremental mode: First run detected. Running full table extraction.")

        # ── Execute via chosen engine ────────────────────────
        if self.engine_name == "polars":
            if fetch_size:
                logger.info(f"Batch execution active. Fetching data in {fetch_size} row chunks via SQLAlchemy yield.")
                try:
                    import sqlalchemy

                    sa_engine = sqlalchemy.create_engine(uri).execution_options(yield_per=fetch_size)

                    all_good: list = []
                    all_bad: list = []
                    batch_idx = 0
                    with sa_engine.connect() as conn:
                        # pl.read_database natively supports iterating batches
                        # when given an SQLAlchemy connection object.
                        for chunk_df in pl.read_database(
                            query, connection=conn, iter_batches=True, batch_size=fetch_size
                        ):
                            batch_idx += 1
                            logger.info(f"Processing database chunk {batch_idx} ({chunk_df.height} rows)...")

                            res = self.run(chunk_df, source_path=f"database://{dataset}")
                            all_good.append(res.good)
                            all_bad.append(res.bad)

                    if batch_idx == 0:
                        logger.warning("Query returned zero rows. Returning empty result.")
                        return ValidationResult(pl.DataFrame(), pl.DataFrame())

                    combined_good = pl.concat(all_good) if all_good else pl.DataFrame()
                    combined_bad = pl.concat(all_bad) if all_bad else pl.DataFrame()
                    logger.info(
                        f"Batch ingestion complete across {batch_idx} chunks. "
                        f"Good: {combined_good.height}, Bad: {combined_bad.height}"
                    )
                    return ValidationResult(combined_good, combined_bad)

                except ImportError:
                    raise ImportError("Batching requires SQLAlchemy. (Tip: pip install SQLAlchemy)")
                except Exception as e:
                    raise RuntimeError(f"Polars batched DB extraction failed. Error: {e}")

            else:
                # Eager / parallel processing
                kwargs = {}
                if partition_column and partition_num:
                    kwargs["partition_on"] = partition_column
                    kwargs["partition_num"] = partition_num
                    kwargs["engine"] = "connectorx"
                    logger.info(
                        f"Running parallel extraction partitioned on '{partition_column}' ({partition_num} slices)."
                    )

                try:
                    logger.debug(f"Executing Polars read_database_uri: {query}")
                    df = pl.read_database_uri(query, uri, **kwargs)
                except Exception as e:
                    raise RuntimeError(
                        f"Polars DB extraction failed. "
                        f"(Tip: pip install connectorx or adbc-driver-postgresql). Error: {e}"
                    )

        elif self.engine_name == "duckdb":
            import duckdb

            # Sniff dialect for accurate DuckDB extension mapping
            dialect = uri.split("://")[0].lower()
            extension = "postgres"
            scanner = "postgres_scan"

            if "mysql" in dialect:
                extension = "mysql"
                scanner = "mysql_scan"
            elif "sqlite" in dialect:
                extension = "sqlite"
                scanner = "sqlite_scan"

            duckdb.sql(f"INSTALL {extension}; LOAD {extension};")
            try:
                # DuckDB sqlite_scan expects a raw path, not a URI
                duckdb_uri = uri
                if extension == "sqlite":
                    duckdb_uri = duckdb_uri.replace("sqlite:///", "").replace("sqlite://", "")

                duckdb_query = f"SELECT * FROM {scanner}('{duckdb_uri}', '{dataset}')"
                if load_mode == "incremental" and watermark is not None:
                    duckdb_query += f" WHERE {watermark_field} > '{watermark_iso}'"

                logger.debug(f"Executing DuckDB {scanner}: {duckdb_query}")
                df = duckdb.sql(duckdb_query).pl()
            except Exception as e:
                raise RuntimeError(f"DuckDB {extension} DB extraction failed. Error: {e}")

        else:
            raise ValueError("Database source natively supports engines 'polars' and 'duckdb'.")

        logger.info(f"database: loaded {df.height} rows, {df.width} columns")

        return self.run(
            df,
            source_path=f"database://{dataset}",
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
            raise ValueError(f"Streaming mode requires the 'polars' engine, got '{self.engine_name}'.")

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
            raise ValueError(f"Streaming mode supports .parquet, .csv, .ndjson/.jsonl files. Got: {path_str}")

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

    # ── GDPR & HIPAA Utilities ───────────────────────────────────────────────

    def forget_hipaa(
        self,
        df: Any,
        subject_column: str,
        subject_ids: List[str],
        *,
        erasure_strategy: str = "nullify",
        hash_salt: str = "",
        audit: bool = True,
    ) -> Any:
        """
        GDPR Right-to-be-Forgotten: erase PII for specific data subjects.
        Delegates to lakelogic.core.gdpr.forget_subjects using this processor's contract.
        """
        from lakelogic.core.gdpr import forget_subjects

        return forget_subjects(
            df=df,
            contract=self.contract,
            subject_column=subject_column,
            subject_ids=subject_ids,
            erasure_strategy=erasure_strategy,
            hash_salt=hash_salt,
            audit=audit,
        )

    def mask_pii_hipaa(
        self,
        df: Any,
        *,
        strategy: str = "nullify",
        hash_salt: str = "",
        columns: Optional[List[str]] = None,
    ) -> Any:
        """
        Mask all PII columns across all rows in a dataframe.
        Delegates to lakelogic.core.gdpr.mask_pii_columns.
        """
        from lakelogic.core.gdpr import mask_pii_columns

        return mask_pii_columns(
            df,
            self.contract,
            strategy=strategy,
            hash_salt=hash_salt,
            columns=columns,
        )

    def forget_patient(
        self,
        df: Any,
        patient_column: str,
        patient_ids: List[str],
        *,
        erasure_strategy: str = "nullify",
        hash_salt: str = "",
        audit: bool = True,
    ) -> Any:
        """
        HIPAA Right-to-be-Forgotten: erase PHI for specific patients.
        Delegates to lakelogic.core.hipaa.forget_patients using this processor's contract.
        """
        from lakelogic.core.hipaa import forget_patients

        return forget_patients(
            df=df,
            contract=self.contract,
            patient_column=patient_column,
            patient_ids=patient_ids,
            erasure_strategy=erasure_strategy,
            hash_salt=hash_salt,
            audit=audit,
        )

    def mask_phi(
        self,
        df: Any,
        *,
        strategy: str = "nullify",
        hash_salt: str = "",
        columns: Optional[List[str]] = None,
    ) -> Any:
        """
        Mask all PHI columns across all rows in a dataframe (Safe Harbor).
        Delegates to lakelogic.core.hipaa.mask_phi_columns.
        """
        from lakelogic.core.hipaa import mask_phi_columns

        return mask_phi_columns(
            df,
            self.contract,
            strategy=strategy,
            hash_salt=hash_salt,
            columns=columns,
        )
