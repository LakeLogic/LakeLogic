import yaml
import re
import os
import sys
from typing import Any, Tuple, Union, Dict, Optional
from pathlib import Path
from datetime import datetime, timezone
import uuid

from lakeguard.core.models import DataContract
from lakeguard.engines.base import EngineAdapter
from lakeguard.notifications.base import get_notification_adapter
from lakeguard.core.materialization import materialize_dataframe, materialize_quarantine, write_run_log
from loguru import logger

class DataProcessor:
    """
    The main entry point for running LakeGuard contracts.
    
    This class handles contract loading, engine selection, and dispatches 
    processing to the appropriate engine adapter.
    """

    def __init__(
        self,
        contract: Union[str, Path, dict, DataContract],
        engine: Optional[str] = None,
        *,
        pipeline_run_id: Optional[str] = None,
    ):
        """
        Initialize the DataProcessor.
        
        Args:
            contract: The Data Contract definition (path to YAML, dict, or DataContract object).
            engine: The execution engine to use. If None, it uses the auto-discovery logic.
            pipeline_run_id: Optional pipeline-level run id for correlation across contracts.
        """
        self.engine_name = (engine or self._discover_engine()).lower()
        self.contract = self._load_contract(contract)
        self.adapter = self._get_adapter()
        self.adapter.engine_name = self.engine_name
        self.last_report: Optional[Dict[str, Any]] = None
        self.last_run_id: Optional[str] = None
        self.pipeline_run_id: Optional[str] = pipeline_run_id
        self.last_source_path: Optional[str] = None

    def _discover_engine(self) -> str:
        """
        Automatically discovers the best available engine.
        Priority:
        1. LAKEGUARD_ENGINE env var
        2. Spark (if running in Databricks/Spark environment)
        3. Polars (if installed)
        4. DuckDB (if installed)
        5. Pandas (fallback)
        """
        # 1. Check Env Var
        env_engine = os.getenv("LAKEGUARD_ENGINE")
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
            from lakeguard.engines.polars import PolarsAdapter
            return PolarsAdapter(self.contract)
        elif self.engine_name == "pandas":
            from lakeguard.engines.pandas import PandasAdapter
            return PandasAdapter(self.contract)
        elif self.engine_name == "duckdb":
            from lakeguard.engines.duckdb import DuckDBAdapter
            return DuckDBAdapter(self.contract)
        elif self.engine_name in ["spark", "pyspark"]:
            from lakeguard.engines.spark import SparkAdapter
            return SparkAdapter(self.contract)
        elif self.engine_name == "snowflake":
            from lakeguard.engines.snowflake import SnowflakeAdapter
            return SnowflakeAdapter(self.contract)
        elif self.engine_name == "bigquery":
            from lakeguard.engines.bigquery import BigQueryAdapter
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
            return contract
        if isinstance(contract, dict):
            return DataContract(**contract)
        
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
            except Exception:
                pass
            return contract

    def run(
        self,
        df: Any,
        source_path: Optional[Union[str, Path]] = None,
        materialize: bool = False,
        materialize_target: Optional[Union[str, Path]] = None,
    ) -> Tuple[Any, Any]:
        """
        Runs the contract against the provided dataframe.

        Args:
            df: Input dataframe.
            source_path: Optional source path for lineage/run reporting.
            materialize: Whether to write outputs to materialization targets.
            materialize_target: Optional override target for materialization.

        Returns:
            Tuple of (good_df, bad_df).
        """
        contract_title = self.contract.info.title if self.contract.info else (self.contract.dataset or "unknown")
        self.last_run_id = uuid.uuid4().hex
        self.last_source_path = str(source_path) if source_path else None
        logger.info(f"???  Starting LakeGuard run [Auto-Engine: {self.engine_name}, Contract: {contract_title}]")
        
        # Execute via adapter
        good_df, bad_df = self.adapter.execute(df)

        # Inject lineage metadata
        good_df, bad_df = self._inject_lineage(good_df, bad_df, source_path)
        
        # Summary logging
        counts = self._compute_counts(df, good_df, bad_df)
        total = counts.get("total")
        bad = counts.get("quarantined")
        if total is not None and bad is not None:
            ratio = counts.get("quarantine_ratio")
            ratio_display = f"{ratio:.2%}" if ratio is not None else "n/a"
            logger.info(f"? Run complete. Total: {total}, Good: {counts.get('good')}, Quarantined: {bad}, Ratio: {ratio_display}")

            if bad > 0:
                msg = f"LakeGuard Alert: {bad} records quarantined in '{contract_title}'. Total: {total} (ratio {ratio_display})"
                self.notify(event="quarantine", message=msg)

        # Check dataset rules
        if hasattr(self.adapter, "dataset_rule_results"):
            failures = [r for r in self.adapter.dataset_rule_results if not r.get("passed")]
            if failures:
                details = "; ".join([f"{r.get('name')}={r.get('value')}" for r in failures])
                msg = f"LakeGuard dataset rule failures in '{contract_title}': {details}"
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
        slos = self._compute_slos(good_df, counts)
        row_rule_failures = self._extract_row_rule_failures(bad_df)
        self.last_report = self._build_report(contract_title, counts, slos, row_rule_failures, drift)
        write_run_log(self.last_report, self.contract, engine_name=self.engine_name)

        # Optional external logic hook (python/notebook)
        good_df, external_handled = self._apply_external_logic(good_df)

        # Materialize if requested
        if materialize and not external_handled:
            self.materialize(good_df, bad_df, target_path=materialize_target)

        return good_df, bad_df

    def run_source(self, source: Union[str, Path]) -> Tuple[Any, Any]:
        """
        Loads data from a source file and runs the contract in one step.
        The data is loaded using the engine's optimized reader.

        Args:
            source: File path to load.

        Returns:
            Tuple of (good_df, bad_df).
        """
        path = str(source)
        logger.info(f"?? Loading source: {path} via {self.engine_name}")
        
        df = None
        if self.engine_name == "polars":
            import polars as pl
            if self.contract.server and self.contract.server.format:
                fmt = self.contract.server.format.lower()
                if fmt in ["delta", "iceberg"]:
                    raise ValueError("Delta/Iceberg sources require Spark engine.")
            if path.endswith(".csv"): df = pl.read_csv(path)
            elif path.endswith(".parquet"): df = pl.read_parquet(path)
            else: df = pl.read_csv(path) # default
        elif self.engine_name == "pandas":
            import pandas as pd
            if self.contract.server and self.contract.server.format:
                fmt = self.contract.server.format.lower()
                if fmt in ["delta", "iceberg"]:
                    raise ValueError("Delta/Iceberg sources require Spark engine.")
            if path.endswith(".csv"): df = pd.read_csv(path)
            elif path.endswith(".parquet"): df = pd.read_parquet(path)
            else: df = pd.read_csv(path)
        elif self.engine_name == "duckdb":
            import duckdb
            if self.contract.server and self.contract.server.format:
                fmt = self.contract.server.format.lower()
                if fmt in ["delta", "iceberg"]:
                    raise ValueError("Delta/Iceberg sources require Spark engine.")
            df = duckdb.read_csv(path) if path.endswith(".csv") else duckdb.read_parquet(path)
        elif self.engine_name == "spark":
            from pyspark.sql import SparkSession
            spark = SparkSession.builder.getOrCreate()
            if path.startswith("table:"):
                table_name = path[6:]
                df = spark.table(table_name)
                return self.run(df, source_path=table_name)
            fmt = None
            if path.endswith(".csv"):
                fmt = "csv"
            elif path.endswith(".parquet"):
                fmt = "parquet"
            elif self.contract.server and self.contract.server.format:
                fmt = self.contract.server.format.lower()

            fmt = fmt or "parquet"
            reader = spark.read.format(fmt)
            if fmt == "csv":
                reader = reader.option("header", "true")
            df = reader.load(path)
        elif self.engine_name in ["snowflake", "bigquery"]:
            table_name = path[6:] if path.startswith("table:") else path
            return self.run(table_name, source_path=table_name)
            
        if df is None:
            raise ValueError(f"Could not load data from {path} using engine {self.engine_name}")
            
        return self.run(df, source_path=path)

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
                    adapter.send(message, subject=f"LakeGuard {event.capitalize()} Alert")
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

        Args:
            source_df: Original input dataframe.
            good_df: Validated dataframe.
            bad_df: Quarantined dataframe.

        Returns:
            Dict with total, good, quarantined, and ratio values.
        """
        def _count(obj: Any) -> Optional[int]:
            """
            Return row count for supported dataframe types.

            Args:
                obj: Dataframe-like object.

            Returns:
                Row count or None.
            """
            try:
                if self.engine_name == "spark":
                    return int(obj.count())
                return len(obj)
            except Exception:
                return None

        total = _count(source_df)
        good = _count(good_df)
        bad = _count(bad_df)
        ratio = None
        if total is not None and bad is not None and total > 0:
            ratio = bad / total
        return {"total": total, "good": good, "quarantined": bad, "quarantine_ratio": ratio}

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
        return {
            "run_id": self.last_run_id,
            "pipeline_run_id": self.pipeline_run_id,
            "engine": self.engine_name,
            "contract": contract_title,
            "source_path": self.last_source_path,
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
        """
        Inject lineage columns into good and bad dataframes.

        Args:
            good_df: Validated dataframe.
            bad_df: Quarantined dataframe.
            source_path: Optional source path for lineage.

        Returns:
            Tuple of (good_df, bad_df) with lineage columns injected.
        """
        lineage = self.contract.lineage
        if not lineage or not getattr(lineage, "enabled", False):
            return good_df, bad_df

        source_value = str(source_path) if source_path else None
        timestamp_value = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        run_id_value = self.last_run_id

        columns: Dict[str, Any] = {}
        if getattr(lineage, "capture_source_path", False):
            columns[lineage.source_column_name] = source_value
        if getattr(lineage, "capture_timestamp", False):
            columns[lineage.timestamp_column_name] = timestamp_value
        if getattr(lineage, "capture_run_id", False):
            columns[lineage.run_id_column_name] = run_id_value

        if not columns:
            return good_df, bad_df

        return self._add_columns(good_df, columns), self._add_columns(bad_df, columns)

    def _add_columns(self, df: Any, columns: Dict[str, Any]) -> Any:
        """
        Add constant columns to a dataframe across supported engines.

        Args:
            df: Engine dataframe.
            columns: Mapping of column name to constant value.

        Returns:
            Updated dataframe.
        """
        if df is None:
            return df
        try:
            import polars as pl
            if isinstance(df, pl.DataFrame):
                return df.with_columns([pl.lit(value).alias(name) for name, value in columns.items()])
        except Exception:
            pass

        try:
            import pandas as pd
            if isinstance(df, pd.DataFrame):
                updated = df.copy()
                for name, value in columns.items():
                    updated[name] = value
                return updated
        except Exception:
            pass

        if self.engine_name == "spark":
            try:
                from pyspark.sql import functions as F
                updated = df
                for name, value in columns.items():
                    updated = updated.withColumn(name, F.lit(value))
                return updated
            except Exception:
                return df

        return df

    def _apply_external_logic(self, good_df: Any) -> Tuple[Any, bool]:
        """
        Execute optional external logic hooks.

        Args:
            good_df: Validated dataframe.

        Returns:
            Tuple of (updated_good_df, external_handled_output).
        """
        logic = self.contract.external_logic
        if not logic:
            return good_df, False

        logic_type = (logic.type or "").lower()
        if not logic.path:
            logger.warning("external_logic configured without path; skipping.")
            return good_df, False

        base_path = getattr(self.contract, "_base_path", None)
        path = Path(logic.path)
        if not path.is_absolute() and base_path:
            path = Path(base_path) / path

        if logic_type == "python":
            return self._run_python_logic(path, logic, good_df)

        if logic_type == "notebook":
            return self._run_notebook_logic(path, logic, good_df)

        logger.warning(f"Unsupported external_logic.type: {logic.type}")
        return good_df, False

    def _run_python_logic(self, path: Path, logic, good_df: Any) -> Tuple[Any, bool]:
        """
        Execute an external python module and return updated dataframe if provided.

        Args:
            path: Path to python file.
            logic: ExternalLogic config.
            good_df: Validated dataframe.

        Returns:
            Tuple of (updated_good_df, external_handled_output).
        """
        import importlib.util
        if not path.exists():
            raise FileNotFoundError(f"External logic file not found: {path}")

        spec = importlib.util.spec_from_file_location(f"lakeguard_external_{self.last_run_id}", path)
        if spec is None or spec.loader is None:
            raise ValueError(f"Could not load external logic module: {path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)  # type: ignore[arg-type]

        entrypoint = getattr(logic, "entrypoint", "run")
        if not hasattr(module, entrypoint):
            raise AttributeError(f"External logic entrypoint '{entrypoint}' not found in {path}")

        fn = getattr(module, entrypoint)
        args = logic.args or {}
        result = fn(good_df, contract=self.contract, engine=self.engine_name, **args)

        if result is None:
            handled = bool(logic.handles_output)
            return good_df, handled

        # If a path is returned, load it as a dataframe
        if isinstance(result, (str, Path)):
            output_df = self._load_output_frame(Path(result), logic.output_format)
            return output_df, False

        # If tuple, take first element as the dataframe
        if isinstance(result, tuple) and result:
            return result[0], False

        return result, False

    def _run_notebook_logic(self, path: Path, logic, good_df: Any) -> Tuple[Any, bool]:
        """
        Execute an external notebook.

        Args:
            path: Path to notebook file.
            logic: ExternalLogic config.
            good_df: Validated dataframe.

        Returns:
            Tuple of (updated_good_df, external_handled_output).
        """
        if not path.exists():
            raise FileNotFoundError(f"External notebook not found: {path}")

        try:
            import nbformat  # type: ignore
            from nbclient import NotebookClient  # type: ignore
        except Exception as exc:
            raise ValueError("Notebook execution requires nbformat and nbclient. Install lakeguard[notebook].") from exc

        params = dict(logic.args or {})
        base_path = getattr(self.contract, "_base_path", None)
        if base_path:
            params.setdefault("lakeguard_contract_dir", str(Path(base_path)))
        params.setdefault("lakeguard_engine", self.engine_name)
        params.setdefault("lakeguard_run_id", self.last_run_id)
        params.setdefault("lakeguard_source_path", self.last_source_path)

        # Write validated input to a temp CSV for notebook access
        tmp_dir = Path(base_path) / ".lakeguard" if base_path else (Path.cwd() / ".lakeguard")
        tmp_dir.mkdir(parents=True, exist_ok=True)
        input_path = tmp_dir / f"input_{self.last_run_id}.csv"
        try:
            import pandas as pd
            if hasattr(good_df, "to_pandas"):
                pdf = good_df.to_pandas()
            elif hasattr(good_df, "toPandas"):
                pdf = good_df.toPandas()
            else:
                pdf = good_df
            if not isinstance(pdf, pd.DataFrame):
                pdf = pd.DataFrame(pdf)
            pdf.to_csv(input_path, index=False)
            params.setdefault("lakeguard_input_path", str(input_path))
            params.setdefault("lakeguard_input_format", "csv")
        except Exception as exc:
            logger.warning(f"Failed to write notebook input data: {exc}")

        output_path = None
        if logic.output_path:
            output_path = Path(logic.output_path)
            if not output_path.is_absolute() and getattr(self.contract, "_base_path", None):
                output_path = Path(self.contract._base_path) / output_path
            params.setdefault("lakeguard_output_path", str(output_path))

        nb = nbformat.read(path, as_version=4)
        inject_cell = nbformat.v4.new_code_cell(f"LAKEGUARD_PARAMS = {repr(params)}")
        nb.cells.insert(0, inject_cell)

        client = NotebookClient(nb, kernel_name=logic.kernel_name)
        client.execute()

        if output_path:
            output_df = self._load_output_frame(output_path, logic.output_format)
            return output_df, False

        handled = True if logic.handles_output is None else bool(logic.handles_output)
        return good_df, handled

    def _load_output_frame(self, path: Path, fmt: Optional[str]) -> Any:
        """
        Load an output dataframe from disk.

        Args:
            path: Output path.
            fmt: Optional format override.

        Returns:
            pandas.DataFrame
        """
        import pandas as pd
        output_format = (fmt or path.suffix.lstrip(".") or "csv").lower()
        if output_format == "parquet":
            return pd.read_parquet(path)
        return pd.read_csv(path)

    def _compute_slos(self, good_df: Any, counts: Dict[str, Optional[int]]) -> Dict[str, Any]:
        """
        Compute freshness and availability SLO metrics.

        Args:
            good_df: Validated dataframe.
            counts: Count metrics for the run.

        Returns:
            Dict containing SLO results.
        """
        slos: Dict[str, Any] = {}
        svc = self.contract.service_levels
        if not svc:
            return slos

        if svc.freshness:
            slos["freshness"] = self._compute_freshness(good_df, svc.freshness)

        if svc.availability is not None:
            slos["availability"] = self._compute_availability(good_df, counts, svc.availability)

        return slos

    def _parse_duration_seconds(self, value: Any) -> Optional[float]:
        """
        Parse a duration string to seconds.

        Args:
            value: Duration (e.g., 24h, 30m) or numeric hours.

        Returns:
            Duration in seconds, if parseable.
        """
        if value is None:
            return None
        if isinstance(value, (int, float)):
            # Interpret numeric freshness threshold as hours
            return float(value) * 3600.0
        if isinstance(value, str):
            text = value.strip().lower()
            if not text:
                return None
            try:
                import re
                match = re.match(r"^(\d+(?:\.\d+)?)([smhdw])$", text)
                if not match:
                    return None
                amount = float(match.group(1))
                unit = match.group(2)
                multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
                return amount * multipliers[unit]
            except Exception:
                return None
        return None

    def _get_max_timestamp(self, df: Any, field: str) -> Optional[datetime]:
        """
        Get the max timestamp from a dataframe column.

        Args:
            df: Engine dataframe.
            field: Column name to inspect.

        Returns:
            Max timestamp or None.
        """
        if not field:
            return None
        try:
            import polars as pl
            if isinstance(df, pl.DataFrame):
                if field not in df.columns:
                    return None
                value = df.select(pl.col(field).max()).to_series()[0]
                return self._coerce_datetime(value)
        except Exception:
            pass

        try:
            import pandas as pd
            if isinstance(df, pd.DataFrame):
                if field not in df.columns:
                    return None
                value = df[field].max()
                return self._coerce_datetime(value)
        except Exception:
            pass

        if self.engine_name == "spark":
            try:
                from pyspark.sql import functions as F
                if field not in df.columns:
                    return None
                value = df.agg(F.max(field).alias("max_value")).collect()[0][0]
                return self._coerce_datetime(value)
            except Exception:
                return None

        return None

    def _coerce_datetime(self, value: Any) -> Optional[datetime]:
        """
        Coerce a value to a timezone-aware datetime.

        Args:
            value: Input value (datetime/string).

        Returns:
            Datetime in UTC or None.
        """
        if value is None:
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        try:
            import pandas as pd
            ts = pd.to_datetime(value, utc=True, errors="coerce")
            if ts is not None and ts is not pd.NaT:
                return ts.to_pydatetime()
        except Exception:
            pass
        if isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
                return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except Exception:
                return None
        return None

    def _compute_freshness(self, good_df: Any, freshness_obj: Any) -> Dict[str, Any]:
        """
        Compute freshness metrics from the newest record timestamp.

        Args:
            good_df: Validated dataframe.
            freshness_obj: Service level definition.

        Returns:
            Dict containing freshness metrics.
        """
        result: Dict[str, Any] = {"passed": None}
        field = None
        threshold = None
        if hasattr(freshness_obj, "field"):
            field = getattr(freshness_obj, "field", None)
            threshold = getattr(freshness_obj, "threshold", None)
        elif isinstance(freshness_obj, dict):
            field = freshness_obj.get("field")
            threshold = freshness_obj.get("threshold")
        elif isinstance(freshness_obj, str):
            threshold = freshness_obj

        threshold_seconds = self._parse_duration_seconds(threshold)
        if threshold_seconds is not None:
            result["threshold_seconds"] = threshold_seconds

        max_ts = self._get_max_timestamp(good_df, field) if field else None
        if max_ts is None:
            return result

        now = datetime.now(timezone.utc)
        age_seconds = (now - max_ts).total_seconds()
        result["max_timestamp"] = max_ts.isoformat()
        result["age_seconds"] = age_seconds
        if threshold_seconds is not None:
            result["passed"] = age_seconds <= threshold_seconds
        return result

    def _compute_availability(self, good_df: Any, counts: Dict[str, Optional[int]], availability_obj: Any) -> Dict[str, Any]:
        """
        Compute availability metrics based on non-null ratio or good/total.

        Args:
            good_df: Validated dataframe.
            counts: Count metrics.
            availability_obj: Service level definition.

        Returns:
            Dict containing availability metrics.
        """
        result: Dict[str, Any] = {"passed": None}
        field = None
        threshold = None

        if hasattr(availability_obj, "field"):
            field = getattr(availability_obj, "field", None)
            threshold = getattr(availability_obj, "threshold", None)
        elif isinstance(availability_obj, dict):
            field = availability_obj.get("field")
            threshold = availability_obj.get("threshold")
        else:
            threshold = availability_obj

        ratio = None
        if field:
            ratio = self._non_null_ratio(good_df, field)
        else:
            total = counts.get("total")
            good = counts.get("good")
            if total and good is not None:
                ratio = good / total

        result["ratio"] = ratio

        if threshold is not None:
            try:
                threshold_val = float(threshold)
            except Exception:
                threshold_val = None
            if threshold_val is not None:
                if threshold_val > 1:
                    threshold_val = threshold_val / 100.0
                result["threshold"] = threshold_val
                if ratio is not None:
                    result["passed"] = ratio >= threshold_val

        return result

    def _non_null_ratio(self, df: Any, field: str) -> Optional[float]:
        """
        Calculate the non-null ratio for a given column.

        Args:
            df: Engine dataframe.
            field: Column name.

        Returns:
            Ratio of non-null values.
        """
        try:
            import polars as pl
            if isinstance(df, pl.DataFrame):
                if field not in df.columns:
                    return None
                total = df.height
                if total == 0:
                    return None
                non_null = df.select(pl.col(field).is_not_null().sum()).to_series()[0]
                return float(non_null) / float(total)
        except Exception:
            pass

        try:
            import pandas as pd
            if isinstance(df, pd.DataFrame):
                if field not in df.columns:
                    return None
                total = len(df)
                if total == 0:
                    return None
                non_null = df[field].notna().sum()
                return float(non_null) / float(total)
        except Exception:
            pass

        if self.engine_name == "spark":
            try:
                from pyspark.sql import functions as F
                if field not in df.columns:
                    return None
                total = df.count()
                if total == 0:
                    return None
                non_null = df.filter(F.col(field).isNotNull()).count()
                return float(non_null) / float(total)
            except Exception:
                return None

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
        error_col = getattr(self.adapter, "ERROR_COLUMN", "_lakeguard_errors")
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
