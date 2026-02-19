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
    Richer result object for LakeLogic runs.
    Unpacks as (raw_df, good_df, bad_df) for flexible usage, 
    but also provides .raw, .good, and .bad attributes for clarity.
    """
    def __init__(self, good, bad, raw):
        self.good = good
        self.bad = bad
        self.raw = raw
    
    def __iter__(self):
        yield self.raw
        yield self.good
        yield self.bad
        
    def __getitem__(self, idx):
        return [self.raw, self.good, self.bad][idx]

    def __len__(self):
        return 3

    def __repr__(self):
        def _count(obj):
            if obj is None: return 0
            if hasattr(obj, "height"): return obj.height
            if hasattr(obj, "count"): 
                try: return obj.count().fetchone()[0]
                except: return "?"
            try: return len(obj)
            except: return "?"
        return f"ValidationResult(good={_count(self.good)}, bad={_count(self.bad)}, raw={_count(self.raw)})"

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
    ):
        """
        Initialize the DataProcessor.
        
        Args:
            contract: The Data Contract definition (path to YAML, dict, or DataContract object).
            engine: The execution engine to use. If None, it uses the auto-discovery logic.
            stage: Optional stage override (e.g., bronze/silver) applied from contract "stages".
            pipeline_run_id: Optional pipeline-level run id for correlation across contracts.
        """
        self._configure_logging()
        self.engine_name = (engine or self._discover_engine()).lower()
        self.stage = stage
        self.contract = self._load_contract(contract)
        self.adapter = self._get_adapter()
        self.adapter.engine_name = self.engine_name
        self.last_report: Optional[Dict[str, Any]] = None
        self.last_run_id: Optional[str] = None
        self.pipeline_run_id: Optional[str] = pipeline_run_id
        self.last_source_path: Optional[str] = None
        self._source_files: List[Dict[str, Any]] = []
        self._source_max_mtime: Optional[float] = None

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
    ) -> ValidationResult:
        """
        Runs the contract against the provided dataframe.

        Args:
            df: Input dataframe.
            source_path: Optional source path for lineage/run reporting.
            materialize: Whether to write outputs to materialization targets.
            materialize_target: Optional override target for materialization.

        Returns:
            ValidationResult object (unpacks to good_df, bad_df).
        """
        contract_title = self.contract.info.title if self.contract.info else (self.contract.dataset or "unknown")
        self.last_run_id = uuid.uuid4().hex
        self.last_source_path = str(source_path) if source_path else None
        logger.info(f"Starting LakeLogic run [Auto-Engine: {self.engine_name}, Contract: {contract_title}]")
        
        # Execute via adapter
        good_df, bad_df = self.adapter.execute(df)

        # Inject lineage metadata
        good_df, bad_df = self._inject_lineage(good_df, bad_df, source_path)
        
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
        slos = self._compute_slos(good_df, counts)
        row_rule_failures = self._extract_row_rule_failures(bad_df)
        self.last_report = self._build_report(contract_title, counts, slos, row_rule_failures, drift)
        write_run_log(self.last_report, self.contract, engine_name=self.engine_name)

        # Optional external logic hook (python/notebook)
        good_df, external_handled = self._apply_external_logic(good_df)

        # Materialize if requested
        if materialize and not external_handled:
            self.materialize(good_df, bad_df, target_path=materialize_target)

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

        return ValidationResult(good_df, bad_df, df)

    def run_source(self, source: Optional[Union[str, Path]] = None) -> ValidationResult:
        """
        Loads data from a source file and runs the contract in one step.
        The data is loaded using the engine's optimized reader.

        Args:
            source: Optional file path to load. If None, uses path from contract.

        Returns:
            ValidationResult object (unpacks to good_df, bad_df).
        """
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
        if self.engine_name == "polars":
            import polars as pl
            if self.contract.server and self.contract.server.format:
                fmt = self.contract.server.format.lower()
                if fmt == "delta":
                    # Use Delta-RS for Spark-free Delta Lake operations
                    try:
                        from lakelogic.engines.delta_adapter import DeltaAdapter
                        adapter = DeltaAdapter()
                        df = adapter.read(path, as_polars=True)
                        logger.info(f"Loaded Delta table via Delta-RS: {path}")
                    except ImportError:
                        raise ValueError(
                            "Delta Lake sources require Delta-RS. Install with: pip install 'lakelogic[delta]' or pip install deltalake"
                        )
                elif fmt == "iceberg":
                    raise ValueError("Iceberg sources require Spark engine.")
            if df is None:  # Not Delta, use standard Polars readers
                if file_paths:
                    if len(file_paths) == 1:
                        if path.endswith(".parquet"):
                            df = pl.read_parquet(file_paths[0])
                        elif path.endswith(".xml"):
                            df = pl.read_xml(file_paths[0])
                        elif path.endswith((".xlsx", ".xls")):
                            df = pl.read_excel(file_paths[0])
                        else:
                            df = pl.read_csv(file_paths[0])
                    else:
                        if path.endswith(".parquet"):
                            df = pl.concat([pl.read_parquet(p) for p in file_paths], how="vertical")
                        elif path.endswith(".xml"):
                            df = pl.concat([pl.read_xml(p) for p in file_paths], how="vertical")
                        elif path.endswith((".xlsx", ".xls")):
                            df = pl.concat([pl.read_excel(p) for p in file_paths], how="vertical")
                        else:
                            df = pl.concat([pl.read_csv(p) for p in file_paths], how="vertical")
                else:
                    if path.endswith(".csv"): df = pl.read_csv(path)
                    elif path.endswith(".parquet"): df = pl.read_parquet(path)
                    elif path.endswith(".xml"): df = pl.read_xml(path)
                    elif path.endswith((".xlsx", ".xls")): df = pl.read_excel(path)
                    else: df = pl.read_csv(path) # default
        elif self.engine_name == "pandas":
            import pandas as pd
            if self.contract.server and self.contract.server.format:
                fmt = self.contract.server.format.lower()
                if fmt == "delta":
                    # Use Delta-RS for Spark-free Delta Lake operations
                    try:
                        from lakelogic.engines.delta_adapter import DeltaAdapter
                        adapter = DeltaAdapter()
                        df = adapter.read(path, as_polars=False)  # Returns Pandas
                        logger.info(f"Loaded Delta table via Delta-RS: {path}")
                    except ImportError:
                        raise ValueError(
                            "Delta Lake sources require Delta-RS. Install with: pip install 'lakelogic[delta]' or pip install deltalake"
                        )
                elif fmt == "iceberg":
                    raise ValueError("Iceberg sources require Spark engine.")
            if df is None:  # Not Delta, use standard Pandas readers
                if file_paths:
                    frames = []
                    for file in file_paths:
                        if file.endswith(".parquet"):
                            frames.append(pd.read_parquet(file))
                        elif file.endswith(".xml"):
                            frames.append(pd.read_xml(file))
                        elif file.endswith((".xlsx", ".xls")):
                            frames.append(pd.read_excel(file))
                        else:
                            frames.append(pd.read_csv(file))
                    df = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
                else:
                    if path.endswith(".csv"): df = pd.read_csv(path)
                    elif path.endswith(".parquet"): df = pd.read_parquet(path)
                    elif path.endswith(".xml"): df = pd.read_xml(path)
                    elif path.endswith((".xlsx", ".xls")): df = pd.read_excel(path)
                    else: df = pd.read_csv(path)
        elif self.engine_name == "duckdb":
            import duckdb
            if self.contract.server and self.contract.server.format:
                fmt = self.contract.server.format.lower()
                if fmt == "delta":
                    # Use Delta-RS for Spark-free Delta Lake operations
                    try:
                        from lakelogic.engines.delta_adapter import DeltaAdapter
                        adapter = DeltaAdapter()
                        df = adapter.read(path, as_polars=False)  # Returns Pandas (DuckDB compatible)
                        logger.info(f"Loaded Delta table via Delta-RS: {path}")
                    except ImportError:
                        raise ValueError(
                            "Delta Lake sources require Delta-RS. Install with: pip install 'lakelogic[delta]' or pip install deltalake"
                        )
                elif fmt == "iceberg":
                    raise ValueError("Iceberg sources require Spark engine.")
            def _duckdb_read_csv(paths):
                try:
                    return duckdb.read_csv(paths)
                except Exception as exc:
                    logger.warning(
                        f"DuckDB CSV auto-detect failed for {paths}. Retrying with relaxed settings. Error: {exc}"
                    )
                    return duckdb.read_csv(
                        paths,
                        delim=",",
                        quote='"',
                        escape='"',
                        header=True,
                        strict_mode=False,
                    )
            # Convert to Pandas DF immediately to ensure connection-agnostic transfer
            if df is None:  # Not Delta, use standard DuckDB readers
                if file_paths:
                    if path.endswith(".parquet"):
                        rel = duckdb.read_parquet(file_paths)
                    elif path.endswith(".xml"):
                        import pandas as pd
                        df = pd.concat([pd.read_xml(f) for f in file_paths], ignore_index=True)
                        rel = None
                    elif path.endswith((".xlsx", ".xls")):
                        import pandas as pd
                        df = pd.concat([pd.read_excel(f) for f in file_paths], ignore_index=True)
                        rel = None
                    else:
                        rel = _duckdb_read_csv(file_paths)
                else:
                    if path.endswith(".csv"): rel = _duckdb_read_csv(path)
                    elif path.endswith(".parquet"): rel = duckdb.read_parquet(path)
                    elif path.endswith(".xml"):
                        import pandas as pd
                        df = pd.read_xml(path)
                        rel = None
                    elif path.endswith((".xlsx", ".xls")):
                        import pandas as pd
                        df = pd.read_excel(path)
                        rel = None
                    else: rel = _duckdb_read_csv(path)
                
                if rel is not None:
                    df = rel.df()
        elif self.engine_name == "spark":
            from pyspark.sql import SparkSession
            
            # Determine format first to check if we need special packages
            fmt = None
            if path.endswith(".csv"):
                fmt = "csv"
            elif path.endswith(".parquet"):
                fmt = "parquet"
            elif path.endswith(".xml"):
                fmt = "xml"
            elif path.endswith((".xlsx", ".xls")):
                fmt = "excel"
            elif self.contract.server and self.contract.server.format:
                fmt = self.contract.server.format.lower()
            
            # Auto-configure Spark packages for XML and Excel if needed
            spark_builder = SparkSession.builder
            if fmt == "xml":
                # Check if spark-xml is already available, if not, add it
                try:
                    spark = SparkSession.getActiveSession()
                    if spark is None:
                        logger.info("Adding spark-xml package for XML support")
                        spark_builder = spark_builder.config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.18.0")
                        spark = spark_builder.getOrCreate()
                    else:
                        spark = spark_builder.getOrCreate()
                except Exception:
                    spark = spark_builder.getOrCreate()
            elif fmt == "excel":
                # Check if spark-excel is already available, if not, add it
                try:
                    spark = SparkSession.getActiveSession()
                    if spark is None:
                        logger.info("Adding spark-excel package for Excel support")
                        spark_builder = spark_builder.config("spark.jars.packages", "com.crealytics:spark-excel_2.12:3.4.0_0.20.2")
                        spark = spark_builder.getOrCreate()
                    else:
                        spark = spark_builder.getOrCreate()
                except Exception:
                    spark = spark_builder.getOrCreate()
            else:
                spark = spark_builder.getOrCreate()
            
            if path.startswith("table:"):
                table_name = path[6:]
                df = spark.table(table_name)
                return self.run(df, source_path=table_name)

            fmt = fmt or "parquet"
            reader = spark.read.format(fmt)
            if fmt == "csv":
                reader = reader.option("header", "true")
            elif fmt == "excel":
                reader = reader.option("header", "true").option("inferSchema", "true")
            
            load_path = path
            if not self._is_uri_path(path):
                try:
                    load_path = Path(path).expanduser().resolve().as_posix()
                except Exception:
                    load_path = path
            if file_paths:
                df = reader.load(file_paths)
            else:
                df = reader.load(load_path)
        elif self.engine_name in ["snowflake", "bigquery"]:
            table_name = path[6:] if path.startswith("table:") else path
            return self.run(table_name, source_path=table_name)
            
        # If the adapter can handle paths natively, pass the path string
        # Otherwise, the engine-specific loading above will have populated 'df'
        input_data = df if df is not None else path
            
        return self.run(input_data, source_path=path)

    def _expand_source_files(self, path: str) -> Optional[List[Dict[str, Any]]]:
        """
        Expand local file patterns into concrete file paths and mtimes.
        """
        if self._is_uri_path(path) or path.startswith("table:"):
            return None
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
        metadata = getattr(self.contract, "metadata", {}) or {}
        domain = metadata.get("domain")
        system = metadata.get("system")
        data_layer = metadata.get("data_layer")
        return {
            "run_id": self.last_run_id,
            "pipeline_run_id": self.pipeline_run_id,
            "engine": self.engine_name,
            "contract": contract_title,
            "stage": self.stage or "default",
            "dataset": self.contract.dataset,
            "domain": domain,
            "system": system,
            "data_layer": data_layer,
            "source_path": self.last_source_path,
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

        preserve_cols = list(getattr(lineage, "preserve_upstream", []) or [])
        if preserve_cols:
            prefix = getattr(lineage, "upstream_prefix", "_upstream") or "_upstream"
            good_df = self._preserve_upstream_lineage(good_df, preserve_cols, prefix)
            bad_df = self._preserve_upstream_lineage(bad_df, preserve_cols, prefix)

        source_value = str(source_path) if source_path else None
        timestamp_value = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
        run_id_value = self.last_run_id
        if getattr(lineage, "run_id_source", "run_id") == "pipeline_run_id" and self.pipeline_run_id:
            run_id_value = self.pipeline_run_id

        # If any capture_* field is explicitly set, only honor those explicitly set fields.
        capture_fields = {
            "capture_source_path",
            "capture_timestamp",
            "capture_run_id",
            "capture_domain",
            "capture_system",
        }
        explicit_fields = set()
        if hasattr(lineage, "model_fields_set"):
            explicit_fields = set(getattr(lineage, "model_fields_set"))
        elif hasattr(lineage, "__pydantic_fields_set__"):
            explicit_fields = set(getattr(lineage, "__pydantic_fields_set__"))
        explicit_capture = explicit_fields & capture_fields

        metadata = getattr(self.contract, "metadata", {}) or {}
        domain_value = metadata.get("domain")
        system_value = metadata.get("system")
        columns: Dict[str, Any] = {}
        if (("capture_source_path" in explicit_capture) if explicit_capture else True) and getattr(lineage, "capture_source_path", False):
            columns[lineage.source_column_name] = source_value
        if (("capture_timestamp" in explicit_capture) if explicit_capture else True) and getattr(lineage, "capture_timestamp", False):
            columns[lineage.timestamp_column_name] = timestamp_value
        if (("capture_run_id" in explicit_capture) if explicit_capture else True) and getattr(lineage, "capture_run_id", False):
            columns[lineage.run_id_column_name] = run_id_value
        if (("capture_domain" in explicit_capture) if explicit_capture else True) and getattr(lineage, "capture_domain", False) and domain_value is not None:
            columns[lineage.domain_column_name] = domain_value
        if (("capture_system" in explicit_capture) if explicit_capture else True) and getattr(lineage, "capture_system", False) and system_value is not None:
            columns[lineage.system_column_name] = system_value

        if not columns:
            return good_df, bad_df

        return self._add_columns(good_df, columns), self._add_columns(bad_df, columns)

    def _preserve_upstream_lineage(self, df: Any, columns: List[str], prefix: str) -> Any:
        """
        Rename existing lineage columns to preserve upstream lineage before injecting new lineage values.

        Args:
            df: Engine dataframe.
            columns: Column names to preserve.
            prefix: Prefix for preserved columns.

        Returns:
            Updated dataframe.
        """
        if df is None:
            return df

        def _new_name(col: str) -> str:
            if col.startswith("_lakelogic_"):
                return col.replace("_lakelogic", prefix, 1)
            return f"{prefix}_{col.lstrip('_')}"

        rename_map: Dict[str, str] = {col: _new_name(col) for col in columns}

        try:
            import polars as pl
            if isinstance(df, pl.DataFrame):
                existing = set(df.columns)
                mapping = {src: dst for src, dst in rename_map.items() if src in existing and dst not in existing}
                return df.rename(mapping) if mapping else df
        except Exception:
            pass

        try:
            import pandas as pd
            if isinstance(df, pd.DataFrame):
                existing = set(df.columns)
                mapping = {src: dst for src, dst in rename_map.items() if src in existing and dst not in existing}
                return df.rename(columns=mapping) if mapping else df
        except Exception:
            pass

        if self.engine_name == "spark":
            try:
                existing = set(df.columns)
                updated = df
                for src, dst in rename_map.items():
                    if src in existing and dst not in existing:
                        updated = updated.withColumnRenamed(src, dst)
                return updated
            except Exception:
                return df

        try:
            import duckdb
            if isinstance(df, duckdb.DuckDBPyRelation):
                cols = []
                try:
                    cols = list(df.columns)
                except Exception:
                    try:
                        cols = [row[0] for row in df.connection.execute(f"DESCRIBE {df.sql_query()}").fetchall()]
                    except Exception:
                        cols = [row[0] for row in df.connection.execute(f"DESCRIBE SELECT * FROM ({df.sql_query()})").fetchall()]

                existing = set(cols)
                target_set = set(rename_map.values())
                exprs = []
                for col in cols:
                    if col in rename_map and rename_map[col] not in existing:
                        col_name = col.replace('"', '""')
                        new_name = rename_map[col].replace('"', '""')
                        exprs.append(f"\"{col_name}\" AS \"{new_name}\"")
                    elif col in target_set:
                        # Drop existing target columns to avoid duplicates.
                        continue
                    else:
                        exprs.append(f"\"{col.replace('\"', '\"\"')}\"")
                query = f"SELECT {', '.join(exprs)} FROM ({df.sql_query()})"
                return df.connection.sql(query)
        except Exception:
            pass

        return df

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

        # DuckDB relation support
        try:
            import duckdb
            if isinstance(df, duckdb.DuckDBPyRelation):
                def _lit(val):
                    if val is None:
                        return "NULL"
                    if isinstance(val, bool):
                        return "TRUE" if val else "FALSE"
                    if isinstance(val, (int, float)):
                        return str(val)
                    text = str(val).replace("'", "''")
                    return f"'{text}'"

                exprs = ["*"]
                for name, value in columns.items():
                    col_name = str(name).replace('"', '""')
                    exprs.append(f"{_lit(value)} AS \"{col_name}\"")
                query = f"SELECT {', '.join(exprs)} FROM ({df.sql_query()})"
                return df.connection.sql(query)
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

        spec = importlib.util.spec_from_file_location(f"lakelogic_external_{self.last_run_id}", path)
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
            raise ValueError("Notebook execution requires nbformat and nbclient. Install lakelogic[notebook].") from exc

        params = dict(logic.args or {})
        base_path = getattr(self.contract, "_base_path", None)
        if base_path:
            params.setdefault("lakelogic_contract_dir", str(Path(base_path)))
        params.setdefault("lakelogic_engine", self.engine_name)
        params.setdefault("lakelogic_run_id", self.last_run_id)
        params.setdefault("lakelogic_source_path", self.last_source_path)

        # Write validated input to a temp CSV for notebook access
        tmp_dir = Path(base_path) / ".lakelogic" if base_path else (Path.cwd() / ".lakelogic")
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
            params.setdefault("lakelogic_input_path", str(input_path))
            params.setdefault("lakelogic_input_format", "csv")
        except Exception as exc:
            logger.warning(f"Failed to write notebook input data: {exc}")

        output_path = None
        if logic.output_path:
            output_path = Path(logic.output_path)
            if not output_path.is_absolute() and getattr(self.contract, "_base_path", None):
                output_path = Path(self.contract._base_path) / output_path
            params.setdefault("lakelogic_output_path", str(output_path))

        nb = nbformat.read(path, as_version=4)
        inject_cell = nbformat.v4.new_code_cell(f"LAKELOGIC_PARAMS = {repr(params)}")
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
                # Single aggregation: compute total and non-null count together
                # Avoids two separate .count() calls (each triggering full DAG)
                result = df.agg(
                    F.count("*").alias("total"),
                    F.count(F.col(field)).alias("non_null")
                ).first()
                total = result["total"]
                non_null = result["non_null"]
                if total == 0:
                    return None
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
