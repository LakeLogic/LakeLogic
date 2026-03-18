"""
Declarative Data Mesh Pipeline engine.

Takes a DomainRegistry and executes data contracts in sequence
(bronze -> silver -> gold), handling dependencies, retries, GDPR erasures,
and HIPAA masking automatically.
"""

from __future__ import annotations

import json
import os
import time
import uuid
from typing import Any, Dict, List, Optional, Set

from loguru import logger

from lakelogic.core.models import DataContract
from lakelogic.core.observer import RemoteObserver
from lakelogic.core.processor import DataProcessor
from lakelogic.core.registry import DomainRegistry, RegistryContract


class PipelineRunSummary:
    """Standardized summary of a pipeline execution."""

    def __init__(self, run_id: str, environment: str, dry_run: bool):
        self.run_id = run_id
        self.environment = environment
        self.dry_run = dry_run
        self.results: List[Dict[str, Any]] = []

    def append(self, contract: str, layer: str, status: str, rows: Any = "-", error: str = ""):
        self.results.append({"contract": contract, "layer": layer, "status": status, "rows": rows, "error": error})

    def to_dict(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "environment": self.environment,
            "dry_run": self.dry_run,
            "results": self.results,
        }


class LakehousePipeline:
    """
    Executes a DomainRegistry through a pipeline run.
    """

    def __init__(self, registry: DomainRegistry, engine: str = "spark", spark: Any = None):
        self.registry = registry
        self.engine = engine
        self.spark = spark
        self.run_id = str(uuid.uuid4())

        if self.engine == "spark" and not self.spark:
            # Try to auto-resolve if inside Databricks
            try:
                from pyspark.sql import SparkSession

                self.spark = SparkSession.builder.getOrCreate()
            except ImportError:
                pass

    # ── Phase 1: Setup & Resets ──────────────────────────────────────────────

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
            elif layer in reload_layers:
                logger.info(f"Reloading (truncate) [{layer}] {name}")
                # Complex Spark logic ported from notebook
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

                processor = DataProcessor(contract=contract_dict, engine=self.engine, pipeline_run_id=self.run_id)
                if not dry_run:
                    processor.reset(targets=["watermark", "run_log"])
                else:
                    processor.reset(targets=["watermark", "run_log"], dry_run=True)

    # ── Phase 2: DDL Only ────────────────────────────────────────────────────

    def generate_ddl_only(self, active_contracts: List[RegistryContract], dry_run: bool) -> PipelineRunSummary:
        """Create target tables from schemas without data loading."""
        summary = PipelineRunSummary(self.run_id, "ddl_only", dry_run)

        for c in active_contracts:
            try:
                processor = DataProcessor(contract=c.contract_dict, engine=self.engine, pipeline_run_id=self.run_id)
                if dry_run:
                    ddl = processor.generate_ddl(backend=self.engine)
                    logger.info(f"DRY RUN DDL Preview for {c.entity}:\n{ddl}")
                    summary.append(c.entity, c.layer, "ddl_dry_run")
                else:
                    processor.create_table(backend=self.engine)
                    logger.info(f"Table created for {c.entity}")
                    summary.append(c.entity, c.layer, "ddl_created")
            except Exception as e:
                logger.error(f"DDL failed for {c.entity}: {e}")
                summary.append(c.entity, c.layer, "ddl_failed", error=str(e))

        return summary

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
    ) -> PipelineRunSummary:
        """
        Execute the pipeline loop.
        """
        forget_values = forget_values or []
        forget_patient_ids = forget_patient_ids or []

        targets = [layer.strip().lower() for layer in target_layers.split(",") if layer.strip()]
        layer_order = ["bronze", "silver", "gold"]
        target_set = set(layer_order) if "all" in targets else set(targets)

        resets = {layer.strip().lower() for layer in reset_layers.split(",") if layer.strip()}
        reloads = {layer.strip().lower() for layer in reload_layers.split(",") if layer.strip()}
        entities = {entity.strip().lower() for entity in entity_filter.split(",") if entity.strip()}

        # Filter active contracts
        all_active = self.registry.get_active_contracts()
        if entities:
            all_active = [c for c in all_active if c.entity.lower() in entities]

        summary = PipelineRunSummary(self.run_id, "unknown", dry_run)

        # 1. Resets
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
            return self.generate_ddl_only(all_active, dry_run)

        # 4. Processing Loop
        layers_with_new_data = set()
        upstream_map = {"silver": "bronze", "gold": "silver"}

        for layer in layer_order:
            if layer not in target_set:
                continue

            layer_contracts = [c for c in all_active if c.layer == layer]
            if not layer_contracts:
                continue

            logger.info(f"── Processing Layer: {layer.upper()} ({len(layer_contracts)} contracts) ──")

            # Skip downstream if upstream had no new data entirely
            upstream = upstream_map.get(layer)
            if upstream and upstream not in layers_with_new_data and upstream in target_set:
                logger.info(f"Upstream '{upstream}' had no new data — skipping {layer}")
                for c in layer_contracts:
                    summary.append(c.entity, layer, "skipped_no_upstream")
                continue

            for c in layer_contracts:
                # Log contract + target for observability
                _mat = (c.contract_dict or {}).get("materialization", {})
                _target = _mat.get("location") or _mat.get("target_path") or "n/a"
                _title = (c.contract_dict or {}).get("info", {}).get("title", c.entity)
                logger.info("  ─────────────────────────────────────────────────────────")
                logger.info(f"  📄 [{layer}] {c.entity} | contract: {_title} | target: {_target}")

                if dry_run:
                    logger.info(f"DRY RUN - skipping {c.entity}")
                    summary.append(c.entity, layer, "dry_run")
                    continue

                try:
                    processor = DataProcessor(contract=c.contract_dict, engine=self.engine, pipeline_run_id=self.run_id)
                    result = processor.run_source(
                        reprocess_from=reprocess_from,
                        reprocess_to=reprocess_to,
                        reprocess_column=reprocess_column,
                        reprocess_values=reprocess_values,
                    )

                    df_good = result.good
                    is_empty = (
                        df_good is None
                        or (isinstance(df_good, list) and len(df_good) == 0)
                        or (hasattr(df_good, "is_empty") and df_good.is_empty())
                        or (hasattr(df_good, "__len__") and len(df_good) == 0)
                    )

                    if is_empty:
                        logger.info(f"No new rows for {c.entity} - incremental load is up to date.")
                        summary.append(c.entity, layer, "no_new_rows")
                        continue

                    # Spark compatibility layer mapping (avoid polars void type issues)
                    if self.engine == "spark" and not hasattr(df_good, "sparkSession"):
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

                        df_bad = result.bad
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
                    else:
                        df_bad = result.bad

                    row_count = (
                        len(df_good)
                        if hasattr(df_good, "__len__")
                        else (df_good.count() if hasattr(df_good, "count") else "?")
                    )
                    mat_result = processor.materialize(df_good, df_bad)

                    logger.info(f"✅ Materialized {row_count} rows for {c.entity} -> {mat_result}")
                    layers_with_new_data.add(layer)
                    summary.append(c.entity, layer, "success", rows=row_count)

                except Exception as e:
                    logger.error(f"❌ Failed to process {c.entity}: {e}")
                    summary.append(c.entity, layer, "failed", error=str(e))
                    raise  # Fail fast for orchestrator auto-retries

        return summary
