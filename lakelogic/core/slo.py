"""
SLO Validation engine for Lakehouse Domains.

Evaluates continuous out-of-band observability metrics across the
data mesh. Scans materialized files and run logs to validate:
- Freshness (max delay SLAs)
- Row volume constraints and historical anomaly detection
- Dataset quality severity bounds
- Schedule and duration execution metrics
"""

from __future__ import annotations

import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field
from loguru import logger

from lakelogic.core.paths import (
    enrich_azure_storage_options,
    make_table_name,
    resolve_materialization_path,
    resolve_run_log_ref,
    to_sql_table_ref,
)
from lakelogic.core.registry import DomainRegistry


def _read_delta_local(path: str, storage_options: Optional[dict] = None):
    """Read a Delta table. Delegates to the shared compatibility reader.

    This function used to carry its own workaround for the deltalake 0.17.x Schema
    incompatibility — and it fell back to ``pl.read_delta`` for CLOUD paths, so it
    stayed broken exactly where the data is biggest. The same breakage recurred at
    deltalake 1.x because the workaround lived here while four other modules called
    ``pl.read_delta`` directly. It is now one shared reader.
    """
    from lakelogic.core.delta_compat import read_delta

    return read_delta(path, storage_options=storage_options)


def _coerce_utc(ts: Any) -> datetime.datetime:
    """Coerce a timestamp-like value to a tz-aware **UTC** ``datetime``.

    Accepts a ``datetime``/pandas·polars ``Timestamp`` (tz-aware or naive) or an ISO-8601
    string. A **naive** value is assumed to already be UTC and is stamped as such — it is
    *never* localized to the host timezone. This is the load-bearing fix: the previous
    ``datetime.fromtimestamp(ts.timestamp(), tz=utc)`` path applied the machine's local
    offset to naive timestamps (polars strips tz on ``cast(pl.Datetime)``), so freshness /
    retention ages were wrong by the host's UTC offset on any non-UTC operator — green on
    UTC CI, silently off elsewhere.
    """
    if isinstance(ts, str):
        parsed = datetime.datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=datetime.timezone.utc)
    if getattr(ts, "tzinfo", None) is not None:
        return ts.astimezone(datetime.timezone.utc)
    return ts.replace(tzinfo=datetime.timezone.utc)


class SLOCheckResult(BaseModel):
    layer: str
    entity: str
    check_type: str = "freshness"  # freshness | row_count | quality | schedule | retention
    status: str
    passed: bool
    severity: str = "fail"  # "pass" | "warn" | "fail"
    latest_ts: Optional[str] = None
    delay_minutes: Optional[float] = None
    slo_max_minutes: Optional[int] = None
    row_count: Optional[int] = None
    slo_min_rows: Optional[int] = None
    slo_max_rows: Optional[int] = None
    # Source freshness (upstream data staleness)
    source_delay_minutes: Optional[float] = None
    source_slo_max_minutes: Optional[int] = None
    source_column_used: Optional[str] = None  # which source column was resolved
    source_passed: Optional[bool] = None
    # Anomaly detection
    anomaly_ratio: Optional[float] = None  # actual / baseline
    anomaly_baseline: Optional[float] = None  # median/avg of lookback
    # Quality
    quality_ratio: Optional[float] = None
    quality_severity: Optional[str] = None  # highest failing severity
    # Duration
    duration_seconds: Optional[float] = None


class SLOReport(BaseModel):
    domain: str
    system: str
    timestamp: str
    passed: bool
    check_run_id: str = ""
    pipeline_run_id: Optional[str] = None
    failures: List[SLOCheckResult] = Field(default_factory=list)
    results: List[SLOCheckResult] = Field(default_factory=list)


def _slo_covers(layer_slo, entity: str) -> bool:
    """Does this per-layer SLO config apply to ``entity``?

    Prefers the config's own ``covers()``; duck-typed doubles that predate it fall
    back to the exclusion list alone.
    """
    covers = getattr(layer_slo, "covers", None)
    if callable(covers):
        return covers(entity)
    from fnmatch import fnmatch

    include = list(getattr(layer_slo, "include_tables", None) or [])
    exclude = list(getattr(layer_slo, "exclude_tables", None) or [])
    if include and not any(entity == p or fnmatch(entity, p) for p in include):
        return False
    return not any(entity == p or fnmatch(entity, p) for p in exclude)


class SLOValidator:
    """
    Validates Data Contracts and Domain Registries against their defined Service Levels (SLOs).

    This operates continuously out-of-band from ingestion pipelines, evaluating:
    1. Freshness: By physically scanning Delta/Parquet file timestamps on storage.
    2. Data Volume: By checking row count min/max bounds and anomaly detection against run log baselines.
    3. Pipeline Health: By validating execution schedules and dataset quality quarantine ratios.
    """

    def __init__(
        self,
        registry: DomainRegistry,
        spark: Any = None,
        polars: bool = False,
        duckdb_con: Any = None,
        storage_options: dict = None,
    ):
        self.registry = registry
        self.spark = spark
        self.polars = polars
        self.duckdb_con = duckdb_con
        self._storage_options = storage_options

    def _resolve_storage_opts(self, path: str) -> dict:
        """Resolve storage options for Polars reads.

        If ``storage_options`` was passed to the constructor, enrich and
        return it directly.  Otherwise fall back to the automatic
        credential resolver from ``cloud_credentials``.
        """
        if self._storage_options is not None:
            return enrich_azure_storage_options(dict(self._storage_options))
        from lakelogic.engines.cloud_credentials import resolve_storage_options

        return enrich_azure_storage_options(resolve_storage_options(path))

    def check_freshness(self) -> List[SLOCheckResult]:
        """
        Check the freshness of all active contracts against the layer SLOs.
        """
        if not self.spark and not self.polars and not self.duckdb_con:
            logger.warning(
                "SLOValidator.check_freshness requires a Spark session, polars=True, or duckdb_con. Skipping."
            )
            return []

        now = datetime.datetime.now(datetime.timezone.utc)
        results = []
        logger.info(
            f"🔍 SLO Freshness Check: scanning {len(self.registry.get_active_contracts())} contracts in {self.registry.domain}/{self.registry.system}"  # noqa: E501
        )

        freshness_config = self.registry.slo.freshness
        storage = self.registry.storage

        layer_roots = {
            "bronze": storage.bronze_root,
            "silver": storage.silver_root,
            "gold": storage.gold_root,
        }

        # Validate all active contracts
        for reg_contract in self.registry.get_active_contracts():
            layer = reg_contract.layer
            entity = reg_contract.entity

            schema_root = layer_roots.get(layer)

            # ── Resolve table path via centralized paths module ──
            polars_path = resolve_materialization_path(
                contract=reg_contract,
                registry_storage=storage,
                layer=layer,
                system=self.registry.system,
                entity=entity,
            )

            if not self.polars and not self.duckdb_con and not schema_root and not polars_path:
                continue

            # Build engine-specific SQL table reference
            entity_table = make_table_name(layer, self.registry.system, entity)
            table_name = (
                f"{schema_root}.{entity_table}".replace("`", "")
                if schema_root
                else to_sql_table_ref(polars_path, "spark")
            )

            # Get the SLO rules for this specific layer
            layer_slo = freshness_config.get(layer)
            # `covers` honours include_tables/exclude_tables and matches patterns,
            # so a reference-data family (`dim_*`) is scoped with one entry. Falls
            # back to the plain exclusion test for duck-typed configs in tests.
            if layer_slo and not _slo_covers(layer_slo, entity):
                continue

            max_delay = layer_slo.max_delay_minutes if layer_slo else 999999

            # One ordered candidate list, first present wins. The previous code
            # appended "_lakelogic_loaded_at" unconditionally as "the standard audit
            # column" — but that name is only written by systems that configure it;
            # the framework default is "_lakelogic_processed_at", so the guaranteed
            # fallback was frequently a column that did not exist.
            check_cols = (
                list(layer_slo.check_columns)
                if layer_slo
                else [
                    "_lakelogic_processed_at",
                    "_lakelogic_loaded_at",
                ]
            )
            for audit in ("_lakelogic_processed_at", "_lakelogic_loaded_at"):
                if audit not in check_cols:
                    check_cols.append(audit)

            latest_ts = None
            found_col = None
            existing_col = None  # a column that exists but held no timestamp
            last_error = None

            for col in check_cols:
                try:
                    if self.spark:
                        row = self.spark.sql(f"SELECT MAX({col}) as latest_ts FROM {table_name}").first()
                        latest_ts = row["latest_ts"]
                    elif self.duckdb_con:
                        try:
                            result = self.duckdb_con.execute(
                                f"SELECT MAX({col}) as latest_ts FROM delta_scan('{polars_path}')"
                            ).fetchone()
                        except Exception:
                            result = self.duckdb_con.execute(
                                f"SELECT MAX({col}) as latest_ts FROM parquet_scan('{polars_path}')"
                            ).fetchone()
                        latest_ts = result[0] if result else None
                    else:
                        import polars as pl

                        storage_opts = self._resolve_storage_opts(polars_path)
                        try:
                            df = _read_delta_local(polars_path, storage_options=storage_opts)
                            latest_ts = df.select(pl.col(col).max()).item()
                        except Exception as delta_e:
                            try:
                                df = pl.read_parquet(polars_path, storage_options=storage_opts)
                                latest_ts = df.select(pl.col(col).max()).item()
                            except Exception as parquet_e:
                                raise Exception(
                                    f"read_delta failed: {str(delta_e)[:150]}... | read_parquet fallback failed: {str(parquet_e)[:150]}..."  # noqa: E501
                                ) from delta_e

                    # "First present wins" must mean the first column that yields a
                    # TIMESTAMP, not merely the first that queries without error. A
                    # column can exist and be entirely NULL (common on optional
                    # business timestamps); breaking there abandoned the remaining
                    # candidates and reported NO DATA on a table the audit column
                    # could have measured perfectly well.
                    if latest_ts is not None:
                        found_col = col
                        break
                    if existing_col is None:
                        existing_col = col  # exists but empty — keep looking
                    continue
                except Exception as e:
                    last_error = e
                    continue

            # Every candidate was empty, but at least one existed → NO DATA below,
            # which is a truthful "table has no timestamps yet" rather than an error.
            if found_col is None and existing_col is not None:
                found_col = existing_col

            if not found_col:
                # Table might not exist or none of the columns exist
                results.append(
                    SLOCheckResult(
                        layer=layer,
                        entity=entity,
                        status=f"⚠️ ERROR: {str(last_error)[:200]}",
                        passed=False,
                        slo_max_minutes=max_delay,
                    )
                )
                continue

            if latest_ts is None:
                results.append(
                    SLOCheckResult(
                        layer=layer,
                        entity=entity,
                        status="⚠️ NO DATA",
                        passed=False,
                        slo_max_minutes=max_delay,
                    )
                )
                continue

            try:
                # Calculate pipeline delay
                # Naive timestamps are assumed UTC (never host-localized); see _coerce_utc.
                latest_utc = _coerce_utc(latest_ts)

                delay = (now - latest_utc).total_seconds() / 60
                passed = delay <= max_delay

                # ── Source freshness ─────────────────────────────────────
                # Formerly a second check with its own column list and its own
                # threshold. Both resolved a timestamp on the SAME table from a
                # candidate list, so once the two lists merged into
                # `source_check_columns` this became the identical query with a
                # different limit — a duplicate full-table MAX() scan per table.
                # It is now the same measurement, reported under both names so the
                # result schema is unchanged.
                source_delay_min = round(delay, 1)
                source_col_used = found_col
                source_passed = passed

                overall_passed = passed

                status = "✅ OK" if overall_passed else "❌ STALE"

                results.append(
                    SLOCheckResult(
                        layer=layer,
                        entity=entity,
                        status=status,
                        passed=overall_passed,
                        latest_ts=str(latest_ts),
                        delay_minutes=round(delay, 1),
                        slo_max_minutes=max_delay,
                        source_delay_minutes=source_delay_min,
                        source_slo_max_minutes=max_delay,
                        source_column_used=source_col_used,
                        source_passed=source_passed,
                    )
                )

            except Exception as e:
                # Table might not exist or column might be missing
                results.append(
                    SLOCheckResult(
                        layer=layer,
                        entity=entity,
                        status=f"⚠️ ERROR: {str(e)[:200]}",
                        passed=False,
                        slo_max_minutes=max_delay,
                    )
                )

        # ── Summary logging ──────────────────────────────────────────────
        n_pass = sum(1 for r in results if r.passed)
        n_fail = sum(1 for r in results if not r.passed and "ERROR" not in r.status)
        n_err = sum(1 for r in results if "ERROR" in r.status)
        logger.info(
            f"📊 SLO Freshness Summary: {len(results)} checks | "
            f"✅ {n_pass} passed | ❌ {n_fail} failed | ⚠️ {n_err} errors"
        )
        for r in results:
            source_info = ""
            if r.source_column_used:
                source_info = (
                    f", source: {r.source_delay_minutes}min via "
                    f"'{r.source_column_used}' (SLO: {r.source_slo_max_minutes}min)"
                )
            if r.passed:
                logger.info(
                    f"   ✅ [{r.layer}] {r.entity}: {r.status} (delay: {r.delay_minutes}min, SLO: {r.slo_max_minutes}min{source_info})"  # noqa: E501
                )
            else:
                logger.warning(
                    f"   ❌ [{r.layer}] {r.entity}: {r.status} (delay: {r.delay_minutes}min{source_info})"  # noqa: E501
                )

        return results

    def check_row_counts(self) -> List[SLOCheckResult]:
        """
        Check the row counts of the most recent run log entry for each active
        contract against the per-layer thresholds defined in ``slo.row_count``.

        Reads from the run log table (no live COUNT queries) using the existing
        ``counts_good`` / ``counts_source`` / ``counts_total`` columns.
        """
        if not self.spark and not self.polars and not self.duckdb_con:
            logger.warning(
                "SLOValidator.check_row_counts requires a Spark session, polars=True, or duckdb_con. Skipping."
            )
            return []

        results = []
        row_count_config = self.registry.slo.row_count
        storage = self.registry.storage
        run_log_table = storage.run_log_table

        if not run_log_table:
            logger.warning("No run_log_table configured in storage; cannot check row counts.")
            return []

        # Strip backticks for Spark SQL compatibility
        run_log_table.replace("`", "")
        # Engine-specific SQL references via centralized paths module
        spark_table_ref = resolve_run_log_ref(run_log_table, "spark")
        duckdb_table_ref = resolve_run_log_ref(run_log_table, "duckdb")

        for contract in self.registry.get_active_contracts():
            layer = contract.layer
            entity = contract.entity

            layer_slo = row_count_config.get(layer)
            if not layer_slo:
                continue

            if not _slo_covers(layer_slo, entity):
                continue

            min_rows = layer_slo.min_rows
            max_rows = layer_slo.max_rows
            check_field = layer_slo.check_field or "counts_good"

            if min_rows is None and max_rows is None:
                continue

            try:
                if self.spark:
                    row = self.spark.sql(f"""
                        SELECT {check_field}, timestamp
                        FROM {spark_table_ref}
                        WHERE data_layer = '{layer}'
                          AND dataset = '{entity}'
                          AND stage NOT IN ('no_new_data', 'reprocess')
                        ORDER BY timestamp DESC
                        LIMIT 1
                    """).first()
                elif self.duckdb_con:
                    result = self.duckdb_con.execute(f"""
                        SELECT {check_field}, timestamp
                        FROM {duckdb_table_ref}
                        WHERE data_layer = '{layer}'
                          AND dataset = '{entity}'
                          AND stage NOT IN ('no_new_data', 'reprocess')
                        ORDER BY timestamp DESC
                        LIMIT 1
                    """).fetchone()
                    if result:
                        row = {check_field: result[0], "timestamp": result[1]}
                    else:
                        row = None
                else:
                    import polars as pl

                    storage_opts = self._resolve_storage_opts(run_log_table)
                    try:
                        df = _read_delta_local(run_log_table, storage_options=storage_opts)
                    except Exception as delta_e:
                        try:
                            df = pl.read_parquet(run_log_table, storage_options=storage_opts)
                        except Exception as parquet_e:
                            raise Exception(
                                f"read_delta failed: {str(delta_e)[:150]}... | read_parquet fallback failed: {str(parquet_e)[:150]}..."  # noqa: E501
                            ) from delta_e

                    filtered = (
                        df.filter(
                            (pl.col("data_layer") == layer)
                            & (pl.col("dataset") == entity)
                            & (~pl.col("stage").is_in(["no_new_data", "reprocess"]))
                        )
                        .sort("timestamp", descending=True)
                        .head(1)
                    )

                    if not filtered.is_empty():
                        row_dict = filtered.to_dicts()[0]
                        row = {check_field: row_dict.get(check_field), "timestamp": row_dict.get("timestamp")}
                    else:
                        row = None

            except Exception as e:
                results.append(
                    SLOCheckResult(
                        layer=layer,
                        entity=entity,
                        status=f"⚠️ ERROR: {str(e)[:200]}",
                        passed=False,
                        slo_min_rows=min_rows,
                        slo_max_rows=max_rows,
                    )
                )
                continue

            if row is None or row[check_field] is None:
                results.append(
                    SLOCheckResult(
                        layer=layer,
                        entity=entity,
                        status="⚠️ NO DATA",
                        passed=False,
                        slo_min_rows=min_rows,
                        slo_max_rows=max_rows,
                    )
                )
                continue

            actual_count = int(row[check_field])
            passed = True
            status_parts = []

            if min_rows is not None and actual_count < min_rows:
                passed = False
                status_parts.append(f"❌ TOO FEW ROWS ({actual_count} < {min_rows})")

            if max_rows is not None and actual_count > max_rows:
                passed = False
                status_parts.append(f"❌ TOO MANY ROWS ({actual_count} > {max_rows})")

            if passed:
                status = f"✅ OK ({actual_count} rows)"
            else:
                status = "; ".join(status_parts)

            results.append(
                SLOCheckResult(
                    layer=layer,
                    entity=entity,
                    status=status,
                    passed=passed,
                    row_count=actual_count,
                    slo_min_rows=min_rows,
                    slo_max_rows=max_rows,
                    latest_ts=str(row["timestamp"]) if row["timestamp"] else None,
                )
            )

        return results

    def check_schedule(self, environment: Optional[str] = None) -> List[SLOCheckResult]:
        """
        Check schedule SLOs: completion deadline, start-time, and duration.

        Respects ``schedule.environments`` — if the current environment is not
        in the list, checks are skipped.
        """
        results = []
        now = datetime.datetime.now(datetime.timezone.utc)
        schedule = self.registry.slo.schedule

        if not schedule:
            return results

        # Environment filter
        if schedule.environments and environment:
            if environment not in schedule.environments:
                return results

        # Completion deadline
        if schedule.expected_completion_utc:
            try:
                expected_hour, expected_min = map(int, schedule.expected_completion_utc.split(":"))
                deadline = now.replace(hour=expected_hour, minute=expected_min, second=0, microsecond=0)

                if now <= deadline:
                    results.append(
                        SLOCheckResult(
                            layer="schedule",
                            entity="pipeline",
                            check_type="schedule",
                            status="✅ ON TIME",
                            passed=True,
                            severity="pass",
                            delay_minutes=round((now - deadline).total_seconds() / 60, 1),
                        )
                    )
                else:
                    results.append(
                        SLOCheckResult(
                            layer="schedule",
                            entity="pipeline",
                            check_type="schedule",
                            status=f"❌ LATE by {(now - deadline).total_seconds() / 60:.0f} min",
                            passed=False,
                            severity="fail",
                            delay_minutes=round((now - deadline).total_seconds() / 60, 1),
                        )
                    )
            except Exception as e:
                logger.error(f"Failed to parse schedule SLO: {e}")

        # Start-time check
        if schedule.expected_start_utc:
            try:
                sh, sm = map(int, schedule.expected_start_utc.split(":"))
                start_deadline = now.replace(hour=sh, minute=sm, second=0, microsecond=0)
                if now > start_deadline:
                    # Pipeline should have started by now — check via run log
                    results.append(
                        SLOCheckResult(
                            layer="schedule",
                            entity="pipeline_start",
                            check_type="schedule",
                            status="⚠️ Check if pipeline has started",
                            passed=True,
                            severity="warn",
                        )
                    )
            except Exception:
                pass

        return results

    def check_quality(self, report: Optional[Dict[str, Any]] = None) -> List[SLOCheckResult]:
        """
        Evaluate quality SLOs using quarantine ratios and severity-weighted thresholds.

        Two evaluation paths share the same gate (:meth:`_evaluate_quality_counts`):

        * **Post-pipeline** — when a live ``report`` is supplied, counts come from
          that run (and severity-weighted checks are available).
        * **Out-of-band** — scheduled/independent runs (``report is None``) read the
          latest run-log counts per active contract, so a materialization that
          quarantined every row is still caught instead of silently showing green.
        """
        quality = self.registry.slo.quality

        if report:
            counts = report.get("counts", {}) or {}
            results = self._evaluate_quality_counts("pipeline", counts, quality)

            # Severity-weighted checks (only available with a live report).
            total = counts.get("total") or counts.get("source") or 0
            if quality and quality.by_severity and total > 0 and report.get("rule_failures_by_severity"):
                failures_by_severity = report["rule_failures_by_severity"]
                for sev in ["critical", "high", "medium", "low"]:
                    threshold = quality.by_severity.get(sev)
                    if not threshold:
                        continue
                    sev_failures = failures_by_severity.get(sev, 0)
                    sev_good_ratio = (total - sev_failures) / total if total > 0 else 1.0
                    if sev_good_ratio < threshold.min_good_ratio:
                        results.append(
                            SLOCheckResult(
                                layer="quality",
                                entity=f"severity_{sev}",
                                check_type="quality",
                                status=(
                                    f"❌ {sev.upper()} quality breach "
                                    f"({sev_good_ratio:.1%} < {threshold.min_good_ratio:.1%})"
                                ),
                                passed=False,
                                severity="fail" if sev in ("critical", "high") else "warn",
                                quality_ratio=round(sev_good_ratio, 4),
                                quality_severity=sev,
                            )
                        )
            return results

        # Out-of-band scan: no live report — read the latest run-log counts so
        # scheduled SLO evaluations still gate on quarantine.
        return self._check_quality_from_run_log(quality)

    @staticmethod
    def _evaluate_quality_counts(entity: str, counts: Dict[str, Any], quality: Any) -> List[SLOCheckResult]:
        """Gate a single run's counts against the quality SLO.

        Shared by the post-pipeline and out-of-band paths so both behave
        identically. Rules:

        * ``total == 0`` → no result (benign ``no_new_data`` tick; prolonged
          absence is the freshness SLO's job, not the quality gate).
        * ``good == 0`` while ``total > 0`` → total data loss. **Fails** when a
          quality SLO is configured (the domain opted into gating); otherwise a
          non-blocking **warn** so the loss is still visible without failing a
          domain that deliberately left ``slo.quality`` unset.
        * otherwise → threshold gate, but only when a quality SLO is configured.
        """
        results: List[SLOCheckResult] = []
        total = counts.get("total") or counts.get("source") or 0
        good = counts.get("good") or 0
        quarantined = counts.get("quarantined") or 0

        if total == 0:
            return results

        good_ratio = good / total
        quarantine_ratio = quarantined / total

        if good == 0:
            # Total data loss. Respect the domain's opt-in: hard-fail only when a
            # quality SLO is configured; otherwise surface a non-blocking warning
            # (passed=True, severity="warn") so it's visible but doesn't fail a
            # domain that never opted into quality gating.
            if quality:
                results.append(
                    SLOCheckResult(
                        layer="quality",
                        entity=entity,
                        check_type="quality",
                        status=f"❌ ALL ROWS QUARANTINED ({quarantined} of {total} blocked, 0 materialized)",
                        passed=False,
                        severity="fail",
                        quality_ratio=0.0,
                    )
                )
            else:
                results.append(
                    SLOCheckResult(
                        layer="quality",
                        entity=entity,
                        check_type="quality",
                        status=(
                            f"⚠️ ALL ROWS QUARANTINED ({quarantined} of {total} blocked, "
                            f"0 materialized) — no quality SLO set"
                        ),
                        passed=True,
                        severity="warn",
                        quality_ratio=0.0,
                    )
                )
            return results

        if quality:
            passed = good_ratio >= quality.min_good_ratio and quarantine_ratio <= quality.max_quarantine_ratio
            status = (
                f"✅ OK (good={good_ratio:.1%})"
                if passed
                else f"❌ QUALITY ({good_ratio:.1%} good, {quarantine_ratio:.1%} quarantined)"
            )
            results.append(
                SLOCheckResult(
                    layer="quality",
                    entity=entity,
                    check_type="quality",
                    status=status,
                    passed=passed,
                    severity="pass" if passed else "fail",
                    quality_ratio=round(good_ratio, 4),
                )
            )
        return results

    def _check_quality_from_run_log(self, quality: Any) -> List[SLOCheckResult]:
        """Evaluate quality for scheduled/out-of-band runs from the run log.

        Reads the latest non-``no_new_data`` run-log row per active contract and
        feeds its counts through :meth:`_evaluate_quality_counts`. Requires an
        engine and a configured ``run_log_table``; returns ``[]`` otherwise.
        """
        results: List[SLOCheckResult] = []
        if not self.spark and not self.polars and not self.duckdb_con:
            return results

        storage = self.registry.storage
        run_log_table = storage.run_log_table
        if not run_log_table:
            return results

        run_log_table.replace("`", "")
        spark_ref = resolve_run_log_ref(run_log_table, "spark")
        duckdb_ref = resolve_run_log_ref(run_log_table, "duckdb")

        for contract in self.registry.get_active_contracts():
            layer = contract.layer
            entity = contract.entity
            counts: Optional[Dict[str, Any]] = None
            try:
                if self.spark:
                    row = self.spark.sql(f"""
                        SELECT counts_source, counts_total, counts_good, counts_quarantined
                        FROM {spark_ref}
                        WHERE data_layer = '{layer}'
                          AND dataset = '{entity}'
                          AND stage NOT IN ('no_new_data', 'reprocess')
                        ORDER BY timestamp DESC
                        LIMIT 1
                    """).first()
                    if row:
                        counts = {
                            "source": row["counts_source"],
                            "total": row["counts_total"],
                            "good": row["counts_good"],
                            "quarantined": row["counts_quarantined"],
                        }
                elif self.duckdb_con:
                    res = self.duckdb_con.execute(f"""
                        SELECT counts_source, counts_total, counts_good, counts_quarantined
                        FROM {duckdb_ref}
                        WHERE data_layer = '{layer}'
                          AND dataset = '{entity}'
                          AND stage NOT IN ('no_new_data', 'reprocess')
                        ORDER BY timestamp DESC
                        LIMIT 1
                    """).fetchone()
                    if res:
                        counts = {"source": res[0], "total": res[1], "good": res[2], "quarantined": res[3]}
                else:
                    import polars as pl

                    storage_opts = self._resolve_storage_opts(run_log_table)
                    try:
                        df = _read_delta_local(run_log_table, storage_options=storage_opts)
                    except Exception:
                        df = pl.read_parquet(run_log_table, storage_options=storage_opts)
                    filtered = (
                        df.filter(
                            (pl.col("data_layer") == layer)
                            & (pl.col("dataset") == entity)
                            & (~pl.col("stage").is_in(["no_new_data", "reprocess"]))
                        )
                        .sort("timestamp", descending=True)
                        .head(1)
                    )
                    if not filtered.is_empty():
                        d = filtered.to_dicts()[0]
                        counts = {
                            "source": d.get("counts_source"),
                            "total": d.get("counts_total"),
                            "good": d.get("counts_good"),
                            "quarantined": d.get("counts_quarantined"),
                        }
            except Exception as e:
                logger.debug(f"Quality run-log read failed for {entity}: {e}")
                continue

            if counts is not None:
                results.extend(self._evaluate_quality_counts(entity, counts, quality))

        return results

    def check_row_count_anomaly(
        self, entity: str, layer: str, actual_count: int, anomaly_cfg
    ) -> Optional[SLOCheckResult]:
        """
        Compare actual row count against historical baseline from run logs.
        """
        if not anomaly_cfg or not anomaly_cfg.enabled:
            return None

        if not self.spark and not self.polars and not self.duckdb_con:
            return None

        storage = self.registry.storage
        run_log_table = storage.run_log_table
        if not run_log_table:
            return None

        run_log_table.replace("`", "")

        try:
            check_field_name = anomaly_cfg.check_field if hasattr(anomaly_cfg, "check_field") else "counts_good"
            spark_ref = resolve_run_log_ref(run_log_table, "spark")
            duckdb_ref = resolve_run_log_ref(run_log_table, "duckdb")
            if self.spark:
                rows = self.spark.sql(f"""
                    SELECT {check_field_name} as cnt
                    FROM {spark_ref}
                    WHERE data_layer = '{layer}'
                      AND dataset = '{entity}'
                      AND stage NOT IN ('no_new_data', 'reprocess')
                    ORDER BY timestamp DESC
                    LIMIT {anomaly_cfg.lookback_runs}
                """).collect()
            elif self.duckdb_con:
                duckdb_rows = self.duckdb_con.execute(f"""
                    SELECT {check_field_name} as cnt
                    FROM {duckdb_ref}
                    WHERE data_layer = '{layer}'
                      AND dataset = '{entity}'
                      AND stage NOT IN ('no_new_data', 'reprocess')
                    ORDER BY timestamp DESC
                    LIMIT {anomaly_cfg.lookback_runs}
                """).fetchall()
                rows = [{"cnt": r[0]} for r in duckdb_rows]
            else:
                import polars as pl

                storage_opts = self._resolve_storage_opts(run_log_table)
                try:
                    df = _read_delta_local(run_log_table, storage_options=storage_opts)
                except Exception as delta_e:
                    try:
                        df = pl.read_parquet(run_log_table, storage_options=storage_opts)
                    except Exception as parquet_e:
                        raise Exception(
                            f"read_delta failed: {str(delta_e)[:150]}... | read_parquet fallback failed: {str(parquet_e)[:150]}..."  # noqa: E501
                        ) from delta_e

                filtered = (
                    df.filter(
                        (pl.col("data_layer") == layer)
                        & (pl.col("dataset") == entity)
                        & (~pl.col("stage").is_in(["no_new_data", "reprocess"]))
                    )
                    .sort("timestamp", descending=True)
                    .head(anomaly_cfg.lookback_runs)
                )

                rows = [{"cnt": r.get(check_field_name)} for r in filtered.to_dicts()]

        except Exception as e:
            logger.debug(f"Anomaly check query failed for {entity}: {e}")
            return None

        historical = [r["cnt"] for r in rows if r["cnt"] is not None]

        if len(historical) < anomaly_cfg.min_runs_before_enforcement:
            logger.debug(
                f"Anomaly check skipped for {entity}: only {len(historical)} "
                f"runs (need {anomaly_cfg.min_runs_before_enforcement})"
            )
            return None

        # Compute baseline
        if anomaly_cfg.method == "median":
            sorted_h = sorted(historical)
            mid = len(sorted_h) // 2
            baseline = sorted_h[mid] if len(sorted_h) % 2 == 1 else (sorted_h[mid - 1] + sorted_h[mid]) / 2
        else:  # rolling_average
            baseline = sum(historical) / len(historical)

        if baseline == 0:
            return None

        ratio = actual_count / baseline
        passed = anomaly_cfg.min_ratio <= ratio <= anomaly_cfg.max_ratio

        if passed:
            status = f"✅ OK (ratio={ratio:.2f}x vs {anomaly_cfg.method})"
        elif ratio < anomaly_cfg.min_ratio:
            status = f"❌ VOLUME DROP ({ratio:.2f}x < {anomaly_cfg.min_ratio}x baseline)"
        else:
            status = f"❌ VOLUME SPIKE ({ratio:.2f}x > {anomaly_cfg.max_ratio}x baseline)"

        return SLOCheckResult(
            layer=layer,
            entity=entity,
            check_type="row_count",
            status=status,
            passed=passed,
            severity="pass" if passed else "warn",
            row_count=actual_count,
            anomaly_ratio=round(ratio, 4),
            anomaly_baseline=round(baseline, 1),
        )

    def check_retention(self) -> List[SLOCheckResult]:
        """
        Check that no table contains records older than its layer's retention period.

        Retention is a MIN-timestamp check: queries MIN(check_columns[first present]) and
        fails if the oldest record exceeds the ISO 8601 retention window defined in
        registry.retention (e.g. bronze: P7D, silver: P90D, gold: P7Y).

        This is distinct from freshness (MAX check) — it detects data that should
        have been purged but is still present in the table.
        """
        from lakelogic.core.registry import _iso_period_to_minutes

        if not self.registry.retention:
            return []
        if not self.spark and not self.polars and not self.duckdb_con:
            logger.warning(
                "SLOValidator.check_retention requires a Spark session, polars=True, or duckdb_con. Skipping."
            )
            return []

        now = datetime.datetime.now(datetime.timezone.utc)
        results = []
        storage = self.registry.storage
        freshness_config = self.registry.slo.freshness

        layer_roots = {
            "bronze": storage.bronze_root,
            "silver": storage.silver_root,
            "gold": storage.gold_root,
        }

        for reg_contract in self.registry.get_active_contracts():
            layer = reg_contract.layer
            entity = reg_contract.entity

            iso_period = self.registry.retention.get(layer)
            if not iso_period:
                continue

            retention_minutes = _iso_period_to_minutes(iso_period)
            if not retention_minutes:
                logger.warning(f"  ⚠ Retention [{layer}]: could not parse period '{iso_period}' — skipping {entity}")
                continue

            # Source columns to probe — prefer freshness SLO config, fall back to common names
            layer_slo = freshness_config.get(layer)
            source_cols = list(layer_slo.check_columns) if layer_slo else []
            if not source_cols:
                logger.debug(f"  ⏭ Retention [{layer}] {entity}: no check_columns configured — skipped")
                continue

            schema_root = layer_roots.get(layer)
            polars_path = resolve_materialization_path(
                contract=reg_contract,
                registry_storage=storage,
                layer=layer,
                system=self.registry.system,
                entity=entity,
            )

            entity_table = make_table_name(layer, self.registry.system, entity)
            table_name = (
                f"{schema_root}.{entity_table}".replace("`", "")
                if schema_root
                else to_sql_table_ref(polars_path, "spark")
            )

            min_ts = None
            col_used = None

            for src_col in source_cols:
                try:
                    if self.spark:
                        row = self.spark.sql(
                            f"SELECT MIN(TRY_CAST({src_col} AS TIMESTAMP)) AS min_ts FROM {table_name}"
                        ).first()
                        min_ts = row["min_ts"] if row else None
                    elif self.duckdb_con:
                        try:
                            res = self.duckdb_con.execute(
                                f"SELECT MIN(TRY_CAST({src_col} AS TIMESTAMP)) AS min_ts "
                                f"FROM delta_scan('{polars_path}')"
                            ).fetchone()
                        except Exception:
                            res = self.duckdb_con.execute(
                                f"SELECT MIN(TRY_CAST({src_col} AS TIMESTAMP)) AS min_ts "
                                f"FROM parquet_scan('{polars_path}')"
                            ).fetchone()
                        min_ts = res[0] if res else None
                    else:
                        import polars as pl

                        storage_opts = self._resolve_storage_opts(polars_path)
                        try:
                            src_df = _read_delta_local(polars_path, storage_options=storage_opts)
                        except Exception:
                            src_df = pl.read_parquet(polars_path, storage_options=storage_opts)
                        try:
                            min_ts = src_df.select(pl.col(src_col).cast(pl.Datetime, strict=False).min()).item()
                        except Exception:
                            min_ts = None

                    if min_ts is not None:
                        col_used = src_col
                        break
                except Exception:
                    continue

            if min_ts is None:
                logger.debug(f"  ⏭ Retention [{layer}] {entity}: no valid timestamp found in {source_cols} — skipped")
                continue

            # Naive timestamps are assumed UTC (never host-localized); see _coerce_utc.
            min_utc = _coerce_utc(min_ts)

            age_minutes = round((now - min_utc).total_seconds() / 60, 1)
            passed = age_minutes <= retention_minutes

            status = (
                f"✅ OK (oldest record {age_minutes:.0f}min, limit {retention_minutes}min via '{col_used}')"
                if passed
                else (
                    f"? RETENTION BREACH: oldest record {age_minutes:.0f}min old exceeds "
                    f"{iso_period} ({retention_minutes}min) via '{col_used}'"
                )
            )
            logger.debug(f"   🗄 Retention [{layer}] {entity}: {status}")

            results.append(
                SLOCheckResult(
                    layer=layer,
                    entity=entity,
                    check_type="retention",
                    status=status,
                    passed=passed,
                    severity="pass" if passed else "fail",
                    source_delay_minutes=age_minutes,
                    source_slo_max_minutes=retention_minutes,
                    source_column_used=col_used,
                    source_passed=passed,
                )
            )

        return results

    def run_checks(
        self,
        environment: Optional[str] = None,
        report: Optional[Dict[str, Any]] = None,
        pipeline_run_id: Optional[str] = None,
    ) -> SLOReport:
        """
        Run all configured SLO checks and return a unified report.

        Args:
            environment:     Target environment name (dev/staging/prod).
            report:          Pipeline run report dict — enables quality checks
                             when called post-pipeline. None when running
                             independently (quality checks read from run_log instead).
            pipeline_run_id: Optional FK to run_log. Populated when triggered
                             post-pipeline; None for scheduled independent runs.
        """
        from uuid import uuid4

        check_run_id = str(uuid4())
        now = datetime.datetime.now(datetime.timezone.utc)
        results = []

        if self.registry.slo.freshness:
            results.extend(self.check_freshness())

        if self.registry.slo.row_count:
            results.extend(self.check_row_counts())

        if self.registry.retention:
            results.extend(self.check_retention())

        schedule_results = self.check_schedule(environment=environment)
        results.extend(schedule_results)

        quality_results = self.check_quality(report=report)
        results.extend(quality_results)

        failures = [r for r in results if not r.passed]

        # Write results to _slo_checks table — non-blocking, never raises
        try:
            from lakelogic.core.run_log import write_slo_checks

            write_slo_checks(self.registry, results, check_run_id, pipeline_run_id)
        except Exception as exc:
            logger.warning(f"SLO checks write failed (results still returned): {exc}")

        return SLOReport(
            domain=self.registry.domain,
            system=self.registry.system,
            timestamp=now.isoformat(),
            passed=len(failures) == 0,
            check_run_id=check_run_id,
            pipeline_run_id=pipeline_run_id,
            failures=failures,
            results=results,
        )

    def notify_breaches(self, breaches: List[SLOCheckResult]) -> None:
        """
        Send notifications to domain owners for all failed SLO checks via Apprise.
        Extracts webhooks from the global notifications block in the Domain Registry
        that have explicitly subscribed to the 'slo_breach' event.
        """
        failures = [b for b in breaches if not b.passed]
        if not failures:
            return

        try:
            import apprise

            apobj = apprise.Apprise()
        except ImportError:
            logger.warning("apprise is not installed. Skipping SLO Slack notifications. (pip install apprise)")
            return

        # Load webhooks from registry notifications block
        notifications = getattr(self.registry, "notifications", [])
        channel_count = 0

        for cfg in notifications:
            target = cfg.get("target")
            events = [e.lower() for e in cfg.get("on_events", [])]
            if target and "slo_breach" in events:
                apobj.add(target)
                channel_count += 1

        # Also load from legacy / metadata domain ownership contacts
        contacts = self.registry.ownership.get("contacts", []) if self.registry.ownership else []
        for contact in contacts:
            slack_url = contact.get("slack")
            if slack_url:
                apobj.add(slack_url)
                channel_count += 1

            teams_url = contact.get("teams")
            if teams_url:
                apobj.add(teams_url)
                channel_count += 1

            webhook_url = contact.get("webhook")
            if webhook_url:
                apobj.add(webhook_url)
                channel_count += 1

            email = contact.get("email")
            if email:
                if "://" in email:  # Ensure it is an apprise URI (mailto://)
                    apobj.add(email)
                    channel_count += 1
                else:
                    import os

                    global_smtp = os.getenv("LAKELOGIC_SMTP_URI")
                    if global_smtp:
                        # Append the target email as a path object. Apprise natively standardizes
                        # this structure for both mailto:// and sendgrid:// and other APIs!
                        base_url = global_smtp.rstrip("/")
                        apobj.add(f"{base_url}/{email}")
                        channel_count += 1
                    else:
                        logger.warning(
                            f"Email contact '{email}' is not a valid Apprise URI. Emails require full SMTP configuration schema (e.g. mailto://...) or the LAKELOGIC_SMTP_URI environment variable. Skipping this target."  # noqa: E501
                        )

        if channel_count == 0:
            logger.debug("No valid notification targets found or subscribed to 'slo_breach'. SLO alerts skipped.")
            return

        msg = f"LakeLogic SLO Alert: {len(failures)} SLA breaches detected in the '{self.registry.domain}' domain.\n\n"
        for f in failures:
            msg += f"- {f.layer}.{f.entity}: {f.status} (Type: {f.check_type})\n"

        apobj.notify(body=msg, title=f"LakeLogic Domain SLO Breach ({self.registry.domain})")
        logger.info(f"Dispatched SLO alerts to {channel_count} channels via Apprise.")


# ── Standalone helpers (used by DataProcessor.run) ───────────────────────────


def _parse_duration_seconds(value: Any) -> Optional[float]:
    """Parse a duration string (e.g. '24h', '30m') into seconds.

    Numeric values are treated as *hours* for backward compatibility.
    Returns ``None`` for ``None`` input.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value) * 3600.0  # treat raw numbers as hours
    s = str(value).strip().lower()
    if s.endswith("h"):
        return float(s[:-1]) * 3600.0
    if s.endswith("m"):
        return float(s[:-1]) * 60.0
    if s.endswith("s"):
        return float(s[:-1])
    if s.endswith("d"):
        return float(s[:-1]) * 86400.0
    # Fallback: treat as hours
    try:
        return float(s) * 3600.0
    except ValueError:
        return None


def _coerce_datetime(value: Any) -> Optional[datetime.datetime]:
    """Coerce a value to a timezone-aware ``datetime``.

    Returns ``None`` for ``None`` or unparsable input.
    """
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=datetime.timezone.utc)
        return value
    try:
        s = str(value).strip()
        dt = datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=datetime.timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def _get_max_timestamp(df: Any, field: str, engine_name: str) -> Optional[datetime.datetime]:
    """Get the maximum timestamp value from a dataframe column."""
    try:
        if engine_name == "polars":
            import polars as pl

            if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
                if isinstance(df, pl.LazyFrame):
                    df = df.collect()
                if field not in df.columns or df.is_empty():
                    return None
                val = df.select(pl.col(field).max()).item()
                return _coerce_datetime(val)
        if engine_name == "duckdb":
            import duckdb

            if isinstance(df, duckdb.DuckDBPyRelation):
                result = df.aggregate(f"MAX({field})").fetchone()
                return _coerce_datetime(result[0]) if result else None
    except Exception as e:
        logger.debug(f"_get_max_timestamp failed: {e}")
    return None


def _non_null_ratio(df: Any, field: str, engine_name: str) -> Optional[float]:
    """Compute the ratio of non-null values for a given column."""
    try:
        if engine_name == "polars":
            import polars as pl

            if isinstance(df, (pl.DataFrame, pl.LazyFrame)):
                if isinstance(df, pl.LazyFrame):
                    df = df.collect()
                if field not in df.columns or df.is_empty():
                    return None
                total = len(df)
                non_null = total - df.select(pl.col(field).null_count()).item()
                return non_null / total if total > 0 else None
        if engine_name == "duckdb":
            import duckdb

            if isinstance(df, duckdb.DuckDBPyRelation):
                result = df.aggregate(f"COUNT({field}), COUNT(*)").fetchone()
                if result and result[1] > 0:
                    return result[0] / result[1]
                return None
    except Exception as e:
        logger.debug(f"_non_null_ratio failed: {e}")
    return None


def _compute_freshness(good_df: Any, freshness_obj: Any, engine_name: str) -> Dict[str, Any]:
    """Evaluate freshness SLO for a single contract run."""
    if freshness_obj is None:
        return {}
    field = freshness_obj.get("field") if isinstance(freshness_obj, dict) else getattr(freshness_obj, "field", None)
    threshold = (
        freshness_obj.get("threshold") if isinstance(freshness_obj, dict) else getattr(freshness_obj, "threshold", None)
    )

    if not field:
        return {}

    max_ts = _get_max_timestamp(good_df, field, engine_name)
    threshold_secs = _parse_duration_seconds(threshold)
    if max_ts is None or threshold_secs is None:
        return {"field": field, "passed": False, "reason": "no_data_or_threshold"}

    now = datetime.datetime.now(datetime.timezone.utc)
    delay_secs = (now - max_ts).total_seconds()
    passed = delay_secs <= threshold_secs

    result = {
        "field": field,
        "threshold": str(threshold),
        "delay_seconds": round(delay_secs, 1),
        "passed": passed,
    }

    # Source-time freshness (if configured at contract level)
    source_field = (
        freshness_obj.get("source_field")
        if isinstance(freshness_obj, dict)
        else getattr(freshness_obj, "source_field", None)
    )
    source_threshold = (
        freshness_obj.get("source_threshold")
        if isinstance(freshness_obj, dict)
        else getattr(freshness_obj, "source_threshold", None)
    )
    if source_field:
        source_ts = _get_max_timestamp(good_df, source_field, engine_name)
        source_threshold_secs = _parse_duration_seconds(source_threshold)
        if source_ts is not None and source_threshold_secs is not None:
            source_delay = (now - source_ts).total_seconds()
            result["source_age_seconds"] = round(source_delay, 1)
            result["source_passed"] = source_delay <= source_threshold_secs

    return result


def _compute_availability(
    good_df: Any,
    counts: Dict[str, Optional[int]],
    availability_obj: Any,
    engine_name: str,
) -> Dict[str, Any]:
    """Evaluate availability SLO for a single contract run."""
    if availability_obj is None:
        return {}
    field = (
        availability_obj.get("field")
        if isinstance(availability_obj, dict)
        else getattr(availability_obj, "field", None)
    )
    threshold = (
        availability_obj.get("threshold")
        if isinstance(availability_obj, dict)
        else getattr(availability_obj, "threshold", None)
    )

    if not field or threshold is None:
        return {}

    ratio = _non_null_ratio(good_df, field, engine_name)
    if ratio is None:
        return {"field": field, "passed": False, "reason": "no_data"}

    pct = ratio * 100.0
    passed = pct >= float(threshold)

    return {
        "field": field,
        "threshold": float(threshold),
        "actual_pct": round(pct, 2),
        "passed": passed,
    }


def _merge_slo_config(system_slo: Optional[Dict[str, Any]], contract_slo: Any) -> Dict[str, Any]:
    """Merge contract-level SLOs over system-level defaults (deep merge).

    Contract-level values take precedence. If a contract doesn't define
    a specific SLO section, the system default applies.
    """
    merged: Dict[str, Any] = {}
    if system_slo:
        merged.update(system_slo)
    if contract_slo is None:
        return merged
    contract_dict = (
        contract_slo
        if isinstance(contract_slo, dict)
        else (contract_slo.model_dump() if hasattr(contract_slo, "model_dump") else {})
    )
    for k, v in contract_dict.items():
        if v is None:
            continue
        if isinstance(v, dict) and k in merged and isinstance(merged[k], dict):
            merged[k] = {**merged[k], **v}
        else:
            merged[k] = v
    return merged


def compute_slos(
    contract: Any,
    good_df: Any,
    counts: Dict[str, Optional[int]],
    engine_name: str,
    registry_slo: Optional[Any] = None,
) -> Dict[str, Any]:
    """Compute per-contract SLO scores (freshness + availability + quality).

    This is the lightweight, per-run variant used by ``DataProcessor.run()``.
    For domain-wide SLO checks, use :class:`SLOValidator`.

    Parameters
    ----------
    contract : DataContract
        The contract being processed.
    good_df : DataFrame
        The good (non-quarantined) output dataframe.
    counts : dict
        Row counts dict (source, good, quarantined, total).
    engine_name : str
        Engine name (polars, spark, duckdb).
    registry_slo : RegistrySLO, optional
        System-level SLO config from the registry. Used as defaults when
        the contract doesn't define its own ``service_levels``.
    """
    contract_slo = getattr(contract, "service_levels", None)

    # Merge contract-level overrides with system-level defaults
    system_slo_dict = None
    if registry_slo:
        system_slo_dict = registry_slo.model_dump() if hasattr(registry_slo, "model_dump") else {}
    slo_cfg = _merge_slo_config(system_slo_dict, contract_slo)

    if not slo_cfg:
        return {}

    result: Dict[str, Any] = {}

    freshness = slo_cfg.get("freshness")
    if freshness:
        result["freshness"] = _compute_freshness(good_df, freshness, engine_name)

    availability = slo_cfg.get("availability")
    if availability:
        result["availability"] = _compute_availability(good_df, counts, availability, engine_name)

    return result
