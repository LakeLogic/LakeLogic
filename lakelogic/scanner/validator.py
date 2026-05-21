"""
ScannerValidator — runs metadata-based SLO checks against discovered tables.

Lighter than SLOValidator: no DomainRegistry, no contracts. Takes a
ScannerConfig + BaseConnector and runs freshness / volume / schema drift /
retention checks using only table metadata and lightweight SQL aggregates.

Results use the same SLOCheckResult / SLOReport models as the OSS pipeline,
so write_slo_checks() and Observatory push work without modification.
"""

from __future__ import annotations

import datetime
from types import SimpleNamespace
from typing import Any, Dict, List, Optional
from uuid import uuid4

from loguru import logger

from lakelogic.core.registry import _iso_period_to_minutes
from lakelogic.core.slo import SLOCheckResult, SLOReport
from lakelogic.scanner.config import ScannerConfig
from lakelogic.scanner.connector import BaseConnector, ScannedTable, TableMetadata
from lakelogic.scanner.schema_drift import BaselineStore, LocalBaselineStore, compare_schemas


class ScannerValidator:
    """
    Runs metadata-based SLO checks against tables discovered by a connector.

    Checks performed (no row-level data read):
      - freshness:     MAX last_modified vs. max_delay_minutes
      - volume:        num_rows within bounds; anomaly vs. history baseline
      - schema_drift:  current schema vs. stored baseline (requires BaselineStore)
      - retention:     MIN(timestamp_col) vs. retention period (one SQL aggregate)

    Results flow:
      ScannerValidator.run()
        → write_slo_checks()  (local _slo_checks Delta/DuckDB/SQLite — optional)
        → Observatory push    (if endpoint configured)
        → SLOReport           (always returned)
    """

    def __init__(
        self,
        config: ScannerConfig,
        connector: BaseConnector,
        baseline_store: Optional[BaselineStore] = None,
    ):
        self.config = config
        self.connector = connector
        self.baseline_store: BaselineStore = baseline_store or LocalBaselineStore()

    # ── Individual checks ─────────────────────────────────────────────────────

    def _check_freshness(self, table: ScannedTable, meta: TableMetadata) -> SLOCheckResult:
        now = datetime.datetime.now(datetime.timezone.utc)
        fd = self.config.slo_defaults.freshness

        if meta.last_modified is None:
            return SLOCheckResult(
                layer=table.layer,
                entity=table.full_name,
                check_type="freshness",
                status="⏭ SKIPPED — no last_modified available",
                passed=True,
                severity="pass",
            )

        lm = meta.last_modified
        if lm.tzinfo is None:
            lm = lm.replace(tzinfo=datetime.timezone.utc)

        delay = round((now - lm).total_seconds() / 60, 1)
        passed = delay <= fd.max_delay_minutes
        warn = fd.warn_at_minutes and delay > fd.warn_at_minutes and passed

        if passed and not warn:
            status = f"✅ OK ({delay:.0f}min delay, limit {fd.max_delay_minutes}min)"
            severity = "pass"
        elif warn:
            status = f"⚠ WARN ({delay:.0f}min delay approaching limit {fd.max_delay_minutes}min)"
            severity = "warn"
        else:
            status = f"❌ STALE ({delay:.0f}min delay exceeds {fd.max_delay_minutes}min)"
            severity = "fail"

        return SLOCheckResult(
            layer=table.layer,
            entity=table.full_name,
            check_type="freshness",
            status=status,
            passed=passed,
            severity=severity,
            delay_minutes=delay,
            slo_max_minutes=fd.max_delay_minutes,
        )

    def _check_volume(self, table: ScannedTable, meta: TableMetadata) -> Optional[SLOCheckResult]:
        if meta.num_rows is None:
            return None

        vd = self.config.slo_defaults.volume
        if not vd.anomaly_enabled or len(meta.history) < 2:
            return SLOCheckResult(
                layer=table.layer,
                entity=table.full_name,
                check_type="row_count",
                status=f"✅ OK ({meta.num_rows:,} rows — no anomaly baseline yet)",
                passed=True,
                severity="pass",
                row_count=meta.num_rows,
            )

        # Build baseline from history row counts
        historical = [
            h.num_output_rows
            for h in meta.history[1:]  # skip most recent (current run)
            if h.num_output_rows is not None and h.num_output_rows > 0
        ]
        if len(historical) < vd.lookback_runs:
            return SLOCheckResult(
                layer=table.layer,
                entity=table.full_name,
                check_type="row_count",
                status=f"✅ OK ({meta.num_rows:,} rows — building baseline, {len(historical)}/{vd.lookback_runs} runs)",
                passed=True,
                severity="pass",
                row_count=meta.num_rows,
            )

        historical_sorted = sorted(historical)
        mid = len(historical_sorted) // 2
        baseline = (
            (historical_sorted[mid - 1] + historical_sorted[mid]) / 2
            if len(historical_sorted) % 2 == 0
            else float(historical_sorted[mid])
        )

        if baseline == 0:
            return None

        recent = meta.history[0].num_output_rows or meta.num_rows
        ratio = recent / baseline

        passed = vd.min_ratio <= ratio <= vd.max_ratio
        status = (
            f"✅ OK (ratio={ratio:.2f}x vs median {baseline:,.0f})"
            if passed
            else (
                f"❌ VOLUME DROP ({ratio:.2f}x < {vd.min_ratio}x baseline)"
                if ratio < vd.min_ratio
                else f"❌ VOLUME SPIKE ({ratio:.2f}x > {vd.max_ratio}x baseline)"
            )
        )

        return SLOCheckResult(
            layer=table.layer,
            entity=table.full_name,
            check_type="row_count",
            status=status,
            passed=passed,
            severity="pass" if passed else "warn",
            row_count=recent,
            anomaly_ratio=round(ratio, 4),
            anomaly_baseline=round(baseline, 1),
        )

    def _check_schema_drift(self, table: ScannedTable, meta: TableMetadata) -> Optional[SLOCheckResult]:
        if not meta.schema_fields:
            return None

        sd = self.config.slo_defaults.schema_drift
        baseline = self.baseline_store.get(table.full_name)

        if baseline is None:
            self.baseline_store.save(table.full_name, meta.schema_fields)
            return SLOCheckResult(
                layer=table.layer,
                entity=table.full_name,
                check_type="schema_drift",
                status=f"✅ BASELINE SET ({len(meta.schema_fields)} columns recorded)",
                passed=True,
                severity="pass",
            )

        diff = compare_schemas(baseline, meta.schema_fields)

        if diff.is_empty:
            return SLOCheckResult(
                layer=table.layer,
                entity=table.full_name,
                check_type="schema_drift",
                status="✅ NO DRIFT",
                passed=True,
                severity="pass",
            )

        # Update baseline to current
        self.baseline_store.save(table.full_name, meta.schema_fields)

        severity_level = diff.severity()  # breaking | warning | info
        on_breaking = sd.on_breaking_change
        on_added = sd.on_column_added

        if severity_level == "breaking":
            action = on_breaking
        elif diff.added and not diff.removed and not diff.type_changes:
            action = on_added
        else:
            action = "warn"

        passed = action != "fail"
        icon = "✅" if action == "ignore" else ("⚠" if action == "warn" else "❌")

        return SLOCheckResult(
            layer=table.layer,
            entity=table.full_name,
            check_type="schema_drift",
            status=f"{icon} {severity_level.upper()} DRIFT — {diff.summary()}",
            passed=passed,
            severity="fail" if not passed else ("warn" if action == "warn" else "pass"),
            details={"diff": diff.to_dict()},
        )

    def _check_retention(self, table: ScannedTable, meta: TableMetadata) -> Optional[SLOCheckResult]:
        iso_period = self.config.slo_defaults.retention.default
        if not iso_period:
            return None

        retention_minutes = _iso_period_to_minutes(iso_period)
        if not retention_minutes:
            return None

        ts_cols = self.config.discovery.timestamp_columns
        min_ts = self.connector.query_min_timestamp(table, ts_cols)

        if min_ts is None:
            return SLOCheckResult(
                layer=table.layer,
                entity=table.full_name,
                check_type="retention",
                status=f"⏭ SKIPPED — no timestamp column found in {ts_cols}",
                passed=True,
                severity="pass",
            )

        now = datetime.datetime.now(datetime.timezone.utc)
        if min_ts.tzinfo is None:
            min_ts = min_ts.replace(tzinfo=datetime.timezone.utc)

        age_minutes = round((now - min_ts).total_seconds() / 60, 1)
        passed = age_minutes <= retention_minutes

        status = (
            f"✅ OK (oldest record {age_minutes:.0f}min, limit {retention_minutes}min [{iso_period}])"
            if passed
            else (f"RETENTION BREACH: oldest record {age_minutes:.0f}min exceeds {iso_period} ({retention_minutes}min)")
        )

        return SLOCheckResult(
            layer=table.layer,
            entity=table.full_name,
            check_type="retention",
            status=status,
            passed=passed,
            severity="pass" if passed else "fail",
            source_delay_minutes=age_minutes,
            source_slo_max_minutes=retention_minutes,
        )

    # ── Table scan ────────────────────────────────────────────────────────────

    def scan_table(self, table: ScannedTable) -> List[SLOCheckResult]:
        """Fetch metadata and run all configured checks for one table."""
        try:
            meta = self.connector.get_metadata(table)
        except Exception as exc:
            logger.warning(f"  ✗ {table.full_name}: metadata fetch failed — {exc}")
            return [
                SLOCheckResult(
                    layer=table.layer,
                    entity=table.full_name,
                    check_type="freshness",
                    status=f"❌ ERROR — {exc}",
                    passed=False,
                    severity="fail",
                )
            ]

        results: List[SLOCheckResult] = []

        results.append(self._check_freshness(table, meta))

        vol = self._check_volume(table, meta)
        if vol:
            results.append(vol)

        drift = self._check_schema_drift(table, meta)
        if drift:
            results.append(drift)

        retention = self._check_retention(table, meta)
        if retention:
            results.append(retention)

        return results

    # ── Main entry point ──────────────────────────────────────────────────────

    def run(self, pipeline_run_id: Optional[str] = None) -> SLOReport:
        """
        Discover all tables, run checks on each, write results, return report.

        Args:
            pipeline_run_id: Optional FK to run_log — passed through to
                             write_slo_checks() for cross-table joins.
        """
        check_run_id = str(uuid4())
        now = datetime.datetime.now(datetime.timezone.utc)

        logger.info(f"🔍 LakeLogic Scanner: {self.config.connection.type} catalog={self.config.domain}")

        # Discover
        try:
            tables = self.connector.discover(self.config.discovery)
        except Exception as exc:
            logger.error(f"Discovery failed: {exc}")
            return SLOReport(
                domain=self.config.domain,
                system="scanner",
                timestamp=now.isoformat(),
                passed=False,
                check_run_id=check_run_id,
            )

        logger.info(f"  → {len(tables)} tables to scan")

        # Scan each table
        all_results: List[SLOCheckResult] = []
        for table in tables:
            logger.debug(f"  scanning {table.full_name}...")
            results = self.scan_table(table)
            all_results.extend(results)

        failures = [r for r in all_results if not r.passed]
        passed = len(failures) == 0

        logger.info(
            f"  ✅ {len(all_results) - len(failures)} passed  "
            f"{'❌ ' + str(len(failures)) + ' failed' if failures else ''}"
        )

        # Write _slo_checks locally if output is configured
        self._write_results(all_results, check_run_id, pipeline_run_id, now.isoformat())

        return SLOReport(
            domain=self.config.domain,
            system="scanner",
            timestamp=now.isoformat(),
            passed=passed,
            check_run_id=check_run_id,
            pipeline_run_id=pipeline_run_id,
            failures=failures,
            results=all_results,
        )

    def _write_results(
        self,
        results: List[SLOCheckResult],
        check_run_id: str,
        pipeline_run_id: Optional[str],
        checked_at: str,
    ) -> None:
        """Write to local _slo_checks table and push to Observatory — both non-blocking."""
        if not results:
            return

        # Local write — uses same write_slo_checks() as SLOValidator
        out = self.config.output
        if out.slo_checks_table:
            try:
                from lakelogic.core.run_log import write_slo_checks

                # Build a minimal registry-like namespace so write_slo_checks
                # can read slo_checks_table / slo_checks_backend without a full DomainRegistry
                fake_registry = SimpleNamespace(
                    storage=SimpleNamespace(slo_checks_table=out.slo_checks_table),
                    metadata={"slo_checks_backend": out.slo_checks_backend},
                    domain=self.config.domain,
                    system="scanner",
                )
                write_slo_checks(fake_registry, results, check_run_id, pipeline_run_id)
            except Exception as exc:
                logger.warning(f"SLO checks write failed: {exc}")

        # Observatory push
        obs = self.config.observatory
        if obs.endpoint and obs.api_key:
            try:
                self._push_observatory(results, check_run_id, pipeline_run_id, checked_at)
            except Exception as exc:
                logger.warning(f"Observatory push failed: {exc}")

    def _push_observatory(
        self,
        results: List[SLOCheckResult],
        check_run_id: str,
        pipeline_run_id: Optional[str],
        checked_at: str,
    ) -> None:
        import httpx

        obs = self.config.observatory
        payload: Dict[str, Any] = {
            "check_run_id": check_run_id,
            "pipeline_run_id": pipeline_run_id,
            "checked_at": checked_at,
            "domain": self.config.domain,
            "system": "scanner",
            "source": "scanner",
            "results": [r.model_dump() for r in results],
        }
        r = httpx.post(
            f"{obs.endpoint.rstrip('/')}/slo-checks",
            json=payload,
            headers={"Authorization": f"Bearer {obs.api_key}"},
            timeout=15,
        )
        if r.status_code >= 400:
            logger.warning(f"Observatory push returned {r.status_code}: {r.text[:200]}")
        else:
            logger.info(f"Observatory: pushed {len(results)} SLO check results")
