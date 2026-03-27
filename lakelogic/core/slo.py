"""
SLO Validation engine for Lakehouse Domains.

Evaluates data freshness and pipeline schedule guarantees
against rules defined in `_registry.yaml`.
"""

from __future__ import annotations

import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field
from loguru import logger

from lakelogic.core.registry import DomainRegistry


class SLOCheckResult(BaseModel):
    layer: str
    entity: str
    status: str
    passed: bool
    latest_ts: Optional[str] = None
    delay_minutes: Optional[float] = None
    slo_max_minutes: Optional[int] = None
    row_count: Optional[int] = None
    slo_min_rows: Optional[int] = None
    slo_max_rows: Optional[int] = None


class SLOReport(BaseModel):
    domain: str
    system: str
    timestamp: str
    passed: bool
    failures: List[SLOCheckResult] = Field(default_factory=list)
    results: List[SLOCheckResult] = Field(default_factory=list)


class SLOValidator:
    """
    Validates data freshness pipelines against Data Contract and Registry SLAs.
    """

    def __init__(self, registry: DomainRegistry, spark: Any = None):
        self.registry = registry
        self.spark = spark

    def check_freshness(self) -> List[SLOCheckResult]:
        """
        Check the freshness of all active contracts against the layer SLOs.
        """
        if not self.spark:
            logger.warning("SLOValidator.check_freshness requires a Spark session. Skipping.")
            return []

        now = datetime.datetime.now(datetime.timezone.utc)
        results = []

        freshness_config = self.registry.slo.freshness
        storage = self.registry.storage

        layer_roots = {
            "bronze": storage.bronze_root,
            "silver": storage.silver_root,
            "gold": storage.gold_root,
        }

        # Validate all active contracts
        for contract in self.registry.get_active_contracts():
            layer = contract.layer
            entity = contract.entity

            if layer not in layer_roots or not layer_roots[layer]:
                continue

            schema_root = layer_roots[layer]
            table_name = f"{schema_root}.{entity}".replace("`", "")

            # Get the SLO rules for this specific layer
            layer_slo = freshness_config.get(layer)
            if layer_slo and entity in layer_slo.exclude_tables:
                continue

            max_delay = layer_slo.max_delay_minutes if layer_slo else 999999

            check_col_conf = layer_slo.check_column if layer_slo else "_lakelogic_loaded_at"
            if isinstance(check_col_conf, str):
                check_cols = [check_col_conf]
            else:
                check_cols = list(check_col_conf)

            # Always fallback to the standard audit column if not explicitly in the list
            if "_lakelogic_loaded_at" not in check_cols:
                check_cols.append("_lakelogic_loaded_at")

            latest_ts = None
            found_col = None
            last_error = None

            for col in check_cols:
                try:
                    # Query the latest timestamp from the table using the current column candidate
                    row = self.spark.sql(f"SELECT MAX({col}) as latest_ts FROM {table_name}").first()
                    latest_ts = row["latest_ts"]
                    found_col = col
                    break
                except Exception as e:
                    last_error = e
                    continue

            if not found_col:
                # Table might not exist or none of the columns exist
                results.append(
                    SLOCheckResult(
                        layer=layer,
                        entity=entity,
                        status=f"⚠️ ERROR: {str(last_error)[:80]}",
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
                # Calculate delay
                if hasattr(latest_ts, "timestamp"):
                    latest_utc = datetime.datetime.fromtimestamp(latest_ts.timestamp(), tz=datetime.timezone.utc)
                else:
                    latest_utc = latest_ts.replace(tzinfo=datetime.timezone.utc)

                delay = (now - latest_utc).total_seconds() / 60
                passed = delay <= max_delay

                results.append(
                    SLOCheckResult(
                        layer=layer,
                        entity=entity,
                        status="✅ OK" if passed else "❌ STALE",
                        passed=passed,
                        latest_ts=str(latest_ts),
                        delay_minutes=round(delay, 1),
                        slo_max_minutes=max_delay,
                    )
                )

            except Exception as e:
                # Table might not exist or column might be missing
                results.append(
                    SLOCheckResult(
                        layer=layer,
                        entity=entity,
                        status=f"⚠️ ERROR: {str(e)[:80]}",
                        passed=False,
                        slo_max_minutes=max_delay,
                    )
                )

        return results

    def check_row_counts(self) -> List[SLOCheckResult]:
        """
        Check the row counts of the most recent run log entry for each active
        contract against the per-layer thresholds defined in ``slo.row_count``.

        Reads from the run log table (no live COUNT queries) using the existing
        ``counts_good`` / ``counts_source`` / ``counts_total`` columns.
        """
        if not self.spark:
            logger.warning("SLOValidator.check_row_counts requires a Spark session. Skipping.")
            return []

        results = []
        row_count_config = self.registry.slo.row_count
        storage = self.registry.storage
        run_log_table = storage.run_log_table

        if not run_log_table:
            logger.warning("No run_log_table configured in storage; cannot check row counts.")
            return []

        # Strip backticks for Spark SQL compatibility
        run_log_table_clean = run_log_table.replace("`", "")

        for contract in self.registry.get_active_contracts():
            layer = contract.layer
            entity = contract.entity

            layer_slo = row_count_config.get(layer)
            if not layer_slo:
                continue

            if entity in layer_slo.exclude_tables:
                continue

            min_rows = layer_slo.min_rows
            max_rows = layer_slo.max_rows
            check_field = layer_slo.check_field or "counts_good"

            if min_rows is None and max_rows is None:
                continue

            try:
                # Query the most recent run log entry for this entity + layer
                row = self.spark.sql(f"""
                    SELECT {check_field}, timestamp
                    FROM {run_log_table_clean}
                    WHERE data_layer = '{layer}'
                      AND dataset = '{entity}'
                      AND stage NOT IN ('no_new_data', 'reprocess')
                    ORDER BY timestamp DESC
                    LIMIT 1
                """).first()
            except Exception as e:
                results.append(
                    SLOCheckResult(
                        layer=layer,
                        entity=entity,
                        status=f"⚠️ ERROR: {str(e)[:80]}",
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

    def check_schedule(self) -> Optional[SLOCheckResult]:
        """
        Check if the pipeline completed before the expected UTC deadline.
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        schedule = self.registry.slo.schedule

        if not schedule or not schedule.expected_completion_utc:
            return None

        time_str = schedule.expected_completion_utc
        try:
            expected_hour, expected_min = map(int, time_str.split(":"))
            deadline = now.replace(hour=expected_hour, minute=expected_min, second=0, microsecond=0)

            if now <= deadline:
                return SLOCheckResult(
                    layer="schedule",
                    entity="pipeline",
                    status="✅ ON TIME",
                    passed=True,
                    delay_minutes=round((now - deadline).total_seconds() / 60, 1),
                )
            else:
                return SLOCheckResult(
                    layer="schedule",
                    entity="pipeline",
                    status=f"❌ LATE by {(now - deadline).total_seconds() / 60:.0f} min",
                    passed=False,
                    delay_minutes=round((now - deadline).total_seconds() / 60, 1),
                )
        except Exception as e:
            logger.error(f"Failed to parse or evaluate schedule SLO: {e}")
            return None

    def run_checks(self) -> SLOReport:
        """
        Run all configured SLO checks and return a unified report.
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        results = []

        if self.registry.slo.freshness:
            results.extend(self.check_freshness())

        if self.registry.slo.row_count:
            results.extend(self.check_row_counts())

        schedule_res = self.check_schedule()
        if schedule_res:
            results.append(schedule_res)

        failures = [r for r in results if not r.passed]

        return SLOReport(
            domain=self.registry.domain,
            system=self.registry.system,
            timestamp=now.isoformat(),
            passed=len(failures) == 0,
            failures=failures,
            results=results,
        )


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
        if engine_name == "pandas":
            if field not in df.columns or df.empty:
                return None
            val = df[field].max()
            return _coerce_datetime(val)
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
        if engine_name == "pandas":
            if field not in df.columns or df.empty:
                return None
            total = len(df)
            non_null = df[field].notna().sum()
            return non_null / total if total > 0 else None
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

    return {
        "field": field,
        "threshold": str(threshold),
        "delay_seconds": round(delay_secs, 1),
        "passed": passed,
    }


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


def compute_slos(
    contract: Any,
    good_df: Any,
    counts: Dict[str, Optional[int]],
    engine_name: str,
) -> Dict[str, Any]:
    """Compute per-contract SLO scores (freshness + availability).

    This is the lightweight, per-run variant used by ``DataProcessor.run()``.
    For domain-wide SLO checks, use :class:`SLOValidator`.
    """
    slo_cfg = getattr(contract, "service_levels", None)
    if slo_cfg is None:
        return {}

    result: Dict[str, Any] = {}

    freshness = slo_cfg.get("freshness") if isinstance(slo_cfg, dict) else getattr(slo_cfg, "freshness", None)
    if freshness:
        result["freshness"] = _compute_freshness(good_df, freshness, engine_name)

    availability = slo_cfg.get("availability") if isinstance(slo_cfg, dict) else getattr(slo_cfg, "availability", None)
    if availability:
        result["availability"] = _compute_availability(good_df, counts, availability, engine_name)

    return result
