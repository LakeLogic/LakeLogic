"""What a dataset's row count SHOULD have been — the one definition behind `slo.row_count.anomaly`.

Pure: no engine, no I/O. `SLOValidator.check_row_count_anomaly` fetches the history and this
module decides, so the rules can be tested without a run log.

THREE RULES, each learned the hard way in the LakeLogic platform's own volume detector before
it moved here (see the platform's docs/specs/volume-monitoring-one-rulebook.md):

1. **A baseline belongs to ONE environment.** Where dev, staging and prod write to one run log
   (the Snowflake mesh points all three at one database), pooling them judged a dev smoke run
   against a prod median — a false "-59%" — and hid a real -99% dev collapse behind a later
   healthy run from another environment. The judged run's environment scopes its history.

2. **Seasonal only when it can stand on its own.** `seasonal_median` compares a Monday with
   past Mondays, because weekends are quieter by design. It switches to same-weekday samples
   only when there are at least ``max(2, min_runs_before_enforcement)`` of them, and otherwise
   uses the trailing median of the window. The first version switched at 2 and then rejected
   the result for having fewer than 3 — so a daily dataset in its second and third week had
   no baseline at all, and a collapse in that window could not be detected.

3. **A broken run must not define "normal".** For `seasonal_median` (a volume method), runs
   that wrote 0 rows are left out of the baseline. The older `median` / `rolling_average`
   methods keep their zeros: they also baseline ratio expressions such as
   ``counts_deduplicated / NULLIF(counts_source, 0)``, where 0 is a legitimate value.

The verdict carries everything it was judged against — expected, floor, ceiling, method,
seasonal, samples, environment, severity — so a later change to the config never re-judges an
old run, and a chart can draw the exact line that fired.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional

#: Days of history `seasonal_median` looks back over when `lookback_days` is not set.
DEFAULT_LOOKBACK_DAYS = 35

METHODS = ("median", "rolling_average", "seasonal_median")


@dataclass(frozen=True)
class RowCountBaseline:
    expected: float
    method: str
    #: True when the median came from same-weekday runs only.
    seasonal: bool
    #: How many prior runs the baseline was taken over — the evidence behind the number.
    samples: int
    #: The environment the history was scoped to; None when the run log has no such column.
    environment: Optional[str]


@dataclass(frozen=True)
class RowCountVerdict:
    ratio: float
    passed: bool
    floor: float
    ceiling: float
    #: "pass" | "warn" | "critical"
    severity: str
    direction: Optional[str]  # "drop" | "spike" | None


def _median(xs: List[float]) -> float:
    s = sorted(xs)
    n = len(s)
    mid = n // 2
    return float(s[mid]) if n % 2 else (s[mid - 1] + s[mid]) / 2.0


def _as_datetime(value: Any) -> Optional[_dt.datetime]:
    """A run-log timestamp as a naive-UTC datetime, or None when it is not one."""
    if isinstance(value, _dt.datetime):
        ts = value
    elif isinstance(value, str) and value:
        try:
            ts = _dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if ts.tzinfo is not None:
        ts = ts.astimezone(_dt.timezone.utc).replace(tzinfo=None)
    return ts


def _cfg(cfg: Any, name: str, default: Any) -> Any:
    value = getattr(cfg, name, None)
    return default if value is None else value


def row_count_baseline(
    judged: Mapping[str, Any],
    history: List[Mapping[str, Any]],
    cfg: Any,
) -> Optional[RowCountBaseline]:
    """The expected count for ``judged``, from ``history`` — strictly earlier runs, newest first.

    Each row carries ``cnt`` and, when the run log has them, ``timestamp`` and ``environment``.
    Returns None when there is not enough usable history: that is "no baseline", and the check
    must stay quiet rather than judge against nothing.
    """
    method = _cfg(cfg, "method", "median")
    min_runs = int(_cfg(cfg, "min_runs_before_enforcement", 5))

    # Rule 1 — one environment. Only when the column exists: a run log written before it did
    # carries no environment at all, and then every row is the same (unknown) environment.
    environment = judged.get("environment")
    priors = [h for h in history if h.get("cnt") is not None]
    if "environment" in judged and environment is not None:
        priors = [h for h in priors if h.get("environment") == environment]

    if method == "seasonal_median":
        judged_at = _as_datetime(judged.get("timestamp"))
        # Rule 3 — a run that wrote nothing is not evidence of normal volume.
        priors = [h for h in priors if float(h["cnt"]) > 0]
        if judged_at is not None:
            since = judged_at - _dt.timedelta(days=int(_cfg(cfg, "lookback_days", DEFAULT_LOOKBACK_DAYS)))
            dated = [(h, _as_datetime(h.get("timestamp"))) for h in priors]
            window = [h for h, ts in dated if ts is not None and since <= ts < judged_at]
            same_weekday = [
                h
                for h, ts in dated
                if ts is not None and since <= ts < judged_at and ts.weekday() == judged_at.weekday()
            ]
        else:
            # No timestamps to reason about: the trailing window is the most that can be said.
            window = priors[: int(_cfg(cfg, "lookback_runs", 14))]
            same_weekday = []
        # Rule 2 — seasonal only when it can stand on its own.
        seasonal = len(same_weekday) >= max(2, min_runs)
        samples = same_weekday if seasonal else window
        if len(samples) < min_runs:
            return None
        expected = _median([float(h["cnt"]) for h in samples])
    else:
        seasonal = False
        samples = priors[: int(_cfg(cfg, "lookback_runs", 14))]
        if len(samples) < min_runs:
            return None
        values = [float(h["cnt"]) for h in samples]
        expected = _median(values) if method == "median" else sum(values) / len(values)

    if expected <= 0:
        return None
    return RowCountBaseline(
        expected=expected,
        method=method,
        seasonal=seasonal,
        samples=len(samples),
        environment=environment if "environment" in judged else None,
    )


def row_count_verdict(actual: float, baseline: RowCountBaseline, cfg: Any) -> RowCountVerdict:
    """Judge ``actual`` against ``baseline`` with the configured ratios.

    CRITICAL is a drop past ``critical_ratio`` — or to zero rows against a positive baseline,
    which is the silent-upstream failure this check exists for. It is carried separately from
    the check's pass/warn/fail severity, so no existing consumer of that field changes meaning.
    """
    min_ratio = float(_cfg(cfg, "min_ratio", 0.5))
    max_ratio = float(_cfg(cfg, "max_ratio", 2.0))
    critical_ratio = getattr(cfg, "critical_ratio", None)

    ratio = float(actual) / baseline.expected
    passed = min_ratio <= ratio <= max_ratio
    direction = None if passed else ("drop" if ratio < min_ratio else "spike")
    if passed:
        severity = "pass"
    elif direction == "drop" and (actual == 0 or (critical_ratio is not None and ratio < float(critical_ratio))):
        severity = "critical"
    else:
        severity = "warn"
    return RowCountVerdict(
        ratio=ratio,
        passed=passed,
        floor=baseline.expected * min_ratio,
        ceiling=baseline.expected * max_ratio,
        severity=severity,
        direction=direction,
    )


def describe(actual: float, baseline: RowCountBaseline, verdict: RowCountVerdict, cfg: Any) -> str:
    """The status line: what was seen, against what, in which environment."""
    basis = {
        "seasonal_median": "same-weekday median" if baseline.seasonal else "trailing median",
        "median": "median",
        "rolling_average": "rolling average",
    }.get(baseline.method, baseline.method)
    where = f", {baseline.environment}" if baseline.environment else ""
    against = f"{basis} {baseline.expected:,.0f} over {baseline.samples} runs{where}"
    if verdict.passed:
        return f"✅ OK (ratio={verdict.ratio:.2f}x vs {against})"
    label = "VOLUME DROP" if verdict.direction == "drop" else "VOLUME SPIKE"
    if verdict.severity == "critical":
        label += " — CRITICAL"
    bound = _cfg(cfg, "min_ratio", 0.5) if verdict.direction == "drop" else _cfg(cfg, "max_ratio", 2.0)
    op = "<" if verdict.direction == "drop" else ">"
    return f"❌ {label} ({verdict.ratio:.2f}x {op} {bound}x {against})"


def stamp(baseline: RowCountBaseline, verdict: RowCountVerdict) -> Dict[str, Any]:
    """The evidence fields for an SLOCheckResult — what the verdict was judged against."""
    return {
        "anomaly_ratio": round(verdict.ratio, 4),
        "anomaly_baseline": round(baseline.expected, 1),
        "anomaly_floor": round(verdict.floor, 1),
        "anomaly_ceiling": round(verdict.ceiling, 1),
        "anomaly_method": baseline.method,
        "anomaly_seasonal": baseline.seasonal,
        "anomaly_samples": baseline.samples,
        "anomaly_environment": baseline.environment,
        "anomaly_severity": verdict.severity,
    }
