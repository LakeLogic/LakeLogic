"""`slo.row_count.anomaly` is the one volume rulebook — so it must be as correct as the detector
it replaces.

The LakeLogic platform ran its own volume-drop detector beside this check, with different rules,
so the Volume page and Service Levels could disagree about the same dataset. The platform is
standardising on THIS check. Taken as it was, that would have reintroduced three defects the
platform had already found and fixed in its own detector (2026-09-11):

1. POOLED ENVIRONMENTS. The history query never filtered on `environment`. Where dev, staging
   and prod share one run log — the Snowflake mesh does — a dev smoke run was judged against a
   prod median (a false "-59%"), and a real -99% dev collapse hid behind a later healthy run.
2. NO SEASONALITY, and the first seasonal version's gap: it switched to same-weekday samples
   at 2, then rejected them for being fewer than `min_runs_before_enforcement` (3), leaving a
   daily dataset in its 2nd and 3rd week with no baseline at all.
3. NO CRITICAL. Every breach was a warning, so a collapse to zero rows and a 45% dip read the
   same — and only the first should open an incident.

Plus one found on the way: the check's engine guard listed three engines, so on Snowflake it
never ran at all.
"""

from __future__ import annotations

import datetime as dt
from types import SimpleNamespace

import pytest

from lakelogic.core.registry import SLORowCountAnomalyConfig
from lakelogic.core.run_log import emit_slo_report
from lakelogic.core.slo import SLOCheckResult, SLOValidator
from lakelogic.core.volume_baseline import row_count_baseline, row_count_verdict

FRIDAY = dt.datetime(2026, 9, 11, 6, 0)


def _cfg(**over):
    base = dict(
        enabled=True,
        method="seasonal_median",
        lookback_days=35,
        lookback_runs=14,
        min_ratio=0.6,
        max_ratio=2.0,
        critical_ratio=0.3,
        min_runs_before_enforcement=3,
    )
    base.update(over)
    return SLORowCountAnomalyConfig(**base)


def _daily(values, *, end=FRIDAY, environment="dev"):
    """Runs one per day ending the day BEFORE `end`, newest first — as the query returns them."""
    n = len(values)
    return [
        {"cnt": v, "timestamp": end - dt.timedelta(days=n - i), "environment": environment}
        for i, v in enumerate(values)
    ][::-1]


# ── 1. One environment ───────────────────────────────────────────────────────


def test_a_baseline_is_scoped_to_the_judged_runs_environment():
    """The false alarm, as observed: a small local run judged against dev's volume."""
    history = sorted(
        _daily([1000] * 20, environment="dev") + _daily([400] * 20, environment="local_polars"),
        key=lambda h: h["timestamp"],
        reverse=True,
    )
    judged = {"cnt": 400, "timestamp": FRIDAY, "environment": "local_polars"}

    base = row_count_baseline(judged, history, _cfg())

    assert base.expected == 400 and base.environment == "local_polars"
    assert row_count_verdict(400, base, _cfg()).passed


def test_a_collapse_is_not_hidden_by_another_environments_history():
    history = sorted(
        _daily([566] * 20, environment="dev") + _daily([9000] * 20, environment="prod"),
        key=lambda h: h["timestamp"],
        reverse=True,
    )
    judged = {"cnt": 8, "timestamp": FRIDAY, "environment": "dev"}

    base = row_count_baseline(judged, history, _cfg())
    verdict = row_count_verdict(8, base, _cfg())

    assert base.expected == 566
    assert verdict.severity == "critical"


def test_a_run_log_without_an_environment_column_is_not_scoped():
    """Older run logs have no such column. Every row is then the same, unknown, environment."""
    history = [{"cnt": 100, "timestamp": FRIDAY - dt.timedelta(days=d)} for d in range(1, 10)]
    base = row_count_baseline({"cnt": 100, "timestamp": FRIDAY}, history, _cfg())

    assert base.expected == 100 and base.environment is None


# ── 2. Seasonal, with the corrected fallback ─────────────────────────────────


def test_two_same_weekday_runs_fall_back_to_the_trailing_median():
    """20 days at 700, then 40 on a Friday. Only two priors are Fridays — fewer than the three
    enforcement needs — so the trailing median applies. It must not return "no baseline"."""
    base = row_count_baseline({"cnt": 40, "timestamp": FRIDAY, "environment": "dev"}, _daily([700] * 20), _cfg())

    assert base is not None, "a 94% collapse with 20 days of history must be judged"
    assert (base.expected, base.seasonal) == (700, False)
    assert row_count_verdict(40, base, _cfg()).severity == "critical"


def test_a_quiet_weekend_is_judged_against_past_weekends():
    """Sundays run at 200, weekdays at 1000. A plain median calls every Sunday a drop."""
    sunday = dt.datetime(2026, 9, 13, 6, 0)
    history = [
        {
            "cnt": 200 if (sunday - dt.timedelta(days=d)).weekday() == 6 else 1000,
            "timestamp": sunday - dt.timedelta(days=d),
            "environment": "dev",
        }
        for d in range(1, 35)
    ]
    judged = {"cnt": 200, "timestamp": sunday, "environment": "dev"}

    seasonal = row_count_baseline(judged, history, _cfg())
    plain = row_count_baseline(judged, history, _cfg(method="median"))

    assert seasonal.seasonal and seasonal.expected == 200
    assert row_count_verdict(200, seasonal, _cfg()).passed
    assert not row_count_verdict(200, plain, _cfg(method="median")).passed, "what the old method said"


def test_history_older_than_the_window_is_ignored():
    old = [{"cnt": 5000, "timestamp": FRIDAY - dt.timedelta(days=d), "environment": "dev"} for d in range(40, 60)]
    recent = _daily([300] * 10)
    base = row_count_baseline({"cnt": 300, "timestamp": FRIDAY, "environment": "dev"}, recent + old, _cfg())

    assert base.expected == 300


def test_a_zero_row_run_does_not_define_normal_volume():
    history = _daily([500, 0, 500, 0, 500, 0, 500])
    base = row_count_baseline({"cnt": 480, "timestamp": FRIDAY, "environment": "dev"}, history, _cfg())

    assert base.expected == 500


def test_the_run_count_methods_keep_their_zeros():
    """`median` also baselines ratio expressions, where 0 is a real value — unchanged."""
    history = [{"cnt": v} for v in (0, 0, 0, 0.2)]
    base = row_count_baseline({"cnt": 0}, history, _cfg(method="median"))

    assert base is None, "median of mostly-zero history is 0, which is no baseline — as before"


def test_too_little_history_is_no_baseline():
    assert row_count_baseline({"cnt": 5, "timestamp": FRIDAY, "environment": "dev"}, _daily([500, 500]), _cfg()) is None


# ── 3. Severity ─────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "actual, expected_severity",
    [
        (0, "critical"),  # silent upstream
        (100, "critical"),  # 0.1x, below critical_ratio 0.3
        (400, "warn"),  # 0.4x, a dip
        (900, "pass"),
        (2500, "warn"),  # a spike is never critical
    ],
)
def test_severity_follows_the_ratio(actual, expected_severity):
    base = row_count_baseline({"cnt": actual, "timestamp": FRIDAY, "environment": "dev"}, _daily([1000] * 20), _cfg())
    assert row_count_verdict(actual, base, _cfg()).severity == expected_severity


def test_without_a_critical_ratio_only_zero_rows_is_critical():
    base = row_count_baseline(
        {"cnt": 100, "timestamp": FRIDAY, "environment": "dev"}, _daily([1000] * 20), _cfg(critical_ratio=None)
    )
    assert row_count_verdict(100, base, _cfg(critical_ratio=None)).severity == "warn"
    assert row_count_verdict(0, base, _cfg(critical_ratio=None)).severity == "critical"


# ── Through the validator: DuckDB, Snowflake, and the wire ──────────────────


def _duck_run_log(rows):
    duckdb = pytest.importorskip("duckdb")
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE run_logs (data_layer VARCHAR, dataset VARCHAR, stage VARCHAR, "
        "counts_good BIGINT, timestamp TIMESTAMP, environment VARCHAR)"
    )
    con.executemany("INSERT INTO run_logs VALUES (?, ?, ?, ?, ?, ?)", rows)
    return con


def test_a_shared_run_log_is_judged_per_environment_end_to_end():
    """The Snowflake-mesh shape on DuckDB: one run log, two environments, interleaved."""
    rows = []
    for d in range(1, 21):
        ts = FRIDAY - dt.timedelta(days=d)
        rows.append(("bronze", "orders", "ok", 566, ts, "dev"))
        rows.append(("bronze", "orders", "ok", 9000, ts + dt.timedelta(hours=1), "prod"))
    rows.append(("bronze", "orders", "ok", 8, FRIDAY, "dev"))  # the newest row: judged
    registry = SimpleNamespace(storage=SimpleNamespace(run_log_table="run_logs"))

    result = SLOValidator(registry, duckdb_con=_duck_run_log(rows)).check_row_count_anomaly(
        "orders", "bronze", 8, _cfg(), check_field="counts_good"
    )

    assert result.anomaly_baseline == 566.0, "dev history only — prod's 9000 would read -99.9%"
    assert result.anomaly_environment == "dev"
    assert result.anomaly_severity == "critical"
    assert result.passed is False
    assert "VOLUME DROP" in result.status


def test_the_check_runs_on_snowflake():
    """It used to return None before querying anything: Snowflake was not in its engine list."""
    history = [(1000, FRIDAY - dt.timedelta(days=d), "prod") for d in range(0, 21)]

    class _Cursor:
        def execute(self, sql):
            self.sql = sql

        def fetchall(self):
            return history

        def close(self):
            pass

    registry = SimpleNamespace(storage=SimpleNamespace(run_log_table="DB.META._PIPELINE_RUN_LOG"))
    con = SimpleNamespace(cursor=_Cursor)

    result = SLOValidator(registry, snowflake_con=con).check_row_count_anomaly("orders", "bronze", 1000, _cfg())

    assert result is not None and result.passed
    assert result.anomaly_environment == "prod"


def test_the_rule_rides_to_the_platform_with_the_verdict(monkeypatch):
    bodies = []

    class _Resp:
        status_code = 202
        text = ""

    import requests

    monkeypatch.setattr(
        requests, "post", lambda url, json=None, headers=None, timeout=None: bodies.append(json) or _Resp()
    )

    class _Registry:
        domain = "marketplace"
        system = "rideflow"
        observatory = {"enabled": True, "endpoint": "https://example.invalid/ingest", "api_key": "k"}

    emit_slo_report(
        _Registry(),
        [
            SLOCheckResult(
                layer="bronze",
                entity="orders",
                check_type="row_count",
                status="drop",
                passed=False,
                severity="warn",
                row_count=8,
                anomaly_ratio=0.0141,
                anomaly_baseline=566.0,
                anomaly_floor=339.6,
                anomaly_ceiling=1132.0,
                anomaly_method="seasonal_median",
                anomaly_seasonal=False,
                anomaly_samples=20,
                anomaly_environment="dev",
                anomaly_severity="critical",
            )
        ],
        environment="dev",
    )

    section = bodies[0]["metadata"]["slo_json"]["row_count"]
    assert section["anomaly_floor"] == 339.6
    assert section["anomaly_severity"] == "critical"
    assert section["anomaly_environment"] == "dev"
    assert section["anomaly_samples"] == 20


def test_the_new_settings_parse_from_the_domain_config():
    cfg = SLORowCountAnomalyConfig(enabled=True, method="seasonal_median", lookback_days=28, critical_ratio=0.25)
    assert (cfg.lookback_days, cfg.critical_ratio) == (28, 0.25)
    # Existing configs are untouched.
    assert (SLORowCountAnomalyConfig().method, SLORowCountAnomalyConfig().critical_ratio) == ("median", None)
