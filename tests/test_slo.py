"""Tests for SLOValidator.check_row_counts() — system-level row count SLO."""

from __future__ import annotations

import datetime
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from lakelogic.core import slo
from lakelogic.core.slo import SLOCheckResult, SLOReport, SLOValidator

# ── helpers ──────────────────────────────────────────────────────────────────


def _make_registry(row_count_cfg: dict | None = None, contracts=None):
    """Build a minimal DomainRegistry-like object for SLO tests."""
    from lakelogic.core.registry import (
        DomainRegistry,
        RegistryContract,
        RegistrySLO,
        RegistryStorage,
        SLORowCountConfig,
    )

    rc = {}
    if row_count_cfg:
        for layer, cfg in row_count_cfg.items():
            rc[layer] = SLORowCountConfig(**cfg)

    slo = RegistrySLO(row_count=rc)
    storage = RegistryStorage(run_log_table="`catalog`.domain._run_logs")

    if contracts is None:
        contracts = [
            RegistryContract(
                layer="bronze",
                entity="events",
                path="dummy.yaml",
                enabled=True,
                contract_dict={"info": {}},
            ),
            RegistryContract(
                layer="bronze",
                entity="sessions",
                path="dummy.yaml",
                enabled=True,
                contract_dict={"info": {}},
            ),
            RegistryContract(
                layer="silver",
                entity="clean_events",
                path="dummy.yaml",
                enabled=True,
                contract_dict={"info": {}},
            ),
        ]

    return DomainRegistry(
        domain="marketing",
        system="google_analytics",
        slo=slo,
        storage=storage,
        contracts=contracts,
    )


def _mock_spark_row(check_field_value, timestamp="2026-03-26T12:00:00"):
    """Create a mock Spark Row returned by spark.sql(...).first()."""
    row = MagicMock()
    row.__getitem__ = lambda self, key: {
        "counts_good": check_field_value,
        "counts_source": check_field_value,
        "counts_total": check_field_value,
        "timestamp": timestamp,
    }.get(key)
    return row


# ── tests ────────────────────────────────────────────────────────────────────


class TestCheckRowCounts:
    def test_passes_when_above_min(self):
        """Row count of 50 with min_rows=20 should pass."""
        registry = _make_registry({"bronze": {"min_rows": 20}})
        spark = MagicMock()
        spark.sql.return_value.first.return_value = _mock_spark_row(50)

        validator = SLOValidator(registry, spark=spark)
        results = validator.check_row_counts()

        bronze_results = [r for r in results if r.layer == "bronze"]
        assert len(bronze_results) == 2  # events + sessions
        for r in bronze_results:
            assert r.passed is True
            assert r.row_count == 50
            assert "OK" in r.status

    def test_fails_too_few_rows(self):
        """Row count of 5 with min_rows=20 should fail."""
        registry = _make_registry({"bronze": {"min_rows": 20}})
        spark = MagicMock()
        spark.sql.return_value.first.return_value = _mock_spark_row(5)

        validator = SLOValidator(registry, spark=spark)
        results = validator.check_row_counts()

        bronze_results = [r for r in results if r.layer == "bronze"]
        assert len(bronze_results) == 2
        for r in bronze_results:
            assert r.passed is False
            assert "TOO FEW" in r.status
            assert r.row_count == 5
            assert r.slo_min_rows == 20

    def test_fails_too_many_rows(self):
        """Row count of 2_000_000 with max_rows=1_000_000 should fail."""
        registry = _make_registry({"bronze": {"max_rows": 1_000_000}})
        spark = MagicMock()
        spark.sql.return_value.first.return_value = _mock_spark_row(2_000_000)

        validator = SLOValidator(registry, spark=spark)
        results = validator.check_row_counts()

        bronze_results = [r for r in results if r.layer == "bronze"]
        for r in bronze_results:
            assert r.passed is False
            assert "TOO MANY" in r.status

    def test_no_data_in_run_log(self):
        """No run log entry should report NO DATA failure."""
        registry = _make_registry({"bronze": {"min_rows": 20}})
        spark = MagicMock()
        spark.sql.return_value.first.return_value = None

        validator = SLOValidator(registry, spark=spark)
        results = validator.check_row_counts()

        bronze_results = [r for r in results if r.layer == "bronze"]
        assert len(bronze_results) == 2
        for r in bronze_results:
            assert r.passed is False
            assert "NO DATA" in r.status

    def test_excluded_entity(self):
        """Entities in exclude_tables should be skipped."""
        registry = _make_registry({"bronze": {"min_rows": 20, "exclude_tables": ["sessions"]}})
        spark = MagicMock()
        spark.sql.return_value.first.return_value = _mock_spark_row(50)

        validator = SLOValidator(registry, spark=spark)
        results = validator.check_row_counts()

        bronze_results = [r for r in results if r.layer == "bronze"]
        assert len(bronze_results) == 1  # only events, sessions excluded
        assert bronze_results[0].entity == "events"

    def test_no_spark_returns_empty(self):
        """Without a Spark session, check_row_counts returns empty."""
        registry = _make_registry({"bronze": {"min_rows": 20}})
        validator = SLOValidator(registry, spark=None)
        results = validator.check_row_counts()
        assert results == []

    def test_layer_without_config_skipped(self):
        """Layer without row_count config should produce no results."""
        registry = _make_registry({"silver": {"min_rows": 10}})
        spark = MagicMock()
        spark.sql.return_value.first.return_value = _mock_spark_row(50)

        validator = SLOValidator(registry, spark=spark)
        results = validator.check_row_counts()

        # Only silver entity should appear
        assert len(results) == 1
        assert results[0].layer == "silver"
        assert results[0].entity == "clean_events"
        assert results[0].passed is True

    def test_run_checks_includes_row_counts(self):
        """run_checks() should include row count results when configured."""
        registry = _make_registry({"bronze": {"min_rows": 20}})
        spark = MagicMock()
        spark.sql.return_value.first.return_value = _mock_spark_row(5)

        validator = SLOValidator(registry, spark=spark)
        report = validator.run_checks()

        assert isinstance(report, SLOReport)
        assert report.passed is False
        assert len(report.failures) >= 2  # at least the 2 bronze entities
        row_count_failures = [f for f in report.failures if "TOO FEW" in f.status]
        assert len(row_count_failures) == 2

    def test_spark_error_handled_gracefully(self):
        """Spark SQL errors should produce ERROR result, not raise."""
        registry = _make_registry({"bronze": {"min_rows": 20}})
        spark = MagicMock()
        spark.sql.side_effect = Exception("Table not found")

        validator = SLOValidator(registry, spark=spark)
        results = validator.check_row_counts()

        bronze_results = [r for r in results if r.layer == "bronze"]
        for r in bronze_results:
            assert r.passed is False
            assert "ERROR" in r.status


def test_check_schedule_on_time_late_and_environment_filter(monkeypatch):
    class FakeDateTime(datetime.datetime):
        current = datetime.datetime(2026, 3, 26, 5, 30, tzinfo=datetime.timezone.utc)

        @classmethod
        def now(cls, tz=None):
            return cls.current

    monkeypatch.setattr(slo.datetime, "datetime", FakeDateTime)

    schedule = SimpleNamespace(
        expected_completion_utc="06:00",
        expected_start_utc="05:00",
        expected_duration_minutes=None,
        warn_if_duration_exceeds_minutes=None,
        timezone="UTC",
        environments=["prod"],
        pipeline_cron=None,
    )
    registry = SimpleNamespace(slo=SimpleNamespace(schedule=schedule))
    validator = SLOValidator(registry)

    assert validator.check_schedule(environment="dev") == []

    on_time = validator.check_schedule(environment="prod")
    assert [result.status for result in on_time] == ["✅ ON TIME", "⚠️ Check if pipeline has started"]
    assert all(result.passed is True for result in on_time)

    FakeDateTime.current = datetime.datetime(2026, 3, 26, 6, 45, tzinfo=datetime.timezone.utc)
    late = validator.check_schedule(environment="prod")
    assert late[0].status == "❌ LATE by 45 min"
    assert late[0].passed is False


def test_check_quality_and_severity_breaches():
    quality = SimpleNamespace(
        min_good_ratio=0.95,
        max_quarantine_ratio=0.05,
        by_severity={
            "critical": SimpleNamespace(min_good_ratio=0.99),
            "low": SimpleNamespace(min_good_ratio=0.98),
        },
    )
    registry = SimpleNamespace(slo=SimpleNamespace(quality=quality))
    validator = SLOValidator(registry)

    report = {
        "counts": {"total": 100, "good": 90, "quarantined": 10},
        "rule_failures_by_severity": {"critical": 2, "low": 5},
    }
    results = validator.check_quality(report=report)

    assert results[0].check_type == "quality"
    assert results[0].passed is False
    assert "QUALITY" in results[0].status
    assert results[0].quality_ratio == 0.9
    assert results[1].entity == "severity_critical"
    assert results[1].severity == "fail"
    assert results[2].entity == "severity_low"
    assert results[2].severity == "warn"


def test_check_row_count_anomaly_for_median_and_spike():
    anomaly = SimpleNamespace(
        enabled=True,
        lookback_runs=4,
        min_ratio=0.5,
        max_ratio=1.5,
        method="median",
        min_runs_before_enforcement=3,
        check_field="counts_good",
    )
    registry = SimpleNamespace(storage=SimpleNamespace(run_log_table="catalog.logs"))
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = [
        {"cnt": 100},
        {"cnt": 120},
        {"cnt": 80},
        {"cnt": 100},
    ]

    validator = SLOValidator(registry, spark=spark)
    ok_result = validator.check_row_count_anomaly("orders", "bronze", 110, anomaly)
    assert ok_result.passed is True
    assert ok_result.anomaly_baseline == 100.0
    assert ok_result.anomaly_ratio == 1.1

    spike_result = validator.check_row_count_anomaly("orders", "bronze", 200, anomaly)
    assert spike_result.passed is False
    assert "VOLUME SPIKE" in spike_result.status


def test_check_row_count_anomaly_skips_without_enough_history():
    anomaly = SimpleNamespace(
        enabled=True,
        lookback_runs=4,
        min_ratio=0.5,
        max_ratio=1.5,
        method="rolling_average",
        min_runs_before_enforcement=5,
        check_field="counts_good",
    )
    registry = SimpleNamespace(storage=SimpleNamespace(run_log_table="catalog.logs"))
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = [{"cnt": 100}, {"cnt": 90}]

    validator = SLOValidator(registry, spark=spark)
    assert validator.check_row_count_anomaly("orders", "bronze", 110, anomaly) is None


def test_resolve_storage_opts_uses_explicit_options_and_cloud_resolver(monkeypatch):
    validator = SLOValidator(SimpleNamespace(), storage_options={"account_name": "demo"})
    monkeypatch.setattr(slo, "enrich_azure_storage_options", lambda opts: {**opts, "enriched": True})
    assert validator._resolve_storage_opts("abfss://container/path") == {"account_name": "demo", "enriched": True}

    cloud_calls = []
    fake_cloud_module = SimpleNamespace(
        resolve_storage_options=lambda path: cloud_calls.append(path) or {"token": "abc"}
    )
    monkeypatch.setitem(sys.modules, "lakelogic.engines.cloud_credentials", fake_cloud_module)
    validator = SLOValidator(SimpleNamespace())
    assert validator._resolve_storage_opts("abfss://container/path") == {"token": "abc", "enriched": True}
    assert cloud_calls == ["abfss://container/path"]


def test_check_freshness_spark_success_stale_and_missing_column(monkeypatch):
    now = datetime.datetime(2026, 3, 26, 12, 0, tzinfo=datetime.timezone.utc)

    class FakeDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    monkeypatch.setattr(slo.datetime, "datetime", FakeDateTime)
    monkeypatch.setattr(slo, "resolve_materialization_path", lambda **kwargs: f"/tmp/{kwargs['entity']}")
    monkeypatch.setattr(slo, "make_table_name", lambda layer, system, entity: f"{layer}_{entity}")
    monkeypatch.setattr(slo, "to_sql_table_ref", lambda path, engine: f"ref_{engine}_{path.rsplit('/', 1)[-1]}")

    contracts = [
        SimpleNamespace(layer="bronze", entity="events"),
        SimpleNamespace(layer="silver", entity="sessions"),
        SimpleNamespace(layer="gold", entity="missing"),
        SimpleNamespace(layer="bronze", entity="skip_me"),
    ]

    freshness = {
        "bronze": SimpleNamespace(max_delay_minutes=60, exclude_tables=["skip_me"], check_columns=["fresh_col"]),
        "silver": SimpleNamespace(max_delay_minutes=15, exclude_tables=[], check_columns=["fresh_col"]),
        "gold": SimpleNamespace(max_delay_minutes=30, exclude_tables=[], check_columns=["missing_col"]),
    }
    registry = SimpleNamespace(
        domain="commerce",
        system="erp",
        slo=SimpleNamespace(freshness=freshness),
        storage=SimpleNamespace(bronze_root="bronze_db", silver_root="silver_db", gold_root="gold_db"),
        get_active_contracts=lambda: contracts,
    )

    class FakeSparkResult:
        def __init__(self, row):
            self._row = row

        def first(self):
            return self._row

    queries = []

    def fake_sql(query):
        queries.append(query)
        if "bronze_db.bronze_events" in query:
            return FakeSparkResult({"latest_ts": now - datetime.timedelta(minutes=10)})
        if "silver_db.silver_sessions" in query:
            return FakeSparkResult({"latest_ts": now - datetime.timedelta(minutes=45)})
        raise RuntimeError("column not found")

    validator = SLOValidator(registry, spark=SimpleNamespace(sql=fake_sql))
    results = validator.check_freshness()

    by_entity = {result.entity: result for result in results}
    assert by_entity["events"].passed is True
    assert by_entity["events"].status == "✅ OK"
    assert by_entity["sessions"].passed is False
    assert by_entity["sessions"].status == "❌ STALE"
    assert by_entity["missing"].passed is False
    assert "ERROR" in by_entity["missing"].status
    assert "skip_me" not in by_entity
    assert len(queries) >= 3


def test_check_freshness_source_columns_pass_fail_and_skip(monkeypatch):
    """Source freshness: pass when fresh, fail when stale, skip when columns missing."""
    now = datetime.datetime(2026, 3, 26, 12, 0, tzinfo=datetime.timezone.utc)

    class FakeDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    monkeypatch.setattr(slo.datetime, "datetime", FakeDateTime)
    monkeypatch.setattr(slo, "resolve_materialization_path", lambda **kwargs: f"/tmp/{kwargs['entity']}")
    monkeypatch.setattr(slo, "make_table_name", lambda layer, system, entity: f"{layer}_{entity}")
    monkeypatch.setattr(slo, "to_sql_table_ref", lambda path, engine: f"ref_{engine}_{path.rsplit('/', 1)[-1]}")

    contracts = [
        SimpleNamespace(layer="bronze", entity="fresh_src"),  # source is fresh
        SimpleNamespace(layer="bronze", entity="stale_src"),  # source is stale
        SimpleNamespace(layer="bronze", entity="no_src_col"),  # no source columns found
    ]

    freshness = {
        "bronze": SimpleNamespace(
            exclude_tables=[],
            # One list, business timestamps first and the audit column last, and
            # one threshold — the source/data-staleness limit.
            max_delay_minutes=30,
            check_columns=["updated_at", "last_modified", "loaded_at"],
        ),
    }
    registry = SimpleNamespace(
        domain="commerce",
        system="erp",
        slo=SimpleNamespace(freshness=freshness),
        storage=SimpleNamespace(bronze_root="bronze_db", silver_root=None, gold_root=None),
        get_active_contracts=lambda: contracts,
    )

    class FakeSparkResult:
        def __init__(self, row):
            self._row = row

        def first(self):
            return self._row

    def fake_sql(query):
        # ONE check now: iterate check_columns in order, first that yields a
        # timestamp wins. Business columns first, the audit column last.
        if "MAX(updated_at)" in query:
            if "fresh_src" in query:
                return FakeSparkResult({"latest_ts": now - datetime.timedelta(minutes=5)})
            if "stale_src" in query:
                return FakeSparkResult({"latest_ts": now - datetime.timedelta(minutes=120)})
            if "no_src_col" in query:
                return FakeSparkResult({"latest_ts": None})  # present but all NULL
        if "MAX(last_modified)" in query:
            if "no_src_col" in query:
                raise RuntimeError("column not found")  # absent entirely
            return FakeSparkResult({"latest_ts": None})
        if "MAX(loaded_at)" in query or "MAX(_lakelogic_processed_at)" in query:
            return FakeSparkResult({"latest_ts": now - datetime.timedelta(minutes=10)})
        return FakeSparkResult({"latest_ts": None})

    validator = SLOValidator(registry, spark=SimpleNamespace(sql=fake_sql))
    results = validator.check_freshness()

    by_entity = {r.entity: r for r in results}

    # fresh_src: resolves the FIRST candidate (updated_at, 5 min < 30) → OK
    assert by_entity["fresh_src"].passed is True
    assert by_entity["fresh_src"].status == "✅ OK"
    assert by_entity["fresh_src"].source_column_used == "updated_at"
    assert by_entity["fresh_src"].source_delay_minutes == 5.0

    # stale_src: same column, 120 min > 30 → STALE. The business timestamp decides,
    # which is the point of ordering it ahead of the audit column: `loaded_at` is
    # 10 min old on every table here and would have masked this.
    assert by_entity["stale_src"].passed is False
    assert by_entity["stale_src"].status == "❌ STALE"
    assert by_entity["stale_src"].source_delay_minutes == 120.0

    # no_src_col: updated_at all-NULL, last_modified absent → falls back to the
    # audit column rather than being skipped, so the table is still measured.
    assert by_entity["no_src_col"].passed is True
    assert by_entity["no_src_col"].source_column_used == "loaded_at"


def test_check_freshness_duckdb_fallback_and_no_data(monkeypatch):
    now = datetime.datetime(2026, 3, 26, 12, 0, tzinfo=datetime.timezone.utc)

    class FakeDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    monkeypatch.setattr(slo.datetime, "datetime", FakeDateTime)
    monkeypatch.setattr(slo, "resolve_materialization_path", lambda **kwargs: f"/warehouse/{kwargs['entity']}")
    monkeypatch.setattr(slo, "make_table_name", lambda layer, system, entity: f"{layer}_{entity}")
    monkeypatch.setattr(slo, "to_sql_table_ref", lambda path, engine: path)

    registry = SimpleNamespace(
        domain="commerce",
        system="erp",
        slo=SimpleNamespace(
            freshness={"bronze": SimpleNamespace(max_delay_minutes=30, exclude_tables=[], check_columns=["loaded_at"])}
        ),
        storage=SimpleNamespace(bronze_root=None, silver_root=None, gold_root=None),
        get_active_contracts=lambda: [
            SimpleNamespace(layer="bronze", entity="orders"),
            SimpleNamespace(layer="bronze", entity="empty"),
        ],
    )

    class FakeDuckResult:
        def __init__(self, value):
            self._value = value

        def fetchone(self):
            return self._value

    calls = []

    class FakeDuckConnection:
        def execute(self, query):
            calls.append(query)
            if "delta_scan('/warehouse/orders')" in query:
                raise RuntimeError("not delta")
            if "parquet_scan('/warehouse/orders')" in query:
                return FakeDuckResult((now - datetime.timedelta(minutes=5),))
            if "delta_scan('/warehouse/empty')" in query:
                return FakeDuckResult((None,))
            return FakeDuckResult((None,))

    validator = SLOValidator(registry, duckdb_con=FakeDuckConnection())
    results = validator.check_freshness()

    by_entity = {result.entity: result for result in results}
    assert by_entity["orders"].passed is True
    assert by_entity["empty"].passed is False
    assert by_entity["empty"].status == "⚠️ NO DATA"
    assert any("delta_scan('/warehouse/orders')" in query for query in calls)
    assert any("parquet_scan('/warehouse/orders')" in query for query in calls)


def test_check_freshness_polars_uses_delta_then_parquet(monkeypatch):
    now = datetime.datetime(2026, 3, 26, 12, 0, tzinfo=datetime.timezone.utc)

    class FakeDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    monkeypatch.setattr(slo.datetime, "datetime", FakeDateTime)
    monkeypatch.setattr(slo, "resolve_materialization_path", lambda **kwargs: f"abfss://lake/{kwargs['entity']}")
    monkeypatch.setattr(slo, "make_table_name", lambda layer, system, entity: f"{layer}_{entity}")
    monkeypatch.setattr(slo, "to_sql_table_ref", lambda path, engine: path)
    monkeypatch.setattr(SLOValidator, "_resolve_storage_opts", lambda self, path: {"token": "abc"})

    registry = SimpleNamespace(
        domain="commerce",
        system="erp",
        slo=SimpleNamespace(
            freshness={
                "bronze": SimpleNamespace(
                    max_delay_minutes=20, exclude_tables=[], check_columns=["missing", "loaded_at"]
                )
            }
        ),
        storage=SimpleNamespace(bronze_root=None, silver_root=None, gold_root=None),
        get_active_contracts=lambda: [SimpleNamespace(layer="bronze", entity="orders")],
    )

    read_calls = []

    class FakeSelected:
        def __init__(self, value):
            self._value = value

        def item(self):
            return self._value

    class FakeFrame:
        def __init__(self, value):
            self._value = value

        def select(self, expr):
            return FakeSelected(self._value)

    fake_polars = SimpleNamespace(
        col=lambda name: SimpleNamespace(max=lambda: name),
        read_delta=lambda path, storage_options=None: (
            read_calls.append(("delta", path, storage_options)) or (_ for _ in ()).throw(RuntimeError("delta failed"))
        ),
        read_parquet=lambda path, storage_options=None: (
            read_calls.append(("parquet", path, storage_options)) or FakeFrame(now - datetime.timedelta(minutes=4))
        ),
    )
    monkeypatch.setitem(sys.modules, "polars", fake_polars)

    validator = SLOValidator(registry, polars=True)
    results = validator.check_freshness()

    assert len(results) == 1
    assert results[0].passed is True
    assert results[0].delay_minutes == 4.0
    assert read_calls[0][0] == "delta"
    assert read_calls[1][0] == "parquet"


def test_notify_breaches_uses_registered_targets_and_smtp_env(monkeypatch):
    sent = []
    added = []

    class FakeApprise:
        def add(self, target):
            added.append(target)

        def notify(self, body=None, title=None):
            sent.append((title, body))

    fake_apprise = SimpleNamespace(Apprise=lambda: FakeApprise())
    monkeypatch.setitem(sys.modules, "apprise", fake_apprise)
    monkeypatch.setenv("LAKELOGIC_SMTP_URI", "mailto://smtp.example.com")

    registry = SimpleNamespace(
        domain="marketing",
        notifications=[{"target": "slack://team", "on_events": ["slo_breach"]}],
        ownership={
            "contacts": [
                {
                    "slack": "slack://owner",
                    "teams": "teams://owner",
                    "webhook": "https://hook",
                    "email": "ops@example.com",
                }
            ]
        },
    )
    validator = SLOValidator(registry)

    validator.notify_breaches(
        [SLOCheckResult(layer="bronze", entity="orders", status="❌ STALE", passed=False, check_type="freshness")]
    )

    assert "slack://team" in added
    assert "slack://owner" in added
    assert "teams://owner" in added
    assert "https://hook" in added
    assert "mailto://smtp.example.com/ops@example.com" in added
    assert sent and "LakeLogic Domain SLO Breach (marketing)" == sent[0][0]


def test_check_row_counts_duckdb_polars_and_configuration_edges(monkeypatch):
    import polars as pl

    now = datetime.datetime(2026, 3, 26, 12, 0, tzinfo=datetime.timezone.utc)
    contracts = [
        SimpleNamespace(layer="bronze", entity="orders"),
        SimpleNamespace(layer="bronze", entity="empty"),
        SimpleNamespace(layer="silver", entity="skipped"),
    ]
    row_count = {
        "bronze": SimpleNamespace(
            min_rows=10,
            max_rows=100,
            check_field="counts_good",
            exclude_tables=[],
            anomaly=None,
        ),
        "silver": SimpleNamespace(
            min_rows=None,
            max_rows=None,
            check_field="counts_good",
            exclude_tables=[],
            anomaly=None,
        ),
    }
    registry = SimpleNamespace(
        slo=SimpleNamespace(row_count=row_count),
        storage=SimpleNamespace(run_log_table="run_logs"),
        get_active_contracts=lambda: contracts,
    )

    class DuckResult:
        def __init__(self, value):
            self._value = value

        def fetchone(self):
            return self._value

    class DuckCon:
        def execute(self, query):
            if "dataset = 'orders'" in query:
                return DuckResult((150, now))
            if "dataset = 'empty'" in query:
                return DuckResult(None)
            raise AssertionError(query)

    duck_results = SLOValidator(registry, duckdb_con=DuckCon()).check_row_counts()
    assert {r.entity: r.status for r in duck_results} == {
        "orders": "❌ TOO MANY ROWS (150 > 100)",
        "empty": "⚠️ NO DATA",
    }

    log_df = pl.DataFrame(
        {
            "data_layer": ["bronze", "bronze", "bronze"],
            "dataset": ["orders", "orders", "empty"],
            "stage": ["ok", "no_new_data", "ok"],
            "counts_good": [25, 999, None],
            "timestamp": [now, now + datetime.timedelta(minutes=1), now],
        }
    )
    monkeypatch.setattr(SLOValidator, "_resolve_storage_opts", lambda self, path: {})
    monkeypatch.setattr(slo, "_read_delta_local", lambda path, storage_options=None: log_df)
    polars_results = SLOValidator(registry, polars=True).check_row_counts()
    by_entity = {r.entity: r for r in polars_results}
    assert by_entity["orders"].passed is True
    assert by_entity["orders"].row_count == 25
    assert by_entity["empty"].status == "⚠️ NO DATA"

    missing_run_log = SimpleNamespace(
        slo=SimpleNamespace(row_count=row_count),
        storage=SimpleNamespace(run_log_table=None),
        get_active_contracts=lambda: contracts,
    )
    assert SLOValidator(missing_run_log, polars=True).check_row_counts() == []


def test_row_count_anomaly_duckdb_polars_and_disabled_edges(monkeypatch):
    import polars as pl

    anomaly = SimpleNamespace(
        enabled=True,
        lookback_runs=4,
        min_ratio=0.5,
        max_ratio=1.5,
        method="rolling_average",
        min_runs_before_enforcement=2,
        check_field="counts_good",
    )
    registry = SimpleNamespace(storage=SimpleNamespace(run_log_table="run_logs"))

    class DuckRows:
        def __init__(self, values):
            self._values = values

        def fetchall(self):
            return [(v,) for v in self._values]

    duck_validator = SLOValidator(
        registry,
        duckdb_con=SimpleNamespace(execute=lambda query: DuckRows([100, 100, 100, None])),
    )
    drop = duck_validator.check_row_count_anomaly("orders", "bronze", 10, anomaly)
    assert drop is not None
    assert drop.passed is False
    assert "VOLUME DROP" in drop.status

    zero_validator = SLOValidator(
        registry,
        duckdb_con=SimpleNamespace(execute=lambda query: DuckRows([0, 0, 0])),
    )
    assert zero_validator.check_row_count_anomaly("orders", "bronze", 10, anomaly) is None
    assert (
        SLOValidator(registry, duckdb_con=zero_validator.duckdb_con).check_row_count_anomaly(
            "orders", "bronze", 10, SimpleNamespace(enabled=False)
        )
        is None
    )
    assert (
        SLOValidator(
            SimpleNamespace(storage=SimpleNamespace(run_log_table=None)), duckdb_con=zero_validator.duckdb_con
        ).check_row_count_anomaly("orders", "bronze", 10, anomaly)
        is None
    )

    log_df = pl.DataFrame(
        {
            "data_layer": ["bronze", "bronze", "bronze", "silver"],
            "dataset": ["orders", "orders", "orders", "orders"],
            "stage": ["ok", "reprocess", "ok", "ok"],
            "counts_good": [90, 999, 110, 1000],
            "timestamp": [3, 4, 2, 1],
        }
    )
    monkeypatch.setattr(SLOValidator, "_resolve_storage_opts", lambda self, path: {})
    monkeypatch.setattr(slo, "_read_delta_local", lambda path, storage_options=None: log_df)
    ok = SLOValidator(registry, polars=True).check_row_count_anomaly("orders", "bronze", 100, anomaly)
    assert ok is not None
    assert ok.passed is True
    assert ok.anomaly_baseline == 100.0


def test_check_retention_duckdb_and_polars_paths(monkeypatch):
    import polars as pl

    now = datetime.datetime(2026, 3, 26, 12, 0, tzinfo=datetime.timezone.utc)

    class FakeDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    monkeypatch.setattr(slo.datetime, "datetime", FakeDateTime)
    monkeypatch.setattr(slo, "resolve_materialization_path", lambda **kwargs: f"/warehouse/{kwargs['entity']}")
    monkeypatch.setattr(slo, "make_table_name", lambda layer, system, entity: f"{layer}_{entity}")
    monkeypatch.setattr(slo, "to_sql_table_ref", lambda path, engine: path)

    contracts = [
        SimpleNamespace(layer="bronze", entity="fresh"),
        SimpleNamespace(layer="bronze", entity="old"),
        SimpleNamespace(layer="bronze", entity="none"),
        SimpleNamespace(layer="silver", entity="no_source_columns"),
        SimpleNamespace(layer="gold", entity="bad_period"),
    ]
    freshness = {
        "bronze": SimpleNamespace(check_columns=["event_ts"]),
        "silver": SimpleNamespace(check_columns=[]),
        "gold": SimpleNamespace(check_columns=["event_ts"]),
    }
    registry = SimpleNamespace(
        domain="commerce",
        system="erp",
        retention={"bronze": "PT1H", "silver": "P1D", "gold": "not-a-period"},
        slo=SimpleNamespace(freshness=freshness),
        storage=SimpleNamespace(bronze_root=None, silver_root=None, gold_root=None),
        get_active_contracts=lambda: contracts,
    )

    class DuckResult:
        def __init__(self, value):
            self._value = value

        def fetchone(self):
            return self._value

    class DuckCon:
        def execute(self, query):
            if "delta_scan('/warehouse/fresh')" in query:
                raise RuntimeError("delta unavailable")
            if "parquet_scan('/warehouse/fresh')" in query:
                return DuckResult((now - datetime.timedelta(minutes=30),))
            if "delta_scan('/warehouse/old')" in query:
                return DuckResult((now - datetime.timedelta(hours=3),))
            if "delta_scan('/warehouse/none')" in query:
                return DuckResult((None,))
            raise AssertionError(query)

    duck_results = SLOValidator(registry, duckdb_con=DuckCon()).check_retention()
    by_entity = {r.entity: r for r in duck_results}
    assert by_entity["fresh"].passed is True
    assert by_entity["old"].passed is False
    assert "RETENTION BREACH" in by_entity["old"].status
    assert "none" not in by_entity
    assert "no_source_columns" not in by_entity
    assert "bad_period" not in by_entity

    monkeypatch.setattr(SLOValidator, "_resolve_storage_opts", lambda self, path: {})
    monkeypatch.setattr(
        slo, "_read_delta_local", lambda path, storage_options=None: (_ for _ in ()).throw(RuntimeError("delta"))
    )
    parquet = pl.DataFrame({"event_ts": [now - datetime.timedelta(minutes=45)]})
    monkeypatch.setattr(pl, "read_parquet", lambda path, storage_options=None: parquet)
    polars_results = SLOValidator(registry, polars=True).check_retention()
    assert {r.entity for r in polars_results} == {"fresh", "old", "none"}
    assert all(r.passed for r in polars_results)


def test_coerce_utc_treats_naive_timestamps_as_utc_not_host_local():
    """Regression guard for the tz bug that was green on UTC CI but wrong everywhere else.

    ``polars`` strips the timezone when it casts a column to ``pl.Datetime``, so the
    freshness/retention paths receive a *naive* datetime holding the UTC wall-clock. The old
    code ran it through ``datetime.fromtimestamp(ts.timestamp(), tz=utc)``, which interprets a
    naive value in the **host** timezone — silently shifting the record by the machine's UTC
    offset (retention breaches miscomputed on any non-UTC operator; green only on UTC CI).
    This test forces a non-UTC local zone so a reintroduction is caught even on UTC CI.
    """
    import datetime as dt
    import os
    import time

    naive = dt.datetime(2026, 3, 26, 11, 15, 0)  # a UTC wall-clock, tz stripped by polars
    expected = naive.replace(tzinfo=dt.timezone.utc)

    # A naive value is stamped UTC, never localized.
    assert slo._coerce_utc(naive) == expected
    # An aware value in another zone is converted to the same instant in UTC.
    aware = dt.datetime(2026, 3, 26, 14, 15, 0, tzinfo=dt.timezone(dt.timedelta(hours=3)))
    assert slo._coerce_utc(aware) == expected
    # ISO strings keep working (naive → UTC, aware → converted).
    assert slo._coerce_utc("2026-03-26T11:15:00") == expected
    assert slo._coerce_utc("2026-03-26T11:15:00Z") == expected

    # Under a forced non-UTC host zone the naive value must STILL be treated as UTC — the
    # assertion the original bug failed (only) off UTC. Skipped where tzset is absent (Windows).
    if not hasattr(time, "tzset"):
        return
    prior = os.environ.get("TZ")
    try:
        os.environ["TZ"] = "America/New_York"  # UTC-4/-5, definitively not UTC
        time.tzset()
        assert slo._coerce_utc(naive) == expected
    finally:
        if prior is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = prior
        time.tzset()


def test_run_checks_includes_retention_and_tolerates_write_failure(monkeypatch):
    registry = SimpleNamespace(
        domain="marketing",
        system="ads",
        retention={"bronze": "PT1H"},
        slo=SimpleNamespace(freshness={}, row_count={}, schedule=None, quality=None),
    )
    validator = SLOValidator(registry)
    monkeypatch.setattr(
        validator,
        "check_retention",
        lambda: [
            SLOCheckResult(
                layer="bronze",
                entity="orders",
                check_type="retention",
                status="❌ RETENTION BREACH",
                passed=False,
            )
        ],
    )
    monkeypatch.setattr(
        "lakelogic.core.run_log.write_slo_checks", lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom"))
    )

    report = validator.run_checks(pipeline_run_id="pipe-1")

    assert report.passed is False
    assert report.pipeline_run_id == "pipe-1"
    assert report.failures[0].check_type == "retention"


def test_notify_breaches_noop_import_error_and_missing_smtp(monkeypatch):
    validator = SLOValidator(SimpleNamespace(domain="marketing", notifications=[], ownership={}))
    validator.notify_breaches([SLOCheckResult(layer="bronze", entity="orders", status="OK", passed=True)])

    monkeypatch.delitem(sys.modules, "apprise", raising=False)
    monkeypatch.setitem(sys.modules, "apprise", None)
    validator.notify_breaches([SLOCheckResult(layer="bronze", entity="orders", status="bad", passed=False)])

    added = []

    class FakeApprise:
        def add(self, target):
            added.append(target)

        def notify(self, body=None, title=None):
            raise AssertionError("should not notify without valid channels")

    monkeypatch.setitem(sys.modules, "apprise", SimpleNamespace(Apprise=lambda: FakeApprise()))
    monkeypatch.delenv("LAKELOGIC_SMTP_URI", raising=False)
    no_channels = SimpleNamespace(
        domain="marketing",
        notifications=[{"target": "slack://ignored", "on_events": ["pipeline_done"]}],
        ownership={"contacts": [{"email": "ops@example.com"}]},
    )
    SLOValidator(no_channels).notify_breaches(
        [SLOCheckResult(layer="bronze", entity="orders", status="bad", passed=False)]
    )
    assert added == []


def test_standalone_slo_helpers_parse_coerce_and_compute_polars(monkeypatch):
    import polars as pl

    now = datetime.datetime(2026, 3, 26, 12, 0, tzinfo=datetime.timezone.utc)

    class FakeDateTime(datetime.datetime):
        @classmethod
        def now(cls, tz=None):
            return now

    monkeypatch.setattr(slo.datetime, "datetime", FakeDateTime)

    assert slo._parse_duration_seconds(None) is None
    assert slo._parse_duration_seconds(2) == 7200.0
    assert slo._parse_duration_seconds("2h") == 7200.0
    assert slo._parse_duration_seconds("15m") == 900.0
    assert slo._parse_duration_seconds("30s") == 30.0
    assert slo._parse_duration_seconds("1d") == 86400.0
    assert slo._parse_duration_seconds("3") == 10800.0
    assert slo._parse_duration_seconds("nope") is None

    assert slo._coerce_datetime(None) is None
    assert slo._coerce_datetime("bad-date") is None
    assert slo._coerce_datetime("2026-03-26T11:30:00Z").tzinfo is not None
    assert slo._coerce_datetime(datetime.datetime(2026, 3, 26, 11, 30)).tzinfo is not None

    df = pl.DataFrame(
        {
            "loaded_at": [now - datetime.timedelta(minutes=10), now - datetime.timedelta(minutes=5)],
            "source_at": [now - datetime.timedelta(minutes=40), now - datetime.timedelta(minutes=30)],
            "present": [1, None],
        }
    )
    contract = SimpleNamespace(
        service_levels={
            "freshness": {
                "field": "loaded_at",
                "threshold": "10m",
                "source_field": "source_at",
                "source_threshold": "20m",
            },
            "availability": {"field": "present", "threshold": 75},
        }
    )

    result = slo.compute_slos(contract, df.lazy(), {"good": 2}, "polars")

    assert result["freshness"]["passed"] is True
    assert result["freshness"]["source_passed"] is False
    assert result["availability"] == {
        "field": "present",
        "threshold": 75.0,
        "actual_pct": 50.0,
        "passed": False,
    }
    assert slo._get_max_timestamp(df, "missing", "polars") is None
    assert slo._non_null_ratio(pl.DataFrame(), "present", "polars") is None
    assert slo._compute_freshness(df, None, "polars") == {}
    assert slo._compute_freshness(df, {}, "polars") == {}
    assert slo._compute_freshness(df, {"field": "missing", "threshold": "1h"}, "polars") == {
        "field": "missing",
        "passed": False,
        "reason": "no_data_or_threshold",
    }
    assert slo._compute_availability(df, {}, None, "polars") == {}
    assert slo._compute_availability(df, {}, {"field": "missing", "threshold": 90}, "polars") == {
        "field": "missing",
        "passed": False,
        "reason": "no_data",
    }


def test_standalone_slo_helpers_duckdb_and_merge_paths():
    duckdb = pytest.importorskip("duckdb")

    con = duckdb.connect(database=":memory:")
    rel = con.sql(
        """
        SELECT * FROM (
            VALUES
                (TIMESTAMP '2026-03-26 11:00:00', 1),
                (TIMESTAMP '2026-03-26 11:30:00', NULL)
        ) AS t(loaded_at, present)
        """
    )

    assert slo._get_max_timestamp(rel, "loaded_at", "duckdb").isoformat().startswith("2026-03-26T11:30")
    assert slo._non_null_ratio(rel, "present", "duckdb") == 0.5
    assert slo._get_max_timestamp(object(), "loaded_at", "unknown") is None
    assert slo._non_null_ratio(object(), "present", "unknown") is None

    contract_slo = SimpleNamespace(
        model_dump=lambda: {
            "freshness": {"field": "contract_loaded_at"},
            "availability": None,
            "row_count": {"min": 5},
        }
    )
    merged = slo._merge_slo_config(
        {"freshness": {"threshold": "1h"}, "availability": {"field": "id", "threshold": 90}},
        contract_slo,
    )
    assert merged == {
        "freshness": {"threshold": "1h", "field": "contract_loaded_at"},
        "availability": {"field": "id", "threshold": 90},
        "row_count": {"min": 5},
    }

    registry_slo = SimpleNamespace(model_dump=lambda: {"freshness": {"field": "missing", "threshold": "1h"}})
    assert (
        slo.compute_slos(SimpleNamespace(service_levels=None), rel, {}, "duckdb", registry_slo)["freshness"]["passed"]
        is False
    )
    assert slo.compute_slos(SimpleNamespace(service_levels=None), rel, {}, "duckdb") == {}

    con.close()


# ── Quality gate: all-quarantined leak (SLOValidator._evaluate_quality_counts) ──
#
# Regression for the "everything blocked but SLO shows green" gap. The shared gate
# is used by BOTH the post-pipeline (report) and out-of-band (run-log) paths, so
# testing it directly covers both.


class TestQualityGate:
    """SLOValidator._evaluate_quality_counts — the shared quality gate."""

    @staticmethod
    def _quality(min_good=0.9, max_quar=0.1):
        return SimpleNamespace(min_good_ratio=min_good, max_quarantine_ratio=max_quar, by_severity=None)

    def test_all_quarantined_warns_without_quality_config(self):
        # good=0, total>0 and NO quality SLO configured → non-blocking WARN.
        # Total data loss stays visible, but a domain that never opted into
        # quality gating is not failed.
        res = SLOValidator._evaluate_quality_counts("dim_rider", {"total": 100, "good": 0, "quarantined": 100}, None)
        assert len(res) == 1
        r = res[0]
        assert r.passed is True  # non-blocking
        assert r.severity == "warn"
        assert r.quality_ratio == 0.0
        assert "ALL ROWS QUARANTINED" in r.status

    def test_all_quarantined_fails_with_quality_config(self):
        # With a quality SLO configured (opted in) → hard FAIL.
        res = SLOValidator._evaluate_quality_counts(
            "dim_rider", {"total": 100, "good": 0, "quarantined": 100}, self._quality()
        )
        assert len(res) == 1
        assert res[0].passed is False and res[0].severity == "fail"

    def test_all_quarantined_uses_source_when_total_missing(self):
        # Run log may carry counts_source but a null counts_total. With a quality
        # SLO configured, this is a hard fail.
        res = SLOValidator._evaluate_quality_counts(
            "dim_rider", {"total": None, "source": 50, "good": 0, "quarantined": 50}, self._quality()
        )
        assert len(res) == 1 and res[0].passed is False

    def test_empty_run_is_benign_no_result(self):
        # total==0 (no_new_data / empty tick) → no quality result at all.
        # Prolonged absence is the freshness SLO's responsibility, not this gate.
        res = SLOValidator._evaluate_quality_counts(
            "dim_rider", {"total": 0, "good": 0, "quarantined": 0}, self._quality()
        )
        assert res == []

    def test_healthy_run_passes(self):
        res = SLOValidator._evaluate_quality_counts(
            "dim_rider", {"total": 100, "good": 98, "quarantined": 2}, self._quality()
        )
        assert len(res) == 1 and res[0].passed is True

    def test_partial_quarantine_breach_fails_with_config(self):
        # good_ratio 0.5 < 0.9 → threshold breach fails (but not the unconditional path).
        res = SLOValidator._evaluate_quality_counts(
            "dim_rider", {"total": 100, "good": 50, "quarantined": 50}, self._quality()
        )
        assert len(res) == 1 and res[0].passed is False
        assert "ALL ROWS QUARANTINED" not in res[0].status

    def test_partial_quarantine_without_config_is_not_flagged(self):
        # good>0 and no quality SLO configured → gate is silent (only good==0 is
        # unconditional). Avoids false positives on runs that never opted into a
        # quality threshold.
        res = SLOValidator._evaluate_quality_counts("dim_rider", {"total": 100, "good": 50, "quarantined": 50}, None)
        assert res == []
