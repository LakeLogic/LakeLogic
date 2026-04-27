"""Tests for SLOValidator.check_row_counts() — system-level row count SLO."""
from __future__ import annotations

import datetime
import os
import sys
from unittest.mock import MagicMock, patch, PropertyMock
from types import SimpleNamespace

import pytest

from lakelogic.core import slo
from lakelogic.core.slo import SLOValidator, SLOCheckResult, SLOReport


# ── helpers ──────────────────────────────────────────────────────────────────


def _make_registry(row_count_cfg: dict | None = None, contracts=None):
    """Build a minimal DomainRegistry-like object for SLO tests."""
    from lakelogic.core.registry import (
        DomainRegistry,
        RegistrySLO,
        RegistryStorage,
        RegistryContract,
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
                layer="bronze", entity="events", path="dummy.yaml",
                enabled=True, contract_dict={"info": {}},
            ),
            RegistryContract(
                layer="bronze", entity="sessions", path="dummy.yaml",
                enabled=True, contract_dict={"info": {}},
            ),
            RegistryContract(
                layer="silver", entity="clean_events", path="dummy.yaml",
                enabled=True, contract_dict={"info": {}},
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
        registry = _make_registry({
            "bronze": {"min_rows": 20, "exclude_tables": ["sessions"]}
        })
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
    fake_cloud_module = SimpleNamespace(resolve_storage_options=lambda path: cloud_calls.append(path) or {"token": "abc"})
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
        "bronze": SimpleNamespace(max_delay_minutes=60, exclude_tables=["skip_me"], check_column=["fresh_col"]),
        "silver": SimpleNamespace(max_delay_minutes=15, exclude_tables=[], check_column="fresh_col"),
        "gold": SimpleNamespace(max_delay_minutes=30, exclude_tables=[], check_column=["missing_col"]),
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
        SimpleNamespace(layer="bronze", entity="fresh_src"),    # source is fresh
        SimpleNamespace(layer="bronze", entity="stale_src"),    # source is stale
        SimpleNamespace(layer="bronze", entity="no_src_col"),   # no source columns found
    ]

    freshness = {
        "bronze": SimpleNamespace(
            max_delay_minutes=60,
            exclude_tables=[],
            check_column="loaded_at",
            max_source_delay_minutes=30,  # source must be < 30 min old
            source_check_columns=["updated_at", "last_modified"],
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
        # Pipeline freshness — all tables are fresh (10 min delay)
        if "MAX(loaded_at)" in query or "MAX(_lakelogic_loaded_at)" in query:
            return FakeSparkResult({"latest_ts": now - datetime.timedelta(minutes=10)})
        # Source freshness via TRY_CAST
        if "TRY_CAST(updated_at" in query:
            if "fresh_src" in query:
                return FakeSparkResult({"src_ts": now - datetime.timedelta(minutes=5)})
            if "stale_src" in query:
                return FakeSparkResult({"src_ts": now - datetime.timedelta(minutes=120)})
            if "no_src_col" in query:
                return FakeSparkResult({"src_ts": None})  # column exists but all NULL
        if "TRY_CAST(last_modified" in query:
            if "no_src_col" in query:
                raise RuntimeError("column not found")  # doesn't exist either
            return FakeSparkResult({"src_ts": None})
        return FakeSparkResult({"latest_ts": None})

    validator = SLOValidator(registry, spark=SimpleNamespace(sql=fake_sql))
    results = validator.check_freshness()

    by_entity = {r.entity: r for r in results}

    # fresh_src: pipeline OK (10 min < 60 min), source OK (5 min < 30 min)
    assert by_entity["fresh_src"].passed is True
    assert by_entity["fresh_src"].status == "✅ OK"
    assert by_entity["fresh_src"].source_passed is True
    assert by_entity["fresh_src"].source_column_used == "updated_at"
    assert by_entity["fresh_src"].source_delay_minutes == 5.0

    # stale_src: pipeline OK (10 min < 60 min), but source STALE (120 min > 30 min)
    assert by_entity["stale_src"].passed is False
    assert "SOURCE STALE" in by_entity["stale_src"].status
    assert by_entity["stale_src"].source_passed is False
    assert by_entity["stale_src"].source_column_used == "updated_at"
    assert by_entity["stale_src"].source_delay_minutes == 120.0

    # no_src_col: pipeline OK, source skipped (no valid columns) — overall passes
    assert by_entity["no_src_col"].passed is True
    assert by_entity["no_src_col"].source_passed is None
    assert by_entity["no_src_col"].source_column_used is None


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
        slo=SimpleNamespace(freshness={"bronze": SimpleNamespace(max_delay_minutes=30, exclude_tables=[], check_column="loaded_at")}),
        storage=SimpleNamespace(bronze_root=None, silver_root=None, gold_root=None),
        get_active_contracts=lambda: [SimpleNamespace(layer="bronze", entity="orders"), SimpleNamespace(layer="bronze", entity="empty")],
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
        slo=SimpleNamespace(freshness={"bronze": SimpleNamespace(max_delay_minutes=20, exclude_tables=[], check_column=["missing", "loaded_at"])}),
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
        read_delta=lambda path, storage_options=None: read_calls.append(("delta", path, storage_options)) or (_ for _ in ()).throw(RuntimeError("delta failed")),
        read_parquet=lambda path, storage_options=None: read_calls.append(("parquet", path, storage_options)) or FakeFrame(now - datetime.timedelta(minutes=4)),
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
                {"slack": "slack://owner", "teams": "teams://owner", "webhook": "https://hook", "email": "ops@example.com"}
            ]
        },
    )
    validator = SLOValidator(registry)

    validator.notify_breaches([
        SLOCheckResult(layer="bronze", entity="orders", status="❌ STALE", passed=False, check_type="freshness")
    ])

    assert "slack://team" in added
    assert "slack://owner" in added
    assert "teams://owner" in added
    assert "https://hook" in added
    assert "mailto://smtp.example.com/ops@example.com" in added
    assert sent and "LakeLogic Domain SLO Breach (marketing)" == sent[0][0]
