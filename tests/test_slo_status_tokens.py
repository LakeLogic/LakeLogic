"""Stored SLO statuses are plain tokens; icons live only in display.

`SLOCheckResult.status` is persisted to `_slo_checks`, `details_json` and retention
evidence. It used to hold "✅ OK (8194 rows)" / "⚠️ NO DATA" — icons that broke
queries and encodings, and counts that belonged in their own columns. It is now one
token from `SLO_STATUSES`; the sentence is `message`; `format_status` adds icons.

And no target is not a huge target: freshness with no declared limit used to judge
against 999999 minutes and report OK. It is now NOT_SET, `passed=None`, never a breach.
"""

from __future__ import annotations

import datetime
import sqlite3
from types import SimpleNamespace

import pytest

from lakelogic.core import slo
from lakelogic.core.slo import (
    SLO_STATUSES,
    SLOCheckResult,
    SLOValidator,
    format_status,
    normalize_status,
    status_icon,
)


def _is_token(value: str) -> bool:
    return value in SLO_STATUSES and value.isascii() and value == value.upper() and " " not in value


# ── the vocabulary ────────────────────────────────────────────────────────────


def test_every_token_is_plain_ascii_uppercase():
    for token in SLO_STATUSES:
        assert _is_token(token), token


@pytest.mark.parametrize(
    "legacy, token",
    [
        ("✅ OK", "OK"),
        ("✅ OK (8194 rows)", "OK"),
        ("❌ STALE", "STALE"),
        ("⚠️ NO DATA", "NO_DATA"),
        ("⚠️ ERROR: table not found", "ERROR"),
        ("❌ TOO FEW ROWS (3 < 10)", "TOO_FEW_ROWS"),
        ("❌ TOO MANY ROWS (150 > 100)", "TOO_MANY_ROWS"),
        ("❌ VOLUME DROP — CRITICAL (0.10x < 0.5x median)", "VOLUME_DROP"),
        ("❌ VOLUME SPIKE (3.00x > 2.0x median)", "VOLUME_SPIKE"),
        ("❌ ALL ROWS QUARANTINED (5 of 5 blocked, 0 materialized)", "ALL_QUARANTINED"),
        ("❌ QUALITY (55.0% good, 45.0% quarantined)", "QUALITY_BREACH"),
        ("❌ HIGH quality breach (80.0% < 90.0%)", "QUALITY_BREACH"),
        ("❌ RETENTION BREACH: oldest record 9.0 days exceeds P7D", "BREACHED"),
        ("RETENTION BREACH: oldest record 12000min exceeds P7D", "BREACHED"),
        ("⏭ SKIPPED — no last_modified available", "SKIPPED"),
        ("✅ BASELINE SET (3 columns recorded)", "BASELINE_SET"),
        ("✅ NO DRIFT", "OK"),
        ("⚠ WARNING DRIFT — added: x", "SCHEMA_DRIFT"),
        ("⚠ WARN (90min delay approaching limit 120min)", "WARN"),
        ("OK", "OK"),
        ("NOT_SET", "NOT_SET"),
    ],
)
def test_normalise_maps_old_icon_strings_and_new_tokens(legacy, token):
    assert normalize_status(legacy) == token


def test_normalise_unknown_and_none():
    assert normalize_status(None) is None
    assert normalize_status("something else entirely") is None


def test_display_adds_icons_storage_does_not():
    ok = SLOCheckResult(layer="bronze", entity="t", status="OK", passed=True, message="8194 rows")
    stale = SLOCheckResult(layer="bronze", entity="t", status="STALE", passed=False, message="latest 90 min")
    unset = SLOCheckResult(layer="gold", entity="t", status="NOT_SET", passed=None)
    advisory = SLOCheckResult(
        layer="quality", entity="t", status="ALL_QUARANTINED", passed=True, severity="warn", message="5 of 5"
    )
    assert format_status(ok) == "✅ OK (8194 rows)"
    assert format_status(stale) == "❌ STALE (latest 90 min)"
    assert format_status(unset) == "➖ NOT_SET"
    assert format_status(advisory).startswith("⚠️ ALL_QUARANTINED")
    assert status_icon("NO_DATA") == "⚠️"
    assert status_icon("BREACHED", False) == "❌"
    # The stored value is untouched by display.
    assert ok.status == "OK"


# ── every check type stores a token and a message ───────────────────────────────


class _FakeSpark:
    def __init__(self, handler):
        self._handler = handler

    def sql(self, query):
        row = self._handler(query)
        return SimpleNamespace(first=lambda: row)


def _freshness_registry(freshness, contracts):
    return SimpleNamespace(
        domain="d",
        system="s",
        slo=SimpleNamespace(freshness=freshness),
        storage=SimpleNamespace(bronze_root="b", silver_root="s", gold_root="g"),
        get_active_contracts=lambda: contracts,
    )


def _patch_paths(monkeypatch):
    monkeypatch.setattr(slo, "resolve_materialization_path", lambda **kw: None)
    monkeypatch.setattr(slo, "make_table_name", lambda layer, system, entity: f"{layer}_{entity}")


def test_freshness_tokens_and_messages(monkeypatch):
    _patch_paths(monkeypatch)
    now = datetime.datetime.now(datetime.timezone.utc)
    freshness = {
        "bronze": SimpleNamespace(max_delay_minutes=60, exclude_tables=[], check_columns=["ts"]),
        "silver": SimpleNamespace(max_delay_minutes=15, exclude_tables=[], check_columns=["ts"]),
    }
    contracts = [
        SimpleNamespace(layer="bronze", entity="fresh"),
        SimpleNamespace(layer="silver", entity="stale"),
        SimpleNamespace(layer="silver", entity="broken"),
    ]

    def handler(q):
        if "b.bronze_fresh" in q:
            return {"latest_ts": now - datetime.timedelta(minutes=5)}
        if "s.silver_stale" in q:
            return {"latest_ts": now - datetime.timedelta(minutes=90)}
        raise RuntimeError("⚠ boom")

    results = SLOValidator(_freshness_registry(freshness, contracts), spark=_FakeSpark(handler)).check_freshness()
    by = {r.entity: r for r in results}
    assert by["fresh"].status == "OK" and by["fresh"].passed is True
    assert by["stale"].status == "STALE" and by["stale"].passed is False
    assert by["broken"].status == "ERROR"
    for r in results:
        assert _is_token(r.status), r.status
        assert r.message, r
    assert "limit 60 min" in by["fresh"].message


def test_freshness_without_a_target_is_not_set_not_ok(monkeypatch):
    """Gold declared no freshness objective: the old code judged it against 999999."""
    _patch_paths(monkeypatch)
    now = datetime.datetime.now(datetime.timezone.utc)
    freshness = {"bronze": SimpleNamespace(max_delay_minutes=60, exclude_tables=[], check_columns=["ts"])}
    contracts = [SimpleNamespace(layer="gold", entity="fact_trips")]

    spark = _FakeSpark(lambda q: {"latest_ts": now - datetime.timedelta(minutes=5)})
    registry = _freshness_registry(freshness, contracts)
    (r,) = SLOValidator(registry, spark=spark).check_freshness()
    assert r.status == "NOT_SET"
    assert r.passed is None
    assert r.slo_max_minutes is None
    assert r.source_slo_max_minutes is None
    assert r.delay_minutes is not None  # measured age is still reported
    assert "no freshness target" in r.message


def test_run_checks_does_not_count_not_set_as_a_failure(monkeypatch):
    _patch_paths(monkeypatch)
    now = datetime.datetime.now(datetime.timezone.utc)
    registry = _freshness_registry({}, [SimpleNamespace(layer="gold", entity="fact_trips")])
    registry.slo = SimpleNamespace(freshness={"x": None}, row_count=None, quality=None)
    registry.retention = None
    monkeypatch.setattr("lakelogic.core.run_log.write_slo_checks", lambda *a, **k: None)
    spark = _FakeSpark(lambda q: {"latest_ts": now - datetime.timedelta(minutes=5)})
    report = SLOValidator(registry, spark=spark).run_checks()
    assert [r.status for r in report.results] == ["NOT_SET"]
    assert report.failures == []
    assert report.passed is True


def test_row_count_quality_retention_anomaly_tokens():
    # quality, all three shapes
    q = SimpleNamespace(min_good_ratio=0.9, max_quarantine_ratio=0.1, by_severity=None)
    ok = SLOValidator._evaluate_quality_counts("e", {"total": 10, "good": 10, "quarantined": 0}, q)[0]
    bad = SLOValidator._evaluate_quality_counts("e", {"total": 10, "good": 5, "quarantined": 5}, q)[0]
    lost = SLOValidator._evaluate_quality_counts("e", {"total": 5, "good": 0, "quarantined": 5}, q)[0]
    lost_unset = SLOValidator._evaluate_quality_counts("e", {"total": 5, "good": 0, "quarantined": 5}, None)[0]
    assert (ok.status, bad.status, lost.status, lost_unset.status) == (
        "OK",
        "QUALITY_BREACH",
        "ALL_QUARANTINED",
        "ALL_QUARANTINED",
    )
    assert lost_unset.passed is True and "no quality SLO set" in lost_unset.message
    for r in (ok, bad, lost, lost_unset):
        assert _is_token(r.status) and r.message

    # anomaly verdict tokens come from the one rulebook
    from lakelogic.core.volume_baseline import verdict_status

    assert verdict_status(SimpleNamespace(passed=True, direction=None)) == "OK"
    assert verdict_status(SimpleNamespace(passed=False, direction="drop")) == "VOLUME_DROP"
    assert verdict_status(SimpleNamespace(passed=False, direction="spike")) == "VOLUME_SPIKE"


def test_scanner_results_store_tokens():
    from lakelogic.scanner import validator as sv

    src = open(sv.__file__, encoding="utf-8").read()
    # No icon literal is assigned to a stored status anywhere in the scanner.
    for icon in ("✅", "❌", "⚠", "⏭"):
        assert f'status=f"{icon}' not in src and f'status="{icon}' not in src


def test_no_icon_literal_is_assigned_to_status_in_slo_module():
    src = open(slo.__file__, encoding="utf-8").read()
    for icon in ("✅", "❌", "⚠", "⏭"):
        assert f'status=f"{icon}' not in src and f'status="{icon}' not in src
    assert "999999" not in src.replace("fall back to 999999 minutes", "")


# ── writers widen old tables with `message` ─────────────────────────────────────

_OLD_DUCK_DDL = """
    CREATE TABLE {t} (
        check_run_id VARCHAR, pipeline_run_id VARCHAR, checked_at VARCHAR,
        domain VARCHAR, system VARCHAR, layer VARCHAR, entity VARCHAR,
        check_type VARCHAR, passed BOOLEAN, severity VARCHAR, status VARCHAR,
        delay_minutes DOUBLE, slo_max_minutes BIGINT,
        source_delay_minutes DOUBLE, source_slo_max_minutes BIGINT,
        source_column_used VARCHAR, row_count BIGINT,
        slo_min_rows BIGINT, slo_max_rows BIGINT,
        anomaly_ratio DOUBLE, anomaly_baseline DOUBLE,
        quality_ratio DOUBLE, quality_severity VARCHAR,
        duration_seconds DOUBLE, details_json VARCHAR
    )
"""


def _records():
    from lakelogic.core.run_log import _flatten_slo_check

    results = [
        SLOCheckResult(layer="bronze", entity="t", status="OK", passed=True, message="8194 rows"),
        SLOCheckResult(layer="gold", entity="g", status="NOT_SET", passed=None, message="no target"),
    ]
    return [_flatten_slo_check(r, "run1", None, "2026-01-01T00:00:00", "d", "s") for r in results]


def test_duckdb_writer_adds_message_to_an_old_schema_table(tmp_path):
    duckdb = pytest.importorskip("duckdb")
    from lakelogic.core.run_log import _write_slo_checks_table

    db = tmp_path / "slo.duckdb"
    con = duckdb.connect(str(db))
    con.execute(_OLD_DUCK_DDL.format(t="slo_checks"))
    con.close()

    registry = SimpleNamespace(
        storage=SimpleNamespace(slo_checks_table="slo_checks"),
        metadata={"slo_checks_backend": "duckdb", "slo_checks_database": str(db)},
    )
    assert _write_slo_checks_table(registry, _records())

    con = duckdb.connect(str(db))
    rows = con.execute("SELECT status, message, passed FROM slo_checks ORDER BY entity").fetchall()
    con.close()
    assert rows == [("NOT_SET", "no target", None), ("OK", "8194 rows", True)]
    for status, _, _ in rows:
        assert status.isascii()


def test_sqlite_writer_adds_message_to_an_old_schema_table(tmp_path):
    from lakelogic.core.run_log import _write_slo_checks_table

    db = tmp_path / "slo.sqlite"
    con = sqlite3.connect(str(db))
    con.execute(_OLD_DUCK_DDL.format(t="slo_checks").replace("VARCHAR", "TEXT").replace("DOUBLE", "REAL"))
    con.commit()
    con.close()

    registry = SimpleNamespace(
        storage=SimpleNamespace(slo_checks_table="slo_checks"),
        metadata={"slo_checks_backend": "sqlite", "slo_checks_database": str(db)},
    )
    assert _write_slo_checks_table(registry, _records())
    con = sqlite3.connect(str(db))
    rows = con.execute("SELECT status, message FROM slo_checks ORDER BY entity").fetchall()
    con.close()
    assert rows == [("NOT_SET", "no target"), ("OK", "8194 rows")]


def test_flattened_row_and_details_json_carry_no_icons():
    for rec in _records():
        assert rec["status"].isascii()
        assert "✅" not in rec["details_json"] and "❌" not in rec["details_json"]
        assert rec["message"]


def test_emit_sends_no_section_for_not_set():
    """NOT_SET must reach the platform as neither a breach nor a met promise."""
    import json as _json
    from unittest.mock import MagicMock, patch

    from lakelogic.core.run_log import emit_slo_report

    class _Registry:
        domain = "d"
        system = "s"
        observatory = {"enabled": True, "endpoint": "https://example.invalid/ingest", "api_key": "k"}

    results = [
        SLOCheckResult(layer="gold", entity="g", status="NOT_SET", passed=None, check_type="freshness"),
        SLOCheckResult(layer="gold", entity="g", status="OK", passed=True, check_type="row_count", row_count=5),
    ]
    with patch("requests.post") as post, patch("lakelogic.core.observatory_spool.flush_spool", return_value=0):
        post.return_value = MagicMock(status_code=200, text="ok")
        emit_slo_report(_Registry(), results, environment="dev")
    payload = post.call_args_list[0].kwargs["json"]
    assert payload["status"] == "success"
    assert '"freshness"' not in _json.dumps(payload)
