"""No icons in LakeLogic-owned tables, and a slim, documented report_json.

Owner directive 2026-09-27: every value persisted to a _lakelogic_* table (or a
_lakelogic_* row column) is plain and standardised; report_json carries only
what no run-log column already holds.
"""

import json
import re
import types

import polars as pl
import pytest

from lakelogic import DataProcessor
from lakelogic.core.plain_values import has_icon, plain_record, plain_text, plain_value
from lakelogic.core.run_log import (
    REPORT_JSON_KEYS,
    _flatten_report,
    capture_failure,
    report_json_payload,
    write_run_log,
    write_slo_checks,
)
from lakelogic.core.slo import SLOCheckResult

ICON_RE = re.compile("[\U0001f000-\U0001faff☀-➿⬀-⯿⌀-⏿️‍]")


def _no_icons_anywhere(value, where=""):
    """Scan a persisted value, decoding JSON strings (json.dumps escapes emoji)."""
    if isinstance(value, str):
        text = value
        if value.lstrip()[:1] in ("{", "["):
            try:
                text = json.dumps(json.loads(value), ensure_ascii=False)
            except ValueError:
                pass
        assert not ICON_RE.search(text), f"icon in {where}: {text[:200]!r}"


# ── sanitiser ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("✅ OK (8194 rows)", "OK (8194 rows)"),
        ("⚠️ NO DATA", "NO DATA"),
        ("❌  RETENTION BREACH: x", "RETENTION BREACH: x"),
        ("rule 🔴 name", "rule name"),
        ("👍🏽 thumbs", "thumbs"),  # skin-tone modifier
        ("👨‍👩‍👧 family", "family"),  # ZWJ sequence
        ("⏭ skipped", "skipped"),
    ],
)
def test_plain_text_removes_icons_and_collapses_spaces(raw, expected):
    assert plain_text(raw) == expected
    assert not has_icon(plain_text(raw))


def test_plain_text_leaves_plain_strings_byte_identical():
    tb = "ValueError: x\n  file.py:1 in f\n\tindented  twice"
    assert plain_text(tb) is tb
    # arrows and bullets are text, not icons
    assert plain_text("a → b • c") == "a → b • c"
    assert plain_text(None) is None and plain_text(3) == 3


def test_plain_value_cleans_serialised_json_and_nested_structures():
    dumped = json.dumps({"status": "✅ OK", "items": ["❌ bad", 1]})
    assert "\\u2705" in dumped  # json.dumps escapes the icon — a raw scan misses it
    cleaned = json.loads(plain_value(dumped))
    assert cleaned == {"status": "OK", "items": ["bad", 1]}
    assert plain_record({"a": "🔴 x", "b": None}) == {"a": "x", "b": None}
    untouched = json.dumps({"a": "plain"})
    assert plain_value(untouched) is untouched


# ── report_json schema ───────────────────────────────────────────────────────

# Every key the flattened row stores as its own column.
_COLUMN_KEYS = {
    "pipeline_run_id",
    "run_id",
    "timestamp",
    "start_time",
    "end_time",
    "run_duration_seconds",
    "engine",
    "contract",
    "contract_version",
    "stage",
    "dataset",
    "domain",
    "system",
    "environment",
    "data_layer",
    "status",
    "error_message",
    "error_traceback",
    "lakelogic_version",
    "source_path",
    "estimated_cost",
    "cost_currency",
    "cost_confidence",
    "max_source_mtime",
    "max_watermark_value",
    "dlt_state_json",
    "slos",
}


def _full_report():
    return {
        "run_id": "r1",
        "pipeline_run_id": "p1",
        "engine": "spark",
        "contract": "Orders",
        "contract_file_name": "orders.yaml",
        "contract_version": "1.0.0",
        "stage": "default",
        "dataset": "orders",
        "domain": "sales",
        "system": "erp",
        "environment": "dev",
        "data_layer": "silver",
        "source_path": "s3://x/a.csv",
        "source_files": [{"path": "s3://x/a.csv", "mtime": None}],
        "max_source_mtime": 1.0,
        "timestamp": "2026-09-27T00:00:00+00:00",
        "counts": {
            "source": 10,
            "total": 10,
            "good": 9,
            "quarantined": 1,
            "quarantine_ratio": 0.1,
            "pre_transform_dropped": 0,
            "pre_transform_added": None,
        },
        "dataset_rules": [],
        "slos": {"freshness": {"delay_seconds": 12.0, "passed": True}},
        "row_rule_failures": [{"name": "r", "message": "Rule failed: r (x > 0)", "count": 1}],
        "schema_drift": {},
        "incremental_metadata": {"to_version": 7},
        "execution_context": {
            "engine": "spark",
            "engine_version": "3.5",
            "python_version": "3.11",
            "wall_clock_seconds": 3.0,
            "peak_memory_mb": None,
            "polars": {"predicate_pushdown": True},
        },
        "start_time": "a",
        "end_time": "b",
        "run_duration_seconds": 3.0,
        "estimated_cost": 0.0,
        "cost_currency": "USD",
        "cost_confidence": "none",
        "status": "failed",
        "error_message": "boom",
        "error_traceback": "T" * 5000,
        "lakelogic_version": "1.0",
        "max_watermark_value": "w",
        "dlt_state_json": "{}",
        "slo_row_count_min": 1,
        "slo_quality_pass": True,
    }


def test_report_json_has_no_keys_duplicated_by_columns():
    row = _flatten_report(_full_report())
    payload = json.loads(row["report_json"])
    assert set(payload) <= set(REPORT_JSON_KEYS)
    assert not (set(payload) & _COLUMN_KEYS)
    assert not any(k.startswith("slo_") for k in payload)
    # column-backed counts dropped; nothing left → counts omitted entirely
    assert "counts" not in payload
    # execution_context: duplicates and constants gone, nulls pruned
    assert payload["execution_context"] == {"engine_version": "3.5", "python_version": "3.11"}
    # no nulls / empty containers
    assert "schema_drift" not in payload and "dataset_rules" not in payload
    assert payload["source_files"] == [{"path": "s3://x/a.csv"}]


def test_readers_still_find_what_they_need():
    payload = report_json_payload(_full_report())
    # engine: incremental.py reads $.incremental_metadata.to_version
    assert payload["incremental_metadata"]["to_version"] == 7
    # SaaS: row_rule_failures / source_files / partition_presence
    assert payload["row_rule_failures"][0]["message"].startswith("Rule failed:")
    report = _full_report()
    report["partition_presence"] = {"grain": "day", "missing_count": 1}
    assert report_json_payload(report)["partition_presence"]["missing_count"] == 1
    # the freshness delay lives on in slo_json (slos no longer copied)
    assert json.loads(_flatten_report(_full_report())["slo_json"])["freshness"]["seconds"] == 12.0


def test_unmeasured_cost_is_null_not_zero():
    row = _flatten_report(_full_report())
    assert row["estimated_cost"] is None
    assert row["cost_currency"] is None
    assert row["cost_confidence"] == "none"
    measured = _full_report() | {"estimated_cost": 1.5, "cost_confidence": "estimated"}
    row = _flatten_report(measured)
    assert row["estimated_cost"] == 1.5 and row["cost_currency"] == "USD"


def test_error_message_is_one_capped_line_and_traceback_is_capped_tail():
    report = _full_report() | {
        "error_message": "x" * 900 + "\nsecond line",
        "error_traceback": "H" + "T" * 5000 + "END",
    }
    row = _flatten_report(report)
    assert row["error_message"] == "x" * 500
    assert len(row["error_traceback"]) == 2000 and row["error_traceback"].endswith("END")
    assert "error_traceback" not in json.loads(row["report_json"])


def test_capture_failure_keeps_frames_when_first_line_is_huge_and_strips_icons():
    try:
        raise RuntimeError("❌ " + "J" * 6000 + "\n\tat org.apache.Foo(Foo.scala:1)" * 300)
    except RuntimeError as exc:
        failure = capture_failure(exc)
    assert failure["error_message"] == "J" * 500
    assert len(failure["error_traceback"]) <= 2000
    assert "test_plain_values_and_slim_report.py" in failure["error_traceback"]  # the frame survived
    assert "org.apache" not in failure["error_traceback"]
    assert not has_icon(failure["error_traceback"])


# ── representative runs: no icons persisted, payload budget ──────────────────


@pytest.mark.parametrize("engine", ["polars", "duckdb"])
def test_representative_run_persists_no_icons_and_a_small_report(tmp_path, engine):
    import duckdb

    db = tmp_path / "rl.duckdb"
    contract = {
        "version": "1.0.0",
        "dataset": "orders",
        "info": {"title": "Orders ✅", "version": "1.2.0"},
        "metadata": {
            "run_log_table": "run_logs",
            "run_log_backend": "duckdb",
            "run_log_database": str(db),
            "domain": "sales",
            "system": "erp",
            "data_layer": "silver",
            "environment": "dev",
        },
        "quality": {
            "row_rules": [
                {"name": "amount_positive ✅", "sql": "amount > 0"},
                {"name": "id_not_null", "sql": "id IS NOT NULL"},
            ],
            "dataset_rules": [{"name": "min_rows 📦", "sql": "SELECT COUNT(*) FROM source", "must_be_greater_than": 1}],
        },
    }
    df = pl.DataFrame({"id": [1, 2, None, 4] * 5, "amount": [10.0, -1.0, 3.0, 5.0] * 5})
    proc = DataProcessor(engine=engine, contract=contract)
    good, bad = proc.run(df if engine == "polars" else df.to_pandas())

    # quarantine metadata column is plain at the source
    errors = bad["_lakelogic_errors"].to_list() if engine == "polars" else list(bad["_lakelogic_errors"])
    for row_errors in errors:
        for message in row_errors:
            _no_icons_anywhere(message, "_lakelogic_errors")

    report = proc.last_report
    report["status"] = "failed"
    try:
        raise RuntimeError("⚠️ write failed")
    except RuntimeError as exc:
        report.update(capture_failure(exc))
    write_run_log(report, proc.contract, engine_name=engine)

    con = duckdb.connect(str(db))
    cur = con.execute("SELECT * FROM run_logs")
    cols = [d[0] for d in cur.description]
    row = dict(zip(cols, cur.fetchone()))
    con.close()
    for key, value in row.items():
        _no_icons_anywhere(value, key)
    assert row["error_message"] == "write failed"
    assert row["estimated_cost"] is None and row["cost_confidence"] == "none"
    payload = json.loads(row["report_json"])
    assert not (set(payload) & _COLUMN_KEYS)
    # was ~1.8 KB for this run before the slimming (full report copied verbatim)
    assert len(row["report_json"].encode()) < 900


def test_slo_checks_and_evidence_rows_are_plain(tmp_path):
    import duckdb

    from lakelogic.core.evidence_tables import _coerce

    result = SLOCheckResult(
        layer="silver", entity="orders", check_type="freshness", status="✅ OK", passed=True, severity="pass"
    )
    result.message = "⚠️ within target"
    db = tmp_path / "slo.duckdb"
    registry = types.SimpleNamespace(
        domain="sales",
        system="erp",
        storage=types.SimpleNamespace(slo_checks_table="slo_checks"),
        metadata={"slo_checks_backend": "duckdb", "slo_checks_database": str(db)},
    )
    write_slo_checks(registry, [result], "check-1")
    con = duckdb.connect(str(db))
    cur = con.execute("SELECT * FROM slo_checks")
    cols = [d[0] for d in cur.description]
    row = dict(zip(cols, cur.fetchone()))
    con.close()
    for key, value in row.items():
        _no_icons_anywhere(value, key)
    assert row["status"] == "OK"

    rec = _coerce([{"status": "❌ failed", "n": 1}], {"status": "string", "n": "bigint"})[0]
    assert rec == {"status": "failed", "n": 1}


def test_pipeline_runs_summary_is_plain():
    from lakelogic.cli.observability import flatten_summary

    rec = flatten_summary({"run_id": "r", "metrics": {}, "entities": [{"error": "❌ boom"}]})
    _no_icons_anywhere(rec["summary_json"], "summary_json")
    assert "boom" in rec["summary_json"]


def test_source_files_are_capped_with_a_total():
    from lakelogic.core.run_log import SOURCE_FILES_KEPT, report_json_payload

    files = [{"path": f"landing/d/{i}.json", "mtime": i} for i in range(720)]
    out = report_json_payload({"source_files": files})
    assert len(out["source_files"]) == SOURCE_FILES_KEPT and out["source_file_count"] == 720
    small = report_json_payload({"source_files": files[:3]})
    assert len(small["source_files"]) == 3 and "source_file_count" not in small
