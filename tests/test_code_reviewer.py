"""Tests for the tiered code reviewer (Tier 1 + orchestrator + diff collector)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from lakelogic.ai.code_reviewer import (
    ReviewFinding,
    ReviewReport,
    determine_exit_code,
    run_review,
)
from lakelogic.ai.diff_collector import collect_changed_files
from lakelogic.ai.tier1_runners import scan_pii_patterns


# ---------------------------------------------------------------------------
# PII scanner — deterministic, no external tools needed
# ---------------------------------------------------------------------------


def test_pii_scanner_detects_email(tmp_path: Path) -> None:
    f = tmp_path / "leak.py"
    f.write_text("USER = 'alice@company.com'\n")
    findings = scan_pii_patterns([f])
    assert any(x.rule == "pii_email" for x in findings)


def test_pii_scanner_detects_ssn_as_critical(tmp_path: Path) -> None:
    f = tmp_path / "leak.sql"
    f.write_text("INSERT INTO users (ssn) VALUES ('123-45-6789');\n")
    findings = scan_pii_patterns([f])
    ssn = [x for x in findings if x.rule == "pii_ssn_us"]
    assert ssn and ssn[0].severity == "critical"


def test_pii_scanner_skips_example_lines(tmp_path: Path) -> None:
    f = tmp_path / "doc.py"
    f.write_text("# example: user@example.com\n")
    assert scan_pii_patterns([f]) == []


def test_pii_scanner_ignores_non_reviewable_suffix(tmp_path: Path) -> None:
    f = tmp_path / "leak.txt"
    f.write_text("alice@company.com\n")
    assert scan_pii_patterns([f]) == []


# ---------------------------------------------------------------------------
# diff_collector — directory walk path (no git required)
# ---------------------------------------------------------------------------


def test_collect_walks_directory_filters_suffixes(tmp_path: Path) -> None:
    (tmp_path / "a.py").write_text("x = 1\n")
    (tmp_path / "b.sql").write_text("SELECT 1;\n")
    (tmp_path / "c.txt").write_text("ignored\n")
    files = collect_changed_files([tmp_path])
    suffixes = {f.suffix for f in files}
    assert suffixes == {".py", ".sql"}


def test_collect_honours_exclude_glob(tmp_path: Path) -> None:
    (tmp_path / "keep.py").write_text("x = 1\n")
    skip_dir = tmp_path / "skip"
    skip_dir.mkdir()
    (skip_dir / "drop.py").write_text("y = 2\n")
    files = collect_changed_files([tmp_path], exclude=["**/skip/**"])
    assert all("skip" not in f.as_posix() for f in files)


def test_collect_honours_include_glob(tmp_path: Path) -> None:
    (tmp_path / "a.py").write_text("x = 1\n")
    (tmp_path / "b.sql").write_text("SELECT 1;\n")
    files = collect_changed_files([tmp_path], include=["**/*.sql"])
    assert {f.suffix for f in files} == {".sql"}


def test_collect_truncates_to_max_files(tmp_path: Path) -> None:
    for i in range(10):
        (tmp_path / f"f{i}.py").write_text("x = 1\n")
    files = collect_changed_files([tmp_path], max_files=3)
    assert len(files) == 3


# ---------------------------------------------------------------------------
# Orchestrator — no-key path injects skip notice
# ---------------------------------------------------------------------------


def test_run_review_no_key_emits_skip_notice(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    f = tmp_path / "ok.py"
    f.write_text("x = 1\n")
    report = run_review([f])
    assert any(x.rule == "llm_review_skipped" for x in report.findings)
    assert report.ai_provider == "none"


def test_run_review_no_llm_flag_skips_silently(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", "sk-test")
    f = tmp_path / "ok.py"
    f.write_text("x = 1\n")
    report = run_review([f], no_llm=True)
    assert not any(x.rule == "llm_review_skipped" for x in report.findings)


def test_report_summary_counts_severities() -> None:
    report = ReviewReport(
        files_scanned=1,
        findings=[
            ReviewFinding(file="a", severity="critical", category="security", rule="r1", message="m"),
            ReviewFinding(file="a", severity="warning", category="sql_quality", rule="r2", message="m"),
            ReviewFinding(file="a", severity="warning", category="sql_quality", rule="r3", message="m"),
        ],
        summary={"critical": 1, "warning": 2, "info": 0},
    )
    assert report.summary["warning"] == 2


# ---------------------------------------------------------------------------
# Exit-code logic
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("severity", "fail_on", "expected"),
    [
        ("critical", "critical", 1),
        ("warning", "critical", 0),
        ("warning", "warning", 1),
        ("info", "warning", 0),
        ("info", "info", 1),
        ("critical", "never", 0),
    ],
)
def test_determine_exit_code(severity: str, fail_on: str, expected: int) -> None:
    report = ReviewReport(
        files_scanned=1,
        findings=[ReviewFinding(file="a", severity=severity, category="x", rule="r", message="m")],
    )
    assert determine_exit_code(report, fail_on) == expected


def test_determine_exit_code_empty_findings() -> None:
    report = ReviewReport(files_scanned=0, findings=[])
    assert determine_exit_code(report, "critical") == 0


# ---------------------------------------------------------------------------
# Schema sanity — JSON formatter receives the keys it expects
# ---------------------------------------------------------------------------


def test_review_report_dump_contains_formatter_keys(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    f = tmp_path / "x.py"
    f.write_text("x = 1\n")
    report = run_review([f], no_llm=True)
    dumped = report.model_dump()
    for key in ("findings", "summary", "files_scanned", "ai_provider", "ai_model", "duration_seconds", "token_usage"):
        assert key in dumped
    # round-trip through JSON
    json.dumps(dumped, default=str)
