"""P3/P4 tests: ADO formatter, diff-hash cache, datacontract diff runner."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from lakelogic.ai import review_cache
from lakelogic.ai.code_reviewer import ReviewFinding, ReviewReport, run_review
from lakelogic.ai.review_formatters import format_azure_pr
from lakelogic.ai.tier1_runners import _parse_datacontract_diff, run_datacontract_diff


# ---------------------------------------------------------------------------
# format_azure_pr
# ---------------------------------------------------------------------------


def _sample_report() -> dict:
    return {
        "findings": [
            {
                "file": "models/x.sql",
                "line": 14,
                "severity": "critical",
                "category": "security",
                "rule": "hardcoded_secret",
                "message": "API key in plain text",
                "suggestion": "Use a secrets manager",
            }
        ],
        "summary": {"critical": 1, "warning": 0, "info": 0},
    }


def test_azure_pr_falls_back_to_json_when_env_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    for v in (
        "SYSTEM_PULLREQUEST_PULLREQUESTID",
        "SYSTEM_TEAMFOUNDATIONCOLLECTIONURI",
        "SYSTEM_TEAMPROJECT",
        "BUILD_REPOSITORY_ID",
        "SYSTEM_ACCESSTOKEN",
    ):
        monkeypatch.delenv(v, raising=False)

    out = format_azure_pr(_sample_report())
    payload = json.loads(out)
    assert payload["summary"]["critical"] == 1
    assert payload["threads"][0]["rule"] == "security/hardcoded_secret"


def test_azure_pr_posts_when_env_present(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_PULLREQUEST_PULLREQUESTID", "42")
    monkeypatch.setenv("SYSTEM_TEAMFOUNDATIONCOLLECTIONURI", "https://dev.azure.com/myorg/")
    monkeypatch.setenv("SYSTEM_TEAMPROJECT", "myproj")
    monkeypatch.setenv("BUILD_REPOSITORY_ID", "abc-123")
    monkeypatch.setenv("SYSTEM_ACCESSTOKEN", "tok")

    fake_response = MagicMock(status_code=200, text="ok")
    fake_client = MagicMock()
    fake_client.__enter__.return_value = fake_client
    fake_client.__exit__.return_value = None
    fake_client.post.return_value = fake_response

    with patch("httpx.Client", return_value=fake_client):
        out = format_azure_pr(_sample_report())

    assert "Posted" in out
    assert "PR #42" in out
    # 1 summary + 1 inline = 2 POSTs
    assert fake_client.post.call_count == 2


# ---------------------------------------------------------------------------
# review_cache
# ---------------------------------------------------------------------------


def test_cache_key_stable_for_same_files(tmp_path: Path) -> None:
    f = tmp_path / "x.py"
    f.write_text("x = 1\n")
    k1 = review_cache.compute_cache_key([f], extra="anthropic:sonnet")
    k2 = review_cache.compute_cache_key([f], extra="anthropic:sonnet")
    assert k1 == k2


def test_cache_key_changes_when_content_changes(tmp_path: Path) -> None:
    f = tmp_path / "x.py"
    f.write_text("x = 1\n")
    k1 = review_cache.compute_cache_key([f])
    f.write_text("x = 2\n")
    k2 = review_cache.compute_cache_key([f])
    assert k1 != k2


def test_cache_key_changes_when_provider_changes(tmp_path: Path) -> None:
    f = tmp_path / "x.py"
    f.write_text("x = 1\n")
    assert review_cache.compute_cache_key([f], extra="anthropic") != review_cache.compute_cache_key(
        [f], extra="openai"
    )


def test_cache_roundtrip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("LAKELOGIC_REVIEW_CACHE_DIR", str(tmp_path))
    review_cache.save_cached_report("k1", {"findings": [], "summary": {}})
    assert review_cache.load_cached_report("k1") == {"findings": [], "summary": {}}
    assert review_cache.load_cached_report("missing") is None


def test_orchestrator_uses_cache_when_available(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LAKELOGIC_REVIEW_CACHE_DIR", str(tmp_path / "cache"))
    f = tmp_path / "x.py"
    f.write_text("x = 1\n")

    # Pre-seed the cache for the exact key the orchestrator will compute
    key = review_cache.compute_cache_key([f], extra="anthropic:claude-sonnet-4-6")
    cached_report = ReviewReport(
        files_scanned=99,  # sentinel value to prove we read from cache
        findings=[],
        summary={"critical": 0, "warning": 0, "info": 0},
        ai_provider="anthropic",
        ai_model="claude-sonnet-4-6",
    ).model_dump()
    review_cache.save_cached_report(key, cached_report)

    with patch("lakelogic.ai.llm_client.review_batch") as mock_call:
        report = run_review(
            [f],
            api_key_present=True,
            provider="anthropic",
            model="claude-sonnet-4-6",
        )

    mock_call.assert_not_called()
    assert report.files_scanned == 99


def test_orchestrator_skips_cache_with_no_cache_flag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("LAKELOGIC_REVIEW_CACHE_DIR", str(tmp_path / "cache"))
    f = tmp_path / "x.py"
    f.write_text("x = 1\n")

    with patch("lakelogic.ai.llm_client.review_batch", return_value=([], {"total": 0})) as mock_call:
        run_review(
            [f],
            api_key_present=True,
            provider="anthropic",
            model="claude-sonnet-4-6",
            use_cache=False,
        )
    mock_call.assert_called_once()


# ---------------------------------------------------------------------------
# datacontract diff parser
# ---------------------------------------------------------------------------


def test_parse_datacontract_diff_extracts_severity_and_message() -> None:
    text = (
        "INFO: contract version bumped\n"
        "ERROR: field 'driver_id' was removed\n"
        "WARNING: type of 'amount' changed string -> number\n"
        "blah blah no severity prefix\n"
    )
    findings = _parse_datacontract_diff(text, "contracts/orders.datacontract.yaml")
    severities = [f.severity for f in findings]
    assert severities == ["info", "critical", "warning"]
    assert findings[1].rule == "datacontract_breaking_change"
    assert "driver_id" in findings[1].message
    assert all(f.category == "governance" for f in findings)


def test_run_datacontract_diff_skips_without_base_ref(tmp_path: Path) -> None:
    f = tmp_path / "orders.datacontract.yaml"
    f.write_text("dataContractSpecification: 0.9.3\n")
    assert run_datacontract_diff([f], base_ref=None) == []


def test_run_datacontract_diff_skips_when_no_contract_files(tmp_path: Path) -> None:
    f = tmp_path / "regular.yaml"
    f.write_text("foo: bar\n")
    assert run_datacontract_diff([f], base_ref="main") == []


def test_run_datacontract_diff_handles_missing_base_version(tmp_path: Path) -> None:
    """A newly-added contract has no base — runner should silently skip it."""
    f = tmp_path / "new.datacontract.yaml"
    f.write_text("dataContractSpecification: 0.9.3\n")
    with patch("lakelogic.ai.tier1_runners._git_show", return_value=None):
        assert run_datacontract_diff([f], base_ref="main") == []


def test_run_datacontract_diff_parses_subprocess_output(tmp_path: Path) -> None:
    f = tmp_path / "orders.datacontract.yaml"
    f.write_text("dataContractSpecification: 0.9.3\n")

    fake_proc = MagicMock(stdout="ERROR: field removed\n", stderr="")
    with (
        patch("lakelogic.ai.tier1_runners._git_show", return_value="dataContractSpecification: 0.9.2\n"),
        patch("subprocess.run", return_value=fake_proc),
    ):
        findings = run_datacontract_diff([f], base_ref="main")
    assert len(findings) == 1
    assert findings[0].severity == "critical"
    assert findings[0].category == "governance"


def test_run_datacontract_diff_graceful_when_cli_missing(tmp_path: Path) -> None:
    f = tmp_path / "orders.datacontract.yaml"
    f.write_text("dataContractSpecification: 0.9.3\n")
    with (
        patch("lakelogic.ai.tier1_runners._git_show", return_value="x"),
        patch("subprocess.run", side_effect=FileNotFoundError()),
    ):
        assert run_datacontract_diff([f], base_ref="main") == []
