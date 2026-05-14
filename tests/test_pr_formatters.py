"""P5 tests: CodeRabbit-style PR formatters (GitHub + Azure DevOps)."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from lakelogic.ai.review_formatters import (
    SUMMARY_MARKER,
    _build_summary_markdown,
    _inline_comment_body,
    format_azure_pr,
    format_github_pr,
    write_output,
)


def _report(findings=None, **summary) -> dict:
    findings = findings if findings is not None else []
    s = {"critical": 0, "warning": 0, "info": 0}
    s.update(summary)
    return {
        "findings": findings,
        "summary": s,
        "files_scanned": len(findings) or 1,
        "ai_provider": "anthropic",
        "ai_model": "claude-sonnet-4-6",
        "duration_seconds": 1.2,
        "token_usage": {"total": 4321},
    }


def _finding(**kw) -> dict:
    base = {
        "file": "src/x.py",
        "line": 10,
        "severity": "warning",
        "category": "python_quality",
        "rule": "ruff_e501",
        "message": "Line too long",
        "suggestion": None,
        "code_snippet": None,
        "code_suggestion": None,
        "end_line": None,
    }
    base.update(kw)
    return base


# ---------------------------------------------------------------------------
# Markdown summary builder
# ---------------------------------------------------------------------------


def test_summary_markdown_includes_marker_and_counts() -> None:
    md = _build_summary_markdown(_report([_finding(severity="critical")], critical=1))
    assert SUMMARY_MARKER in md
    assert "🔴 critical" in md
    assert "Findings by file" in md
    assert "`src/x.py`" in md


def test_summary_markdown_no_findings_friendly_message() -> None:
    md = _build_summary_markdown(_report([]))
    assert "No issues found" in md
    assert "Findings by file" not in md


def test_inline_comment_body_includes_suggestion_block_when_code_present() -> None:
    body = _inline_comment_body(_finding(code_suggestion="x = 1  # fixed"))
    assert "```suggestion" in body
    assert "x = 1  # fixed" in body


def test_inline_comment_body_omits_suggestion_block_when_no_code() -> None:
    body = _inline_comment_body(_finding())
    assert "```suggestion" not in body


def test_inline_comment_body_includes_text_suggestion_separately() -> None:
    body = _inline_comment_body(_finding(suggestion="Wrap at 100 chars"))
    assert "How to fix:" in body
    assert "Wrap at 100 chars" in body


# ---------------------------------------------------------------------------
# format_github_pr — fallback when env missing
# ---------------------------------------------------------------------------


def test_github_pr_falls_back_to_json_when_env_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    for v in ("GITHUB_TOKEN", "GITHUB_REPOSITORY", "GITHUB_EVENT_PATH"):
        monkeypatch.delenv(v, raising=False)
    out = format_github_pr(_report([_finding()]))
    payload = json.loads(out)
    assert "summary_markdown" in payload
    assert payload["inline_comments"][0]["path"] == "src/x.py"


def test_github_pr_creates_review_and_summary_on_first_run(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    event = tmp_path / "event.json"
    event.write_text(json.dumps({"pull_request": {"number": 42}}))
    monkeypatch.setenv("GITHUB_TOKEN", "ghs_x")
    monkeypatch.setenv("GITHUB_REPOSITORY", "acme/repo")
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(event))

    fake_client = MagicMock()
    fake_client.__enter__.return_value = fake_client
    fake_client.__exit__.return_value = None
    # GET existing comments → none with our marker
    fake_client.get.return_value = MagicMock(status_code=200, json=lambda: [])
    # POST review + POST summary
    fake_client.post.side_effect = [
        MagicMock(status_code=200, json=lambda: {"id": 555}),  # review
        MagicMock(status_code=201, json=lambda: {"id": 999}),  # summary
    ]

    with patch("httpx.Client", return_value=fake_client):
        result = format_github_pr(_report([_finding(severity="critical")], critical=1))

    assert "review 555" in result
    assert "summary comment 999" in result
    assert "PR #42" in result
    # Review POST was called with REQUEST_CHANGES because there's a critical
    review_post_kwargs = fake_client.post.call_args_list[0].kwargs
    assert review_post_kwargs["json"]["event"] == "REQUEST_CHANGES"


def test_github_pr_updates_existing_summary_on_rerun(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    event = tmp_path / "event.json"
    event.write_text(json.dumps({"pull_request": {"number": 7}}))
    monkeypatch.setenv("GITHUB_TOKEN", "ghs_x")
    monkeypatch.setenv("GITHUB_REPOSITORY", "acme/repo")
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(event))

    fake_client = MagicMock()
    fake_client.__enter__.return_value = fake_client
    fake_client.__exit__.return_value = None
    # GET returns one prior comment with our marker → triggers PATCH path
    fake_client.get.return_value = MagicMock(
        status_code=200,
        json=lambda: [{"id": 111, "body": f"{SUMMARY_MARKER}\nold body"}],
    )
    fake_client.post.return_value = MagicMock(status_code=200, json=lambda: {"id": 222})
    fake_client.patch.return_value = MagicMock(status_code=200, json=lambda: {"id": 111})

    with patch("httpx.Client", return_value=fake_client):
        result = format_github_pr(_report([_finding()], warning=1))

    fake_client.patch.assert_called_once()
    assert "summary comment 111" in result


def test_github_pr_uses_comment_event_when_no_critical(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    event = tmp_path / "e.json"
    event.write_text(json.dumps({"pull_request": {"number": 1}}))
    monkeypatch.setenv("GITHUB_TOKEN", "x")
    monkeypatch.setenv("GITHUB_REPOSITORY", "a/b")
    monkeypatch.setenv("GITHUB_EVENT_PATH", str(event))

    fake_client = MagicMock()
    fake_client.__enter__.return_value = fake_client
    fake_client.__exit__.return_value = None
    fake_client.get.return_value = MagicMock(status_code=200, json=lambda: [])
    fake_client.post.return_value = MagicMock(status_code=200, json=lambda: {"id": 1})

    with patch("httpx.Client", return_value=fake_client):
        format_github_pr(_report([_finding(severity="warning")], warning=1))

    # First POST was the review; check event is COMMENT, not REQUEST_CHANGES
    assert fake_client.post.call_args_list[0].kwargs["json"]["event"] == "COMMENT"


# ---------------------------------------------------------------------------
# format_azure_pr — marker-based update behaviour
# ---------------------------------------------------------------------------


def test_azure_pr_updates_existing_summary_on_rerun(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_PULLREQUEST_PULLREQUESTID", "13")
    monkeypatch.setenv("SYSTEM_TEAMFOUNDATIONCOLLECTIONURI", "https://dev.azure.com/org/")
    monkeypatch.setenv("SYSTEM_TEAMPROJECT", "proj")
    monkeypatch.setenv("BUILD_REPOSITORY_ID", "repo-guid")
    monkeypatch.setenv("SYSTEM_ACCESSTOKEN", "tok")

    fake_client = MagicMock()
    fake_client.__enter__.return_value = fake_client
    fake_client.__exit__.return_value = None
    fake_client.get.return_value = MagicMock(
        status_code=200,
        json=lambda: {
            "value": [
                {
                    "id": 77,
                    "comments": [{"id": 88, "content": f"{SUMMARY_MARKER}\nold summary"}],
                }
            ]
        },
    )
    fake_client.patch.return_value = MagicMock(status_code=200, text="ok")
    fake_client.post.return_value = MagicMock(status_code=200, text="ok")

    with patch("httpx.Client", return_value=fake_client):
        out = format_azure_pr(_report([_finding()], warning=1))

    # Should PATCH the existing summary, NOT POST a new one for the summary
    assert fake_client.patch.called
    # The single inline finding still triggers a POST for the inline thread
    assert "Posted" in out


def test_azure_pr_creates_new_summary_when_no_existing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SYSTEM_PULLREQUEST_PULLREQUESTID", "1")
    monkeypatch.setenv("SYSTEM_TEAMFOUNDATIONCOLLECTIONURI", "https://dev.azure.com/o/")
    monkeypatch.setenv("SYSTEM_TEAMPROJECT", "p")
    monkeypatch.setenv("BUILD_REPOSITORY_ID", "r")
    monkeypatch.setenv("SYSTEM_ACCESSTOKEN", "t")

    fake_client = MagicMock()
    fake_client.__enter__.return_value = fake_client
    fake_client.__exit__.return_value = None
    fake_client.get.return_value = MagicMock(status_code=200, json=lambda: {"value": []})
    fake_client.post.return_value = MagicMock(status_code=200, text="ok")

    with patch("httpx.Client", return_value=fake_client):
        format_azure_pr(_report([_finding()], warning=1))

    # 1 summary POST + 1 inline POST = 2 POSTs total, no PATCH
    assert fake_client.patch.call_count == 0
    assert fake_client.post.call_count == 2


# ---------------------------------------------------------------------------
# write_output dispatch
# ---------------------------------------------------------------------------


def test_write_output_dispatches_to_github_pr(monkeypatch: pytest.MonkeyPatch) -> None:
    for v in ("GITHUB_TOKEN", "GITHUB_REPOSITORY", "GITHUB_EVENT_PATH"):
        monkeypatch.delenv(v, raising=False)
    out = write_output(_report([_finding()]), "github_pr")
    # Falls back to JSON when env missing — still routed to format_github_pr
    payload = json.loads(out)
    assert "summary_markdown" in payload
