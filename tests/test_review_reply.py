"""P7 tests: reply-aware bot (parsing, file context, ignore file, GitHub posting)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from lakelogic.cli.review_reply import (
    _explain,
    _fetch_comment_body,
    _file_context,
    _ignore,
    _post_pr_comment,
    parse_command,
)


# ---------------------------------------------------------------------------
# parse_command
# ---------------------------------------------------------------------------


def test_parse_command_explain_no_args() -> None:
    assert parse_command("hey @lakelogic explain please") == ("explain", "please")


def test_parse_command_explain_with_file_line() -> None:
    out = parse_command("@lakelogic explain models/x.sql:42")
    assert out == ("explain", "models/x.sql:42")


def test_parse_command_ignore_with_rule() -> None:
    assert parse_command("@lakelogic ignore ruff_e501") == ("ignore", "ruff_e501")


def test_parse_command_returns_none_when_no_mention() -> None:
    assert parse_command("just a regular comment") is None


def test_parse_command_returns_none_for_empty() -> None:
    assert parse_command("") is None


def test_parse_command_case_insensitive_trigger() -> None:
    assert parse_command("@LakeLogic Explain x.py:1") == ("explain", "x.py:1")


def test_parse_command_handles_command_with_no_args() -> None:
    out = parse_command("@lakelogic explain")
    assert out == ("explain", None)


# ---------------------------------------------------------------------------
# _file_context
# ---------------------------------------------------------------------------


def test_file_context_returns_window_with_marker(tmp_path: Path) -> None:
    f = tmp_path / "x.py"
    f.write_text("\n".join(f"line{i}" for i in range(1, 21)))
    out = _file_context(f, line_no=10, window=2)
    # Marker on the requested line
    assert ">   10  line10" in out
    # Adjacent lines (within window) present
    assert "line8" in out
    assert "line12" in out
    # Outside window not included
    assert "line1\n" not in out
    assert "line20" not in out


def test_file_context_handles_missing_file(tmp_path: Path) -> None:
    out = _file_context(tmp_path / "missing.py", line_no=1)
    assert "File not found" in out


def test_file_context_handles_line_at_start(tmp_path: Path) -> None:
    f = tmp_path / "x.py"
    f.write_text("a\nb\nc\n")
    out = _file_context(f, line_no=1, window=5)
    # First line has the marker
    assert ">    1  a" in out
    # All three lines visible
    assert "     2  b" in out
    assert "     3  c" in out


# ---------------------------------------------------------------------------
# _ignore
# ---------------------------------------------------------------------------


def test_ignore_creates_file_with_rule(tmp_path: Path) -> None:
    out = _ignore("ruff_e501", tmp_path)
    target = tmp_path / ".lakelogic-review-ignore"
    assert target.exists()
    assert target.read_text(encoding="utf-8").strip() == "ruff_e501"
    assert "Added" in out


def test_ignore_appends_to_existing_file(tmp_path: Path) -> None:
    target = tmp_path / ".lakelogic-review-ignore"
    target.write_text("rule_a\n", encoding="utf-8")
    _ignore("rule_b", tmp_path)
    assert target.read_text(encoding="utf-8").splitlines() == ["rule_a", "rule_b"]


def test_ignore_skips_duplicate_rule(tmp_path: Path) -> None:
    target = tmp_path / ".lakelogic-review-ignore"
    target.write_text("rule_a\n", encoding="utf-8")
    out = _ignore("rule_a", tmp_path)
    assert "already ignored" in out
    # File unchanged
    assert target.read_text(encoding="utf-8").splitlines() == ["rule_a"]


def test_ignore_returns_usage_when_no_args(tmp_path: Path) -> None:
    out = _ignore(None, tmp_path)
    assert "Usage" in out
    assert not (tmp_path / ".lakelogic-review-ignore").exists()


def test_ignore_takes_only_first_word(tmp_path: Path) -> None:
    """`@lakelogic ignore ruff_e501 because too noisy` → only ruff_e501 is the rule."""
    _ignore("ruff_e501 because too noisy", tmp_path)
    target = tmp_path / ".lakelogic-review-ignore"
    assert target.read_text(encoding="utf-8").splitlines() == ["ruff_e501"]


# ---------------------------------------------------------------------------
# _explain — graceful degrade when SDK missing
# ---------------------------------------------------------------------------


def test_explain_returns_helpful_message_when_sdk_missing(tmp_path: Path) -> None:
    """No instructor installed → friendly install hint."""
    # The instructor import inside _build_client raises; _explain catches and
    # returns a user-facing message
    out = _explain("nope.py:1", tmp_path)
    assert out.startswith("🤖")


def test_explain_uses_file_context_when_args_match_pattern(tmp_path: Path) -> None:
    """The explain path resolves args as file:line and feeds context to LLM."""
    f = tmp_path / "x.py"
    f.write_text("line1\nline2\nline3\n")
    fake_client = MagicMock()
    fake_client.chat.completions.create.return_value = "Here's why."

    with patch("lakelogic.ai.llm_client._build_client", return_value=fake_client):
        out = _explain("x.py:2", tmp_path)

    assert "Here's why." in out
    # Verify the user prompt included our file context
    called_messages = fake_client.chat.completions.create.call_args.kwargs["messages"]
    user_msg = next(m["content"] for m in called_messages if m["role"] == "user")
    assert ">    2  line2" in user_msg


# ---------------------------------------------------------------------------
# GitHub posting helpers
# ---------------------------------------------------------------------------


def test_post_pr_comment_returns_id_on_success() -> None:
    fake = MagicMock()
    fake.__enter__.return_value = fake
    fake.__exit__.return_value = None
    fake.post.return_value = MagicMock(status_code=201, json=lambda: {"id": 9999})
    with patch("httpx.Client", return_value=fake):
        cid = _post_pr_comment(token="t", repo="o/r", pr_number=42, body="hi")
    assert cid == 9999


def test_post_pr_comment_returns_none_on_failure() -> None:
    fake = MagicMock()
    fake.__enter__.return_value = fake
    fake.__exit__.return_value = None
    fake.post.return_value = MagicMock(status_code=500, text="boom", json=lambda: {})
    with patch("httpx.Client", return_value=fake):
        assert _post_pr_comment(token="t", repo="o/r", pr_number=1, body="x") is None


def test_fetch_comment_body_returns_body_string() -> None:
    fake = MagicMock()
    fake.__enter__.return_value = fake
    fake.__exit__.return_value = None
    fake.get.return_value = MagicMock(status_code=200, json=lambda: {"body": "@lakelogic ignore foo"})
    with patch("httpx.Client", return_value=fake):
        body = _fetch_comment_body(token="t", repo="o/r", comment_id=5)
    assert body == "@lakelogic ignore foo"


def test_fetch_comment_body_returns_none_on_404() -> None:
    fake = MagicMock()
    fake.__enter__.return_value = fake
    fake.__exit__.return_value = None
    fake.get.return_value = MagicMock(status_code=404, text="not found", json=lambda: {})
    with patch("httpx.Client", return_value=fake):
        assert _fetch_comment_body(token="t", repo="o/r", comment_id=5) is None


# ---------------------------------------------------------------------------
# _explain — args parsing branches + LLM-init failure path
# ---------------------------------------------------------------------------


def test_explain_with_non_file_line_args_falls_through_to_general_context(tmp_path: Path) -> None:
    """When args don't match `file:line`, _explain still calls the LLM with
    a generic 'User asked about: ...' context (line 84)."""
    fake_client = MagicMock()
    fake_client.chat.completions.create.return_value = "OK."

    with patch("lakelogic.ai.llm_client._build_client", return_value=fake_client):
        out = _explain("how does idempotency work?", tmp_path)

    user_msg = next(
        m["content"]
        for m in fake_client.chat.completions.create.call_args.kwargs["messages"]
        if m["role"] == "user"
    )
    assert "User asked about: how does idempotency work?" in user_msg
    assert "Could not parse as file:line" in user_msg
    assert "OK." in out


def test_explain_with_no_args_uses_general_context(tmp_path: Path) -> None:
    """args=None → 'No specific file/line provided' context (line 86)."""
    fake_client = MagicMock()
    fake_client.chat.completions.create.return_value = "Sure."

    with patch("lakelogic.ai.llm_client._build_client", return_value=fake_client):
        _explain(None, tmp_path)

    user_msg = next(
        m["content"]
        for m in fake_client.chat.completions.create.call_args.kwargs["messages"]
        if m["role"] == "user"
    )
    assert "No specific file/line provided" in user_msg


def test_explain_returns_friendly_message_when_build_client_raises(tmp_path: Path) -> None:
    """If _build_client itself raises (e.g. provider not configured), the user
    gets a 'Could not initialise LLM' message instead of a stack trace (L98-99)."""
    with patch(
        "lakelogic.ai.llm_client._build_client", side_effect=RuntimeError("no provider")
    ):
        out = _explain("foo.py:1", tmp_path)
    assert out.startswith("🤖")
    assert "Could not initialise LLM" in out
    assert "no provider" in out


# ---------------------------------------------------------------------------
# _file_context — OSError on read
# ---------------------------------------------------------------------------


def test_file_context_handles_unreadable_file(tmp_path: Path) -> None:
    """OSError on read returns a friendly message (lines 123-124)."""
    f = tmp_path / "x.py"
    f.write_text("a\n")
    with patch.object(Path, "read_text", side_effect=OSError("permission denied")):
        out = _file_context(f, line_no=1)
    assert "Could not read" in out
    assert "permission denied" in out


# ---------------------------------------------------------------------------
# _post_pr_comment / _fetch_comment_body — network exception paths
# ---------------------------------------------------------------------------


def test_post_pr_comment_returns_none_on_httpx_error() -> None:
    """httpx.HTTPError during post → returns None, logs warning (lines 186-187)."""
    import httpx

    fake = MagicMock()
    fake.__enter__.return_value = fake
    fake.__exit__.return_value = None
    fake.post.side_effect = httpx.ConnectError("network down")
    with patch("httpx.Client", return_value=fake):
        assert _post_pr_comment(token="t", repo="o/r", pr_number=1, body="x") is None


def test_fetch_comment_body_returns_none_on_httpx_error() -> None:
    """httpx.HTTPError during get → returns None (lines 206-207)."""
    import httpx

    fake = MagicMock()
    fake.__enter__.return_value = fake
    fake.__exit__.return_value = None
    fake.get.side_effect = httpx.ConnectError("network down")
    with patch("httpx.Client", return_value=fake):
        assert _fetch_comment_body(token="t", repo="o/r", comment_id=5) is None


# ---------------------------------------------------------------------------
# review_reply_command — CLI entrypoint (Typer integration)
# ---------------------------------------------------------------------------


def _run_reply_cli(monkeypatch: pytest.MonkeyPatch, *args: str, env: dict | None = None):
    """Invoke `lakelogic review-reply ...` via Typer's CliRunner."""
    import typer
    from typer.testing import CliRunner

    from lakelogic.cli.review_reply import review_reply_command

    for k, v in (env or {}).items():
        monkeypatch.setenv(k, v)

    # Single-command Typer apps drop the subcommand name in argv resolution,
    # so we invoke with the raw option args (no leading "reply").
    app = typer.Typer()
    app.command()(review_reply_command)
    runner = CliRunner()
    return runner.invoke(app, list(args))


def test_cli_dry_run_with_inline_body_for_unknown_command(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """--body skips fetch; --dry-run skips post. Unknown command produces a
    usage-hint reply (lines 270-280)."""
    result = _run_reply_cli(
        monkeypatch,
        "--body",
        "@lakelogic frobnicate",
        "--repo-root",
        str(tmp_path),
        "--dry-run",
    )
    assert result.exit_code == 0
    assert "Unknown command `frobnicate`" in result.output


def test_cli_dry_run_with_explain_command_uses_explain(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """`@lakelogic explain` dispatches to _explain in dry-run mode."""
    with patch("lakelogic.cli.review_reply._explain", return_value="### Stub"):
        result = _run_reply_cli(
            monkeypatch,
            "--body",
            "@lakelogic explain",
            "--repo-root",
            str(tmp_path),
            "--dry-run",
        )
    assert result.exit_code == 0
    assert "### Stub" in result.output


def test_cli_dry_run_with_ignore_command_writes_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """`@lakelogic ignore <rule>` writes the ignore file even in dry-run."""
    result = _run_reply_cli(
        monkeypatch,
        "--body",
        "@lakelogic ignore ruff_e501",
        "--repo-root",
        str(tmp_path),
        "--dry-run",
    )
    assert result.exit_code == 0
    assert (tmp_path / ".lakelogic-review-ignore").read_text(encoding="utf-8").strip() == "ruff_e501"


def test_cli_no_at_mention_exits_zero_without_action(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A comment with no @lakelogic mention exits 0 with a polite no-op message."""
    result = _run_reply_cli(
        monkeypatch,
        "--body",
        "just a regular PR comment",
        "--repo-root",
        str(tmp_path),
        "--dry-run",
    )
    assert result.exit_code == 0
    assert "No @lakelogic mention" in result.output


def test_cli_errors_when_body_and_env_both_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """No --body, no GITHUB_TOKEN/REPOSITORY/--comment-id → exit 2 with usage hint."""
    for v in ("GITHUB_TOKEN", "GITHUB_REPOSITORY", "GITHUB_EVENT_PATH"):
        monkeypatch.delenv(v, raising=False)
    result = _run_reply_cli(monkeypatch, "--repo-root", str(tmp_path), "--pr", "1")
    assert result.exit_code == 2
    assert "Need either --body" in result.output


def test_cli_resolves_pr_from_github_event_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When --pr is omitted, the PR number is read from GITHUB_EVENT_PATH JSON."""
    import json as _json

    event = tmp_path / "event.json"
    event.write_text(_json.dumps({"issue": {"number": 99}}))

    # Provide a body so the test doesn't try to fetch — and dry-run so no post
    result = _run_reply_cli(
        monkeypatch,
        "--body",
        "@lakelogic ignore foo",
        "--repo-root",
        str(tmp_path),
        "--dry-run",
        env={"GITHUB_EVENT_PATH": str(event)},
    )
    assert result.exit_code == 0
    # PR resolution doesn't itself produce visible output in dry-run, but the
    # command must have succeeded — confirms the event-parsing branch ran
    assert "Added" in result.output  # ignore command's confirmation


def test_cli_posts_reply_when_token_pr_provided(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Non-dry-run path: token + repo + pr present → _post_pr_comment is called."""
    with patch(
        "lakelogic.cli.review_reply._post_pr_comment", return_value=12345
    ) as mock_post:
        result = _run_reply_cli(
            monkeypatch,
            "--body",
            "@lakelogic ignore foo",
            "--repo-root",
            str(tmp_path),
            "--pr",
            "42",
            env={"GITHUB_TOKEN": "ghs_x", "GITHUB_REPOSITORY": "acme/repo"},
        )
    mock_post.assert_called_once()
    assert result.exit_code == 0
    assert "Posted reply comment 12345" in result.output


def test_cli_exits_one_when_post_fails(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """If _post_pr_comment returns None (failure), exit 1."""
    with patch("lakelogic.cli.review_reply._post_pr_comment", return_value=None):
        result = _run_reply_cli(
            monkeypatch,
            "--body",
            "@lakelogic ignore foo",
            "--repo-root",
            str(tmp_path),
            "--pr",
            "42",
            env={"GITHUB_TOKEN": "x", "GITHUB_REPOSITORY": "a/b"},
        )
    assert result.exit_code == 1
    assert "Reply post failed" in result.output


def test_cli_fetches_body_when_not_provided_inline(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """No --body but full env → _fetch_comment_body is called and its result
    drives parse_command (covers the fetch branch at L256)."""
    with patch(
        "lakelogic.cli.review_reply._fetch_comment_body",
        return_value="@lakelogic ignore from_remote",
    ) as mock_fetch:
        result = _run_reply_cli(
            monkeypatch,
            "--comment-id",
            "777",
            "--pr",
            "1",
            "--repo-root",
            str(tmp_path),
            "--dry-run",
            env={"GITHUB_TOKEN": "ghs_x", "GITHUB_REPOSITORY": "acme/repo"},
        )
    mock_fetch.assert_called_once()
    assert result.exit_code == 0
    # Confirm the fetched body was parsed and the ignore command ran
    assert (tmp_path / ".lakelogic-review-ignore").read_text(encoding="utf-8").strip() == "from_remote"


def test_cli_tolerates_malformed_github_event_payload(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A garbage GITHUB_EVENT_PATH JSON shouldn't crash — pr stays None and
    the CLI falls back to the dry-run / no-post path (covers L245-246)."""
    bad_event = tmp_path / "broken.json"
    bad_event.write_text("{not valid json", encoding="utf-8")

    result = _run_reply_cli(
        monkeypatch,
        "--body",
        "@lakelogic ignore foo",
        "--repo-root",
        str(tmp_path),
        "--dry-run",
        env={"GITHUB_EVENT_PATH": str(bad_event)},
    )
    # No crash — exits 0 because dry_run is set
    assert result.exit_code == 0
    assert "Added" in result.output
