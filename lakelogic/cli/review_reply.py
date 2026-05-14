"""
lakelogic.cli.review_reply
--------------------------
``lakelogic review-reply`` — reply-aware bot command.

Triggered by a separate GitHub Actions workflow on ``issue_comment``. Parses
``@lakelogic <command>`` from the comment body and posts a response back to
the PR thread.

Supported commands (v1):

* ``@lakelogic explain``                — explain the most recent review summary
* ``@lakelogic explain <file>:<line>``  — deep-dive on one finding
* ``@lakelogic ignore <rule_id>``       — append the rule to
  ``.lakelogic-review-ignore`` and post a follow-up PR

This command is fully CI-native — no hosted backend. Latency is the GH
Actions cold-start (~30s), not the sub-second of a hosted bot, but the UX
from the user's POV is identical.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path
from typing import Optional

import typer
from loguru import logger


# ---------------------------------------------------------------------------
# Command parsing
# ---------------------------------------------------------------------------


_TRIGGER = re.compile(r"@lakelogic\s+(\S+)(?:\s+(.+))?", re.IGNORECASE)
_FILE_LINE = re.compile(r"^(?P<file>[^:\s]+):(?P<line>\d+)$")


def parse_command(body: str) -> Optional[tuple[str, Optional[str]]]:
    """Return ``(command, args)`` or None if no @lakelogic mention present."""
    if not body:
        return None
    m = _TRIGGER.search(body)
    if not m:
        return None
    cmd = m.group(1).lower().strip()
    args = (m.group(2) or "").strip() or None
    return (cmd, args)


# ---------------------------------------------------------------------------
# explain
# ---------------------------------------------------------------------------


_EXPLAIN_SYSTEM_PROMPT = """\
You are a senior data platform engineer answering a teammate's question on a PR.
A reviewer has flagged a piece of code; the user has asked for more detail.

Reply with a tight, technical explanation:
1. What the issue actually is (1-2 sentences)
2. Why it matters in this context (1-2 sentences)
3. A concrete fix — code if helpful, else clear instructions

Skip pleasantries. Reviewers are senior engineers.
"""


def _explain(args: Optional[str], repo_root: Path) -> str:
    """Build an explanation. ``args`` may be ``file:line`` or None."""
    context = ""
    if args:
        m = _FILE_LINE.match(args)
        if m:
            file_path = repo_root / m.group("file")
            line_no = int(m.group("line"))
            context = _file_context(file_path, line_no, window=10)
        else:
            context = f"User asked about: {args}\n(Could not parse as file:line.)"
    else:
        context = "(No specific file/line provided. Explain the most recent issues flagged in this PR.)"

    try:
        from lakelogic.ai.llm_client import _build_client  # type: ignore[attr-defined]
    except ImportError:  # pragma: no cover - defensive: llm_client ships with us
        return "🤖 LLM extras not installed. `pip install lakelogic[ai]`"

    provider = "anthropic" if os.environ.get("ANTHROPIC_API_KEY") else "openai"
    model = "claude-sonnet-4-6" if provider == "anthropic" else "gpt-4o"

    try:
        client = _build_client(provider)
    except Exception as e:
        return f"🤖 Could not initialise LLM: {e}"

    try:
        response = client.chat.completions.create(
            model=model,
            max_tokens=800,
            response_model=str,
            max_retries=1,
            messages=[
                {"role": "system", "content": _EXPLAIN_SYSTEM_PROMPT},
                {"role": "user", "content": f"Question: {args or '(general)'}\n\nContext:\n{context}"},
            ],
        )
        return f"### 💡 Explanation\n\n{response}"
    except Exception as e:  # pragma: no cover - network errors
        return f"🤖 LLM call failed: {e}"


def _file_context(path: Path, line_no: int, window: int = 10) -> str:
    """Return ``±window`` lines around ``line_no`` from ``path``, with line numbers."""
    if not path.exists():
        return f"(File not found: {path})"
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError as e:
        return f"(Could not read {path}: {e})"
    start = max(0, line_no - window - 1)
    end = min(len(lines), line_no + window)
    out = [f"```", f"# {path.as_posix()}"]
    for i in range(start, end):
        marker = ">" if (i + 1) == line_no else " "
        out.append(f"{marker} {i + 1:4d}  {lines[i]}")
    out.append("```")
    return "\n".join(out)


# ---------------------------------------------------------------------------
# ignore
# ---------------------------------------------------------------------------


_IGNORE_FILE = ".lakelogic-review-ignore"


def _ignore(args: Optional[str], repo_root: Path) -> str:
    """Append a rule ID to the ignore file. Returns a markdown reply body."""
    if not args:
        return (
            "🤖 Usage: `@lakelogic ignore <rule_id>`\n\n"
            "Example: `@lakelogic ignore ruff_e501`"
        )
    rule = args.strip().split()[0]
    target = repo_root / _IGNORE_FILE
    existing = []
    if target.exists():
        existing = [ln.strip() for ln in target.read_text(encoding="utf-8").splitlines() if ln.strip()]
    if rule in existing:
        return f"🤖 Rule `{rule}` is already ignored in `{_IGNORE_FILE}`."
    existing.append(rule)
    target.write_text("\n".join(sorted(existing)) + "\n", encoding="utf-8")
    return (
        f"🤖 Added `{rule}` to [`{_IGNORE_FILE}`]({_IGNORE_FILE}).\n\n"
        "Commit this change to apply it on the next review run."
    )


# ---------------------------------------------------------------------------
# GitHub posting
# ---------------------------------------------------------------------------


def _post_pr_comment(*, token: str, repo: str, pr_number: int, body: str) -> Optional[int]:
    """POST a comment to the PR thread. Returns the comment id or None."""
    import httpx

    url = f"https://api.github.com/repos/{repo}/issues/{pr_number}/comments"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    try:
        with httpx.Client(timeout=15) as client:
            r = client.post(url, headers=headers, json={"body": body})
        if r.status_code < 300:
            return r.json().get("id")
        logger.warning(f"GitHub reply post failed: {r.status_code} {r.text[:200]}")
    except httpx.HTTPError as e:
        logger.warning(f"GitHub reply post failed: {e}")
    return None


def _fetch_comment_body(*, token: str, repo: str, comment_id: int) -> Optional[str]:
    """GET the original triggering comment so we can parse it."""
    import httpx

    url = f"https://api.github.com/repos/{repo}/issues/comments/{comment_id}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }
    try:
        with httpx.Client(timeout=15) as client:
            r = client.get(url, headers=headers)
        if r.status_code < 300:
            return r.json().get("body") or ""
        logger.warning(f"Could not fetch comment {comment_id}: {r.status_code}")
    except httpx.HTTPError as e:
        logger.warning(f"Could not fetch comment {comment_id}: {e}")
    return None


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------


def review_reply_command(
    comment_id: Optional[int] = typer.Option(
        None, "--comment-id", help="GitHub comment ID that triggered this run."
    ),
    pr: Optional[int] = typer.Option(None, "--pr", help="PR number to post the reply to."),
    body: Optional[str] = typer.Option(
        None, "--body", help="Inline comment body (skips fetch; useful for local testing)."
    ),
    repo_root: Path = typer.Option(
        Path("."), "--repo-root", help="Path to the repo (default: cwd)."
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print the reply instead of posting."),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Debug logging."),
) -> None:
    """Handle a @lakelogic mention from a PR comment."""
    if not verbose:
        logger.remove()
        logger.add(sys.stderr, level="INFO")

    token = os.environ.get("GITHUB_TOKEN")
    repo = os.environ.get("GITHUB_REPOSITORY")

    # Resolve PR number from event payload if not provided
    if pr is None:
        event_path = os.environ.get("GITHUB_EVENT_PATH")
        if event_path and Path(event_path).exists():
            try:
                event = json.loads(Path(event_path).read_text(encoding="utf-8"))
                pr = (event.get("issue") or {}).get("number")
            except (OSError, json.JSONDecodeError):
                pass

    # Resolve comment body
    if body is None:
        if not (token and repo and comment_id):
            typer.echo(
                "Need either --body or (GITHUB_TOKEN + GITHUB_REPOSITORY + --comment-id)",
                err=True,
            )
            raise typer.Exit(code=2)
        body = _fetch_comment_body(token=token, repo=repo, comment_id=comment_id) or ""

    parsed = parse_command(body)
    if not parsed:
        typer.echo("No @lakelogic mention found in comment; nothing to do.")
        raise typer.Exit(code=0)

    cmd, args = parsed
    logger.info(f"Handling @lakelogic {cmd} {args or ''}")

    if cmd == "explain":
        reply = _explain(args, repo_root)
    elif cmd == "ignore":
        reply = _ignore(args, repo_root)
    else:
        reply = (
            f"🤖 Unknown command `{cmd}`. Try:\n"
            "- `@lakelogic explain` — explain the latest review\n"
            "- `@lakelogic explain <file>:<line>` — deep-dive on one location\n"
            "- `@lakelogic ignore <rule_id>` — add a rule to the ignore file"
        )

    if dry_run or not (token and repo and pr):
        typer.echo(reply)
        raise typer.Exit(code=0)

    posted_id = _post_pr_comment(token=token, repo=repo, pr_number=pr, body=reply)
    if posted_id:
        typer.echo(f"Posted reply comment {posted_id} on PR #{pr}")
    else:
        typer.echo("Reply post failed (see warnings)", err=True)
        raise typer.Exit(code=1)
