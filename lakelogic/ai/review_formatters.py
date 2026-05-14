"""
lakelogic.ai.review_formatters
-------------------------------
Output formatters for code review findings.

Supports:
- ``terminal``  — Rich table output for interactive use
- ``json``      — Machine-readable JSON
- ``sarif``     — SARIF v2.1 for GitHub Security tab
- ``github``    — GitHub Actions annotation format (::warning, ::error)
"""

from __future__ import annotations

import json
from typing import Any, Optional

from loguru import logger


def format_terminal(report: dict[str, Any]) -> str:
    """Format review report as a rich terminal output.

    Args:
        report: ReviewReport dict with ``findings``, ``summary``, etc.

    Returns:
        Formatted string for terminal display.
    """
    lines: list[str] = []

    lines.append("")
    lines.append(" 🔍 LakeLogic Code Review")
    lines.append(" " + "─" * 70)
    lines.append("")
    lines.append(
        f" Scanned: {report['files_scanned']} files | Provider: {report['ai_provider']} / {report['ai_model']}"
    )
    lines.append(f" Duration: {report['duration_seconds']:.1f}s | Tokens: {report['token_usage'].get('total', 0):,}")
    lines.append("")
    lines.append(" " + "─" * 70)

    findings = report.get("findings", [])
    if not findings:
        lines.append("")
        lines.append(" ✅ No issues found. Your code looks great!")
        lines.append("")
        return "\n".join(lines)

    # Group by severity
    critical = [f for f in findings if f["severity"] == "critical"]
    warnings = [f for f in findings if f["severity"] == "warning"]
    info = [f for f in findings if f["severity"] == "info"]

    for severity_group, icon, label in [
        (critical, "🔴", "CRITICAL"),
        (warnings, "🟡", "WARNING"),
        (info, "🔵", "INFO"),
    ]:
        if not severity_group:
            continue

        lines.append("")
        lines.append(f" {icon} {label} ({len(severity_group)})")
        lines.append("")

        for finding in severity_group:
            loc = finding["file"]
            if finding.get("line"):
                loc += f":{finding['line']}"
            lines.append(f"   {loc}")
            lines.append(f"   [{finding['category']}/{finding['rule']}]")
            lines.append(f"   {finding['message']}")
            if finding.get("suggestion"):
                lines.append(f"   → {finding['suggestion']}")
            lines.append("")

    # Summary
    lines.append(" " + "─" * 70)
    summary = report.get("summary", {})
    lines.append(
        f" Summary: {summary.get('critical', 0)} critical, "
        f"{summary.get('warning', 0)} warnings, "
        f"{summary.get('info', 0)} info"
    )
    lines.append("")

    return "\n".join(lines)


def format_json(report: dict[str, Any]) -> str:
    """Format review report as JSON.

    Args:
        report: ReviewReport dict.

    Returns:
        Pretty-printed JSON string.
    """
    return json.dumps(report, indent=2, default=str)


def format_github(report: dict[str, Any]) -> str:
    """Format review findings as GitHub Actions workflow commands.

    Produces ``::error`` and ``::warning`` annotations that appear
    directly on PR diffs.

    Args:
        report: ReviewReport dict.

    Returns:
        GitHub annotation commands as a string.
    """
    lines: list[str] = []

    for finding in report.get("findings", []):
        if finding["severity"] == "critical":
            cmd = "error"
        elif finding["severity"] == "warning":
            cmd = "warning"
        else:
            cmd = "notice"

        file_part = f"file={finding['file']}" if finding.get("file") else ""
        line_part = f",line={finding['line']}" if finding.get("line") else ""
        title = f"[{finding['category']}/{finding['rule']}]"
        msg = finding["message"]
        if finding.get("suggestion"):
            msg += f" Fix: {finding['suggestion']}"

        lines.append(f"::{cmd} {file_part}{line_part},title={title}::{msg}")

    return "\n".join(lines)


def format_sarif(report: dict[str, Any]) -> str:
    """Format review findings as SARIF v2.1.0 for GitHub Security tab.

    Args:
        report: ReviewReport dict.

    Returns:
        SARIF JSON string.
    """
    severity_map = {
        "critical": "error",
        "warning": "warning",
        "info": "note",
    }

    rules: list[dict] = []
    results: list[dict] = []
    rule_index: dict[str, int] = {}

    for finding in report.get("findings", []):
        rule_id = f"{finding['category']}/{finding['rule']}"

        if rule_id not in rule_index:
            rule_index[rule_id] = len(rules)
            rule_def: dict[str, Any] = {
                "id": rule_id,
                "shortDescription": {"text": finding["message"]},
            }
            if finding.get("suggestion"):
                rule_def["help"] = {"text": finding["suggestion"]}
            rules.append(rule_def)

        location: dict[str, Any] = {}
        if finding.get("file"):
            physical: dict[str, Any] = {"artifactLocation": {"uri": finding["file"]}}
            if finding.get("line"):
                physical["region"] = {"startLine": finding["line"]}
            location = {"physicalLocation": physical}

        result: dict[str, Any] = {
            "ruleId": rule_id,
            "ruleIndex": rule_index[rule_id],
            "level": severity_map.get(finding["severity"], "note"),
            "message": {"text": finding["message"]},
        }
        if location:
            result["locations"] = [{"physicalLocation": location.get("physicalLocation", {})}]

        results.append(result)

    sarif = {
        "$schema": "https://json.schemastore.org/sarif-2.1.0.json",
        "version": "2.1.0",
        "runs": [
            {
                "tool": {
                    "driver": {
                        "name": "lakelogic-review",
                        "version": "1.0.0",
                        "informationUri": "https://lakelogic.io",
                        "rules": rules,
                    }
                },
                "results": results,
            }
        ],
    }

    return json.dumps(sarif, indent=2)


def format_azure_pr(report: dict[str, Any]) -> str:
    """Post inline PR comments to Azure DevOps and return a summary string.

    Reads ADO context from the standard pipeline env vars:

    * ``SYSTEM_ACCESSTOKEN``               — short-lived bearer (enable in YAML)
    * ``SYSTEM_TEAMFOUNDATIONCOLLECTIONURI`` — e.g. https://dev.azure.com/myorg/
    * ``SYSTEM_TEAMPROJECT``               — project name
    * ``BUILD_REPOSITORY_ID``              — repo GUID
    * ``SYSTEM_PULLREQUEST_PULLREQUESTID`` — PR id (only set on PR triggers)

    If any of these are missing, falls back to the JSON payload (e.g. for
    local testing). Network errors are logged but never raise.
    """
    import json as _json
    import os

    findings = report.get("findings", [])

    pr_id = os.environ.get("SYSTEM_PULLREQUEST_PULLREQUESTID")
    org_url = os.environ.get("SYSTEM_TEAMFOUNDATIONCOLLECTIONURI")
    project = os.environ.get("SYSTEM_TEAMPROJECT")
    repo_id = os.environ.get("BUILD_REPOSITORY_ID")
    token = os.environ.get("SYSTEM_ACCESSTOKEN")

    if not (pr_id and org_url and project and repo_id and token):
        logger.warning(
            "format=azure_pr requested but ADO pipeline env vars are missing; "
            "returning JSON payload instead. Ensure the pipeline step has "
            "'env: SYSTEM_ACCESSTOKEN: $(System.AccessToken)' set."
        )
        return _json.dumps(_azure_pr_payload(report), indent=2)

    posted = _post_azure_threads(
        findings=findings,
        summary=report.get("summary", {}),
        org_url=org_url.rstrip("/"),
        project=project,
        repo_id=repo_id,
        pr_id=pr_id,
        token=token,
    )
    return f"Posted {posted} thread(s) to ADO PR #{pr_id}"


def _azure_pr_payload(report: dict[str, Any]) -> dict[str, Any]:
    """JSON payload returned when ADO env vars are missing (local dev fallback)."""
    return {
        "summary": report.get("summary", {}),
        "threads": [
            {
                "file": f.get("file"),
                "line": f.get("line"),
                "severity": f.get("severity"),
                "rule": f"{f.get('category')}/{f.get('rule')}",
                "message": f.get("message"),
                "suggestion": f.get("suggestion"),
            }
            for f in report.get("findings", [])
        ],
    }


_ADO_STATUS_MAP = {
    "critical": 1,  # active
    "warning": 1,  # active
    "info": 4,  # informational comment, no thread status
}


def _post_azure_threads(
    *,
    findings: list[dict[str, Any]],
    summary: dict[str, int],
    org_url: str,
    project: str,
    repo_id: str,
    pr_id: str,
    token: str,
) -> int:
    """POST one thread per finding + a single summary thread. Returns count posted."""
    import base64

    import httpx

    auth = base64.b64encode(f":{token}".encode()).decode()
    headers = {
        "Authorization": f"Basic {auth}",
        "Content-Type": "application/json",
    }
    base = f"{org_url}/{project}/_apis/git/repositories/{repo_id}/pullRequests/{pr_id}/threads?api-version=7.1"

    posted = 0
    with httpx.Client(timeout=15) as client:
        # Summary thread — find existing (by sentinel marker) and update,
        # else create. Same UX as GitHub: one persistent comment per PR.
        summary_body = _build_summary_markdown({"findings": findings, "summary": summary})
        existing = _find_existing_ado_summary(client, base, headers)
        if existing:
            update_url = (
                f"{org_url}/{project}/_apis/git/repositories/{repo_id}"
                f"/pullRequests/{pr_id}/threads/{existing['thread_id']}"
                f"/comments/{existing['comment_id']}?api-version=7.1"
            )
            try:
                r = client.patch(update_url, headers=headers, json={"content": summary_body})
                if r.status_code < 300:
                    posted += 1
                else:
                    logger.warning(f"ADO summary update failed: {r.status_code} {r.text[:200]}")
            except httpx.HTTPError as e:
                logger.warning(f"ADO summary update failed: {e}")
        else:
            try:
                r = client.post(
                    base,
                    headers=headers,
                    json={
                        "comments": [{"parentCommentId": 0, "content": summary_body, "commentType": 1}],
                        "status": 1,
                    },
                )
                if r.status_code < 300:
                    posted += 1
                else:
                    logger.warning(f"ADO summary thread failed: {r.status_code} {r.text[:200]}")
            except httpx.HTTPError as e:
                logger.warning(f"ADO summary thread failed: {e}")

        # Per-finding inline threads
        for f in findings:
            file_path = f.get("file")
            line = f.get("line")
            if not file_path or not line:
                continue  # need both to anchor an inline comment

            content = _inline_comment_body(f)

            payload = {
                "comments": [{"parentCommentId": 0, "content": content, "commentType": 1}],
                "status": _ADO_STATUS_MAP.get(f.get("severity", "info"), 4),
                "threadContext": {
                    "filePath": f"/{file_path.lstrip('/')}",
                    "rightFileStart": {"line": line, "offset": 1},
                    "rightFileEnd": {"line": line, "offset": 1},
                },
            }
            try:
                r = client.post(base, headers=headers, json=payload)
                if r.status_code < 300:
                    posted += 1
                else:
                    logger.warning(f"ADO inline thread for {file_path}:{line} failed: {r.status_code} {r.text[:200]}")
            except httpx.HTTPError as e:
                logger.warning(f"ADO inline thread for {file_path}:{line} failed: {e}")

    return posted


def _find_existing_ado_summary(client, base: str, headers: dict) -> Optional[dict]:
    """Find a prior LakeLogic summary thread on this PR (by sentinel marker).

    Returns ``{'thread_id', 'comment_id'}`` or None.
    """
    import httpx

    try:
        r = client.get(base, headers=headers)
    except httpx.HTTPError:
        return None
    if r.status_code >= 300:
        return None
    for thread in (r.json() or {}).get("value", []):
        for comment in thread.get("comments") or []:
            if SUMMARY_MARKER in (comment.get("content") or ""):
                return {"thread_id": thread.get("id"), "comment_id": comment.get("id")}
    return None


# ---------------------------------------------------------------------------
# Shared helpers — markdown summary table + sentinel marker for update-on-rerun
# ---------------------------------------------------------------------------

# Sentinel HTML comment included in summary bodies so we can find and update
# the previous comment on subsequent CI runs instead of duplicating.
SUMMARY_MARKER = "<!-- lakelogic-review:summary -->"

_SEVERITY_ICON = {"critical": "🔴", "warning": "🟡", "info": "🔵"}


def _build_summary_markdown(report: dict[str, Any], heading: str = "LakeLogic Review") -> str:
    """Return a CodeRabbit-style markdown summary with counts + per-file table."""
    findings = report.get("findings", [])
    summary = report.get("summary", {})
    crit = summary.get("critical", 0)
    warn = summary.get("warning", 0)
    info = summary.get("info", 0)

    lines = [SUMMARY_MARKER, "", f"## 🔍 {heading}", ""]

    # P6: walkthrough section (rendered above findings if present)
    walkthrough = report.get("walkthrough")
    if walkthrough:
        from lakelogic.ai.walkthrough import WalkthroughResult, render_walkthrough_markdown

        try:
            lines.append(render_walkthrough_markdown(WalkthroughResult.model_validate(walkthrough)))
            lines.append("")
        except Exception as e:  # pragma: no cover - defensive: never block findings on walkthrough
            logger.warning(f"Could not render walkthrough: {e}")

    if not findings:
        lines.append("✅ **No issues found.** Nice work.")
        lines.append("")
        lines.append(_engine_footer(report))
        return "\n".join(lines)

    lines.append(f"**{crit}** 🔴 critical · **{warn}** 🟡 warning · **{info}** 🔵 info")
    lines.append("")

    # Per-file breakdown
    by_file: dict[str, dict[str, int]] = {}
    for f in findings:
        path = f.get("file") or "(unknown)"
        bucket = by_file.setdefault(path, {"critical": 0, "warning": 0, "info": 0})
        bucket[f.get("severity", "info")] = bucket.get(f.get("severity", "info"), 0) + 1

    if by_file:
        lines.append("### Findings by file")
        lines.append("")
        lines.append("| File | 🔴 | 🟡 | 🔵 |")
        lines.append("| --- | ---: | ---: | ---: |")
        for path in sorted(by_file):
            b = by_file[path]
            lines.append(f"| `{path}` | {b['critical']} | {b['warning']} | {b['info']} |")
        lines.append("")

    # Top 10 critical/warning details
    important = [f for f in findings if f.get("severity") in ("critical", "warning")][:10]
    if important:
        lines.append("<details><summary>Top issues</summary>")
        lines.append("")
        for f in important:
            loc = f.get("file", "?")
            if f.get("line"):
                loc += f":{f['line']}"
            icon = _SEVERITY_ICON.get(f.get("severity", "info"), "·")
            rule = f"{f.get('category')}/{f.get('rule')}"
            lines.append(f"- {icon} `{loc}` — **{rule}** — {f.get('message', '')}")
        lines.append("")
        lines.append("</details>")
        lines.append("")

    lines.append(_engine_footer(report))
    return "\n".join(lines)


def _engine_footer(report: dict[str, Any]) -> str:
    provider = report.get("ai_provider", "none")
    model = report.get("ai_model", "none")
    tokens = (report.get("token_usage") or {}).get("total", 0)
    duration = report.get("duration_seconds", 0.0)
    return (
        f"<sub>Scanned {report.get('files_scanned', 0)} file(s) in {duration:.1f}s · "
        f"engine: {provider}/{model} · tokens: {tokens:,}</sub>"
    )


def _inline_comment_body(f: dict[str, Any]) -> str:
    """Markdown body for a single inline review comment."""
    icon = _SEVERITY_ICON.get(f.get("severity", "info"), "·")
    rule = f"{f.get('category')}/{f.get('rule')}"
    parts = [f"{icon} **[{f.get('severity', 'info').upper()}] {rule}**", "", f.get("message", "")]
    if f.get("suggestion"):
        parts += ["", f"**How to fix:** {f['suggestion']}"]
    if f.get("code_suggestion"):
        # GitHub renders this as a one-click "Apply suggestion" button.
        # ADO doesn't render the block but it still reads as a code fence.
        parts += ["", "```suggestion", f["code_suggestion"], "```"]
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# format_github_pr — CodeRabbit-style review (real PR review comments + summary)
# ---------------------------------------------------------------------------


def format_github_pr(report: dict[str, Any]) -> str:
    """Post a GitHub PR review with inline comments and an updating summary.

    Reads from standard GitHub Actions env vars:
    * ``GITHUB_TOKEN``       — workflow token (needs ``pull-requests: write``)
    * ``GITHUB_REPOSITORY``  — ``owner/repo``
    * ``GITHUB_EVENT_PATH``  — JSON file containing the PR number under
      ``pull_request.number`` (set automatically on ``pull_request`` triggers)

    Behaviour:
    * Creates one PR Review (``POST /pulls/{n}/reviews``) carrying every
      inline comment in a single batch.
    * Maintains a *single* summary issue comment, edited in place on every
      re-run (identified by an HTML-comment marker).
    * Findings with ``code_suggestion`` get GitHub's one-click ```suggestion```
      block — works for ruff autofixes and any LLM finding that proposes
      replacement code.

    Falls back to the JSON payload (for local dev) if env vars are missing.
    Network failures are logged but never raise.
    """
    import json as _json
    import os

    token = os.environ.get("GITHUB_TOKEN")
    repo = os.environ.get("GITHUB_REPOSITORY")
    event_path = os.environ.get("GITHUB_EVENT_PATH")

    pr_number: Optional[int] = None
    if event_path and os.path.exists(event_path):
        try:
            with open(event_path, encoding="utf-8") as f:
                event = _json.load(f)
            pr_number = (event.get("pull_request") or {}).get("number")
        except (OSError, _json.JSONDecodeError) as e:
            logger.warning(f"Could not read GITHUB_EVENT_PATH: {e}")

    if not (token and repo and pr_number):
        logger.warning(
            "format=github_pr requested but GitHub env vars are missing "
            "(GITHUB_TOKEN, GITHUB_REPOSITORY, GITHUB_EVENT_PATH→pull_request.number); "
            "returning JSON payload instead."
        )
        return _json.dumps(_github_pr_payload(report), indent=2)

    review_id, summary_id = _post_github_pr_review(
        report=report, token=token, repo=repo, pr_number=pr_number
    )
    return f"Posted GitHub PR review {review_id} + summary comment {summary_id} on PR #{pr_number}"


def _github_pr_payload(report: dict[str, Any]) -> dict[str, Any]:
    """JSON dump returned when GitHub env is missing (local dev / debugging)."""
    return {
        "summary_markdown": _build_summary_markdown(report),
        "inline_comments": [
            {
                "path": f.get("file"),
                "line": f.get("line"),
                "side": "RIGHT",
                "body": _inline_comment_body(f),
            }
            for f in report.get("findings", [])
            if f.get("file") and f.get("line")
        ],
    }


def _post_github_pr_review(
    *,
    report: dict[str, Any],
    token: str,
    repo: str,
    pr_number: int,
) -> tuple[Any, Any]:
    """POST one PR review with all inline comments + upsert the summary comment."""
    import httpx

    base = f"https://api.github.com/repos/{repo}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    findings = report.get("findings", [])
    inline = []
    for f in findings:
        if not f.get("file") or not f.get("line"):
            continue
        comment: dict[str, Any] = {
            "path": f["file"],
            "side": "RIGHT",
            "line": int(f["line"]),
            "body": _inline_comment_body(f),
        }
        if f.get("end_line") and int(f["end_line"]) > int(f["line"]):
            comment["start_line"] = int(f["line"])
            comment["start_side"] = "RIGHT"
            comment["line"] = int(f["end_line"])
        inline.append(comment)

    review_id: Any = None
    summary_id: Any = None

    with httpx.Client(timeout=20) as client:
        # 1. Inline review (request changes if any critical, comment otherwise)
        if inline:
            event = "REQUEST_CHANGES" if any(f.get("severity") == "critical" for f in findings) else "COMMENT"
            try:
                r = client.post(
                    f"{base}/pulls/{pr_number}/reviews",
                    headers=headers,
                    json={
                        "event": event,
                        "body": f"LakeLogic Review — {len(inline)} inline finding(s).",
                        "comments": inline,
                    },
                )
                if r.status_code < 300:
                    review_id = r.json().get("id")
                else:
                    logger.warning(f"GitHub PR review failed: {r.status_code} {r.text[:300]}")
            except httpx.HTTPError as e:
                logger.warning(f"GitHub PR review POST failed: {e}")

        # 2. Upsert summary issue comment (PRs are issues for comment endpoints)
        summary_body = _build_summary_markdown(report)
        existing_id = _find_existing_marker_comment(client, base, headers, pr_number)
        try:
            if existing_id:
                r = client.patch(
                    f"{base}/issues/comments/{existing_id}",
                    headers=headers,
                    json={"body": summary_body},
                )
            else:
                r = client.post(
                    f"{base}/issues/{pr_number}/comments",
                    headers=headers,
                    json={"body": summary_body},
                )
            if r.status_code < 300:
                summary_id = r.json().get("id")
            else:
                logger.warning(f"GitHub summary comment failed: {r.status_code} {r.text[:300]}")
        except httpx.HTTPError as e:
            logger.warning(f"GitHub summary comment POST failed: {e}")

    return review_id, summary_id


def _find_existing_marker_comment(client, base: str, headers: dict, pr_number: int) -> Optional[int]:
    """Find a prior LakeLogic summary comment on this PR (by sentinel marker)."""
    import httpx

    try:
        r = client.get(
            f"{base}/issues/{pr_number}/comments?per_page=100",
            headers=headers,
        )
    except httpx.HTTPError:
        return None
    if r.status_code >= 300:
        return None
    for c in r.json() or []:
        if SUMMARY_MARKER in (c.get("body") or ""):
            return c.get("id")
    return None


def write_output(report: dict[str, Any], output_format: str) -> str:
    """Route to the appropriate formatter and return the output string.

    Args:
        report: ReviewReport dict.
        output_format: One of ``terminal``, ``json``, ``sarif``, ``github``,
            ``github_pr``, ``azure_pr``.

    Returns:
        Formatted output string.
    """
    formatters = {
        "terminal": format_terminal,
        "json": format_json,
        "sarif": format_sarif,
        "github": format_github,
        "github_pr": format_github_pr,
        "azure_pr": format_azure_pr,
    }

    formatter = formatters.get(output_format)
    if not formatter:
        logger.warning(f"Unknown output format '{output_format}', falling back to terminal")
        formatter = format_terminal

    return formatter(report)
