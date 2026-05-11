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
import sys
from typing import Any

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
        f" Scanned: {report['files_scanned']} files | "
        f"Provider: {report['ai_provider']} / {report['ai_model']}"
    )
    lines.append(
        f" Duration: {report['duration_seconds']:.1f}s | "
        f"Tokens: {report['token_usage'].get('total', 0):,}"
    )
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
            physical: dict[str, Any] = {
                "artifactLocation": {"uri": finding["file"]}
            }
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
    "warning": 1,   # active
    "info": 4,      # informational comment, no thread status
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
    base = (
        f"{org_url}/{project}/_apis/git/repositories/{repo_id}"
        f"/pullRequests/{pr_id}/threads?api-version=7.1"
    )

    posted = 0
    with httpx.Client(timeout=15) as client:
        # Summary comment (always posted, even if no findings)
        summary_body = (
            f"**LakeLogic Review** — {summary.get('critical', 0)} critical, "
            f"{summary.get('warning', 0)} warnings, {summary.get('info', 0)} info"
        )
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

            content = f"**[{f.get('severity', 'info').upper()}] {f.get('category')}/{f.get('rule')}**\n\n{f.get('message', '')}"
            if f.get("suggestion"):
                content += f"\n\n**Fix:** {f['suggestion']}"

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
                    logger.warning(
                        f"ADO inline thread for {file_path}:{line} failed: "
                        f"{r.status_code} {r.text[:200]}"
                    )
            except httpx.HTTPError as e:
                logger.warning(f"ADO inline thread for {file_path}:{line} failed: {e}")

    return posted


def write_output(report: dict[str, Any], output_format: str) -> str:
    """Route to the appropriate formatter and return the output string.

    Args:
        report: ReviewReport dict.
        output_format: One of ``terminal``, ``json``, ``sarif``, ``github``, ``azure_pr``.

    Returns:
        Formatted output string.
    """
    formatters = {
        "terminal": format_terminal,
        "json": format_json,
        "sarif": format_sarif,
        "github": format_github,
        "azure_pr": format_azure_pr,
    }

    formatter = formatters.get(output_format)
    if not formatter:
        logger.warning(f"Unknown output format '{output_format}', falling back to terminal")
        formatter = format_terminal

    return formatter(report)
