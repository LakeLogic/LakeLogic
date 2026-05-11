"""
lakelogic.ai.code_reviewer
--------------------------
Orchestrator for the tiered (deterministic + LLM) code reviewer.

Tier 1 (always runs, no API key required):
    ruff + sqlfluff + regex PII scanner. See ``tier1_runners``.

Tier 2 (only if an API key is present and ``no_llm`` is False):
    LLM-powered review via the prompts in ``review_prompts``,
    dispatched through ``llm_client.review_batch``.

Output is rendered by ``review_formatters`` (terminal | json | sarif | github).
"""

from __future__ import annotations

import time
from collections import Counter
from pathlib import Path
from typing import Literal, Optional

from loguru import logger
from pydantic import BaseModel, Field

Severity = Literal["critical", "warning", "info"]


class ReviewFinding(BaseModel):
    """A single code-review finding.

    Schema mirrors the JSON contract in ``review_prompts.SYSTEM_PROMPT``
    so LLM output and Tier 1 runner output share one type.
    """

    file: str
    line: Optional[int] = None
    severity: Severity
    category: str
    rule: str
    message: str
    suggestion: Optional[str] = None
    code_snippet: Optional[str] = None


class ReviewReport(BaseModel):
    """Aggregated review output consumed by ``review_formatters``."""

    files_scanned: int
    findings: list[ReviewFinding]
    summary: dict[str, int] = Field(default_factory=dict)
    ai_provider: str = "none"
    ai_model: str = "none"
    duration_seconds: float = 0.0
    token_usage: dict[str, int] = Field(default_factory=lambda: {"total": 0})


def _summarise(findings: list[ReviewFinding]) -> dict[str, int]:
    counts = Counter(f.severity for f in findings)
    return {
        "critical": counts.get("critical", 0),
        "warning": counts.get("warning", 0),
        "info": counts.get("info", 0),
    }


def _apply_severity_overrides(
    findings: list[ReviewFinding], overrides: dict[str, str]
) -> list[ReviewFinding]:
    """Remap finding severities based on the rule-id → severity override map."""
    if not overrides:
        return findings
    valid = {"critical", "warning", "info"}
    out: list[ReviewFinding] = []
    for f in findings:
        new_sev = overrides.get(f.rule)
        if new_sev and new_sev in valid and new_sev != f.severity:
            out.append(f.model_copy(update={"severity": new_sev}))
        else:
            out.append(f)
    return out


def run_review(
    files: list[Path],
    *,
    no_llm: bool = False,
    custom_rules: Optional[list[str]] = None,
    provider: str = "none",
    model: str = "none",
    api_key_present: bool = False,
    severity_overrides: Optional[dict[str, str]] = None,
    max_tokens: int = 30_000,
    base_ref: Optional[str] = None,
    use_cache: bool = True,
) -> ReviewReport:
    """Run Tier 1 (and Tier 2 if available) over the given files.

    Args:
        files: Files to review (already filtered by diff_collector).
        no_llm: Skip Tier 2 even if an API key is present.
        custom_rules: Plain-English rules from ``.lakelogic-review.toml``.
        provider: LLM provider to use ('anthropic' | 'openai' | 'none').
        model: Model name for the chosen provider.
        api_key_present: Whether the env has a usable API key.
        severity_overrides: Rule-ID → severity remapping from config.
        max_tokens: Per-batch token budget for Tier 2.

    Returns:
        ReviewReport ready to hand to ``review_formatters.write_output``.
    """
    from lakelogic.ai import review_cache, tier1_runners  # local import to avoid cycles

    start = time.perf_counter()

    # Cache check — only meaningful when Tier 2 would otherwise run.
    cache_key = ""
    if use_cache and not no_llm and api_key_present and provider != "none":
        cache_key = review_cache.compute_cache_key(files, extra=f"{provider}:{model}")
        cached = review_cache.load_cached_report(cache_key)
        if cached:
            logger.info("Reusing cached review (no diff change since last run)")
            return ReviewReport.model_validate(cached)

    tier1: list[ReviewFinding] = []
    tier1.extend(tier1_runners.run_ruff(files))
    tier1.extend(tier1_runners.run_sqlfluff(files))
    tier1.extend(tier1_runners.scan_pii_patterns(files))
    tier1.extend(tier1_runners.scan_perf_smells(files))
    tier1.extend(tier1_runners.scan_unused_cache(files))
    tier1.extend(tier1_runners.scan_withcolumn_in_loop(files))
    tier1.extend(tier1_runners.run_datacontract_diff(files, base_ref=base_ref))

    findings = list(tier1)
    token_usage = {"total": 0}

    if no_llm:
        logger.info("Tier 2 skipped: --no-llm")
    elif not api_key_present or provider == "none":
        logger.info("Tier 2 skipped: no API key found")
        findings.append(
            ReviewFinding(
                file=".",
                severity="info",
                category="config",
                rule="llm_review_skipped",
                message=(
                    "Tier 2 LLM review skipped: set ANTHROPIC_API_KEY or "
                    "OPENAI_API_KEY to enable architectural / governance checks."
                ),
                suggestion="export ANTHROPIC_API_KEY=sk-ant-...",
            )
        )
    else:
        from lakelogic.ai.llm_client import review_batch

        logger.info(f"Running Tier 2 LLM review with {provider}/{model}")
        tier1_dicts = [f.model_dump() for f in tier1]
        llm_findings, token_usage = review_batch(
            files,
            tier1_findings=tier1_dicts,
            custom_rules=custom_rules,
            provider=provider,
            model=model,
            max_tokens=max_tokens,
        )
        findings.extend(llm_findings)

    findings = _apply_severity_overrides(findings, severity_overrides or {})

    duration = time.perf_counter() - start

    report = ReviewReport(
        files_scanned=len(files),
        findings=findings,
        summary=_summarise(findings),
        ai_provider=provider,
        ai_model=model,
        duration_seconds=duration,
        token_usage=token_usage,
    )

    if cache_key:
        review_cache.save_cached_report(cache_key, report.model_dump())

    return report


def determine_exit_code(report: ReviewReport, fail_on: str) -> int:
    """Return 1 if any finding meets the ``fail_on`` threshold, else 0."""
    if fail_on == "never":
        return 0
    thresholds = {
        "critical": ["critical"],
        "warning": ["critical", "warning"],
        "info": ["critical", "warning", "info"],
    }
    blocking = thresholds.get(fail_on, ["critical"])
    for f in report.findings:
        if f.severity in blocking:
            return 1
    return 0
