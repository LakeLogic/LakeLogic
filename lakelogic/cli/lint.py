"""``lakelogic lint`` — deterministic data-contract governance lint.

Reviews contracts for governance gaps (PII tagging/masking, keys, delete
strategy, quality rules, SCD2 config, source layout, freshness/volume SLOs).
Rules-based and reproducible — safe to run in CI on every contract PR.
"""

from __future__ import annotations

from pathlib import Path
from typing import List

import typer


def lint_command(
    paths: List[Path] = typer.Argument(..., help="Contract file(s) or directory to lint."),
    fmt: str = typer.Option("text", "--format", "-f", help="Output format: text | json | github."),
    fail_on: str = typer.Option(
        "none",
        "--fail-on",
        help="Exit non-zero if any RULES finding is at least this severity: none | info | warning | critical. "
        "(LLM/judgment findings are advisory and never gate.)",
    ),
    llm: bool = typer.Option(
        False,
        "--llm",
        help="Also run the LLM judgment layer (advisory). Requires an API key; a no-op without one.",
    ),
):
    """Review data contracts for governance gaps."""
    import yaml

    from lakelogic.core.contract_lint import (
        SEVERITY_RANK,
        gate_severity,
        iter_contract_files,
        load_context,
        render_github,
        review_paths,
    )

    report = review_paths(paths)

    if llm:
        from lakelogic.ai.contract_judge import judge_contract

        for f in iter_contract_files(paths):
            try:
                raw = yaml.safe_load(f.read_text(encoding="utf-8"))
            except Exception:
                continue
            if not isinstance(raw, dict):
                continue
            for finding in judge_contract(raw, f.stem, load_context(f)):
                finding.file = str(f)
                report.findings.append(finding)
                report.summary[finding.severity] = report.summary.get(finding.severity, 0) + 1

    if fmt == "json":
        typer.echo(report.model_dump_json(indent=2))
    elif fmt == "github":
        typer.echo(render_github(report))
    else:
        _render_text(report, SEVERITY_RANK)

    if fail_on != "none":
        worst = gate_severity(report)  # rules-only; LLM findings never block
        if worst is not None and SEVERITY_RANK.get(worst, 0) >= SEVERITY_RANK.get(fail_on, 99):
            raise typer.Exit(code=1)
    raise typer.Exit(code=0)


_SEV_STYLE = {
    "critical": ("✖", typer.colors.RED),
    "warning": ("⚠", typer.colors.YELLOW),
    "info": ("•", typer.colors.CYAN),
}


def _render_text(report, rank) -> None:
    by_contract: dict = {}
    for f in report.findings:
        by_contract.setdefault(f.contract, []).append(f)

    if not report.findings:
        typer.echo(typer.style("✓ No governance findings.", fg=typer.colors.GREEN, bold=True))

    for contract, findings in by_contract.items():
        typer.echo(typer.style(f"\n{contract}", bold=True))
        for f in sorted(findings, key=lambda x: -rank.get(x.severity, 0)):
            icon, color = _SEV_STYLE.get(f.severity, ("•", None))
            head = typer.style(f"  {icon} {f.check_id}", fg=color, bold=True)
            loc = typer.style(f" [{f.field}]", fg=typer.colors.MAGENTA) if f.field else ""
            typer.echo(f"{head}{loc}  {f.message}")
            if f.suggestion:
                typer.echo(typer.style(f"      → {f.suggestion}", dim=True))

    s = report.summary
    typer.echo(
        typer.style(
            f"\n{report.contracts_scanned} contract(s) · "
            f"{s.get('critical', 0)} critical · {s.get('warning', 0)} warning · {s.get('info', 0)} info",
            bold=True,
        )
    )
