"""
lakelogic.cli.review
--------------------
``lakelogic review`` — tiered (deterministic + LLM) code-quality reviewer.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import typer
from loguru import logger

from lakelogic.ai.code_reviewer import determine_exit_code, run_review
from lakelogic.ai.diff_collector import ShallowCloneError, collect_changed_files
from lakelogic.ai.review_config import load_config, render_check_config
from lakelogic.ai.review_formatters import write_output


def review_command(
    paths: list[Path] = typer.Argument(None, help="Files or directories to review (default: '.')."),
    diff: Optional[str] = typer.Option(
        None, "--diff", help="Only review files changed vs this git ref (e.g. origin/main)."
    ),
    include: Optional[list[str]] = typer.Option(None, "--include", help="Glob to include (repeatable)."),
    exclude: Optional[list[str]] = typer.Option(None, "--exclude", help="Glob to exclude (repeatable)."),
    no_llm: bool = typer.Option(False, "--no-llm", help="Skip Tier 2 LLM review."),
    provider: Optional[str] = typer.Option(
        None, "--provider", help="LLM provider: anthropic | openai (default: env auto-detect)."
    ),
    model: Optional[str] = typer.Option(None, "--model", help="Model name override."),
    output_format: str = typer.Option("terminal", "--format", help="terminal | json | sarif | github"),
    output: Optional[Path] = typer.Option(None, "--output", "-o", help="Output file (default: stdout)."),
    fail_on: Optional[str] = typer.Option(
        None,
        "--fail-on",
        help="Exit 1 on findings at: critical | warning | info | never (default: critical).",
    ),
    max_files: Optional[int] = typer.Option(None, "--max-files", help="Cap on files reviewed per run (default: 50)."),
    config: Optional[Path] = typer.Option(
        None, "--config", help="Path to .lakelogic-review.toml (default: ./.lakelogic-review.toml)."
    ),
    check_config: bool = typer.Option(
        False, "--check-config", help="Print resolved config and exit (never prints key value)."
    ),
    no_cache: bool = typer.Option(False, "--no-cache", help="Bypass the diff-hash cache."),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Debug logging."),
) -> None:
    """Run code review against the given paths or git diff."""
    if not verbose:
        logger.remove()
        logger.add(sys.stderr, level="INFO")

    cfg = load_config(
        config,
        cli_provider=provider,
        cli_model=model,
        cli_fail_on=fail_on,
        cli_max_files=max_files,
        cli_include=include,
        cli_exclude=exclude,
    )

    if check_config:
        typer.echo(render_check_config(cfg))
        raise typer.Exit(code=0)

    try:
        files = collect_changed_files(
            paths or [Path(".")],
            diff_ref=diff,
            include=cfg.include or None,
            exclude=cfg.exclude or None,
            max_files=cfg.max_files,
        )
    except ShallowCloneError as e:
        typer.echo(str(e), err=True)
        raise typer.Exit(code=2) from e

    if not files:
        typer.echo("No reviewable files found.", err=True)
        raise typer.Exit(code=0)

    report = run_review(
        files,
        no_llm=no_llm,
        custom_rules=cfg.custom_rules,
        provider=cfg.provider,
        model=cfg.model,
        api_key_present=cfg.api_key_present,
        severity_overrides=cfg.severity_overrides,
        max_tokens=cfg.max_tokens_per_batch,
        base_ref=diff,
        use_cache=not no_cache,
    )

    rendered = write_output(report.model_dump(), output_format)

    if output:
        output.write_text(rendered, encoding="utf-8")
    else:
        typer.echo(rendered)

    raise typer.Exit(code=determine_exit_code(report, cfg.fail_on))
