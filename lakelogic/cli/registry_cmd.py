"""``lakelogic registry`` — validate and inspect the mesh registry layer.

Subcommands:
  * ``validate PATH`` — structural + referential validation of ``_domain.yaml`` /
    ``_system.yaml`` (a single file or a whole tree).
  * ``explain PATH``  — for a ``_system.yaml``, show where each governance/identity key
    comes from: declared on the system, inherited from the domain, or a default.
  * ``schema {domain|system}`` — print (or write) the JSON Schema for a manifest.

The registry layer sits above individual OLC contracts; see
``docs/contracts/inheritance.md`` for the precedence these commands reflect.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import typer

from lakelogic.registry.models import (
    DomainManifestV1,
    SystemManifestV1,
)
from lakelogic.registry.provenance import Origin
from lakelogic.registry.resolver import resolve_system
from lakelogic.registry.validator import (
    _kind_of,
    _sibling_domain,
    summarize,
    validate_file,
    validate_tree,
)

registry_app = typer.Typer(
    name="registry",
    help="Validate and inspect the LakeLogic mesh registry (_domain.yaml / _system.yaml).",
    no_args_is_help=True,
)

_LEVEL_STYLE = {
    "error": (typer.colors.RED, "✖"),
    "warning": (typer.colors.YELLOW, "⚠"),
    "info": (typer.colors.CYAN, "ℹ"),
}


@registry_app.command("validate")
def validate_cmd(
    path: Path = typer.Argument(..., help="A registry file or a directory to walk."),
    strict: bool = typer.Option(False, "--strict", help="Treat warnings as failures (exit 1)."),
):
    """Validate registry manifests structurally and referentially.

    Exit code 0 = clean; non-zero = at least one error (or a warning under --strict).
    """
    if not path.exists():
        typer.secho(f"✖  Path not found: {path}", fg=typer.colors.RED)
        raise typer.Exit(2)

    reports = validate_tree(path)
    if not reports:
        typer.secho(f"ℹ  No _domain.yaml / _system.yaml found under {path}", fg=typer.colors.CYAN)
        raise typer.Exit(0)

    for r in reports:
        if not r.findings and r.ok:
            typer.secho(f"✓  {r.path}", fg=typer.colors.GREEN)
            continue
        icon_color = typer.colors.GREEN if r.ok else typer.colors.RED
        typer.secho(f"{'✓' if r.ok else '✖'}  {r.path}", fg=icon_color, bold=not r.ok)
        for f in r.findings:
            color, icon = _LEVEL_STYLE.get(f.level, (typer.colors.WHITE, "•"))
            typer.secho(f"     {icon} {f.message}", fg=color)
            if f.hint:
                typer.secho(f"        → {f.hint}", dim=True)

    files, errors, warnings = summarize(reports)
    typer.echo("")
    summary = f"{files} file(s), {errors} error(s), {warnings} warning(s)"
    if errors or (strict and warnings):
        typer.secho(f"✖  {summary}", fg=typer.colors.RED, bold=True)
        raise typer.Exit(1)
    typer.secho(f"✓  {summary}", fg=typer.colors.GREEN, bold=True)


@registry_app.command("explain")
def explain_cmd(
    path: Path = typer.Argument(..., help="A _system.yaml to explain."),
    deep: bool = typer.Option(False, "--deep", help="Show per-leaf origin inside merged blocks."),
    key: Optional[str] = typer.Option(None, "--key", help="Explain a single key or dotted path (e.g. slo.freshness)."),
    env: Optional[str] = typer.Option(None, "--env", help="Annotate values that vary by this environment (e.g. dev)."),
):
    """Show where each key on a resolved system comes from — declared on the system,
    inherited from the domain, deep-merged, or domain-locked.

    Backed by the resolver's real domain → system merge, so the origin/reason reflect the
    actual precedence rules. Environment substitution and per-contract injection are not
    applied here (they remain the runtime's job)."""
    if _kind_of(path) != "system":
        typer.secho("✖  explain expects a _system.yaml file.", fg=typer.colors.RED)
        raise typer.Exit(2)
    if not path.exists():
        typer.secho(f"✖  File not found: {path}", fg=typer.colors.RED)
        raise typer.Exit(2)

    report = validate_file(path, sibling_domain=_sibling_domain(path))
    if not report.ok:
        typer.secho("✖  System manifest is invalid — fix it before explaining:", fg=typer.colors.RED)
        for f in report.findings:
            if f.is_error:
                typer.secho(f"     ✖ {f.message}", fg=typer.colors.RED)
        raise typer.Exit(1)

    resolved = resolve_system(path, environment=env)
    typer.secho(f"\n  {resolved.domain or '?'} / {resolved.system or '?'}", bold=True)
    typer.secho(f"  domain defaults: {resolved.domain_file or '(none found)'}", dim=True)
    if env:
        typer.secho(f"  environment:     {env}", dim=True)
    typer.echo("")

    _origin_color = {
        Origin.SYSTEM: typer.colors.GREEN,
        Origin.DOMAIN: typer.colors.CYAN,
        Origin.BOTH: typer.colors.CYAN,
        Origin.DEFAULT: typer.colors.WHITE,
        Origin.ENVIRONMENT: typer.colors.MAGENTA,
    }

    def _emit(k: str) -> None:
        p = resolved.provenance[k]
        color = (
            typer.colors.YELLOW
            if p.reason.value.endswith("locked")
            else _origin_color.get(p.origin, typer.colors.WHITE)
        )
        typer.secho(f"  {k:<32} ", nl=False)
        typer.secho(f"{p.origin.value:<16} ", fg=color, nl=False)
        typer.secho(p.reason.value, dim=True)

    typer.secho(f"  {'KEY':<32} {'ORIGIN':<16} WHY", bold=True)
    typer.secho("  " + "─" * 64, dim=True)

    # Filtering: a single key/prefix, all leaves (--deep), or just top-level.
    keys = [k for k in sorted(resolved.provenance) if not k.startswith("x-")]
    if key:
        keys = [k for k in keys if k == key or k.startswith(key + ".")]
        if not keys:
            typer.secho(f"  (no key matching '{key}')", dim=True)
    elif not deep:
        # top-level keys, plus any environment annotations (which are dotted paths).
        keys = [k for k in keys if "." not in k or resolved.provenance[k].origin == Origin.ENVIRONMENT]

    for k in keys:
        _emit(k)
    if not deep and not key:
        typer.secho("\n  Tip: add --deep (or --key slo.freshness) to trace nested values.", dim=True)
    typer.echo("")


@registry_app.command("schema")
def schema_cmd(
    which: str = typer.Argument(..., help="Which manifest: 'domain' or 'system'."),
    output: Optional[Path] = typer.Option(None, "--output", "-o", help="Write the schema to a file."),
):
    """Print the JSON Schema for a registry manifest (or write it with --output)."""
    which = which.strip().lower()
    model = {"domain": DomainManifestV1, "system": SystemManifestV1}.get(which)
    if model is None:
        typer.secho("✖  which must be 'domain' or 'system'.", fg=typer.colors.RED)
        raise typer.Exit(2)

    schema = model.model_json_schema()
    text = json.dumps(schema, indent=2)
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text + "\n", encoding="utf-8")
        typer.secho(f"✓  Wrote {which} manifest schema → {output}", fg=typer.colors.GREEN)
    else:
        typer.echo(text)
