"""``lakelogic migrate-options`` — move contracts onto OLC 0.21 typed source options."""

from __future__ import annotations

import io
from pathlib import Path
from typing import List

import typer


def _yaml():
    """ruamel (keeps comments and key order) when installed, else PyYAML."""
    try:
        from ruamel.yaml import YAML

        y = YAML()
        y.preserve_quotes = True
        y.width = 4096
        return "ruamel", y
    except ImportError:  # pragma: no cover - depends on the environment
        import yaml

        return "pyyaml", yaml


def _files(paths: List[Path]) -> List[Path]:
    out: List[Path] = []
    for p in paths:
        if p.is_dir():
            out += sorted(q for q in p.rglob("*") if q.suffix in (".yaml", ".yml"))
        else:
            out.append(p)
    return out


def migrate_options_command(
    paths: List[Path] = typer.Argument(..., help="Contract files or folders (searched for *.yaml / *.yml)."),
    write: bool = typer.Option(False, "--write", help="Rewrite the files. Without it, only report."),
) -> None:
    """Rewrite contracts to OLC 0.21 typed source options.

    Moves format settings from `source` into `source.options`, renames old spellings
    (`sep` → `delimiter`, `multiLine` → `multiline`), moves unknown options to
    `options.engine_options`, and REMOVES literal credentials (reported, never copied).
    """
    from lakelogic.core.migrate_options import migrate_contract

    kind, y = _yaml()
    if write and kind == "pyyaml":
        typer.echo("note: ruamel.yaml is not installed — comments in rewritten files are lost.")
    changed = failed = 0
    for path in _files(paths):
        text = path.read_text(encoding="utf-8")
        try:
            data = y.load(text) if kind == "ruamel" else y.safe_load(text)
        except Exception as exc:  # noqa: BLE001 - report and continue
            typer.echo(f"{path}: not YAML ({exc.__class__.__name__}) — skipped")
            continue
        if not isinstance(data, dict) or not isinstance(data.get("source"), dict):
            continue
        notes, problems = migrate_contract(data)
        if not notes and not problems:
            continue
        typer.echo(f"{path}:")
        for n in notes:
            typer.echo(f"  - {n}")
        for p in problems:
            typer.echo(f"  ! still invalid: {p}")
        failed += bool(problems)
        if notes:
            changed += 1
            if write:
                if kind == "ruamel":
                    buf = io.StringIO()
                    y.dump(data, buf)
                    path.write_text(buf.getvalue(), encoding="utf-8")
                else:
                    path.write_text(y.safe_dump(data, sort_keys=False), encoding="utf-8")
    verb = "rewrote" if write else "would rewrite (use --write)"
    typer.echo(f"{verb} {changed} contract(s); {failed} still need a manual fix.")
    if failed:
        raise typer.Exit(code=1)
