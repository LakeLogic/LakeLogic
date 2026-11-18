"""``lakelogic agent-guide`` — how any AI agent uses LakeLogic Core (owner, 2026-11-18).

An agent in another program sees only the installed package and its command help. This prints
the guide that ships in the wheel (``lakelogic/agent_guide.md``) plus a table of EVERY command,
generated from the CLI itself, so a new command can never be missing from what agents read.
``--write AGENTS.md`` saves it where agents look for standing instructions.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

GUIDE_PATH = Path(__file__).resolve().parents[1] / "agent_guide.md"


def _first_line(text: Optional[str]) -> str:
    """The first sentence of the first paragraph (help text wraps across lines)."""
    para = " ".join((text or "").strip().split("\n\n", 1)[0].split())
    cut = para.find(". ")
    return (para[: cut + 1] if cut >= 0 else para).replace("|", "\\|")


def command_table() -> str:
    """Every ``lakelogic`` command and sub-command with its one-line help, from the Typer app."""
    from lakelogic.cli.main import app

    rows: list[tuple[str, str]] = []
    for cmd in app.registered_commands:
        name = cmd.name or (cmd.callback.__name__.replace("_", "-") if cmd.callback else "")
        if name == "agent-guide" or cmd.hidden:
            continue
        rows.append((f"lakelogic {name}", _first_line(cmd.help or (cmd.callback.__doc__ if cmd.callback else ""))))
    for group in app.registered_groups:
        sub = group.typer_instance
        for cmd in sub.registered_commands:
            name = cmd.name or (cmd.callback.__name__.replace("_", "-") if cmd.callback else "")
            rows.append((f"lakelogic {group.name} {name}",
                         _first_line(cmd.help or (cmd.callback.__doc__ if cmd.callback else ""))))
    rows.append(("lakelogic-driver", "Run a registry of contracts bronze → silver → gold (`lakelogic-driver --help`)."))
    out = ["## Every command", "", "Options for any command: `lakelogic <command> --help`.", "",
           "| Command | What it does |", "|---|---|"]
    out += [f"| `{n}` | {h} |" for n, h in sorted(rows)]
    return "\n".join(out) + "\n"


def full_guide() -> str:
    return GUIDE_PATH.read_text(encoding="utf-8").rstrip() + "\n\n" + command_table()


def agent_guide_command(
    write: Optional[Path] = typer.Option(
        None, "--write", help="Save the guide to this file (e.g. AGENTS.md) instead of printing it."
    ),
) -> None:
    """How an AI agent uses LakeLogic: install, run a contract, outputs, every command."""
    text = full_guide()
    if write is None:
        typer.echo(text)
        return
    if write.exists():
        typer.echo(f"{write} exists; not overwritten. Choose another path or remove it first.", err=True)
        raise typer.Exit(1)
    write.write_text(text, encoding="utf-8")
    typer.echo(f"Wrote {write}")
