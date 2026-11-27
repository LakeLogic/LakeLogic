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
#: WHEN to use LakeLogic, not how: the standing rules an agent follows in a governed repository.
#: First in the guide, and the section `scaffold` writes into AGENTS.md / CLAUDE.md (2026-11-24).
RULES_PATH = Path(__file__).resolve().parents[1] / "agent_rules.md"

#: The marked block LakeLogic owns inside an agent-instructions file. Everything outside it is
#: the repository's own and is never touched; writing again replaces only the block.
SECTION_START = "<!-- lakelogic:start -->"
SECTION_END = "<!-- lakelogic:end -->"


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
            rows.append(
                (
                    f"lakelogic {group.name} {name}",
                    _first_line(cmd.help or (cmd.callback.__doc__ if cmd.callback else "")),
                )
            )
    rows.append(("lakelogic-driver", "Run a registry of contracts bronze → silver → gold (`lakelogic-driver --help`)."))
    out = [
        "## Every command",
        "",
        "Options for any command: `lakelogic <command> --help`.",
        "",
        "| Command | What it does |",
        "|---|---|",
    ]
    out += [f"| `{n}` | {h} |" for n, h in sorted(rows)]
    return "\n".join(out) + "\n"


def standing_rules() -> str:
    return RULES_PATH.read_text(encoding="utf-8").rstrip() + "\n"


def full_guide() -> str:
    guide = GUIDE_PATH.read_text(encoding="utf-8").rstrip()
    # The rules go straight after the guide's title and opening paragraphs, before Install: an
    # agent that reads only the top of the file still learns when to use LakeLogic.
    head, sep, rest = guide.partition("\n## ")
    body = f"{head.rstrip()}\n\n{standing_rules()}\n## {rest}" if sep else f"{guide}\n\n{standing_rules()}"
    return body.rstrip() + "\n\n" + command_table()


def write_section(path: Path, text: str) -> str:
    """Put ``text`` in ``path`` between the LakeLogic markers. Creates the file if missing,
    replaces an existing LakeLogic block, or appends one; returns "created", "updated" or
    "added". Content outside the markers is kept byte for byte."""
    block = f"{SECTION_START}\n{text.rstrip()}\n{SECTION_END}\n"
    if not path.exists():
        path.write_text(block, encoding="utf-8")
        return "created"
    current = path.read_text(encoding="utf-8")
    start, end = current.find(SECTION_START), current.find(SECTION_END)
    if start != -1 and end > start:
        path.write_text(current[:start] + block.rstrip("\n") + current[end + len(SECTION_END) :], encoding="utf-8")
        return "updated"
    sep = "" if current.endswith("\n\n") else ("\n" if current.endswith("\n") else "\n\n")
    path.write_text(current + sep + block, encoding="utf-8")
    return "added"


def agent_guide_command(
    write: Optional[Path] = typer.Option(
        None,
        "--write",
        help="Write the guide into this file (e.g. AGENTS.md) between LakeLogic markers; the rest of the file is kept.",
    ),
) -> None:
    """How an AI agent uses LakeLogic: when to use it, install, run a contract, outputs, every command."""
    text = full_guide()
    if write is None:
        typer.echo(text)
        return
    # An existing AGENTS.md is the repository's own: refusing it meant the guide reached only
    # empty repos. Only the marked LakeLogic block is written; the rest is left as it was.
    outcome = write_section(write, text)
    typer.echo(f"{outcome.capitalize()} the LakeLogic section in {write}")
