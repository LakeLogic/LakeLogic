"""`lakelogic agent-guide`: what any AI agent reads to use Core (owner, 2026-11-18).

Complete (the command table is generated from the CLI, and every command the guide names must
exist) and true (its quick start runs as written).
"""

from __future__ import annotations

import re

import pytest
from typer.testing import CliRunner

from lakelogic.cli.agent_guide_cmd import (
    GUIDE_PATH,
    RULES_PATH,
    SECTION_END,
    SECTION_START,
    command_table,
    full_guide,
    standing_rules,
    write_section,
)
from lakelogic.cli.main import app


def test_guide_ships_in_the_package_and_names_both_entry_points():
    text = GUIDE_PATH.read_text(encoding="utf-8")
    assert "olc guide" in text and "lakelogic agent-guide" in text


def test_every_command_the_guide_names_exists():
    table = command_table()
    named = set(re.findall(r"`lakelogic ([a-z][a-z-]*(?: [a-z][a-z-]*)?)", GUIDE_PATH.read_text(encoding="utf-8")))
    for cmd in named:
        first = cmd.split()[0]
        if first == "agent-guide":
            continue
        assert f"`lakelogic {first}" in table, f"the guide names `lakelogic {cmd}`, which does not exist"


def test_command_table_lists_run_and_sub_commands():
    table = command_table()
    assert "`lakelogic run`" in table and "`lakelogic registry validate`" in table


def test_cli_prints_and_writes_into_an_existing_agents_file(tmp_path):
    """--write refused an existing AGENTS.md, so the guide reached only empty repos
    (2026-11-24). It now writes a marked block and keeps the repository's own text."""
    runner = CliRunner()
    out = runner.invoke(app, ["agent-guide"])
    assert out.exit_code == 0 and "## Every command" in out.output
    target = tmp_path / "AGENTS.md"
    target.write_text("# Our repo\n\nUse tabs.\n", encoding="utf-8")
    assert runner.invoke(app, ["agent-guide", "--write", str(target)]).exit_code == 0
    text = target.read_text(encoding="utf-8")
    assert text.startswith("# Our repo\n\nUse tabs.\n")
    assert SECTION_START in text and full_guide().rstrip() in text
    # Writing again replaces the block, never duplicates it.
    assert runner.invoke(app, ["agent-guide", "--write", str(target)]).exit_code == 0
    assert target.read_text(encoding="utf-8") == text


def test_quick_start_runs_as_written(tmp_path):
    pytest.importorskip("polars")
    from lakelogic import DataProcessor

    (tmp_path / "orders.csv").write_text("order_id,amount\n1,10\n2,-5\n", encoding="utf-8")
    contract = {
        "version": "1.0.0",
        "info": {"title": "orders"},
        "model": {"fields": [{"name": "order_id", "type": "integer"}, {"name": "amount", "type": "integer"}]},
        "quality": {"row_rules": [{"name": "amount_positive", "sql": "amount > 0"}]},
    }
    proc = DataProcessor(contract, engine="polars")
    result = proc.run_source(str(tmp_path / "orders.csv"))
    good, bad = result
    assert len(good) == 1 and len(bad) == 1
    assert "_lakelogic_errors" in bad.columns
    assert proc.last_report


def test_run_reads_the_contracts_source_path_when_source_is_omitted(tmp_path):
    import yaml

    csv = tmp_path / "orders.csv"
    csv.write_text("order_id,amount\n1,10\n", encoding="utf-8")
    contract = tmp_path / "orders.olc.yaml"
    contract.write_text(
        yaml.safe_dump(
            {
                "version": "1.0.0",
                "info": {"title": "orders"},
                "source": {"type": "landing", "path": str(csv), "format": "csv"},
                "model": {"fields": [{"name": "order_id", "type": "integer"}, {"name": "amount", "type": "integer"}]},
            }
        ),
        encoding="utf-8",
    )
    out = CliRunner().invoke(app, ["run", "--contract", str(contract)])
    assert out.exit_code == 0, out.output


def test_a_failing_dataset_rule_is_one_readable_line(tmp_path):
    import yaml

    csv = tmp_path / "orders.csv"
    csv.write_text("order_id,amount\n1,10\n1,20\n", encoding="utf-8")
    contract = tmp_path / "orders.olc.yaml"
    contract.write_text(
        yaml.safe_dump(
            {
                "version": "1.0.0",
                "info": {"title": "orders"},
                "model": {"fields": [{"name": "order_id", "type": "integer"}, {"name": "amount", "type": "integer"}]},
                "quality": {"fail_pipeline_on_dataset_error": True, "dataset_rules": [{"unique": "order_id"}]},
            }
        ),
        encoding="utf-8",
    )
    out = CliRunner().invoke(app, ["run", "-c", str(contract), "--source", str(csv)])
    assert out.exit_code == 1
    assert "Traceback" not in out.output


def test_unique_failures_are_described_in_words():
    from lakelogic.core.processor import _describe_dataset_failure

    assert (
        _describe_dataset_failure({"name": "order_id_unique", "value": "1 (expected < 1.0)"})
        == "order_id_unique: 1 duplicate value(s)"
    )
    assert (
        _describe_dataset_failure({"name": "min_rows", "value": "0 (expected > 1)"}) == "min_rows: got 0, must be > 1"
    )


# ── Standing rules: WHEN an agent uses LakeLogic (2026-11-24) ─────────────────────────────────
def test_the_rules_ship_and_every_command_they_name_exists():
    text = RULES_PATH.read_text(encoding="utf-8")
    table = command_table()
    named = set(re.findall(r"`lakelogic ([a-z][a-z-]*)", text))
    assert {"validate", "generate", "run", "assert"} <= named
    for cmd in named - {"agent-guide"}:
        assert f"`lakelogic {cmd}" in table, f"the rules name `lakelogic {cmd}`, which does not exist"


def test_the_rules_come_before_install_in_the_guide():
    guide = full_guide()
    assert guide.index("## Standing rules for this repository") < guide.index("## Install")


def test_write_section_creates_updates_and_keeps_the_rest(tmp_path):
    path = tmp_path / "CLAUDE.md"
    assert write_section(path, "one") == "created"
    assert path.read_text(encoding="utf-8") == f"{SECTION_START}\none\n{SECTION_END}\n"
    path.write_text("before\n" + path.read_text(encoding="utf-8") + "after\n", encoding="utf-8")
    assert write_section(path, "two") == "updated"
    assert path.read_text(encoding="utf-8") == f"before\n{SECTION_START}\ntwo\n{SECTION_END}\nafter\n"
    other = tmp_path / "AGENTS.md"
    other.write_text("ours", encoding="utf-8")
    assert write_section(other, "three") == "added"
    assert other.read_text(encoding="utf-8") == f"ours\n\n{SECTION_START}\nthree\n{SECTION_END}\n"


def test_scaffold_writes_the_rules_for_claude_and_other_agents(tmp_path):
    import yaml

    contracts = tmp_path / "contracts"
    contracts.mkdir()
    (contracts / "orders.yaml").write_text(
        yaml.safe_dump(
            {
                "version": "1.0.0",
                "info": {
                    "title": "orders",
                    "table_name": "orders",
                    "target_layer": "bronze",
                    "domain": "sales",
                    "system": "shop",
                },
                "source": {"type": "landing", "path": "landing/orders", "format": "csv"},
                "model": {"fields": [{"name": "order_id", "type": "integer"}]},
            }
        ),
        encoding="utf-8",
    )
    out = tmp_path / "project"
    result = CliRunner().invoke(app, ["scaffold", str(contracts), "--out", str(out)])
    assert result.exit_code == 0, result.output
    for name in ("AGENTS.md", "CLAUDE.md"):
        text = (out / name).read_text(encoding="utf-8")
        assert SECTION_START in text and standing_rules().rstrip() in text


def test_the_claude_code_skill_names_only_real_commands():
    from pathlib import Path

    skill = Path(__file__).resolve().parents[1] / "skills" / "govern-pipeline" / "SKILL.md"
    text = skill.read_text(encoding="utf-8")
    assert text.startswith("---\nname: govern-pipeline\ndescription: ")
    table = command_table()
    for cmd in set(re.findall(r"`lakelogic ([a-z][a-z-]*)", text)) - {"agent-guide"}:
        assert f"`lakelogic {cmd}" in table, f"the skill names `lakelogic {cmd}`, which does not exist"
