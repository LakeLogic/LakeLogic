"""`lakelogic agent-guide`: what any AI agent reads to use Core (owner, 2026-11-18).

Complete (the command table is generated from the CLI, and every command the guide names must
exist) and true (its quick start runs as written).
"""
from __future__ import annotations

import re

import pytest
from typer.testing import CliRunner

from lakelogic.cli.agent_guide_cmd import GUIDE_PATH, command_table, full_guide
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


def test_cli_prints_and_refuses_to_overwrite(tmp_path):
    runner = CliRunner()
    out = runner.invoke(app, ["agent-guide"])
    assert out.exit_code == 0 and "## Every command" in out.output
    target = tmp_path / "AGENTS.md"
    assert runner.invoke(app, ["agent-guide", "--write", str(target)]).exit_code == 0
    assert target.read_text(encoding="utf-8") == full_guide()
    assert runner.invoke(app, ["agent-guide", "--write", str(target)]).exit_code == 1


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
    contract.write_text(yaml.safe_dump({
        "version": "1.0.0", "info": {"title": "orders"},
        "source": {"type": "landing", "path": str(csv), "format": "csv"},
        "model": {"fields": [{"name": "order_id", "type": "integer"}, {"name": "amount", "type": "integer"}]},
    }), encoding="utf-8")
    out = CliRunner().invoke(app, ["run", "--contract", str(contract)])
    assert out.exit_code == 0, out.output


def test_a_failing_dataset_rule_is_one_readable_line(tmp_path):
    import yaml

    csv = tmp_path / "orders.csv"
    csv.write_text("order_id,amount\n1,10\n1,20\n", encoding="utf-8")
    contract = tmp_path / "orders.olc.yaml"
    contract.write_text(yaml.safe_dump({
        "version": "1.0.0", "info": {"title": "orders"},
        "model": {"fields": [{"name": "order_id", "type": "integer"}, {"name": "amount", "type": "integer"}]},
        "quality": {"fail_pipeline_on_dataset_error": True, "dataset_rules": [{"unique": "order_id"}]},
    }), encoding="utf-8")
    out = CliRunner().invoke(app, ["run", "-c", str(contract), "--source", str(csv)])
    assert out.exit_code == 1
    assert "Traceback" not in out.output


def test_unique_failures_are_described_in_words():
    from lakelogic.core.processor import _describe_dataset_failure

    assert _describe_dataset_failure({"name": "order_id_unique", "value": "1 (expected < 1.0)"}) == \
        "order_id_unique: 1 duplicate value(s)"
    assert _describe_dataset_failure({"name": "min_rows", "value": "0 (expected > 1)"}) == \
        "min_rows: got 0, expected > 1"
