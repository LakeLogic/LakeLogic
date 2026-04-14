import pytest
typer = pytest.importorskip("typer")
from typer.testing import CliRunner
from lakelogic.cli.main import app
import os

runner = CliRunner()

def test_cli_help():
    """Test that the CLI help command works."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "Usage" in result.output

def test_cli_run_missing_args():
    """Test that CLI fails gracefully with missing arguments."""
    result = runner.invoke(app, ["run"])
    assert result.exit_code != 0
    # CliRunner mixes stderr into output; no need to concatenate
    assert "Missing option" in result.output or "Error" in result.output
