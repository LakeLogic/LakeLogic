import pytest
from typer.testing import CliRunner
from lakeguard.cli.main import app
import os

runner = CliRunner()

def test_cli_help():
    """Test that the CLI help command works."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "LakeGuard" in result.stdout

def test_cli_run_missing_args():
    """Test that CLI fails gracefully with missing arguments."""
    result = runner.invoke(app, ["run"])
    assert result.exit_code != 0
    assert "Missing option" in result.stdout
