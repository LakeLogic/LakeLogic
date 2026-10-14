"""docs/contracts/lint_rules.md is generated from the catalogue; it must never drift from it."""

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_the_lint_rules_page_matches_the_catalogue():
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "generate_lint_catalogue.py"), "--check"],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr
