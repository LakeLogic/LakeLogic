"""An extra must ship the library that does the work, not only the driver.

`source.type: database` contracts run through `DataProcessor._run_database_source`, whose
batched path builds an SQLAlchemy engine and hands polars a live connection. SQLAlchemy
was declared in **no extra at all** — so `pip install lakelogic[databases]` gave you
pyodbc, psycopg2, pymysql and pymongo, every driver and not the library that performs the
extraction. Every database source raised:

    ImportError: Batching requires SQLAlchemy.

It stayed invisible because developer machines have SQLAlchemy pulled in by something
else. The notebook CI runner does not, which is where it finally surfaced — one published
example failing on a clean Linux box while passing everywhere it was written.

The guard is a packaging assertion, because that is where the defect lives: no amount of
unit testing `_run_database_source` catches an undeclared dependency.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

if sys.version_info >= (3, 11):
    import tomllib
else:  # pragma: no cover - the project targets 3.11+
    tomllib = pytest.importorskip("tomli")

PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"

#: Extras that promise "you can now read from this database". Each must be sufficient on
#: its own. MongoDB is absent on purpose: it is not an SQLAlchemy dialect and does not
#: use this code path.
SQL_BACKED_EXTRAS = ["sql", "databases", "azuresql", "postgresql", "mysql"]


@pytest.fixture(scope="module")
def extras() -> dict[str, list[str]]:
    data = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    return data["project"]["optional-dependencies"]


def _names(requirements: list[str]) -> set[str]:
    """Bare distribution names, lowercased — `sqlalchemy>=2.0.0` -> `sqlalchemy`."""
    out = set()
    for req in requirements:
        name = req.split(";")[0].strip()
        for sep in ("[", ">", "<", "=", "!", "~", " "):
            name = name.split(sep)[0]
        out.add(name.strip().lower())
    return out


@pytest.mark.parametrize("extra", SQL_BACKED_EXTRAS)
def test_sql_backed_extra_declares_sqlalchemy(extras, extra):
    assert extra in extras, f"extra '{extra}' is gone; the database source still needs one"
    assert "sqlalchemy" in _names(extras[extra]), (
        f"extra '{extra}' enables a source.type: database contract but does not declare "
        "SQLAlchemy, so the batched extraction path raises ImportError at runtime"
    )


def test_the_lean_sql_extra_stays_lean(extras):
    """`sql` exists so SQLite users are not made to build pyodbc.

    If it grows drivers it stops being installable on a runner without unixODBC headers,
    and the notebook CI job that installs it goes back to failing — for a different
    reason, which is the harder kind to diagnose.
    """
    assert _names(extras["sql"]) == {"sqlalchemy"}


def test_mongodb_is_not_claimed_as_sql_backed(extras):
    """Over-correction guard: MongoDB has no SQLAlchemy dialect. Adding SQLAlchemy there
    would be a dependency that its code path never imports."""
    assert "sqlalchemy" not in _names(extras["mongodb"])
