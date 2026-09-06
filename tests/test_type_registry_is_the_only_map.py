"""No module may keep its own contract-type dictionary.

``lakelogic/core/types.py`` exists because nine hand-written type maps disagreed
about what `float` and `integer` mean, and the disagreement was invisible from
either side. Consolidating them fixed the instance. THIS test fixes the pattern:
it fails if anyone adds the tenth map.

Without it, the registry is merely tidier code. A future engine — or a hurried
fix that "just needs a small mapping here" — reintroduces the same class of bug,
and nothing notices until a pipeline write is rejected on a cluster.

The scan is deliberately shape-based, not name-based: it looks for a dict literal
whose keys are contract type names, wherever it appears and whatever it is
called. Renaming the variable does not evade it.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

from lakelogic.core import types as registry

ROOT = pathlib.Path(__file__).resolve().parents[1] / "lakelogic"

# The registry itself is where the maps are SUPPOSED to live.
EXEMPT_FILES = {"core/types.py"}

# Maps that are NOT contract-type -> physical-type, and so cannot drift the way
# the registry exists to prevent. Every entry carries its reason: an unexplained
# exemption is how a guard quietly stops guarding. Keyed by "path:symbol-ish".
ALLOWED = {
    # Different DIRECTION: physical/foreign type -> contract type. These read a
    # type someone else declared; they do not decide what a column is created as.
    "core/bootstrap.py": "Spark DDL -> contract type (inverse of the registry)",
    "core/models.py": "ODCS logicalType -> contract type (a foreign vocabulary)",
    "core/quarantine.py": "pandas dtype -> DuckDB type for ALTER TABLE",
    # Different CONCERN: logical -> logical, or SQL text rewriting.
    "core/ddl.py": "_SAFE_WIDENINGS: which promotions are lossless, not renderings",
    "engines/duckdb.py": "Spark SQL keyword -> ANSI, inside CAST() text",
    "engines/polars.py": "Spark SQL keyword -> ANSI, inside CAST() text",
    "engines/bigquery.py": "SQL text normalisation of already-rendered types",
    # Different TARGET: dbt and CLI vocabularies, not lakehouse columns.
    "adapters/dbt.py": "dbt column-type vocabulary",
    "scaffold/dbt_project.py": "dbt scaffold vocabulary",
    "cli/main.py": "display formatting for `lakelogic describe`",
}

# A dict literal counts as a type map if this many of its keys are contract type
# names. Three avoids flagging incidental dicts that happen to have a "date" or
# "string" key, while still catching any real mapping (the smallest one found in
# the original code had eleven).
MIN_TYPE_KEYS = 3

KNOWN = registry.known_types()


def _python_files():
    for path in sorted(ROOT.rglob("*.py")):
        rel = path.relative_to(ROOT).as_posix()
        if rel in EXEMPT_FILES or "__pycache__" in rel:
            continue
        yield rel, path


def _type_map_literals(tree: ast.AST):
    """Every dict literal in the tree whose keys look like contract type names."""
    found = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Dict):
            continue
        keys = [k.value for k in node.keys if isinstance(k, ast.Constant) and isinstance(k.value, str)]
        if not keys:
            continue
        hits = [k for k in keys if k.lower() in KNOWN]
        # Require that the type-ish keys DOMINATE the dict, so a config dict with
        # a "string" option is not mistaken for a type map.
        if len(hits) >= MIN_TYPE_KEYS and len(hits) >= len(keys) * 0.6:
            found.append((node.lineno, sorted(hits)[:6]))
    return found


def test_no_module_declares_its_own_contract_type_map():
    offenders = []
    for rel, path in _python_files():
        if rel in ALLOWED:
            continue
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for lineno, keys in _type_map_literals(tree):
            offenders.append(f"{rel}:{lineno} maps {keys}")

    assert not offenders, (
        "These modules declare their own contract-type dictionary. Use "
        "lakelogic.core.types instead — a second map is how `float` came to mean "
        "32-bit in the DDL and 64-bit in the engines, which broke every write to "
        "a float column.\n  " + "\n  ".join(offenders)
    )


def test_the_registry_is_actually_reachable_from_the_engines():
    """A guard that only forbids is useless if nothing uses the replacement."""
    importers = [
        rel
        for rel, path in _python_files()
        if "types as _types" in path.read_text(encoding="utf-8")
        or "from .types import" in path.read_text(encoding="utf-8")
    ]
    for expected in (
        "core/ddl.py",
        "engines/spark.py",
        "engines/duckdb.py",
        "engines/polars.py",
        "engines/generic_sql.py",
    ):
        assert expected in importers, f"{expected} no longer imports the type registry"


def test_import_time_validation_rejects_a_disagreeing_cast():
    """The other guard: a cast override that changes the stored type must fail
    at import, not on a cluster. Proven by constructing one."""
    bad = registry.TypeSpec(
        logical="broken",
        kind=registry.INT,
        bits=32,
        ddl={d: "INT" for d in registry.DIALECTS},
        arrow="int32",
        polars="Int32",
        cast_overrides={"spark": "STRING"},
    )
    registry._REGISTRY["broken"] = bad
    try:
        with pytest.raises(ValueError, match="different kinds of value"):
            registry.validate_registry()
    finally:
        del registry._REGISTRY["broken"]
    registry.validate_registry()  # clean again


def test_an_unknown_type_raises_instead_of_silently_becoming_a_string():
    """The quieter half of the original bug: `.get(type, "VARCHAR")` turned an
    unmapped type into a text column, so the contract's declared type was
    ignored without a word."""
    with pytest.raises(registry.UnknownContractType):
        registry.cast_type("definitely_not_a_type", "spark")


def test_every_allowance_is_still_needed():
    """An allowlist that outlives its entries becomes a place to hide things.

    If a file on the list no longer contains a type-shaped dict, the entry must
    go — otherwise it silently exempts whatever gets added there next.
    """
    stale = []
    for rel, path in _python_files():
        if rel not in ALLOWED:
            continue
        if not _type_map_literals(ast.parse(path.read_text(encoding="utf-8"))):
            stale.append(rel)
    assert not stale, f"remove these from ALLOWED, they no longer have a type map: {stale}"
