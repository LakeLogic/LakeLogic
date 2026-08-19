"""The checked-in registry JSON Schemas must match the models (no drift).

If this fails, regenerate: ``python scripts/generate_registry_schema.py``.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
_SCHEMA_DIR = _ROOT / "schemas" / "registry"


def _gen():
    spec = importlib.util.spec_from_file_location(
        "_gen_registry_schema", _ROOT / "scripts" / "generate_registry_schema.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.mark.parametrize("filename", ["domain-manifest-v1.schema.json", "system-manifest-v1.schema.json"])
def test_checked_in_schema_matches_model(filename):
    gen = _gen()
    model = gen._TARGETS[filename]
    expected = gen.render(model)
    actual = (_SCHEMA_DIR / filename).read_text(encoding="utf-8")
    assert actual == expected, f"{filename} is stale — run `python scripts/generate_registry_schema.py`"
