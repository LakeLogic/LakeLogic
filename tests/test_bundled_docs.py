"""The wheel ships a fixed set of reference docs at lakelogic/_bundled/docs/.

An agent reads them from the installed package, at the same relative path they
have under docs/. The list lives in pyproject.toml (hatch force-include); these
tests pin it against docs/ so a new pattern page is not silently left out, a
draft under docs/specs/ is never shipped, and the root docs/ingestion.md
duplicate is not bundled beside the data_product_contracts one.
"""

import sys
from pathlib import Path

import pytest

if sys.version_info < (3, 11):  # pragma: no cover
    pytest.skip("tomllib needs Python 3.11+", allow_module_level=True)
import tomllib

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def _force_include() -> dict:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return data["tool"]["hatch"]["build"]["targets"]["wheel"]["force-include"]


def _expected() -> set:
    pages = set()
    for folder in ("patterns", "contracts/data_product_contracts"):
        pages |= {p.relative_to(ROOT).as_posix() for p in (DOCS / folder).glob("*.md")}
    for name in ("schema_model", "system_config", "slo", "inheritance", "versioning"):
        pages.add(f"docs/contracts/{name}.md")
    for name in ("streaming_sources", "reprocessing", "reconciliation", "delta_lake_support"):
        pages.add(f"docs/{name}.md")
    return pages


def test_bundle_list_matches_the_docs_on_disk():
    assert set(_force_include()) == _expected()


def test_every_bundled_page_exists():
    missing = [src for src in _force_include() if not (ROOT / src).is_file()]
    assert missing == []


def test_each_page_keeps_its_relative_path_under_bundled_docs():
    for src, dst in _force_include().items():
        assert dst == "lakelogic/_bundled/" + src


def test_no_drafts_and_no_duplicate_ingestion_page():
    bundled = set(_force_include())
    assert not any(src.startswith("docs/specs/") for src in bundled)
    assert "docs/ingestion.md" not in bundled
    assert "docs/contracts/data_product_contracts/ingestion.md" in bundled
