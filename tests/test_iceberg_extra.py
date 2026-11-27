"""The `iceberg` extra (2026-11-27): a clean Polars install could not write Iceberg — pyiceberg was a
test-only dependency and merge / SCD2 also need pandas. Found proving Iceberg on a REST catalog."""
from __future__ import annotations

import builtins
import tomllib
from pathlib import Path

import pytest

from lakelogic.core import materialization as m


def test_the_iceberg_extra_installs_everything_a_catalog_write_needs():
    extras = tomllib.loads((Path(__file__).resolve().parents[1] / "pyproject.toml").read_text())["project"]["optional-dependencies"]
    names = {d.split("[")[0].split(">")[0].split("<")[0].strip() for d in extras["iceberg"]}
    assert {"pyiceberg", "pandas", "pyarrow"} <= names


def _without(monkeypatch, module):
    real = builtins.__import__

    def fake(name, *a, **k):
        if name == module or name.startswith(module + "."):
            raise ImportError(module)
        return real(name, *a, **k)
    monkeypatch.setattr(builtins, "__import__", fake)


def test_a_missing_pyiceberg_names_the_extra(monkeypatch):
    _without(monkeypatch, "pyiceberg")
    with pytest.raises(ValueError, match=r"lakelogic\[iceberg\]"):
        m._write_iceberg_via_catalog(None, "rest.ns.t", object(), object(), strategy="append")


def test_a_merge_without_pandas_says_so_before_writing(monkeypatch):
    pytest.importorskip("pyiceberg")
    _without(monkeypatch, "pandas")
    with pytest.raises(ValueError, match="needs pandas"):
        m._write_iceberg_via_catalog(None, "rest.ns.t", object(), object(), strategy="merge")
