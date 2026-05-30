"""
test_base_path_separation
=========================

Locks in the architectural rule:

  Storage paths   (materialization, quarantine, run_log, link, source-glob)
  → resolved by the registry from {storage_root}/{bronze_path}/etc placeholders
  → NEVER anchored on `_base_path` (the contract YAML's directory)

  Contract-local files (external_logic.path, future schema includes, fixtures)
  → resolved relative to `_base_path` so the contract is portable as a package

Background
----------
Previously the OSS code inconsistently prefixed storage paths with `_base_path`
when it was set. This caused subtle path-resolution bugs whenever a contract
loaded via `DomainRegistry.from_yaml(...)` (which used to leave `_base_path`
unset) was mixed with one loaded via the CLI driver (which always set it):
the same contract would land its delta tables in different places. The fix
was to:

  1. Always set `_base_path` on contracts loaded from a known path.
  2. Strip `_base_path` from every storage-resolution site (materialization,
     quarantine, run_log, link, source-glob).
  3. Leave it consulted only where it belongs — `external_logic` and friends.

These tests pin the new behaviour in place so the separation can't silently
regress. If one of these fails, someone has either:
  - Re-introduced `_base_path` into a storage-path resolution site, or
  - Stopped setting `_base_path` for contracts loaded from a known file.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

import pytest

from lakelogic.core import external_logic as ext
from lakelogic.core import materialization as mat
from lakelogic.core import quarantine as q
from lakelogic.core import processor as proc
from lakelogic.core import run_log as rl


# ─────────────────────────────────────────────────────────────────────────────
# 1) Storage targets must NOT be anchored on _base_path
# ─────────────────────────────────────────────────────────────────────────────


def test_materialization_target_ignores_base_path(tmp_path):
    """A CWD-relative storage target stays CWD-relative even when
    `_base_path` is set. Without this rule, registry-loaded contracts would
    land their delta tables under `<contract YAML dir>/lakehouse_polars/...`
    — nonsense for any test_data_driver style notebook."""
    contract = types.SimpleNamespace(
        materialization=types.SimpleNamespace(target_path="out/orders", path=None, format="csv"),
        _base_path=tmp_path,  # set, but must NOT prefix the storage path
        effective_server=lambda: None,
    )

    target, _ = mat._resolve_target(contract)

    assert target == Path("out/orders")
    assert str(target) != str(tmp_path / "out" / "orders")


def test_materialization_target_keeps_absolute_paths_intact(tmp_path):
    """An absolute storage target stays absolute regardless of `_base_path`."""
    absolute = tmp_path / "explicit" / "table"
    contract = types.SimpleNamespace(
        materialization=types.SimpleNamespace(target_path=str(absolute), path=None, format="parquet"),
        _base_path=tmp_path / "elsewhere",  # deliberately different
        effective_server=lambda: None,
    )

    target, _ = mat._resolve_target(contract)
    assert target == absolute


def test_default_quarantine_db_uses_cwd_when_caller_passes_none(monkeypatch, tmp_path):
    """`_default_quarantine_db(None, ...)` should land the cache under CWD,
    not under whatever `_base_path` happened to be. The writers in
    quarantine.py now always pass None so storage paths never silently
    reach into contract-local territory."""
    monkeypatch.chdir(tmp_path)
    out = q._default_quarantine_db(None, "duckdb")
    assert out.parent == tmp_path / ".lakelogic"
    assert out.name == "quarantine.duckdb"


def test_processor_source_glob_ignores_base_path(monkeypatch, tmp_path):
    """`_expand_source_files` is a STORAGE path resolver — it must NOT prefix
    glob patterns with `_base_path`. Source paths come from
    `{landing_root}` etc., resolved by the registry at load time."""
    # Layout:
    #   tmp_path/landing/a.csv
    #   tmp_path/landing/b.csv
    #   tmp_path/elsewhere/x.csv  ← would be picked if _base_path leaked
    (tmp_path / "landing").mkdir()
    (tmp_path / "landing" / "a.csv").write_text("a", encoding="utf-8")
    (tmp_path / "landing" / "b.csv").write_text("b", encoding="utf-8")
    (tmp_path / "elsewhere").mkdir()
    (tmp_path / "elsewhere" / "x.csv").write_text("x", encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    processor = object.__new__(proc.DataProcessor)
    # _base_path points at the WRONG directory deliberately — if the resolver
    # respected it, we'd see x.csv too and the count would be 3.
    processor.contract = types.SimpleNamespace(
        _base_path=tmp_path / "elsewhere",
        source=types.SimpleNamespace(type="landing"),
    )
    processor._is_uri_path = lambda path: False

    results = processor._expand_source_files("landing/*.csv")
    assert results is not None
    assert len(results) == 2
    names = sorted(Path(r["path"]).name for r in results)
    assert names == ["a.csv", "b.csv"]


# ─────────────────────────────────────────────────────────────────────────────
# 2) Contract-local files (external_logic) DO honour _base_path
# ─────────────────────────────────────────────────────────────────────────────


def test_external_logic_path_anchors_on_base_path(monkeypatch, tmp_path):
    """`external_logic.path` is a packaging artefact of the contract; it
    travels with the YAML and must resolve relative to `_base_path`. This is
    the OTHER side of the separation — proving it still works after the
    storage-path strip."""
    script = tmp_path / "_generators" / "do_thing.py"
    script.parent.mkdir(parents=True)
    script.write_text("def run(df, **kw): return df\n", encoding="utf-8")

    captured = {}

    def fake_run_python(path, *args):
        captured["path"] = path
        return ("ran", True)

    monkeypatch.setattr(ext, "_run_python_logic", fake_run_python)

    logic = types.SimpleNamespace(
        type="python",
        path="_generators/do_thing.py",  # relative to _base_path
        entrypoint="run",
        args={},
    )
    contract = types.SimpleNamespace(external_logic=logic, _base_path=tmp_path)

    result = ext.apply_external_logic(contract, "df", "engine", "run-1", None)
    assert result == ("ran", True)
    assert captured["path"] == tmp_path / "_generators" / "do_thing.py"


def test_external_logic_absolute_path_passes_through(monkeypatch, tmp_path):
    """An absolute `external_logic.path` is not re-anchored — same rule the
    storage paths follow. Absolute means absolute."""
    script = tmp_path / "absolute_logic.py"
    script.write_text("def run(df, **kw): return df\n", encoding="utf-8")

    captured = {}

    def fake_run_python(path, *args):
        captured["path"] = path
        return ("ran", True)

    monkeypatch.setattr(ext, "_run_python_logic", fake_run_python)

    logic = types.SimpleNamespace(
        type="python",
        path=str(script),  # absolute
        entrypoint="run",
        args={},
    )
    contract = types.SimpleNamespace(external_logic=logic, _base_path=tmp_path / "elsewhere")

    ext.apply_external_logic(contract, "df", "engine", "run-1", None)
    assert captured["path"] == Path(str(script))


# ─────────────────────────────────────────────────────────────────────────────
# 3) Contract loading anchors _base_path consistently
# ─────────────────────────────────────────────────────────────────────────────


def test_processor_load_contract_sets_base_path_from_dict_file_marker(tmp_path):
    """When a contract dict carries the `__file__` marker injected by
    `DomainRegistry.from_yaml`, `processor._load_contract` must set
    `_base_path` to the YAML's directory. Without this the registry path
    diverges from the CLI path — external_logic etc. then break in subtle
    cwd-dependent ways."""
    yaml_path = tmp_path / "domains" / "x" / "contracts" / "bronze" / "thing.yaml"
    yaml_path.parent.mkdir(parents=True)
    yaml_path.write_text("placeholder", encoding="utf-8")

    contract_dict = {
        "version": "1.0.0",
        "info": {"title": "Thing", "domain": "x", "system": "y"},
        "dataset": "thing",
        "__file__": str(yaml_path),
    }

    # Construct just enough of a DataProcessor to call _load_contract — we
    # avoid the full __init__ so we don't need to satisfy every engine
    # prerequisite. Attributes consulted by the downstream `_apply_*` hooks
    # are stubbed to no-op shapes.
    processor = object.__new__(proc.DataProcessor)
    processor.engine_name = "polars"
    processor.stage = None
    processor._apply_stage_overrides = lambda c: c
    processor._apply_fact_governance = lambda c: c
    processor._apply_cdc_defaults = lambda c: c
    loaded = proc.DataProcessor._load_contract(processor, contract_dict)

    assert getattr(loaded, "_base_path", None) == yaml_path.parent


def test_processor_load_contract_handles_missing_file_marker_gracefully(tmp_path):
    """Loading from a bare dict (no `__file__`) is also legal — used by
    in-memory tests and ad-hoc materialisation. `_base_path` is simply left
    unset; no exception."""
    contract_dict = {
        "version": "1.0.0",
        "info": {"title": "Thing", "domain": "x", "system": "y"},
        "dataset": "thing",
        # no __file__
    }
    processor = object.__new__(proc.DataProcessor)
    processor.engine_name = "polars"
    processor.stage = None
    processor._apply_stage_overrides = lambda c: c
    processor._apply_fact_governance = lambda c: c
    processor._apply_cdc_defaults = lambda c: c
    loaded = proc.DataProcessor._load_contract(processor, contract_dict)
    # Either unset or None — both mean "no contract-local anchor available".
    assert getattr(loaded, "_base_path", None) is None
