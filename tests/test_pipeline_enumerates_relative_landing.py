"""The mount translation must work THROUGH THE PIPELINE, which passes contracts as dicts.

This is the test the first attempt at this fix did not have. That fix derived the local mount
from `contract._contract_path`, which is set only when a contract is loaded from a YAML file —
and `LakehousePipeline` never does that:

    processor = DataProcessor(contract=c.contract_dict, ...)      # runner.py

So on every real run the attribute was None, the translation silently no-opped, and a Fabric
deploy reproduced the original failure exactly. The unit test passed because it built the
processor from a path — the one construction shape production never uses.
"""
from pathlib import Path

import pytest

from lakelogic.core.processor import DataProcessor


def _estate(tmp_path: Path):
    mount = tmp_path / "mount"
    landing = mount / "Files" / "landing_d" / "s" / "cities" / "snapshot_dt=2026-09-07"
    landing.mkdir(parents=True)
    for i in range(3):
        (landing / f"batch_0{i}_a.csv").write_text("city_code\nLDN\n", encoding="utf-8")
    contract_path = mount / "Files" / "_contracts" / "d" / "s" / "contracts" / "bronze" / "c.yaml"
    contract_path.parent.mkdir(parents=True)
    contract_path.write_text(
        "version: 1.0.0\n"
        "info:\n  title: cities\n  table_name: bronze_cities\n"
        "model:\n  fields:\n    - name: city_code\n      type: string\n"
        "source:\n  type: landing\n  path: Files/landing_d/s/cities\n  format: csv\n",
        encoding="utf-8",
    )
    return mount, contract_path


PATTERN = "Files/landing_d/s/cities/**/*.csv"


def test_a_dict_contract_still_enumerates_when_given_the_path(tmp_path, monkeypatch):
    """How the pipeline builds it: contract as a DICT, location supplied separately."""
    import yaml

    mount, contract_path = _estate(tmp_path)
    monkeypatch.chdir(tmp_path / "elsewhere" if (tmp_path / "elsewhere").exists() else tmp_path)
    doc = yaml.safe_load(contract_path.read_text(encoding="utf-8"))

    proc = DataProcessor(contract=doc, engine="polars", contract_path=str(contract_path))
    found = proc._expand_source_files(PATTERN)

    assert found, "a dict-constructed processor could not enumerate — the pipeline's shape"
    assert len(found) == 3
    for entry in found:
        assert entry["path"].startswith("Files/landing_d/"), entry["path"]


def test_a_dict_contract_with_no_path_cannot_translate(tmp_path, monkeypatch):
    """The honest limit, stated so it is not mistaken for the bug above: with neither an
    explicit path nor one on the contract, there is nothing to anchor against."""
    import yaml

    mount, contract_path = _estate(tmp_path)
    monkeypatch.chdir(tmp_path)
    doc = yaml.safe_load(contract_path.read_text(encoding="utf-8"))

    proc = DataProcessor(contract=doc, engine="polars")
    assert proc._local_mount_prefix() is None


def test_the_runner_supplies_the_path_it_resolved(tmp_path):
    """The wiring itself: if the runner stops passing `contract_path`, the fix is inert again
    and nothing else would notice."""
    import inspect

    from lakelogic.pipeline import runner

    src = inspect.getsource(runner)
    # Injected by attribute, beside `_ownership` / `_notifications` — a constructor argument
    # would break every caller that substitutes its own processor, for a value only the read
    # path uses.
    assert "_explicit_contract_path" in src, "the runner no longer supplies the path"
    # And specifically on the READ path, which is the one that enumerates source files.
    read_site = src[src.index("run_log_mode=resolved_mode"):][:700]
    assert "_explicit_contract_path" in read_site
