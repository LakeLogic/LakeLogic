"""A storage-relative landing path must enumerate, whatever the process CWD is.

THE LIVE FAILURE. On Microsoft Fabric one location has two addresses: Spark reads
`Files/landing_.../cities` and CANNOT read `/lakehouse/default/Files/landing_.../cities`
(`400 Bad Request`); Python's filesystem sees only the second. Measured in the runtime:

    cwd                     /mnt/var/hadoop/tmp/.../container_.../
    glob("Files/...")       0 files          glob("/lakehouse/default/Files/...")   10 files
    spark.read("Files/...") 200 rows         spark.read("/lakehouse/default/...")   400 error

`_expand_source_files` globbed the engine-facing path directly, so it resolved against the
container's scratch CWD, matched nothing, and a partitioned bronze source reported
`no_new_data` over a directory holding ten files — for hours, with no error anywhere.
"""
from pathlib import Path

import pytest

from lakelogic.core.processor import DataProcessor


def _estate(tmp_path: Path):
    """A contract loaded through a mount, whose source path is storage-relative."""
    mount = tmp_path / "mount"
    landing = mount / "Files" / "landing_internal" / "internal" / "cities" / "snapshot_dt=2026-09-07"
    landing.mkdir(parents=True)
    for i in range(3):
        (landing / f"batch_0{i}_abc.csv").write_text("city_code,city_name\nLDN,London\n", encoding="utf-8")

    contract_path = mount / "Files" / "_contracts" / "internal" / "internal" / "c.yaml"
    contract_path.parent.mkdir(parents=True)
    contract_path.write_text(
        'version: 1.0.0\n'
        "info:\n  title: cities\n  table_name: bronze_internal_cities\n"
        "model:\n  fields:\n    - name: city_code\n      type: string\n"
        "source:\n  type: landing\n  path: Files/landing_internal/internal/cities\n  format: csv\n",
        encoding="utf-8",
    )
    return mount, contract_path


def test_a_storage_relative_pattern_enumerates_from_any_cwd(tmp_path, monkeypatch):
    """The bug: enumeration resolved against the CWD, which is never the data root."""
    mount, contract_path = _estate(tmp_path)
    # A CWD that is NOT the mount — exactly what a Fabric/YARN container gives you.
    elsewhere = tmp_path / "container_scratch"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)

    proc = DataProcessor(str(contract_path), engine="polars")
    found = proc._expand_source_files("Files/landing_internal/internal/cities/**/*.csv")

    assert found, "enumeration found nothing — the relative path resolved against the CWD"
    assert len(found) == 3


def test_the_paths_returned_stay_in_the_engines_address_space(tmp_path, monkeypatch):
    """Translating one way only moves the failure downstream: these paths are READ next, and
    the engine cannot open the mounted form."""
    mount, contract_path = _estate(tmp_path)
    elsewhere = tmp_path / "container_scratch"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)

    proc = DataProcessor(str(contract_path), engine="polars")
    found = proc._expand_source_files("Files/landing_internal/internal/cities/**/*.csv")

    for entry in found:
        assert entry["path"].startswith("Files/landing_internal/"), entry["path"]
        assert str(mount) not in entry["path"], "a mount-local path would fail the engine read"


def test_an_absolute_path_is_untouched(tmp_path, monkeypatch):
    """No translation where the two views already agree — every platform but this case."""
    mount, contract_path = _estate(tmp_path)
    monkeypatch.chdir(tmp_path)
    proc = DataProcessor(str(contract_path), engine="polars")

    pattern = str(mount / "Files/landing_internal/internal/cities/**/*.csv").replace("\\", "/")
    found = proc._expand_source_files(pattern)
    assert found and len(found) == 3
    for entry in found:
        assert Path(entry["path"]).is_absolute()


def test_a_genuinely_missing_path_still_reports_nothing(tmp_path, monkeypatch):
    """The translation must not invent data: absent is still absent."""
    mount, contract_path = _estate(tmp_path)
    monkeypatch.chdir(tmp_path)
    proc = DataProcessor(str(contract_path), engine="polars")
    assert proc._expand_source_files("Files/landing_internal/internal/nope/**/*.csv") is None
