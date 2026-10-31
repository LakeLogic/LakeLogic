"""Files a Bronze load READ but could not clean up: remembered, retried first, never read twice.

The archive folder is the contract's TABLE name (what the end user sees), not the registry entity.
"""

from __future__ import annotations

import types
from pathlib import Path

import pytest

from lakelogic.core.processor import DataProcessor
from lakelogic.pipeline import landing_lifecycle as ll
from lakelogic.pipeline.runner import LakehousePipeline


def _write(path: Path, text: str = "id,v\n1,x\n") -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return str(path)


def _pipe(system="shop", storage=None):
    p = LakehousePipeline.__new__(LakehousePipeline)
    p.spark = None
    p.registry = types.SimpleNamespace(storage=storage, system=system, domain="sales")
    return p


def _rc(entity="orders", layer="bronze", **contract):
    return types.SimpleNamespace(entity=entity, layer=layer, contract_dict=contract)


def _block(folder: Path) -> Path:
    """A FILE where the success folder must go, so every move into it fails."""
    folder.mkdir(parents=True, exist_ok=True)
    blocker = folder / "success"
    blocker.write_text("a file where a folder must go", encoding="utf-8")
    return blocker


# ── the folder is the table name ───────────────────────────────────────────


@pytest.mark.parametrize(
    "contract, expected",
    [
        ({"info": {"table_name": "{bronze_layer}_{system}_orders"}}, "bronze_shop_orders"),
        ({"info": {"table_name": "bronze_orders"}}, "bronze_orders"),
        ({"materialization": {"target_path": "table:`cat`.sales.bronze_orders"}}, "bronze_orders"),
        ({"materialization": {"location": "abfss://c@a.dfs.core.windows.net/bronze/orders_tbl"}}, "orders_tbl"),
        ({}, "orders"),  # no table named: the entity
        ({"info": {"table_name": "{unknown_token}_orders"}}, "orders"),
    ],
)
def test_the_table_name_is_what_the_contract_declares(contract, expected):
    assert _pipe()._landing_table_name(_rc(**contract)) == expected


def test_files_are_archived_under_the_table_name(tmp_path):
    f = _write(tmp_path / "landing" / "a.csv")
    rc = _rc(
        info={"table_name": "{bronze_layer}_{system}_orders"},
        source={"path": str(tmp_path / "landing")},
        server={"post_ingestion": {"action": "archive", "archive_path": str(tmp_path / "arch")}},
    )
    _pipe()._execute_post_ingestion_cleanup(rc, types.SimpleNamespace(_source_files=[{"path": f}]))
    assert (tmp_path / "arch" / "bronze_shop_orders" / "success" / "a.csv").exists()


# ── the manifest ───────────────────────────────────────────────────────────


def test_manifest_sits_beside_the_archive_or_falls_back_to_the_log_folder(tmp_path):
    arch = ll.LandingPolicy(action="archive", archive_root=str(tmp_path / "arch"))
    assert ll.manifest_path(arch, "bronze_orders") == str(tmp_path / "arch" / "bronze_orders" / ll.ORPHANS_FILE)
    delete = ll.LandingPolicy(action="delete")
    assert ll.manifest_path(delete, "bronze_orders") is None
    assert ll.manifest_path(delete, "bronze_orders", str(tmp_path / "logs")) == str(
        tmp_path / "logs" / "bronze_orders" / ll.ORPHANS_FILE
    )


def test_record_keeps_a_union_and_retry_returns_what_is_still_stuck(tmp_path):
    m = str(tmp_path / "m" / ll.ORPHANS_FILE)
    gone = str(tmp_path / "landing" / "gone.csv")
    stuck = _write(tmp_path / "landing" / "stuck.csv")
    ll.record_orphans(m, [gone])
    ll.record_orphans(m, [stuck, gone])
    assert sorted(ll.read_orphans(m)) == sorted([gone, stuck])
    # the archive root is a FILE, so the move fails: the stuck one stays remembered, the missing one is done
    blocker = tmp_path / "blocker"
    blocker.write_text("x", encoding="utf-8")
    pol = ll.LandingPolicy(action="archive", archive_root=str(blocker))
    assert ll.retry_orphans(m, pol, entity="t", landing_path=None) == [stuck]
    assert ll.read_orphans(m) == [stuck]


def test_retry_with_retain_holds_nothing_back(tmp_path):
    m = str(tmp_path / ll.ORPHANS_FILE)
    ll.record_orphans(m, [str(tmp_path / "a.csv")])
    assert ll.retry_orphans(m, ll.LandingPolicy(action="retain"), entity="t", landing_path=None) == []


def test_an_unreadable_manifest_raises_rather_than_guessing_empty(tmp_path):
    m = tmp_path / ll.ORPHANS_FILE
    m.write_text("not json", encoding="utf-8")
    with pytest.raises(ValueError):
        ll.read_orphans(str(m))


def test_a_failed_cleanup_is_remembered_and_retried_next_run(tmp_path):
    f = _write(tmp_path / "landing" / "a.csv")
    arch = tmp_path / "arch"
    blocker = _block(arch / "orders")
    rc = _rc(
        source={"path": str(tmp_path / "landing")},
        server={"post_ingestion": {"action": "archive", "archive_path": str(arch)}},
    )
    pipe = _pipe()
    pipe._execute_post_ingestion_cleanup(rc, types.SimpleNamespace(_source_files=[{"path": f}]))
    manifest = arch / "orders" / ll.ORPHANS_FILE
    assert ll.read_orphans(str(manifest)) == [f]
    assert pipe._retry_orphaned_landing_files(rc) == {f}  # still blocked: held back
    blocker.unlink()
    assert pipe._retry_orphaned_landing_files(rc) == set()  # retried and archived
    assert (arch / "orders" / "success" / "a.csv").exists() and ll.read_orphans(str(manifest)) == []


def test_a_blocking_cleanup_failure_still_remembers_the_files(tmp_path):
    f = _write(tmp_path / "landing" / "a.csv")
    arch = tmp_path / "arch"
    _block(arch / "orders")
    rc = _rc(
        source={"path": str(tmp_path / "landing")},
        server={"post_ingestion": {"action": "archive", "archive_path": str(arch), "cleanup_is_blocking": True}},
    )
    with pytest.raises(RuntimeError):
        _pipe()._execute_post_ingestion_cleanup(rc, types.SimpleNamespace(_source_files=[{"path": f}]))
    assert ll.read_orphans(str(arch / "orders" / ll.ORPHANS_FILE)) == [f]


# ── end to end on Polars: no file is loaded twice ──────────────────────────


def test_a_file_whose_cleanup_failed_is_not_loaded_again_and_is_cleaned_up_later(tmp_path):
    land = tmp_path / "landing"
    arch = tmp_path / "arch"
    blocker = _block(arch / "orders")
    contract = {
        "version": "1.0.0",
        "info": {"title": "bronze orders", "target_layer": "bronze", "table_name": "orders"},
        "source": {"type": "landing", "path": str(land), "format": "csv"},
        "model": {"fields": [{"name": "id", "type": "string"}, {"name": "v", "type": "string"}]},
        "server": {
            "type": "local",
            "path": str(tmp_path / "out"),
            "mode": "ingest",
            "post_ingestion": {"action": "archive", "archive_path": str(arch)},
        },
    }
    pipe = _pipe()

    def run():
        rc = _rc(**contract)
        proc = DataProcessor(contract=contract, engine="polars")
        proc._landing_entity = pipe._landing_table_name(rc)
        proc._orphaned_files = pipe._retry_orphaned_landing_files(rc)
        result = proc.run_source()
        rows = len(result.good)
        pipe._execute_post_ingestion_cleanup(rc, proc)
        return rows

    _write(land / "a.csv", "id,v\n1,x\n")
    assert run() == 1  # loaded; archiving is blocked, so the file stays and is remembered
    assert (land / "a.csv").exists()
    assert run() == 0  # still blocked: held back, NOT loaded a second time
    blocker.unlink()
    _write(land / "b.csv", "id,v\n2,y\n")
    assert run() == 1  # only b.csv is new; a.csv is archived by the retry
    assert not (land / "a.csv").exists() and not (land / "b.csv").exists()
    assert sorted(p.name for p in (arch / "orders" / "success").iterdir()) == ["a.csv", "b.csv"]
    assert ll.read_orphans(str(arch / "orders" / ll.ORPHANS_FILE)) == []


def test_retry_orphaned_files_off_neither_remembers_nor_holds_back(tmp_path):
    f = _write(tmp_path / "landing" / "a.csv")
    arch = tmp_path / "arch"
    _block(arch / "orders")
    rc = _rc(
        source={"path": str(tmp_path / "landing")},
        server={"post_ingestion": {"action": "archive", "archive_path": str(arch), "retry_orphaned_files": False}},
    )
    pipe = _pipe()
    pipe._execute_post_ingestion_cleanup(rc, types.SimpleNamespace(_source_files=[{"path": f}]))
    assert not (arch / "orders" / ll.ORPHANS_FILE).exists()
    assert pipe._retry_orphaned_landing_files(rc) == set()


def test_retry_orphaned_files_defaults_on():
    assert ll.resolve_policy(None, {"post_ingestion": {"action": "delete"}}).retry_orphans is True
    assert ll.resolve_policy(None, {"post_ingestion": {"retry_orphaned_files": False}}).retry_orphans is False


def test_a_stuck_file_and_a_new_one_only_the_new_one_is_read(tmp_path):
    land = tmp_path / "landing"
    arch = tmp_path / "arch"
    _block(arch / "orders")
    contract = {
        "version": "1.0.0",
        "info": {"title": "b", "target_layer": "bronze", "table_name": "orders"},
        "source": {"type": "landing", "path": str(land), "format": "csv"},
        "model": {"fields": [{"name": "id", "type": "string"}, {"name": "v", "type": "string"}]},
        "server": {
            "type": "local",
            "path": str(tmp_path / "out"),
            "mode": "ingest",
            "post_ingestion": {"action": "archive", "archive_path": str(arch)},
        },
    }
    pipe = _pipe()

    def run():
        rc = _rc(**contract)
        proc = DataProcessor(contract=contract, engine="polars")
        proc._orphaned_files = pipe._retry_orphaned_landing_files(rc)
        result = proc.run_source()
        pipe._execute_post_ingestion_cleanup(rc, proc)
        return sorted(result.good["id"].to_list()) if len(result.good) else []

    _write(land / "a.csv", "id,v\n1,x\n")
    assert run() == ["1"]
    _write(land / "b.csv", "id,v\n2,y\n")
    assert run() == ["2"]  # a.csv is still stuck and held back; only b.csv is read
