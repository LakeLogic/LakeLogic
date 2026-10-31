"""Landing files after a Bronze load: <archive_path>/<table>/<success|error>/<relative path>."""

from __future__ import annotations

import types
from pathlib import Path

import pytest

from lakelogic.pipeline import landing_lifecycle as ll
from lakelogic.pipeline.runner import LakehousePipeline


def _write(path: Path, text: str = "id\n1\n") -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return str(path)


# ── policy ─────────────────────────────────────────────────────────────────


def test_policy_defaults_to_retain():
    p = ll.resolve_policy(None, None)
    assert (p.action, p.on_error, p.archive_root, p.cleanup_is_blocking) == ("retain", "retain", None, False)


def test_policy_merges_contract_over_system_field_by_field():
    server = {"post_ingestion": {"action": "archive", "archive_path": "/arch", "cleanup_is_blocking": True}}
    source = {"post_ingestion": {"on_error": "move"}}
    p = ll.resolve_policy(source, server)
    # the contract only set on_error; it keeps the system's action and root
    assert (p.action, p.on_error, p.archive_root, p.cleanup_is_blocking) == ("archive", "move", "/arch", True)


def test_policy_contract_overrides_the_system_value():
    server = {"post_ingestion": {"action": "delete"}}
    source = {"post_ingestion": {"action": "archive", "archive_path": "/own"}}
    assert ll.resolve_policy(source, server).action == "archive"
    assert ll.resolve_policy(source, server).archive_root == "/own"


def test_policy_falls_back_to_storage_archive_path():
    p = ll.resolve_policy(None, {"post_ingestion": {"action": "archive"}}, "/from/storage")
    assert p.archive_root == "/from/storage"


# ── layout ─────────────────────────────────────────────────────────────────


def test_destination_adds_entity_and_outcome_under_the_root():
    assert ll.destination_dir("s3://arch/sales", "orders", ll.SUCCESS) == "s3://arch/sales/orders/success"
    assert ll.destination_dir("s3://arch/sales/", "orders", ll.ERROR) == "s3://arch/sales/orders/error"


def test_entity_folder_is_one_safe_name():
    assert ll.entity_folder("bronze/orders v2") == "bronze_orders_v2"
    assert ll.entity_folder("") == "unnamed"


def test_relative_paths_keep_date_folders_under_a_glob_landing(tmp_path):
    a = _write(tmp_path / "landing" / "dt=2026-10-01" / "a.csv")
    b = _write(tmp_path / "landing" / "dt=2026-10-02" / "b.csv")
    rels = ll.relative_paths([a, b], str(tmp_path / "landing" / "**" / "*.csv"))
    assert rels[a] == "dt=2026-10-01/a.csv"
    assert rels[b] == "dt=2026-10-02/b.csv"


def test_relative_paths_fall_back_to_the_common_folder(tmp_path):
    a = _write(tmp_path / "x" / "d1" / "a.csv")
    b = _write(tmp_path / "x" / "d2" / "b.csv")
    rels = ll.relative_paths([a, b], "Files/not/the/real/path/*.csv")
    assert rels[a] == "d1/a.csv" and rels[b] == "d2/b.csv"


# ── success ────────────────────────────────────────────────────────────────


def test_delete_removes_only_the_files_the_load_read(tmp_path):
    read = _write(tmp_path / "landing" / "read.csv")
    late = _write(tmp_path / "landing" / "arrived_during_the_load.csv")
    n = ll.clean_after_success(
        [read], ll.LandingPolicy(action="delete"), entity="orders", landing_path=str(tmp_path / "landing")
    )
    assert n == 1
    assert not Path(read).exists()
    assert Path(late).exists()


def test_archive_lands_under_entity_and_success(tmp_path):
    f = _write(tmp_path / "landing" / "dt=1" / "a.csv", "id\n7\n")
    root = tmp_path / "archive"
    ll.clean_after_success(
        [f],
        ll.LandingPolicy(action="archive", archive_root=str(root)),
        entity="orders",
        landing_path=str(tmp_path / "landing" / "**" / "*.csv"),
    )
    moved = root / "orders" / "success" / "dt=1" / "a.csv"
    assert moved.read_text(encoding="utf-8") == "id\n7\n"
    assert not Path(f).exists()


def test_two_entities_share_one_root_without_colliding(tmp_path):
    root = str(tmp_path / "archive")
    pol = ll.LandingPolicy(action="archive", archive_root=root)
    a = _write(tmp_path / "la" / "same.csv", "a")
    b = _write(tmp_path / "lb" / "same.csv", "b")
    ll.clean_after_success([a], pol, entity="orders", landing_path=str(tmp_path / "la"))
    ll.clean_after_success([b], pol, entity="drivers", landing_path=str(tmp_path / "lb"))
    assert (tmp_path / "archive" / "orders" / "success" / "same.csv").read_text(encoding="utf-8") == "a"
    assert (tmp_path / "archive" / "drivers" / "success" / "same.csv").read_text(encoding="utf-8") == "b"


def test_a_repeated_file_name_is_never_overwritten(tmp_path):
    root = tmp_path / "archive"
    pol = ll.LandingPolicy(action="archive", archive_root=str(root))
    first = _write(tmp_path / "landing" / "a.csv", "first")
    ll.clean_after_success([first], pol, entity="orders", landing_path=str(tmp_path / "landing"))
    again = _write(tmp_path / "landing" / "a.csv", "second")
    ll.clean_after_success([again], pol, entity="orders", landing_path=str(tmp_path / "landing"))
    kept = sorted((root / "orders" / "success").iterdir())
    assert len(kept) == 2
    assert {p.read_text(encoding="utf-8") for p in kept} == {"first", "second"}


def test_archive_without_a_root_leaves_the_files_and_does_not_raise(tmp_path):
    f = _write(tmp_path / "landing" / "a.csv")
    n = ll.clean_after_success([f], ll.LandingPolicy(action="archive"), entity="orders", landing_path=str(tmp_path))
    assert n == 0 and Path(f).exists()


def test_retain_touches_nothing(tmp_path):
    f = _write(tmp_path / "landing" / "a.csv")
    assert ll.clean_after_success([f], ll.LandingPolicy(), entity="orders", landing_path=str(tmp_path)) == 0
    assert Path(f).exists()


def test_a_missing_file_is_skipped_not_an_error(tmp_path):
    n = ll.clean_after_success(
        [str(tmp_path / "gone.csv")], ll.LandingPolicy(action="delete"), entity="orders", landing_path=str(tmp_path)
    )
    assert n == 0


# ── error ──────────────────────────────────────────────────────────────────


def test_move_failed_sends_the_files_to_the_error_folder(tmp_path):
    f = _write(tmp_path / "landing" / "bad.csv", "not,a,csv\n\x00")
    pol = ll.LandingPolicy(action="delete", on_error="move", archive_root=str(tmp_path / "archive"))
    n = ll.move_failed([f], pol, entity="orders", landing_path=str(tmp_path / "landing"))
    assert n == 1
    assert (tmp_path / "archive" / "orders" / "error" / "bad.csv").exists()
    assert not Path(f).exists()


def test_move_failed_needs_on_error_move(tmp_path):
    f = _write(tmp_path / "landing" / "bad.csv")
    pol = ll.LandingPolicy(action="archive", archive_root=str(tmp_path / "archive"))
    assert ll.move_failed([f], pol, entity="orders", landing_path=str(tmp_path)) == 0
    assert Path(f).exists()


def test_move_failed_without_a_root_leaves_the_files(tmp_path):
    f = _write(tmp_path / "landing" / "bad.csv")
    assert ll.move_failed([f], ll.LandingPolicy(on_error="move"), entity="orders", landing_path=str(tmp_path)) == 0
    assert Path(f).exists()


# ── the runner's use of it ─────────────────────────────────────────────────


def _runner(archive_root: str | None, **post):
    r = LakehousePipeline.__new__(LakehousePipeline)
    r.spark = None
    r.registry = types.SimpleNamespace(storage=types.SimpleNamespace(archive_path=archive_root))
    return r


def _contract(landing: Path, **post):
    return types.SimpleNamespace(
        entity="orders",
        contract_dict={"source": {"path": str(landing)}, "server": {"post_ingestion": post}},
    )


def test_runner_moves_a_read_failure_to_the_error_folder(tmp_path):
    f = _write(tmp_path / "landing" / "bad.csv")
    proc = types.SimpleNamespace(_source_files=[{"path": f}], _validation_started=False)
    c = _contract(tmp_path / "landing", action="delete", on_error="move", archive_path=str(tmp_path / "arch"))
    _runner(None)._move_failed_landing_files(c, proc)
    assert (tmp_path / "arch" / "orders" / "error" / "bad.csv").exists()


def test_runner_leaves_files_when_the_failure_came_after_the_read(tmp_path):
    f = _write(tmp_path / "landing" / "good.csv")
    proc = types.SimpleNamespace(_source_files=[{"path": f}], _validation_started=True)
    c = _contract(tmp_path / "landing", on_error="move", archive_path=str(tmp_path / "arch"))
    _runner(None)._move_failed_landing_files(c, proc)
    assert Path(f).exists()


def test_runner_uses_the_system_storage_root_when_nothing_else_names_one(tmp_path):
    f = _write(tmp_path / "landing" / "bad.csv")
    proc = types.SimpleNamespace(_source_files=[{"path": f}], _validation_started=False)
    c = _contract(tmp_path / "landing", on_error="move")
    _runner(str(tmp_path / "from_storage"))._move_failed_landing_files(c, proc)
    assert (tmp_path / "from_storage" / "orders" / "error" / "bad.csv").exists()


def test_runner_archives_a_committed_load_by_entity(tmp_path):
    f = _write(tmp_path / "landing" / "ok.csv")
    proc = types.SimpleNamespace(_source_files=[{"path": f}])
    c = _contract(tmp_path / "landing", action="archive", archive_path=str(tmp_path / "arch"))
    _runner(None)._execute_post_ingestion_cleanup(c, proc)
    assert (tmp_path / "arch" / "orders" / "success" / "ok.csv").exists()


def test_runner_blocking_cleanup_failure_raises(tmp_path):
    f = _write(tmp_path / "landing" / "ok.csv")
    proc = types.SimpleNamespace(_source_files=[{"path": f}])
    c = _contract(tmp_path / "landing", action="archive", archive_path=str(tmp_path / "arch"), cleanup_is_blocking=True)
    runner = _runner(None)
    original = ll.apply
    ll.apply = lambda *a, **k: (0, ["boom"])
    try:
        with pytest.raises(RuntimeError, match="could not be cleaned up"):
            runner._execute_post_ingestion_cleanup(c, proc)
    finally:
        ll.apply = original


# ── Spark reads lazily: the error names the file ───────────────────────────


def test_named_in_error_matches_a_path_inside_a_spark_message(tmp_path):
    bad = _write(tmp_path / "landing" / "bad.parquet")
    ok = _write(tmp_path / "landing" / "ok.parquet")
    msg = f"[FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER] Error while reading file dbfs:{bad.replace(chr(92), '/')}. Could not read footer."
    assert ll.named_in_error([bad, ok], msg) == [bad]


def test_a_message_that_names_no_file_names_none(tmp_path):
    f = _write(tmp_path / "landing" / "a.csv")
    assert ll.named_in_error([f], "a rule crashed") == []
    assert ll.named_in_error([f], "") == []


def test_runner_moves_only_the_file_a_spark_error_names(tmp_path):
    bad = _write(tmp_path / "landing" / "bad.parquet")
    ok = _write(tmp_path / "landing" / "ok.parquet")
    proc = types.SimpleNamespace(_source_files=[{"path": bad}, {"path": ok}], _validation_started=True)
    c = _contract(tmp_path / "landing", action="archive", on_error="move", archive_path=str(tmp_path / "arch"))
    err = RuntimeError(f"Error while reading file dbfs:{bad.replace(chr(92), '/')}. Could not read footer.")
    _runner(None)._move_failed_landing_files(c, proc, err)
    assert (tmp_path / "arch" / "orders" / "error" / "bad.parquet").exists()
    assert Path(ok).exists()


def test_runner_leaves_files_when_the_error_after_the_read_names_none(tmp_path):
    f = _write(tmp_path / "landing" / "a.csv")
    proc = types.SimpleNamespace(_source_files=[{"path": f}], _validation_started=True)
    c = _contract(tmp_path / "landing", on_error="move", archive_path=str(tmp_path / "arch"))
    _runner(None)._move_failed_landing_files(c, proc, RuntimeError("rule crashed"))
    assert Path(f).exists()


# ── Auto Loader (kind: files stream): the folder is listed before the run ───


def test_list_folder_skips_hidden_files_and_folders(tmp_path):
    a = _write(tmp_path / "landing" / "a.csv")
    b = _write(tmp_path / "landing" / "dt=1" / "b.csv")
    _write(tmp_path / "landing" / "_SUCCESS")
    _write(tmp_path / "landing" / ".hidden.csv")
    _write(tmp_path / "landing" / "_tmp" / "c.csv")
    assert ll.list_folder(str(tmp_path / "landing")) == sorted([a, b])


def test_list_folder_of_a_missing_or_blank_folder_is_empty(tmp_path):
    assert ll.list_folder(str(tmp_path / "nope")) == []
    assert ll.list_folder("") == []


def test_runner_cleans_the_files_a_stream_listed_before_its_run(tmp_path):
    f = _write(tmp_path / "landing" / "ok.csv")
    proc = types.SimpleNamespace(_stream_landing_files=[f])
    c = _contract(tmp_path / "landing", action="archive", archive_path=str(tmp_path / "arch"))
    _runner(None)._execute_post_ingestion_cleanup(c, proc)
    assert (tmp_path / "arch" / "orders" / "success" / "ok.csv").exists()


def test_a_file_that_landed_during_the_run_is_left_for_the_next(tmp_path):
    listed = _write(tmp_path / "landing" / "before.csv")
    late = _write(tmp_path / "landing" / "during.csv")
    proc = types.SimpleNamespace(_stream_landing_files=[listed])
    c = _contract(tmp_path / "landing", action="delete")
    _runner(None)._execute_post_ingestion_cleanup(c, proc)
    assert not Path(listed).exists() and Path(late).exists()
