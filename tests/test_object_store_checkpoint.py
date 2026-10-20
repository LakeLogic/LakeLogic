"""The durable checkpoint store: a stream's position kept beside the data on S3 / ADLS / GCS.

The SQLite store keeps the cursor on local disk, which a container, a Fabric session or a job
cluster throws away after each run, so the next run restarted from ``starting_offsets``. These
tests use an in-memory backend with the same conditional-write rules the real stores enforce
(proven live on S3, ADLS and GCS on 2026-10-20), so CI needs no cloud.
"""

from __future__ import annotations

import pytest

from lakelogic.core import stream_sink as ss
from lakelogic.core.stream_sink import (
    Checkpoint,
    CheckpointConflict,
    ObjectStoreCheckpointStore,
    SQLiteCheckpointStore,
    StreamSink,
    checkpoint_store_for,
)
from tests.test_stream_sink import _events, _FakeProcessor


class _MemoryBackend(ss._ObjectBackend):
    """An object store in a dict: every put bumps a version token; a put whose token is stale
    (or that creates an object that now exists) is refused, as S3/ADLS/GCS refuse it."""

    def __init__(self):
        self.objects = {}

    def get(self, key):
        return self.objects.get(key)

    def put_if(self, key, body, token):
        current = self.objects.get(key)
        if (current is None and token is not None) or (current is not None and current[1] != token):
            raise CheckpointConflict(f"checkpoint {key!r} was committed by another run")
        new = str(int(current[1]) + 1) if current else "1"
        self.objects[key] = (body, new)
        return new


def _store(backend):
    return ObjectStoreCheckpointStore("s3://bucket/ckpt", backend=backend)


def test_a_new_container_resumes_where_the_last_one_stopped():
    shared = _MemoryBackend()
    data = _events(120)
    StreamSink(
        source=data, processor=_FakeProcessor(), checkpoint=_store(shared), checkpoint_key="k", batch_size=50
    ).run("available_now")
    # A fresh process: new store object, same bucket. Nothing is re-processed.
    proc2 = _FakeProcessor()
    s2 = StreamSink(source=data, processor=proc2, checkpoint=_store(shared), checkpoint_key="k", batch_size=50).run(
        "available_now"
    )
    assert s2.resumed_from == 120 and s2.batches == 0
    assert proc2.materialized_good == []


def test_an_overlapping_run_cannot_overwrite_the_other_runs_commit():
    shared = _MemoryBackend()
    first, second = _store(shared), _store(shared)
    first.load("k")
    second.load("k")
    first.commit("k", Checkpoint(cursor=20, batch_id=1))
    with pytest.raises(CheckpointConflict):
        second.commit("k", Checkpoint(cursor=15, batch_id=1))
    assert _store(shared).load("k").cursor == 20


def test_a_run_that_re_reads_after_a_conflict_commits_cleanly():
    shared = _MemoryBackend()
    a, b = _store(shared), _store(shared)
    a.load("k")
    b.load("k")
    a.commit("k", Checkpoint(cursor=1, batch_id=1))
    with pytest.raises(CheckpointConflict):
        b.commit("k", Checkpoint(cursor=2, batch_id=1))
    b.load("k")
    b.commit("k", Checkpoint(cursor=3, batch_id=2))
    assert _store(shared).load("k").batch_id == 2


def test_successive_commits_by_one_run_are_not_conflicts():
    shared = _MemoryBackend()
    s = _store(shared)
    for i in range(1, 5):
        s.commit("k", Checkpoint(cursor=i, batch_id=i))
    assert _store(shared).load("k").cursor == 4


def test_a_cloud_location_gets_the_durable_store_and_a_local_one_keeps_sqlite(tmp_path, monkeypatch):
    monkeypatch.setattr(ss, "_backend_for", lambda uri: _MemoryBackend())
    for uri in ("s3://b/c", "abfss://c@acct.dfs.core.windows.net/p", "gs://b/c"):
        assert isinstance(checkpoint_store_for(uri), ObjectStoreCheckpointStore), uri
    assert isinstance(checkpoint_store_for(str(tmp_path / "ck")), SQLiteCheckpointStore)


def test_an_unsupported_scheme_is_refused_by_name():
    with pytest.raises(ValueError, match="s3://, abfss://, gs://"):
        ss._backend_for("ftp://host/ckpt")
