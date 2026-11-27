"""Parallel stream sinks on one Kafka topic (2026-11-27): worker i of N reads only its partitions,
under its own checkpoint; changing N resumes from the previous layout."""

from __future__ import annotations

import pytest

from lakelogic.core.processor import _stream_worker, stream_worker_keys
from lakelogic.core.stream_sink import KafkaOffsetSource, SQLiteCheckpointStore, StreamSink, _SimpleTP
from tests.test_stream_sink import _events, _FakeKafkaConsumer, _FakeProcessor

DATA = {0: _events(30), 1: _events(40), 2: _events(50), 3: _events(60)}  # 180 events, 4 partitions


def _worker(i, n, data=DATA):
    return KafkaOffsetSource(
        "trips",
        consumer=_FakeKafkaConsumer("trips", data),
        tp_factory=_SimpleTP,
        max_poll_records=25,
        worker_index=i,
        worker_count=n,
    )


def _run(store, i, n, data=DATA):
    keys = stream_worker_keys("trips", i, n)
    proc = _FakeProcessor()
    s = StreamSink(
        source=_worker(i, n, data),
        processor=proc,
        checkpoint=store,
        checkpoint_key=keys["key"],
        seed_keys=keys["seeds"],
        batch_size=50,
    ).run("available_now")
    return s, proc


def test_two_workers_split_the_partitions_with_no_overlap_or_gap(tmp_path):
    store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
    s0, p0 = _run(store, 0, 2)
    s1, p1 = _run(store, 1, 2)
    assert s0.cursor == {"trips:0": 30, "trips:2": 50}  # even partitions
    assert s1.cursor == {"trips:1": 40, "trips:3": 60}  # odd partitions
    assert s0.source_count + s1.source_count == 180
    assert store.load("stream::trips#0/2").cursor == s0.cursor
    assert store.load("stream::trips#1/2").cursor == s1.cursor


def test_a_killed_worker_resumes_on_its_own_without_touching_the_others(tmp_path):
    store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
    _run(store, 0, 2)
    _run(store, 1, 2)
    grown = {p: v + _events(10) for p, v in DATA.items()}  # 10 new events per partition
    s1, _ = _run(store, 1, 2, grown)
    assert s1.source_count == 20 and s1.cursor == {"trips:1": 50, "trips:3": 70}
    assert store.load("stream::trips#0/2").cursor == {"trips:0": 30, "trips:2": 50}  # untouched


def test_going_from_one_sink_to_two_continues_where_the_single_sink_stopped(tmp_path):
    store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
    single, _ = _run(store, 0, 1)
    assert single.source_count == 180 and store.load("stream::trips").cursor["trips:3"] == 60
    grown = {p: v + _events(5) for p, v in DATA.items()}
    s0, _ = _run(store, 0, 2, grown)
    s1, _ = _run(store, 1, 2, grown)
    assert s0.source_count + s1.source_count == 20  # only the new events: nothing re-read, nothing skipped


def test_going_from_two_sinks_to_three_seeds_each_partition_from_whoever_had_it(tmp_path):
    store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
    _run(store, 0, 2)
    _run(store, 1, 2)
    grown = {p: v + _events(1) for p, v in DATA.items()}
    total = sum(_run(store, i, 3, grown)[0].source_count for i in range(3))
    assert total == 4  # one new event per partition


def test_more_workers_than_partitions_leaves_the_extra_worker_idle():
    src = KafkaOffsetSource(
        "t",
        consumer=_FakeKafkaConsumer("t", {0: _events(3)}),
        tp_factory=_SimpleTP,
        worker_index=1,
        worker_count=2,
        drain=True,
        drain_idle_timeout_s=0.1,
        poll_timeout_ms=10,
    )
    src.seek(None)
    assert src.current_cursor() == {} and list(src.stream()) == []


def test_worker_settings_are_validated(monkeypatch):
    with pytest.raises(ValueError):
        KafkaOffsetSource("t", consumer=object(), tp_factory=_SimpleTP, worker_index=2, worker_count=2)
    monkeypatch.delenv("LAKELOGIC_STREAM_WORKER", raising=False)
    assert _stream_worker({}) == {"workers": 1, "worker": 0}
    with pytest.raises(ValueError, match="LAKELOGIC_STREAM_WORKER"):
        _stream_worker({"workers": 3})
    monkeypatch.setenv("LAKELOGIC_STREAM_WORKER", "3")
    with pytest.raises(ValueError):
        _stream_worker({"workers": 3})
    monkeypatch.setenv("LAKELOGIC_STREAM_WORKER", "2")
    assert _stream_worker({"workers": 3}) == {"workers": 3, "worker": 2}


def test_a_single_sink_keeps_todays_checkpoint_key():
    assert stream_worker_keys("trips", 0, 1)["key"] == "stream::trips"
    keys = stream_worker_keys("trips", 1, 3)
    assert keys["key"] == "stream::trips#1/3"
    assert "stream::trips" in keys["seeds"] and "stream::trips#0/2" in keys["seeds"]
    assert not any(k.endswith("/3") for k in keys["seeds"])
