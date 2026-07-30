"""Tests for the engine-agnostic streaming micro-batch sink + checkpoint store
(lakelogic/core/stream_sink.py). Implements the resumability / at-least-once
acceptance criteria from docs/specs/streaming-contracts.md §12.B.

The loop/checkpoint/resume logic is tested with a fake processor (deterministic,
no engine deps); one end-to-end test exercises a real DataProcessor + Polars.
"""
from __future__ import annotations

import types

import pytest

from lakelogic.core.stream_sink import (
    Checkpoint,
    KafkaOffsetSource,
    SSEOffsetSource,
    SparkStreamSink,
    SQLiteCheckpointStore,
    StreamSink,
    WatermarkChunkSource,
    _SimpleTP,
)


# ── Fakes ─────────────────────────────────────────────────────────────────────


class _FakeResult:
    def __init__(self, good, bad):
        self.good = good
        self.bad = bad
        self.good_count = len(good)
        self.bad_count = len(bad)
        self.source_count = len(good) + len(bad)


class _FakeProcessor:
    """Duck-typed stand-in for DataProcessor: splits rows on amount >= 0 and
    records what it materialized. Can inject a write- or commit-time failure."""

    def __init__(self, fail_on_write_batch=None):
        self.contract = types.SimpleNamespace(dataset="orders")
        self.materialized_good = []  # flat list of every good row ever written
        self._write_batches = 0
        self.fail_on_write_batch = fail_on_write_batch

    def run(self, frame):
        rows = frame.to_dicts()
        good = [r for r in rows if r.get("amount", 0) >= 0]
        bad = [r for r in rows if r.get("amount", 0) < 0]
        return _FakeResult(good, bad)

    def materialize(self, good, bad, target_path=None):
        self._write_batches += 1
        if self.fail_on_write_batch == self._write_batches:
            raise RuntimeError("injected write failure")
        self.materialized_good.extend(good)
        return {"rows_written": len(good)}


def _events(n, *, bad_every=0):
    """n event dicts; every ``bad_every``-th has a negative amount (quarantined)."""
    out = []
    for i in range(n):
        amount = -1.0 if (bad_every and i % bad_every == 0) else float(i)
        out.append({"id": i, "amount": amount})
    return out


# ── Checkpoint store ────────────────────────────────────────────────────────


class TestCheckpointStore:
    def test_load_missing_returns_none(self, tmp_path):
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        assert store.load("nope") is None

    def test_commit_then_load_roundtrip(self, tmp_path):
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        store.commit("k", Checkpoint(cursor=42, batch_id=3, committed_at="t", good_count=40, bad_count=2))
        got = store.load("k")
        assert got.cursor == 42 and got.batch_id == 3
        assert got.good_count == 40 and got.bad_count == 2

    def test_commit_upserts(self, tmp_path):
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        store.commit("k", Checkpoint(cursor=10, batch_id=1))
        store.commit("k", Checkpoint(cursor=20, batch_id=2))
        got = store.load("k")
        assert got.cursor == 20 and got.batch_id == 2

    def test_json_cursor_survives(self, tmp_path):
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        store.commit("k", Checkpoint(cursor={"partition_0": 1500}, batch_id=1))
        assert store.load("k").cursor == {"partition_0": 1500}

    def test_string_cursor_survives(self, tmp_path):
        # SSE Last-Event-ID is a scalar string cursor.
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        store.commit("k", Checkpoint(cursor="evt-42", batch_id=1))
        assert store.load("k").cursor == "evt-42"

    def test_distinct_keys_are_independent(self, tmp_path):
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        store.commit("a", Checkpoint(cursor=1, batch_id=1))
        store.commit("b", Checkpoint(cursor=2, batch_id=1))
        assert store.load("a").cursor == 1 and store.load("b").cursor == 2

    def test_persists_across_reopen(self, tmp_path):
        path = tmp_path / "ck.sqlite"
        SQLiteCheckpointStore(path).commit("k", Checkpoint(cursor=7, batch_id=1))
        # A fresh store over the same file (simulating a process restart) sees it.
        assert SQLiteCheckpointStore(path).load("k").cursor == 7

    def test_close_is_safe(self, tmp_path):
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        store.commit("k", Checkpoint(cursor=1, batch_id=1))
        store.close()  # must not raise


# ── Drain / batching ──────────────────────────────────────────────────────────


class TestAvailableNow:
    def test_drains_all_records(self, tmp_path):
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        proc = _FakeProcessor()
        sink = StreamSink(source=_events(250, bad_every=10), processor=proc,
                          checkpoint=store, checkpoint_key="k", batch_size=100)
        s = sink.run("available_now")
        assert s.batches == 3                 # 100 + 100 + 50
        assert s.source_count == 250
        assert s.good_count + s.bad_count == 250
        assert s.bad_count == 25              # every 10th
        assert s.cursor == 250                # running-count cursor
        assert len(proc.materialized_good) == s.good_count

    def test_checkpoint_committed_after_run(self, tmp_path):
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        StreamSink(source=_events(50), processor=_FakeProcessor(),
                   checkpoint=store, checkpoint_key="k", batch_size=25).run("available_now")
        ck = store.load("k")
        assert ck is not None and ck.cursor == 50 and ck.batch_id == 2

    def test_partial_final_batch_flushed(self, tmp_path):
        proc = _FakeProcessor()
        s = StreamSink(source=_events(30), processor=proc, batch_size=100,
                       checkpoint=SQLiteCheckpointStore(tmp_path / "ck.sqlite"),
                       checkpoint_key="k").run("available_now")
        assert s.batches == 1 and s.source_count == 30

    def test_empty_source_no_batches(self, tmp_path):
        s = StreamSink(source=[], processor=_FakeProcessor(),
                       checkpoint=SQLiteCheckpointStore(tmp_path / "ck.sqlite"),
                       checkpoint_key="k").run("available_now")
        assert s.batches == 0 and s.source_count == 0


# ── Resume / failure semantics (§12.B) ─────────────────────────────────────────


class TestResume:
    def test_second_run_resumes_and_processes_nothing_new(self, tmp_path):
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        data = _events(120)
        StreamSink(source=data, processor=_FakeProcessor(), checkpoint=store,
                   checkpoint_key="k", batch_size=50).run("available_now")
        # Same bounded source, re-run: cursor==120 already, nothing left.
        proc2 = _FakeProcessor()
        s2 = StreamSink(source=data, processor=proc2, checkpoint=store,
                        checkpoint_key="k", batch_size=50).run("available_now")
        assert s2.resumed_from == 120
        assert s2.source_count == 0 and s2.batches == 0
        assert proc2.materialized_good == []   # no re-processing of committed rows

    def test_crash_before_commit_resumes_with_no_gap_no_dup(self, tmp_path):
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        data = _events(250)
        # Run 1: fail on the 2nd write (batch 1 commits cursor=100, batch 2 never writes/commits).
        proc1 = _FakeProcessor(fail_on_write_batch=2)
        with pytest.raises(RuntimeError, match="injected write failure"):
            StreamSink(source=data, processor=proc1, checkpoint=store,
                       checkpoint_key="k", batch_size=100).run("available_now")
        assert store.load("k").cursor == 100          # only batch 1 committed
        assert len(proc1.materialized_good) == 100     # batch 2 wrote nothing

        # Run 2: clean processor resumes from 100.
        proc2 = _FakeProcessor()
        s2 = StreamSink(source=data, processor=proc2, checkpoint=store,
                        checkpoint_key="k", batch_size=100).run("available_now")
        assert s2.resumed_from == 100
        assert s2.source_count == 150 and s2.cursor == 250

        # No gap and no duplicate across the two runs: every id 0..249 written once.
        all_ids = sorted(r["id"] for r in proc1.materialized_good + proc2.materialized_good)
        assert all_ids == list(range(250))

    def test_commit_failure_reprocesses_at_least_once(self, tmp_path):
        """Crash AFTER write, BEFORE commit → the batch is reprocessed on re-run
        (at-least-once). Documents why resumable sources need merge/overwrite."""
        data = _events(200)

        class _CommitOnceFails(SQLiteCheckpointStore):
            def __init__(self, path):
                super().__init__(path)
                self.calls = 0

            def commit(self, key, checkpoint):
                self.calls += 1
                if self.calls == 1:
                    raise RuntimeError("injected commit failure")
                super().commit(key, checkpoint)

        store = _CommitOnceFails(tmp_path / "ck.sqlite")
        proc1 = _FakeProcessor()
        with pytest.raises(RuntimeError, match="injected commit failure"):
            StreamSink(source=data, processor=proc1, checkpoint=store,
                       checkpoint_key="k", batch_size=100).run("available_now")
        # Batch 1 was WRITTEN (100 rows) but its commit failed → no checkpoint.
        assert len(proc1.materialized_good) == 100
        assert store.load("k") is None

        # Re-run reprocesses batch 1 (rows 0..99) → duplicated absent idempotency.
        proc2 = _FakeProcessor()
        StreamSink(source=data, processor=proc2, checkpoint=store,
                   checkpoint_key="k", batch_size=100).run("available_now")
        first_100_again = [r for r in proc2.materialized_good if r["id"] < 100]
        assert len(first_100_again) == 100   # at-least-once: rows 0..99 seen twice total


# ── Connector protocol + continuous bound ──────────────────────────────────────


class TestConnectorAndModes:
    def test_accepts_object_with_stream_method(self, tmp_path):
        class _Conn:
            def stream(self):
                yield from _events(40)

        s = StreamSink(source=_Conn(), processor=_FakeProcessor(), batch_size=10,
                       checkpoint=SQLiteCheckpointStore(tmp_path / "ck.sqlite"),
                       checkpoint_key="k").run("available_now")
        assert s.source_count == 40 and s.batches == 4

    def test_continuous_respects_max_batches(self, tmp_path):
        s = StreamSink(source=_events(1000), processor=_FakeProcessor(), batch_size=10,
                       checkpoint=SQLiteCheckpointStore(tmp_path / "ck.sqlite"),
                       checkpoint_key="k").run("continuous", max_batches=3)
        assert s.batches == 3 and s.source_count == 30

    def test_push_source_rejected_for_available_now(self, tmp_path):
        class _WebhookSource:
            continuous_only = True  # no snapshotable end (§6)

            def stream(self):
                yield from _events(5)

        sink = StreamSink(source=_WebhookSource(), processor=_FakeProcessor(),
                          checkpoint=SQLiteCheckpointStore(tmp_path / "ck.sqlite"),
                          checkpoint_key="k")
        with pytest.raises(ValueError, match="continuous_only"):
            sink.run("available_now")
        # …but it's fine in continuous mode (bounded here via max_batches).
        s = sink.run("continuous", max_batches=1)
        assert s.batches == 1

    def test_cursor_fn_used_for_checkpoint(self, tmp_path):
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        StreamSink(source=_events(30), processor=_FakeProcessor(), batch_size=10,
                   checkpoint=store, checkpoint_key="k",
                   cursor_fn=lambda e: {"offset": e["id"]}).run("available_now")
        # last event of the last batch has id 29
        assert store.load("k").cursor == {"offset": 29}

    def test_invalid_mode_rejected(self, tmp_path):
        sink = StreamSink(source=_events(1), processor=_FakeProcessor(),
                          checkpoint=SQLiteCheckpointStore(tmp_path / "ck.sqlite"),
                          checkpoint_key="k")
        with pytest.raises(ValueError, match="mode must be"):
            sink.run("firehose")

    def test_batch_size_must_be_positive(self, tmp_path):
        with pytest.raises(ValueError, match="batch_size must be"):
            StreamSink(source=_events(1), processor=_FakeProcessor(),
                       checkpoint=SQLiteCheckpointStore(tmp_path / "ck.sqlite"),
                       checkpoint_key="k", batch_size=0)

    def test_window_seconds_flushes_each_record(self, tmp_path):
        # window_seconds=0 → the window is "elapsed" after every append, so each
        # record flushes as its own micro-batch regardless of batch_size.
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        s = StreamSink(source=_events(5), processor=_FakeProcessor(), batch_size=1000,
                       window_seconds=0, checkpoint=store, checkpoint_key="k").run("available_now")
        assert s.batches == 5 and s.source_count == 5
        assert [b["source"] for b in s.per_batch] == [1, 1, 1, 1, 1]

    def test_default_checkpoint_key_from_contract_dataset(self, tmp_path):
        # _FakeProcessor.contract.dataset == "orders" → key "stream::orders".
        sink = StreamSink(source=[], processor=_FakeProcessor(),
                          checkpoint=SQLiteCheckpointStore(tmp_path / "ck.sqlite"))
        assert sink.checkpoint_key == "stream::orders"

    def test_first_run_resumed_from_is_none(self, tmp_path):
        s = StreamSink(source=_events(10), processor=_FakeProcessor(), batch_size=5,
                       checkpoint=SQLiteCheckpointStore(tmp_path / "ck.sqlite"),
                       checkpoint_key="k").run("available_now")
        assert s.resumed_from is None
        assert [b["batch_id"] for b in s.per_batch] == [1, 2]

    def test_available_now_respects_max_batches(self, tmp_path):
        # max_batches also bounds an available_now drain, mid-source.
        s = StreamSink(source=_events(1000), processor=_FakeProcessor(), batch_size=10,
                       checkpoint=SQLiteCheckpointStore(tmp_path / "ck.sqlite"),
                       checkpoint_key="k").run("available_now", max_batches=4)
        assert s.batches == 4 and s.source_count == 40


# ── Native Kafka offset resume (fake broker) ───────────────────────────────────


class _FakeConsumerRecord:
    __slots__ = ("topic", "partition", "offset", "value")

    def __init__(self, topic, partition, offset, value):
        self.topic, self.partition, self.offset, self.value = topic, partition, offset, value


class _FakeKafkaConsumer:
    """Minimal kafka-python-shaped consumer over in-memory partitions, so the
    offset seek/commit/resume logic is testable with no broker.

    ``records_by_partition``: {partition_int: [value_dict, ...]}. Offsets are the
    list indices. ``poll`` returns records from each assigned partition's current
    read position and advances it; ``seek`` moves that position; ``{}`` when
    caught up."""

    def __init__(self, topic, records_by_partition):
        self.topic = topic
        self._data = {p: list(v) for p, v in records_by_partition.items()}
        self._pos = {p: 0 for p in self._data}
        self._assigned = []
        self.closed = False

    def partitions_for_topic(self, topic):
        return set(self._data.keys())

    def assign(self, tps):
        self._assigned = list(tps)

    def seek(self, tp, offset):
        self._pos[tp.partition] = offset

    def poll(self, timeout_ms=None, max_records=500):
        out = {}
        remaining = max_records
        for tp in self._assigned:
            p = tp.partition
            vals = self._data[p]
            start = self._pos[p]
            if start >= len(vals):
                continue
            end = min(len(vals), start + remaining)
            recs = [_FakeConsumerRecord(self.topic, p, o, vals[o]) for o in range(start, end)]
            self._pos[p] = end
            remaining -= len(recs)
            if recs:
                out[tp] = recs
            if remaining <= 0:
                break
        return out

    def close(self):
        self.closed = True


def _kafka_source(records_by_partition, topic="trips"):
    consumer = _FakeKafkaConsumer(topic, records_by_partition)
    return KafkaOffsetSource(topic, consumer=consumer, tp_factory=_SimpleTP, max_poll_records=50)


class TestKafkaOffsetResume:
    def test_drains_all_partitions_and_records_offsets(self, tmp_path):
        # partition 0 has 120 events, partition 1 has 80.
        src = _kafka_source({0: _events(120), 1: _events(80)})
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        s = StreamSink(source=src, processor=_FakeProcessor(), checkpoint=store,
                       checkpoint_key="k", batch_size=100).run("available_now")
        assert s.source_count == 200
        # cursor is the per-partition next-offset (broker cursor), not a count.
        assert s.cursor == {"trips:0": 120, "trips:1": 80}
        assert store.load("k").cursor == {"trips:0": 120, "trips:1": 80}

    def test_resume_seeks_to_committed_offsets_no_reprocess(self, tmp_path):
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        data = {0: _events(150), 1: _events(150)}

        # Run 1 drains everything → offsets at 150/150.
        proc1 = _FakeProcessor()
        StreamSink(source=_kafka_source(data), processor=proc1, checkpoint=store,
                   checkpoint_key="k", batch_size=100).run("available_now")
        assert store.load("k").cursor == {"trips:0": 150, "trips:1": 150}
        seen_run1 = len(proc1.materialized_good)

        # Run 2 on a fresh source (same data) resumes from the committed offsets →
        # it seeks to the end and consumes NOTHING new (no reprocessing).
        proc2 = _FakeProcessor()
        s2 = StreamSink(source=_kafka_source(data), processor=proc2, checkpoint=store,
                        checkpoint_key="k", batch_size=100).run("available_now")
        assert s2.resumed_from == {"trips:0": 150, "trips:1": 150}
        assert s2.source_count == 0 and proc2.materialized_good == []
        assert seen_run1 == 300

    def test_new_records_after_resume_are_consumed(self, tmp_path):
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        # Run 1: 100 events on partition 0.
        StreamSink(source=_kafka_source({0: _events(100)}), processor=_FakeProcessor(),
                   checkpoint=store, checkpoint_key="k", batch_size=100).run("available_now")
        assert store.load("k").cursor == {"trips:0": 100}

        # Run 2: the topic now has 160 events; resume at offset 100 → consume 60 new.
        proc2 = _FakeProcessor()
        s2 = StreamSink(source=_kafka_source({0: _events(160)}), processor=proc2,
                        checkpoint=store, checkpoint_key="k", batch_size=100).run("available_now")
        assert s2.source_count == 60
        assert store.load("k").cursor == {"trips:0": 160}
        # the 60 consumed are ids 100..159 (offset == id here)
        assert sorted(r["id"] for r in proc2.materialized_good) == list(range(100, 160))

    def test_crash_before_commit_resumes_from_broker_offset(self, tmp_path):
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        data = {0: _events(250)}

        proc1 = _FakeProcessor(fail_on_write_batch=2)
        with pytest.raises(RuntimeError, match="injected write failure"):
            StreamSink(source=_kafka_source(data), processor=proc1, checkpoint=store,
                       checkpoint_key="k", batch_size=100).run("available_now")
        assert store.load("k").cursor == {"trips:0": 100}   # only batch 1 committed
        assert len(proc1.materialized_good) == 100

        proc2 = _FakeProcessor()
        s2 = StreamSink(source=_kafka_source(data), processor=proc2, checkpoint=store,
                        checkpoint_key="k", batch_size=100).run("available_now")
        assert s2.resumed_from == {"trips:0": 100}
        assert s2.source_count == 150
        all_ids = sorted(r["id"] for r in proc1.materialized_good + proc2.materialized_good)
        assert all_ids == list(range(250))   # no gap, no dup

    def test_direct_stream_use_without_streamsink(self):
        # Used standalone (no StreamSink resume): assigns from start, drains, decodes.
        src = _kafka_source({0: _events(3)})
        src.drain = True
        rows = list(src.stream())
        assert [r["id"] for r in rows] == [0, 1, 2]
        assert src.current_cursor() == {"trips:0": 3}

    def test_paging_respects_max_poll_records(self, tmp_path):
        # 120 events, max_poll_records=50 → the fake broker yields 50/50/20 across
        # polls; StreamSink still assembles clean 100-row batches.
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        s = StreamSink(source=_kafka_source({0: _events(120)}), processor=_FakeProcessor(),
                       checkpoint=store, checkpoint_key="k", batch_size=100).run("available_now")
        assert s.source_count == 120 and store.load("k").cursor == {"trips:0": 120}

    def test_continuous_offset_aware_bounded_by_max_batches(self, tmp_path):
        # In continuous mode drain=False; bound with max_batches so it doesn't block.
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        s = StreamSink(source=_kafka_source({0: _events(300)}), processor=_FakeProcessor(),
                       checkpoint=store, checkpoint_key="k", batch_size=100).run(
                           "continuous", max_batches=2)
        assert s.batches == 2 and s.source_count == 200

    def test_decode_bytes_json_deserializer_and_passthrough(self):
        src = KafkaOffsetSource("t", consumer=object(), tp_factory=_SimpleTP)
        assert src._decode(b'{"id": 1}') == {"id": 1}          # bytes JSON
        assert src._decode({"id": 2}) == {"id": 2}             # already a dict
        src2 = KafkaOffsetSource("t", consumer=object(), tp_factory=_SimpleTP,
                                 value_deserializer=lambda b: {"raw": b.decode()})
        assert src2._decode(b"hi") == {"raw": "hi"}            # custom deserializer

    def test_close_swallows_errors(self):
        class _BadConsumer:
            def close(self):
                raise RuntimeError("boom")

        KafkaOffsetSource("t", consumer=_BadConsumer(), tp_factory=_SimpleTP).close()  # no raise


# ── Native SSE Last-Event-ID resume (fake feed) ────────────────────────────────


class _SSEEvent:
    """Minimal sseclient-shaped event: ``.id`` + ``.data`` (JSON string)."""

    __slots__ = ("id", "data")

    def __init__(self, id, data):
        self.id = id
        self.data = data


def _sse_feed(events):
    """Build an injectable connect(last_event_id) over an ordered event log.

    ``events``: [(id, value_dict), ...] in server order. Resume replays every
    event *after* the armed Last-Event-ID (mirrors a real SSE server), so a
    fresh connect with a cursor yields only newer events."""

    def connect(last_event_id):
        started = last_event_id is None
        for eid, value in events:
            if not started:
                if str(eid) == str(last_event_id):
                    started = True
                continue
            import json as _json

            yield _SSEEvent(id=str(eid), data=_json.dumps(value))

    return connect


class TestSSEOffsetResume:
    def test_drains_feed_and_records_last_event_id(self, tmp_path):
        events = [(i, {"id": i, "amount": float(i)}) for i in range(50)]
        src = SSEOffsetSource(connect=_sse_feed(events))
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        s = StreamSink(source=src, processor=_FakeProcessor(), checkpoint=store,
                       checkpoint_key="k", batch_size=20).run("available_now")
        assert s.source_count == 50
        # cursor is the Last-Event-ID (a string), not a count.
        assert s.cursor == "49"
        assert store.load("k").cursor == "49"

    def test_resume_replays_after_last_event_id_no_reprocess(self, tmp_path):
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        events = [(i, {"id": i, "amount": float(i)}) for i in range(100)]

        proc1 = _FakeProcessor()
        StreamSink(source=SSEOffsetSource(connect=_sse_feed(events)), processor=proc1,
                   checkpoint=store, checkpoint_key="k", batch_size=40).run("available_now")
        assert store.load("k").cursor == "99"

        # Fresh connect resumes from Last-Event-ID=99 → nothing after it.
        proc2 = _FakeProcessor()
        s2 = StreamSink(source=SSEOffsetSource(connect=_sse_feed(events)), processor=proc2,
                        checkpoint=store, checkpoint_key="k", batch_size=40).run("available_now")
        assert s2.resumed_from == "99"
        assert s2.source_count == 0 and proc2.materialized_good == []

    def test_new_events_after_resume_are_consumed(self, tmp_path):
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        first = [(i, {"id": i, "amount": float(i)}) for i in range(100)]
        StreamSink(source=SSEOffsetSource(connect=_sse_feed(first)), processor=_FakeProcessor(),
                   checkpoint=store, checkpoint_key="k", batch_size=100).run("available_now")
        assert store.load("k").cursor == "99"

        # Feed grew to 160 events; resume at id 99 → consume ids 100..159.
        grown = [(i, {"id": i, "amount": float(i)}) for i in range(160)]
        proc2 = _FakeProcessor()
        s2 = StreamSink(source=SSEOffsetSource(connect=_sse_feed(grown)), processor=proc2,
                        checkpoint=store, checkpoint_key="k", batch_size=100).run("available_now")
        assert s2.source_count == 60
        assert store.load("k").cursor == "159"
        assert sorted(r["id"] for r in proc2.materialized_good) == list(range(100, 160))

    def test_events_without_id_do_not_advance_cursor(self, tmp_path):
        # Per the SSE spec, an event without an id doesn't change Last-Event-ID.
        events = [("a", {"id": 0, "amount": 1.0}),
                  (None, {"id": 1, "amount": 2.0}),   # no id → cursor stays "a"
                  ("c", {"id": 2, "amount": 3.0})]
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        s = StreamSink(source=SSEOffsetSource(connect=_sse_feed(events)),
                       processor=_FakeProcessor(), checkpoint=store,
                       checkpoint_key="k", batch_size=100).run("available_now")
        assert s.source_count == 3
        assert s.cursor == "c"   # last *identified* event id

    def test_crash_before_commit_resumes_from_last_event_id(self, tmp_path):
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        events = [(i, {"id": i, "amount": float(i)}) for i in range(250)]

        proc1 = _FakeProcessor(fail_on_write_batch=2)
        with pytest.raises(RuntimeError, match="injected write failure"):
            StreamSink(source=SSEOffsetSource(connect=_sse_feed(events)), processor=proc1,
                       checkpoint=store, checkpoint_key="k", batch_size=100).run("available_now")
        assert store.load("k").cursor == "99"   # only batch 1 committed
        assert len(proc1.materialized_good) == 100

        proc2 = _FakeProcessor()
        s2 = StreamSink(source=SSEOffsetSource(connect=_sse_feed(events)), processor=proc2,
                        checkpoint=store, checkpoint_key="k", batch_size=100).run("available_now")
        assert s2.resumed_from == "99"
        assert s2.source_count == 150
        all_ids = sorted(r["id"] for r in proc1.materialized_good + proc2.materialized_good)
        assert all_ids == list(range(250))   # no gap, no dup

    def test_keepalive_comment_lines_skipped(self, tmp_path):
        # An SSE keep-alive has no data field → nothing to process, cursor untouched.
        def connect(last_id):
            yield _SSEEvent(id="1", data=None)         # comment / keep-alive
            yield _SSEEvent(id="2", data='{"id": 5, "amount": 1.0}')

        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        s = StreamSink(source=SSEOffsetSource(connect=connect), processor=_FakeProcessor(),
                       checkpoint=store, checkpoint_key="k", batch_size=100).run("available_now")
        assert s.source_count == 1 and s.cursor == "2"

    def test_drain_stops_on_none_sentinel(self, tmp_path):
        # A None event signals "caught up"; drain=True (available_now) → stop there.
        def connect(last_id):
            yield _SSEEvent(id="1", data='{"id": 1, "amount": 1.0}')
            yield None                                  # caught-up sentinel
            yield _SSEEvent(id="2", data='{"id": 2, "amount": 1.0}')  # never reached

        s = StreamSink(source=SSEOffsetSource(connect=connect), processor=_FakeProcessor(),
                       checkpoint=SQLiteCheckpointStore(tmp_path / "ck.sqlite"),
                       checkpoint_key="k", batch_size=100).run("available_now")
        assert s.source_count == 1 and s.cursor == "1"

    def test_decode_variants(self):
        src = SSEOffsetSource(connect=lambda _id: [])
        assert src._decode('{"a": 1}') == {"a": 1}          # JSON string
        assert src._decode("plain text") == {"data": "plain text"}  # non-JSON → wrapped
        assert src._decode(b'{"b": 2}') == {"b": 2}          # bytes JSON
        src2 = SSEOffsetSource(connect=lambda _id: [], value_deserializer=lambda d: {"v": d})
        assert src2._decode("x") == {"v": "x"}               # custom deserializer

    def test_dict_shaped_events_supported(self, tmp_path):
        # sseclient-style objects OR plain dicts both work via _field.
        def connect(last_id):
            yield {"id": "10", "data": '{"id": 10, "amount": 2.0}'}

        s = StreamSink(source=SSEOffsetSource(connect=connect), processor=_FakeProcessor(),
                       checkpoint=SQLiteCheckpointStore(tmp_path / "ck.sqlite"),
                       checkpoint_key="k", batch_size=100).run("available_now")
        assert s.source_count == 1 and s.cursor == "10"


# ── Keyset/watermark chunk source (resumable large loads, §15.3) ────────────────


def _keyset_fetch(rows):
    """Build a fetch_chunk(after, limit) over an in-memory table sorted by 'wm'
    (== row id here) — mimics `WHERE wm > :after ORDER BY wm LIMIT :n`."""
    table = sorted(rows, key=lambda r: r["wm"])

    def fetch(after, limit):
        start = 0 if after is None else next(
            (i for i, r in enumerate(table) if r["wm"] > after), len(table)
        )
        return table[start:start + limit]

    return fetch


class TestWatermarkChunkSource:
    def test_pages_whole_table_and_records_watermark(self, tmp_path):
        rows = [{"id": i, "wm": i, "amount": float(i)} for i in range(250)]
        src = WatermarkChunkSource(_keyset_fetch(rows), watermark_field="wm", chunk_size=100)
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        s = StreamSink(source=src, processor=_FakeProcessor(), checkpoint=store,
                       checkpoint_key="k", batch_size=100).run("available_now")
        assert s.source_count == 250
        assert s.cursor == 249                 # last watermark value, not a count
        assert store.load("k").cursor == 249

    def test_resume_from_committed_watermark_no_reprocess(self, tmp_path):
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        rows = [{"id": i, "wm": i, "amount": float(i)} for i in range(150)]
        StreamSink(source=WatermarkChunkSource(_keyset_fetch(rows), watermark_field="wm", chunk_size=50),
                   processor=_FakeProcessor(), checkpoint=store, checkpoint_key="k",
                   batch_size=50).run("available_now")
        assert store.load("k").cursor == 149

        proc2 = _FakeProcessor()
        s2 = StreamSink(source=WatermarkChunkSource(_keyset_fetch(rows), watermark_field="wm", chunk_size=50),
                        processor=proc2, checkpoint=store, checkpoint_key="k",
                        batch_size=50).run("available_now")
        assert s2.resumed_from == 149
        assert s2.source_count == 0 and proc2.materialized_good == []

    def test_new_rows_after_resume_are_paged(self, tmp_path):
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        first = [{"id": i, "wm": i, "amount": float(i)} for i in range(100)]
        StreamSink(source=WatermarkChunkSource(_keyset_fetch(first), watermark_field="wm", chunk_size=100),
                   processor=_FakeProcessor(), checkpoint=store, checkpoint_key="k",
                   batch_size=100).run("available_now")
        assert store.load("k").cursor == 99

        grown = [{"id": i, "wm": i, "amount": float(i)} for i in range(160)]
        proc2 = _FakeProcessor()
        s2 = StreamSink(source=WatermarkChunkSource(_keyset_fetch(grown), watermark_field="wm", chunk_size=100),
                        processor=proc2, checkpoint=store, checkpoint_key="k",
                        batch_size=100).run("available_now")
        assert s2.source_count == 60
        assert sorted(r["id"] for r in proc2.materialized_good) == list(range(100, 160))

    def test_crash_before_commit_resumes_from_watermark(self, tmp_path):
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        rows = [{"id": i, "wm": i, "amount": float(i)} for i in range(250)]

        proc1 = _FakeProcessor(fail_on_write_batch=2)
        with pytest.raises(RuntimeError, match="injected write failure"):
            StreamSink(source=WatermarkChunkSource(_keyset_fetch(rows), watermark_field="wm", chunk_size=100),
                       processor=proc1, checkpoint=store, checkpoint_key="k",
                       batch_size=100).run("available_now")
        assert store.load("k").cursor == 99   # only batch 1 committed

        proc2 = _FakeProcessor()
        StreamSink(source=WatermarkChunkSource(_keyset_fetch(rows), watermark_field="wm", chunk_size=100),
                   processor=proc2, checkpoint=store, checkpoint_key="k",
                   batch_size=100).run("available_now")
        all_ids = sorted(r["id"] for r in proc1.materialized_good + proc2.materialized_good)
        assert all_ids == list(range(250))   # no gap, no dup

    def test_chunk_size_must_be_positive(self):
        with pytest.raises(ValueError, match="chunk_size must be"):
            WatermarkChunkSource(lambda a, n: [], watermark_field="wm", chunk_size=0)

    def test_short_final_page_drains(self, tmp_path):
        # 25 rows, chunk_size 10 → pages of 10/10/5; the short page (5) ends the drain.
        rows = [{"id": i, "wm": i, "amount": float(i)} for i in range(25)]
        s = StreamSink(source=WatermarkChunkSource(_keyset_fetch(rows), watermark_field="wm", chunk_size=10),
                       processor=_FakeProcessor(),
                       checkpoint=SQLiteCheckpointStore(tmp_path / "ck.sqlite"),
                       checkpoint_key="k", batch_size=1000).run("available_now")
        assert s.source_count == 25 and s.cursor == 24

    def test_continuous_bounded_by_max_batches(self, tmp_path):
        # continuous (drain=False) bounded by max_batches so it doesn't block.
        rows = [{"id": i, "wm": i, "amount": float(i)} for i in range(300)]
        s = StreamSink(source=WatermarkChunkSource(_keyset_fetch(rows), watermark_field="wm", chunk_size=10),
                       processor=_FakeProcessor(),
                       checkpoint=SQLiteCheckpointStore(tmp_path / "ck.sqlite"),
                       checkpoint_key="k", batch_size=10).run("continuous", max_batches=2)
        assert s.batches == 2 and s.source_count == 20


# ── Spark foreachBatch path (fake Structured Streaming writer) ──────────────────


class _FakeSparkFrame:
    """Micro-batch stand-in exposing ``.to_dicts()`` so _FakeProcessor can split it."""

    def __init__(self, rows):
        self._rows = rows

    def to_dicts(self):
        return self._rows


class _FakeStreamWriter:
    """kafka-python-free stand-in for Spark's DataStreamWriter. Records the wiring
    and, on ``start()``, replays the provided micro-batches through foreachBatch —
    exactly as Spark drives an ``availableNow`` drain."""

    def __init__(self, micro_batches):
        self._micro_batches = micro_batches
        self.options = {}
        self.output_mode_val = None
        self.trigger_kwargs = None
        self._fn = None
        self.terminated = False

    def foreachBatch(self, fn):
        self._fn = fn
        return self

    def option(self, k, v):
        self.options[k] = v
        return self

    def outputMode(self, mode):
        self.output_mode_val = mode
        return self

    def trigger(self, **kwargs):
        self.trigger_kwargs = kwargs
        return self

    def start(self):
        # Spark assigns monotonically increasing batch ids from 0.
        for batch_id, rows in enumerate(self._micro_batches):
            self._fn(_FakeSparkFrame(rows), batch_id)
        return self  # acts as the StreamingQuery too

    def awaitTermination(self):
        self.terminated = True


class _FakeStreamDF:
    def __init__(self, micro_batches):
        self.writeStream = _FakeStreamWriter(micro_batches)


class TestSparkStreamSink:
    def test_requires_checkpoint_location(self):
        with pytest.raises(ValueError, match="checkpoint_location is required"):
            SparkStreamSink(stream_df=_FakeStreamDF([]), processor=_FakeProcessor(),
                            checkpoint_location=None)

    def test_processing_time_trigger_needs_interval(self):
        with pytest.raises(ValueError, match="processing_time interval required"):
            SparkStreamSink(stream_df=_FakeStreamDF([]), processor=_FakeProcessor(),
                            checkpoint_location="/chk", trigger="processing_time")

    def test_available_now_drains_micro_batches_through_contract(self):
        # 3 micro-batches; every 5th row is quarantined by _FakeProcessor.
        batches = [_events(20, bad_every=5), _events(20, bad_every=5), _events(10, bad_every=5)]
        proc = _FakeProcessor()
        df = _FakeStreamDF(batches)
        sink = SparkStreamSink(stream_df=df, processor=proc, checkpoint_location="/chk/trips")
        query = sink.run()

        # Every micro-batch went through the SAME contract engine.
        assert len(sink.batches) == 3
        assert [b.batch_id for b in sink.batches] == [0, 1, 2]
        assert sum(b.source_count for b in sink.batches) == 50
        good_total = sum(b.good_count for b in sink.batches)
        assert len(proc.materialized_good) == good_total
        # Spark owns resumability: checkpointLocation wired, availableNow drain, waited.
        assert df.writeStream.options["checkpointLocation"] == "/chk/trips"
        assert df.writeStream.trigger_kwargs == {"availableNow": True}
        assert query.terminated is True

    def test_processing_time_trigger_wiring_and_no_block_by_default(self):
        df = _FakeStreamDF([_events(10)])
        sink = SparkStreamSink(stream_df=df, processor=_FakeProcessor(),
                               checkpoint_location="/chk", trigger="processing_time",
                               processing_time="30 seconds", output_mode="update")
        query = sink.run()
        assert df.writeStream.trigger_kwargs == {"processingTime": "30 seconds"}
        assert df.writeStream.output_mode_val == "update"
        assert query.terminated is False   # continuous: don't block by default

    def test_on_batch_hook_fires_per_micro_batch(self):
        seen = []
        df = _FakeStreamDF([_events(10), _events(10)])
        SparkStreamSink(stream_df=df, processor=_FakeProcessor(), checkpoint_location="/chk",
                        on_batch=lambda b: seen.append(b.batch_id)).run()
        assert seen == [0, 1]   # per-window run-log/metrics hook

    def test_invalid_trigger_rejected(self):
        with pytest.raises(ValueError, match="trigger must be"):
            SparkStreamSink(stream_df=_FakeStreamDF([]), processor=_FakeProcessor(),
                            checkpoint_location="/chk", trigger="firehose")

    def test_await_termination_override(self):
        # Force-block a processing_time stream …
        df = _FakeStreamDF([_events(5)])
        q = SparkStreamSink(stream_df=df, processor=_FakeProcessor(), checkpoint_location="/chk",
                            trigger="processing_time", processing_time="10 seconds").run(
                                await_termination=True)
        assert q.terminated is True
        # … and don't block an available_now drain.
        df2 = _FakeStreamDF([_events(5)])
        q2 = SparkStreamSink(stream_df=df2, processor=_FakeProcessor(),
                             checkpoint_location="/chk").run(await_termination=False)
        assert q2.terminated is False

    def test_micro_batch_result_carries_counts(self):
        df = _FakeStreamDF([_events(20, bad_every=4)])   # 5 quarantined of 20
        sink = SparkStreamSink(stream_df=df, processor=_FakeProcessor(), checkpoint_location="/chk")
        sink.run()
        b = sink.batches[0]
        assert b.source_count == 20 and b.bad_count == 5 and b.good_count == 15


# ── End-to-end with a real DataProcessor (Polars) ───────────────────────────────


class TestEndToEnd:
    def test_real_processor_validates_and_checkpoints(self, tmp_path):
        contract = {
            "version": "1.0.0",
            "dataset": "orders_stream",
            "info": {"title": "bronze_orders_stream"},
            "model": {
                "fields": [
                    {"name": "id", "type": "integer", "required": True},
                    {"name": "amount", "type": "float"},
                ]
            },
            "quality": {
                "row_rules": [
                    {"name": "positive_amount", "sql": "amount >= 0"},
                ]
            },
            "materialization": {
                "strategy": "append",
                "format": "parquet",
                "target_path": str(tmp_path / "bronze"),
            },
        }
        store = SQLiteCheckpointStore(tmp_path / "ck.sqlite")
        sink = StreamSink(
            contract=contract,
            source=_events(20, bad_every=5),   # 4 negative-amount rows
            engine="polars",
            checkpoint=store,
            checkpoint_key="orders",
            batch_size=1000,                    # single batch, avoid append fan-out
            target_path=str(tmp_path / "bronze"),
        )
        s = sink.run("available_now")
        assert s.source_count == 20
        assert s.bad_count == 4 and s.good_count == 16
        ck = store.load("orders")
        assert ck is not None and ck.cursor == 20 and ck.good_count == 16

    def test_merge_contract_is_effectively_once_on_replay(self, tmp_path):
        """The at-least-once loop + a `merge` contract = effectively-once: replaying
        already-processed ids does NOT duplicate rows in the target (§8)."""
        pytest.importorskip("deltalake")
        import deltalake

        target = str(tmp_path / "bronze_merge")
        contract = {
            "version": "1.0.0",
            "dataset": "orders_merge",
            "info": {"title": "bronze_orders_merge"},
            "primary_key": ["id"],
            "model": {
                "fields": [
                    {"name": "id", "type": "integer", "required": True},
                    {"name": "amount", "type": "float"},
                ]
            },
            "materialization": {"strategy": "merge", "format": "delta", "target_path": target},
        }

        # Run 1: ids 0..19.
        StreamSink(contract=contract, source=_events(20), engine="polars",
                   checkpoint=SQLiteCheckpointStore(tmp_path / "ck1.sqlite"),
                   checkpoint_key="m", batch_size=1000, target_path=target).run("available_now")
        ids1 = sorted(r["id"] for r in deltalake.DeltaTable(target).to_pyarrow_table().to_pylist())
        assert ids1 == list(range(20))

        # Replay: a fresh checkpoint forces full reprocessing of 0..19 PLUS 10 new
        # (20..29). Under bare append this would leave 0..19 duplicated; merge keys
        # on id, so the target must hold exactly 30 distinct rows.
        StreamSink(contract=contract, source=_events(30), engine="polars",
                   checkpoint=SQLiteCheckpointStore(tmp_path / "ck2.sqlite"),
                   checkpoint_key="m", batch_size=1000, target_path=target).run("available_now")
        ids2 = sorted(r["id"] for r in deltalake.DeltaTable(target).to_pyarrow_table().to_pylist())
        assert ids2 == list(range(30))   # effectively-once: no duplicate 0..19
