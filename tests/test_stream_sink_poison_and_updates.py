"""Found against a real Kafka broker (2026-10-07), proven here without one.

1. A message that is not JSON killed the run before its checkpoint, so every restart died on it:
   the stream stopped for good. It is now a row the contract QUARANTINES, and the stream moves on.
2. Two events for one key in one micro-batch both reached the merge and both were inserted.
   The last one now wins; the earlier one is counted as superseded.
"""

import json
from types import SimpleNamespace

import pytest

from lakelogic import SQLiteCheckpointStore, StreamSink
from lakelogic.core.stream_sink import UNREADABLE_COLUMN, KafkaOffsetSource

pytest.importorskip("polars")


class _Consumer:
    """The kafka-python consumer surface KafkaOffsetSource uses, over fixed records."""

    def __init__(self, values):
        self._batches = [
            {"tp": [SimpleNamespace(topic="t", partition=0, offset=i, value=v) for i, v in enumerate(values)]}
        ]

    def partitions_for_topic(self, topic):
        return {0}

    def assign(self, tps):
        pass

    def seek(self, tp, offset):
        pass

    def poll(self, timeout_ms=0, max_records=0):
        return self._batches.pop(0) if self._batches else {}


CONTRACT = {
    "version": "1.0.0",
    "info": {"title": "rides"},
    "primary_key": ["ride_id"],
    "model": {"fields": [{"name": "ride_id", "type": "long", "required": True}, {"name": "status", "type": "string"}]},
}


def test_an_undecodable_message_is_quarantined_and_the_stream_continues(tmp_path):
    values = [json.dumps({"ride_id": 1}).encode(), b"{not json", b"[1, 2]", json.dumps({"ride_id": 2}).encode()]
    src = KafkaOffsetSource("t", consumer=_Consumer(values), drain=True)
    events = list(src.stream())
    assert "not valid JSON" in events[1][UNREADABLE_COLUMN] and "offset 1" in events[1][UNREADABLE_COLUMN]
    assert "not a JSON object" in events[2][UNREADABLE_COLUMN]
    assert src.current_cursor() == {"t:0": 4}  # the cursor moved past the bad messages

    src = KafkaOffsetSource("t", consumer=_Consumer(values), drain=True)
    s = StreamSink(
        contract=CONTRACT,
        source=src,
        checkpoint=SQLiteCheckpointStore(tmp_path / "c.sqlite"),
        checkpoint_key="k",
        processor=None,
    ).run("available_now")
    assert (s.source_count, s.good_count, s.bad_count) == (4, 2, 2)


class _RecordingProcessor:
    """Captures what reaches the contract engine; good = every row (no rules)."""

    def __init__(self, strategy):
        self.contract = SimpleNamespace(
            primary_key=["ride_id"], materialization=SimpleNamespace(strategy=strategy), dataset="rides", info=None
        )
        self.seen = []

    def run(self, df):
        self.seen.append(df.to_dicts())
        return SimpleNamespace(good=df, bad=None, source_count=df.height, good_count=df.height, bad_count=0)

    def materialize(self, good, bad, target_path=None):
        pass


def _run(tmp_path, strategy, events):
    proc = _RecordingProcessor(strategy)
    s = StreamSink(
        source=events,
        processor=proc,
        checkpoint=SQLiteCheckpointStore(tmp_path / "c.sqlite"),
        checkpoint_key="k",
        batch_size=100,
    ).run("available_now")
    return s, proc.seen[0]


def test_a_merge_keeps_the_last_event_per_key_in_a_batch(tmp_path):
    events = [
        {"ride_id": 1, "status": "completed"},
        {"ride_id": 2, "status": "completed"},
        {"ride_id": 1, "status": "refunded"},
        {"ride_id": None, "status": "x"},
        {"ride_id": None, "status": "y"},
    ]
    s, seen = _run(tmp_path, "merge", events)
    assert [(e["ride_id"], e["status"]) for e in seen] == [(2, "completed"), (1, "refunded"), (None, "x"), (None, "y")]
    assert (s.source_count, s.superseded_count) == (5, 1)  # read = good + bad + superseded


def test_an_append_contract_keeps_every_event(tmp_path):
    events = [{"ride_id": 1, "status": "completed"}, {"ride_id": 1, "status": "refunded"}]
    s, seen = _run(tmp_path, "append", events)
    assert len(seen) == 2 and s.superseded_count == 0


def test_quarantined_rows_without_a_target_are_reported_not_silently_dropped(tmp_path):
    from loguru import logger

    seen = []
    sink_id = logger.add(lambda m: seen.append(m.record["message"]), level="WARNING")
    try:
        contract = {
            **CONTRACT,
            "materialization": {"strategy": "merge", "format": "delta", "target_path": str(tmp_path / "bronze")},
        }
        events = [{"ride_id": i} for i in range(5)] + [{"status": "no id"}] * 2
        for _ in range(2):  # two batches: warned once, not per batch
            StreamSink(
                contract=contract,
                source=events,
                checkpoint=SQLiteCheckpointStore(tmp_path / "c.sqlite"),
                checkpoint_key=f"k{_}",
                batch_size=3,
                target_path=str(tmp_path / "bronze"),
            ).run("available_now")
    finally:
        logger.remove(sink_id)
    warned = [m for m in seen if "were NOT saved" in m]
    assert warned and "quarantine.target" in warned[0]
    assert len(warned) == 2  # once per processor (one per StreamSink here), not once per micro-batch


class _SlowStartConsumer(_Consumer):
    """A broker whose first polls after a reconnect come back EMPTY although data is waiting
    (seen on Azure Event Hubs). end_offsets says 3 messages are there."""

    def __init__(self, values, empty_first=2):
        super().__init__(values)
        self._empty = empty_first

    def end_offsets(self, tps):
        return {tp: 3 for tp in tps}

    def position(self, tp):
        return 0

    def poll(self, timeout_ms=0, max_records=0):
        if self._empty:
            self._empty -= 1
            return {}
        return super().poll(timeout_ms, max_records)


def test_available_now_drains_to_the_end_offsets_not_the_first_empty_poll():
    values = [json.dumps({"ride_id": i}).encode() for i in range(3)]
    src = KafkaOffsetSource("t", consumer=_SlowStartConsumer(values), drain=True)
    assert [e["ride_id"] for e in src.stream()] == [0, 1, 2]  # used to return [] on the first empty poll
