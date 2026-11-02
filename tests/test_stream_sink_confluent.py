"""KafkaOffsetSource on confluent-kafka (librdkafka), proven without a broker.

``ConfluentConsumer`` puts confluent-kafka's ``Consumer`` behind the kafka-python calls the source
already makes, so offsets, resume and the AvailableNow boundary behave the same on either client.
"""

import json
from types import SimpleNamespace

import pytest

ck = pytest.importorskip("confluent_kafka")
pytest.importorskip("polars")

from lakelogic import SQLiteCheckpointStore, StreamSink  # noqa: E402
from lakelogic.core import stream_sink  # noqa: E402
from lakelogic.core.stream_sink import ConfluentConsumer, KafkaOffsetSource, librdkafka_config  # noqa: E402


class _Msg:
    def __init__(self, topic, partition, offset, value, error=None):
        self._t, self._p, self._o, self._v, self._e = topic, partition, offset, value, error

    def topic(self):
        return self._t

    def partition(self):
        return self._p

    def offset(self):
        return self._o

    def value(self):
        return self._v

    def error(self):
        return self._e


class _Librdkafka:
    """The confluent_kafka.Consumer surface ConfluentConsumer uses, over fixed partitions."""

    def __init__(self, data, topic="t"):
        self.topic, self._data = topic, data  # {partition: [bytes, ...]}
        self._pos = {}
        self.assigns = []
        self.extra = []  # messages (e.g. errors) returned by the next consume()

    def list_topics(self, topic, timeout=None):
        parts = {p: object() for p in self._data}
        return SimpleNamespace(topics={topic: SimpleNamespace(error=None, partitions=parts)})

    def assign(self, ctps):
        self.assigns.append([(c.partition, c.offset) for c in ctps])
        for c in ctps:  # OFFSET_BEGINNING (-2) → offset 0
            self._pos[c.partition] = 0 if c.offset < 0 else c.offset

    def get_watermark_offsets(self, ctp, timeout=None):
        return 0, len(self._data[ctp.partition])

    def position(self, ctps):
        return [SimpleNamespace(offset=ck.OFFSET_INVALID) for _ in ctps]  # nothing fetched yet

    def consume(self, num_messages=1, timeout=None):
        out, self.extra = list(self.extra), []
        for p, vals in self._data.items():
            while self._pos.get(p, 0) < len(vals) and len(out) < num_messages:
                o = self._pos[p]
                out.append(_Msg(self.topic, p, o, vals[o]))
                self._pos[p] = o + 1
        return out

    def close(self):
        pass


def _rides(*ids):
    return [json.dumps({"ride_id": i, "status": "done"}).encode() for i in ids]


CONTRACT = {
    "version": "1.0.0",
    "info": {"title": "rides"},
    "primary_key": ["ride_id"],
    "model": {"fields": [{"name": "ride_id", "type": "long", "required": True}, {"name": "status", "type": "string"}]},
}


def _source(raw, **kw):
    return KafkaOffsetSource("t", consumer=ConfluentConsumer(consumer=raw), drain=True, **kw)


def test_kafka_python_keywords_become_librdkafka_properties():
    assert librdkafka_config(
        {
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_plain_username": "$ConnectionString",
            "sasl_plain_password": "secret",
            "fetch.min.bytes": 1024,
            "client_id": None,
        }
    ) == {
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "$ConnectionString",
        "sasl.password": "secret",
        "fetch.min.bytes": 1024,
    }


def test_drains_every_partition_to_the_end_offsets():
    raw = _Librdkafka({0: _rides(1, 2, 3), 1: _rides(4, 5)})
    src = _source(raw)
    events = list(src.stream())
    assert sorted(e["ride_id"] for e in events) == [1, 2, 3, 4, 5]
    assert src.current_cursor() == {"t:0": 3, "t:1": 2}


def test_resume_assigns_the_stored_offsets_once():
    """One assign, with every stored offset. Assigning per seek let the first assignment's fetch
    from offset 0 leak in: on a real broker a restart re-read the whole topic (2026-11-02)."""
    raw = _Librdkafka({0: _rides(1, 2, 3), 1: _rides(4, 5)})
    src = _source(raw)
    src.seek({"t:0": 2})  # partition 1 absent → left at auto_offset_reset
    assert sorted(e["ride_id"] for e in src.stream()) == [3, 4, 5]
    assert raw.assigns == [[(0, 2), (1, ck.OFFSET_BEGINNING)]]


def test_partition_eof_is_skipped_and_a_real_error_raises():
    raw = _Librdkafka({0: _rides(1)})
    eof = SimpleNamespace(code=lambda: ck.KafkaError._PARTITION_EOF)
    raw.extra = [_Msg("t", 0, -1, None, error=eof)]
    c = ConfluentConsumer(consumer=raw)
    c.assign([SimpleNamespace(topic="t", partition=0)])
    batch = c.poll(timeout_ms=10, max_records=10)
    assert [r.offset for recs in batch.values() for r in recs] == [0]

    raw.extra = [_Msg("t", 0, -1, None, error=ck.KafkaError(ck.KafkaError._TRANSPORT))]
    with pytest.raises(ck.KafkaException):
        c.poll(timeout_ms=10, max_records=10)


def test_position_before_the_first_fetch_falls_back_to_reset():
    raw = _Librdkafka({0: _rides(1, 2, 3)})
    tp = SimpleNamespace(topic="t", partition=0)
    assert ConfluentConsumer(consumer=raw, auto_offset_reset="earliest").position(tp) == 0
    assert ConfluentConsumer(consumer=raw, auto_offset_reset="latest").position(tp) == 3


def test_stream_sink_commits_and_a_restart_reads_only_new_messages(tmp_path):
    data = {0: _rides(1, 2, 3)}
    store = SQLiteCheckpointStore(tmp_path / "ckpt.sqlite")

    def run():
        sink = StreamSink(
            contract=CONTRACT,
            source=_source(_Librdkafka(data)),
            engine="polars",
            checkpoint=store,
            checkpoint_key="rides",
            batch_size=2,
        )
        sink.processor.materialize = lambda good, bad, target_path=None: None
        return sink.run("available_now")

    first = run()
    assert first.source_count == 3 and first.cursor == {"t:0": 3}

    data[0] += _rides(4)
    second = run()
    assert second.source_count == 1 and second.cursor == {"t:0": 4}


def test_confluent_is_preferred_when_installed(monkeypatch):
    built = {}

    class _Spy(ConfluentConsumer):
        def __init__(self, **kw):
            built.update(kw)
            super().__init__(consumer=_Librdkafka({0: []}))

    monkeypatch.setattr(stream_sink, "ConfluentConsumer", _Spy)
    KafkaOffsetSource("t", brokers="b:9092", group_id="g", security_protocol="SASL_SSL")
    assert built == {
        "brokers": "b:9092",
        "group_id": "g",
        "auto_offset_reset": "earliest",
        "security_protocol": "SASL_SSL",
    }


def test_kafka_python_is_the_fallback(monkeypatch):
    monkeypatch.setattr(stream_sink, "_confluent_available", lambda: False)
    made = {}

    class _KC:
        def __init__(self, **kw):
            made.update(kw)

    monkeypatch.setitem(__import__("sys").modules, "kafka", SimpleNamespace(KafkaConsumer=_KC, TopicPartition=tuple))
    KafkaOffsetSource("t", brokers="b:9092", group_id="g")
    assert made["enable_auto_commit"] is False and made["bootstrap_servers"] == "b:9092"
