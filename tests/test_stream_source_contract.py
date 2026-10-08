"""A Kafka / Event Hubs source declared in the contract (``source.type: stream``, ``options.kind``).

Before 2026-10-08 a broker could only be wired in Python: a ``stream`` contract was read as a
folder of files, so the broker, topic, offsets and SASL login had no place in the contract.
"""

from pathlib import Path
from types import SimpleNamespace

import pytest

from lakelogic.core.processor import _kafka_stream_settings, _sqlite_absolute, _stream_kind


def _src(**options):
    return SimpleNamespace(type="stream", path=None, options=options)


def test_only_a_stream_with_a_broker_kind_is_read_from_a_broker():
    assert _stream_kind(_src(kind="Kafka")) == "kafka"
    assert _stream_kind(_src()) is None  # a folder of files, as before
    assert _stream_kind(SimpleNamespace(type="landing", options={"kind": "kafka"})) is None


def test_kafka_settings_resolve_the_brokers_and_password_from_the_environment(monkeypatch):
    monkeypatch.setenv("KB", "broker:9092")
    monkeypatch.setenv("KP", "s3cret")
    cfg = _kafka_stream_settings(
        _src(kind="kafka", brokers="env:KB", topic="rides", security_protocol="SASL_SSL",
             sasl_mechanism="PLAIN", sasl_username="u", sasl_password="env:KP", batch_size=50)
    )
    assert cfg["brokers"] == "broker:9092" and cfg["topic"] == "rides" and cfg["batch_size"] == 50
    assert cfg["auth"] == {"security_protocol": "SASL_SSL", "sasl_mechanism": "PLAIN",
                           "sasl_username": "u", "sasl_password": "s3cret"}


def test_a_literal_password_in_the_contract_is_refused():
    with pytest.raises(ValueError, match="environment variable"):
        _kafka_stream_settings(_src(kind="kafka", brokers="b:9092", topic="t", sasl_password="s3cret"))


def test_an_unset_secret_variable_is_named(monkeypatch):
    monkeypatch.delenv("NOPE", raising=False)
    with pytest.raises(ValueError, match="not set"):
        _kafka_stream_settings(_src(kind="kafka", brokers="b:9092", topic="t", sasl_password="env:NOPE"))


def test_eventhubs_needs_only_the_connection_string(monkeypatch):
    cs = "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=k;SharedAccessKey=x"
    monkeypatch.setenv("EH", cs)
    cfg = _kafka_stream_settings(_src(kind="eventhubs", connection_string="env:EH", topic="rides"))
    assert cfg["brokers"] == "ns.servicebus.windows.net:9093"
    assert cfg["auth"]["sasl_username"] == "$ConnectionString" and cfg["auth"]["sasl_password"] == cs


def test_a_relative_sqlite_uri_is_made_absolute_for_every_reader():
    # SQLAlchemy reads sqlite:///data/x.db as relative; ConnectorX read it as /data/x.db.
    assert _sqlite_absolute("sqlite:///data/x.db") == "sqlite:///" + Path("data/x.db").resolve().as_posix()
    for uri in ("sqlite:////abs/x.db", "sqlite:///C:/x.db", "postgresql://h/db", None):
        assert _sqlite_absolute(uri) == uri
