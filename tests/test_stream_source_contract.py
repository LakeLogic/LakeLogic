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
        _src(
            kind="kafka",
            brokers="env:KB",
            topic="rides",
            security_protocol="SASL_SSL",
            sasl_mechanism="PLAIN",
            sasl_username="u",
            sasl_password="env:KP",
            batch_size=50,
        )
    )
    assert cfg["brokers"] == "broker:9092" and cfg["topic"] == "rides" and cfg["batch_size"] == 50
    assert cfg["auth"] == {
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "PLAIN",
        "sasl_username": "u",
        "sasl_password": "s3cret",
    }


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


def test_the_sasl_login_class_is_shaded_on_databricks(monkeypatch):
    """Databricks ships Kafka under ``kafkashaded.``: the open-source class name does not load
    there, so an Event Hubs stream could never log in (found before the first Databricks run,
    2026-10-26)."""
    from lakelogic.core.processor import _plain_login_module

    monkeypatch.delenv("DATABRICKS_RUNTIME_VERSION", raising=False)
    assert _plain_login_module() == "org.apache.kafka.common.security.plain.PlainLoginModule"
    monkeypatch.setenv("DATABRICKS_RUNTIME_VERSION", "client.2.1")
    assert _plain_login_module() == "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule"


def test_a_type_stream_contract_is_classed_as_streaming():
    """Core's own stream type was missing from the lint's list, and its trigger is written under
    ``source.options``: a real Event Hubs contract read as batch (2026-10-26)."""
    from lakelogic.core.contract_lint import _is_resumable_stream

    doc = {"source": {"type": "stream", "path": "rides", "options": {"kind": "eventhubs", "trigger": "available_now"}}}
    assert _is_resumable_stream(doc)
    assert _is_resumable_stream({"source": {"type": "landing", "options": {"trigger": "available_now"}}})
    assert not _is_resumable_stream({"source": {"type": "landing", "path": "x"}})


def test_a_placeholder_inside_a_placeholder_value_resolves():
    """`ls_folder: landing_{domain}` resolved only when `domain` came after `ls_folder` in the
    map, so a landing path kept a literal `{domain}` (RideFlow on Databricks, 2026-10-26)."""
    from lakelogic.core.registry import _resolve_placeholders

    path = "abfss://landing@acct.dfs.core.windows.net/{ls_folder}/{system}"
    for order in (
        {"domain": "business", "ls_folder": "landing_{domain}", "system": "salesforce"},
        {"ls_folder": "landing_{domain}", "system": "salesforce", "domain": "business"},
    ):
        assert _resolve_placeholders(path, order).endswith("/landing_business/salesforce")
    assert _resolve_placeholders("{a}", {"a": "{a}"}) == "{a}"  # a self-reference stops


def test_a_delta_stream_is_read_incrementally_where_there_is_no_streaming():
    """Silver over a streaming Bronze (``kind: delta``): Polars / DuckDB read the same table
    incrementally, by the load time Bronze stamps (2026-10-27)."""
    from lakelogic.core.models import SourceConfig as Source
    from lakelogic.core.processor import _delta_stream_as_incremental_table

    src = Source(type="stream", path="cat.dom.bronze_events", options={"kind": "delta"})
    out = _delta_stream_as_incremental_table(src)
    assert out.type == "table" and out.path == "table:cat.dom.bronze_events"
    assert out.load_mode == "incremental" and out.watermark_field == "_lakelogic_processed_at"


def test_a_delta_stream_on_spark_reads_the_table_with_its_own_checkpoint(monkeypatch):
    import sys
    import types

    from lakelogic.core import processor as p
    from lakelogic.core import stream_sink

    calls = {}

    class _Reader:
        def format(self, f):
            calls["format"] = f
            return self

        def table(self, t):
            calls["table"] = t
            return "STREAM"

    class _Session:
        readStream = _Reader()

        class builder:
            @staticmethod
            def getOrCreate():
                return _Session()

    fake_sql = types.ModuleType("pyspark.sql")
    fake_sql.SparkSession = _Session
    monkeypatch.setitem(sys.modules, "pyspark", types.ModuleType("pyspark"))
    monkeypatch.setitem(sys.modules, "pyspark.sql", fake_sql)

    class _Sink:
        def __init__(self, **kw):
            calls["sink"] = kw
            self.batches = []

        def run(self):
            calls["ran"] = True

    monkeypatch.setattr(stream_sink, "SparkStreamSink", _Sink)
    proc = SimpleNamespace(
        contract=SimpleNamespace(
            dataset="silver_events",
            info=None,
            source=SimpleNamespace(
                type="stream",
                path="table:c.d.bronze_events",
                options={"kind": "delta", "checkpoint": "abfss://x/_checkpoints/silver_events"},
            ),
        ),
        _empty_frame=lambda: None,
        _quarantine_row_count=lambda: None,
        _pin_stream_run_id=lambda: None,
        _spark_stream_summary=lambda sink, query, q: None,
        _write_stream_run_log=lambda summary: None,
    )
    monkeypatch.setattr(p, "ValidationResult", lambda *a: SimpleNamespace())
    result = p.DataProcessor._run_delta_stream_source(proc)
    assert calls["format"] == "delta" and calls["table"] == "c.d.bronze_events" and calls["ran"]
    assert calls["sink"]["stream_df"] == "STREAM" and calls["sink"]["trigger"] == "available_now"
    assert calls["sink"]["checkpoint_location"] == "abfss://x/_checkpoints/silver_events"
    assert result.stream_summary["batches"] == 0


def test_a_stream_run_writes_one_run_log_row_with_its_totals(monkeypatch):
    """A streaming bronze/silver loaded and quarantined rows and the run log said nothing
    (Databricks, 2026-10-27): each stream run now writes its totals."""
    from lakelogic.core import processor as p

    written = []
    monkeypatch.setattr(p, "write_run_log", lambda report, contract, **kw: written.append(report))
    proc = SimpleNamespace(
        contract=SimpleNamespace(info=SimpleNamespace(title="silver_events"), dataset="silver_events"),
        engine_name="spark",
        _run_log_mode="table",
        _build_report=lambda title, counts: {"title": title, "counts": dict(counts), "status": "succeeded"},
    )
    proc._write_empty_run_log = lambda stage: written.append({"status": stage})
    p.DataProcessor._write_stream_run_log(
        proc, {"batches": 2, "source_count": 62, "good_count": 60, "bad_count": 1, "superseded_count": 1}
    )
    assert written[-1]["counts"] == {
        "source": 62,
        "total": 61,
        "good": 60,
        "quarantined": 1,
        "deduplicated": 1,
        "quarantine_ratio": 1 / 61,
    }
    p.DataProcessor._write_stream_run_log(proc, {"batches": 0, "source_count": 0})
    assert written[-1] == {"status": "no_new_data"}


def test_on_serverless_the_stream_totals_are_measured_on_the_driver():
    """foreachBatch runs in another Python process on serverless, so the sink's tallies stayed
    there: 288 rows loaded and the run log said 0 (Databricks, 2026-10-27)."""
    from lakelogic.core import processor as p

    counts = iter([10, 13])  # quarantine rows before, after
    proc = SimpleNamespace(_quarantine_row_count=lambda: next(counts), _stamped_stream_counts=lambda: None)
    q_before = proc._quarantine_row_count()
    query = SimpleNamespace(recentProgress=[{"numInputRows": 200}, {"numInputRows": 91}, {"numInputRows": 0}])
    summary = p.DataProcessor._spark_stream_summary(proc, SimpleNamespace(batches=[]), query, q_before)
    assert summary == {"batches": 2, "source_count": 291, "good_count": 288, "bad_count": 3, "superseded_count": None}
    # The sink kept its own tallies (classic compute, local Spark): those are used instead.
    assert p.DataProcessor._spark_stream_summary(proc, SimpleNamespace(batches=[1]), query, 0) is None


def test_a_stream_run_log_row_is_the_only_one():
    from lakelogic.core import processor as p

    proc = SimpleNamespace(_write_empty_run_log=lambda stage: None, _run_log_already_written=False)
    p.DataProcessor._write_stream_run_log(proc, {"batches": 0, "source_count": 0})
    assert proc._run_log_already_written is True  # the runner writes no second row


def test_spark_connect_progress_arrives_as_json_text():
    """Through Spark Connect `recentProgress` is JSON text: 20 events loaded, the run log said 0."""
    import json

    from lakelogic.core import processor as p

    counts = iter([0, 0])
    proc = SimpleNamespace(_quarantine_row_count=lambda: next(counts), _stamped_stream_counts=lambda: None)
    query = SimpleNamespace(recentProgress=[json.dumps({"numInputRows": 20}), "not json"])
    summary = p.DataProcessor._spark_stream_summary(proc, SimpleNamespace(batches=[]), query, 0)
    assert summary["source_count"] == 20 and summary["batches"] == 1


def test_when_no_batch_counts_were_recorded_the_run_is_counted_by_its_run_id():
    """The micro-batch's counts file was missing and the progress said 0: a run that loaded 8
    rows and quarantined 1 logged no_new_data (Databricks, 2026-10-27). The rows it wrote carry
    its run id, so those are counted."""
    from lakelogic.core import processor as p

    stamped = {
        "batches": 1,
        "source_count": 9,
        "good_count": 8,
        "bad_count": 1,
        "superseded_count": None,
        "counted_by": "run_id",
    }
    proc = SimpleNamespace(_quarantine_row_count=lambda: 0, _stamped_stream_counts=lambda: stamped)
    sink = SimpleNamespace(batches=[], driver_totals=lambda: {"batches": 0, "source_count": 0})
    query = SimpleNamespace(recentProgress=[{"numInputRows": 0}])
    assert p.DataProcessor._spark_stream_summary(proc, sink, query, 0) == stamped


def test_good_rows_are_never_negative():
    """Progress said 0 rows read while the quarantine grew by 1: good was logged as -1."""
    from lakelogic.core import processor as p

    counts = iter([4, 5])
    proc = SimpleNamespace(_quarantine_row_count=lambda: next(counts), _stamped_stream_counts=lambda: None)
    q_before = proc._quarantine_row_count()
    summary = p.DataProcessor._spark_stream_summary(
        proc, SimpleNamespace(batches=[]), SimpleNamespace(recentProgress=[{"numInputRows": 0}]), q_before
    )
    assert summary["good_count"] is None and summary["bad_count"] == 1


def test_a_stream_run_pins_one_run_id_before_it_starts():
    from lakelogic.core import processor as p

    proc = SimpleNamespace(last_run_id=None)
    p.DataProcessor._pin_stream_run_id(proc)
    first = proc.last_run_id
    assert first
    p.DataProcessor._pin_stream_run_id(proc)
    assert proc.last_run_id != first  # each stream run gets its own


def test_every_micro_batch_stamps_the_run_id_the_driver_pinned():
    """The sink cleared the id at each micro-batch, so rows carried an id the driver never knew
    and a run that loaded 6 rows logged 0 (Databricks, 2026-10-27)."""
    from lakelogic.core.stream_sink import SparkStreamSink

    seen = []

    class _Proc:
        last_run_id = "pinned"
        contract = None

        def run(self, df):
            seen.append(self.last_run_id)
            return SimpleNamespace(good=df, bad=None, source_count=1, good_count=1, bad_count=0)

        def materialize(self, *a, **k):
            return None

    sink = SparkStreamSink(stream_df=None, checkpoint_location="ckpt", processor=_Proc())
    sink.stream_run_id = sink.processor.last_run_id  # what run() captures before start
    sink.process_micro_batch([], 0)
    sink.process_micro_batch([], 1)
    assert seen == ["pinned", "pinned"]

    unpinned = SparkStreamSink(stream_df=None, checkpoint_location="ckpt", processor=_Proc())
    unpinned.process_micro_batch([], 0)
    assert seen[-1] is None  # no pin: each batch makes its own id, as before


def _kinesis_source(**opts):
    return SimpleNamespace(
        type="stream", path="trip_requests", options={"kind": "kinesis", "region": "eu-west-1", **opts}
    )


def test_kinesis_settings_take_secrets_from_the_environment_only(monkeypatch):
    from lakelogic.core.processor import _kinesis_stream_settings

    monkeypatch.setenv("KINESIS_KEY_ID", "AKIA_TEST")
    monkeypatch.setenv("KINESIS_SECRET", "s3cret")
    cfg = _kinesis_stream_settings(
        _kinesis_source(access_key_id="env:KINESIS_KEY_ID", secret_access_key="env:KINESIS_SECRET")
    )
    assert cfg["stream"] == "trip_requests" and cfg["region"] == "eu-west-1"
    assert cfg["auth"] == {"awsAccessKey": "AKIA_TEST", "awsSecretKey": "s3cret"}
    assert cfg["starting_position"] == "earliest" and cfg["trigger"] == "available_now"
    with pytest.raises(ValueError, match="environment variable"):
        _kinesis_stream_settings(_kinesis_source(access_key_id="AKIA_LITERAL", secret_access_key="x"))
    with pytest.raises(ValueError, match="both"):
        _kinesis_stream_settings(_kinesis_source(access_key_id="env:KINESIS_KEY_ID"))
    with pytest.raises(ValueError, match="region"):
        _kinesis_stream_settings(SimpleNamespace(type="stream", path="t", options={"kind": "kinesis"}))
    # No keys: the compute's own identity (an instance profile or a role the cluster assumes).
    assert _kinesis_stream_settings(_kinesis_source(role_arn="arn:aws:iam::1:role/r"))["auth"] == {
        "roleArn": "arn:aws:iam::1:role/r"
    }


def test_kinesis_says_plainly_it_needs_spark_on_databricks():
    from lakelogic.core import processor as p

    assert "kinesis" in p.CLOUD_STREAM_KINDS
    assert p._stream_kind(_kinesis_source()) == "kinesis"


def _pubsub_source(**opts):
    return SimpleNamespace(
        type="stream", path="trip_requests", options={"kind": "pubsub", "project": "rideflow-dev", **opts}
    )


def test_pubsub_settings_read_one_service_account_key_from_the_environment(monkeypatch):
    import json

    from lakelogic.core.processor import _pubsub_stream_settings

    key = {
        "type": "service_account",
        "client_id": "123",
        "client_email": "reader@p.iam.gserviceaccount.com",
        "private_key": "-----BEGIN PRIVATE KEY-----\nabc\n-----END PRIVATE KEY-----\n",
        "private_key_id": "k1",
    }
    monkeypatch.setenv("PUBSUB_KEY", json.dumps(key))
    cfg = _pubsub_stream_settings(_pubsub_source(credentials_json="env:PUBSUB_KEY"))
    assert cfg["subscription"] == "trip_requests" and cfg["topic"] == "trip_requests"
    assert cfg["project"] == "rideflow-dev"
    assert cfg["auth"] == {
        "clientId": "123",
        "clientEmail": "reader@p.iam.gserviceaccount.com",
        "privateKey": key["private_key"],
        "privateKeyId": "k1",
    }
    with pytest.raises(ValueError, match="environment variable"):
        _pubsub_stream_settings(_pubsub_source(credentials_json=json.dumps(key)))
    monkeypatch.setenv("PUBSUB_BAD", "{not json")
    with pytest.raises(ValueError, match="not a service account key"):
        _pubsub_stream_settings(_pubsub_source(credentials_json="env:PUBSUB_BAD"))
    monkeypatch.setenv("PUBSUB_PART", json.dumps({"client_email": "x"}))
    with pytest.raises(ValueError, match="private_key"):
        _pubsub_stream_settings(_pubsub_source(credentials_json="env:PUBSUB_PART"))
    with pytest.raises(ValueError, match="project"):
        _pubsub_stream_settings(SimpleNamespace(type="stream", path="t", options={"kind": "pubsub"}))
    assert "pubsub" in __import__("lakelogic.core.processor", fromlist=["x"]).CLOUD_STREAM_KINDS


def test_a_piped_secret_with_a_trailing_line_break_still_logs_in(monkeypatch):
    """A secret piped into a store ends with \r\n; the Kafka login string then failed to parse:
    "Value not specified for key ' password='" (2026-10-27)."""
    from lakelogic.core.processor import _jaas_quote, _kafka_stream_settings

    monkeypatch.setenv("K_USER", "lakelogic\r\n")
    monkeypatch.setenv("K_PASS", "s3cret\r\n")
    src = SimpleNamespace(
        type="stream",
        path="trips",
        options={
            "kind": "kafka",
            "brokers": "broker:9093",
            "security_protocol": "SASL_SSL",
            "sasl_mechanism": "PLAIN",
            "sasl_username": "env:K_USER",
            "sasl_password": "env:K_PASS",
        },
    )
    auth = _kafka_stream_settings(src)["auth"]
    assert auth["sasl_username"] == "lakelogic" and auth["sasl_password"] == "s3cret"
    # One quote and one backslash in, each escaped once in the JAAS string.
    assert _jaas_quote('p"a' + chr(92) + "ss") == "p" + chr(92) + '"a' + chr(92) * 2 + "ss"
