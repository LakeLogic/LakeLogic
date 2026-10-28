"""An Iceberg table read as a stream (``source.type: stream``, ``options.kind: iceberg``), on a
real local Spark + Iceberg session: the first run reads every appended row, the next only what was
appended since, by the checkpoint (2026-10-27).

Skipped when the Iceberg Spark runtime is not in the local Ivy cache (no download in tests).
"""

from pathlib import Path
from types import SimpleNamespace

import pytest

JAR = next(
    iter(sorted((Path.home() / ".ivy2.5.2" / "jars").glob("org.apache.iceberg_iceberg-spark-runtime-4.0_2.13-*.jar"))
         + sorted((Path.home() / ".ivy2" / "jars").glob("org.apache.iceberg_iceberg-spark-runtime-4.0_2.13-*.jar"))),
    None,
)


@pytest.fixture(scope="module")
def iceberg_spark(tmp_path_factory):
    pytest.importorskip("pyspark")
    if JAR is None:
        pytest.skip("Iceberg Spark runtime jar not in the local Ivy cache")
    from pyspark.sql import SparkSession

    active = SparkSession.getActiveSession()
    if active is not None:
        active.stop()
    warehouse = tmp_path_factory.mktemp("iceberg_wh").as_posix()
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("lakelogic-iceberg-stream")
        .config("spark.jars", str(JAR))
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "hadoop")
        .config("spark.sql.catalog.local.warehouse", warehouse)
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


class _Collect:
    """A processor stand-in: records each micro-batch's rows; writes nothing."""

    def __init__(self):
        self.ids = []

    def run(self, df):
        rows = [r["id"] for r in df.collect()]
        self.ids.extend(rows)
        return SimpleNamespace(good=df, bad=df.limit(0), source_count=len(rows),
                               good_count=len(rows), bad_count=0)

    def materialize(self, *a, **k):
        return None


def _stream_once(processor_cls, spark, checkpoint):
    from lakelogic.core import processor as p

    sink_proc = _Collect()
    proc = SimpleNamespace(
        contract=SimpleNamespace(
            dataset="silver_events", info=None,
            source=SimpleNamespace(type="stream", path="table:local.db.bronze_events",
                                   options={"kind": "iceberg", "checkpoint": checkpoint}),
        ),
        _empty_frame=lambda: None,
        _write_stream_run_log=lambda summary: None,
        run=sink_proc.run, materialize=sink_proc.materialize,
        _quarantine_row_count=lambda: None,
        _stamped_stream_counts=lambda: None,
        last_run_id=None,
    )
    proc._pin_stream_run_id = lambda: p.DataProcessor._pin_stream_run_id(proc)
    proc._spark_stream_summary = lambda sink, query, q: p.DataProcessor._spark_stream_summary(proc, sink, query, q)
    result = p.DataProcessor._run_delta_stream_source(proc)
    return sink_proc.ids, result.stream_summary


def test_an_iceberg_table_streams_and_resumes_from_its_checkpoint(iceberg_spark, tmp_path):
    from lakelogic.core.processor import DataProcessor

    spark = iceberg_spark
    spark.sql("CREATE TABLE local.db.bronze_events (id STRING, v INT) USING iceberg")
    spark.sql("INSERT INTO local.db.bronze_events VALUES ('a', 1), ('b', 2), ('c', 3)")
    checkpoint = (tmp_path / "ckpt").as_posix()

    first, summary = _stream_once(DataProcessor, spark, checkpoint)
    assert sorted(first) == ["a", "b", "c"]
    assert summary["source_count"] == 3

    spark.sql("INSERT INTO local.db.bronze_events VALUES ('d', 4), ('e', 5)")
    second, _ = _stream_once(DataProcessor, spark, checkpoint)
    assert sorted(second) == ["d", "e"]  # only what was appended since: the checkpoint held

    # Each micro-batch recorded its counts beside the checkpoint (what the driver reads back on
    # serverless, where foreachBatch runs in another process): 3 rows, then 2.
    recorded = spark.read.json(checkpoint + "/_lakelogic_batch_counts").groupBy("run_tag").sum("source_count").collect()
    assert sorted(r[1] for r in recorded) == [2, 3]


def test_a_stream_run_is_counted_by_its_run_id_in_the_tables_it_wrote(iceberg_spark):
    """Serverless lost the micro-batch counts; the rows a run wrote carry its run id, so the
    driver counts those: 2 good and 1 quarantined for run r1, none for a run that wrote nothing."""
    from lakelogic.core import processor as p

    spark = iceberg_spark
    spark.createDataFrame([("a", "r1"), ("b", "r1"), ("c", "r0")], ["id", "_lakelogic_run_id"])         .write.mode("overwrite").saveAsTable("local.db.run_events")
    spark.createDataFrame([("x", "r1"), ("y", "r0")], ["id", "_lakelogic_run_id"])         .write.mode("overwrite").saveAsTable("local.db.run_events_quarantine")
    proc = SimpleNamespace(
        last_run_id="r1",
        contract=SimpleNamespace(materialization=SimpleNamespace(target_path="table:local.db.run_events",
                                                                 format="iceberg"),
                                 quarantine=SimpleNamespace(target="table:local.db.run_events_quarantine"),
                                 effective_server=lambda: None),
    )
    proc._rows_with_run_id = lambda t: p.DataProcessor._rows_with_run_id(proc, t)
    got = p.DataProcessor._stamped_stream_counts(proc)
    assert got["good_count"] == 2 and got["bad_count"] == 1 and got["source_count"] == 3
    proc.last_run_id = "nothing-written"
    assert p.DataProcessor._stamped_stream_counts(proc)["source_count"] == 0


def test_a_landing_folder_streams_each_new_file_once(iceberg_spark, tmp_path):
    """`kind: files`: the first run reads every file in the folder, the next only the file that
    landed since, by the checkpoint; every column arrives as text (2026-10-27)."""
    from lakelogic.core import processor as p

    folder = tmp_path / "landing" / "trips"
    folder.mkdir(parents=True)
    # T2's address holds a line break inside quotes: still one row, its later columns intact.
    (folder / "f1.csv").write_text('id,address,fare\nT1,a,10.5\nT2,"14 Main St\nSpringfield",7\nT3,c,3\n',
                                   encoding="utf-8")
    checkpoint = (tmp_path / "ckpt").as_posix()

    frames = []

    class _Keep(_Collect):
        def run(self, df):
            frames.append(df.collect())
            return super().run(df)

    def run_once():
        seen = _Keep()
        proc = SimpleNamespace(
            contract=SimpleNamespace(
                dataset="bronze_trips", info=None,
                source=SimpleNamespace(type="stream", path=folder.as_posix(), format="csv",
                                       options={"kind": "files", "checkpoint": checkpoint}),
            ),
            _empty_frame=lambda: None,
            _write_stream_run_log=lambda summary: None,
            run=seen.run, materialize=seen.materialize,
            _quarantine_row_count=lambda: None,
            _stamped_stream_counts=lambda: None,
            last_run_id=None,
        )
        proc._pin_stream_run_id = lambda: p.DataProcessor._pin_stream_run_id(proc)
        proc._stream_checkpoint = lambda o, fb: p.DataProcessor._stream_checkpoint(proc, o, fb)
        proc._file_stream = lambda *a: p.DataProcessor._file_stream(proc, *a)
        proc._spark_stream_summary = lambda sink, query, q: p.DataProcessor._spark_stream_summary(proc, sink, query, q)
        result = p.DataProcessor._run_file_stream_source(proc)
        return seen.ids, result.stream_summary

    first, summary = run_once()
    assert sorted(first) == ["T1", "T2", "T3"]
    t2 = next(r for r in frames[0] if r["id"] == "T2")
    assert t2["fare"] == "7" and "Springfield" in t2["address"]  # not split at the line break
    assert t2["_source_file"].endswith("f1.csv")  # each row knows its file
    assert summary["source_count"] == 3

    (folder / "f2.csv").write_text("id,address,fare\nT4,d,12\nT5,e,9\n", encoding="utf-8")
    new, second = run_once()
    assert sorted(new) == ["T4", "T5"] and second["source_count"] == 2  # only the new file
    _, third = run_once()
    assert not third["source_count"]  # nothing new landed

    df = iceberg_spark.read.json(checkpoint + "/_lakelogic_batch_counts")
    assert df.count() >= 2


def test_a_streamed_rows_file_becomes_its_lakelogic_source(iceberg_spark):
    """A stream run has no single source path; each row's own file is written instead (2026-10-27)."""
    from lakelogic.core.lineage import INPUT_FILE_COLUMN, add_columns

    df = iceberg_spark.createDataFrame([("T1", "abfss://l@a/landing/f1.csv")], ["id", INPUT_FILE_COLUMN])
    out = add_columns(df, {"_lakelogic_source": None}, "spark")
    assert INPUT_FILE_COLUMN not in out.columns
    assert out.collect()[0]["_lakelogic_source"] == "abfss://l@a/landing/f1.csv"


def test_kinesis_records_parse_like_kafka_messages(iceberg_spark):
    """Kinesis's `data/approximateArrivalTimestamp/shardId/sequenceNumber` become the contract's
    fields, and a record that is not JSON is marked for quarantine with its shard and sequence."""
    import datetime as _dt

    from lakelogic.core.stream_sink import UNREADABLE_COLUMN, json_message_columns

    ts = _dt.datetime(2026, 10, 27, 12, 0, 0)
    raw = iceberg_spark.createDataFrame(
        [(bytearray(b'{"request_id": "R1", "fare": "9.5"}'), ts, "shardId-0", "100"),
         (bytearray(b"not json"), ts, "shardId-0", "101")],
        "data binary, approximateArrivalTimestamp timestamp, shardId string, sequenceNumber string")
    rows = json_message_columns(raw, ["request_id", "fare"], value="data", timestamp="approximateArrivalTimestamp",
                                partition="shardId", offset="sequenceNumber").collect()
    assert rows[0]["request_id"] == "R1" and rows[0]["fare"] == "9.5" and rows[0][UNREADABLE_COLUMN] is None
    assert rows[1]["request_id"] is None and "shardId-0" in rows[1][UNREADABLE_COLUMN]
    assert "101" in rows[1][UNREADABLE_COLUMN]


def test_pubsub_messages_parse_like_kafka_messages(iceberg_spark):
    """Pub/Sub's `payload` / `publishTimestampInMillis` / `messageId` become the contract's fields;
    a message that is not JSON is marked for quarantine with its subscription and message id."""
    from pyspark.sql import functions as F

    from lakelogic.core.stream_sink import UNREADABLE_COLUMN, json_message_columns

    raw = iceberg_spark.createDataFrame(
        [(bytearray(b'{"request_id": "R1", "fare": "9.5"}'), "1793120000000", "m-1"),
         (bytearray(b"not json"), "1793120000500", "m-2")],
        "payload binary, publishTimestampInMillis string, messageId string")
    raw = (raw.withColumn("__pubsub_published", F.timestamp_millis(F.col("publishTimestampInMillis").cast("long")))
              .withColumn("__pubsub_subscription", F.lit("trip_requests")))
    rows = json_message_columns(raw, ["request_id", "fare"], value="payload", timestamp="__pubsub_published",
                                partition="__pubsub_subscription", offset="messageId").collect()
    assert rows[0]["request_id"] == "R1" and rows[0][UNREADABLE_COLUMN] is None
    assert "trip_requests" in rows[1][UNREADABLE_COLUMN] and "m-2" in rows[1][UNREADABLE_COLUMN]
