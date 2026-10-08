"""The streaming scenarios on Spark STRUCTURED STREAMING (SparkStreamSink + kafka_json_stream).

Same broker, contract, events and checks as kafka_scenarios.py; only the sink differs: Spark reads
Kafka itself (`spark.readStream`), Spark's checkpoint folder tracks the offsets, and each micro-batch
runs the contract through `foreachBatch`. Needs the Spark Kafka connector and Delta:

    python structured_scenarios.py            # ENGINE is always spark here
"""

# ruff: noqa: I001  (ENGINE must be set before kafka_scenarios is imported)
from __future__ import annotations

import os

os.environ["ENGINE"] = "spark"

import kafka_scenarios as K  # noqa: E402 - ENGINE must be set first
from lakelogic.core.stream_sink import SparkStreamSink, kafka_json_stream  # noqa: E402

FIELDS = ["ride_id", "city", "fare", "status"]
SPARK_KAFKA = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"


class Summary:
    """StreamSink-style totals over SparkStreamSink's per-batch results."""

    def __init__(self, sink):
        b = sink.batches
        self.batches = len(b)
        self.source_count = sum(x.source_count for x in b)
        self.good_count = sum(x.good_count for x in b)
        self.bad_count = sum(x.bad_count for x in b)
        self.superseded_count = sum(x.superseded_count for x in b)


def structured(r: K.Run, key: str = "main", per_batch: int | None = None, fail_in_batch=None):
    """Run one available_now drain of r's topic through SparkStreamSink; return a Summary.

    ``fail_in_batch=(n, "before"|"after")`` raises inside the n-th micro-batch, before or after
    its write — Spark then does not commit that batch, and the next run re-reads it.
    """
    from pyspark.sql import SparkSession

    spark = SparkSession.getActiveSession()
    opts = {"maxOffsetsPerTrigger": str(per_batch)} if per_batch else {}
    if K.KAFKA_AUTH:  # Azure Event Hubs: SASL_SSL / PLAIN with the namespace connection string
        opts.update(
            {
                "kafka.security.protocol": "SASL_SSL",
                "kafka.sasl.mechanism": "PLAIN",
                "kafka.sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required "
                f'username="$ConnectionString" password="{K.KAFKA_AUTH["sasl_plain_password"]}";',
            }
        )
    stream = kafka_json_stream(spark, r.topic, FIELDS, brokers=K.BROKERS, **opts)
    sink = SparkStreamSink(
        contract=K.contract(r.target, r.quarantine_target),
        stream_df=stream,
        checkpoint_location=str(r.dir / f"spark_checkpoint_{key}"),
        target_path=r.target,
    )
    if fail_in_batch:
        n, when = fail_in_batch
        real = sink.processor.materialize
        seen = {"i": 0}

        def flaky(good, bad, target_path=None):
            seen["i"] += 1
            if seen["i"] == n and when == "before":
                raise RuntimeError("simulated crash before the write")
            out = real(good, bad, target_path=target_path)
            if seen["i"] == n and when == "after":
                raise RuntimeError("simulated crash after the write, before Spark commits the batch")
            return out

        sink.processor.materialize = flaky
    try:
        sink.run()
    except Exception as exc:  # noqa: BLE001 - the simulated crash surfaces as a query failure
        if not fail_in_batch:
            raise
        del exc
    return Summary(sink)


def drain():
    r = K.Run("s_drain")
    r.send(
        [K.ride(i) for i in range(500)]
        + [K.ride(1000 + i, fare=-5.0) for i in range(20)]
        + [{"city": "Rome", "fare": 9.0} for _ in range(10)]
    )
    s = structured(r)
    return {
        "consumed": s.source_count,
        "good": s.good_count,
        "quarantined": s.bad_count,
        "rows_in_bronze": r.table().height,
        "rows_in_quarantine": r.quarantine().height,
    }


def resume():
    r = K.Run("s_resume")
    r.send([K.ride(i) for i in range(200)])
    a = structured(r)
    r.send([K.ride(i) for i in range(200, 260)])
    b = structured(r)
    c = structured(r)
    return {
        "first_run": a.source_count,
        "second_run": b.source_count,
        "third_run": c.source_count,
        "rows_in_bronze": r.table().height,
    }


def crash_before_write():
    r = K.Run("s_crash")
    r.send([K.ride(i) for i in range(300)])
    crashed = structured(r, per_batch=100, fail_in_batch=(2, "before"))
    rest = structured(r, per_batch=100)
    t = r.table()
    return {
        "committed_plus_reread": crashed.source_count + rest.source_count,
        "rows_in_bronze": t.height,
        "no_duplicates": K.table_has_no_duplicates(t),
    }


def crash_after_write():
    r = K.Run("s_crash_commit")
    r.send([K.ride(i) for i in range(300)])
    crashed = structured(r, per_batch=100, fail_in_batch=(2, "after"))
    again = structured(r, per_batch=100)
    t = r.table()
    return {
        "committed_plus_reread": crashed.source_count + again.source_count,
        "rows_in_bronze": t.height,
        "no_duplicates": K.table_has_no_duplicates(t),
    }


def replay():
    r = K.Run("s_replay")
    r.send([K.ride(i) for i in range(150)])
    structured(r, key="first")
    again = structured(r, key="replay_everything")
    t = r.table()
    return {"replayed": again.source_count, "rows_in_bronze": t.height, "no_duplicates": K.table_has_no_duplicates(t)}


def not_json():
    r = K.Run("s_malformed")
    r.send(
        [K.json.dumps(K.ride(i)).encode() for i in range(50)]
        + [b"{not json", b"\xff\xfe"]
        + [K.json.dumps(K.ride(i)).encode() for i in range(50, 100)],
        raw=True,
    )
    s = structured(r)
    q = r.quarantine()
    reasons = r.quarantine_reasons()
    return {
        "consumed": s.source_count,
        "good": s.good_count,
        "quarantined": s.bad_count,
        "rows_in_bronze": r.table().height,
        "rows_in_quarantine": q.height,
        "reason_says_not_json": sum("not valid JSON" in e for e in reasons) == 2,
    }


def drift():
    r = K.Run("s_drift")
    r.send(
        [K.ride(i) for i in range(40)]
        + [K.ride(100 + i, fare="abc") for i in range(5)]
        + [K.ride(200 + i, coupon="SAVE10") for i in range(5)]
    )
    s = structured(r)
    return {"good": s.good_count, "quarantined": s.bad_count, "rows_in_bronze": r.table().height}


def updates():
    r = K.Run("s_updates", partitions=1)
    r.send([K.ride(i) for i in range(30)] + [K.ride(i, status="refunded", fare=0.0) for i in range(10)])
    s = structured(r)
    t = r.table()
    refunded = t.filter(K.pl.col("status") == "refunded").height if t.height else 0
    return {
        "consumed": s.source_count,
        "superseded": s.superseded_count,
        "rows_in_bronze": t.height,
        "refunded": refunded,
        "no_duplicates": K.table_has_no_duplicates(t),
    }


ALL = [
    (
        "drain: 500 good, 20 negative fares, 10 missing ride_id",
        drain,
        dict(consumed=530, good=500, quarantined=30, rows_in_bronze=500, rows_in_quarantine=30),
    ),
    (
        "resume: a later run reads only the new events",
        resume,
        dict(first_run=200, second_run=60, third_run=0, rows_in_bronze=260),
    ),
    (
        "crash inside batch 2, before its write; then resume",
        crash_before_write,
        dict(committed_plus_reread=300, rows_in_bronze=300, no_duplicates=True),
    ),
    (
        "crash inside batch 2, after its write; then resume",
        crash_after_write,
        dict(committed_plus_reread=300, rows_in_bronze=300, no_duplicates=True),
    ),
    (
        "replay the whole topic: merge keeps one row per ride",
        replay,
        dict(replayed=150, rows_in_bronze=150, no_duplicates=True),
    ),
    (
        "two messages are not JSON",
        not_json,
        dict(
            consumed=102, good=100, quarantined=2, rows_in_bronze=100, rows_in_quarantine=2, reason_says_not_json=True
        ),
    ),
    ("wrong type in a field; a new field appears", drift, dict(good=45, quarantined=5, rows_in_bronze=45)),
    (
        "the same ride updated later in the same batch",
        updates,
        dict(consumed=40, superseded=10, rows_in_bronze=30, refunded=10, no_duplicates=True),
    ),
]


def start_spark():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder, extra_packages=[SPARK_KAFKA]).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


if __name__ == "__main__":
    import sys

    from loguru import logger

    logger.remove()
    start_spark()
    wanted = sys.argv[1:]
    for name, fn, expect in ALL:
        if not wanted or any(w in name for w in wanted):
            K.scenario(f"[structured] {name}", fn, **expect)
    K.summary()
