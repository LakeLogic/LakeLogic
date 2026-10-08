"""Streaming scenarios against a REAL Kafka broker (Redpanda in Docker).

Each scenario creates its own topic, checkpoint and Delta target, produces events, runs
LakeLogic's StreamSink + KafkaOffsetSource, and checks the outcome: rows in bronze, rows
quarantined, events consumed, and no duplicates. Used by kafka_streaming.ipynb; also runs alone:

    docker run -d --name lakelogic-kafka-dev -p 19092:19092 docker.redpanda.com/redpandadata/redpanda:v24.2.7 \\
      redpanda start --mode dev-container --smp 1 --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092 \\
      --advertise-kafka-addr internal://localhost:9092,external://localhost:19092
    ENGINE=polars python kafka_scenarios.py          # or duckdb / spark
"""

from __future__ import annotations

import json
import os
import shutil
import uuid
from pathlib import Path

import deltalake
import polars as pl
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

from lakelogic import SQLiteCheckpointStore, StreamSink
from lakelogic.core.stream_sink import KafkaOffsetSource

BROKERS = os.environ.get("KAFKA_BROKERS", "localhost:19092")
ENGINE = os.environ.get("ENGINE", "polars")
WORK = Path(__file__).resolve().parent / "data" / "stream_runs" / ENGINE
RESULTS: list[dict] = []
_CREATED_TOPICS: list[str] = []

# Azure Event Hubs (or any SASL/PLAIN Kafka): set KAFKA_BROKERS=<namespace>.servicebus.windows.net:9093 and
# KAFKA_SASL_CONNECTION_STRING to the namespace connection string. Never written to a file.
_EH = os.environ.get("KAFKA_SASL_CONNECTION_STRING")
KAFKA_AUTH = (
    {
        "security_protocol": "SASL_SSL",
        "sasl_mechanism": "PLAIN",
        "sasl_plain_username": "$ConnectionString",
        "sasl_plain_password": _EH,
    }
    if _EH
    else {}
)


def contract(target: str, quarantine: str = "") -> dict:
    return {
        "version": "1.0.0",
        "dataset": "rides",
        "info": {"title": "bronze_rides"},
        "primary_key": ["ride_id"],
        "model": {
            "fields": [
                {"name": "ride_id", "type": "long", "required": True},
                {"name": "city", "type": "string", "required": True},
                {"name": "fare", "type": "double", "required": True},
                {"name": "status", "type": "string"},
            ]
        },
        "quality": {"row_rules": [{"name": "fare_not_negative", "sql": "fare >= 0"}]},
        "materialization": {"strategy": "merge", "format": "delta", "target_path": target},
        **({"quarantine": {"target": quarantine, "format": "delta"}} if quarantine else {}),
    }


def ride(i: int, **over) -> dict:
    return {
        "ride_id": i,
        "city": ["London", "Paris", "Lagos"][i % 3],
        "fare": round(5 + i % 40, 2),
        "status": "completed",
        **over,
    }


class Run:
    """One scenario's topic, checkpoint and Delta target."""

    def __init__(self, name: str, partitions: int = 3):
        self.topic = f"rides_{ENGINE}_{name}_{uuid.uuid4().hex[:6]}"
        self.dir = WORK / name
        shutil.rmtree(self.dir, ignore_errors=True)
        self.dir.mkdir(parents=True)
        self.target = str(self.dir / "bronze")
        self.quarantine_target = str(self.dir / "quarantine")
        admin = KafkaAdminClient(bootstrap_servers=BROKERS, **KAFKA_AUTH)
        admin.create_topics([NewTopic(self.topic, num_partitions=partitions, replication_factor=1)])
        admin.close()
        _CREATED_TOPICS.append(self.topic)
        self.producer = KafkaProducer(bootstrap_servers=BROKERS, **KAFKA_AUTH)

    def send(self, events, raw: bool = False):
        for e in events:
            self.producer.send(self.topic, e if raw else json.dumps(e).encode())
        self.producer.flush()

    def sink(self, key: str = "main", batch_size: int = 100, deserializer=None):
        source = KafkaOffsetSource(
            self.topic, brokers=BROKERS, value_deserializer=deserializer or json.loads, **KAFKA_AUTH
        )
        return StreamSink(
            contract=contract(self.target, self.quarantine_target),
            source=source,
            engine=ENGINE,
            checkpoint=SQLiteCheckpointStore(self.dir / "checkpoints.sqlite"),
            checkpoint_key=key,
            batch_size=batch_size,
            target_path=self.target,
        )

    def quarantine(self) -> pl.DataFrame:
        try:
            return pl.from_arrow(deltalake.DeltaTable(self.quarantine_target + "/rides").to_pyarrow_table())
        except Exception:
            return pl.DataFrame()

    def quarantine_reasons(self) -> list:
        """Every quarantine reason, read through Arrow (Polars can crash on a list column Spark wrote)."""
        try:
            col = deltalake.DeltaTable(self.quarantine_target + "/rides").to_pyarrow_table().column("_lakelogic_errors")
        except Exception:
            return []
        return [e for row in col.to_pylist() for e in (row or [])]

    def table(self) -> pl.DataFrame:
        try:
            return pl.from_arrow(deltalake.DeltaTable(self.target).to_pyarrow_table())
        except Exception:
            return pl.DataFrame()


def scenario(name, fn, **expect):
    """Run fn() -> dict of measured values; compare with `expect` (PASS/FAIL)."""
    try:
        got = fn()
        problems = [f"{k}: expected {v}, got {got.get(k)}" for k, v in expect.items() if got.get(k) != v]
        outcome = ", ".join(f"{k}={v}" for k, v in got.items())
    except Exception as exc:  # noqa: BLE001 - a scenario may surface a real failure
        problems, outcome = [f"{type(exc).__name__}: {str(exc).splitlines()[0][:160]}"], "error"
    _drop_topics()
    status = "PASS" if not problems else "FAIL"
    RESULTS.append({"scenario": name, "status": status, "result": outcome, "why": "; ".join(problems)})
    print(f"[{status}] {name:<58} {outcome}" + (f"  <- {'; '.join(problems)}" if problems else ""))


def _drop_topics() -> None:
    """Delete this scenario's topics (Event Hubs Standard allows only 10 per namespace)."""
    if not _CREATED_TOPICS:
        return
    try:
        admin = KafkaAdminClient(bootstrap_servers=BROKERS, **KAFKA_AUTH)
        admin.delete_topics(list(_CREATED_TOPICS))
        admin.close()
    except Exception as exc:  # noqa: BLE001 - cleanup must not hide the scenario's result
        # kafka-python's admin delete can fail on Event Hubs (a socket error on Windows); fall back to
        # the Azure CLI when EH_RESOURCE_GROUP and EH_NAMESPACE say where the hubs live.
        rg, ns = os.environ.get("EH_RESOURCE_GROUP"), os.environ.get("EH_NAMESPACE")
        if rg and ns:
            import shutil as _sh
            import subprocess

            az = _sh.which("az") or _sh.which("az.cmd")
            for t in _CREATED_TOPICS:
                subprocess.run(
                    [az, "eventhubs", "eventhub", "delete", "-g", rg, "--namespace-name", ns, "-n", t],
                    capture_output=True,
                    check=False,
                )
        else:
            print(f"  (could not delete topics {_CREATED_TOPICS}: {exc})")
    _CREATED_TOPICS.clear()


def table_has_no_duplicates(t: pl.DataFrame) -> bool:
    return t.height == 0 or t["ride_id"].n_unique() == t.height


# ── scenarios ────────────────────────────────────────────────────────────────


def drain_with_bad_events():
    r = Run("drain")
    good = [ride(i) for i in range(500)]
    bad = [ride(1000 + i, fare=-5.0) for i in range(20)] + [{"city": "Rome", "fare": 9.0} for _ in range(10)]
    r.send(good + bad)
    s = r.sink().run("available_now")
    t = r.table()
    return {
        "consumed": s.source_count,
        "good": s.good_count,
        "quarantined": s.bad_count,
        "rows_in_bronze": t.height,
        "rows_in_quarantine": r.quarantine().height,
    }


def resume_reads_only_new_events():
    r = Run("resume")
    r.send([ride(i) for i in range(200)])
    first = r.sink().run("available_now")
    r.send([ride(i) for i in range(200, 260)])
    second = r.sink().run("available_now")
    third = r.sink().run("available_now")  # nothing new: must read nothing
    return {
        "first_run": first.source_count,
        "second_run": second.source_count,
        "third_run": third.source_count,
        "rows_in_bronze": r.table().height,
    }


def crash_after_one_batch_then_resume():
    r = Run("crash")
    r.send([ride(i) for i in range(300)])
    r.sink(batch_size=100).run("continuous", max_batches=1)  # stops after one committed batch
    rest = r.sink(batch_size=100).run("available_now")
    t = r.table()
    return {
        "resumed_and_read": rest.source_count,
        "rows_in_bronze": t.height,
        "no_duplicates": table_has_no_duplicates(t),
    }


def crash_before_checkpoint_commit():
    r = Run("crash_commit")
    r.send([ride(i) for i in range(300)])
    sink = r.sink(batch_size=100)
    real_commit, calls = sink.checkpoint.commit, {"n": 0}

    def flaky_commit(key, cp):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("simulated crash after the write, before the checkpoint commit")
        return real_commit(key, cp)

    sink.checkpoint.commit = flaky_commit
    try:
        sink.run("available_now")
    except RuntimeError:
        pass
    again = r.sink(batch_size=100).run("available_now")  # at-least-once: batch 2 is read again
    t = r.table()
    return {
        "reread_after_crash": again.source_count,
        "rows_in_bronze": t.height,
        "no_duplicates": table_has_no_duplicates(t),
    }


def replay_from_the_beginning_is_idempotent():
    r = Run("replay")
    r.send([ride(i) for i in range(150)])
    r.sink(key="first").run("available_now")
    replay = r.sink(key="replay_everything").run("available_now")  # new key: re-reads the topic
    t = r.table()
    return {"replayed": replay.source_count, "rows_in_bronze": t.height, "no_duplicates": table_has_no_duplicates(t)}


def a_message_that_is_not_json():
    r = Run("malformed")
    r.send(
        [json.dumps(ride(i)).encode() for i in range(50)]
        + [b"{not json", b"\xff\xfe"]
        + [json.dumps(ride(i)).encode() for i in range(50, 100)],
        raw=True,
    )
    s = r.sink().run("available_now")
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


def wrong_types_and_new_fields():
    r = Run("drift")
    r.send(
        [ride(i) for i in range(40)]
        + [ride(100 + i, fare="abc") for i in range(5)]
        + [ride(200 + i, coupon="SAVE10") for i in range(5)]
    )
    s = r.sink().run("available_now")
    return {"good": s.good_count, "quarantined": s.bad_count, "rows_in_bronze": r.table().height}


def the_same_ride_updated_in_one_batch():
    r = Run("updates", partitions=1)  # one partition: the stream's order is the event order
    r.send([ride(i) for i in range(30)] + [ride(i, status="refunded", fare=0.0) for i in range(10)])
    s = r.sink(batch_size=1000).run("available_now")
    t = r.table()
    refunded = t.filter(pl.col("status") == "refunded").height if t.height else 0
    return {
        "consumed": s.source_count,
        "superseded": s.superseded_count,
        "rows_in_bronze": t.height,
        "refunded": refunded,
        "no_duplicates": table_has_no_duplicates(t),
    }


ALL = [
    (
        "drain: 500 good, 20 negative fares, 10 missing ride_id",
        drain_with_bad_events,
        dict(consumed=530, good=500, quarantined=30, rows_in_bronze=500, rows_in_quarantine=30),
    ),
    (
        "resume: a later run reads only the new events",
        resume_reads_only_new_events,
        dict(first_run=200, second_run=60, third_run=0, rows_in_bronze=260),
    ),
    (
        "crash after one batch, then resume",
        crash_after_one_batch_then_resume,
        dict(resumed_and_read=200, rows_in_bronze=300, no_duplicates=True),
    ),
    (
        "crash between the write and the checkpoint commit",
        crash_before_checkpoint_commit,
        dict(reread_after_crash=200, rows_in_bronze=300, no_duplicates=True),
    ),
    (
        "replay the whole topic: merge keeps one row per ride",
        replay_from_the_beginning_is_idempotent,
        dict(replayed=150, rows_in_bronze=150, no_duplicates=True),
    ),
    (
        "two messages are not JSON",
        a_message_that_is_not_json,
        dict(
            consumed=102, good=100, quarantined=2, rows_in_bronze=100, rows_in_quarantine=2, reason_says_not_json=True
        ),
    ),
    (
        "wrong type in a field; a new field appears",
        wrong_types_and_new_fields,
        dict(good=45, quarantined=5, rows_in_bronze=45),
    ),
    (
        "the same ride updated later in the same batch",
        the_same_ride_updated_in_one_batch,
        dict(consumed=40, superseded=10, rows_in_bronze=30, refunded=10, no_duplicates=True),
    ),
]


def summary() -> pl.DataFrame:
    df = pl.DataFrame(RESULTS)
    print(f"\n{(df['status'] == 'PASS').sum()} of {len(df)} streaming scenarios behaved as expected ({ENGINE}).")
    return df


if __name__ == "__main__":
    from loguru import logger

    logger.remove()
    if ENGINE == "spark":  # one local session with Delta, shared by every scenario
        from delta import configure_spark_with_delta_pip
        from pyspark.sql import SparkSession

        builder = (
            SparkSession.builder.master("local[2]")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        )
        configure_spark_with_delta_pip(builder).getOrCreate().sparkContext.setLogLevel("ERROR")
    import sys

    wanted = sys.argv[1:]  # e.g. `python kafka_scenarios.py json` runs scenarios whose name contains "json"
    for name, fn, expect in ALL:
        if not wanted or any(w in name for w in wanted):
            scenario(name, fn, **expect)
    summary()


# ── helpers for the contract-file section of the notebooks ──────────────────────


def reset_topic(topic: str, partitions: int = 3, auth: dict | None = None, brokers: str | None = None) -> None:
    """Delete and recreate a topic (Kafka). On Event Hubs the hub is created in Azure instead."""
    import time

    admin = KafkaAdminClient(bootstrap_servers=brokers or BROKERS, **(auth or {}))
    try:
        admin.delete_topics([topic])
        time.sleep(2)
    except Exception:  # noqa: BLE001 - not there yet
        pass
    admin.create_topics([NewTopic(topic, partitions, 1)])
    admin.close()


def produce(topic: str, events, auth: dict | None = None, brokers: str | None = None) -> None:
    """Send dicts as JSON (bytes are sent as they are)."""
    p = KafkaProducer(bootstrap_servers=brokers or BROKERS, **(auth or {}))
    for e in events:
        p.send(topic, e if isinstance(e, bytes) else json.dumps(e).encode())
    p.flush()
    p.close()
