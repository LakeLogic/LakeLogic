"""Builds kafka_streaming.ipynb (run once; the notebook is committed)."""

import json
from pathlib import Path

cells = []


def md(t):
    cells.append({"cell_type": "markdown", "metadata": {}, "source": t})


def code(t):
    cells.append({"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [], "source": t})


md("""# Streaming from Kafka with LakeLogic

A real Kafka broker (Redpanda, in Docker), real events, and the cases that decide whether a stream can be trusted:

| Case | What must hold |
|---|---|
| Drain the topic | good rows land in bronze; bad rows land in quarantine **with the reason** |
| Run again later | only the **new** events are read |
| Crash mid-run, restart | no gap and no duplicate |
| Replay the whole topic | the `merge` target still has one row per ride |
| A message that is not JSON | it is quarantined; the stream does **not** stop |
| The same ride updated twice in one batch | the **last** version wins; the earlier one is counted as `superseded` |

**Start the broker** (once):

```
docker run -d --name lakelogic-kafka-dev -p 19092:19092 docker.redpanda.com/redpandadata/redpanda:v24.2.7 \\
  redpanda start --mode dev-container --smp 1 \\
  --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:19092 \\
  --advertise-kafka-addr internal://localhost:9092,external://localhost:19092
```

Needs `pip install "lakelogic[kafka]" deltalake`. Set `ENGINE` below to `polars`, `duckdb` or `spark`.""")

code("""import os
import sys
from pathlib import Path

LAKELOGIC_SRC = None   # a local lakelogic checkout, if you are not using the released package
if LAKELOGIC_SRC:
    sys.path.insert(0, LAKELOGIC_SRC)
os.environ.setdefault("ENGINE", "polars")      # polars | duckdb | spark
os.environ.setdefault("KAFKA_BROKERS", "localhost:19092")

import polars as pl
from loguru import logger
from kafka.admin import KafkaAdminClient

logger.remove()  # quiet; add a sink to watch the runs
pl.Config.set_tbl_rows(12)
pl.Config.set_fmt_str_lengths(90)
pl.Config.set_tbl_width_chars(200)

import kafka_scenarios as K   # the contract, the event generator and the checks live here

print("broker:", KafkaAdminClient(bootstrap_servers=K.BROKERS).list_topics()[:3], "... reachable")
print("engine:", K.ENGINE)""")

md("""## Run it from the contract file

Everything the run needs is in [`contracts/rides_kafka.yaml`](contracts/rides_kafka.yaml): the broker, the topic,
where the offsets are kept, the schema, the rule, and where good and bad rows go. No Python wiring:
`DataProcessor(contract=...).run_source()` (or the pipeline runner) drains the topic, writes every micro-batch,
and commits the offsets after each write.""")
code("""print(Path("contracts/rides_kafka.yaml").read_text())""")
code("""import shutil
from lakelogic import DataProcessor

for d in ("data/bronze", "data/quarantine", "data/checkpoints"):
    shutil.rmtree(d, ignore_errors=True)
K.reset_topic("rides")
K.produce("rides", [K.ride(i) for i in range(100)]
                   + [K.ride(900 + i, fare=-1.0) for i in range(5)]
                   + [b"not json"])

def run_contract(path="contracts/rides_kafka.yaml"):
    s = DataProcessor(engine=K.ENGINE, contract=path).run_source().stream_summary
    print(f"read {s['source_count']} events: {s['good_count']} good, {s['bad_count']} quarantined, "
          f"{s['superseded_count']} replaced by a later event for the same ride")

run_contract()                                       # 100 good; 5 negative fares + 1 non-JSON quarantined
K.produce("rides", [K.ride(i) for i in range(200, 210)])
run_contract()                                       # only the 10 new events
run_contract()                                       # nothing new""")

md("""### The same contract on Azure Event Hubs

[`contracts/rides_eventhubs.yaml`](contracts/rides_eventhubs.yaml) changes only the source: `kind: eventhubs` and
`connection_string: env:EVENTHUBS_CONNECTION_STRING`. The brokers and the SASL login come from that connection
string, which stays in the environment. Needs an event hub named `rides`. Skipped when the variable is not set.

An event hub keeps events for its retention period and cannot be emptied, so a fresh checkpoint also reads
events sent by earlier runs. The `merge` still leaves one row per ride: a ride sent twice is counted as replaced.""")
code("""print(Path("contracts/rides_eventhubs.yaml").read_text())""")
code("""cs = os.environ.get("EVENTHUBS_CONNECTION_STRING")
if cs:
    for d in ("data/bronze", "data/quarantine", "data/checkpoints"):
        shutil.rmtree(d, ignore_errors=True)
    eh = {"security_protocol": "SASL_SSL", "sasl_mechanism": "PLAIN",
          "sasl_plain_username": "$ConnectionString", "sasl_plain_password": cs}
    host = cs.split("sb://")[1].split("/")[0] + ":9093"
    K.produce("rides", [K.ride(i) for i in range(50)] + [K.ride(900, fare=-1.0)], auth=eh, brokers=host)
    run_contract("contracts/rides_eventhubs.yaml")
    run_contract("contracts/rides_eventhubs.yaml")    # nothing new
    import deltalake
    print("rows in bronze:", deltalake.DeltaTable("data/bronze").to_pyarrow_table().num_rows)
else:
    print("EVENTHUBS_CONNECTION_STRING not set — skipped")""")

md("""## Under the hood: the hard cases, step by step

The sections below drive the same machinery through the Python API (`StreamSink` + `KafkaOffsetSource`),
each on its own topic, to show the cases that decide whether a stream can be trusted.""")

md("""## 1. Drain the topic: good rows to bronze, bad rows to quarantine

500 good rides, 20 with a negative fare, 10 with no `ride_id`, sent to a topic with 3 partitions.
`available_now` reads everything that is there, then stops.""")
code("""r = K.Run("walkthrough")
r.send([K.ride(i) for i in range(500)]
       + [K.ride(1000 + i, fare=-5.0) for i in range(20)]
       + [{"city": "Rome", "fare": 9.0} for _ in range(10)])

s = r.sink().run("available_now")
print(f"read {s.source_count} events in {s.batches} batches: {s.good_count} good, {s.bad_count} quarantined")
print("checkpoint (next offset per partition):", s.cursor)
display(r.table().sort("ride_id").head(5))""")
code("""q = r.quarantine()
print(q.height, "rows in quarantine. Why:")
display(q.select("ride_id", "city", "fare", "_lakelogic_errors").head(6))""")

md("""## 2. Run again later: only the new events are read

The checkpoint stores the next offset of every partition, committed **after** each write.""")
code("""r.send([K.ride(i) for i in range(2000, 2060)])
s2 = r.sink().run("available_now")
print("second run read", s2.source_count, "events (the 60 new ones)")
s3 = r.sink().run("available_now")
print("third run read", s3.source_count, "events (nothing new)")
print("rows in bronze:", r.table().height)""")

md("""## 3. A message that is not JSON

One bad message used to stop the stream for good: it failed to decode, the run died before its checkpoint,
and every restart hit the same message again.
Now it is quarantined with the partition, the offset and its first bytes, and the stream carries on.""")
code("""r.send([b"{not json", b"\\xff\\xfe"], raw=True)
r.send([K.ride(i) for i in range(3000, 3010)])
s4 = r.sink().run("available_now")
print(f"read {s4.source_count}: {s4.good_count} good, {s4.bad_count} quarantined")
q = r.quarantine()
display(q.filter(pl.col("ride_id").is_null() & pl.col("city").is_null())
         .select(pl.col("_lakelogic_errors").list.first().alias("reason")))""")

md("""## 4. The same ride updated twice in one batch

Ride 3000 completes, then is refunded, and both events arrive in the same micro-batch.
The last event wins; the earlier one is counted as `superseded`, so the numbers still add up:
`read = good + quarantined + superseded`.""")
code("""r.send([K.ride(3000, status="refunded", fare=0.0)])   # 3000 was written above as completed
r.send([K.ride(4000), K.ride(4000, status="refunded", fare=0.0)])  # both in one batch
s5 = r.sink().run("available_now")
print(f"read {s5.source_count}: good {s5.good_count}, superseded {s5.superseded_count}")
display(r.table().filter(pl.col("ride_id").is_in([3000, 4000])))""")

md("""## 5. Replay everything: the merge keeps one row per ride

A new checkpoint key reads the topic from the start again.
Every event is processed a second time and the table does not grow.""")
code("""before = r.table().height
replay = r.sink(key="replay_everything").run("available_now")
after = r.table()
print(f"replayed {replay.source_count} events; bronze rows before {before}, after {after.height}")
print("one row per ride:", after["ride_id"].n_unique() == after.height)""")

md("""## Scenario suite

Every case above and the crash cases, each on its own topic, checked automatically.
Run it with `ENGINE=duckdb` or `ENGINE=spark` to see the same answers on the other engines.""")
code("""K.RESULTS.clear()
for name, fn, expect in K.ALL:
    K.scenario(name, fn, **expect)
display(K.summary().select("status", "scenario", "result", "why"))""")

nb = {
    "cells": cells,
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python"},
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}
for i, c in enumerate(nb["cells"]):
    c["id"] = f"c{i}"
Path(__file__).with_name("kafka_streaming.ipynb").write_text(json.dumps(nb, indent=1), encoding="utf-8")
print("wrote kafka_streaming.ipynb")


# ── The Spark Structured Streaming notebook ─────────────────────────────────────
cells = []
md("""# Spark Structured Streaming from Kafka with LakeLogic

The same broker, contract, events and checks as `kafka_streaming.ipynb`, but **Spark reads Kafka itself**:
`spark.readStream` pulls the topic, Spark's checkpoint folder tracks the offsets, and every micro-batch runs the
LakeLogic contract inside `foreachBatch` (`SparkStreamSink`). This is the path for Databricks and Fabric.

`kafka_json_stream(spark, topic, fields, brokers=...)` turns raw Kafka messages into rows for the contract:
- each message must be a JSON object; the contract's fields come out as text, so the engine's type checks decide;
- a message that is not a JSON object is **quarantined with the reason** (partition, offset, first bytes),
  where Spark's own `from_json` would quietly give a row of nulls;
- Kafka's timestamp, partition and offset are kept for ordering updates, then dropped.

Needs PySpark 3.5 with `delta-spark`; Spark downloads its Kafka connector on first run.""")
code("""import os
import sys
os.environ.setdefault("KAFKA_BROKERS", "localhost:19092")
from loguru import logger
import polars as pl

logger.remove()
pl.Config.set_tbl_rows(12)
pl.Config.set_fmt_str_lengths(90)

import structured_scenarios as SS   # sets ENGINE=spark; reuses kafka_scenarios (K)
K = SS.K
spark = SS.start_spark()
print("Spark", spark.version)""")
md("""## Run it from the contract file

The **same** [`contracts/rides_kafka.yaml`](contracts/rides_kafka.yaml) as the Polars/DuckDB notebook. On the Spark
engine `run_source()` uses Structured Streaming (`readStream` + `foreachBatch`), and Spark keeps its own offsets
in a checkpoint folder beside the one named in the contract (`rides_kafka_spark/`).""")
code("""from pathlib import Path
print(Path("contracts/rides_kafka.yaml").read_text())""")
code("""import shutil
from lakelogic import DataProcessor

for d in ("data/bronze", "data/quarantine", "data/checkpoints"):
    shutil.rmtree(d, ignore_errors=True)
K.reset_topic("rides")
K.produce("rides", [K.ride(i) for i in range(100)]
                   + [K.ride(900 + i, fare=-1.0) for i in range(5)]
                   + [b"not json"])

def run_contract(path="contracts/rides_kafka.yaml"):
    s = DataProcessor(engine="spark", contract=path).run_source().stream_summary
    print(f"read {s['source_count']} events in {s['batches']} micro-batch(es): "
          f"{s['good_count']} good, {s['bad_count']} quarantined")

run_contract()                                       # 100 good; 5 negative fares + 1 non-JSON quarantined
K.produce("rides", [K.ride(i) for i in range(200, 210)])
run_contract()                                       # only the 10 new events
run_contract()                                       # nothing new""")

md("""## Under the hood: the hard cases, step by step""")

md("""## 1. Drain the topic with Structured Streaming

The `available_now` trigger reads everything in the topic, in micro-batches, then stops.""")
code("""r = K.Run("nb_structured")
r.send([K.ride(i) for i in range(500)]
       + [K.ride(1000 + i, fare=-5.0) for i in range(20)]
       + [{"city": "Rome", "fare": 9.0} for _ in range(10)])
r.send([b"{not json"], raw=True)

s = SS.structured(r)
print(f"read {s.source_count} in {s.batches} micro-batch(es): {s.good_count} good, {s.bad_count} quarantined")
display(r.table().sort("ride_id").head(5))
print("quarantine reasons (first of each kind):")
for reason in sorted({x.split(' (')[0] for x in r.quarantine_reasons()}):
    print(" -", reason)""")
md("""## 2. Run again: Spark's checkpoint reads only the new events""")
code("""r.send([K.ride(i) for i in range(2000, 2060)])
again = SS.structured(r)
print("second run read", again.source_count, "events; bronze rows:", r.table().height)""")
md("""## 3. A crash inside a micro-batch

The second micro-batch fails after its write. Spark does not commit it, so the next run reads it again,
and the `merge` keeps one row per ride.""")
code("""c = K.Run("nb_structured_crash")
c.send([K.ride(i) for i in range(300)])
crashed = SS.structured(c, per_batch=100, fail_in_batch=(2, "after"))
rest = SS.structured(c, per_batch=100)
t = c.table()
print(f"committed before the crash {crashed.source_count}, read after restart {rest.source_count}")
print("rows in bronze", t.height, "| one row per ride:", K.table_has_no_duplicates(t))""")
md("""## 4. The same ride updated twice in one micro-batch: the last event wins""")
code("""u = K.Run("nb_structured_updates", partitions=1)
u.send([K.ride(i) for i in range(5)] + [K.ride(i, status="refunded", fare=0.0) for i in range(2)])
su = SS.structured(u)
print(f"read {su.source_count}: good {su.good_count}, superseded {su.superseded_count}")
display(u.table().sort("ride_id"))""")
md("""## Scenario suite (Structured Streaming)""")
code("""K.RESULTS.clear()
for name, fn, expect in SS.ALL:
    K.scenario(f"[structured] {name}", fn, **expect)
display(K.summary().select("status", "scenario", "result", "why"))""")
nb = {
    "cells": cells,
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python"},
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}
for i, c in enumerate(nb["cells"]):
    c["id"] = f"c{i}"
Path(__file__).with_name("spark_structured_streaming.ipynb").write_text(json.dumps(nb, indent=1), encoding="utf-8")
print("wrote spark_structured_streaming.ipynb")
