# Real-Time Streaming 🌊

LakeLogic provides a unified API for streaming data through the same contract-driven processing engine used for batch. This allows for seamless transitions between batch ELT and real-time streaming pipelines.

## 🚀 Overview

LakeLogic supports multiple streaming connectors including SSE, WebSocket, and major cloud messaging services.

| Connector | Support | Typical Use Cases |
|-----------|---------|-------------------|
| **SSE** | Native | Wikimedia, GitHub Events, Custom SSE streams |
| **WebSocket** | Native | Financial tickers (Coinbase, Binance), Real-time feeds |
| **Kafka** | Native | Apache Kafka, Confluent, AWS MSK, Azure Event Hubs |
| **Azure** | Native | Event Grid, Service Bus |
| **AWS** | Native | SQS (Simple Queue Service) |
| **GCP** | Native | Pub/Sub messaging |

## 📦 Installation

Streaming support is available as an extra:

```bash
# Install all streaming features
pip install "lakelogic[streaming]"

# Or install specific extras
pip install "lakelogic[kafka]"
pip install "lakelogic[sse]"
pip install "lakelogic[websocket]"
```

## 🔌 Connector Examples

All LakeLogic connectors follow a unified iterator-based API for clean, readable pipelines.

### SSE (Server-Sent Events)
```python
from lakelogic.engines.streaming_connectors import SSEConnector

# Connect to a public event stream
connector = SSEConnector("https://stream.wikimedia.org/v2/stream/recentchange")

for event in connector.stream():
    print(f"{event['type']} | {event['title']}")
```

### WebSocket (Financial Data)
```python
from lakelogic.engines.streaming_connectors import WebSocketConnector

connector = WebSocketConnector(
    url="wss://ws-feed.exchange.coinbase.com",
    subscribe_message={
        "type": "subscribe",
        "channels": [{"name": "ticker", "product_ids": ["BTC-USD"]}]
    }
)

for event in connector.stream():
    print(f"BTC Price: ${event.get('price')}")
```

### Apache Kafka
```python
from lakelogic.engines.streaming_connectors import KafkaConnector

connector = KafkaConnector(
    brokers=["localhost:9092"],
    topic="production_orders",
    consumer_group="lakelogic_processor"
)

for event in connector.stream():
    # LakeLogic automatically parses JSON messages
    print(event)
```

## ♻️ Resumable, Crash-Safe Ingestion

The connectors above **read** a stream. To **process** one safely — validating every
event against a contract and surviving a crash without losing or duplicating data —
use **`StreamSink`**.

Streaming's hard problem isn't reading; it's failure. A job dies mid-run and you
either **lose** the in-flight events or **replay** them and double-count. This isn't
a new problem — it's exactly what Spark Structured Streaming and Kafka consumer
offsets already solve with *commit-after-write checkpointing*. `StreamSink` brings
that same proven guarantee to **every** engine through **one contract**:

> The cursor (Kafka offset / SSE `Last-Event-ID` / watermark) is committed to a
> durable checkpoint **after** the batch is written — so a killed stream resumes from
> exactly where it stopped. Delivery is **at-least-once**; pair it with a `merge`
> contract for **effectively-once** (no duplicates).

```python
from lakelogic import StreamSink, SQLiteCheckpointStore

sink = StreamSink(
    "contracts/bronze_orders.yaml",   # the SAME contract you use for batch
    connector,                        # any connector above, or an iterable of dicts
    engine="polars",                  # or "duckdb" / "spark"
    checkpoint=SQLiteCheckpointStore("checkpoints.sqlite"),
    checkpoint_key="orders",
    batch_size=1000,
)

summary = sink.run("available_now")   # drain to the current end, then exit
print(summary.good_count, summary.bad_count, summary.cursor)
```

### Two lifetimes, one loop

| Mode | Behavior | Best for |
|------|----------|----------|
| `available_now` | Drain everything currently available, commit, and **exit** | Scheduled / serverless jobs (cheapest) |
| `continuous`    | Keep looping, blocking for new records | Always-on, sub-minute freshness SLOs |

Same code, same checkpoint, same "process each record once" — only the run lifetime
differs (mirrors Spark's `Trigger.AvailableNow` vs `Trigger.ProcessingTime`).

### Native offset resume (not a fragile row count)

Any source exposing `seek(cursor)` + `current_cursor()` resumes from its **own**
native position. Two reference sources ship today:

```python
from lakelogic import KafkaOffsetSource, SSEOffsetSource, WatermarkChunkSource

# Kafka: cursor is a per-partition broker offset — auto-commit off, LakeLogic commits
# after the write. On restart it seeks to the committed offset per partition.
src = KafkaOffsetSource("orders", brokers="broker:9092", group_id="lakelogic-bronze")

# SSE: cursor is the Last-Event-ID string; the feed replays after it on reconnect.
src = SSEOffsetSource("https://example.com/stream")

# Huge historical backfill, resumable + memory-bounded (keyset paging, one chunk at a
# time — the opposite of an all-or-nothing bulk read).
src = WatermarkChunkSource(fetch_chunk, watermark_field="id", chunk_size=1000)
```

### Effectively-once

Because delivery is at-least-once, a crash between *write* and *commit* replays the
in-flight batch on restart. A `merge` contract keyed on the primary key makes that
harmless — replayed rows update in place instead of duplicating:

```yaml
materialization:
  strategy: merge     # never bare `append` on a resumable source
primary_key: [order_id]
```

### Stateful layers on PySpark

Bronze ingest is single-node-friendly, but *stateful* transforms — windowed
aggregations, stream-stream joins, watermarked dedup — belong on Spark Structured
Streaming, which owns that state. **`SparkStreamSink`** runs the **same contract**
inside `writeStream.foreachBatch`, with Spark's own `checkpointLocation` as the
resumability source of truth. Runs on any PySpark platform (Databricks, EMR,
Dataproc, Synapse, or your own cluster):

```python
from lakelogic import SparkStreamSink

silver = spark.readStream.format("delta").load("lake/bronze_orders")
SparkStreamSink(
    "contracts/silver_orders.yaml",
    silver,
    checkpoint_location="/checkpoints/silver_orders",   # Spark owns resume
    trigger="available_now",                            # or "processing_time"
).run()
```

### Governance: catch the append-on-replay footgun

The deterministic contract lint (`lakelogic lint`, also wired into CI) flags unsafe
streaming contracts **before** they ship:

- **`STREAM-001`** — a resumable/streaming source with bare `append` will duplicate
  on replay → use `merge`/`overwrite`.
- **`STREAM-002`** — `trigger: continuous` implies an always-on cluster → confirm the
  freshness SLO justifies the cost.

> **Try it end-to-end:** the [Real-Time & Streaming example notebook](examples/09_streaming_realtime.ipynb)
> runs all of this — Kafka offset resume, SSE `Last-Event-ID`, watermark backfill,
> Spark `foreachBatch`, the lint, and an effectively-once replay proof — with **no
> Kafka, cloud, or cluster** required. See the full design in
> [`docs/specs/streaming-contracts.md`](https://github.com/lakelogic/LakeLogic/blob/main/docs/specs/streaming-contracts.md).

## 🔐 Authentication

Cloud connectors leverage automatic credential resolution, meaning most production environments require zero configuration:

| Provider | Authentication Method |
|----------|----------------------|
| **Azure** | `DefaultAzureCredential` (Azure AD) |
| **AWS** | IAM Roles (boto3 automatic) |
| **GCP** | Application Default Credentials (ADC) |

## 🧪 Testing with Live Data

For a list of public datasets you can use to test your streaming pipelines, see the [Public Streaming Sources](streaming_sources.md) guide.
