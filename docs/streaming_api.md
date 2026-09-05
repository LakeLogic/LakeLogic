# Streaming API Reference

API reference for LakeLogic's resumable, crash-safe streaming layer. For the
conceptual guide (what it does and why), see
[Real-Time Streaming](streaming.md); for a runnable, infra-free walkthrough see the
[Real-Time & Streaming example notebook](examples/09_streaming_realtime.ipynb).

All classes below are importable directly from the package:

```python
from lakelogic import (
    StreamSink,
    StreamRunSummary,
    Checkpoint,
    CheckpointStore,
    SQLiteCheckpointStore,
    KafkaOffsetSource,
    SSEOffsetSource,
    WatermarkChunkSource,
    SparkStreamSink,
    SparkBatchResult,
)
```

---

## `StreamSink`

Micro-batches a stream of event dicts through a contract, committing a durable
cursor **after** each write so a killed run resumes exactly where it stopped.
Engine-agnostic (`polars` / `duckdb` / `spark`) and memory-bounded (one batch
buffered at a time).

```python
StreamSink(
    contract=None,
    source=(),
    *,
    engine="polars",
    checkpoint=None,
    checkpoint_key=None,
    batch_size=1000,
    window_seconds=None,
    cursor_fn=None,
    target_path=None,
    processor=None,
)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `contract` | path / YAML / dict / `DataContract` | – | The data contract. Used to build a `DataProcessor` unless `processor` is supplied. |
| `source` | connector or iterable of dict | `()` | An object with `.stream() -> Iterator[dict]` (any streaming connector) **or** a plain iterable of event dicts. |
| `engine` | str | `"polars"` | Contract engine used for validation. |
| `checkpoint` | `CheckpointStore` | `SQLiteCheckpointStore()` | Durable cursor store (externalized state → serverless-safe). |
| `checkpoint_key` | str | contract dataset/title | Stable key identifying this `(source, contract)` stream. Resuming reads the cursor under this key. |
| `batch_size` | int | `1000` | Flush a micro-batch when it reaches this many records. Must be ≥ 1. |
| `window_seconds` | float | `None` | Also flush when this many seconds have elapsed since the batch opened (whichever comes first). |
| `cursor_fn` | `callable(event) -> cursor` | `None` | Extract a connector-native cursor from the **last** event of a batch. Omit to use a running record count (skip-resume for replayable sources). Ignored for offset-aware sources. |
| `target_path` | str / Path | `None` | Optional bronze target override passed to `materialize`. |
| `processor` | object | `None` | Inject a pre-built `DataProcessor` (mainly for reuse / tests). |

### `run(mode="available_now", *, max_batches=None) -> StreamRunSummary`

Drain or stream the source.

| Argument | Description |
|---|---|
| `mode="available_now"` | Drain everything currently available, commit, and **exit** (AvailableNow — ideal for scheduled / serverless compute). |
| `mode="continuous"` | Keep looping, blocking for new records (always-on compute). |
| `max_batches` | Optional cap on the number of micro-batches (mainly for `continuous` control / tests). |

> **Delivery is at-least-once.** The commit happens after the durable write, so a
> crash between write and commit reprocesses the in-flight batch on restart. Make
> replays idempotent with `materialization.strategy: merge` (+ `primary_key`) or
> `overwrite` — never bare `append` on a resumable source.

---

## `StreamRunSummary`

Returned by `StreamSink.run`.

| Field | Type | Description |
|---|---|---|
| `batches` | int | Micro-batches committed this run. |
| `source_count` | int | Events read. |
| `good_count` | int | Events that passed the contract. |
| `bad_count` | int | Events quarantined. |
| `cursor` | Any | The last committed cursor. |
| `resumed_from` | Any | The cursor this run resumed from (`None` on a first run). |
| `per_batch` | list[dict] | Per-batch detail: `batch_id`, `source`, `good`, `bad`, `cursor`. |

---

## Checkpoint stores

### `Checkpoint`

A committed position for one `(source, contract)` stream.

| Field | Type | Description |
|---|---|---|
| `cursor` | Any | Connector-native position (offset dict / event-id / count). |
| `batch_id` | int | Monotonic batch counter. |
| `committed_at` | str | UTC ISO timestamp of the commit. |
| `source_count` / `good_count` / `bad_count` | int | Row tallies for the committed batch. |

### `CheckpointStore` (abstract)

Durable cursor store. Implement to back checkpoints with your own store.

| Method | Description |
|---|---|
| `load(key) -> Checkpoint \| None` | Last committed checkpoint for `key`, or `None` on first run. |
| `commit(key, checkpoint)` | Upsert the checkpoint. Called **after** the durable write. |
| `close()` | Optional cleanup. |

### `SQLiteCheckpointStore`

Zero-dependency default (stdlib `sqlite3`); a `key → cursor` upsert table with
sub-millisecond commits.

```python
SQLiteCheckpointStore(path="logs/lakelogic_stream_checkpoints.sqlite")
```

The cursor is JSON-serialized, so dict / string / int cursors all round-trip. A
fresh store over the same file (a process restart) sees prior commits.

---

## Offset-aware sources

An **offset-aware source** exposes both:

- `seek(cursor)` — position the source at a stored cursor (resume), and
- `current_cursor()` — the source's position after the records yielded so far.

When `StreamSink` detects these, it drives resume/commit from the source's **native**
cursor instead of the count-skip fallback. Three reference implementations ship:

### `KafkaOffsetSource`

Kafka source with native, checkpoint-committed offsets. Auto-commit is **disabled**
so LakeLogic owns the commit. Cursor = `{"topic:partition": next_offset}`.

```python
KafkaOffsetSource(
    topic,
    *,
    brokers=None,
    group_id=None,
    auto_offset_reset="earliest",
    value_deserializer=None,
    poll_timeout_ms=1000,
    max_poll_records=500,
    drain=False,
    consumer=None,      # injectable (testing without a live broker)
    tp_factory=None,
    **kafka_config,
)
```

| Parameter | Default | Description |
|---|---|---|
| `topic` | – | Kafka topic. |
| `brokers` | `None` | Bootstrap servers (str or list). |
| `group_id` | `None` | Consumer group. |
| `auto_offset_reset` | `"earliest"` | Where a partition with no committed offset starts. |
| `value_deserializer` | `None` | `callable(bytes) -> value`. Defaults to JSON. |
| `drain` | `False` | Set automatically by `StreamSink`: `True` for `available_now` (stop on an empty poll), `False` for `continuous` (block). |
| `consumer` / `tp_factory` | `None` | Inject a consumer/topic-partition factory for testing. |

Requires `pip install "lakelogic[kafka]"` for the live path.

### `SSEOffsetSource`

Server-Sent-Events source with native `Last-Event-ID` resume. Cursor = the id of the
last event seen (a string). Events without an `id` don't advance the cursor (per the
SSE spec).

```python
SSEOffsetSource(
    url=None,
    *,
    connect=None,       # injectable: callable(last_event_id) -> iterable[event]
    id_field="id",
    data_field="data",
    drain=False,
    headers=None,
    value_deserializer=None,
)
```

Requires `pip install "lakelogic[sse]"` for the live path (`sseclient-py` + `requests`).

### `WatermarkChunkSource`

Keyset-paged source for large DB/file loads: resumable **and** memory-bounded
(one chunk at a time). Cursor = the last watermark value seen.

```python
WatermarkChunkSource(
    fetch_chunk,        # callable(after, limit) -> list[dict]
    *,
    watermark_field,
    chunk_size=1000,
    drain=False,
)
```

`fetch_chunk(after, limit)` returns up to `limit` rows with watermark strictly
greater than `after` (`after is None` on the first page), ordered ascending — back
it with a keyset SQL query (`WHERE wm > :after ORDER BY wm LIMIT :n`), a Delta
version scan, or a sorted file read.

---

## `SparkStreamSink`

Runs a LakeLogic contract inside Spark Structured Streaming via
`writeStream.foreachBatch` — the same contract engine on every micro-batch, with
**Spark's** `checkpointLocation` as the single resumability source of truth. Runs on
any PySpark platform (Databricks, EMR, Dataproc, Synapse, self-managed).

```python
SparkStreamSink(
    contract=None,
    stream_df=None,
    *,
    checkpoint_location=None,    # required
    engine="spark",
    trigger="available_now",     # or "processing_time"
    processing_time=None,        # e.g. "30 seconds" (required for processing_time)
    output_mode="append",        # "update" / "complete" for aggregations
    target_path=None,
    processor=None,
    on_batch=None,               # callable(SparkBatchResult) — per-window run-log/metrics hook
)
```

| Method | Description |
|---|---|
| `run(await_termination=None) -> StreamingQuery` | Start the query. Blocks by default for `available_now`; returns the live query for `processing_time`. Pass `await_termination` to override. |
| `process_micro_batch(batch_df, batch_id) -> SparkBatchResult` | The `foreachBatch` handler (validate + write one micro-batch). Spark supplies a deterministic `batch_id`, so an idempotent write yields effectively-once. |

### `SparkBatchResult`

Per-micro-batch outcome passed to `on_batch`: `batch_id`, `source_count`,
`good_count`, `bad_count`.

---

## Governance lint

The deterministic contract lint (`lakelogic lint`, also wired into CI) flags unsafe
streaming contracts:

| Check | Severity | Fires when | Fix |
|---|---|---|---|
| `STREAM-001` | warning | A resumable/streaming source uses bare `append` (at-least-once replays duplicate) | Use `merge` (+ `primary_key`) or `overwrite` |
| `STREAM-002` | info | `trigger: continuous` (implies an always-on cluster) | Prefer `available_now` unless the freshness SLO is sub-minute |

See the [Real-Time Streaming guide](streaming.md#governance-catch-the-append-on-replay-footgun)
and the full design in
[`docs/specs/streaming-contracts.md`](https://github.com/lakelogic/LakeLogic/blob/main/docs/specs/streaming-contracts.md).
