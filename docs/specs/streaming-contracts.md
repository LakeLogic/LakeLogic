# Streaming & Resumable Ingestion Contracts

**Status:** MVP implemented (core loop + checkpoint); remainder proposed
**Scope:** LakeLogic OSS core — one contract vocabulary spanning batch, resumable
micro-batch (AvailableNow-style), and continuous streaming, plus the failure /
resumability model that the current `fetch_size` path lacks.
**Related:** `docs/streaming.md` (connectors), `docs/streaming_sources.md`,
`lakelogic/core/stream_sink.py` (**this MVP**), `lakelogic/core/incremental.py`,
`lakelogic/core/run_log.py`, `lakelogic/engines/streaming_connectors.py`,
`lakelogic/core/processor.py` (`run_source`, `fetch_size` path).

### Implementation status

**Built** (`lakelogic/core/stream_sink.py` + `contract_lint.py`;
`tests/test_stream_sink.py` 35 tests + `test_contract_lint.py` STREAM checks):
- `StreamSink` — micro-batch loop over any `.stream()` connector or iterable of
  event dicts; `available_now` (drain + exit) and `continuous` (`max_batches`-bounded)
  modes; engine-agnostic validation via `DataProcessor.run(df)`; write via
  `materialize`; **commit-after-write** → at-least-once; memory-bounded (one batch
  buffered). Exported from `lakelogic`.
- `CheckpointStore` (ABC) + `SQLiteCheckpointStore` — zero-dep stdlib SQLite,
  key→cursor upsert; externalized state → ephemeral/serverless-safe.
- **Offset-aware source protocol** (`seek(cursor)` + `current_cursor()`) with
  `KafkaOffsetSource` — auto-commit off; cursor = `{"topic:partition": next_offset}`;
  seek-to-committed-offsets on resume; StreamSink commits the broker cursor after
  the write. Verified against a fake broker: drain, resume-with-no-reprocess,
  consume-only-new-after-resume, crash-before-commit resume-from-offset (no gap/dup).
- **`SSEOffsetSource`** — second reference implementation of the offset-aware
  protocol; cursor = the SSE `Last-Event-ID` (a scalar string) armed on the next
  connect; events without an `id` don't advance the cursor (per the SSE spec).
  Verified against a fake feed: drain, resume-after-id-no-reprocess, consume-only-
  new-after-resume, id-less-event-no-advance, crash-before-commit resume-from-id.
- Resume verified by crash-injection for count-, offset- and Last-Event-ID cursors:
  no gap / no dup (with idempotent write); at-least-once reprocessing on
  commit-failure shown.
- **`SparkStreamSink`** — Spark Structured Streaming path for the stateful tier:
  runs the SAME contract engine (`DataProcessor.run` + `materialize`) inside
  `writeStream.foreachBatch`, with **Spark's** offset/commit log under
  `checkpointLocation` as the single resumability source of truth (shared-checkpoint
  path — no double-booked cursor). `available_now` (drain + block) and
  `processing_time` triggers; deterministic `batch_id` → effectively-once with an
  idempotent write. `on_batch` hook exposes per-micro-batch results for run-log /
  metrics emission. Verified with a fake Structured Streaming writer.
- **`WatermarkChunkSource`** — offset-aware keyset-paged source (cursor = last
  watermark value) delivering §15.3's intent: large DB/file loads become resumable
  + memory-bounded through the same micro-batch loop, *without* changing the batch
  `fetch_size` path. Third proof the offset-aware protocol generalizes.
- **Push-source guard** — `available_now` on a `continuous_only` source (raw
  WebSocket/Webhook, no snapshotable end, §6) fails fast instead of never
  terminating.
- **Lint rules (§9)** in `contract_lint.py`: `STREAM-001` warns on bare `append`
  for a streaming/resumable source (at-least-once replays duplicate); `STREAM-002`
  flags `trigger: continuous` as an always-on-cluster cost advisory.
- **Effectively-once** proven end-to-end: a real `merge` contract replayed over
  already-processed ids leaves the Delta target with no duplicates.

**Pending** (still proposed): SQS / Pub-Sub ack cursors (the offset-aware protocol
is in place — Kafka, SSE and watermark sources prove it generalizes across dict-,
scalar- and value-cursor sources; these just implement it). Everything else in the
spec is built and tested.

---

## 1. Problem & goals

LakeLogic's contract engine is engine-agnostic (Polars / DuckDB / Spark) but
**batch-only**: every engine assumes a materialized DataFrame and does
count/collect/aggregate passes. Streaming today is partial and fragmented:

- Connectors (`streaming_connectors.py`) are real transports that yield
  `Iterator[dict]`, but **nothing consumes them into a governed landing/bronze
  layer** — the one consumer (`core/streaming_processor.py`) is orphaned and its
  sinks are `StdOutSink`/CSV `TODO` stubs.
- `core.streaming.StreamingSimulator` is a **batch test-data generator**, not a
  stream.
- The `fetch_size` "massive initial load" path is **accumulate-then-write**:
  all-or-nothing, not resumable, and its memory-safety does not extend to the
  write (§10).

**Goals**

1. A single **contract** that expresses data semantics + freshness once, and
   runs as batch, resumable micro-batch, or continuous stream **without change**.
2. A **resumable checkpoint** that both continuous and micro-batch modes share —
   "continue from the last committed point" (AvailableNow semantics).
3. An engine-agnostic **StreamSink** (Polars/DuckDB) that lands governed
   bronze + quarantine, with Spark as the scale/stateful path — not the only path.
4. Honest, documented **delivery + failure semantics**.

**Non-goals**

- Native contract execution on an unbounded streaming DataFrame (physically
  impossible for the count/collect passes; streaming is always batch-per-
  micro-batch, exactly as Spark `foreachBatch` works).
- Making Polars/DuckDB do cross-batch stateful ops (dedup/aggregation/SCD2 across
  batches / streaming joins) — that tier is Spark (§9).

---

## 2. Core principle: contract declares intent, runtime picks compute & cadence

Cadence (how often, on what compute, continuous vs scheduled) is an
**orchestration** concern, not a contract concern. The same `silver.trips`
contract must run nightly-batch in dev and AvailableNow-every-5-min in prod
**unchanged**. Therefore:

- **Contract** = data semantics + correctness + *how fresh* (portable).
- **Runtime binding** = *when* and *with what compute* (per deployment).
- **Checkpoint** = the shared cursor both run-modes read and advance.

The latency *requirement* lives in the contract as a **freshness SLO**; the
*mechanism* to meet it (continuous vs scheduled drain) lives in the runtime.

---

## 3. Contract fields (cadence-independent data semantics)

Additive to the existing `model` / `quality` / `service_levels` /
`materialization` / `incremental` schema. **No `trigger`, `interval`, `engine`,
or `checkpoint.location` in the contract** — those are runtime (§4).

```yaml
source:
  type: kafka           # file | database | landing | delta | kafka | eventhubs
                        # | sse | websocket | sqs | pubsub | servicebus | eventgrid
  # connection details are referenced by name/env, not hard-coded secrets

model: { ... }          # unchanged — schema
quality: { ... }        # unchanged — row/dataset rules (run per micro-batch)

incremental:
  strategy: max_target  # existing: max_target | delta_version | pipeline_log | watermark
  watermark_field: event_ts   # existing
  event_time: event_ts        # streaming event-time
  allowed_lateness: 10m       # → stateful (Spark) dedup/aggregation window
  dedup_keys: [event_id]      # identify duplicates on replay (see §8)

materialization:
  target: rideflow.silver.trips
  layer: silver               # bronze | silver | gold
  mode: merge                 # append | overwrite | merge | scd2
  merge_keys: [trip_id]       # required for merge/upsert
  scd2:                       # for dimensions
    business_key: [driver_id]
    hash_columns: [name, license, rating]
    effective_from: valid_from
    effective_to: valid_to

service_levels:
  freshness:
    max_delay: 5m             # THE latency intent — the runtime chooses how to meet it
```

`materialization.mode` + keys, `incremental.event_time/allowed_lateness/
dedup_keys`, and `freshness` are all true regardless of cadence — they describe
*the dataset*, not *the schedule*.

---

## 4. Runtime binding (per deployment — NOT in the contract)

Carried by the CLI flags / Databricks Asset Bundle job / Workflow config:

```yaml
runtime:
  engine: auto            # auto | polars | duckdb | spark  (subject to §9 lint)
  trigger:
    type: available_now   # once | available_now | micro_batch | continuous
    interval: 30s          # ONLY for micro_batch / continuous (implies always-on compute)
  checkpoint:
    location: abfss://.../_ckpt/silver_trips
    store: duckdb          # duckdb | sqlite | delta | spark_native  (see §7)
  delivery: at_least_once  # at_least_once | effectively_once
```

Cadence maps to Databricks compute:

| `trigger.type` | Databricks Workflow | Compute | Latency |
|---|---|---|---|
| `available_now` (recommended default) | scheduled cron / file-arrival | spins up, drains, **stops** | = schedule interval (mins) |
| `continuous` / `micro_batch: 30s` | `continuous: {}` job | **24/7 cluster** | seconds / sub-second |
| `once` | one-shot job | runs once | n/a (backfill) |

> **Streaming ≠ 24/7 by necessity.** Only `continuous`/`micro_batch` keep a
> cluster alive. `available_now` gives streaming *checkpoint semantics* with
> *batch economics*. Reserve always-on compute for a genuine sub-minute SLO.

### 4.1 Serverless suitability

The checkpoint **externalizes state** — the offset/watermark lives in the durable
checkpoint store, not in process memory — so the compute is free to be
**ephemeral**: a run can start, drain, exit (or crash) and the next invocation
resumes exactly from the last committed cursor. This is precisely what makes
**serverless the ideal fit** for the `available_now` cadence, and it's a
deliberate design property, not an accident.

| Latency need | Serverless verdict |
|---|---|
| minutes → low-seconds (`available_now`) | **best fit** — trigger → spin up → drain → commit → **scale to zero**; pay only for processing; checkpoint-resumable |
| few seconds, steady | serverless continuous *or* scheduled both fine |
| **true sub-second, always-on** (`continuous`) | needs a long-running process → serverless-**managed** (always-on, billed 24/7) or dedicated; the scale-to-zero benefit is gone — "serverless" then means *managed*, not *cheap/bursty* |

Where it maps:

- **Databricks Serverless Jobs / DLT** — scheduled `available_now`, scale-to-zero
  between runs. The native, cheapest Databricks path.
- **Serverless containers** (Cloud Run, Azure Container Apps, Fargate) — ideal for
  the DuckDB StreamSink drain-and-stop: lightweight single-node, spin up, drain,
  exit.
- **FaaS** (Lambda / Azure Functions) — works for *modest* drain-and-stop within
  the function time limit; the checkpoint makes chained short invocations safe.

**Boundary:** FaaS execution-time limits + cold starts make it a poor host for a
24/7 blocking consumer; and a "serverless" *continuous* stream is still always-on
compute (its cost model converges with a dedicated cluster). So: **serverless
covers the common case cheaply; you pay for always-on only when the freshness SLO
is genuinely sub-second** — the same boundary drawn in §7 (checkpoint commit
latency) and §9 (engine capability).

---

## 5. Execution model — one resumable loop, two lifetimes

The pivot: a **single checkpoint** both modes consume. The only difference
between "streaming" and "micro-batch" is the loop lifetime, decided by compute.

```
load cursor from checkpoint                 # "continue from last point"
snapshot end := latest available now        # AvailableNow bound (per source, §6)
loop:
    read records in (cursor, end], chunked by max_records
    result = run_dataframe(chunk)           # normal batch contract engine
    write result.good  -> target (append/merge/scd2)
    write result.bad   -> quarantine
    commit cursor                           # AFTER durable write (§8)
    ── continuous (24/7): re-snapshot end, block for new records, keep looping
    ── available_now (scheduled): cursor == end -> exit
```

- **Continuous** never exits and streams; **AvailableNow** drains to the start
  snapshot and stops. Same code, same checkpoint, same "process each record once."
- **Spark** gets this natively: identical `checkpointLocation`, switch
  `.trigger(availableNow=True)` ↔ `.trigger(processingTime=...)`. Zero contract
  change. The engine-agnostic StreamSink reproduces it with the checkpoint store.

---

## 6. Per-source cursor & end-snapshot

AvailableNow needs a **queryable "end."** Sources split into two camps:

| Source | Cursor | End snapshot | Resumable AvailableNow? |
|---|---|---|---|
| Kafka | partition offsets | high-water offsets at start | ✅ |
| Delta source | table version | latest version (`delta_version` strategy exists) | ✅ |
| Database (watermark col) | `WHERE wm > last` | `MAX(wm)` at start | ✅ (see §11) |
| File / landing | `max_source_mtime` | current file listing (exists) | ✅ |
| SQS / Service Bus / Pub/Sub | broker ack/receipt | drain-until-empty | ✅ (broker-managed) |
| SSE | `Last-Event-ID` | — (only if server supports) | ⚠️ partial |
| WebSocket / Webhook | none | **no snapshotable end** | ❌ continuous-only |

**Boundary to document:** pure-push sources with no snapshotable end
(raw WebSocket/Webhook) cannot do bounded "drain to now, stop" — they are
**continuous-only** (or at-most-once). Everything else supports AvailableNow.

---

## 7. Checkpoint store — the store doesn't create real-time; it bounds how you commit

Real-time is achieved by the **execution model** (continuous loop + *blocking*
live reads + small windows), not by the store. The store choice determines how
fast / durably / concurrently you can commit the cursor — which bounds how
real-time you can safely go.

| Store | Commit latency | Concurrency | Best fit |
|---|---|---|---|
| **DuckDB / SQLite** (local file) | sub-ms–low-ms | single-writer, single node | **sub-second continuous on one node** (default for the agnostic StreamSink) |
| **Delta** (ADLS/S3) | 100s ms/commit (a transaction) | multi-writer, governed, shareable | **AvailableNow / micro-batch cadence**, lakehouse governance |
| **Spark native `checkpointLocation`** | Spark-managed | distributed executors | **scalable continuous streaming** (hot path bypasses our store) |

**Trap:** committing to Delta every few hundred ms bloats the transaction log /
small files and *raises* latency. Delta's transactionality is a liability at high
commit frequency, an asset at low.

**Defaults:**
- Agnostic StreamSink → **DuckDB/SQLite local** (fast commits unlock low-latency
  single-node streaming; zero-dep; matches the run-log DB backend).
- Databricks / lakehouse, AvailableNow → **Delta** (governed, shareable cursor).
- Spark continuous → **native `checkpointLocation`**; mirror the committed offset
  into Delta / run-log for Observatory/Zeus visibility only.

Persistence is modeled on the existing multi-backend writer in `run_log.py`
(duckdb/sqlite/delta/spark, fsspec cloud paths), reusing the proven
`get_last_run_dlt_state()` / `get_last_run_watermark()` round-trip pattern.

---

## 8. Delivery semantics — at-least-once + contract idempotency

**Commit-after-write ⇒ at-least-once.** A crash between the durable write and the
cursor commit reprocesses that last batch on restart. Exactly-once is not
promised (same honest ceiling as Spark structured streaming).

**The contract's own fields make replays idempotent ⇒ effectively-once:**

- `dedup_keys` → drop already-seen records.
- `materialization.mode: merge` + `merge_keys` → upsert instead of duplicate.
- `materialization.mode: overwrite` → wholesale replace (clean on retry).

> **Any resumable/retried load MUST use `overwrite` or `merge` (never bare
> `append`)** — replays are expected, and `append` duplicates. Lint should warn
> when `append` is combined with a resumable trigger.

`pipeline_logs` role: **observability + audit + fallback**, not the hot-path
cursor. Emit one run log per *window* (not per micro-batch — avoid flooding),
stamp the committed offset into `max_watermark_value`, and allow recovering an
approximate position from the last run log if the checkpoint store is lost.

---

## 9. Engine capability boundary (derived, lint-enforced)

The contract does **not** name an engine; its semantic fields *imply* a
requirement, and the linter validates the chosen runtime engine against it.

| Contract option | Polars / DuckDB | Spark |
|---|---|---|
| `mode: append/overwrite`, `trigger: available_now/micro_batch` | ✅ | ✅ |
| `mode: merge` (Delta upsert) | ⚠️ delta-rs, limited | ✅ |
| `mode: scd2`, `dedup_keys`, `allowed_lateness` (cross-batch state) | ❌ | ✅ |
| `trigger: continuous`, streaming joins, `output_mode: update/complete` | ❌ | ✅ |

Lint examples:
- `mode: scd2` + engine polars → **reject**: "requires a stateful engine (spark)."
- `trigger: continuous` on any engine → **warn**: "implies an always-on cluster;
  confirm the freshness SLO justifies it, else use `available_now`."
- `mode: append` + resumable trigger → **warn**: "replays will duplicate; use
  merge/overwrite."

---

## 10. Failure & Resumability

Two ingestion paths with very different failure behavior. Choose by load size /
failure tolerance.

### 10.1 `fetch_size` (existing) — simple, all-or-nothing, NOT resumable

`source.options.fetch_size` chunks the DB round-trip (Polars, SQLAlchemy
`yield_per`), validating each chunk independently — but it is
**accumulate-then-write**: all chunks are concatenated in memory and materialized
**once at the end** (`processor.py` `run_source` polars branch;
`Runner._run_contract_stage` materializes once). Therefore:

- **Mid-run failure ⇒ nothing is written.** A failure in chunk *K* aborts the run
  **before** `materialize()`. The target is untouched (no partial rows, no
  rollback needed); **all** in-memory progress from chunks 1…K-1 is discarded.
- **Watermark does not advance** on failure — the run-log row carrying
  `max_source_mtime` is written only on the success path, so a re-run reissues
  the same query from the last committed watermark.
- **Not resumable:** `batch_idx` is a bare counter; there is no per-chunk
  checkpoint. A re-run restarts from row 0 / the last committed watermark, never
  mid-fetch.
- **The write is NOT memory-bounded:** `pl.concat` buffers the entire good/bad
  result. A genuinely huge load can OOM at the concat/write even though each
  chunk was small — `fetch_size`'s "flat memory" promise holds for *reading /
  validating*, not for *writing*.
- **Re-run duplication depends on `mode`:** `append` → duplicates; `overwrite` →
  clean replace; `merge` → idempotent upsert.

**Verdict:** fail-*safe* against partial-target corruption (all-or-nothing) but
coarse — a failure discards the whole run, and it does not scale the write.
Fine as a first bulk load of a bounded table with `mode: overwrite`/`merge`.

> The `fetch_size` branch now logs this explicitly on entry and carries a
> semantics comment (`processor.py`), so operators are not surprised.

### 10.2 Checkpointed micro-batch (this spec) — resumable, memory-bounded writes

The §5 loop **writes each chunk and commits the cursor after each write**.
Consequently:

- **Mid-run failure ⇒ resume from the last committed chunk**, not row 0. Work
  already written stays written; only the in-flight chunk is retried.
- **Writes are memory-bounded** — one chunk in memory at a time; no full-result
  `concat`. This is what makes genuinely large loads safe.
- **Idempotent retries** via `dedup_keys` / `merge` (§8), so re-processing the
  in-flight chunk does not duplicate.

**This is not only the streaming engine — it is also the correct engine for
large / failure-prone batch loads.** The same `read → validate → write → commit`
loop fixes both `fetch_size` limitations (whole-run loss + write-memory ceiling)
and provides streaming. `fetch_size` remains the simple path for small bounded
tables; the checkpointed loop is the path for large batch *and* streaming.

---

## 11. Relationship to existing mechanisms (what we reuse, not rebuild)

- **Pull / queryable sources are ~80% there today.** Combine `fetch_size`
  (memory-safe first read) + existing incremental (`max_target` /
  `delta_version` / `max_source_mtime`, run-log-backed with read-back) and you
  already have resumable, AvailableNow-style ingestion: first run drains the
  table, records the watermark; later runs read `WHERE wm > last`. **The
  watermark is the checkpoint.** The new checkpoint store is needed mainly for
  **event/queue connectors with no queryable watermark** (Kafka offsets, SSE
  last-id, broker acks).
- **Run logs** already persist locally (duckdb/sqlite/json) *and* read back
  (`get_last_run_watermark`, `get_last_run_dlt_state`, `RunLogReader.last_success`),
  filtering out `failed` / `no_new_data` — the proven "commit a cursor, reload on
  startup" pattern. The checkpoint store generalizes it to connector offsets.
- **`incremental.py`** strategies (`max_target`, `delta_version`, `pipeline_log`,
  `manifest`) provide the batch/pull-source cursor; the StreamSink adds the
  connector-offset cursor and the continuous lifetime.

---

## 12. Acceptance criteria (done = all true)

### A. Contract ⁄ runtime separation (§2–§4)
- [ ] A **single, unchanged** contract runs as `once` (batch), `available_now`
      (drain + stop), and `continuous` (stream) — only the runtime binding differs.
- [ ] The contract carries **no** `trigger` / `interval` / `engine` /
      `checkpoint.location`; a lint check rejects those keys inside a contract.
- [ ] `service_levels.freshness.max_delay` is the only latency expression in the
      contract; removing/altering the runtime trigger does not change the contract.
- [ ] The same contract file validates identically across Polars, DuckDB and Spark
      for the batch/append case (byte-identical `good`/`bad` counts on a fixture).

### B. Execution & resumability (§5–§6)
- [ ] Checkpoint is committed **after** the durable write, never before.
- [ ] **Crash-injection test:** kill the process mid-run after N chunks; re-run;
      assert (a) no source record is skipped (no gap) and (b) with `mode: merge`
      no record is duplicated (idempotent resume from last committed cursor).
- [ ] `available_now` snapshots an end position at start, drains `(cursor, end]`,
      commits, and **exits**; `continuous` re-snapshots and keeps looping — both
      from the **same** checkpoint.
- [ ] Offset-queryable sources (Kafka, Delta, DB-watermark, file, broker queues)
      resume from their native cursor; push-only sources (WebSocket/Webhook) are
      correctly rejected for `available_now` with a clear "continuous-only" error.
- [ ] Writes are **memory-bounded** — peak RSS stays flat across a load an order
      of magnitude larger than memory (no full-result `concat`).

### C. Engine boundary & lint (§9)
- [ ] `mode: scd2` (or `allowed_lateness`/cross-batch `dedup`) with a non-Spark
      engine → lint **rejection** naming the offending field.
- [ ] `trigger: continuous` → lint **warning** referencing the freshness SLO.
- [ ] `mode: append` + a resumable trigger → lint **warning** ("replays will
      duplicate; use merge/overwrite").
- [ ] Engine requirement is **derivable from the contract alone** (no engine field
      needed) and the derivation is unit-tested per rule.

### D. Failure semantics (§10)
- [ ] `fetch_size` path logs its accumulate-then-write / not-resumable / write-not-
      memory-bounded semantics on entry, and carries the explanatory comment. *(done)*
- [ ] A mid-run `fetch_size` failure leaves the target **untouched** (no partial
      rows) and does **not** advance the incremental watermark — asserted by a test
      that injects an error in chunk K and checks target + run-log state.
- [ ] The checkpointed loop, on the same injected failure, leaves chunks 1…K-1
      **written** and resumes at chunk K on re-run.

### E. Observability (§7–§8)
- [ ] Streaming emits a run log per **window** (not per micro-batch) with the
      committed offset stamped into `max_watermark_value`; Observatory/Zeus render
      freshness/quarantine for the stream exactly as for batch.
- [ ] If the checkpoint store is deleted, the loop recovers an approximate position
      from the last run log (fallback path) and logs that it did so.
- [ ] Spark path uses native `checkpointLocation`; agnostic path uses the
      configured store; a test shows both resume identically from the same offsets.

## 13. Success criteria (outcome)

1. **One contract, three cadences — provably.** The identical `silver.trips`
   contract ships to a nightly batch, a 5-minute AvailableNow job, and a 24/7
   continuous cluster with **zero diffs to the contract file**. (Demo-verifiable.)
2. **No 24/7 compute unless the SLO demands it.** The default deployable is
   AvailableNow on a scheduled/file-arrival Workflow — streaming checkpoints,
   batch economics. Continuous compute appears only where `freshness.max_delay`
   is sub-minute. Measured: cluster-uptime ∝ data volume, not wall-clock.
3. **Resumable by construction.** A killed load never loses committed work and
   never duplicates (with `merge`/`overwrite`) — the crash-injection test passes
   on every source type, and large loads complete under a fixed memory ceiling.
   This is the concrete fix for the `fetch_size` whole-run-loss failure mode.
4. **Engine-agnostic where it can be, Spark where it must be — honestly.**
   Bronze ingest + append/overwrite/merge run on Polars/DuckDB with no cluster;
   only genuinely stateful tiers (scd2, cross-batch dedup/aggregation, streaming
   joins) require Spark, and the contract *says so* via lint before deploy — never
   a runtime surprise.
5. **Truthful capability claims.** Marketing/exec claims trace to shipped behavior:
   "streaming ingestion into governed bronze, resumable, engine-agnostic" and
   "runs inside Spark Structured Streaming via foreachBatch for stateful silver/
   gold." No claim of native streaming contract execution or exactly-once.

## 14. Test plan

- **Unit:** engine-requirement derivation per §9 rule; end-snapshot resolver per
  source; cursor serialize/reload round-trip (mirrors `get_last_run_dlt_state`).
- **Resumability (integration):** for each offset-queryable source — seed a bounded
  set, kill after K chunks, re-run, assert no-gap + no-dup (`merge`) and correct
  resume offset. Repeat for `fetch_size` to assert the *opposite* (whole-run
  restart, no partial target, watermark unchanged).
- **Memory:** load a source ≫ memory through the checkpointed loop; assert flat
  peak RSS; contrast with `fetch_size` OOM ceiling (documented, not a gate).
- **Lint:** table-driven cases for each rejection/warning in §9 + the
  "no runtime keys in contract" check in §12.A.
- **Parity:** same contract, Spark-native checkpoint vs agnostic store, resume from
  identical offsets → identical `good`/`bad`/target state.
- **Cadence:** one contract executed `once` / `available_now` / `continuous`;
  assert identical per-record outcomes, differing only in run lifetime.

## 15. Open decisions

1. **Inferred vs explicit engine:** derive engine from semantics (`scd2` ⇒ spark)
   or require it explicit in the runtime binding? (Lean: derive + allow override,
   lint validates.)
2. **Default checkpoint store:** DuckDB/SQLite (zero-dep, matches run-log DB) vs
   Delta (lakehouse-native). (Lean: DuckDB/SQLite default; Delta for Databricks.)
3. **Per-chunk commit for `fetch_size` bounded loads:** ship the checkpointed
   loop as an opt-in `resumable: true` on database/file sources so large initial
   loads become resumable + memory-bounded without changing the connector story.
