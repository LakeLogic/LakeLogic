# Governed Data Product Blueprints — implementation plan

**Status:** plan only. Nothing in Phase 0 or beyond is built.
**Written:** 2026-08-30

---

## The three values this delivers

1. **A discovery surface that converts.** Technology-named blueprints
   (`Azure SQL CDC to Microsoft Fabric Lakehouse`) rank for what people actually
   search, where scenario names (`01_data_quality_trust`) rank for nothing.
2. **A trust claim competitors cannot make.** Every blueprint carries a
   CI-written `Verified <date> · <version> · <environment>`. Kestra has 690
   blueprints and no statement that any of them was ever executed.
3. **One canonical definition.** Website, Platform and docs all read the same
   `blueprint.yaml`. Nothing is written twice, so nothing drifts.

**The honest caveat:** the catalogue is worth less than nothing until the
examples provably run. Today **10 of 19 Colab notebooks fail**, and the gallery
README already promises they run "in ~30 seconds". Publishing over that converts
a quiet problem into an advertised one. Phase 0 exists for that reason and must
land before anything is published.

---

## Positioning this plan serves

> **From source data to trusted data product — defined once, enforced on every run.**

Core is not a connector product. Connectors move data; Core decides what must be
true before that data becomes a product, and proves the controls ran. It
complements dlt/Airbyte (extraction), Kafka (transport), Kestra/Airflow
(orchestration), Spark/Polars/DuckDB (processing), Databricks/Fabric/Snowflake
(execution).

**Claim now:**
> Build governed lakehouse products from supported databases, files and APIs
> using executable data contracts.

**Claim once streaming is genuinely wired** (see Phase 3):
> Turn database, file, API and event-stream data into governed lakehouse
> products using one executable contract.

**Do not claim** "ingest any data from any source". The capability map below
shows why.

---

## Where we actually are

Verified by execution on 2026-08-30, not by reading docs.

### Ingestion capability

| Category | Declarative (`source.type`) | Library only | Gap |
|---|---|---|---|
| Databases | ✅ `database` — SQL Server, Postgres, MySQL, Oracle, SQLite; full / incremental / **CDC** | `AzureSQLConnector`, `PostgreSQLConnector` (watermark only) | NoSQL (Mongo/Cosmos/Dynamo) are pandas extracts, no `source.type` |
| SFTP / file transfer | ✅ `sftp` — pull with pattern + mtime incremental; **push** via `sftp://` target, atomic | `SFTPConnector` | verified against in-process server only, never a remote vendor server |
| Object storage | ✅ file paths — S3, ADLS, GCS, OneLake; CSV/JSON/Parquet | — | XML, Avro, Excel unverified |
| APIs & SaaS | ⚠️ `dlt` — Salesforce, HubSpot, Stripe | `RESTAPIConnector` | Klaviyo untested here |
| Streams & Messaging | ❌ **none** | Kafka, Event Hubs, Service Bus, SQS, Pub/Sub, SSE, WebSocket, Webhook | 8 connectors, **none wired to `source.type`** |
| Telemetry & IoT | ❌ none | — | nothing at all — no InfluxDB, no Prometheus |
| Documents & Unstructured | ⚠️ `extraction:` block exists | — | not proven end to end |
| Consumers (BI/ML) | — | — | lineage/metadata by design, not ingestion |

### Lifecycle coverage per priority pattern

Read → checkpoint → validate → quarantine → write → reconcile → evidence → recover

| Stage | Database | SFTP / files | REST API | Kafka |
|---|---|---|---|---|
| Read | ✅ | ✅ | ⚠️ via dlt | ❌ |
| Checkpoint | ✅ watermark + CDC LSN | ✅ mtime | ⚠️ dlt cursor | ❌ no offsets |
| Validate | ✅ | ✅ | ✅ | ✅ |
| Quarantine | ✅ | ✅ | ✅ | ✅ |
| Write | ✅ | ✅ | ✅ | ✅ |
| Reconcile | ⚠️ **no core `reconcile()`** | ⚠️ | ⚠️ | ⚠️ |
| Evidence | ✅ run log + control-evidence schema | ✅ | ✅ | ✅ |
| Recover | ✅ `reprocess_from/to`, quarantine reprocess | ⚠️ mtime re-read only | ⚠️ | ❌ no replay/DLQ |

**The middle five stages are source-agnostic** — they live in the engine and work
identically whatever moved the data. That is the product. The weakness is at the
edges (checkpoint, recover), which are per-source.

**Two open decisions blocking the lifecycle claim:**

- `reconcile` appears in examples (`recon.yaml`) but has **no implementation in
  `lakelogic/core/`**. Build it, or remove it from the claim.
- Kafka is **two stages short**, not one connector short. Wiring
  `source.type: kafka` without offsets and replay would produce a streaming
  source that cannot resume or recover.

---

## Phase 0 — make the existing examples true

**Nothing is published until this is done.** Blocking.

| # | Task | Detail |
|---|---|---|
| 0.1 | Fix `pl.read_delta` | **Product bug**, not examples. polars 1.40.1 + deltalake 1.6.2 → `'deltalake._internal.Schema' object is not iterable`, and polars *declares* `deltalake>=1.0.0`, so this is the supported combination. `DeltaTable.to_pyarrow_table()` works. `core/delta_compat.py` is written but **not wired**; 4 core call sites still use `pl.read_delta` (`diagnose_cmd`, `external_logic`, `materialization`, `slo`). |
| 0.2 | Pin the unbounded deps | `polars>=0.20.0` and `deltalake>=0.15.0` resolved across two major versions. This is the root cause of ~5 notebook failures. |
| 0.3 | Fix or retire `08h_backfill_replay` | Only notebook with genuine API drift: `run()` has no `full_refresh` argument. Product decision — is there a current equivalent? |
| 0.4 | Re-test the 4 dep-missing notebooks in a clean env | They may already pass in Colab, where the notebook's own `pip install` runs. **Not judgeable from a local venv.** |
| 0.5 | Notebook CI (`nbmake`) | Executes every notebook; **fails the build** on failure. This is the mechanism the whole trust claim rests on. |

**Exit criterion:** every published notebook executes in CI, or is not published.

---

## Phase 1 — the blueprint model

| # | Task | Detail |
|---|---|---|
| 1.1 | Finalise `blueprint.yaml` | Drafted at `examples/blueprints/azure-sql-cdc-to-fabric-lakehouse/`. One blueprint, many contract modules, `depends_on` between them. |
| 1.2 | Resolve the Colab binding problem | Contracts use `path: "table:{bronze_catalog}..."`, which cannot resolve in Colab — when tested, `source` and `materialization` had to be **stripped** to run. Preferred fix: the notebook binds the placeholder to a local table, showing the substitution explicitly. Same contract, different binding. |
| 1.3 | `blueprint.yaml` validator + test | A blueprint whose contracts do not parse must fail CI like any other test. |
| 1.4 | CI writes `verification` | Scoped to environment **and** version. "Executed once" is not a durable status: passing on 1.46.0 says nothing about 1.47.0; passing in Colab says nothing about Fabric. |

**Model rules already settled:**

- A blueprint is a **solution**; contracts inside it are the data products.
- Split by **data product** (bronze/silver), never by transport step. A blueprint
  whose outcome is "a message arrived somewhere" is a pipeline step, and the
  start of a generic orchestration catalogue.
- A contract **reads and writes**. That is the whole model. No invented
  `source/transport/landing/target` vocabulary — `source.type` is
  `database | dlt | sftp | table` or a file path; `materialization.format` is
  `delta | iceberg | parquet | csv | duckdb | dlt`.
- Getting data *to* the landing path is the **orchestrator's** job, stated
  explicitly under `upstream` so the boundary is not left implied.
- Connections are **env references, never values**. Commented-out connection
  strings are how credentials get committed.
- `title` and `seo_title` are separate fields.

---

## Phase 2 — first blueprints

Launch only categories with genuine fill.

| # | Blueprint | Why | Verification reachable |
|---|---|---|---|
| 2.1 | **SFTP partner file exchange** | Only bidirectional source: pull with mtime incremental, push with atomic `.tmp`-rename. Fully Colab-runnable — asyncssh hosts the server in-process. | `colab: passing` — a **real** badge |
| 2.2 | **Azure SQL CDC → Fabric bronze/silver** | Hardest case; proves multi-module + `depends_on`. Bronze retains deletes, silver is SCD2. Logic already runs on synthetic events. | `colab: passing`, `fabric: not-attempted` |
| 2.3 | Database incremental → lakehouse | Broadest applicability | `colab: passing` |

Each ships: `blueprint.yaml`, contracts, `notebook.ipynb`, synthetic data,
expected output.

**Do not build the gallery UI until 2.1 works end to end.** A catalogue of
partially-functioning examples destroys the trust proposition it exists to sell.

---

## Phase 3 — close the capability gaps

Ordered by value, not ease.

| # | Task | Detail |
|---|---|---|
| 3.1 | Wire streams to `source.type` | 8 connectors exist, none declarable — the same state SFTP was in before it was wired. **But do the lifecycle, not just the connector:** offsets/checkpoint, replay, dead-letter. Two stages short, not one connector short. |
| 3.2 | Decide `reconcile` | Build it in core, or remove it from the lifecycle claim. It is currently claimed by examples and implemented nowhere. |
| 3.3 | Verify unstructured end to end | `extraction:` is declarable but unproven. |
| 3.4 | Register `redshift` / `synapse` in `_get_adapter` | `GenericSQLAdapter` now has a transformation pipeline; only the dispatch wiring is missing. One change covers Redshift, Synapse, Postgres, MySQL, Trino, Azure SQL. |
| 3.5 | Verify native CDC against real Azure SQL | Unit-verified only. Live testing found three bugs today that every mock passed. Azure SQL exists in the MSDN subscription. |
| — | **Telemetry & IoT** | **Omit the heading** until an adapter ships. Naming it implies support. |

---

## Phase 4 — the two surfaces

Only after Phases 0–2.

| Surface | Job | Primary action |
|---|---|---|
| Marketing website | Discovery, SEO, conversion | Run in Colab |
| LakeLogic Platform (Build) | Implementation against connected systems | Open in LakeLogic |
| GitHub | Canonical source | View source |

- Both surfaces **read `blueprint.yaml`**. Nothing typed by hand into a CMS —
  that is how the README came to promise notebooks that had stopped running.
- Card carries the **tier** badge (`Open Source` / `Enterprise`, following the
  pattern Kestra and lakeFS already validate) and, separately, the
  **verification** line. Tier and verification are different axes: a blueprint
  can be Open Source and unverified.
- Two CTAs at two honest speeds: Colab is minutes with no credentials; the
  Platform path is connect → generate → review → PR → deploy, and is not minutes.
- Page states what Colab **does not** cover. For the Azure blueprint that is the
  Service Bus transport and the Fabric write.
- Categories: Databases · Files & Storage · APIs & SaaS · Streams & Messaging ·
  Documents & Unstructured. **Consumers** (BI/ML) is a separate axis — where
  governed data is used, not where it comes from.
- No blueprint counts on category cards until volume justifies them.

Page mock: <https://claude.ai/code/artifact/82c02ad0-52b9-45e0-a8e3-9cb3fb8231ac>
(shows the superseded `bindings` block; regenerate from the corrected file.)

---

## Decisions still open

1. `reconcile` — build in core, or drop from the lifecycle claim?
2. Kafka — connector only, or the full checkpoint/replay/DLQ lifecycle?
3. `08h_backfill_replay` — rewrite to the current API, or retire?
4. Gold contract in the Azure blueprint, or stop at bronze/silver?
5. Categories — outcome-led or technology-led on the gallery?

---

## What is already done

Not part of this plan; context for what Phase 2 can honestly claim.

- **SFTP**: AsyncSSH replacing paramiko (host-key verification on by default,
  where `AutoAddPolicy` accepted any key), declarative `source.type: sftp`,
  incremental by mtime, atomic push. 20 tests against a real in-process server.
- **Native CDC**: SQL Server / Azure SQL `cdc.fn_cdc_get_all_changes_*`, deletes
  captured, before-images filtered, LSN-ordered resumption. 18 tests, unit-level.
- **Conformance**: 17 cross-engine defects fixed, `KNOWN_GAPS` empty, 159 passed
  on DuckDB + Polars + Spark.
- Both blueprint contracts run on synthetic CDC events: bronze 3 accepted
  (including the delete) / 2 quarantined, silver 3 accepted.
