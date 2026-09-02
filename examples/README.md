# LakeLogic Examples - the gallery

Every LakeLogic example, indexed in one place so you never have to hunt across
pages to find a pattern. The philosophy is the same everywhere: **lean contracts
(the Open Lakehouse Contract vocabulary), one obvious path per job, and any
pattern runnable in Colab in ~30 seconds.** Each notebook is self-seeding - it
writes a tiny contract plus a few rows of stand-in data and runs it - so you can
try a recipe with zero setup and zero cloud credentials.

Find what you need three ways: **[by lifecycle stage](#1-by-lifecycle--contract-anatomy)**,
**[by ingestion source](#2-by-ingestion-source--load-mode)**, or
**[by data shape](#3-by-data-shape)**. Every runnable notebook has an Open-in-Colab
link. For the complete set of contract options in one place, jump to the
**[annotated reference contracts](#annotated-reference-contracts--all-options-in-one-place)**.

> **Note:** Colab links use `LakeLogic/lakelogic` + `main` as placeholders -
> replace the org/branch when this repo is published.

---

## 1. By lifecycle / contract anatomy

A contract runs a fixed pipeline: **Ingestion -> Validation -> Transformation ->
Materialization -> Export**, and a mesh composes many contracts under
**Domain / System** ownership. Pick the stage you're working on.

| Stage | What it teaches | Example | Open |
|---|---|---|---|
| **Ingestion** | Read a source into Bronze - file, cloud, DB, API, stream | [`00_quickstart_demo/orders_contract.yaml`](colab/00_quickstart_demo/orders_contract.yaml) | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/00_quickstart.ipynb) |
| **Validation** | Schema enforcement, row rules, reconciliation, SLOs | [`01_data_quality_trust_demo/`](colab/01_data_quality_trust_demo/) (`schema_policy.yaml`, `recon.yaml`, `slo.yaml`) | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/01_data_quality_trust.ipynb) |
| **Transformation** | Derived columns, renames, joins, SCD2, engine scale | [`03_engine_scale_demo/dim_customers.yaml`](colab/03_engine_scale_demo/dim_customers.yaml) | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/03_engine_scale.ipynb) |
| **Materialization** | Write Bronze/Silver/Gold to Delta/Parquet targets + DDL | [`03_engine_scale_demo/`](colab/03_engine_scale_demo/) · [`ddl_demo/`](colab/ddl_demo/) | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/03_engine_scale.ipynb) |
| **Export / egress** | Publish validated Gold *out* to a cloud bucket | [`07_lifecycle_demo/gold_orders_export.yaml`](colab/07_lifecycle_demo/gold_orders_export.yaml) | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/07_lifecycle.ipynb) |
| **Domain / System (mesh)** | Multi-contract mesh - domain ownership, engine portability | [`08*_rideflow`](colab/) series · [`cascade_demo/`](colab/cascade_demo/) · [`dag_demo/`](colab/dag_demo/) | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/08f_rideflow_mesh_products.ipynb) |
| **Governance** | PII masking, lineage, HIPAA/GDPR policy packs | [`02_compliance_governance_demo/employees_pii.yaml`](colab/02_compliance_governance_demo/employees_pii.yaml) | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/02_compliance_governance.ipynb) |

---

## 2. By ingestion source / load mode

### By source

| Source | Example | Open |
|---|---|---|
| **Flat file** (CSV) | [`00_quickstart_demo/orders_contract.yaml`](colab/00_quickstart_demo/orders_contract.yaml) · [`06_integrations_demo/wide_orders.csv`](colab/06_integrations_demo/) | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/00_quickstart.ipynb) |
| **Cloud object store** (S3 / GCS / ADLS) | [`07_lifecycle_demo/cloud_orders.yaml`](colab/07_lifecycle_demo/cloud_orders.yaml) - one contract, any cloud (URI swap) | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/07_lifecycle.ipynb) |
| **Database** (SQL) | [`06_integrations_demo/postgres_users.yaml`](colab/06_integrations_demo/postgres_users.yaml) · [`batch_orders.yaml`](colab/06_integrations_demo/batch_orders.yaml) | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/06_integrations.ipynb) |
| **API** (dlt REST) | [`06_integrations_demo/github_issues.yaml`](colab/06_integrations_demo/github_issues.yaml) | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/06_integrations.ipynb) |
| **SFTP** (ingest) | `SFTPConnector` in [`lakelogic/engines/integration_connectors.py`](../lakelogic/engines/integration_connectors.py) - covered by 06's integration surface | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/06_integrations.ipynb) |
| **Streaming** (WebSocket / Kafka / SSE) | [`06_integrations_demo/btc_trades.yaml`](colab/06_integrations_demo/btc_trades.yaml) · [`09_streaming_realtime.ipynb`](colab/09_streaming_realtime.ipynb) | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/09_streaming_realtime.ipynb) |

### By load mode

| Load mode | Example | Open |
|---|---|---|
| **Full** (snapshot) | [`00_quickstart_demo/orders_contract.yaml`](colab/00_quickstart_demo/orders_contract.yaml) | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/00_quickstart.ipynb) |
| **Incremental** (watermark) | [`03_engine_scale_demo/orders_inc.yaml`](colab/03_engine_scale_demo/orders_inc.yaml) | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/03_engine_scale.ipynb) |
| **CDC** (change data capture) | [`06_integrations_demo/cdc_orders.yaml`](colab/06_integrations_demo/cdc_orders.yaml) | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/06_integrations.ipynb) |

---

## 3. By data shape

| Shape | Status | What it teaches | Example | Open |
|---|---|---|---|---|
| **Structured** (tabular) | ✅ | Typed columns, row rules, derived fields | [`00_quickstart_demo/orders_contract.yaml`](colab/00_quickstart_demo/orders_contract.yaml) | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/00_quickstart.ipynb) |
| **Semi-structured** (nested JSON) | ✅ | Flatten nested objects (`customer.address.city`) into typed Silver via `flatten_nested` | [`07_lifecycle_demo/nested_events.yaml`](colab/07_lifecycle_demo/nested_events.yaml) | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/07_lifecycle.ipynb) |
| **Semi-structured** (API / stream payloads) | ✅ | JSON from a REST API and a live WebSocket feed | [`06_integrations_demo/github_issues.yaml`](colab/06_integrations_demo/github_issues.yaml) · [`btc_trades.yaml`](colab/06_integrations_demo/btc_trades.yaml) | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/06_integrations.ipynb) |
| **Unstructured** (PDF / text / OCR) | ⚠️ **ROADMAP** | LakeLogic ships the extraction machinery (pdf/ocr/spaCy/chunk) but `extract_batch` is **not yet wired** - it currently produces nothing, so there is **no runnable example**. See the spec. | [`docs/specs/tabular-text-extraction.md`](../docs/specs/tabular-text-extraction.md) | — |

---

## Annotated reference contracts - all options in one place

When you want to see *every* field a layer supports (not just the lean default),
read the annotated reference contracts. Each is a single heavily-commented
contract for one medallion layer - the "complete options" companion to the lean
recipes above.

| Layer | Reference | What it documents |
|---|---|---|
| **Bronze** | [`reference/bronze.annotated.yaml`](reference/bronze.annotated.yaml) | Every ingestion + source option: file/cloud/DB/API/stream, load modes, watermarks, schema policy |
| **Silver** | [`reference/silver.annotated.yaml`](reference/silver.annotated.yaml) | Every validation + transformation option: row rules, derives, renames, joins, PII/masking, flattening |
| **Gold** | [`reference/gold.annotated.yaml`](reference/gold.annotated.yaml) | Every materialization + export option: targets, formats, strategies, SCD2, cloud egress |

> The annotated references are maintained alongside these examples; if a file
> above is not present yet it is being added.

---

## Full notebook catalog

Every notebook in [`colab/`](colab/), in learning order.

| # | Notebook | Covers | Open |
|---|---|---|---|
| 00 | `00_quickstart.ipynb` | First pipeline - ingest CSV, apply rules, inspect good/bad | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/00_quickstart.ipynb) |
| 01 | `01_data_quality_trust.ipynb` | Schema policy, reconciliation, SLOs | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/01_data_quality_trust.ipynb) |
| 02 | `02_compliance_governance.ipynb` | PII masking, lineage, HIPAA/GDPR policy packs | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/02_compliance_governance.ipynb) |
| 03 | `03_engine_scale.ipynb` | Incremental, SCD2 dimensions, engine portability, DDL | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/03_engine_scale.ipynb) |
| 04 | `04_developer_experience.ipynb` | Diagnostics, CLI, developer workflow | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/04_developer_experience.ipynb) |
| 05 | `05_data_generation_ai.ipynb` | Synthetic data generation, referential integrity, scenarios | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/05_data_generation_ai.ipynb) |
| 06 | `06_integrations.ipynb` | dbt, dlt/API, database, CDC, streaming | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/06_integrations.ipynb) |
| 07 | `07_dlt_prefect_pipeline.ipynb` | Orchestrate LakeLogic inside a dlt + Prefect pipeline | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/07_dlt_prefect_pipeline.ipynb) |
| 07 | `07_lifecycle.ipynb` **(new)** | Cloud object-store ingest (URI swap), nested JSON -> Silver, Gold -> cloud export | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/07_lifecycle.ipynb) |
| 08 | `08b`-`08i_rideflow*.ipynb` | RideFlow data mesh - domains, marketplace, GDPR RTBF, backfill/replay, dashboards. **Run in order from `08b`** - they share one lakehouse, so a later notebook opened on its own finds no data. | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/08b_rideflow_marketplace.ipynb) |
| 09 | `09_streaming_realtime.ipynb` | Resumable micro-batch streaming, checkpoints, AvailableNow vs continuous | [![Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/LakeLogic/lakelogic/blob/main/examples/colab/09_streaming_realtime.ipynb) |

---

## Run any recipe locally

```bash
pip install "lakelogic[polars,duckdb]"

# Any contract + source file:
lakelogic run -c examples/colab/07_lifecycle_demo/cloud_orders.yaml \
              -s examples/colab/07_lifecycle_demo/orders_sample.csv
```

New to LakeLogic? Start at **`00_quickstart.ipynb`**. Building a lakehouse? Follow
the lifecycle table top to bottom. Want the full option surface? Read the
annotated reference contracts.
