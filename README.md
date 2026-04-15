# LakeLogic

**Your Data Estate. Under Contract.**

[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://LakeLogic.github.io/LakeLogic/)
[![PyPI](https://img.shields.io/pypi/v/lakelogic?logo=pypi&logoColor=white)](https://pypi.org/project/lakelogic/)
[![CI](https://github.com/LakeLogic/LakeLogic/actions/workflows/ci-gate.yml/badge.svg)](https://github.com/LakeLogic/LakeLogic/actions/workflows/ci-gate.yml)
[![Coverage](https://img.shields.io/badge/coverage-100%25-brightgreen?logo=codecov)](https://github.com/LakeLogic/LakeLogic)
[![Python](https://img.shields.io/badge/python-3.9+-blue?logo=python&logoColor=white)](https://www.python.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-green)](LICENSE)

A declarative, contract-driven medallion pipeline engine for data mesh architectures.

> Describe your data products in YAML — LakeLogic materializes them as Delta/Iceberg tables with lineage, quality, and SCD2 built in.
>
> Write once. Run on [Spark](https://spark.apache.org/), [Polars](https://pola.rs/), or [DuckDB](https://duckdb.org/).
> **The vendor-neutral alternative to Databricks Lakeflow Pipelines.**

---

## Data Mesh Alignment

LakeLogic is the missing runtime layer for Data Mesh — where domain ownership and federated governance stop being principles and start being enforced.

| Pillar | How LakeLogic Delivers |
| :--- | :--- |
| **Domain Ownership** | Contracts are owned and defined by domain teams (e.g., CRM, Finance) who know the data best. |
| **Data as a Product** | The contract IS the product interface — a versioned, schema-enforced, SLA-backed guarantee that consuming teams can depend on. |
| **Self-Serve Platform** | A standardized runtime that any team can use to deploy quality gates without infra silos. |
| **Federated Governance** | PII masking rules, SLA thresholds, and schema standards defined once in a central registry — automatically enforced at every domain pipeline. |

---

## Quick Start

```bash
pip install lakelogic
```

```python
from lakelogic import DataProcessor

result = DataProcessor("contract.yaml").run_source()
print(f"Valid: {result.good_count}  |  Quarantined: {result.bad_count}")
```

---

## Technical Capabilities

### Data Quality & Trust

- **100% Reconciliation** — Mathematically guaranteed: `source = good + bad`. Every row is accounted for — nothing silently dropped
- **[Pydantic](https://docs.pydantic.dev/)-Powered Validation** — Every contract, system & domain configs are parsed through strict Pydantic models with `Literal` type enforcement — invalid YAML is caught at load time, not at runtime
- **SQL-First Rules** — Define business logic in the language your team already speaks — no SDK, no custom DSL
- **SLO Monitoring & Anomaly Detection** — Native freshness, row count, and statistical anomaly detection with automatic multi-channel alerting when thresholds breach

> **[✏️ Try it out in Google Colab: Data Quality & Trust](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/01_data_quality_trust.ipynb)**

### Compliance & Governance

- **GDPR & HIPAA Compliance** — Contract-driven `forget_subjects()` with nullify, hash, or redact strategies and immutable audit trail
- **Automatic Lineage** — Every row stamped with Run IDs and source paths — traceable from landing zone to Gold layer
- **Pipeline Cost Intelligence** — Per-entity compute cost attribution with domain-level budget governance, autoscaling-aware estimation, and Databricks Unity Catalog billing integration

> **[✏️ Try it out in Google Colab: Compliance & Governance](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/02_compliance_governance.ipynb)**

### Engine & Scale

- **Engine Agnostic** — Write once, run on [Spark](https://spark.apache.org/), [Polars](https://pola.rs/), or [DuckDB](https://duckdb.org/) — same contract, zero code changes
- **Dimensional Modeling** — Native SCD Type 2 (slowly changing dimensions), merge/upsert (SCD1), append-only fact tables, periodic snapshot overwrites, and partition-aware writes — all declared in YAML, no manual `MERGE INTO` SQL required
- **Incremental-First** — Built-in watermarking, CDC, and file-mtime tracking
- **Parallel Processing** — Concurrent multi-contract execution with data-layer-aware orchestration and topological dependency ordering
- **Backfill & Reprocessing** — Targeted late-arriving data reprocessing with partition-aware filters — no full reload required
- **External Logic** — Plug in custom Python scripts or notebooks for complex Gold-layer transformations while preserving full contract validation and lineage
- **Production Resilience** — Built-in exponential-backoff retries, per-entity timeouts, and circuit-breaker thresholds (`max_consecutive_failures`) — pipelines self-heal transient failures without operator intervention

> **[✏️ Try it out in Google Colab: Engine & Scale](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/03_engine_scale.ipynb)**

### Developer Experience

- **Structured Diagnostics & Observability** — Deep contextual logging out-of-the-box (powered by [`loguru`](https://loguru.readthedocs.io/)) featuring precise timestamps, severity levels, exact function paths, and execution tags to drastically cut troubleshooting time
- **Dry Run Mode** — Validate contracts, resolve dependencies, and preview execution plans without touching any data
- **DDL-Only Mode** — Generate and apply schema DDL (CREATE/ALTER) from contracts without running the pipeline — perfect for CI/CD migrations
- **DAG Dependency Viewer** — Visualize cross-contract lineage and execution order before running — understand your pipeline graph at a glance
- **Data Reset & Reload** — Surgically reset and reload specific entities or data layers (Bronze/Silver/Gold) without impacting the rest of the lakehouse
- **Multi-Channel Alerts** — Powered by [Apprise](https://github.com/caronc/apprise) for Slack, Email (SMTP/SendGrid), Teams, and Webhook notifications with ownership-based auto-routing and full [Jinja2](https://jinja.palletsprojects.com/) templating support for custom formatting

> **[✏️ Try it out in Google Colab: Developer Experience](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/04_developer_experience.ipynb)**

### Data Generation & AI

- **Synthetic Data** — Built-in `DataGenerator` (powered by [Faker](https://faker.readthedocs.io/)) with streaming simulation, time-windowed output, referential integrity, and AI-powered edge case injection — generate realistic error rows (SQL injection, type confusion, boundary values) for stress testing and quarantine validation
- **AI Contract Onboarding** — `lakelogic infer` auto-generates contracts from sample data with LLM-powered enrichment: automatic PII detection, column labelling, and quality rule suggestions
- **Unstructured Processing** — LLM extraction from PDFs, images, audio with same contract validation + lineage
- **Automated Run Logs** — Every pipeline run emits structured JSON with row counts, quality scores, durations, and error details — queryable as a Delta table

> **[✏️ Try it out in Google Colab: Data Generation & AI](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/05_data_generation_ai.ipynb)**

### Integrations

- **[dbt](https://www.getdbt.com/) Adapter** — Import dbt `schema.yml` models and sources as LakeLogic contracts — reuse existing dbt definitions without rewriting
- **[dlt](https://dlthub.com/) (Data Load Tool)** — Native `DltAdapter` supporting 100+ verified sources (Stripe, Shopify, SQL databases, Google Analytics, and more) plus declarative REST API ingestion — all with contract-driven quality gates on arrival

> **[✏️ Try it out in Google Colab: Integrations](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/06_integrations.ipynb)**

---

## What a Contract Looks Like

One YAML file replaces hundreds of lines of validation code:

```yaml
version: "1.0"
info:
  title: "Silver Customers"
  domain: "CRM"
  system: "Salesforce"

model:
  fields:
    - name: customer_id
      type: integer
      required: true
    - name: email
      type: string
      pii: true
      masking: "hash"
    - name: status
      type: string

transformations:
  - deduplicate: [customer_id]
  - sql: "SELECT *, UPPER(status) AS status_norm FROM source"
    phase: pre

quality:
  row_rules:
    - sql: "email LIKE '%@%.%'"
    - sql: "status IN ('active', 'churned', 'pending')"
  dataset_rules:
    - unique: customer_id

materialization:
  strategy: merge
  merge_keys: [customer_id]
  format: delta
```

Same contract, **any engine** — swap `engine="polars"` for `"spark"` or `"duckdb"`. Zero code changes.

> **Analogy:** A contract is like a building inspection checklist. The inspector (LakeLogic) checks every room (row) against the blueprint (schema), flags violations (quarantine), and stamps a certificate (lineage) — regardless of whether the building was constructed with bricks (Spark), timber (Polars), or prefab (DuckDB).

### What this buys you

| Without LakeLogic | With LakeLogic |
| :--- | :--- |
| 500+ lines of PySpark/Pandas validation per table | 40 lines of YAML |
| Bad rows silently dropped or crash the pipeline | Bad rows quarantined with error reasons |
| Schema drift discovered in production dashboards | Schema drift caught at ingestion |
| Manual dedup scripts per team | `deduplicate: [key]` — one line |
| PII scattered across notebooks | `pii: true, masking: hash` — automatic |
| No audit trail | Every row stamped with run ID, source path, timestamp |

> [!TIP]
> **[View the Complete Contract Reference](docs/contract_template.md)** for every available configuration option.

---

## Architecture

LakeLogic enforces Data Contracts as quality gates across the Medallion Architecture (Bronze → Silver → Gold).

![LakeLogic Architecture](docs/assets/lakelogic_architecture.png)

Each layer uses its own contract:

| Layer | Role | Guarantee |
| :--- | :--- | :--- |
| **Bronze** | Capture everything raw, no validation | Immutable record of source |
| **Silver** | Full validation, business rules, dedup | Trusted, queryable data |
| **Gold** | Aggregations, KPIs, ML features | Analytics-ready datasets |
| **Quarantine** | Failed rows isolated with error reasons | Nothing silently dropped |

**Key Guarantee:** `source_count = good_count + bad_count` — 100% reconciliation, always.



## Examples

For a complete list of runnable guides and end-to-end notebooks, please visit the **[Examples section of our Documentation](https://lakelogic.github.io/LakeLogic/examples/colab/00_quickstart.html)**.

---

## Documentation

For full guides, API references, tutorials, and contract templates, please visit the **[LakeLogic Documentation Site](https://lakelogic.github.io/LakeLogic/)**.

## Contributing

See `CONTRIBUTING.md` to get started, or `docs/installation.md#developer-installation` for environment setup.

---

### License

Apache-2.0
