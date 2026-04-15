# LakeLogic

**Your Data Estate. Under Contract.**

<a href="https://img.shields.io/badge/docs-GitHub%20Pages-blue" target="_blank">![Documentation</a>](https://LakeLogic.github.io/LakeLogic/)
<a href="https://img.shields.io/pypi/v/lakelogic?logo=pypi&logoColor=white" target="_blank">![PyPI</a>](https://pypi.org/project/lakelogic/)
<a href="https://github.com/LakeLogic/LakeLogic/actions/workflows/ci-gate.yml/badge.svg" target="_blank">![CI</a>](https://github.com/LakeLogic/LakeLogic/actions/workflows/ci-gate.yml)
<a href="https://img.shields.io/badge/coverage-100%25-brightgreen?logo=codecov" target="_blank">![Coverage</a>](https://github.com/LakeLogic/LakeLogic)
<a href="https://img.shields.io/badge/python-3.9+-blue?logo=python&logoColor=white" target="_blank">![Python</a>](https://www.python.org)
<a href="https://img.shields.io/badge/license-Apache%202.0-green" target="_blank">![License</a>](LICENSE)

A declarative, contract-driven medallion pipeline engine for data mesh architectures.

> Describe your data products in YAML — LakeLogic materializes them as Delta/Iceberg tables with lineage, quality, and SCD2 built in.
>
> Write once. Run on <a href="https://spark.apache.org/" target="_blank">Spark</a>, <a href="https://pola.rs/" target="_blank">Polars</a>, or <a href="https://duckdb.org/" target="_blank">DuckDB</a>.
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
- **<a href="https://docs.pydantic.dev/" target="_blank">Pydantic</a>-Powered Validation** — Every contract, system & domain configs are parsed through strict Pydantic models with `Literal` type enforcement — invalid YAML is caught at load time, not at runtime
- **SQL-First Rules** — Define business logic in the language your team already speaks — no SDK, no custom DSL
- **SLO Monitoring & Anomaly Detection** — Native freshness, row count, and statistical anomaly detection with automatic multi-channel alerting when thresholds breach

> **<a href="https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/01_data_quality_trust.ipynb" target="_blank">✏️ Try it out in Google Colab: Data Quality & Trust</a>**

### Compliance & Governance

- **GDPR & HIPAA Compliance** — Contract-driven `forget_subjects()` with nullify, hash, or redact strategies and immutable audit trail
- **Automatic Lineage** — Every row stamped with Run IDs and source paths — traceable from landing zone to Gold layer
- **Pipeline Cost Intelligence** — Per-entity compute cost attribution with domain-level budget governance, autoscaling-aware estimation, and Databricks Unity Catalog billing integration

> **<a href="https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/02_compliance_governance.ipynb" target="_blank">✏️ Try it out in Google Colab: Compliance & Governance</a>**

### Engine & Scale

- **Engine Agnostic** — Write once, run on <a href="https://spark.apache.org/" target="_blank">Spark</a>, <a href="https://pola.rs/" target="_blank">Polars</a>, or <a href="https://duckdb.org/" target="_blank">DuckDB</a> — same contract, zero code changes
- **Dimensional Modeling** — Native SCD Type 2 (slowly changing dimensions), merge/upsert (SCD1), append-only fact tables, periodic snapshot overwrites, and partition-aware writes — all declared in YAML, no manual `MERGE INTO` SQL required
- **Incremental-First** — Built-in watermarking, CDC, and file-mtime tracking
- **Parallel Processing** — Concurrent multi-contract execution with data-layer-aware orchestration and topological dependency ordering
- **Backfill & Reprocessing** — Targeted late-arriving data reprocessing with partition-aware filters — no full reload required
- **External Logic** — Plug in custom Python scripts or notebooks for complex Gold-layer transformations while preserving full contract validation and lineage
- **Production Resilience** — Built-in exponential-backoff retries, per-entity timeouts, and circuit-breaker thresholds (`max_consecutive_failures`) — pipelines self-heal transient failures without operator intervention

> **<a href="https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/03_engine_scale.ipynb" target="_blank">✏️ Try it out in Google Colab: Engine & Scale</a>**

### Developer Experience

- **Structured Diagnostics & Observability** — Deep contextual logging out-of-the-box (powered by <a href="https://loguru.readthedocs.io/" target="_blank">`loguru`</a>) featuring precise timestamps, severity levels, exact function paths, and execution tags to drastically cut troubleshooting time
- **Dry Run Mode** — Validate contracts, resolve dependencies, and preview execution plans without touching any data
- **DDL-Only Mode** — Generate and apply schema DDL (CREATE/ALTER) from contracts without running the pipeline — perfect for CI/CD migrations
- **DAG Dependency Viewer** — Visualize cross-contract lineage and execution order before running — understand your pipeline graph at a glance
- **Data Reset & Reload** — Surgically reset and reload specific entities or data layers (Bronze/Silver/Gold) without impacting the rest of the lakehouse
- **Multi-Channel Alerts** — Powered by <a href="https://github.com/caronc/apprise" target="_blank">Apprise</a> for Slack, Email (SMTP/SendGrid), Teams, and Webhook notifications with ownership-based auto-routing and full <a href="https://jinja.palletsprojects.com/" target="_blank">Jinja2</a> templating support for custom formatting

> **<a href="https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/04_developer_experience.ipynb" target="_blank">✏️ Try it out in Google Colab: Developer Experience</a>**

### Data Generation & AI

- **Synthetic Data** — Built-in `DataGenerator` (powered by <a href="https://faker.readthedocs.io/" target="_blank">Faker</a>) with streaming simulation, time-windowed output, referential integrity, and edge case injection — generate realistic error rows (SQL injection, type confusion, boundary values) for stress testing and quarantine validation
- **Descriptive AI Test Data** — Steer synthetic data generation with natural language prompts (e.g. *"Generate users who are French or Japanese only, enterprise-tier, over 60 years old with SQL injection attempts in email fields"*) — output strictly adheres to the YAML contract schema
- **AI Contract Onboarding** — `lakelogic infer` auto-generates contracts from sample data with LLM-powered enrichment: automatic PII detection, column labelling, and quality rule suggestions
- **Unstructured Processing** — LLM extraction from PDFs, images, audio with same contract validation + lineage
- **Automated Run Logs** — Every pipeline run emits structured JSON with row counts, quality scores, durations, and error details — queryable as a Delta table

> **<a href="https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/05_data_generation_ai.ipynb" target="_blank">✏️ Try it out in Google Colab: Data Generation & AI</a>**

### Integrations

- **<a href="https://www.getdbt.com/" target="_blank">dbt</a> Adapter** — Import dbt `schema.yml` models and sources as LakeLogic contracts — reuse existing dbt definitions without rewriting
- **<a href="https://dlthub.com/" target="_blank">dlt</a> (Data Load Tool)** — Native `DltAdapter` supporting 100+ verified sources (Stripe, Shopify, SQL databases, Google Analytics, and more) plus declarative REST API ingestion — all with contract-driven quality gates on arrival

> **<a href="https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/06_integrations.ipynb" target="_blank">✏️ Try it out in Google Colab: Integrations</a>**

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

For a complete list of runnable guides and end-to-end notebooks, please visit the **<a href="https://lakelogic.github.io/LakeLogic/examples.html" target="_blank">Examples section of our Documentation</a>**.

---

## Documentation

For full guides, API references, tutorials, and contract templates, please visit the **<a href="https://lakelogic.github.io/LakeLogic/" target="_blank">LakeLogic Documentation Site</a>**.

## Contributing

See `CONTRIBUTING.md` to get started, or `docs/installation.md#developer-installation` for environment setup.

---

### License

Apache-2.0
