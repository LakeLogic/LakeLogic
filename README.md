# LakeLogic

**Your data pipeline breaks silently. LakeLogic catches it.**

One YAML contract. Any engine. Every row validated, quarantined, or promoted — automatically.

[![CI](https://github.com/LakeLogic/LakeLogic/actions/workflows/ci-gate.yml/badge.svg)](https://github.com/LakeLogic/LakeLogic/actions/workflows/ci-gate.yml)
[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://lakelogic.github.io/LakeLogic/)
[![PyPI](https://img.shields.io/pypi/v/lakelogic?logo=pypi&logoColor=white)](https://pypi.org/project/lakelogic/)
[![Python](https://img.shields.io/badge/python-3.9+-blue?logo=python&logoColor=white)](https://www.python.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-green)](LICENSE)
[![Downloads](https://img.shields.io/pypi/dm/lakelogic?logo=pypi&logoColor=white)](https://pypi.org/project/lakelogic/)

---

## The Problem

You write quality checks in Spark. Then you need to run locally with Polars. Now you're maintaining two codebases. Your bronze layer has no validation. Your silver layer silently drops rows. Nobody knows which records failed or why.

## The Solution

```yaml
# contract.yaml — this is your entire quality gate
version: "1.0"
info:
  title: Silver Customers
  owner: data-team
model:
  fields:
    - name: customer_id
      type: integer
      required: true
    - name: email
      type: string
    - name: revenue
      type: float
    - name: status
      type: string
source:
  type: landing
  path: "data/customers/*.csv"
  load_mode: incremental
quality:
  row_rules:
    - sql: "customer_id IS NOT NULL AND email IS NOT NULL"
    - sql: "status IN ('active', 'churned', 'pending')"
    - sql: "revenue >= 0"
    - sql: "email LIKE '%@%.%'"
materialization:
  strategy: merge
  target_path: "silver/customers"
  format: parquet
  merge_keys: [customer_id]
quarantine:
  enabled: true
  target: "quarantine/customers"
```

```python
from lakelogic import DataProcessor

result = DataProcessor("contract.yaml").run_source()

print(f"✅ Valid: {len(result.good)}  |  ❌ Quarantined: {len(result.bad)}")
```

**Same contract runs on Polars, Spark, DuckDB, or Pandas.**
Zero code changes.

---

## Install

```bash
pip install lakelogic                    # Core + Polars
pip install "lakelogic[spark]"           # + PySpark
pip install "lakelogic[delta]"           # + Delta Lake (Spark-free)
pip install "lakelogic[notifications]"   # + Apprise + Jinja2 alerts
pip install "lakelogic[all]"             # Everything
```

## What You Get

### 🔒 Schema & Quality Gate
Define fields, types, required constraints, and SQL-based rules in YAML. Bad rows are quarantined with tagged error reasons — never silently dropped.

### 🔄 Engine Portability
One contract, four engines. Develop locally on Polars in milliseconds. Deploy to Spark at scale. Same validation semantics everywhere.

### 📊 Declarative Transformations
Rename, derive, deduplicate, pivot, unpivot, bucket, join, filter, JSON extract, date range explode — all in YAML, all engine-agnostic.

### 🔗 Automatic Lineage
Every row is stamped with `_lakelogic_source`, `_lakelogic_processed_at`, and `_lakelogic_run_id`. Upstream lineage columns are preserved with `_upstream_*` prefix across layers.

### 📦 Incremental Processing
Watermark-based incremental loads, file-mtime tracking, run logs, and CDC support. Process only what's new.

### 🔔 Notifications
Slack, Teams, Email, Discord, and [90+ channels](https://github.com/caronc/apprise/wiki) via Apprise. Built-in Jinja2 templates per event. Just add a `target` URL.

### 🏗️ Materialization
Write validated data to CSV, Parquet, Delta Lake, or Unity Catalog tables. Supports append, overwrite, merge, and SCD2 strategies.

### 🧪 Synthetic Data
Generate realistic test data from any contract: `lakelogic generate --contract contract.yaml --rows 1000`

### 🔌 dbt Import
Already using dbt? Convert your `schema.yml` in one command: `lakelogic import-dbt --schema models/schema.yml --output contracts/`

---

## Quick Start (5 Minutes)

### 1. Bootstrap a contract from your data

```bash
lakelogic bootstrap --landing data/ --output contracts/
```

This scans your files, infers schemas, detects PII, and generates ready-to-use contracts.

### 2. Run the quality gate

```bash
lakelogic run --contract contracts/customers.yaml --source data/customers.csv
```

### 3. See the results

```
✅ Good records: 847 → output/customers_good.parquet
❌ Quarantined:  23  → output/customers_quarantine.parquet
📊 Quality score: 97.4%
```

### 4. Check your environment

```bash
lakelogic doctor
```

```
LakeLogic Doctor
═══════════════════════════════════════
  Version     : 0.2.0
  Python      : 3.11.7
  OS          : Windows 11

  Engines
  ───────
  ✅ polars    1.18.0
  ✅ duckdb    1.1.3
  ✅ pandas    2.2.1
  ⬚  pyspark  not installed

  Extras
  ──────
  ✅ deltalake  0.22.3
  ✅ jinja2     3.1.4
  ✅ apprise    1.9.0
  ⬚  dataprofiler  not installed
═══════════════════════════════════════
```

---

## Architecture

```text
┌──────────────────────────────────────────────────────────────────┐
│                         Contract YAML                           │
│  schema · SQL quality rules · transforms · lineage · target     │
└────────────────────────────┬─────────────────────────────────────┘
                             │
                      ┌──────▼──────┐
                      │ DataProcessor│
                      └──────┬──────┘
                             │
        ┌────────────┬───────┼───────┬────────────┐
        ▼            ▼       ▼       ▼            │
   ┌────────┐  ┌────────┐ ┌───────┐ ┌────────┐   │
   │ Polars │  │ Spark  │ │DuckDB │ │ Pandas │   │
   └───┬────┘  └───┬────┘ └──┬────┘ └───┬────┘   │
       │           │         │          │         │
       └───────────┴────┬────┴──────────┘         │
                        │                         │
               ┌────────▼────────┐                │
               │  Validated Data  │                │
               │  ┌────┐ ┌─────┐ │                │
               │  │Good│ │ Bad │ │                │
               │  └──┬─┘ └──┬──┘ │                │
               └─────┼──────┼────┘                │
                     │      │                     │
               ┌─────▼┐  ┌──▼────────┐            │
               │Target│  │Quarantine │            │
               └──────┘  └───────────┘            │
```

## Explore the Examples

The [`examples/`](https://github.com/LakeLogic/LakeLogic/tree/main/examples) directory contains 24 runnable notebooks:

| Category | What You'll Learn |
|:---|:---|
| **Quickstart** | Your first contract in 5 minutes, database governance, dbt+PII |
| **Core Patterns** | Medallion architecture, bronze quality gates, SCD2, deduplication, soft deletes |
| **Advanced** | Insurance ELT, GDPR compliance, late-arriving data, external logic, streaming, synthetic data |
| **Compliance** | HIPAA PII masking |

## Documentation

- **[Full Docs](https://LakeLogic.github.io/LakeLogic)** — Complete guides and API reference
- **[Quickstart](https://LakeLogic.github.io/LakeLogic/quickstart/)** — Get running in 5 minutes
- **[Contract Reference](docs/contract_template.md)** — Full YAML field reference
- **[CLI Reference](https://LakeLogic.github.io/LakeLogic/cli/)** — Command-line usage
- **[Delta Lake Support](docs/delta_lake_support.md)** — Spark-free Delta operations
- **[Streaming](docs/streaming_implementation_complete.md)** — Real-time ingestion

## Contributing

See `CONTRIBUTING.md` to get started, or `docs/installation.md#developer-installation` for environment setup.

---

### License

Apache-2.0
