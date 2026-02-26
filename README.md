# LakeLogic

**Trust Your Data. Scale Your Logic.**

*Write Once. Run Anywhere.* — The open-source runtime for data contracts with quarantine.

LakeLogic is a SQL-first, infrastructure-agnostic quality gate that ensures your business decisions are based on data you can trust. It scales your validation logic from local Polars to petabyte-scale Spark without rewriting a single rule.

[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://LineageLogic.github.io/LakeLogic)
[![GitHub](https://img.shields.io/badge/GitHub-Repository-black?logo=github)](https://github.com/LineageLogic/LakeLogic)
[![License](https://img.shields.io/badge/license-Apache%202.0-green)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.9+-blue?logo=python)](https://www.python.org)
[![Version](https://img.shields.io/badge/version-0.1.0-orange)](CHANGELOG.md)

---

## The Core Value: Write Once. Run Anywhere

Stop paying the **"Infrastructure Lock-In Tax."** In a traditional stack, moving from a Warehouse (Snowflake) to a Lakehouse (Databricks) means months of rewriting validation rules. LakeLogic decouples your **Business Logic** from your **Execution Engine**.

1. **Cost Efficiency (The Spark Tax ROI):** Run 80% of your maintenance checks on **Polars** or **DuckDB** for pennies, while reserving **Spark** for your massive production scales.
2. **Risk Mitigation (100% Reconciliation):** Ensure `Source = Good + Quarantined`. Mathematically prove that no record was lost or double-counted across your layers.
3. **Stakeholder Trust (Visual Traceability):** Use aggregate roll-ups to give your business users a visual drill-down from board-level KPIs back to raw source records.

## Key Features

- **SQL-First Logic**: Use the SQL expressions you already know for transformations and quality rules.
- **Schema Enforcement**: Type casting, required fields, and unknown-field handling.
- **Intelligent Quarantine**: Records that fail rules are detoured, tagged with error messages, and saved for correction.
- **Lineage Injection**: Tag records with source path, run ID, and processing timestamp.
- **Materialization**: Write validated data to local CSV/Parquet targets or Delta/Iceberg when running on Spark.
- **Referential Integrity**: Validate keys against dimensions using local reference tables.
- **Contract Inference**: Auto-generate contracts from landing-zone files with `lakelogic bootstrap`.
- **dbt Import**: Convert dbt `schema.yml` / `sources.yml` into LakeLogic contracts with `lakelogic import-dbt`.
- **Synthetic Data Generation**: Generate realistic test data from any contract with `DataGenerator`.
- **External Logic Hooks**: Run dedicated Python modules or notebooks for advanced Gold processing.
- **Policy Packs**: Apply standardised rule sets and defaults across all contracts.
- **Notifications**: Built-in adapters log alerts for quarantine and rule failures.
- **Observability**: Prometheus metrics endpoint, summary tables, and execution tracing.
- **Delta Lake Support (Spark-Free)**: Read/write/merge Delta tables with Polars, DuckDB, or Pandas — no Spark required.
- **Catalog Table Names**: Use Unity Catalog, Fabric LakeDB, and Synapse table names (`catalog.schema.table`) directly.
- **Streaming Ingestion**: Kafka, WebSocket, SSE, Azure Service Bus, GCP Pub/Sub, AWS SQS.
- **Database CDC**: Azure SQL, PostgreSQL, MySQL, MongoDB, Oracle, SQL Server change capture.

## Installation

```bash
# Get the full engine suite
uv pip install "lakelogic[all]"

# Or just use Polars for local speed
uv pip install "lakelogic[polars]"

# Delta Lake support (Spark-free)
uv pip install "lakelogic[delta]"

# Profiling + PII detection (bootstrap)
uv pip install "lakelogic[profiling]"

# Database CDC connectors
uv pip install "lakelogic[databases]"

# Streaming sources
uv pip install "lakelogic[streaming]"
```

See the full installation guide in `docs/installation.md`.

## Quick Start

```python
from lakelogic import DataProcessor

# 1. Run the Quality Gate (Automatic Engine Selection)
processor = DataProcessor(contract="silver_crm_customers.yaml")
good_df, bad_df = processor.run_source()

# good_df -> Ready for Silver Layer
# bad_df  -> Sent to Quarantine
```

`run_source()` automatically reads the source path from your contract. You can also pass an explicit path:

```python
good_df, bad_df = processor.run_source("bronze_crm_customers.csv")
```

The return value is a `ValidationResult` that unpacks as two DataFrames. Access the raw (pre-validation) frame via `result.raw`:

```python
result = processor.run_source()
print(f"Total: {len(result.raw)} | Valid: {len(result.good)} | Quarantined: {len(result.bad)}")
```

## Delta Lake & Catalog Support (Spark-Free!)

### **Unity Catalog (Databricks)**

```python
from lakelogic import DataProcessor

# Use Unity Catalog table names directly (no Spark required!)
processor = DataProcessor(engine="polars", contract="contracts/customers.yaml")
good_df, bad_df = processor.run_source("main.default.customers")

# LakeLogic automatically:
# 1. Resolves table name to storage path
# 2. Uses Delta-RS for fast, Spark-free operations
# 3. Validates data with your contract rules
```

### **Fabric LakeDB (Microsoft)**

```python
processor = DataProcessor(engine="polars", contract="contracts/sales.yaml")
good_df, bad_df = processor.run_source("myworkspace.sales_lakehouse.customers")
```

### **Synapse Analytics (Azure)**

```python
processor = DataProcessor(engine="polars", contract="contracts/sales.yaml")
good_df, bad_df = processor.run_source("salesdb.dbo.customers")
```

**Learn more:** [Delta Lake Support](docs/delta_lake_support.md) | [Catalog Table Names](docs/catalog_table_names.md)

## dbt Integration

Import existing dbt projects directly — no rewrite needed:

```bash
# Convert a dbt model to a LakeLogic contract
lakelogic import-dbt --schema models/schema.yml --model customers --output contracts/

# Or use the Python API
from lakelogic import DataProcessor
proc = DataProcessor.from_dbt("models/schema.yml", model="customers")
good_df, bad_df = proc.run_source()
```

## Get Started

**[📚 Read the Docs](https://LineageLogic.github.io/LakeLogic)** | **[🚀 Quickstart Guide](https://LineageLogic.github.io/LakeLogic/quickstart/)** | **[💬 Discussions](https://github.com/LineageLogic/LakeLogic/discussions)**

### Run Your First Contract (5 Minutes)

```bash
# Clone the repo
git clone https://github.com/LineageLogic/LakeLogic.git
cd LakeLogic/examples/01_quickstart

# Run the example
lakelogic run --contract users_contract.yaml --source data/sample_customers.csv
```

You'll see:
- ✅ Good records that passed validation
- ❌ Quarantined records with error reasons
- 📊 Quality metrics and health scores

## Explore the Examples

The [`examples/`](https://github.com/LineageLogic/LakeLogic/tree/main/examples) directory contains 24 runnable notebooks across 4 tested categories:

| Category | Directory | What You'll Learn |
|---|---|---|
| **Quickstart** | `01_quickstart/` | Your first contract in 5 minutes, database governance, dbt+PII |
| **Core Patterns** | `02_core_patterns/` | Medallion architecture, bronze quality gates, SCD2, deduplication, reference joins, soft deletes |
| **Advanced Workflows** | `03_advanced_workflows/` | Insurance ELT pipeline, GDPR compliance, late-arriving data, external Python logic, environment promotion, bootstrap, date dimensions, multi-tenant isolation, partitioned merge, payments lifecycle, streaming, synthetic data generation |
| **Compliance** | `04_compliance_governance/` | HIPAA PII masking |

> **Looking for more?** Additional examples for data sources, cloud platforms, orchestration, and production patterns are in `examples/_archive/`. These are functional but not yet fully tested.

## Documentation

- **[Full Documentation](https://LineageLogic.github.io/LakeLogic)** — Complete guides and API reference
- **[How It Works](https://LineageLogic.github.io/LakeLogic/concepts/)** — Medallion architecture and core concepts
- **[CLI Reference](https://LineageLogic.github.io/LakeLogic/cli/)** — Command-line usage
- **[API Reference](https://LineageLogic.github.io/LakeLogic/api/)** — Python API documentation
- **[Reprocessing Guide](https://LineageLogic.github.io/LakeLogic/reprocessing/)** — Handle late-arriving data
- **[Contract Template](docs/contract_template.md)** — Full YAML reference for all contract fields
- **[Streaming](docs/streaming_implementation_complete.md)** — Real-time ingestion guide

## Contributing

See `CONTRIBUTING.md` to get started, or `docs/installation.md#developer-installation` for environment setup.

---

### License

Apache-2.0
