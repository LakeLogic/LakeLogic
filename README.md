# LakeLogic

**Trust Your Data. Scale Your Logic.**

*Write Once. Run Anywhere.* — The open-source runtime for data contracts with quarantine.

LakeLogic is a SQL-first, infrastructure-agnostic quality gate that ensures your business decisions are based on data you can trust. It scales your validation logic from local Polars to petabyte-scale Spark without rewriting a single rule.

[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://LineageLogic.github.io/LakeLogic)
[![GitHub](https://img.shields.io/badge/GitHub-Repository-black?logo=github)](https://github.com/LineageLogic/LakeLogic)
[![License](https://img.shields.io/badge/license-Apache%202.0-green)](LICENSE)
[![Python](https://img.shields.io/badge/python-3.9+-blue?logo=python)](https://www.python.org)

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
- **Notifications (Demo)**: Built-in adapters log alerts for quarantine and rule failures.
- **External Logic Hooks**: Run dedicated Python modules or notebooks for advanced Gold processing.
- **🆕 Delta Lake Support (Spark-Free)**: Read/write/merge Delta tables with Polars, DuckDB, or Pandas—no Spark required!
- **🆕 Catalog Table Names**: Use Unity Catalog, Fabric LakeDB, and Synapse table names (`catalog.schema.table`) directly.

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
```

See the full installation guide in `docs/installation.md`.

## Quick Start

```python
# 1. Run the Quality Gate (Automatic Engine Selection)
processor = DataProcessor(contract="silver_crm_customers.yaml")
source_df, good_df, bad_df = processor.run_source("bronze_crm_customers.csv")

# good_df -> Ready for Silver Layer
# bad_df  -> Sent to Quarantine
```

## 🆕 Delta Lake & Catalog Support (Spark-Free!)

### **Unity Catalog (Databricks)**

```python
from lakelogic import DataProcessor

# Use Unity Catalog table names directly (no Spark required!)
processor = DataProcessor(engine="polars", contract="contracts/customers.yaml")
source_df, good_df, bad_df = processor.run_source("main.default.customers")

# LakeLogic automatically:
# 1. Resolves table name to storage path
# 2. Uses Delta-RS for fast, Spark-free operations
# 3. Validates data with your contract rules

print(f"Total: {len(source_df)} | Valid: {len(good_df)} | Invalid: {len(bad_df)}")
```

### **Fabric LakeDB (Microsoft)**

```python
# Use Fabric table names directly
processor = DataProcessor(engine="polars", contract="contracts/sales.yaml")
source_df, good_df, bad_df = processor.run_source("myworkspace.sales_lakehouse.customers")

print(f"Total: {len(source_df)} | Valid: {len(good_df)} | Invalid: {len(bad_df)}")
```

### **Synapse Analytics (Azure)**

```python
# Use Synapse table names directly
processor = DataProcessor(engine="polars", contract="contracts/sales.yaml")
source_df, good_df, bad_df = processor.run_source("salesdb.dbo.customers")

print(f"Total: {len(source_df)} | Valid: {len(good_df)} | Invalid: {len(bad_df)}")
```

**Learn more:** [Delta Lake Support](docs/delta_lake_support.md) | [Catalog Table Names](docs/catalog_table_names.md)

## Get Started

**[📚 Read the Docs](https://LineageLogic.github.io/LakeLogic)** | **[🚀 Quickstart Guide](https://LineageLogic.github.io/LakeLogic/quickstart/)** | **[💬 Discussions](https://github.com/LineageLogic/LakeLogic/discussions)**

### Run Your First Contract (5 Minutes)

```bash
# Clone the repo
git clone https://github.com/LineageLogic/LakeLogic.git
cd LakeLogic/examples/01_getting_started/basic_validation

# Run the example
lakelogic run --contract contract.yaml --source data/sample_customers.csv
```

You'll see:
- ✅ Good records that passed validation
- ❌ Quarantined records with error reasons
- 📊 Quality metrics and health scores

## Explore 90+ Examples

The [`examples/`](https://github.com/LineageLogic/LakeLogic/tree/main/examples) directory contains runnable examples organized by skill level:

- **Getting Started** - Your first contract in 5 minutes
- **Tutorials** - Medallion architecture, reference joins, notifications
- **Patterns** - Bronze quality gates, SCD2, deduplication, late-arriving data
- **Production** - Complete insurance ELT pipeline with multi-entity contracts
- **Integrations** - Airflow, Prefect, Dagster, Databricks job templates

## Documentation

- **[Full Documentation](https://LineageLogic.github.io/LakeLogic)** - Complete guides and API reference
- **[How It Works](https://LineageLogic.github.io/LakeLogic/concepts/)** - Medallion architecture and core concepts
- **[CLI Reference](https://LineageLogic.github.io/LakeLogic/cli/)** - Command-line usage
- **[API Reference](https://LineageLogic.github.io/LakeLogic/api/)** - Python API documentation
- **[Reprocessing Guide](https://LineageLogic.github.io/LakeLogic/reprocessing/)** - Handle late-arriving data

## Contributing

See `docs/installation.md#developer-installation` to get started.

---

### License

Apache-2.0
