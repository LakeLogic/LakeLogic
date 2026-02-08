# LakeGuard

**The Open-Source Runtime Engine for Data Contracts with Quarantine.**

LakeGuard is a SQL-first, infrastructure-agnostic quality gate that ensures your business decisions are based on data you can trust. It scales your validation logic from local Polars to petabyte-scale Spark without rewriting a single rule.

[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://LineageLogic.github.io/LakeGuard)
[![License](https://img.shields.io/badge/license-Apache%202.0-green)](LICENSE)

---

## The Core Value: Write Once. Run Anywhere

Stop paying the **"Infrastructure Lock-In Tax."** In a traditional stack, moving from a Warehouse (Snowflake) to a Lakehouse (Databricks) means months of rewriting validation rules. LakeGuard decouples your **Business Logic** from your **Execution Engine**.

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

## Installation

```bash
# Get the full engine suite
uv pip install "lakeguard[all]"

# Or just use Polars for local speed
uv pip install "lakeguard[polars]"

# Profiling + PII detection (bootstrap)
uv pip install "lakeguard[profiling]"
```

See the full installation guide in `docs/installation.md`.

## Quick Start

```python
# 1. Run the Quality Gate (Automatic Engine Selection)
processor = DataProcessor(contract="silver_crm_customers.yaml")
good_df, bad_df = processor.run_source("bronze_crm_customers.csv")

# good_df -> Ready for Silver Layer
# bad_df  -> Sent to Quarantine
```

## Documentation

Visit our documentation for:

- **Notebooks**:
  - [5-Minute Data Contract Quickstart](examples/tutorial_quickstart/quickstart_tutorial.ipynb)
  - Hands-on tutorials in the `examples/` folder.
- **Playbooks**: End-to-end scenarios with data, contracts, and notebooks.
- Concepts: `docs/concepts.md`
- Reprocessing: `docs/reprocessing.md`
- CLI: `docs/cli.md`

## Contributing

See `docs/installation.md#developer-installation` to get started.

---

### License

Apache-2.0
