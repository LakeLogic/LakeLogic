# Quickstart Tutorials

Choose your starting path to experience LakeLogic's governance-first data pipelines.

## Choose Your Path

### 1. [Hello World (Remote Data)](01_hello_world.ipynb)

**Time**: 1 minute
**Goal**: Experience LakeLogic with zero setup. Pull data from a remote URL and see it validated instantly.

* No local database required
* Demonstrates in-memory contracts
* Perfect for Google Colab/Kaggle

> **💡 Tip**: This demo uses the **Polars** engine by default for maximum speed with remote URLs. While LakeLogic supports **Spark**, the Spark engine on Windows has known limitations reading directly from `https://` CSV files. For remote URLs, stick with the default (Polars).

### 2. [Database Governance](02_database_governance.ipynb)

**Time**: 5 minutes
**Goal**: Learn how to protect your Lakehouse from "dirty" source databases.

* Uses a local SQLite database
* Demonstrates Quality Rules and Quarantining
* Shows "Shift-Left" data governance in action

## Running Locally (CLI)

You can also run these examples directly from your terminal:

```bash
# Run the database governance example
lakelogic run --contract users_contract.yaml
```

## Next Steps

* **[Core Patterns](../02_core_patterns/)**: Learn about Medallion Architecture and Reference Joins.
* **[Data Sources](../03_data_sources/)**: Explore specialized connectors for XML, Excel, and Streaming.
* **[Compliance](../08_compliance_governance/)**: Deep dive into PII masking and HIPAA patterns.
