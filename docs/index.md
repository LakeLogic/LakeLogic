# LakeGuard

The fastest path from data ingestion to production-ready quality gates with Polars, Spark, DuckDB, Pandas, Snowflake, and more.

=== "Python"

    ```python
    from lakeguard import DataProcessor
    
    processor = DataProcessor(
        contract="contract.yaml",
        engine="polars"  # or spark, duckdb, pandas
    )
    
    source_df, good_df, bad_df = processor.run_source("data.csv")
    
    print(f"📊 {len(source_df)} source records")
    print(f"✓ {len(good_df)} validated records")
    print(f"✗ {len(bad_df)} quarantined records")
    ```

=== "CLI"

    ```bash
    # Install LakeGuard
    pip install "lakeguard[all]"
    
    # Run your first contract
    lakeguard run \
      --contract contract.yaml \
      --source data.csv \
      --output-good validated.csv \
      --output-bad quarantine.csv
    ```

=== "YAML Contract"

    ```yaml
    version: "1.0.0"
    dataset: customer_data
    
    model:
      fields:
        - name: email
          type: string
          required: true
        - name: age
          type: integer
    
    quality:
      row_rules:
        - not_null: email
        - regex_match:
            field: email
            pattern: "^[^@]+@[^@]+\\.[^@]+$"
        - range:
            field: age
            min: 18
            max: 120
    
    materialization:
      strategy: merge
      target_path: output/customers
      format: parquet
    ```

=== "Spark"

    ```python
    from lakeguard import DataProcessor
    
    # Auto-discovers Spark in Databricks/Synapse
    processor = DataProcessor(
        contract="contract.yaml"
    )
    
    # Works with Delta Lake, Iceberg, Unity Catalog
    source_df, good_df, bad_df = processor.run_source(
        "catalog.schema.table"
    )
    
    processor.materialize(good_df, bad_df)
    ```

=== "Snowflake"

    ```python
    from lakeguard import DataProcessor
    
    # Direct Snowflake execution (table-only)
    processor = DataProcessor(
        engine="snowflake",
        contract="contract.yaml"
    )
    
    source_df, good_df, bad_df = processor.run_source(
        "ANALYTICS.SILVER.CUSTOMERS"
    )
    ```

[Start building](quickstart.md){ .md-button .md-button--primary }

Follow our Quickstart guide to get started and make your first quality gate in minutes.

!!! tip "Explore More Examples"
    The [`examples/` directory](https://github.com/LineageLogic/LakeGuard/tree/main/examples) in the repo contains 90+ runnable examples organized by skill level:
    
    - **Getting Started**: Your first contract in 5 minutes
    - **Tutorials**: Medallion architecture, reference joins
    - **Patterns**: Bronze quality gates, SCD2, deduplication
    - **Production**: Complete insurance ELT pipeline
    - **Integrations**: Airflow, Prefect, Dagster, Databricks templates

---

## Meet the engines

<div class="grid cards" markdown>

-   :material-lightning-bolt:{ .lg .middle } **Polars**

    ---

    Blazing-fast local engine for single-node processing. Best for development, testing, and production workloads under 100GB.

    [:octicons-arrow-right-24: Learn more](capabilities.md)

-   :material-chart-timeline:{ .lg .middle } **Spark**

    ---

    Distributed processing for petabyte-scale data. Native support for Delta Lake, Iceberg, and Unity Catalog.

    [:octicons-arrow-right-24: Learn more](capabilities.md)

-   :material-database:{ .lg .middle } **DuckDB**

    ---

    Fast analytical SQL engine with native Iceberg and Delta support. Perfect for local development and CI/CD.

    [:octicons-arrow-right-24: Learn more](capabilities.md)

-   :material-snowflake:{ .lg .middle } **Snowflake & BigQuery**

    ---

    Direct warehouse execution with SQL pushdown. Table-only adapters for cloud data warehouses.

    [:octicons-arrow-right-24: Learn more](warehouse_adapters.md)

</div>

---

## Why LakeGuard?

### Write Once. Run Anywhere.

Stop paying the "Re-adaptation Tax." In a traditional stack, moving from a Warehouse (SQL) to a Lakehouse (PySpark) means rewriting your validation rules. With LakeGuard, your **Data Contract is the Source of Truth**.

- **SQL-First:** Define your constraints, rules, and logic in standard SQL—the language your team already speaks.
- **Zero Adaptation:** Move your pipelines from **dbt/Snowflake** to **Databricks/Spark** to **Local/Polars** with **zero changes** to your contract.
- **No Vendor Lock-in:** Your business logic is a portable asset, independent of your cloud provider or execution engine.

### Business ROI: Cost, Risk, & Trust

!!! success "Eliminate the Spark Tax"
    Cut compute spend by up to 80% for maintenance and small-to-medium datasets by using Polars or DuckDB instead of Spark.

!!! info "100% Reconciliation"
    Mathematically provable data integrity. Bad data is detoured into a **Safe Quarantine** area, ensuring production dashboards are never poisoned.

!!! tip "Visual Traceability"
    Gold-layer metrics should never be "Black Boxes." LakeGuard supports aggregate roll-ups that preserve source keys, providing business users with a visual drill-down from board-level KPIs back to the raw source records.

---

## Technical Capabilities

| Feature | Description |
| :--- | :--- |
| **Declarative Contracts** | Human-readable YAML defines schema, rules, and transforms. |
| **Engine Agnostic** | Auto-discovers and optimizes for Spark, Polars, DuckDB, or Pandas. |
| **SQL-First Rules** | Use standard SQL for Completeness, Correctness, and Consistency checks. |
| **Safe Quarantine** | Isolate bad rows without crashing the pipeline, with built-in reason codes. |
| **Lineage Injection** | Automatically audit every record with Run IDs, Timestamps, and Source paths. |
| **Registry Orchestration** | A generic driver to run Bronze → Silver → Gold layers with parallel execution. |

---

## Quick Start

The fastest way to get started is with **[uv](https://github.com/astral-sh/uv)**:

```bash
# Install with all engines
uv pip install "lakeguard[all]"

# Run your first contract (auto-discovers the best engine)
lakeguard run --contract my_contract.yaml --source raw_data.parquet
```

---

## Scale with LineageLogic

LakeGuard is the open-source engine that enforces your rules. For enterprise-scale management, **[LineageLogic](https://lineagelogic.com)** provides:

- **AI-Powered Contract Generation:** Don't write YAML by hand; generate it from your data in seconds.
- **Visual Governance:** See your real-time data health and lineage across your entire mesh.
- **Collaborative Approvals:** Manage contract lifecycle and versioning across decentralized teams.

---

[Quickstart](quickstart.md) | [How It Works](concepts.md) | [Patterns](deployment_patterns.md) | [CLI Usage](cli.md)
