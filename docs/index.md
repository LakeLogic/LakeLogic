<div class="hero-section" markdown>

# Trust Your Data. <span style="color: var(--md-accent-fg-color);">Scale Your Logic.</span>

<p class="hero-subtitle">
Write Once. Run Anywhere. — SQL-first quality gates from Polars to petabytes.
</p>

<div class="hero-cta">
<a href="installation/" class="md-button md-button--primary">
Try in 60 Seconds
</a>
<a href="examples/01_hello_world/" class="md-button">
Hello World Notebook
</a>
<a href="https://github.com/LakeLogic/LakeLogic" class="md-button">
View on GitHub
</a>
</div>

</div>

---

## One Contract. Four Engines. Zero Rewrites.

=== "Polars"

    ```python
    from lakelogic import DataProcessor
    
    # Blazing-fast local processing
    processor = DataProcessor(
        contract="contract.yaml",
        engine="polars"
    )
    
    result = processor.run_source("data.csv")
    
    print(f"✅ {len(result.good)} validated")
    print(f"❌ {len(result.bad)} quarantined")
    ```

=== "Spark"

    ```python
    from lakelogic import DataProcessor
    
    # Petabyte-scale distributed processing
    processor = DataProcessor(
        contract="contract.yaml",
        engine="spark"
    )
    
    # Works with Delta Lake, Unity Catalog
    result = processor.run_source("catalog.schema.table")
    
    processor.materialize(result.good, result.bad)
    ```

=== "DuckDB"

    ```python
    from lakelogic import DataProcessor
    
    # Fast analytical SQL engine
    processor = DataProcessor(
        contract="contract.yaml",
        engine="duckdb"
    )
    
    result = processor.run_source("data.parquet")
    
    # 100% Reconciliation guaranteed
    assert len(result.raw) == len(result.good) + len(result.bad)
    ```

=== "Snowflake"

    ```python
    from lakelogic import DataProcessor
    
    # Direct warehouse execution
    processor = DataProcessor(
        engine="snowflake",
        contract="contract.yaml"
    )
    
    result = processor.run_source("ANALYTICS.SILVER.CUSTOMERS")
    ```

!!! tip "Interactive Examples"
    Jump straight into **executable Jupyter notebooks** that demonstrate LakeLogic's capabilities:
    
    - [Hello World](examples/01_hello_world.ipynb) - Remote data ingestion in 60 seconds
    - [Database Governance](examples/02_database_governance.ipynb) - Quarantine dirty data
    - [HIPAA & PII Masking](examples/hipaa_compliance.ipynb) - Compliance-ready data handling

---

## Delta Lake & Catalog Support (Spark-Free!)

LakeLogic automatically resolves catalog table names and uses **Delta-RS** for fast, Spark-free Delta Lake operations.

=== "Unity Catalog (Databricks)"

    ```python
    from lakelogic import DataProcessor
    
    # Use Unity Catalog table names directly (no Spark required!)
    processor = DataProcessor(
        engine="polars", 
        contract="contracts/customers.yaml"
    )
    
    good_df, bad_df = processor.run_source(
        "main.default.customers"
    )
    
    # LakeLogic automatically:
    # 1. Resolves table name to storage path
    # 2. Uses Delta-RS for fast, Spark-free operations
    # 3. Validates data with your contract rules
    
    print(f"Valid: {len(good_df)} | Invalid: {len(bad_df)}")
    ```

=== "Fabric LakeDB (Microsoft)"

    ```python
    from lakelogic import DataProcessor
    
    # Use Fabric table names directly
    processor = DataProcessor(
        engine="polars",
        contract="contracts/sales.yaml"
    )
    
    good_df, bad_df = processor.run_source(
        "myworkspace.sales_lakehouse.customers"
    )
    
    print(f"Valid: {len(good_df)} | Invalid: {len(bad_df)}")
    ```

=== "Synapse Analytics (Azure)"

    ```python
    from lakelogic import DataProcessor
    
    # Use Synapse table names directly
    processor = DataProcessor(
        engine="polars",
        contract="contracts/sales.yaml"
    )
    
    good_df, bad_df = processor.run_source(
        "salesdb.dbo.customers"
    )
    
    print(f"Valid: {len(good_df)} | Invalid: {len(bad_df)}")
    ```

---

## How It Works (In a Nutshell)

LakeLogic enforces **Data Contracts as Quality Gates** at every layer of your medallion architecture:

```text
┌──────────────────────────────────────────────────┐
│  📂 DATA SOURCE                                  │
│  CSV · Parquet · Delta · JSON · XML · Excel      │
│  APIs · URLs · Databases · Cloud Storage         │
└───────────────────────┬──────────────────────────┘
                        │
                        ▼
┌──────────────────────────────────────────────────┐
│  📜 CONTRACT.YAML                                │
│  Schema · Types · Nullability · Quality Rules    │
└───────────────────────┬──────────────────────────┘
                        │
              ┌─────────┴─────────┐
              │  DataProcessor    │
              │  .run_source()    │
              └─────────┬─────────┘
                        │
          ┌─────────────┼─────────────┐
          ▼             ▼             ▼
  ┌───────────┐ ┌───────────┐ ┌───────────┐
  │  Polars   │ │  Spark    │ │  DuckDB   │  Same contract,
  │  (local)  │ │  (cluster)│ │  (in-proc)│  any engine
  └─────┬─────┘ └─────┬─────┘ └─────┬─────┘
        └──────────────┼──────────────┘
                       │
          ┌────────────┼────────────┐
          ▼                         ▼
┌──────────────────┐     ┌──────────────────┐
│  ✅ good_df      │     │  ❌ bad_df       │
│  ────────────    │     │  ────────────    │
│  Schema valid    │     │  🛑 QUARANTINE   │
│  Rules passed    │     │  Every failed    │
│  Types correct   │     │  row saved with  │
│  Ready for next  │     │  failure reason  │
│  layer           │     │  ↻ Fix & replay  │
└────────┬─────────┘     └──────────────────┘
         │
         ▼
┌──────────────────────────────────────────────────┐
│  📊 PIPELINE ENRICHMENT                          │
│  ✓ Lineage injection (run_id, timestamps)        │
│  ✓ SLO checks (freshness, completeness)          │
│  ✓ Schema drift detection                        │
│  ✓ External logic (Python scripts / notebooks)   │
│  ✓ Materialization (Delta, Parquet, DB)           │
│  ✓ Run log (DuckDB audit trail)                  │
│  ✓ Notifications (alerts on quarantine/failure)   │
└──────────────────────────────────────────────────┘

Each layer in the medallion uses its own contract:

  🟤 BRONZE → Capture everything, catch obvious junk
  ⚪ SILVER → Full validation, business rules, dedup
  🟡 GOLD   → Aggregations, KPIs, analytics-ready

✨ Key Guarantees:
  • 100% Reconciliation: source_count = good_count + bad_count
  • Engine Agnostic: Same contract on Polars, Spark, DuckDB
  • No Silent Failures: Every bad row quarantined with reasons
  • Full Lineage: Source → Bronze → Silver → Gold, all traced
```

[:octicons-arrow-right-24: See detailed architecture](architecture_diagram.md)

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

## :material-microsoft-azure: Azure Reference Architecture

Go from **zero to production Lakehouse in under an hour.** The open-source [Azure Reference Architecture](https://github.com/LakeLogic/reference-architecture-azure-ecommerce) provisions a fully governed **Databricks + ADLS Gen2** platform with a single `terraform apply`.

<div class="grid cards" markdown>

-   :material-database-outline:{ .lg .middle } **Unity Catalog Medallion**

    ---

    Automated **Bronze → Silver → Gold** schemas with per-layer permissions. Isolated catalog, external locations, and storage credentials — all wired to your ADLS containers.

-   :material-harddisk:{ .lg .middle } **Volumes for Raw Data**

    ---

    **Non-Delta files** (CSV, JSON, XML, contracts) land in UC Volumes backed by ADLS. Stream raw data directly into Databricks without format conversion.

-   :material-folder-network:{ .lg .middle } **Standardized Data Lake**

    ---

    Terraform creates a consistent ADLS directory structure — `bronze/`, `silver/`, `gold/`, `quarantine/`, `nondelta/` — with domain-scoped subfolders for every business unit.

-   :material-shield-account:{ .lg .middle } **Auto Identity Sync**

    ---

    Account-level groups are automatically synced to workspaces with correct entitlements. Read-only, read-write, and owner permissions cascade from schemas to external locations.

-   :material-connection:{ .lg .middle } **External Locations & Connections**

    ---

    Service principal credentials, storage credentials, and external locations are provisioned as code. No manual Databricks UI clicks — everything is auditable and repeatable.

-   :material-bell-ring:{ .lg .middle } **Event-Driven Pipelines**

    ---

    **Event Grid** watches ADLS landing zones and triggers **Azure Functions** that run LakeLogic contracts automatically. Data arrives → pipeline fires → medallion layers populate.

</div>

```text
┌──────────────────────────────────────────────────────┐
│  terraform apply                                     │
│  ─────────────────────────────────────────────       │
│                                                      │
│  ADLS Gen2                    Databricks + UC        │
│  ─────────                    ───────────────        │
│  📂 bronze/                   🏷️ Catalog (isolated)  │
│  📂 silver/                   📋 Schemas per layer   │
│  📂 gold/                     🔗 External Locations  │
│  📂 quarantine/               🔑 Storage Credentials │
│  📂 nondelta/                 📦 Volumes             │
│     └─ _data/domain/system/   👥 Groups + ACLs       │
│     └─ _domains/contracts/    🔒 Secret Scopes       │
│  📂 uc-assets/catalog/        ⚡ UC Cluster          │
│                                                      │
│  + Key Vault · VNet · Private Endpoints · Event Grid │
└──────────────────────────────────────────────────────┘
```

[:octicons-arrow-right-24: View the Reference Architecture](https://github.com/LakeLogic/reference-architecture-azure-ecommerce)

---

## Why LakeLogic?

### Write Once. Run Anywhere.

Stop paying the "Re-adaptation Tax." In a traditional stack, moving from a Warehouse (SQL) to a Lakehouse (PySpark) means rewriting your validation rules. With LakeLogic, your **Data Contract is the Source of Truth**.

- **SQL-First:** Define your constraints, rules, and logic in standard SQL—the language your team already speaks.
- **Zero Adaptation:** Move your pipelines from **dbt/Snowflake** to **Databricks/Spark** to **Local/Polars** with **zero changes** to your contract.
- **No Vendor Lock-in:** Your business logic is a portable asset, independent of your cloud provider or execution engine.

### Business ROI: Cost, Risk, & Trust

!!! success "Eliminate the Spark Tax"
    Cut compute spend by up to 80% for maintenance and small-to-medium datasets by using Polars or DuckDB instead of Spark.

!!! info "100% Reconciliation"
    Mathematically provable data integrity. Bad data is detoured into a **Safe Quarantine** area, ensuring production dashboards are never poisoned.

!!! tip "Visual Traceability"
    Gold-layer metrics should never be "Black Boxes." LakeLogic supports aggregate roll-ups that preserve source keys, providing business users with a visual drill-down from board-level KPIs back to the raw source records.

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
uv pip install "lakelogic[all]"

# Run your first contract (auto-discovers the best engine)
lakelogic run --contract my_contract.yaml --source raw_data.parquet
```

---

## Scale with LakeLogic.org

LakeLogic is the open-source engine that enforces your rules. For enterprise-scale management, **[LakeLogic.org](https://lakelogic.org)** provides:

- **AI-Powered Contract Generation:** Don't write YAML by hand; generate it from your data in seconds.
- **Visual Governance:** See your real-time data health and lineage across your entire mesh.
- **Collaborative Approvals:** Manage contract lifecycle and versioning across decentralized teams.

---

[Quickstart](quickstart.md) | [How It Works](concepts.md) | [Patterns](deployment_patterns.md) | [CLI Usage](cli.md)
