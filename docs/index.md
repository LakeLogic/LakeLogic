<div class="hero-section" markdown>

# Your Data Estate. <span style="color: var(--md-accent-fg-color);">Under Contract.</span>

<p class="hero-subtitle">
Open-source data contract library for Python — catch breaking data changes before they reach production.
</p>

<div class="hero-cta">
<a href="examples/01_hello_world/" class="md-button md-button--primary">
Try in 60 Seconds
</a>
<a href="https://github.com/lakelogic/LakeLogic" class="md-button">
View on GitHub
</a>
</div>
---

## 🌐 Data Mesh Alignment

LakeLogic is built for the decentralized data estate, directly supporting the four pillars of **Data Mesh**:

- **Domain Ownership**: Contracts are owned and defined by domain teams (e.g., CRM, Finance) who know the data best.
- **Data as a Product**: Contracts serve as the explicit "product interface," guaranteeing quality for consumers.
- **Self-Serve Platform**: A standardized runtime that any team can use to deploy quality gates without infra silos.
- **Federated Governance**: Global standards (e.g., PII masking) are defined centrally but enforced locally at every layer.

---

## ✅ Define Once. Enforce Everywhere.

LakeLogic makes your **Data Contract the Single Source of Truth**.

### `contract.yaml` — this is your entire quality gate
```yaml
# REQUIRED: Contract version for compatibility tracking
version: "1.0"

# REQUIRED: Metadata for domain and system classification
info:
  title: Silver Customers
  owner: data-team
  domain: CRM
  system: Salesforce

# REQUIRED: Schema definition with types and constraints
model:
  fields:
    - name: customer_id
      type: integer
      required: true
    - name: first_name
      type: string
      pii: true  # Business value: Automatic PII detection and masking
    - name: last_name
      type: string
      pii: true
    - name: email
      type: string
      pii: true
    - name: revenue
      type: float
    - name: status
      type: string

# REQUIRED: Data acquisition pattern and location
source:
  type: landing
  path: "data/customers/*.csv"  # Supports: Files (S3/ADLS) or DB Tables (e.g., Unity Catalog)
  load_mode: incremental

# OPTIONAL EXAMPLE: Data cleaning, enrichment, and deduplication logic
transformations:
  - rename:
      from: "cust_id"
      to: "customer_id"
    phase: "pre"  # PRE: Applied before schema validation (fixes source naming drift)
  - deduplicate:
      columns: ["customer_id"]
      order_by: "updated_at"
  - sql: |
      SELECT 
        *,
        UPPER(status) as status_code,
        revenue * 0.1 as tax_estimate
      FROM source
    phase: "post" # POST: Applied after validation (complex logic and enrichment)

# OPTIONAL EXAMPLE: Validation rules (row-level and dataset-level)
quality:
  row_rules:
    - sql: "customer_id IS NOT NULL AND email IS NOT NULL"
    - sql: "status IN ('active', 'churned', 'pending')"
    - sql: "revenue >= 0"
    - sql: "email LIKE '%@%.%'"
  dataset_rules:
    - unique: "customer_id"

# OPTIONAL EXAMPLE: Data provenance and audit injection
lineage:
  enabled: true

# REQUIRED: Output storage and write strategy
materialization:
  strategy: merge
  target_path: "silver/customers"  # Supports: File paths or DB Tables (e.g., Unity Catalog)
  format: delta
  merge_keys: [customer_id]

# OPTIONAL EXAMPLE: Isolation for failed records
quarantine:
  enabled: true
  target: "quarantine/customers"
```

!!! tip "Full Contract Reference"
    Explore the **[Complete Contract Template](contract_template.md)** showing every available configuration option for Bronze, Silver, and Gold layers.

---

## Meet the Engines

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


!!! tip "Interactive Examples"
    Jump straight into **executable Jupyter notebooks** that demonstrate LakeLogic's capabilities:
    
    - [Hello World](examples/01_hello_world.ipynb) - Remote data ingestion in 60 seconds
    - [Database Governance](examples/02_database_governance.ipynb) - Quarantine dirty data
    - [HIPAA & GDPR Compliance](examples/hipaa_gdpr_compliance.ipynb) - PII masking, consent tracking, and multi-regulation governance
    - [AI Contract Enrichment](examples/ai_enrich_demo.ipynb) - Generate field descriptions, PII flags, and quality rules with AI

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

  🟤 BRONZE → Capture everything raw, no validation
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

## Why LakeLogic?

### Stop the "Fragmented Truth" Problem
In a traditional data stack, moving from a Warehouse (SQL) to a Lakehouse
(PySpark) means rewriting your validation rules. This duplication creates
**Logic Drift** — where your data quality standards differ depending on
which tool is running the code.

With LakeLogic, your **Data Contract is the Source of Truth**.

- **SQL-First Simplicity**: Define your constraints and business logic in standard SQL—the language your team already speaks.
- **Zero-Friction Portability**: Move your pipelines from **dbt/Snowflake** to **Databricks/Spark** to **Local/Polars** with zero changes to your contract.
- **True Ownership**: Your business logic is a portable asset, independent of your cloud provider or execution engine.

### Business Impact: Trust, Speed, and ROI

!!! success "Slash Compute Costs"
    Not every job needs a massive Spark cluster. Reduce compute spend by up to 80% for maintenance tasks and small-to-medium datasets by using high-performance engines like Polars or DuckDB.

!!! info "Guaranteed Integrity"
    LakeLogic detours bad data into a **Safe Quarantine** zone with absolute precision. This ensures downstream dashboards are never poisoned by "dirty" data, maintaining stakeholder trust.

!!! tip "Full Pipeline Transparency"
    Eliminate the "Black Box" problem. LakeLogic provides visual drill-downs from board-level KPIs back to the raw source records, ensuring every number is auditable and explainable.

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

## Go Further with LakeLogic

LakeLogic is the open-source engine that enforces your data contracts.
Here's how to get the most out of it:

- :material-robot-outline: **AI-Powered Contract Generation:** [Bootstrap
  contracts](llm_extraction.md) from raw data with `--ai` — field descriptions, PII detection,
  and SQL quality rules generated in seconds.
- :material-file-tree: **Governance at Scale:** Learn how to [organize your 
  contracts](organization.md) for 1,000s of tables using Domain-First ownership 
  and Registries.
- :material-card-text-outline: **Contract Reference:** Explore the [Complete 
  Contract Template](contract_template.md) showing every available 
  configuration option for Bronze, Silver, and Gold layers.
- :material-molecule: **Detailed Architecture:** Explore how LakeLogic enforces
  [Quality Gates across the Medallion Architecture](architecture_diagram.md)
  including Quarantine logic, Lineage, and multi-engine support.
- :material-test-tube: **Synthetic Test Data:** Generate realistic edge-case
  data from your contracts to stress-test quarantine rules before production.
- :material-web: **Project Hub:** Visit **[lakelogic.org](https://lakelogic.org)**
  for the latest guides, blog posts, and community resources.

---

## From the Blog

!!! abstract "Latest Posts"
    - [**Data Quality Management Without the Platform Tax**](https://lakelogic.org/blog/data-quality-management/)
      — Why YAML contracts beat enterprise DQM platforms on cost, flexibility, 
      and version control.
    - [**Row-Level Data Quality in Polars — Without Writing Validation Code**](https://lakelogic.org/blog/polars-data-quality/)
      — One YAML file replaces 200 lines of Polars validation boilerplate.
    - [**Data Mesh Without the Chaos**](https://lakelogic.org/blog/data-mesh-data-contracts/)
      — How data contracts make domain ownership work at enterprise scale.
    - [**Stop the Spark Tax**](https://lakelogic.org/blog/stop-the-spark-tax/)
      — One data contract, any engine — eliminate logic drift 
      between Spark, Polars, and DuckDB.

---

[Quickstart](examples/01_hello_world.ipynb) | [How It Works](concepts.md) | [Patterns](deployment_patterns.md) | [CLI Usage](cli.md)
