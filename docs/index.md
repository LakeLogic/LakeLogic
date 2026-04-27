---
hide:
  - navigation
---
<div class="hero-section" markdown>

<div class="hero-content" markdown>
# Your Data Estate. <span style="color: var(--md-accent-fg-color);">Under Contract.</span>

<p class="hero-subtitle" style="font-size: 1.3rem; font-weight: 500;">
A declarative, contract-driven medallion<br>
pipeline engine for data mesh architectures.
</p>

<p class="hero-keyword-anchor" style="font-size: 0.95rem; opacity: 0.8; line-height: 1.5;" markdown="1">
Describe your data products in YAML — LakeLogic materializes them as Delta/Iceberg tables with lineage, quality, and SCD2 built in.<br><br>
Write once. Run on [Spark](https://spark.apache.org/){: target="_blank" }, [Polars](https://pola.rs/){: target="_blank" }, or [DuckDB](https://duckdb.org/){: target="_blank" }.<br>
<strong style="color: var(--md-accent-fg-color); font-size: 1.05rem;">The vendor-neutral alternative to Databricks Lakeflow Pipelines.</strong>
</p>

<div class="hero-cta" markdown>
[ :simple-googlecolab: Run 5-Minute Quickstart in Google Colab ](https://colab.research.google.com/github/lakelogic/LakeLogic/blob/main/examples/colab/00_quickstart.ipynb){: target="_blank" .md-button .md-button--primary .md-button--lg }
[ :simple-github: View on GitHub ](https://github.com/lakelogic/LakeLogic){: .md-button .md-button--secondary }
</div>
</div>

<div class="hero-visual" markdown>
=== "1. Define Contract"

    ```yaml title="contract.yaml"
    # 1. Read incrementally from cloud storage
    source:
      path: s3://landing/customers/*.json
      load_mode: incremental
      watermark_strategy: pipeline_log  # Only process files newer than last run

    # 2. Enforce schema & PII masking
    model:
      fields:
        - name: cus_id
          type: string
          required: true
        - name: email
          required: true
          pii: true
          masking: "encrypt"            # AES-256 via LAKELOGIC_PII_KEY env var

    # 3. Apply SQL transformations
    transformations:
      - sql: "LOWER(TRIM(email)) AS email"

    # 4. Enforce quality & SLO guarantees
    quality:
      row_rules:
        - sql: "email LIKE '%@%.%'"
    service_levels:
      freshness_hours: 24

    # 5. Write 100% clean data directly to Catalog
    materialization:
      strategy: merge
      primary_key: [cus_id]
      target_path: catalog.silver.customers
      format: iceberg  # natively supports iceberg, delta, parquet, csv
    ```

=== "2. Run Pipeline"

    ```bash title="Standard CLI"
    lakelogic run contract.yaml
    ```
    
    ```python title="Python / Databricks"
    from lakelogic import DataProcessor

    proc = DataProcessor("contract.yaml")
    
    # Executes the contract end-to-end
    result = proc.run()
    ```

=== "3. View Output"

    ```log title="Execution Logs"
    LakeLogic Alert: 2 records quarantined in 'customers'. Total: 4
    [2026-03-28 12:00:01] INFO  | Wrote 2 quarantined rows to catalog.quarantine.silver_customers
    [2026-03-28 12:00:02] INFO  | Wrote 2 valid rows to catalog.silver.customers
    [2026-03-28 12:00:03] INFO  | Run complete [layer=silver] | Total: 4 | Good: 2 | Quarantine: 2 | Ratio: 50.0%
    ```

    **✅ `result.good` (Passed Quality Gate & PII Masked)**
    
    | cus_id | email                              |
    |--------|------------------------------------|
    | C100   | `enc:a1F3bG9nZ2VkQGV4...`          |
    | C101   | `enc:dXNlcjEwMUBjb3Jw...`          |
    
    **🚨 `result.bad` (Quarantined by LakeLogic)**
    
    | cus_id | email          | _lakelogic_categories           | _lakelogic_errors                                |
    |--------|----------------|---------------------------------|--------------------------------------------------|
    | C102   | not_an_email   | `["correctness"]`               | `["Rule failed: email LIKE '%@%.%'"]`            |
    | C103   | null           | `["completeness"]`              | `["Rule failed: email is required"]`             |
</div>

</div>

---

## Quick Start

```bash
pip install "lakelogic"
```

> **Next step:** Jump straight into the **[Run 5-Minute Quickstart in Google Colab](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/00_quickstart.ipynb)** — run your first pipeline in 5 minutes (no local files required, it downloads sample data automatically).

---

## Data Mesh Is Structural — Not Just a Principle

Data mesh isn't a buzzword in LakeLogic — it's the **architecture**. The `domain → system → contract` hierarchy enforces ownership boundaries at every level:

```
🏢 Domain (Marketing, Sales, Finance)
│   "Who owns this data?"
│   → _domain.yaml — ownership, SLOs, contacts, alerts
│
├── 🏗️ System (Google Analytics, Salesforce, SAP)
│   "Where does this data come from?"
│   → _system.yaml — storage, environments, settings
│
└── 📄 Data Product (events, customers, orders)
    "What does this specific table look like?"
    → entity_v1.0.yaml — schema, quality rules, transforms
```

> **Analogy:** A domain is like a department (Marketing). A system is like a tool that department uses (Google Analytics). A data product is like a specific report from that tool (website sessions).

| Data Mesh Principle | What It Means (Plain English) | How LakeLogic Enables This |
| --- | --- | --- |
| **Domain Ownership** | The people closest to the data own it | `_domain.yaml` names the owner, their contacts, and cost centre |
| **Data as a Product** | Treat each dataset like a product with quality guarantees | Each contract declares schema, quality rules, and SLOs |
| **Self-Serve Platform** | Give teams tools so they don't wait on a central team | Write YAML → run pipeline. No tickets, no handoffs |
| **Federated Governance** | Consistent rules without a bottleneck | Domain-level SLOs inherited automatically by every table |



---

## Define Once. Enforce Everywhere.

LakeLogic makes your **Data Contract the Single Source of Truth**. One YAML file replaces hundreds of lines of ingestion, validation, and materialization code, and it runs on any engine.

> **Think of a contract like a building code.** The architect (data engineer) writes the spec once. Every builder (Spark, Polars, DuckDB) follows the same code — no matter which team or tool runs the pipeline.

| What the Contract Defines | Why It Matters |
| --- | --- |
| **Schema** (fields, types, PII flags) | Catches type mismatches and schema drift before they hit your dashboard |
| **Source** (where to read, how to load) | Declarative ingestion — no boilerplate code |
| **Transformations** (SQL-first) | Business logic lives in the contract, not scattered across notebooks |
| **Quality rules** (row + dataset) | Bad data quarantined automatically, never silently dropped |
| **Materialization** (merge, append, SCD2) | Write strategy declared, not coded |
| **SLOs** (freshness, completeness, anomalies, schedule) | Data reliability promises enforced and tracked |
| **Lineage** (source, run_id, timestamps) | Every row stamped automatically for audit trails |
| **Compliance** (GDPR, EU AI Act) | Regulatory metadata baked into the data layer |

[:octicons-arrow-right-24: See the full contract reference](contracts/data_product_contracts/index.md) · [:octicons-arrow-right-24: Complete annotated template](contract_template.md)

---

## Technical Capabilities

### Data Quality & Trust

- **100% Reconciliation** — Mathematically guaranteed: `source = good + bad`. Every row is accounted for — nothing silently dropped
- **[Pydantic](https://docs.pydantic.dev/){: target="_blank" }-Powered Validation** — Every contract, system & domain configs are parsed through strict Pydantic models with `Literal` type enforcement — invalid YAML is caught at load time, not at runtime
- **SQL-First Rules** — Define business logic in the language your team already speaks — no SDK, no custom DSL
- **SLO Monitoring & Anomaly Detection** — Native freshness, row count, and statistical anomaly detection with automatic multi-channel alerting when thresholds breach
- **Schema Drift Protection** — Configurable `schema_policy` controls how the pipeline reacts to unknown columns and schema evolution — default `"allow"` for frictionless prototyping, opt in to `"strict"` / `"quarantine"` to lock down production contracts

[ :simple-googlecolab: Run the Data Quality & Trust Guide in Google Colab ](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/01_data_quality_trust.ipynb){: target="_blank" .md-button }

### Compliance & Governance

- **GDPR & HIPAA Compliance** — Contract-driven `forget_subjects()` with nullify, hash, or redact strategies and immutable audit trail
- **Zero-Retention Architecture** — Built-in `zero_retention_days` enforcement for transient data layers, automatically purging micro-batches after successful downstream processing
- **Automated PII Handling** — Declarative encryption and hashing (`pii: true`, `masking: "encrypt"`) applied at the Bronze layer before data even reaches rest
- **Pipeline Cost Intelligence** — Per-entity compute cost attribution with domain-level budget governance, autoscaling-aware estimation, and Databricks Unity Catalog billing integration

[ :simple-googlecolab: Run the Compliance & Governance Guide in Google Colab ](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/02_compliance_governance.ipynb){: target="_blank" .md-button }

### Engine & Scale

- **Engine Agnostic** — Write once, run on [Spark](https://spark.apache.org/){: target="_blank" }, [Polars](https://pola.rs/){: target="_blank" }, or [DuckDB](https://duckdb.org/){: target="_blank" } — same contract, zero code changes
- **Multi-Format Materialization** — Natively output validated data to **Apache Iceberg** or **Delta Lake** open-table formats without requiring pipeline rewrites
- **Dimensional Modeling** — Native SCD Type 2 (slowly changing dimensions), merge/upsert (SCD1), append-only fact tables, periodic snapshot overwrites, and partition-aware writes — all declared in YAML, no manual `MERGE INTO` SQL required
- **Incremental-First** — Built-in watermarking, CDC, and file-mtime tracking
- **Parallel Processing** — Concurrent multi-contract execution with data-layer-aware orchestration and topological dependency ordering
- **Backfill & Reprocessing** — Targeted late-arriving data reprocessing with partition-aware filters — no full reload required
- **External Logic** — Plug in custom Python scripts or notebooks for complex Gold-layer transformations while preserving full contract validation and lineage
- **Production Resilience** — Built-in exponential-backoff retries, per-entity timeouts, and circuit-breaker thresholds (`max_consecutive_failures`) — pipelines self-heal transient failures without operator intervention

[ :simple-googlecolab: Run the Engine & Scale Guide in Google Colab ](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/03_engine_scale.ipynb){: target="_blank" .md-button }

### Developer Experience

- **Structured Diagnostics & Observability** — Deep contextual logging out-of-the-box (powered by [`loguru`](https://loguru.readthedocs.io/){: target="_blank" }) featuring precise timestamps, severity levels, exact function paths, and execution tags to drastically cut troubleshooting time
- **Dry Run Mode** — Validate contracts, resolve dependencies, and preview execution plans without touching any data
- **DDL-Only Mode** — Generate and apply schema DDL (CREATE/ALTER) from contracts without running the pipeline — perfect for CI/CD migrations
- **DAG Dependency Viewer** — Visualize cross-contract lineage and execution order before running — understand your pipeline graph at a glance
- **Data Reset & Reload** — Surgically reset and reload specific entities or data layers (Bronze/Silver/Gold) without impacting the rest of the lakehouse
- **Multi-Channel Alerts** — Powered by [Apprise](https://github.com/caronc/apprise){: target="_blank" } for Slack, Email (SMTP/SendGrid), Teams, and Webhook notifications with ownership-based auto-routing and full [Jinja2](https://jinja.palletsprojects.com/){: target="_blank" } templating support for custom formatting

[ :simple-googlecolab: Run the Developer Experience Guide in Google Colab ](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/04_developer_experience.ipynb){: target="_blank" .md-button }
### Data Generation & AI

- **Synthetic Data** — Built-in `DataGenerator` (powered by [Faker](https://faker.readthedocs.io/){: target="_blank" }) with streaming simulation, time-windowed output, referential integrity, and edge case injection — generate realistic error rows (SQL injection, type confusion, boundary values) for stress testing and quarantine validation
- **Descriptive AI Test Data** — Steer synthetic data generation with natural language prompts (e.g. *"Generate users who are French or Japanese only, enterprise-tier, over 60 years old with SQL injection attempts in email fields"*) — output strictly adheres to the YAML contract schema
- **AI Contract Onboarding** — `lakelogic infer` auto-generates contracts from sample data with LLM-powered enrichment: automatic PII detection, column labelling, and quality rule suggestions
- **Unstructured Processing** — LLM extraction from PDFs, images, audio with same contract validation + lineage
- **Automated Run Logs** — Every pipeline run emits structured JSON with row counts, quality scores, durations, and error details — queryable as a Delta table

[ :simple-googlecolab: Run the Data Generation & AI Guide in Google Colab ](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/05_data_generation_ai.ipynb){: target="_blank" .md-button }

### Integrations

- **[dbt](https://www.getdbt.com/){: target="_blank" } Adapter** — Import dbt `schema.yml` models and sources as LakeLogic contracts — reuse existing dbt definitions without rewriting
- **[dlt](https://dlthub.com/){: target="_blank" } (Data Load Tool)** — Native `DltAdapter` supporting 100+ verified sources (Stripe, Shopify, SQL databases, Google Analytics, and more) plus declarative REST API ingestion — all with contract-driven quality gates on arrival
- **Native Streaming Connectors** — Built-in `WebSocketConnector`, `SSEConnector`, `KafkaConnector`, `WebhookConnector` (plus Azure Event Grid, Service Bus, AWS SQS, GCP Pub/Sub) for real-time data feeds piped directly into contract validation with pre-validation rename transformations
- **Native Database Ingestion** — High-performance SQL extraction via [Polars/ConnectorX](https://pola.rs/){: target="_blank" } and [DuckDB](https://duckdb.org/){: target="_blank" } — supports PostgreSQL, MySQL, SQL Server, SQLite, and more with automatic dialect detection
- **Incremental CDC** — Watermark-based change data capture with automatic state tracking — injects `WHERE updated_at > last_watermark` into the SQL engine before data leaves the database
- **Batch Processing** — Memory-safe chunked ingestion via `fetch_size` for massive initial loads — processes 100GB+ tables without OOM errors
- **Column Projection Pushdown** — Automatically constructs precise `SELECT "col1", "col2"` queries from your contract's `model.fields` — only extracts what the contract declares, zero configuration
- **Cloud Data Sources** — Native `abfss://`, `s3://`, `gs://` URI support with automatic credential resolution via `CloudCredentialResolver` — Azure AD, AWS IAM roles, GCP ADC, service principals, and Databricks secret scopes all work out of the box

[ :simple-googlecolab: Run the Integrations Guide in Google Colab ](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/06_integrations.ipynb){: target="_blank" .md-button }

---

## Delta Lake & Catalog Support — Lightweight Mode

LakeLogic automatically resolves catalog table names and uses **Delta-RS** for fast, lightweight Delta Lake operations — no Spark cluster required.

=== "Unity Catalog (Databricks)"

    ```python
    from lakelogic import DataProcessor
    
    # Use Unity Catalog table names directly — lightweight mode
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


## Go Further with LakeLogic

LakeLogic is the open-source engine that enforces your data contracts.
Here's how to get the most out of it:

<div class="grid cards" markdown>

- :material-robot-outline: **[AI-Powered Contract Generation](llm_extraction.md)**
  <hr>
  Bootstrap contracts from raw data with `--ai` — descriptions, PII detection, and SQL rules generated in seconds.

- :material-file-tree: **[Governance at Scale](organization.md)**
  <hr>
  Learn how to organize your contracts for 1,000s of tables using Domain-First ownership and Registries.

- :material-card-text-outline: **[Contract Reference](contract_template.md)**
  <hr>
  Explore the complete template showing every available configuration option for Bronze, Silver, and Gold layers.

- :material-molecule: **[Detailed Architecture](architecture_diagram.md)**
  <hr>
  Explore how LakeLogic enforces Quality Gates across the Medallion Architecture including Quarantine logic.

- :material-test-tube: **[Synthetic Test Data](concepts.md)**
  <hr>
  Generate realistic edge-case data from your contracts to stress-test quarantine rules before production.

- :material-web: **[Project Hub](https://lakelogic.org)**
  <hr>
  Visit lakelogic.org for the latest guides, blog posts, and community resources.

</div>

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

[Quickstart](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/00_quickstart.ipynb) | [How It Works](concepts.md) | [Patterns](deployment_patterns.md) | [CLI Usage](cli.md)
