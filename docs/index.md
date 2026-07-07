---
title: LakeLogic — Open-Source Data Contracts
description: Define data trust once. Enforce it everywhere. Open-source, Git-native data contracts — define schema, quality, PII, lineage and SLOs once in YAML, validated at runtime and in CI across Polars, DuckDB, and Spark.
hide:
  - navigation
---
<div class="hero-section" markdown>

<div class="hero-content" markdown>
# Define data trust once. <span style="color: var(--md-accent-fg-color);">Enforce it everywhere.</span>

<p class="hero-subtitle" style="font-size: 1.3rem; font-weight: 500;">
Bad data breaks dashboards and decisions — usually too late.<br>
LakeLogic is the open-source framework for <strong>Git-native data contracts</strong>: validate at runtime, block bad merges in CI — before bad data ships.
</p>

<p class="hero-keyword-anchor" style="font-size: 0.95rem; opacity: 0.8; line-height: 1.5;" markdown="1">
Define schema, data quality rules, lineage, SLOs, PII handling, and materialization once in YAML — then run the same contract on [Polars](https://pola.rs/){: target="_blank" }, [DuckDB](https://duckdb.org/){: target="_blank" }, or [Spark](https://spark.apache.org/){: target="_blank" }.<br><br>
<strong style="color: var(--md-accent-fg-color); font-size: 1.05rem;">Git-native · in your repo &nbsp;·&nbsp; Zero-egress · runs in your lakehouse &nbsp;·&nbsp; Open core · Apache 2.0</strong>
</p>

<div class="hero-cta" markdown>
[ :simple-googlecolab: Run 5-Minute Quickstart in Google Colab ](https://colab.research.google.com/github/lakelogic/LakeLogic/blob/main/examples/colab/00_quickstart.ipynb){: target="_blank" .md-button .md-button--primary .md-button--lg }
[ :simple-github: Start on GitHub ](https://github.com/lakelogic/LakeLogic){: .md-button .md-button--secondary }
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

=== "4. Enforce in CI/CD"

    ```bash title="Pre-deployment validation (no data needed)"
    lakelogic validate \
      --contract contract.yaml \
      --gates breaking_change,pii_classification,lineage_break
    ```

    ```yaml title=".github/workflows/contracts.yml"
    - name: Validate Contracts
      run: |
        lakelogic validate \
          --contract contracts/customers.yaml \
          --gates breaking_change,pii_classification
    ```

    Block PRs that introduce schema breaks, unmasked PII, or
    broken lineage — **before** they reach production.

</div>

</div>

---

## Quick Start

```bash
pip install "lakelogic"
```

> **Next step:** Jump straight into the **[Run 5-Minute Quickstart in Google Colab](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/00_quickstart.ipynb)** — run your first pipeline in 5 minutes (no local files required, it downloads sample data automatically).

---

## Data Contracts That Ship Business Outcomes

LakeLogic turns data contracts into executable controls for quality, governance, cost, and delivery speed. Instead of rewriting validation and materialization logic in every pipeline, teams declare the rules once and enforce them everywhere.

| Outcome | LakeLogic Capability | Business Value |
| --- | --- | --- |
| **Trusted dashboards** | Row-level quality rules, schema checks, and quarantine | Bad records are isolated before they reach reports, ML features, or downstream teams |
| **Faster delivery** | YAML contracts, SQL-first transformations, and CI/CD gates | Engineers ship reliable pipelines without repeating boilerplate validation code |
| **Lower platform cost** | Spark, Polars, and DuckDB execution from the same contract | Use lightweight engines where they fit instead of defaulting every job to a cluster |
| **Audit-ready data** | Lineage stamps, ownership metadata, SLOs, and PII handling | Every dataset carries the evidence needed for governance, compliance, and incident review |
| **Portable lakehouse pipelines** | Delta Lake, Iceberg, Parquet, CSV, and catalog-aware materialization | Contracts move across tools and storage formats without rewriting business rules |

### Data Quality & Trust

- **Prevent bad data from reaching production** — enforce schema, required fields, SQL rules, SLOs, anomaly checks, and quarantine behavior from the contract.
- **Account for every row** — reconcile `source = good + bad` so records are validated, accepted, or quarantined without silent loss.
- **Catch broken contracts early** — strict model validation and CI/CD gates block unsafe schema, PII, and lineage changes before deployment.

[ :simple-googlecolab: Run the Data Quality & Trust Guide in Google Colab ](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/01_data_quality_trust.ipynb){: target="_blank" .md-button }

### Compliance & Governance

- **Make ownership explicit** — domain, system, and contract files define owners, contacts, SLOs, lineage, and governance rules.
- **Protect sensitive data by default** — declarative PII handling, masking, encryption, and subject-forgetting workflows are part of the pipeline.
- **Track operating cost** — attribute compute cost by entity and domain so governance includes spend, not just policy.

[ :simple-googlecolab: Run the Compliance & Governance Guide in Google Colab ](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/02_compliance_governance.ipynb){: target="_blank" .md-button }

### Engine & Scale

- **Avoid engine lock-in** — run the same contract on [Spark](https://spark.apache.org/){: target="_blank" }, [Polars](https://pola.rs/){: target="_blank" }, or [DuckDB](https://duckdb.org/){: target="_blank" }.
- **Materialize without hand-coded merge logic** — declare append, merge/upsert, SCD2, snapshots, partitioning, Delta Lake, and Iceberg behavior in YAML.
- **Operate production pipelines** — use incremental loads, CDC watermarks, backfills, retries, timeouts, circuit breakers, and dependency-aware execution.

[ :simple-googlecolab: Run the Engine & Scale Guide in Google Colab ](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/03_engine_scale.ipynb){: target="_blank" .md-button }

### Developer Experience

- **Debug faster** — structured logs, dry runs, execution previews, DAG views, and reset/reload commands reduce pipeline triage time.
- **Automate schema operations** — generate and apply DDL from contracts without running the full pipeline.
- **Route alerts to the right owners** — send contract-aware notifications through Slack, email, Teams, webhooks, and other Apprise targets.

[ :simple-googlecolab: Run the Developer Experience Guide in Google Colab ](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/04_developer_experience.ipynb){: target="_blank" .md-button }

### Data Generation & AI

- **Test quality rules before production** — generate realistic synthetic records, edge cases, and referentially valid datasets from the contract.
- **Bootstrap contracts faster** — infer contracts from sample data with AI-assisted descriptions, PII detection, and quality rule suggestions.
- **Apply contracts to unstructured data** — validate LLM extraction from PDFs, images, and audio with the same lineage and quality controls.

[ :simple-googlecolab: Run the Data Generation & AI Guide in Google Colab ](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/05_data_generation_ai.ipynb){: target="_blank" .md-button }

### Integrations

- **Reuse existing definitions** — import dbt `schema.yml` models and sources as LakeLogic contracts.
- **Ingest from operational systems** — connect to dlt sources, REST APIs, databases, Kafka, webhooks, cloud storage, and streaming feeds.
- **Extract only what the contract declares** — apply projection pushdown, chunked reads, CDC filters, and cloud credential resolution automatically.

[ :simple-googlecolab: Run the Integrations Guide in Google Colab ](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/06_integrations.ipynb){: target="_blank" .md-button }

---

## Define Once. Enforce Everywhere.

LakeLogic makes your **Data Contract the Single Source of Truth**. One YAML file replaces hundreds of lines of ingestion, validation, and materialization code, and it runs on any engine.

Contracts enforce on **two surfaces**:

- **Runtime** — `processor.run()` validates rows, quarantines bad data, emits a `quality_score`, and writes lineage stamps automatically.
- **CI/CD** — `lakelogic validate --gates` runs static analysis on contract changes and blocks PRs that break the schema, expose PII, or sever upstream lineage.

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

---

## Data Mesh Without the Slideware

LakeLogic makes data mesh practical by turning ownership, quality, SLOs, and governance into files that run in production. The `domain → system → contract` hierarchy keeps responsibility close to the teams that know the data.

| Data Mesh Principle | LakeLogic Implementation |
| --- | --- |
| **Domain Ownership** | `_domain.yaml` declares owners, contacts, alerts, budgets, and SLOs |
| **Data as a Product** | Each contract defines schema, quality rules, lineage, and service guarantees |
| **Self-Serve Platform** | Teams write YAML and run pipelines without waiting on central platform tickets |
| **Federated Governance** | Shared rules inherit across domains and systems without creating a bottleneck |

[:octicons-arrow-right-24: Learn how to organize contracts at scale](organization.md) · [:octicons-arrow-right-24: Read the data mesh guide](https://lakelogic.org/blog/data-mesh-data-contracts/){: target="_blank" }


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
