# LakeLogic Architecture: Medallion with Quality Gates

This diagram illustrates how LakeLogic enforces data contracts as **quality gates** across the medallion architecture (Bronze → Silver → Gold).

## High-Level Lifecycle

```mermaid
graph LR
    %% Class Definitions for Premium Feel
    classDef driver fill:#4e3e91,stroke:#3b2d71,color:#fff,rx:8,ry:8
    classDef registry fill:#1a1a1a,stroke:#444,stroke-dasharray: 5 5,color:#ccc
    classDef source fill:#10529c,stroke:#0d4380,color:#fff,rx:5,ry:5
    classDef bronze fill:#8d3d23,stroke:#6e301b,color:#fff,rx:5,ry:5
    classDef silver fill:#006151,stroke:#004a3e,color:#fff,rx:5,ry:5
    classDef gold fill:#8c6512,stroke:#6e4f0e,color:#fff,rx:5,ry:5
    classDef oss fill:#4e3e91,stroke:#3b2d71,color:#fff,rx:5,ry:5
    classDef logic fill:#006151,stroke:#004a3e,color:#fff,rx:5,ry:5
    classDef quarantine fill:#8c2020,stroke:#6e1a1a,color:#fff,rx:5,ry:5
    classDef infra fill:#3a3a3a,stroke:#555,color:#fff,rx:5,ry:5
    classDef label fill:none,stroke:none,color:#999

    subgraph Inputs [" "]
        direction TB
        LLM["<b>LLM</b><br/>Discovery"]:::driver
        Users["<b>Business</b><br/>BA · Power users"]:::driver
    end

    subgraph Contracts ["Contracts"]
        direction TB
        subgraph CRM ["Domain — CRM"]
            direction TB
            SF["Salesforce"]:::source --> C_BR1["Bronze"]:::bronze
            C_BR1 --> C_SL1["Silver"]:::silver
            C_SL1 --> C_GL1["Gold"]:::gold
            C_BR1 --- YAML1["yaml"]:::infra
        end
        subgraph MKT ["Domain — Marketing"]
            direction TB
            GA["Google Analytics"]:::source --> C_BR2["Bronze"]:::bronze
            C_BR2 --> C_SL2["Silver"]:::silver
            C_SL2 --> C_GL2["Gold"]:::gold
            C_BR2 --- YAML2["yaml"]:::infra
        end
    end

    Inputs --> Contracts

    subgraph Processing [" "]
        direction TB
        Logic["<b>Py Script /<br/>Custom Logic</b><br/>(Optional)"]:::logic
        OSS["<b>LakeLogic<br/>OSS</b>"]:::oss
        Logic --> OSS
    end

    Contracts --> Processing

    subgraph Lakehouse ["Lakehouse"]
        direction TB
        L_BR["Bronze"]:::bronze --> L_SL["Silver"]:::silver
        L_SL --> L_GL["Gold"]:::gold
        L_BR -.-> L_Q["Quarantine"]:::quarantine
    end

    OSS --> L_SL
    OSS --- E_LBL["Spark · Polars · Pandas · DuckDB"]:::label

    subgraph Targets ["Compute targets"]
        direction LR
        Srv["Serverless"]:::infra
        Spk["Spark"]:::infra
        K8s["Kubernetes"]:::infra
    end

    Processing -.-> Targets
```
---

## Detailed Medallion Flow

### 🟤 BRONZE LAYER: Raw Capture

**Goal**: 100% preservation of source data with zero silent drops.

```text
┌──────────────────────────────────────────────────────────┐
│              "Capture Everything Raw (No Validation)"     │
├──────────────────────────────────────────────────────────┤
│                                                           │
│  📋 Data Contract: bronze_contract.yaml                  │
│  ┌────────────────────────────────────────────────────┐ │
│  │ # No quality rules in Bronze                       │ │
│  │ # Goal is 100% capture of source data              │ │
│  └────────────────────────────────────────────────────┘ │
│                                                           │
│  📊 Strategy: overwrite or append                        │
│  💾 Output: bronze_customers.parquet                     │
└──────────────────────────┬───────────────────────────────┘
                           │
                           ▼
┌──────────────────────────┐      ┌──────────────────────┐
│ 🛡️ Quality Gate:          │      │ 🛑 QUARANTINE ZONE   │
│ Bronze → Silver          │      ├──────────────────────┤
│                          │      │ quarantine/          │
│ 1️⃣ Pre-Processing        │      │  bronze_bad.parquet  │
│   - rename columns       │      │                      │
│   - filter invalid rows  │      │ 📝 Error Reasons:    │
│   - deduplicate          │      │  - email_format      │
│   - trim, lower, cast    │      │  - age_positive      │
│                          │      │  - missing_id        │
│ 2️⃣ Schema Enforcement    │      │                      │
│   - Type validation      │      │ 🔄 Correction Loop:  │
│   - Required fields      │      │  1. Fix source data  │
│   - Unknown field policy │      │  2. Reprocess        │
│                          │      │  3. Flow to Silver   │
│ 3️⃣ Quality Rules         │      └──────────────────────┘
│   - Row: not_null, regex │
│   - Dataset: unique      │
│                          │
│ 4️⃣ Post-Processing       │
│   - derive fields        │
│   - lookup/join dims     │
└──────────┬───────────────┘
```

### ⚪ SILVER LAYER: Business Validation
**Goal**: Trusted, cleaned, and queryable data for bulk analytics.

```text
┌──────────────────────────────────────────────────────────┐
│          "Validated, Cleaned, Business-Ready"             │
├──────────────────────────────────────────────────────────┤
│                                                           │
│  📋 Data Contract: silver_contract.yaml                  │
│  ┌────────────────────────────────────────────────────┐ │
│  │ transformations:                                    │ │
│  │   - deduplicate:                                    │ │
│  │       on: ["customer_id"]                           │ │
│  │                                                      │ │
│  │ quality:                                             │ │
│  │   row_rules:             # Full validation          │ │
│  │     - not_null: email                               │ │
│  │     - regex_match:                                  │ │
│  │         field: email                                │ │
│  │         pattern: "^[^@]+@[^@]+\\.[^@]+$"            │ │
│  └────────────────────────────────────────────────────┘ │
│                                                           │
│  💾 Output: silver_customers.parquet / Delta / Iceberg   │
└──────┬──────────────────────────────────────────┬────────┘
       │                                           │
       │ ✓ PASSED                                  │ ✗ FAILED
       │                                           │
       ▼                                           ▼
┌──────────────────────────┐      ┌──────────────────────┐
│ 🛡️ Quality Gate:          │      │ 🛑 QUARANTINE ZONE   │
│ Silver → Gold            │      ├──────────────────────┤
│                          │      │ quarantine/          │
│ Contract Enforcement:    │      │  silver_bad.parquet  │
│  ✓ Schema validation     │      │                      │
│  ✓ Business rules        │      │ Reason codes logged  │
│  ✓ Referential integrity │      └──────────────────────┘
│  ✓ Statistical checks    │
└──────────┬───────────────┘
```

### 🟡 GOLD LAYER: Aggregated & Enrich
│       "Aggregated, Business KPIs, Analytics-Ready"        │
├──────────────────────────────────────────────────────────┤
│                                                           │
│  📋 Data Contract: gold_contract.yaml                    │
│  ┌────────────────────────────────────────────────────┐ │
│  │ # OPTION 1: SQL-Based Aggregation                  │ │
│  │ transformations:                                    │ │
│  │   - sql: |                                          │ │
│  │       SELECT                                        │ │
│  │         customer_segment,                           │ │
│  │         DATE_TRUNC('month', sale_date) AS month,    │ │
│  │         SUM(revenue) AS total_revenue,              │ │
│  │         COUNT(DISTINCT customer_id) AS customers    │ │
│  │       FROM silver_sales                             │ │
│  │       GROUP BY customer_segment, month              │ │
│  │     phase: post                                     │ │
│  │                                                      │ │
│  │ # OPTION 2: External Python Logic                  │ │
│  │ external_logic:                                     │ │
│  │   type: python                                      │ │
│  │   path: ./gold/build_sales_gold.py                  │ │
│  │   entrypoint: build_gold                            │ │
│  │   args:                                             │ │
│  │     apply_ml_scoring: true                          │ │
│  │                                                      │ │
│  │ # OPTION 3: Jupyter Notebook                        │ │
│  │ external_logic:                                     │ │
│  │   type: notebook                                    │ │
│  │   path: ./gold/sales_analytics.ipynb                │ │
│  │   output_path: output/gold_fact_sales.parquet       │ │
│  └────────────────────────────────────────────────────┘ │
│                                                           │
│  💾 Output: gold_fact_sales.parquet / Delta / Iceberg    │
└──────────────────────────┬────────────────────────────────┘
                           │
                           ▼
                ┌──────────────────────┐
                │ 📊 Business Use      │
                ├──────────────────────┤
                │ • Dashboards         │
                │ • ML Models          │
                │ • APIs               │
                │ • Data Products      │
                └──────────────────────┘
```

## External Python Logic Detail

```text
┌──────────────────────────────────────────────────────────┐
│     EXTERNAL PYTHON LOGIC FOR GOLD LAYER                  │
│          (Advanced Transformations)                       │
└──────────────────────────────────────────────────────────┘

API Call (Spark-Oriented):
```python
# Pass a Spark DataFrame to the contract
contract = DataContract("build_sales_gold.yaml")
good_df, bad_df = contract.run(spark_df, engine="spark")
```

Input: silver_sales (Spark DataFrame)
   │
   ▼
┌──────────────────────────────────────────────────────────┐
│  📄 gold/build_sales_gold.py                             │
│  ┌────────────────────────────────────────────────────┐ │
│  │ from pyspark.sql import functions as F              │ │
│  │                                                      │ │
│  │ def build_gold(df, **kwargs):                        │ │
│  │     """Spark-oriented business logic for Gold"""     │ │
│  │     return (                                        │ │
│  │         df                                           │ │
│  │         # Dynamic partitions / ML scoring            │ │
│  │         .withColumn("churn_risk", predict_udf("id")) │ │
│  │         .withColumn("amount_tax", F.col("amt")*1.1)  │ │
│  │         .withColumn("month", F.month("sale_date"))   │ │
│  │                                                      │ │
│  │         # Filter out outliers                        │ │
│  │         .filter("amt > 100")                         │ │
│  │                                                      │ │
│  │         # Distributed aggregations                   │ │
│  │         .groupBy("segment", "month")                 │ │
│  │         .agg(                                        │ │
│  │             F.sum("amount_tax").alias("total_rev"),  │ │
│  │             F.count("id").alias("txn_count")         │ │
│  │         )                                            │ │
│  │     )                                                │ │
│  └────────────────────────────────────────────────────┘ │
└──────────────────────────────────────────────────────────┘
   │
   ▼
Output: gold_fact_sales.parquet (ML-enriched, aggregated)
```

## Multi-Engine Support

```text
┌──────────────────────────────────────────────────────────┐
│          LAKELOGIC MULTI-ENGINE ARCHITECTURE              │
└──────────────────────────────────────────────────────────┘

Same Contract YAML → Multiple Execution Engines

┌────────────────────────────────────────────────────────┐
│  📋 customer_contract.yaml                             │
│  ┌──────────────────────────────────────────────────┐ │
│  │ version: 1.0.0                                    │ │
│  │ dataset: customers                                │ │
│  │ quality:                                          │ │
│  │   row_rules:                                      │ │
│  │     - name: "email_valid"                         │ │
│  │       sql: "email LIKE '%@%'"                     │ │
│  └──────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────┘
                        │
        ┌───────────────┼───────────────┬───────────────┐
        │               │               │               │
        ▼               ▼               ▼               ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│   Polars     │ │    Spark     │ │   DuckDB     │ │   Pandas     │
│   Adapter    │ │   Adapter    │ │   Adapter    │ │   Adapter    │
├──────────────┤ ├──────────────┤ ├──────────────┤ ├──────────────┤
│              │ │              │ │              │ │              │
│ • Fast local │ │ • Distributed│ │ • Analytical │ │ • Universal  │
│ • LazyFrame  │ │ • Delta Lake │ │ • SQL-first  │ │ • Fallback   │
│ • Rust core  │ │ • Unity Cat  │ │ • OLAP       │ │ • Compatible │
│              │ │              │ │              │ │              │
│ Use Case:    │ │ Use Case:    │ │ Use Case:    │ │ Use Case:    │
│ Dev/Testing  │ │ Production   │ │ Analytics    │ │ Prototyping  │
└──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘

Auto-Discovery Priority:
1. LAKELOGIC_ENGINE env var (manual override)
2. Spark (if in Databricks/Synapse)
3. Polars (preferred for single-node)
4. DuckDB (fast analytical alternative)
5. Pandas (universal fallback)
```

## Key Architecture Principles

### 1. Separation of Concerns

```text
┌──────────────────────────────────────────────────────────┐
│                  LAYER RESPONSIBILITIES                   │
└──────────────────────────────────────────────────────────┘

🟤 BRONZE: Capture & Preserve
   • No validation (Capture 100% of raw data)
   • No quarantine (No silent drops)
   • Schema evolution: append/merge
   • Goal: Immutable Raw Record

⚪ SILVER: Validate & Standardize (Latest Version)
   • Full schema enforcement
   • Business rule validation
   • Deduplication / SCD Type 1
   • Type casting
   • Goal: Trusted, queryable data

🟡 GOLD: Aggregate & Enrich (Snapshots/SCD2)
   • Business KPIs
   • ML feature engineering
   • Dimension joins (SCD Type 2)
   • Goal: Analytics-ready datasets
```

### 2. 100% Reconciliation Guarantee

```text
Mathematical Guarantee:
source_count = good_count + bad_count

Every input row is accounted for:
• Good rows → Next layer
• Bad rows → Quarantine (with error reasons)
• Nothing is silently dropped
```

### 3. Workflow Patterns

```text
┌──────────────────────────────────────────────────────────┐
│                  COMMON WORKFLOWS                         │
└──────────────────────────────────────────────────────────┘

Pattern 1: Bronze as Strings
─────────────────────────────
Bronze: Cast all columns to string (zero failures)
Silver: Type casting + validation (quarantine bad types)
Gold: Business logic on clean data

Pattern 2: Incremental Loading
───────────────────────────────
source:
  load_mode: incremental
  watermark_field: updated_at

Pattern 3: SCD Type 2 History
──────────────────────────────
materialization:
  strategy: scd2
  scd2:
    primary_key: customer_id
    timestamp_field: updated_at
    start_date_field: valid_from
    end_date_field: valid_to

Pattern 4: External ML Scoring
───────────────────────────────
external_logic:
  type: python
  path: ./ml/score_customers.py
  entrypoint: predict_churn
```

## Environment Override

```text
┌──────────────────────────────────────────────────────────┐
│            ENVIRONMENT-SPECIFIC OVERRIDES                 │
└──────────────────────────────────────────────────────────┘

Contract with environment overrides:

server:
  type: s3
  path: s3://prod-bucket/data/customers
  format: delta

environments:
  dev:
    path: s3://dev-bucket/data/customers
    format: parquet
  staging:
    path: s3://staging-bucket/data/customers
    format: delta
  prod:
    path: s3://prod-bucket/data/customers
    format: delta

Usage:
  export LAKELOGIC_ENV=dev
  python run_pipeline.py
```

## Observability & Lineage

```text
┌──────────────────────────────────────────────────────────┐
│              LINEAGE CAPTURE EXAMPLE                      │
└──────────────────────────────────────────────────────────┘

Input Record:
{
  "customer_id": 123,
  "email": "user@example.com",
  "age": 25
}

After LakeLogic Processing (lineage enabled):
{
  "customer_id": 123,
  "email": "user@example.com",
  "age": 25,
  "_lakelogic_source": "s3://bucket/raw/customers.parquet",
  "_lakelogic_processed_at": "2026-02-09T10:05:33Z",
  "_lakelogic_run_id": "a3b8d1b6-0b3b-4b1a-9c1a-1a2b3c4d5e6f",
  "_lakelogic_domain": "sales",
  "_lakelogic_system": "crm"
}

Quarantine Record (if failed):
{
  "customer_id": 123,
  "email": "invalid-email",
  "age": -5,
  "_lakelogic_errors": [
    "Rule failed: email_format (email LIKE '%@%')",
    "Rule failed: age_positive (age >= 0)"
  ],
  "_lakelogic_categories": ["correctness", "correctness"],
  "quarantine_state": "active",
  "quarantine_reprocessed": false
}
```

## Best Practices

### 1. Contract Organization

```text
contracts/
├── bronze/
│   ├── crm_contacts.yaml
│   ├── web_events.yaml
│   └── payment_transactions.yaml
├── silver/
│   ├── customers.yaml
│   ├── orders.yaml
│   └── products.yaml
└── gold/
    ├── customer_metrics.yaml
    └── revenue_summary.yaml
```

### 2. Quality Rule Categories

Use standard categories for consistency:

- `completeness`: Not null, required fields
- `correctness`: Data type, format, range
- `consistency`: Referential integrity, cross-field validation
- `validity`: Business rule compliance
- `accuracy`: Statistical checks, anomaly detection
- `timeliness`: Freshness, staleness
- `uniqueness`: Duplicate detection
- `integrity`: Foreign key constraints

### 3. Error Handling

```python
from lakelogic import DataProcessor

try:
    proc = DataProcessor(contract="contract.yaml")
    source, good, bad = proc.run_source("data.parquet")
    
    # Check quarantine threshold
    quarantine_ratio = len(bad) / (len(good) + len(bad))
    if quarantine_ratio > 0.10:  # 10% threshold
        raise ValueError(
            f"Quarantine ratio {quarantine_ratio:.2%} "
            "exceeds threshold"
        )
    
    proc.materialize(good, bad)
    
except Exception as e:
    notify_team(f"Pipeline failed: {e}")
    raise
```

---

*For more details, see the [LakeLogic Documentation](https://lakelogic.org)*
