# LakeLogic Architecture: Medallion with Quality Gates

This diagram illustrates how LakeLogic enforces data contracts as **quality gates** across the medallion architecture (Bronze → Silver → Gold).

## High-Level Architecture Flow

```text
┌──────────────────────────────────────────────────────────┐
│       LAKELOGIC MEDALLION ARCHITECTURE                   │
│       Data Contracts as Quality Gates                    │
└──────────────────────────────────────────────────────────┘

┌──────────────┐
│  Raw Sources │  ← CSV, JSON, Parquet, Delta, Unity Catalog
│  (Landing)   │
└──────┬───────┘
       │
       ▼
┌──────────────────────────────────────────────────────────┐
│                  🟤 BRONZE LAYER                          │
│              "Capture Everything Raw"                     │
├──────────────────────────────────────────────────────────┤
│                                                           │
│  📋 Data Contract: bronze_contract.yaml                  │
│  ┌────────────────────────────────────────────────────┐ │
│  │ quality:                                            │ │
│  │   row_rules:                                        │ │
│  │     - name: "email_format"  # Minimal gates         │ │
│  │       sql: "email LIKE '%@%'" # Catch garbage       │ │
│  │     - name: "age_positive"                          │ │
│  │       sql: "age IS NULL OR age >= 0"                │ │
│  └────────────────────────────────────────────────────┘ │
│                                                           │
│  📊 Strategy: overwrite or append                        │
│  💾 Output: bronze_customers.parquet                     │
└──────┬──────────────────────────────────────────┬────────┘
       │                                           │
       │ ✓ PASSED VALIDATION                       │ ✗ FAILED
       │                                           │
       ▼                                           ▼
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
           │
           ▼
┌──────────────────────────────────────────────────────────┐
│                   ⚪ SILVER LAYER                         │
│          "Validated, Cleaned, Business-Ready"             │
├──────────────────────────────────────────────────────────┤
│                                                           │
│  📋 Data Contract: silver_contract.yaml                  │
│  ┌────────────────────────────────────────────────────┐ │
│  │ transformations:                                    │ │
│  │   - deduplicate:                                    │ │
│  │       on: ["customer_id"]                           │ │
│  │       sort_by: ["updated_at"]                       │ │
│  │                                                      │ │
│  │ quality:                                             │ │
│  │   row_rules:             # Full validation          │ │
│  │     - not_null: email                               │ │
│  │     - regex_match:                                  │ │
│  │         field: email                                │ │
│  │         pattern: "^[^@]+@[^@]+\\.[^@]+$"            │ │
│  │     - range:                                        │ │
│  │         field: age                                  │ │
│  │         min: 18                                     │ │
│  │         max: 120                                    │ │
│  │     - accepted_values:                              │ │
│  │         field: status                               │ │
│  │         values: ["ACTIVE", "INACTIVE"]              │ │
│  │                                                      │ │
│  │   dataset_rules:                                    │ │
│  │     - unique: customer_id                           │ │
│  │     - null_ratio:                                   │ │
│  │         field: email                                │ │
│  │         max: 0.05      # Max 5% null emails         │ │
│  └────────────────────────────────────────────────────┘ │
│                                                           │
│  📊 Materialization Strategies:                          │
│     - append:    Transaction tables (fact tables)        │
│     - merge:     SCD Type 1 (update existing)            │
│     - scd2:      SCD Type 2 (history tracking)           │
│     - overwrite: Daily snapshots                         │
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
           │
           ▼
┌──────────────────────────────────────────────────────────┐
│                    🟡 GOLD LAYER                          │
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

Input: silver_sales.parquet (validated DataFrame)
   │
   ▼
┌──────────────────────────────────────────────────────────┐
│  📄 gold/build_sales_gold.py                             │
│  ┌────────────────────────────────────────────────────┐ │
│  │ import polars as pl                                 │ │
│  │                                                      │ │
│  │ def build_gold(df: pl.DataFrame) -> pl.DataFrame:  │ │
│  │     """Custom business logic for Gold layer"""     │ │
│  │     return (                                        │ │
│  │         df                                           │ │
│  │         # ML model scoring                          │ │
│  │         .with_columns([                             │ │
│  │             predict_churn(pl.col("customer_id"))    │ │
│  │                 .alias("churn_risk_score"),         │ │
│  │             (pl.col("amount") * 1.1)                │ │
│  │                 .alias("amount_with_tax"),          │ │
│  │             pl.col("sale_date").dt.month()          │ │
│  │                 .alias("sale_month")                │ │
│  │         ])                                           │ │
│  │         # Business filters                          │ │
│  │         .filter(pl.col("amount") > 100)             │ │
│  │         # Aggregations                              │ │
│  │         .group_by(["customer_segment", "month"])    │ │
│  │         .agg([                                      │ │
│  │             pl.sum("amount_with_tax")               │ │
│  │               .alias("total_rev"),                  │ │
│  │             pl.count("customer_id")                 │ │
│  │               .alias("txn_count")                   │ │
│  │         ])                                           │ │
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
   • Minimal validation (catch obvious junk)
   • All strings pattern (optional)
   • Schema evolution: append/merge
   • Goal: Zero data loss

⚪ SILVER: Validate & Standardize
   • Full schema enforcement
   • Business rule validation
   • Deduplication
   • Type casting
   • Goal: Trusted, queryable data

🟡 GOLD: Aggregate & Enrich
   • Business KPIs
   • ML feature engineering
   • Dimension joins
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

## Integration Patterns

### Airflow Integration

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from lakelogic import DataProcessor

def run_quality_gate(**context):
    proc = DataProcessor(
        contract="contracts/silver_customers.yaml",
        engine="spark"
    )
    source, good, bad = proc.run_source()
    proc.materialize(good, bad)
    
    # Push metrics to XCom
    context['ti'].xcom_push(
        key='quarantine_count',
        value=len(bad)
    )

with DAG('customer_pipeline', ...) as dag:
    quality_gate = PythonOperator(
        task_id='silver_quality_gate',
        python_callable=run_quality_gate
    )
```

### dbt Integration (Proposed)

```yaml
# dbt_project.yml
models:
  my_project:
    staging:
      +pre-hook:
        - "{{ lakelogic.validate('contracts/staging.yaml') }}"
```

### Databricks Integration

```python
# Databricks notebook
from lakelogic import DataProcessor

# Read from Unity Catalog
df = spark.table("main.bronze.customers")

# Apply quality gate
proc = DataProcessor(
    contract="/Workspace/contracts/silver_customers.yaml",
    engine="spark"
)
source, good, bad = proc.run(
    df,
    source_path="main.bronze.customers"
)

# Write to Unity Catalog
good.write.mode("overwrite") \
    .saveAsTable("main.silver.customers")
bad.write.mode("append") \
    .saveAsTable("main.quarantine.customers")
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

*For more details, see the [LakeLogic Documentation](https://lineagelogic.com)*
