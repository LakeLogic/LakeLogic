# LakeLogic Architecture: Medallion with Quality Gates

This diagram illustrates how LakeLogic enforces data contracts as **quality gates** across the medallion architecture (Bronze → Silver → Gold).

## High-Level Architecture Flow

```text
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        LAKELOGIC MEDALLION ARCHITECTURE                         │
│                      Data Contracts as Quality Gates                            │
└─────────────────────────────────────────────────────────────────────────────────┘

┌──────────────┐
│  Raw Sources │  ← CSV, JSON, Parquet, Delta Lake, Unity Catalog, APIs
│  (Landing)   │
└──────┬───────┘
       │
       ▼
┌────────────────────────────────────────────────────────────────────────────────┐
│                             🟤 BRONZE LAYER                                     │
│                         "Capture Everything Raw"                                │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  📋 Data Contract: bronze_contract.yaml                                        │
│  ┌──────────────────────────────────────────────────────────────────────────┐ │
│  │ quality:                                                                  │ │
│  │   row_rules:                                                              │ │
│  │     - name: "email_format"           # Minimal quality gates              │ │
│  │       sql: "email LIKE '%@%'"        # Catch obvious garbage              │ │
│  │     - name: "age_positive"                                                │ │
│  │       sql: "age IS NULL OR age >= 0"                                      │ │
│  └──────────────────────────────────────────────────────────────────────────┘ │
│                                                                                 │
│  📊 Strategy: overwrite or append                                              │
│  💾 Output: bronze_customers.parquet                                           │
└──────┬──────────────────────────────────────────────────────────────────┬──────┘
       │                                                                    │
       │                                                                    │
       │ ✓ PASSED VALIDATION                                                │ ✗ FAILED
       │                                                                    │
       ▼                                                                    ▼
┌─────────────────────────────────────────────┐              ┌────────────────────────┐
│   🛡️ Quality Gate: Bronze → Silver          │              │  🛑 QUARANTINE ZONE    │
│                                              │              ├────────────────────────┤
│  1️⃣ Pre-Processing (Cleanse)                │              │ quarantine/            │
│     - rename: align column names            │              │   bronze_bad.parquet   │
│     - filter: drop invalid rows             │              │                        │
│     - deduplicate: keep latest record       │              │ 📝 Error Reasons:      │
│     - trim, lower, cast                     │              │   - email_format       │
│                                              │              │   - age_positive       │
│  2️⃣ Schema Enforcement                      │              │   - missing_id         │
│     - Type validation                       │              │                        │
│     - Required field checks                 │              │ 🔄 Correction Loop:    │
│     - Unknown field policy                  │              │   1. Fix source data   │
│                                              │              │   2. Reprocess         │
│  3️⃣ Quality Rules (Row + Dataset)          │              │   3. Flow to Silver    │
│     - Row: not_null, regex, range           │              └────────────────────────┘
│     - Dataset: unique, null_ratio           │
│                                              │
│  4️⃣ Post-Processing (Enrich)                │
│     - derive: calculate fields              │
│     - lookup: join dimensions               │
└──────────────┬──────────────────────────────┘
               │
               ▼
┌────────────────────────────────────────────────────────────────────────────────┐
│                            ⚪ SILVER LAYER                                      │
│                   "Validated, Cleaned, Business-Ready"                          │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  📋 Data Contract: silver_contract.yaml                                        │
│  ┌──────────────────────────────────────────────────────────────────────────┐ │
│  │ transformations:                                                          │ │
│  │   - deduplicate:                                                          │ │
│  │       on: ["customer_id"]                                                 │ │
│  │       sort_by: ["updated_at"]                                             │ │
│  │                                                                            │ │
│  │ quality:                                                                   │ │
│  │   row_rules:                       # Full validation                      │ │
│  │     - not_null: email                                                     │ │
│  │     - regex_match:                                                        │ │
│  │         field: email                                                      │ │
│  │         pattern: "^[^@]+@[^@]+\\.[^@]+$"                                  │ │
│  │     - range:                                                              │ │
│  │         field: age                                                        │ │
│  │         min: 18                                                           │ │
│  │         max: 120                                                          │ │
│  │     - accepted_values:                                                    │ │
│  │         field: status                                                     │ │
│  │         values: ["ACTIVE", "INACTIVE"]                                    │ │
│  │                                                                            │ │
│  │   dataset_rules:                                                          │ │
│  │     - unique: customer_id                                                 │ │
│  │     - null_ratio:                                                         │ │
│  │         field: email                                                      │ │
│  │         max: 0.05                    # Max 5% null emails                 │ │
│  └──────────────────────────────────────────────────────────────────────────┘ │
│                                                                                 │
│  📊 Materialization Strategies:                                                │
│     - append:    Transaction tables (fact tables)                              │
│     - merge:     SCD Type 1 (update existing)                                  │
│     - scd2:      SCD Type 2 (history tracking)                                 │
│     - overwrite: Daily snapshots                                               │
│                                                                                 │
│  💾 Output: silver_customers.parquet / Delta / Iceberg                         │
└──────┬──────────────────────────────────────────────────────────────────┬──────┘
       │                                                                    │
       │ ✓ PASSED                                                           │ ✗ FAILED
       │                                                                    │
       ▼                                                                    ▼
┌─────────────────────────────────────────────┐              ┌────────────────────────┐
│   🛡️ Quality Gate: Silver → Gold            │              │  🛑 QUARANTINE ZONE    │
│                                              │              ├────────────────────────┤
│  Contract Enforcement:                      │              │ quarantine/            │
│    ✓ Schema validation                      │              │   silver_bad.parquet   │
│    ✓ Business rules                         │              │                        │
│    ✓ Referential integrity                  │              │ Reason codes logged    │
│    ✓ Statistical checks                     │              └────────────────────────┘
└──────────────┬──────────────────────────────┘
               │
               ▼
┌────────────────────────────────────────────────────────────────────────────────┐
│                             🟡 GOLD LAYER                                       │
│              "Aggregated, Business KPIs, Analytics-Ready"                       │
├────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  📋 Data Contract: gold_contract.yaml                                          │
│  ┌──────────────────────────────────────────────────────────────────────────┐ │
│  │ # OPTION 1: SQL-Based Aggregation                                        │ │
│  │ transformations:                                                          │ │
│  │   - sql: |                                                                │ │
│  │       SELECT                                                              │ │
│  │         customer_segment,                                                 │ │
│  │         DATE_TRUNC('month', sale_date) AS sale_month,                     │ │
│  │         SUM(revenue) AS total_revenue,                                    │ │
│  │         COUNT(DISTINCT customer_id) AS unique_customers                   │ │
│  │       FROM silver_sales                                                   │ │
│  │       GROUP BY customer_segment, sale_month                               │ │
│  │     phase: post                                                           │ │
│  │                                                                            │ │
│  │ # OPTION 2: External Python Logic (Advanced Use Cases)                   │ │
│  │ external_logic:                                                           │ │
│  │   type: python                                                            │ │
│  │   path: ./gold/build_sales_gold.py                                        │ │
│  │   entrypoint: build_gold                                                  │ │
│  │   args:                                                                   │ │
│  │     apply_ml_scoring: true                                                │ │
│  │     target_table: gold_fact_sales                                         │ │
│  │                                                                            │ │
│  │ # OPTION 3: Jupyter Notebook                                              │ │
│  │ external_logic:                                                           │ │
│  │   type: notebook                                                          │ │
│  │   path: ./gold/sales_analytics.ipynb                                      │ │
│  │   output_path: output/gold_fact_sales.parquet                             │ │
│  │   output_format: parquet                                                  │ │
│  └──────────────────────────────────────────────────────────────────────────┘ │
│                                                                                 │
│  💾 Output: gold_fact_sales.parquet / Delta / Iceberg                          │
└────────────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
                ┌─────────────────────────────┐
                │  📊 Business Consumption    │
                ├─────────────────────────────┤
                │  • Dashboards (Tableau, PBI)│
                │  • ML Models                │
                │  • External APIs            │
                │  • Data Products            │
                └─────────────────────────────┘
```

## External Python Logic Detail

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│              EXTERNAL PYTHON LOGIC FOR GOLD LAYER                             │
│                  (Advanced Transformations)                                   │
└──────────────────────────────────────────────────────────────────────────────┘

Input: silver_sales.parquet (validated DataFrame)
   │
   ▼
┌────────────────────────────────────────────────────────────────┐
│  📄 gold/build_sales_gold.py                                   │
│  ┌──────────────────────────────────────────────────────────┐ │
│  │ import polars as pl                                       │ │
│  │                                                            │ │
│  │ def build_gold(df: pl.DataFrame) -> pl.DataFrame:        │ │
│  │     """Custom business logic for Gold layer"""           │ │
│  │     return (                                              │ │
│  │         df                                                 │ │
│  │         # ML model scoring                                │ │
│  │         .with_columns([                                   │ │
│  │             predict_churn(pl.col("customer_id"))          │ │
│  │                 .alias("churn_risk_score"),               │ │
│  │             (pl.col("amount") * 1.1)                      │ │
│  │                 .alias("amount_with_tax"),                │ │
│  │             pl.col("sale_date").dt.month()                │ │
│  │                 .alias("sale_month")                      │ │
│  │         ])                                                 │ │
│  │         # Complex aggregations                            │ │
│  │         .filter(pl.col("amount") > 100)                   │ │
│  │         .group_by(["customer_segment", "sale_month"])     │ │
│  │         .agg([                                            │ │
│  │             pl.sum("amount_with_tax").alias("revenue"),   │ │
│  │             pl.count().alias("transactions"),             │ │
│  │             pl.mean("churn_risk_score").alias("avg_risk") │ │
│  │         ])                                                 │ │
│  │     )                                                      │ │
│  └──────────────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────────────┘
   │
   ▼
Output: gold_fact_sales.parquet (business-ready metrics)
```

## Multi-Engine Support

```text
┌──────────────────────────────────────────────────────────────────────────────┐
│                    ENGINE AUTO-DISCOVERY & PORTABILITY                        │
└──────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────┐
│  Same Contract YAML     │  ← Write once, run anywhere
└───────────┬─────────────┘
            │
            ├─────────────────────────────────────────────────────┐
            │                                                     │
            ▼                                                     ▼
┌───────────────────────┐                          ┌──────────────────────────┐
│  🖥️ Local Development  │                          │  ☁️ Production Cluster   │
├───────────────────────┤                          ├──────────────────────────┤
│  Engine: Polars/DuckDB│                          │  Engine: Spark/Databricks│
│  Data: < 100GB        │                          │  Data: Petabyte-scale    │
│  Format: Parquet/CSV  │                          │  Format: Delta Lake      │
└───────────────────────┘                          └──────────────────────────┘

Auto-Discovery Priority:
  1. LAKELOGIC_ENGINE env var (manual override)
  2. Spark (if in Databricks/Synapse)
  3. Polars (preferred for single-node)
  4. DuckDB (fast analytical alternative)
  5. Pandas (universal fallback)
```

## Key Architecture Principles

### 1. **Separation of Concerns**

- **Bronze**: Minimal validation, capture everything
- **Silver**: Full data quality enforcement
- **Gold**: Business logic and aggregations

### 2. **Safe Quarantine**

- Failed rows are **never lost**
- Stored with **error reasons** for debugging
- Can be **corrected and reprocessed**

### 3. **100% Reconciliation**

```text
source_count = good_count + bad_count
```

Every input row is accounted for—no silent failures.

### 4. **Portable Contracts**

- SQL-first syntax (universal)
- Engine-agnostic execution
- No vendor lock-in

### 5. **Flexible Gold Processing**

- **SQL transformations**: For standard aggregations
- **Python functions**: For ML scoring, complex logic
- **Jupyter notebooks**: For exploratory/iterative development

## Workflow: Bronze → Silver → Gold

```python
from lakelogic import DataProcessor

# BRONZE: Ingest raw data with minimal rules
bronze_proc = DataProcessor(
    contract="contracts/bronze_customers.yaml",
    stage="bronze"
)
bronze_source, bronze_good, bronze_bad = bronze_proc.run_source("raw/customers.csv")
bronze_proc.materialize(bronze_good, bronze_bad)

# SILVER: Full validation and transformation
silver_proc = DataProcessor(
    contract="contracts/silver_customers.yaml"
)
silver_source, silver_good, silver_bad = silver_proc.run_source("bronze/customers.parquet")
silver_proc.materialize(silver_good, silver_bad)

# GOLD: Business aggregations with external Python logic
gold_proc = DataProcessor(
    contract="contracts/gold_customer_metrics.yaml"
)
gold_source, gold_good, gold_bad = gold_proc.run_source("silver/customers.parquet")
gold_proc.materialize(gold_good, gold_bad)

print(f"📊 Pipeline Complete:")
print(f"  Bronze: {len(bronze_good)} good, {len(bronze_bad)} quarantined")
print(f"  Silver: {len(silver_good)} good, {len(silver_bad)} quarantined")
print(f"  Gold:   {len(gold_good)} business metrics materialized")
```

## Visual Summary

```text
RAW → [Bronze Gate] → Bronze Layer → [Silver Gate] → Silver Layer → [Gold Gate] → Gold Layer → BI
         ↓                              ↓                              ↓
     Quarantine                    Quarantine                     Quarantine
     (minimal)                     (detailed)                     (rare)

Quality Rules:    Minimal         →    Comprehensive    →    Business Logic
Data Volume:      100%            →    ~95-98%          →    Aggregated
Schema Policy:    Flexible        →    Strict           →    Purpose-built
```

---

**Next Steps:**
- [Quickstart](quickstart.md) - Run your first quality gate
- [Concepts](concepts.md) - Deep dive into medallion architecture
- [Patterns](patterns/bronze_quality_gate.md) - Real-world recipes
- [External Python Logic](patterns/external_python_logic.md) - Advanced transformations
