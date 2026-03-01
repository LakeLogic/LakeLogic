# Quickstart Tutorials

Choose your starting path to experience LakeLogic's governance-first data pipelines.

---

## Tutorials

### 1. [Hello World](01_hello_world.ipynb)
**Time**: 1 minute | **Prerequisites**: None

Experience LakeLogic with zero setup. Pull data from a remote URL and see it validated instantly.

- No local database required
- Demonstrates in-memory contracts
- Perfect for Google Colab / Kaggle

---

### 2. [Database Governance](02_database_governance.ipynb)
**Time**: 5 minutes | **Prerequisites**: None (uses bundled SQLite)

Learn how to protect your Lakehouse from dirty source data.

- Uses a local SQLite database
- Demonstrates quality rules and quarantine
- Shows "shift-left" governance in practice

---

### 3. [dbt → PII → Quality → Data Generation](03_dbt_pii_quality.ipynb) ⭐ New
**Time**: 10 minutes | **Prerequisites**: `pip install lakelogic`

The **end-to-end showcase** — starting from a real dbt `schema.yml` and demonstrating:

| Step | Feature |
|---|---|
| `DbtAdapter` | Import dbt schema → LakeLogic contract automatically |
| `DataGenerator.from_dbt()` | Generate typed synthetic test data from the dbt schema |
| `DataProcessor.from_dbt()` | Run quality validation (dbt tests enforced at ingestion) |
| `forget_subjects()` | GDPR right-to-erasure: nullify, hash, or redact strategies |
| `mask_pii_columns()` | Dataset-wide PII masking for dev/test copies |
| `generate_erasure_report()` | DPO-ready audit trail in JSON |

**Key insight**: the `dbt_schema.yml` in this folder is the **only schema file you maintain**.
LakeLogic reads PII annotations (`meta: {pii: true}`), quality tests (`not_null`,
`accepted_values`, `expression_is_true`), and field types — then wires them into
your full compliance pipeline automatically.

---

## Running from the CLI

```bash
# Import a dbt model → LakeLogic contract YAML
lakelogic import-dbt --schema dbt_schema.yml --model customers --output contracts/

# Dry-run preview (no files written)
lakelogic import-dbt --schema dbt_schema.yml --model customers --dry-run

# Run quality validation against a data file
lakelogic run --contract contracts/customers.yaml --source data/customers.csv

# Generate 500 synthetic test rows
lakelogic generate --contract contracts/customers.yaml --rows 500 \
    --invalid-ratio 0.1 --output data/test_customers.parquet
```

---

## Next Steps

- **[Core Patterns](../02_core_patterns/)** — Medallion architecture and reference joins 
- **[Advanced Workflows](../03_advanced_workflows/)** — Polars streaming, shared governance at scale, GDPR compliance
