# Tutorial: Medallion Architecture

Learn the Bronze → Silver pipeline pattern used in modern lakehouses.

## What You'll Learn

1. **Stages** - Define different processing behaviors per layer
2. **Bronze Layer** - Ingest raw data with minimal rules
3. **Silver Layer** - Apply full quality validation
4. **Materialization** - Write output to Parquet files

## Files

The example files are located at:

```text
examples/02_tutorials/medallion_architecture/
├── contract.yaml              # Multi-stage contract
├── data/
│   └── crm_export.csv         # Raw CRM data
└── quickstart_tutorial.ipynb  # Interactive notebook
```

## The Pattern

```text
Raw CSV → [Bronze Stage] → bronze.parquet → [Silver Stage] → silver.parquet
              ↓                                    ↓
         Minimal rules                      Full validation
         Capture everything                 Quality gates
```

## Contract Breakdown

```yaml
# Bronze: Ingest everything, minimal validation
stages:
  bronze:
    source:
      type: raw_landing
      path: "data/crm_*.csv"
    materialization:
      strategy: overwrite
      target_path: "data/bronze/bronze_customers.parquet"
    quality:
      row_rules: []  # No rules at Bronze - capture all raw data

# Silver: Full validation (default stage)
source:
  type: bronze_layer
  path: "data/bronze/bronze_customers.parquet"

quality:
  row_rules:
    - regex_match:
        field: email
        pattern: "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$"
    - name: positive_spend
      sql: "total_spend >= 0"

materialization:
  strategy: overwrite
  target_path: "data/silver/silver_customers.parquet"
```

## Run It

### Full Pipeline (Bronze then Silver)

```python
from lakelogic import DataProcessor

# Run Bronze stage
proc = DataProcessor(contract="contract.yaml", stage="bronze")
bronze_good, bronze_bad = proc.run("data/crm_export.csv")

# Run Silver stage (reads Bronze output)
proc = DataProcessor(contract="contract.yaml")  # default = silver
silver_good, silver_bad = proc.run()
```

### Interactive Notebook

Open `examples/02_tutorials/medallion_architecture/quickstart_tutorial.ipynb` for a guided walkthrough.

## Key Concepts

| Concept | Bronze | Silver |
|---------|--------|--------|
| **Purpose** | Capture raw data | Validate & clean |
| **Quality Rules** | None or minimal | Full validation |
| **Schema** | Loose | Strict |
| **Failures** | Rare | Expected (quarantine) |

## Next Steps

- [Notifications & Secrets](notifications_secrets.md) - Configure alerts
- [Dedup & Survivorship](../patterns/dedup_survivorship.md) - Handle duplicates
