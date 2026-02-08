# SCD2 Dimension

Build Slowly Changing Dimension Type 2 tables to track historical changes.

## When to Use

- Track customer status changes over time
- Audit trail requirements
- Point-in-time reporting ("What was the status on Jan 1?")

## Files

```
examples/03_patterns/scd2_dimension/
├── contract.yaml
├── README.md
└── data/
    ├── dim_customers_snapshot1.csv
    └── dim_customers_snapshot2.csv
```

## The Pattern

SCD2 adds tracking columns to your dimension:

| Column | Purpose |
|--------|---------|
| `effective_from` | When this version became active |
| `effective_to` | When this version was superseded (NULL = current) |
| `is_current` | Boolean flag for current version |

## Contract

```yaml
dataset: gold_dim_customers
primary_key: ["customer_id"]

model:
  fields:
    - name: customer_id
      type: int
      required: true
    - name: name
      type: string
    - name: status
      type: string
    - name: updated_at
      type: date

quality:
  row_rules:
    - name: valid_status
      sql: "status IN ('active','inactive')"
      category: consistency

materialization:
  strategy: scd2
  target_path: output/gold_dim_customers
  format: csv
  scd2:
    effective_from_field: updated_at
    effective_to_field: effective_to
    current_flag_field: is_current
```

## Example Output

After processing two snapshots:

| customer_id | status | effective_from | effective_to | is_current |
|-------------|--------|----------------|--------------|------------|
| 1 | active | 2024-01-01 | 2024-06-01 | false |
| 1 | inactive | 2024-06-01 | NULL | true |
| 2 | active | 2024-01-01 | NULL | true |

## Query Patterns

```sql
-- Current state
SELECT * FROM gold_dim_customers WHERE is_current = true

-- Point-in-time (as of 2024-03-01)
SELECT * FROM gold_dim_customers
WHERE effective_from <= '2024-03-01'
  AND (effective_to IS NULL OR effective_to > '2024-03-01')
```

## Run It

```bash
cd examples/03_patterns/scd2_dimension
python -c "
from lakelogic import DataProcessor
proc = DataProcessor(contract='contract.yaml')
# Load initial snapshot
proc.run('data/dim_customers_snapshot1.csv')
# Load changes - SCD2 handles versioning
proc.run('data/dim_customers_snapshot2.csv')
"
```

## Engine Notes

- **Spark**: SCD2 runs natively using distributed DataFrame operations. For Delta Lake tables, uses `MERGE INTO` when available. No driver memory bottlenecks at scale.
- **Polars/DuckDB**: Requires pandas for SCD2 materialization.
- **Pandas**: Native support.
