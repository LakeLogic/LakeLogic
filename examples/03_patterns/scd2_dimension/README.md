# SCD2 Dimension

Build Slowly Changing Dimension Type 2 tables to track historical changes.

## When to Use

- Track customer status changes over time
- Audit trail requirements
- Point-in-time reporting ("What was the status on Jan 1?")

## Files

```
scd2_dimension/
├── contract.yaml
└── data/
    ├── dim_customers_snapshot1.csv   # Initial load
    └── dim_customers_snapshot2.csv   # Changed records
```

## The Pattern

SCD2 adds tracking columns to your dimension:

| Column | Purpose |
|--------|---------|
| `effective_from` | When this version became active |
| `effective_to` | When this version was superseded (NULL = current) |
| `is_current` | Boolean flag for current version |

## Contract Breakdown

```yaml
materialization:
  strategy: scd2
  target_path: output/gold_dim_customers
  format: csv
  scd2:
    effective_from_field: updated_at      # Use this as version date
    effective_to_field: effective_to      # Column to add
    current_flag_field: is_current        # Column to add
```

## Example Output

After processing two snapshots:

| customer_id | status | effective_from | effective_to | is_current |
|-------------|--------|----------------|--------------|------------|
| 1 | active | 2024-01-01 | 2024-06-01 | false |
| 1 | inactive | 2024-06-01 | NULL | true |
| 2 | active | 2024-01-01 | NULL | true |

## Run It

```python
from lakeguard import DataProcessor

# Load initial snapshot
proc = DataProcessor(contract="contract.yaml")
proc.run("data/dim_customers_snapshot1.csv")

# Load changes - SCD2 handles versioning
proc.run("data/dim_customers_snapshot2.csv")
```

## Query Patterns

```sql
-- Current state
SELECT * FROM gold_dim_customers WHERE is_current = true

-- Point-in-time (as of 2024-03-01)
SELECT * FROM gold_dim_customers
WHERE effective_from <= '2024-03-01'
  AND (effective_to IS NULL OR effective_to > '2024-03-01')
```
