# Late Arriving Reprocess

Safely backfill partitions when late-arriving data comes in.

## When to Use

- Event data arrives out of order
- Need to reprocess a specific date partition
- Backfill without affecting other partitions

## Files

```
examples/03_patterns/late_arriving_reprocess/
├── contract.yaml
├── README.md
└── data/
    ├── events_run1.csv
    └── events_run2.csv
```

## The Pattern

```yaml
materialization:
  strategy: append
  partition_by: ["event_date"]
  reprocess_policy: overwrite_partition_safe
  target_path: output/silver_pos_sales_events
```

## How It Works

1. **First run**: Appends data, creates partitions by `event_date`
2. **Reprocess run**: Only overwrites partitions present in new data
3. **Other partitions**: Untouched (safe)

## Contract

```yaml
dataset: silver_pos_sales_events
primary_key: ["event_id"]

model:
  fields:
    - name: event_id
      type: int
      required: true
    - name: event_date
      type: date
      required: true
    - name: amount
      type: float
    - name: customer_id
      type: int

quality:
  row_rules:
    - name: positive_amount
      sql: "amount > 0"
      category: correctness

materialization:
  strategy: append
  partition_by: ["event_date"]
  reprocess_policy: overwrite_partition_safe
  target_path: output/silver_pos_sales_events
  format: csv
```

## Reprocess Policies

| Policy | Behavior |
|--------|----------|
| `overwrite_partition_safe` | Replace only partitions in current batch |
| `overwrite_all` | Replace entire table (dangerous) |
| `append` | Add without checking duplicates |
| `merge` | Upsert by primary key |

## Run It

```bash
cd examples/03_patterns/late_arriving_reprocess
python -c "
from lakeguard import DataProcessor
proc = DataProcessor(contract='contract.yaml')

# Run 1: Load initial events
proc.run('data/events_run1.csv')
# Creates: event_date=2024-01-01/, event_date=2024-01-02/

# Run 2: Late data arrives for 2024-01-01
proc.run('data/events_run2.csv')
# Overwrites: event_date=2024-01-01/
# Untouched:  event_date=2024-01-02/
"
```

## Partition Safety

The key is `overwrite_partition_safe`:
- **Safe**: Only affects partitions in the current batch
- **Idempotent**: Re-running the same data produces same result
- **No data loss**: Other partitions are preserved
