# Late Arriving Reprocess

Safely backfill partitions when late-arriving data comes in.

## When to Use

- Event data arrives out of order
- Need to reprocess a specific date partition
- Backfill without affecting other partitions

## Files

```
late_arriving_reprocess/
├── contract.yaml
└── data/
    ├── events_run1.csv   # Initial events
    └── events_run2.csv   # Late-arriving events for same dates
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

## Reprocess Policies

| Policy | Behavior |
|--------|----------|
| `overwrite_partition_safe` | Replace only partitions in current batch |
| `overwrite_all` | Replace entire table (dangerous) |
| `append` | Add without checking duplicates |
| `merge` | Upsert by primary key |

## Run It

```python
from lakeguard import DataProcessor

proc = DataProcessor(contract="contract.yaml")

# Run 1: Load initial events
proc.run("data/events_run1.csv")
# Creates: event_date=2024-01-01/, event_date=2024-01-02/

# Run 2: Late data arrives for 2024-01-01
proc.run("data/events_run2.csv")
# Overwrites: event_date=2024-01-01/
# Untouched:  event_date=2024-01-02/
```

## Partition Safety

The key is `overwrite_partition_safe`:
- **Safe**: Only affects partitions in the current batch
- **Idempotent**: Re-running the same data produces same result
- **No data loss**: Other partitions are preserved
