# Partition-Aware Merge Materialization

Perform merge (upsert) and SCD2 operations scoped to individual partitions,
dramatically reducing I/O for large partitioned tables.

## Components

- **orders_contract.yaml**: Contract with `strategy: merge` + `partition_by`.
- **partitioned_merge.py**: End-to-end demo of partition-scoped merges.
- **data/**: Sample order data with multiple batches.

## When to Use This Pattern

- **Large Partitioned Tables**: Merge against tables with 100+ partitions
  without reading the entire table.
- **Daily Batch Upserts**: Only load and rewrite the partition(s) affected
  by today's batch.
- **Concurrent Pipelines**: Multiple pipelines can merge into different
  partitions simultaneously without conflicts.
- **Cost Optimisation**: Reduce cloud storage I/O costs by avoiding
  unnecessary reads/writes of unchanged partitions.

## How It Works

```
Incoming batch: 1,000 rows for order_date=2024-12-20

Traditional merge:
  Read 50M rows (all dates) → merge → write 50M rows

Partition-aware merge:
  Read 5K rows (2024-12-20 only) → merge → write 6K rows
  Other 364 partitions: UNTOUCHED
```

## Benefits

| Metric | Full-Table Merge | Partition-Aware Merge |
|---|---|---|
| Data read | Entire table | Affected partitions |
| Data written | Entire table | Affected partitions |
| Concurrent writes | ❌ Unsafe | ✅ Safe |
| Memory usage | Table size | Partition size |

## Prerequisites

- Complete **02_core_patterns/** for merge/SCD2 strategies.
- Understanding of partitioned materialization.

## Next Steps

- **late_arriving_reprocess/** for handling late data with partitions.
- **payments_lifecycle/** for SCD2 combined with partitions.
