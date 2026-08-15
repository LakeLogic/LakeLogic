# Materialization

!!! abstract "Open Lakehouse Contract - the standard"
    LakeLogic implements the **[Open Lakehouse Contract (OLC)](https://lakelogic.github.io/open-lakehouse-contract/)**, the open
    standard for lakehouse data contracts. The authoritative field-level spec for
    **materialization** is the OLC reference: **[materialization reference →](https://lakelogic.github.io/open-lakehouse-contract/reference/materialization/)**.
    This page shows how the LakeLogic runtime *applies* that spec (engine behavior,
    examples, and runtime-specific notes).


Materialization controls **how validated data is written** to the target Delta table.

> **Think of it like choosing a filing strategy.** You can append new pages to the end of a folder (append), update existing pages in-place (merge), keep every version of every page with dates (SCD2), or throw everything out and start fresh each day (overwrite).

---

## Write Strategies

| Strategy | Use Case | Typical Layer | Business Value |
| --- | --- | --- | --- |
| `append` | Immutable event logs | Bronze, Gold (facts) | Complete audit trail, nothing is ever lost |
| `merge` | Upsert by primary key (SCD1) | Silver, Gold (dims) | Always current, deduplication built in |
| `scd2` | Track every change with history | Silver, Gold (dims) | Point-in-time analytics, regulatory compliance |
| `overwrite` | Daily snapshots, full refreshes | Gold (aggregates) | Clean slate, simple to reason about |

---

## Append

Adds new rows without checking for duplicates. Ideal for event logs and transaction records.

!!! example "Example: Append with date partitioning"

    ```yaml
    materialization:
      strategy: "append"
      partition_by: ["ingestion_date"]
    ```

---

## Merge (Upsert)

Matches incoming rows against existing rows using `primary_key`, updates matches, inserts new rows.

!!! example "Example: Merge with deduplication guard"

    ```yaml
    primary_key: ["customer_id"]

    materialization:
      strategy: "merge"
      merge_dedup_guard: true        # Dedup incoming batch by PK before merge
    ```

**Key behaviours:**

- **Matched rows** → UPDATE all non-key columns
- **Unmatched incoming** → INSERT
- **Unmatched existing** → kept as-is (no deletes)
- `_lakelogic_processed_at` → updated on every merge (last modified)
- `_lakelogic_created_at` → immutable (first-insert time)

---

## SCD2 (History Tracking)

See [Dimensional Modeling](dimensional_modeling.md) for full SCD2 configuration.

---

## Overwrite

Replaces all data on each run. Use for pre-aggregated Gold tables that are recomputed daily.

!!! example "Example: Daily overwrite with monthly partitions"

    ```yaml
    materialization:
      strategy: "overwrite"
      partition_by: ["month"]
    ```

---

## Partitioning & Clustering

Partitioning organises data into physical directories. Clustering sorts data within partitions for faster queries.

!!! example "Example: Partition by country, cluster by customer"

    ```yaml
    materialization:
      partition_by: ["country", "created_date"]
      cluster_by: ["customer_id"]
    ```

---

## Target Path & Format

!!! example "Example: Explicit target path"

    ```yaml
    materialization:
      target_path: "{data_root}/{silver_layer}_{system}_customers"
      format: "delta"                # parquet | delta | iceberg | csv
      location: "abfss://container@account.dfs.core.windows.net/silver/customers/"
    ```

---

## Soft Deletes (CDC)

When `load_mode: cdc`, LakeLogic automatically handles soft deletes — marking rows as deleted rather than physically removing them. This preserves audit trails and enables "as-of" queries.

!!! example "Example: Soft delete column configuration"

    ```yaml
    materialization:
      soft_delete_column: "_lakelogic_is_deleted"
      soft_delete_value: true
      soft_delete_time_column: "_lakelogic_deleted_at"
      soft_delete_reason_column: "_lakelogic_delete_reason"
    ```

---

## Table Properties

!!! example "Example: Auto-optimize Delta tables"

    ```yaml
    materialization:
      table_properties:
        "delta.autoOptimize.optimizeWrite": "true"
        "delta.autoOptimize.autoCompact": "true"

      compaction:
        auto: true
        vacuum_retention_hours: 168
    ```

---

## Fact Table Configuration

See [Dimensional Modeling](dimensional_modeling.md) for the `fact:` block (transaction, periodic snapshot, accumulating snapshot, factless, aggregate).
