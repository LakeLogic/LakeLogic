# Watermark Strategies

Watermark strategies control **how LakeLogic tracks incremental progress** — which data has already been processed and what's new since the last run.

> **Think of it like a bookmark.** Each time you read a book, you place a bookmark where you stopped. Next time, you start from the bookmark — not from page one. Watermarks work the same way for your data: they remember the "last page you read" so you only process new data.

!!! info "All YAML blocks below are examples — copy only the strategy that fits your use case."

---

## Strategy Comparison

| Strategy | Best For | Source Type | State Stored In | Requires Spark? |
| --- | --- | --- | --- | --- |
| `max_target` | Most batch pipelines | Table only | Target table (self-heal) | No |
| `pipeline_log` | Cross-layer increments (Bronze → Silver) | File or Delta | `_run_logs` table | No |
| `lookback` | Simple rolling windows | File or Table | None (stateless) | No |
| `date_range` | Backfills & widgets | File or Table | None (explicit dates) | No |
| `manifest` | Non-Spark pipelines | File only | JSON manifest file | No |
| `delta_version` | Large streaming tables | Table only ⚠️ | Target TBLPROPERTIES | Yes |

### Cross-Validation Rules (enforced at runtime)

- `delta_version` on a file source → **raises ValueError** (no Delta transaction log)
- `pipeline_log` on a `type: table` source → **raises ValueError** (use `type: delta` for table-backed Bronze sources)
- For cross-layer incremental (Bronze → Silver), use `watermark_strategy: pipeline_log` with `type: delta`

---

## Strategy 1: `max_target` (Default)

Queries `MAX(watermark_field)` on the target Delta table. Self-healing — if target is manually edited, next run re-reads from the actual high-water mark.

```yaml
source:
  type: table
  path: "{domain_catalog}.{bronze_layer}_{system}_events"
  load_mode: incremental
  watermark_strategy: max_target
  watermark_field: "_lakelogic_processed_at"
  target_path: "abfss://silver@acct.dfs.core.windows.net/events"
```

---

## Strategy 2: `pipeline_log`

Queries the `_run_logs` table for the last successful processing boundary. Works with **both file-based and Delta table sources** — making it the recommended strategy for cross-layer reads (Bronze → Silver, Silver → Gold).

### How It Works

The engine queries `_run_logs` filtering by `dataset`, `data_layer`, `domain`, and `system`, then resolves the boundary using a **priority cascade**:

| Priority | Column | Description |
| --- | --- | --- |
| 1 | `max_source_mtime` | Epoch timestamp of the newest file processed in the prior run |
| 2 | `max_watermark_value` | Explicit watermark value recorded by the prior run |
| 3 | `timestamp` | Run timestamp (fallback when neither above is available) |

Only successful runs are considered — failed runs and `no_new_data` runs are excluded.

### File Sources (Landing → Bronze)

For file-based sources, `pipeline_log` compares file modification times against the `max_source_mtime` from the last run. Only files newer than the watermark are read.

```yaml
source:
  type: landing
  path: "abfss://landing@acct.dfs.core.windows.net/events/"
  load_mode: incremental
  watermark_strategy: pipeline_log
```

### Delta Table Sources (Bronze → Silver)

For Delta table sources, `pipeline_log` converts the epoch watermark to an ISO timestamp and applies it as a filter on the watermark column (typically `_lakelogic_processed_at`). This avoids reading the entire Bronze table on each Silver run.

```yaml
source:
  type: delta
  path: "{bronze_path}/{bronze_layer}_{system}_driver_profiles"
  load_mode: incremental
  watermark_strategy: pipeline_log
```

> **Why not `type: table`?** The `type: table` source type triggers a Spark-native `spark.table()` read, which is not compatible with `pipeline_log`. Use `type: delta` for cross-layer reads — LakeLogic reads the Delta directory directly and applies the watermark filter at the storage layer.

### State

State is stored in the `_run_logs` table (configured via `metadata.run_log_table`).

- **Filters by:** `dataset`, `data_layer`, `domain`, `system`
- **Excludes:** failed runs, `no_new_data` runs, reprocess runs
- **Fallback:** if no prior runs exist, scans all data (initial load)

---

## Strategy 3: `lookback`

Sliding window: processes everything from `NOW - duration` to `NOW`. No state needed.

```yaml
# Daily batch
source:
  watermark_strategy: lookback
  lookback: "7 days"
  watermark_field: "_lakelogic_processed_at"

# Near-real-time
source:
  watermark_strategy: lookback
  lookback: "3 hours"
  watermark_field: "event_timestamp"

# Monthly batch
source:
  watermark_strategy: lookback
  lookback: "1 month"
```

Supported durations: `"N days"`, `"N hours"`, `"N mins"`, `"N month(s)"`.

---

## Strategy 4: `date_range`

Explicit from/to dates. Ideal for backfills and Databricks Widgets.

```yaml
source:
  type: table
  path: "{domain_catalog}.{bronze_layer}_{system}_events"
  load_mode: incremental
  watermark_strategy: date_range
  from_date: "2024-01-01"
  to_date: "2024-12-31"
  watermark_field: "event_date"
```

Tip: `from_date`/`to_date` can be overridden at runtime via pipeline params.

---

## Strategy 5: `manifest`

Reads a JSON manifest file listing already-processed partitions. Lightweight alternative for non-Spark environments.

```yaml
source:
  type: landing
  path: "abfss://landing@acct.dfs.core.windows.net/events/"
  load_mode: incremental
  watermark_strategy: manifest
  manifest_path: "abfss://meta@acct.dfs.core.windows.net/manifests/events.json"
  watermark_field: "_snapshot_date"
```

Manifest JSON format:

```json
{ "processed_partitions": ["2024-03-20", "2024-03-21"], "last_updated": "..." }
```

---

## Strategy 6: `delta_version`

Uses Delta transaction log versions instead of timestamp columns. Reads only specific Parquet files added/changed between versions. Fastest strategy for large tables.

⚠️ **Table sources only** — raises ValueError on file sources. Spark-only.

```yaml
source:
  type: table
  path: "catalog.bronze.sessions"
  load_mode: incremental
  watermark_strategy: delta_version
  target_path: "table:catalog.silver.sessions"
```

Safeguards: detects source rollback (`from_v > to_v`) and auto-resets.

---

## Multi-Column Partitions

When temporal boundaries span multiple columns:

```yaml
source:
  watermark_date_parts: ["year", "month", "day"]
  # Also accepts named dict:
  # watermark_date_parts:
  #   year: "partition_year"
  #   month: "partition_month"
```

## Static Partition Filters

Scope incremental reads to specific partitions:

```yaml
source:
  partition_filters:
    country: "GB"
    region: "south"
```
