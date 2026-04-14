# Watermark Strategies

Watermark strategies control **how LakeLogic tracks incremental progress** — which data has already been processed and what's new since the last run.

> **Think of it like a bookmark.** Each time you read a book, you place a bookmark where you stopped. Next time, you start from the bookmark — not from page one. Watermarks work the same way for your data: they remember the "last page you read" so you only process new data.

!!! info "All YAML blocks below are examples — copy only the strategy that fits your use case."

---

## Strategy Comparison

| Strategy | Best For | Source Type | State Stored In | Requires Spark? |
|---|---|---|---|---|
| `max_target` | Most batch pipelines | Table only | Target table (self-heal) | Yes |
| `pipeline_log` | File-based incremental | File only ⚠️ | `_run_logs` table | No |
| `lookback` | Simple rolling windows | File or Table | None (stateless) | No |
| `date_range` | Backfills & widgets | File or Table | None (explicit dates) | No |
| `manifest` | Non-Spark pipelines | File only | JSON manifest file | No |
| `delta_version` | Large streaming tables | Table only ⚠️ | Target TBLPROPERTIES | Yes |

### Cross-Validation Rules (enforced at runtime)

- `pipeline_log` on a table source → **raises ValueError** (no file mtimes to compare)
- `delta_version` on a file source → **raises ValueError** (no Delta transaction log)
- For table-to-table incremental, use `load_mode: cdc` with `cdc_timestamp_field`

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

Queries the `_run_logs` table for the last successful `max_source_mtime` of this dataset. Compares file modification times against the watermark.

⚠️ **File sources only** — raises ValueError on table sources.

```yaml
source:
  type: landing
  path: "abfss://landing@acct.dfs.core.windows.net/events/"
  load_mode: incremental
  watermark_strategy: pipeline_log
```

State: `_run_logs` table (configured via `metadata.run_log_table`). Filters by: dataset, data_layer, domain, system. Excludes: failed runs, no_new_data runs. Fallback: if no prior runs, scans all files.

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
