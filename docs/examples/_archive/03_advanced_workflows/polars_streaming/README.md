# Polars Streaming — Process Files Larger Than Memory

Use Polars `scan_*` (lazy evaluation) and `sink_*` (streaming writes)
to process datasets that exceed available RAM.

## Components

- **streaming_demo.py**: End-to-end streaming pipeline example.
- **streaming_contract.yaml**: Contract for streaming source.

## When to Use This Pattern

- **Files Larger Than RAM**: Process 50GB Parquet files on a 16GB machine.
- **Cost Reduction**: Avoid scaling up compute just for large file processing.
- **ETL Pipelines**: Stream from source → validate → sink to target without
  ever materialising the full dataset in memory.
- **Batch Processing**: Process millions of rows with constant memory usage.

## How It Works

```python
# Traditional (eager): loads everything into memory
proc = DataProcessor("contract.yaml", engine="polars")
result = proc.run_source("huge_file.parquet")  # 💥 Out of memory

# Streaming (lazy): constant memory usage
result = proc.run_source_streaming("huge_file.parquet")  # ✅ Works
```

Under the hood:
1. `scan_parquet()` creates a LazyFrame (no data loaded)
2. Contract rules are applied as lazy expressions
3. Only when you `.collect()` or `.sink_parquet()` does execution happen
4. Polars streams data in chunks, never loading the full file

## Supported Formats

| Format | Scan Function | Sink Function |
|---|---|---|
| Parquet | `scan_parquet` | `sink_parquet` |
| CSV | `scan_csv` | `sink_csv` |
| NDJSON | `scan_ndjson` | — |

## Prerequisites

- LakeLogic with `polars` engine.
- Understanding of **run_source()** from **01_quickstart/**.

## Next Steps

- Combine with **partitioned_merge/** for streaming into partitioned tables.
- **05_orchestration/** for scheduling streaming jobs.
