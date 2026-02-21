"""
Polars Streaming — Process large files with constant memory usage.

This example demonstrates:
  1. Generating a sample dataset
  2. Eager vs streaming processing comparison
  3. Streaming with sink to parquet (no .collect() needed)
  4. Memory-efficient validation pipeline
"""
from pathlib import Path
import time
import polars as pl

from lakelogic.core.processor import DataProcessor

# ── Setup ────────────────────────────────────────────────────────────────────

BASE = Path(__file__).parent
CONTRACT = BASE / "streaming_contract.yaml"
OUTPUT = BASE / "data"
OUTPUT.mkdir(exist_ok=True)

# ── 1. Generate Sample Data ─────────────────────────────────────────────────

print("=" * 70)
print("1. GENERATING SAMPLE DATA")
print("=" * 70)

n_rows = 100_000
sample = pl.DataFrame({
    "event_id": [f"EVT-{i:06d}" for i in range(n_rows)],
    "event_type": ["click" if i % 3 == 0 else "view" if i % 3 == 1 else "purchase"
                   for i in range(n_rows)],
    "user_id": [f"user_{i % 1000:04d}" for i in range(n_rows)],
    "timestamp": [f"2024-12-{(i % 28) + 1:02d}T{(i % 24):02d}:00:00" for i in range(n_rows)],
    "value": [float(i % 500) - 10.0 for i in range(n_rows)],  # Some negatives for quality rules
    "metadata": [f"source=web,session={i}" for i in range(n_rows)],
})

source_file = OUTPUT / "events.parquet"
sample.write_parquet(str(source_file))
file_size_mb = source_file.stat().st_size / (1024 * 1024)
print(f"  Created: {source_file}")
print(f"  Rows: {n_rows:,}")
print(f"  Size: {file_size_mb:.1f} MB")
print()

# ── 2. Eager Loading (Traditional) ──────────────────────────────────────────

print("=" * 70)
print("2. EAGER LOADING (Traditional read_parquet)")
print("=" * 70)

proc = DataProcessor(str(CONTRACT), engine="polars")

t0 = time.perf_counter()
df_eager = pl.read_parquet(str(source_file))
result_eager = proc.run(df_eager)
t1 = time.perf_counter()

good_eager = result_eager.good
if isinstance(good_eager, pl.LazyFrame):
    good_eager = good_eager.collect()
bad_eager = result_eager.bad
if isinstance(bad_eager, pl.LazyFrame):
    bad_eager = bad_eager.collect()

print(f"  Time: {t1 - t0:.3f}s")
print(f"  Good rows: {good_eager.height:,}")
print(f"  Bad rows:  {bad_eager.height:,}")
print(f"  Approach:  Entire file loaded into memory first")
print()

# ── 3. Streaming (Lazy scan_parquet) ─────────────────────────────────────────

print("=" * 70)
print("3. STREAMING (scan_parquet → lazy evaluation)")
print("=" * 70)

t0 = time.perf_counter()
result_stream = proc.run_source_streaming(str(source_file))
t1 = time.perf_counter()

good_stream = result_stream.good
if isinstance(good_stream, pl.LazyFrame):
    good_stream = good_stream.collect()
bad_stream = result_stream.bad
if isinstance(bad_stream, pl.LazyFrame):
    bad_stream = bad_stream.collect()

print(f"  Time: {t1 - t0:.3f}s")
print(f"  Good rows: {good_stream.height:,}")
print(f"  Bad rows:  {bad_stream.height:,}")
print(f"  Approach:  Polars scans and streams in chunks")
print()

# ── 4. Streaming with Sink (No .collect() needed) ───────────────────────────

print("=" * 70)
print("4. STREAMING WITH SINK (source → validate → target, no memory spike)")
print("=" * 70)

output_file = OUTPUT / "validated_events.parquet"

t0 = time.perf_counter()
result_sink = proc.run_source_streaming(str(source_file), output_path=str(output_file))
t1 = time.perf_counter()

output_size_mb = output_file.stat().st_size / (1024 * 1024)
readback = pl.read_parquet(str(output_file))

print(f"  Time: {t1 - t0:.3f}s")
print(f"  Output: {output_file}")
print(f"  Output size: {output_size_mb:.1f} MB")
print(f"  Output rows: {readback.height:,}")
print(f"  Approach:  Sink directly to disk, constant memory")
print()

# ── 5. Comparison ────────────────────────────────────────────────────────────

print("=" * 70)
print("5. SUMMARY")
print("=" * 70)
print(f"  Eager good rows:     {good_eager.height:,}")
print(f"  Streaming good rows: {good_stream.height:,}")
print(f"  Sink output rows:    {readback.height:,}")
print(f"  Results match:       {good_eager.height == good_stream.height}")
print()
print("  Key advantage of streaming:")
print("  • Memory usage proportional to CHUNK size, not FILE size.")
print("  • A 50GB file can be processed on a 8GB machine.")
print("  • sink_parquet() writes directly to disk without .collect().")
print()
print("✅ Polars streaming demo complete.")
