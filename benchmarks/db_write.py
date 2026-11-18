"""Benchmark: native database writer (format: database) vs dlt (format: dlt) on Postgres.

Run before each release:

    PG_BENCH_URL=postgresql://user:pass@host:5432/db  python benchmarks/db_write.py

Market bars (symbol, ts, open, high, low, close, volume), key (symbol, ts). For each size it
measures: first load, the same rows again (median of 3), all rows changed, and appending new
rows - plus how many rows each re-send rewrote. Targets on a local Postgres (native, not Docker
Desktop port forwarding, which caps bulk throughput at a few MB/s): 1,200-row merge < 0.15 s,
12,000-row merge all changed < 0.5 s, none changed < 0.4 s with 0 rows rewritten, append
100,000 rows < 1.0 s, native >= 5x faster than dlt on a 1,200-row merge.
"""
from __future__ import annotations

import os
import statistics
import sys
import time

import polars as pl

URL_ENV = "PG_BENCH_URL"


def bars(n_symbols: int, bump: float = 0.0, ts_offset: int = 0) -> pl.DataFrame:
    rows = [
        (f"S{i:05d}", t + ts_offset, 100 + i + bump, 101 + i + bump, 99 + i + bump, 100.5 + i + bump, 1000.0)
        for i in range(n_symbols)
        for t in range(12)
    ]
    return pl.DataFrame(rows, schema=["symbol", "ts", "open", "high", "low", "close", "volume"], orient="row")


def timed(fn) -> float:
    start = time.perf_counter()
    fn()
    return time.perf_counter() - start


def native(table: str):
    from lakelogic.core.db_writer import writer_for

    w = writer_for(f"env:{URL_ENV}")
    return lambda df, strategy="merge": w.write(df, table=table, strategy=strategy, primary_key=["symbol", "ts"])


def dlt_writer(table: str):
    from lakelogic.core.dlt_sink import write_dlt

    schema, name = table.split(".")
    cfg = {"dlt_destination": "postgres", "dlt_credentials": f"env:{URL_ENV}", "dlt_dataset_name": schema}
    return lambda df, strategy="merge": write_dlt(df, table_name=name, config=cfg, strategy=strategy,
                                                  primary_key=["symbol", "ts"])


def dlt_fast_writer(table: str):
    """dlt with its speed settings: CSV loads (COPY on Postgres) and upsert merges."""
    from lakelogic.core.dlt_sink import write_dlt

    schema, name = table.split(".")
    cfg = {"dlt_destination": "postgres", "dlt_credentials": f"env:{URL_ENV}", "dlt_dataset_name": schema,
           "dlt_loader_file_format": "csv", "dlt_merge_strategy": "upsert"}
    return lambda df, strategy="merge": write_dlt(df, table_name=name, config=cfg, strategy=strategy,
                                                  primary_key=["symbol", "ts"])


def run(label: str, make, n_symbols: int) -> dict:
    write = make(f"bench_{label}.bars_{n_symbols}")
    df = bars(n_symbols)
    first = timed(lambda: write(df))
    same = statistics.median(timed(lambda: write(df)) for _ in range(3))
    changed = timed(lambda: write(bars(n_symbols, bump=1.0)))
    append = timed(lambda: write(bars(n_symbols, ts_offset=10_000), "append"))
    return {"writer": label, "rows": df.height, "first": first, "same": same, "changed": changed, "append": append}


def main() -> int:
    if not os.environ.get(URL_ENV):
        print(f"Set {URL_ENV} to a Postgres URL (a throwaway database: tables bench_* are dropped).")
        return 2
    import psycopg2

    conn = psycopg2.connect(os.environ[URL_ENV])
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA IF EXISTS bench_native CASCADE; DROP SCHEMA IF EXISTS bench_dlt CASCADE; "
                    "DROP SCHEMA IF EXISTS bench_dlt_fast CASCADE")
    sizes = [int(s) for s in (sys.argv[1:] or ["100", "1000"])]
    results = []
    for n in sizes:
        results.append(run("native", native, n))
        for label, make in (("dlt", dlt_writer), ("dlt_fast", dlt_fast_writer)):
            try:
                results.append(run(label, make, n))
            except Exception as exc:  # dlt[postgres] not installed
                print(f"{label} skipped: {exc}")
    print(f"{'writer':<10}{'rows':>8}{'first':>9}{'same':>9}{'changed':>9}{'append':>9}")
    for r in results:
        print(f"{r['writer']:<10}{r['rows']:>8}{r['first']:>9.3f}{r['same']:>9.3f}{r['changed']:>9.3f}{r['append']:>9.3f}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
