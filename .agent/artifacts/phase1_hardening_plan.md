# LakeLogic Phase 1: Narrow & Harden

## Objective
Take LakeLogic from "wide-but-fragile beta" to "narrow-but-bulletproof core" in 3 months.

## P0 — Fix Before Next Release (This Session)

### 1. Fix `ValidationResult` API Stability
- **Problem:** `ValidationResult` unpacks as 3-tuple (raw, good, bad) but tests and downstream code use 2-tuple unpacking `good_df, bad_df = processor.run(df)`.
- **Fix:** Support both 2-tuple and 3-tuple unpacking via `__iter__` that yields `(good, bad)` by default. Keep `.raw` as attribute-only access.
- **Impact:** Tests, CLI driver, all consumer code.
- **Files:** `processor.py` (lines 19-51), `test_processor.py`, `test_materialization.py`, `test_ingestion.py`

### 2. Clean Stray Whitespace (Line 18)
- **File:** `processor.py` line 18

### 3. Fix Bare `except` in `_get_sample_text`
- **File:** `processor.py` line 489

### 4. Synchronize Version
- **`pyproject.toml`:** `0.1.0b3`
- **`__init__.py`:** `0.1.0`
- **Fix:** Align both to `0.1.0b3` with dynamic version from pyproject.toml

### 5. Initialize `rel = None` in DuckDB `run_source` Branch
- **File:** `processor.py` line 577

### 6. Add `pytest` to CI Pipeline
- **File:** `.github/workflows/ci-gate.yml`
- **Action:** Add a test job that installs deps and runs `pytest tests/ -x -q`

## P1 — Next Sprint (Follow-up Sessions)

### 7. Add `run_source` Tests
- Test Polars CSV, Parquet, multi-file loading
- Test DuckDB CSV/Parquet
- Test incremental load mode
- Test error case (no source path)

### 8. Add Tracing Tests
- Test `trace_step` context manager
- Test `show_trace` with and without trace data
- Test `_log_row_samples`

### 9. Stabilize Core Engine Tests
- Ensure all existing tests pass in CI
- Add DuckDB adapter tests (mirror Polars tests)

### 10. Refactor `processor.py` into Focused Modules ✅
- ~~Extract `source_loader.py` from `run_source`~~ (kept in processor — too tightly coupled)
- Extract `slo.py` from SLO computation ✅
- Extract `external_logic.py` from script/notebook execution ✅
- Extract `lineage.py` from lineage injection + add_columns ✅
- Keep `processor.py` as orchestrator only (1712 → 1130 lines, -34%)

## P2 — Backlog

### 11. Thread-safety audit for cloud credential resolver ✅
- Added `threading.Lock` to `CloudCredentialResolver` for token cache and secret cache
- Added double-checked locking to `get_credential_resolver()` singleton

### 12. Add sandbox/timeout for external logic execution ✅
- Added configurable timeout (default 300s) via `threading.Thread.join(timeout=...)`
- Added restricted-import sandbox: blocks subprocess, shutil, socket and exec/eval/compile

### 13. Split `materialization.py` ✅
- Extracted `quarantine.py` (quarantine writes: Spark, DuckDB, SQLite, Snowflake, BigQuery)
- Extracted `run_log.py` (run-log persistence: report flattening, multi-backend writes, watermark queries)
- `materialization.py` reduced from 2,095 → 1,109 lines (-47%)
- Backwards-compatible re-exports: all existing imports still work
- All 49 tests pass (45 + 4 materialization)

### 14. Split `driver.py`
- `driver.py` is 2,035 lines — candidate for future extraction

### 15. Integration test suite for DuckDB + Spark
### 16. Property-based tests for transformation logic
