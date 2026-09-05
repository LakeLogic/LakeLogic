# DataProcessor Return Values - Correct Usage

## ✅ Correct Return Signature

Both `run()` and `run_source()` return a `ValidationResult` object that unpacks to **2 values**:

```python
from lakelogic import DataProcessor

proc = DataProcessor(contract="contract.yaml")

# CORRECT: Unpack 2 values
good_df, bad_df = proc.run(df)
good_df, bad_df = proc.run_source("data.parquet")

# ALSO CORRECT: Use attributes
result = proc.run(df)
result.raw  # Original source data (pre-validation)
result.good  # Records that passed validation (same as good_df)
result.bad  # Records that failed validation (quarantined, same as bad_df)
result.trace  # Step-by-step execution trace
```

## 📊 What Each DataFrame Contains

### 1. `result.raw`
**Original source data before validation**

- Contains all input rows exactly as loaded
- Useful for reconciliation: `len(result.raw) == len(good_df) + len(bad_df)`
- No quality rules applied yet
- No transformations applied

**Use cases:**
- Audit trails
- Reconciliation reports
- Comparing before/after transformations

### 2. `good_df` (or `result.good`)
**Records that passed all quality rules**

- Schema enforcement applied
- All transformations applied (pre and post)
- All row-level quality rules passed
- All dataset-level quality rules passed
- Ready for materialization to next layer

### 3. `bad_df` (or `result.bad`)
**Records that failed quality rules (quarantined)**

- Contains original data plus error metadata
- Includes `_lakelogic_errors` column (array of error messages)
- Includes `_lakelogic_categories` column (error categories)
- Includes `quarantine_state` and `quarantine_reprocessed` columns

## 🔄 Reconciliation Guarantee

LakeLogic guarantees **100% reconciliation**:

```python
result = proc.run_source("data.parquet")
good_df, bad_df = result

# This ALWAYS holds true:
assert len(result.raw) == len(good_df) + len(bad_df)
```

Every input row is accounted for—either in `good_df` or `bad_df`. Nothing is silently dropped.

## 📝 Common Usage Patterns

### Pattern 1: Simple Unpacking (Recommended)

```python
from lakelogic import DataProcessor

proc = DataProcessor(contract="contracts/silver_customers.yaml")
good_df, bad_df = proc.run_source("bronze/customers.parquet")

print(f"Good: {len(good_df)} rows")
print(f"Quarantined: {len(bad_df)} rows")

# Materialize good data
proc.materialize(good_df, bad_df)
```

### Pattern 2: Full Reconciliation Check

```python
result = proc.run_source("data.parquet")
good_df, bad_df = result

source_count = len(result.raw)
good_count = len(good_df)
bad_count = len(bad_df)

assert source_count == good_count + bad_count, f"Reconciliation failed: {source_count} != {good_count} + {bad_count}"

print(f"✅ 100% reconciliation: {source_count} = {good_count} + {bad_count}")
```

### Pattern 3: Threshold-Based Materialization

```python
result = proc.run_source("data.parquet")
good_df, bad_df = result

quarantine_ratio = len(bad_df) / len(result.raw)
THRESHOLD = 0.10  # 10%

if quarantine_ratio > THRESHOLD:
    raise ValueError(
        f"Quarantine ratio {quarantine_ratio:.2%} exceeds threshold {THRESHOLD:.2%}. Investigate before materializing."
    )

proc.materialize(good_df, bad_df)
```

## 🎯 Engine-Specific Behavior

The return type adapts to the engine:

| Engine | `result.raw` Type | `good_df` Type | `bad_df` Type |
|--------|------------------|----------------|---------------|
| **Polars** | `pl.DataFrame` | `pl.DataFrame` | `pl.DataFrame` |
| **Pandas** | `pd.DataFrame` | `pd.DataFrame` | `pd.DataFrame` |
| **Spark** | `pyspark.sql.DataFrame` | `pyspark.sql.DataFrame` | `pyspark.sql.DataFrame` |
| **DuckDB** | `duckdb.DuckDBPyRelation` | `duckdb.DuckDBPyRelation` | `duckdb.DuckDBPyRelation` |

## 🔍 Inspecting Quarantined Records

```python
good_df, bad_df = proc.run_source("data.parquet")

if len(bad_df) > 0:
    print(f"\n🛑 {len(bad_df)} records quarantined\n")

    # View error reasons (Polars example)
    import polars as pl

    errors = bad_df.select(
        [
            "customer_id",
            pl.col("_lakelogic_errors").alias("errors"),
            pl.col("_lakelogic_categories").alias("categories"),
        ]
    ).explode(["errors", "categories"])

    print(errors)
```

## 📚 Summary

| Method | Returns | Unpacks To |
|--------|---------|------------|
| `proc.run(df)` | `ValidationResult` | `good_df, bad_df` |
| `proc.run_source(path)` | `ValidationResult` | `good_df, bad_df` |

**Key Points:**
- ✅ Unpacks to **2 DataFrames**
- ✅ `result.raw` = original data (pre-validation)
- ✅ `good_df` = passed validation (post-transformation)
- ✅ `bad_df` = failed validation (quarantined)
- ✅ Reconciliation: `len(result.raw) == len(good_df) + len(bad_df)`
