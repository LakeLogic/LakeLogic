# DataProcessor Return Values - Correct Usage

## ✅ Correct Return Signature

Both `run()` and `run_source()` return a `ValidationResult` object that unpacks to **3 values**:

```python
from lakelogic import DataProcessor

proc = DataProcessor(contract="contract.yaml")

# CORRECT: Unpack all 3 values
source_df, good_df, bad_df = proc.run(df)
source_df, good_df, bad_df = proc.run_source("data.parquet")

# ALSO CORRECT: Use attributes
result = proc.run(df)
result.raw    # Original source data (before transformations)
result.good   # Records that passed validation
result.bad    # Records that failed validation (quarantined)
```

## 📊 What Each DataFrame Contains

### 1. `source_df` (or `result.raw`)
**Original source data before any transformations**

- Contains all input rows exactly as loaded
- Useful for reconciliation: `len(source_df) == len(good_df) + len(bad_df)`
- No quality rules applied yet
- No transformations applied

**Use cases:**
- Audit trails
- Reconciliation reports
- Debugging data issues
- Comparing before/after transformations

### 2. `good_df` (or `result.good`)
**Records that passed all quality rules**

- Schema enforcement applied
- All transformations applied (pre and post)
- All row-level quality rules passed
- All dataset-level quality rules passed
- Ready for materialization to next layer

**Use cases:**
- Write to Silver/Gold layer
- Feed to downstream systems
- Analytics and reporting

### 3. `bad_df` (or `result.bad`)
**Records that failed quality rules (quarantined)**

- Contains original data plus error metadata
- Includes `_lakelogic_errors` column (array of error messages)
- Includes `_lakelogic_categories` column (error categories)
- Includes `quarantine_state` and `quarantine_reprocessed` columns

**Use cases:**
- Root cause analysis
- Data quality monitoring
- Correction workflows
- Reprocessing after fixes

## 🔄 Reconciliation Guarantee

LakeLogic guarantees **100% reconciliation**:

```python
source_df, good_df, bad_df = proc.run_source("data.parquet")

# This ALWAYS holds true:
assert len(source_df) == len(good_df) + len(bad_df)
```

Every input row is accounted for—either in `good_df` or `bad_df`. Nothing is silently dropped.

## 📝 Common Usage Patterns

### Pattern 1: Full Unpacking (Recommended)

```python
from lakelogic import DataProcessor

proc = DataProcessor(contract="contracts/silver_customers.yaml")
source_df, good_df, bad_df = proc.run_source("bronze/customers.parquet")

print(f"Source: {len(source_df)} rows")
print(f"Good: {len(good_df)} rows")
print(f"Quarantined: {len(bad_df)} rows")
print(f"Quarantine ratio: {len(bad_df) / len(source_df):.2%}")

# Materialize good data
proc.materialize(good_df, bad_df)
```

### Pattern 2: Using Attributes

```python
proc = DataProcessor(contract="contract.yaml")
result = proc.run_source("data.parquet")

# Access via attributes
print(f"Source: {len(result.raw)} rows")
print(f"Good: {len(result.good)} rows")
print(f"Quarantined: {len(result.bad)} rows")

# Materialize
proc.materialize(result.good, result.bad)
```

### Pattern 3: Ignoring Source (Not Recommended)

```python
# If you don't need the source DataFrame, use underscore
_, good_df, bad_df = proc.run_source("data.parquet")

# But this loses reconciliation capability!
```

### Pattern 4: Reconciliation Check

```python
source_df, good_df, bad_df = proc.run_source("data.parquet")

# Verify 100% reconciliation
source_count = len(source_df)
good_count = len(good_df)
bad_count = len(bad_df)

assert source_count == good_count + bad_count, \
    f"Reconciliation failed: {source_count} != {good_count} + {bad_count}"

print(f"✅ 100% reconciliation: {source_count} = {good_count} + {bad_count}")
```

### Pattern 5: Conditional Materialization

```python
source_df, good_df, bad_df = proc.run_source("data.parquet")

# Check quarantine threshold
quarantine_ratio = len(bad_df) / len(source_df)
THRESHOLD = 0.10  # 10%

if quarantine_ratio > THRESHOLD:
    raise ValueError(
        f"Quarantine ratio {quarantine_ratio:.2%} exceeds threshold {THRESHOLD:.2%}. "
        f"Investigate before materializing."
    )

# Safe to materialize
proc.materialize(good_df, bad_df)
```

## 🎯 Engine-Specific Behavior

The return type adapts to the engine:

| Engine | `source_df` Type | `good_df` Type | `bad_df` Type |
|--------|------------------|----------------|---------------|
| **Polars** | `pl.DataFrame` | `pl.DataFrame` | `pl.DataFrame` |
| **Pandas** | `pd.DataFrame` | `pd.DataFrame` | `pd.DataFrame` |
| **Spark** | `pyspark.sql.DataFrame` | `pyspark.sql.DataFrame` | `pyspark.sql.DataFrame` |
| **DuckDB** | `duckdb.DuckDBPyRelation` | `duckdb.DuckDBPyRelation` | `duckdb.DuckDBPyRelation` |

## 🔍 Inspecting Quarantined Records

```python
source_df, good_df, bad_df = proc.run_source("data.parquet")

if len(bad_df) > 0:
    print(f"\n🛑 {len(bad_df)} records quarantined\n")
    
    # View error reasons (Polars example)
    import polars as pl
    
    # Explode error array to see all failures
    errors = (
        bad_df
        .select([
            "customer_id",
            pl.col("_lakelogic_errors").alias("errors"),
            pl.col("_lakelogic_categories").alias("categories")
        ])
        .explode(["errors", "categories"])
    )
    
    print(errors)
    
    # Group by error type
    error_summary = (
        errors
        .group_by("errors")
        .agg(pl.count().alias("count"))
        .sort("count", descending=True)
    )
    
    print("\nError Summary:")
    print(error_summary)
```

## 📊 Lineage Columns

If `lineage.enabled: true` in your contract, the DataFrames will include:

```python
# good_df columns include:
_lakelogic_source           # Source file/table path
_lakelogic_processed_at     # Processing timestamp
_lakelogic_run_id           # Unique run identifier
_lakelogic_domain           # Domain from metadata
_lakelogic_system           # System from metadata

# bad_df columns include all of the above PLUS:
_lakelogic_errors           # Array of error messages
_lakelogic_categories       # Array of error categories
quarantine_state            # "active", "resolved", "ignored"
quarantine_reprocessed      # Boolean flag
```

## ⚠️ Common Mistakes

### ❌ Mistake 1: Only unpacking 2 values

```python
# WRONG: Missing source_df
good_df, bad_df = proc.run_source("data.parquet")
# This will fail! ValidationResult has 3 values.
```

**Fix:**
```python
# CORRECT: Unpack all 3 values
source_df, good_df, bad_df = proc.run_source("data.parquet")

# OR use underscore if you don't need source
_, good_df, bad_df = proc.run_source("data.parquet")
```

### ❌ Mistake 2: Assuming source_df == good_df + bad_df (concatenated)

```python
# WRONG: source_df is NOT a concatenation
source_df, good_df, bad_df = proc.run_source("data.parquet")

# source_df is the ORIGINAL data (before transformations)
# good_df + bad_df is AFTER transformations
# They may have different schemas!
```

**Fix:**
```python
# CORRECT: Understand the difference
source_df   # Original data, no transformations
good_df     # After transformations, passed validation
bad_df      # After transformations, failed validation

# Row count reconciliation holds:
len(source_df) == len(good_df) + len(bad_df)

# But schemas may differ if transformations add/remove columns
```

### ❌ Mistake 3: Not checking quarantine before materializing

```python
# WRONG: Blindly materialize without checking
source_df, good_df, bad_df = proc.run_source("data.parquet")
proc.materialize(good_df, bad_df)  # What if 90% is quarantined?
```

**Fix:**
```python
# CORRECT: Check quarantine ratio
source_df, good_df, bad_df = proc.run_source("data.parquet")

quarantine_ratio = len(bad_df) / len(source_df)
if quarantine_ratio > 0.10:  # 10% threshold
    raise ValueError(f"Too many quarantined records: {quarantine_ratio:.2%}")

proc.materialize(good_df, bad_df)
```

## 🚀 Advanced: Custom Materialization

```python
source_df, good_df, bad_df = proc.run_source("data.parquet")

# Custom logic: only materialize if quality is acceptable
quarantine_ratio = len(bad_df) / len(source_df)

if quarantine_ratio < 0.05:  # Less than 5% quarantined
    # High quality: materialize to production
    good_df.write_parquet("s3://prod-bucket/silver/customers.parquet")
    bad_df.write_parquet("s3://quarantine-bucket/customers.parquet")
    
elif quarantine_ratio < 0.20:  # 5-20% quarantined
    # Medium quality: materialize to staging for review
    good_df.write_parquet("s3://staging-bucket/silver/customers.parquet")
    bad_df.write_parquet("s3://quarantine-bucket/customers.parquet")
    
    # Send alert
    notify_team(f"Warning: {quarantine_ratio:.2%} quarantine ratio")
    
else:  # More than 20% quarantined
    # Low quality: don't materialize, investigate
    raise ValueError(
        f"Critical: {quarantine_ratio:.2%} quarantine ratio. "
        "Investigate before materializing."
    )
```

## 📚 Summary

| Method | Returns | Unpacks To |
|--------|---------|------------|
| `proc.run(df)` | `ValidationResult` | `source_df, good_df, bad_df` |
| `proc.run_source(path)` | `ValidationResult` | `source_df, good_df, bad_df` |

**Key Points:**
- ✅ Always returns 3 DataFrames
- ✅ `source_df` = original data (before transformations)
- ✅ `good_df` = passed validation (after transformations)
- ✅ `bad_df` = failed validation (quarantined)
- ✅ Reconciliation: `len(source_df) == len(good_df) + len(bad_df)`
- ✅ Can use attributes: `result.raw`, `result.good`, `result.bad`

---

*For more examples, see the [LakeLogic Documentation](https://lineagelogic.github.io/LakeLogic/)*
