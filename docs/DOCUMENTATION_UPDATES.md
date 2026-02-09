# Documentation Updates Summary

## ✅ Corrections Made

### Issue Identified
The `DataProcessor` methods (`run()` and `run_source()`) return a `ValidationResult` object that unpacks to **3 values**, not 2:

```python
source_df, good_df, bad_df = proc.run(df)
source_df, good_df, bad_df = proc.run_source("data.parquet")
```

### What Each DataFrame Contains

1. **`source_df`** (or `result.raw`): Original source data before any transformations
2. **`good_df`** (or `result.good`): Records that passed all quality rules
3. **`bad_df`** (or `result.bad`): Records that failed quality rules (quarantined)

### 100% Reconciliation Guarantee

```python
# This ALWAYS holds true:
len(source_df) == len(good_df) + len(bad_df)
```

## 📚 New Documentation Created

### 1. **Return Values Guide** (`docs/return_values.md`)
Comprehensive documentation covering:
- ✅ Correct return signature (3 values)
- ✅ What each DataFrame contains
- ✅ Reconciliation guarantee explanation
- ✅ Common usage patterns
- ✅ Engine-specific behavior
- ✅ Inspecting quarantined records
- ✅ Common mistakes and fixes
- ✅ Advanced custom materialization

**Added to navigation:** Concepts → Return Values

### 2. **Contract Template Reference** (`docs/contract_template.md`)
Complete annotated template showing:
- ✅ All 17 configuration sections
- ✅ Business value for each option
- ✅ Use case scenarios
- ✅ 20+ transformation types
- ✅ Ready-to-copy templates for Bronze/Silver/Gold
- ✅ Quick reference table

**Added to navigation:** Reference → Contract Template

### 3. **Architecture Diagram** (`docs/architecture_diagram.md`)
Revised version with:
- ✅ Narrower diagrams (max 60 chars) - no horizontal scrolling
- ✅ All information preserved
- ✅ Additional sections (multi-engine, patterns, integrations)
- ✅ Better readability

## 📝 Files That Need Updating

The following documentation files currently show incorrect 2-value unpacking and should be updated to 3-value unpacking:

### High Priority (User-Facing Tutorials)
- `docs/tutorials/basic_validation.md` - Line 39
- `docs/tutorials/medallion_architecture.md` - Lines 75, 79

### Medium Priority (Pattern Examples)
- `docs/patterns/bronze_quality_gate.md` - Line 77
- `docs/patterns/dedup_survivorship.md` - Line 85
- `docs/patterns/scd2_dimension.md` - Lines 96, 98
- `docs/patterns/late_arriving_reprocess.md` - Lines 89, 93

### Recommended Updates

**Before:**
```python
good_df, bad_df = proc.run("data.csv")
```

**After:**
```python
source_df, good_df, bad_df = proc.run_source("data.csv")

# Or if you don't need source:
_, good_df, bad_df = proc.run_source("data.csv")
```

## 🎯 Key Takeaways for Users

1. **Always unpack 3 values** from `run()` or `run_source()`
2. **Use `source_df`** for reconciliation and audit trails
3. **Check quarantine ratio** before materializing
4. **100% reconciliation** is guaranteed: `source = good + bad`
5. **Use attributes** if preferred: `result.raw`, `result.good`, `result.bad`

## 📍 Navigation Updates

**mkdocs.yml changes:**
- Added `return_values.md` to **Concepts** section
- Added `contract_template.md` to **Reference** section (first item)

---

*Last Updated: 2026-02-09*
