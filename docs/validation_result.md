# ValidationResult - Accessing LakeLogic Results

LakeLogic's `run_source()` and `run()` methods return a `ValidationResult` object that provides flexible access to your processed data.

## Access Patterns

### 1. Named Attributes (Recommended ✨)

The most readable and self-documenting approach:

```python
from lakelogic import DataProcessor

processor = DataProcessor(contract="contract.yaml")
result = processor.run_source()

# Access via descriptive names
original_data = result.raw  # Raw data before validation
validated_data = result.good  # Records that passed validation
quarantined_data = result.bad  # Records that failed validation

print(result)  # ValidationResult(good=150, bad=10, raw=160)
```

### 2. Tuple Unpacking (Backward Compatible)

For concise code or when you want all three at once:

```python
# Unpack all three dataframes
df_raw, df_good, df_bad = processor.run_source()

# Or just the ones you need
_, validated, _ = processor.run_source()  # Only good data
```

### 3. Index Access

Access by position if needed:

```python
result = processor.run_source()

raw = result[0]
good = result[1]
bad = result[2]
```

## Use Cases

### Production Pipeline (Named Attributes)
```python
result = processor.run_source("data/daily_transactions.csv")

# Write validated data to warehouse
warehouse.write_table("transactions", result.good)

# Send quarantined data to data quality team
if len(result.bad) > 0:
    notify_dq_team(result.bad)
    
# Archive raw data for audit
archive.store(f"raw/{date}", result.raw)
```

### Quick Script (Tuple Unpacking)
```python
# Fast iteration during development
_, valid, invalid = processor.run_source()

print(f"✅ Valid: {len(valid)}")
print(f"❌ Invalid: {len(invalid)}")
```

### Data Quality Dashboard
```python
result = processor.run_source()

metrics = {
    "total_records": len(result.raw),
    "valid_records": len(result.good),
    "quarantined_records": len(result.bad),
    "pass_rate": len(result.good) / len(result.raw) * 100,
}

dashboard.update(metrics)
```

## Why Named Attributes?

**Benefits:**
- ✅ **Self-documenting**: `result.good` is clearer than `result[1]`
- ✅ **IDE support**: Autocomplete and type hints work better
- ✅ **Refactor-safe**: Less prone to mistakes from reordering
- ✅ **Readable code**: `validated_employees = result.good` vs `validated_employees = df_good`

**When to use tuple unpacking:**
- ⚡ Quick scripts and notebooks
- 🔄 Migrating existing code
- 📝 When you want all three upfront

## Under the Hood

The `ValidationResult` class implements both protocols:

```python
class ValidationResult:
    def __init__(self, good, bad, raw):
        self.good = good  # Named attributes
        self.bad = bad
        self.raw = raw

    def __iter__(self):  # Tuple unpacking
        yield self.raw
        yield self.good
        yield self.bad
```

This gives you the flexibility to use whichever pattern fits your use case!
