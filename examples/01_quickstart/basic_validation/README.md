# Basic Validation

The simplest LakeLogic example. Perfect for your first run.

## What You'll Learn

1. Schema definition with field types
2. Quality rules (email format, positive age)
3. Transformations and derived columns
4. Quarantine output with error reasons

## Files

basic_validation/
- contract.yaml
- data/sample_customers.csv
- tutorial.ipynb

## Run It

### Option 1: Command Line

```bash
lakelogic run --contract contract.yaml --source data/sample_customers.csv
```

### Option 2: Python

```python
from lakelogic import DataProcessor

proc = DataProcessor(contract="contract.yaml")
good_df, bad_df = proc.run("data/sample_customers.csv")

print(f"Good records: {len(good_df)}")
print(f"Quarantined: {len(bad_df)}")
```

### Option 3: Jupyter Notebook

Open tutorial.ipynb for an interactive walkthrough.

## Next Steps

- ../../02_core_patterns/medallion_architecture/ - Bronze and Silver stages
- ../../02_core_patterns/reference_joins/ - Enrich data with lookups
