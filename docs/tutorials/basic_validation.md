# Tutorial: Basic Validation

The simplest LakeGuard example. Perfect for your first run.

## What You'll Learn

1. **Schema Definition** - Define field names and types
2. **Quality Rules** - Validate email format and positive age
3. **Transformations** - Deduplicate and derive new columns
4. **Quarantine** - See how bad records are captured with error reasons

## Files

The example files are located at:

```text
examples/01_getting_started/basic_validation/
├── contract.yaml           # The data contract
├── data/
│   └── sample_customers.csv  # Sample input data
└── tutorial.ipynb          # Interactive notebook walkthrough
```

## Run It

### Option 1: Command Line

```bash
cd examples/01_getting_started/basic_validation
lakeguard run --contract contract.yaml --source data/sample_customers.csv
```

### Option 2: Python

```python
from lakeguard import DataProcessor

proc = DataProcessor(contract="contract.yaml")
good_df, bad_df = proc.run("data/sample_customers.csv")

print(f"Good records: {len(good_df)}")
print(f"Quarantined: {len(bad_df)}")
```

### Option 3: Jupyter Notebook

Open `examples/01_getting_started/basic_validation/tutorial.ipynb` for an interactive walkthrough.

## Contract Breakdown

```yaml
# Schema: Define expected fields
model:
  fields:
    - name: id
      type: long
      required: true
    - name: email
      type: string
      required: true

# Quality: Rules that must pass
quality:
  row_rules:
    - name: "Valid Email"
      sql: "email LIKE '%@%'"
    - name: "Valid Age"
      sql: "age > 0"

# Quarantine: What happens to failures
quarantine:
  enabled: true
  include_error_reason: true
```

## Expected Output

- **Good records**: Pass all quality rules - ready for downstream use
- **Quarantined records**: Failed one or more rules - includes `_error_reason` column

## Next Steps

- [Medallion Architecture](medallion_architecture.md) - Learn Bronze → Silver pipelines
- [Notifications & Secrets](notifications_secrets.md) - Configure alerts
