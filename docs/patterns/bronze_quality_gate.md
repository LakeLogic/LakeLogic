# Bronze Quality Gate

Apply quality rules at the Bronze layer to reject obviously bad data early.

## When to Use

- Reject malformed emails before they enter your lakehouse
- Filter out records with missing required fields
- Apply dataset-level sanity checks (e.g., "at least 1 record")

## Files

```
examples/03_patterns/bronze_quality_gate/
├── contract.yaml
├── README.md
└── data/
    └── raw_signups.csv
```

## Contract

```yaml
dataset: bronze_web_signups

schema_policy:
  evolution: strict
  unknown_fields: drop

model:
  fields:
    - name: signup_id
      type: string
      required: true
    - name: email
      type: string
      required: true
    - name: event_date
      type: date
      required: true
    - name: source
      type: string
    - name: age
      type: int

quality:
  row_rules:
    - name: email_format
      sql: "email LIKE '%@%'"
      category: correctness
    - name: age_positive
      sql: "age IS NULL OR age >= 0"
      category: correctness

  dataset_rules:
    - name: total_signups
      sql: "SELECT COUNT(*) FROM bronze_web_signups"
      must_be_greater_than: 0
```

## The Trade-off

| Approach | Pros | Cons |
|----------|------|------|
| **No Bronze rules** | Capture everything | Bad data pollutes Bronze |
| **Bronze quality gate** | Clean Bronze layer | May lose salvageable data |

**Recommendation**: Use Bronze quality gates for obvious garbage (malformed emails, negative IDs). Save complex business rules for Silver.

## Run It

```bash
cd examples/03_patterns/bronze_quality_gate
python -c "
from lakelogic import DataProcessor
proc = DataProcessor(contract='contract.yaml')
good, bad = proc.run('data/raw_signups.csv')
print(f'Passed: {len(good)}, Rejected: {len(bad)}')
"
```
