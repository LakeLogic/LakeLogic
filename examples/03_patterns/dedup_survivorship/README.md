# Dedup & Survivorship

Handle duplicate records by keeping the "winner" based on survivorship rules.

## When to Use

- Multiple records per customer ID (CDC streams, batch updates)
- Need to pick the most recent record
- Merge records from multiple sources

## Files

```
dedup_survivorship/
├── contract.yaml
└── data/
    └── customer_updates.csv
```

## The Pattern

```sql
-- Keep most recent record per customer_id
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY customer_id
    ORDER BY updated_at DESC
  ) AS rn
  FROM source
) AS t
WHERE rn = 1
```

## Contract Breakdown

```yaml
transformations:
  # Dedup in pre-phase (before quality rules)
  - sql: |
      SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (
          PARTITION BY customer_id
          ORDER BY updated_at DESC
        ) AS rn
        FROM source
      ) AS t
      WHERE rn = 1
    phase: pre

  # Derive columns in post-phase
  - sql: |
      SELECT *, status = 'active' AS is_active
      FROM source
    phase: post
```

## Survivorship Strategies

| Strategy | ORDER BY | Use When |
|----------|----------|----------|
| Most recent | `updated_at DESC` | CDC streams |
| First seen | `created_at ASC` | Immutable records |
| Highest value | `score DESC` | Best match wins |
| Custom logic | Complex SQL | Business rules |

## Run It

```python
from lakelogic import DataProcessor

proc = DataProcessor(contract="contract.yaml")
good_df, bad_df = proc.run("data/customer_updates.csv")

# Duplicates removed, only winners remain
print(f"Unique customers: {len(good_df)}")
```
