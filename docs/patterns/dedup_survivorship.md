# Dedup & Survivorship

Handle duplicate records by keeping the "winner" based on survivorship rules.

## When to Use

- Multiple records per customer ID (CDC streams, batch updates)
- Need to pick the most recent record
- Merge records from multiple sources

## Files

```
examples/03_patterns/dedup_survivorship/
├── contract.yaml
├── README.md
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

## Contract

```yaml
dataset: silver_crm_customer_updates

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

quality:
  row_rules:
    - name: email_format
      sql: "email LIKE '%@%'"
      category: correctness
    - name: valid_status
      sql: "status IN ('active','inactive')"
      category: consistency
```

## Survivorship Strategies

| Strategy | ORDER BY | Use When |
|----------|----------|----------|
| Most recent | `updated_at DESC` | CDC streams |
| First seen | `created_at ASC` | Immutable records |
| Highest value | `score DESC` | Best match wins |
| Custom logic | Complex SQL | Business rules |

## Run It

```bash
cd examples/03_patterns/dedup_survivorship
python -c "
from lakeguard import DataProcessor
proc = DataProcessor(contract='contract.yaml')
good, bad = proc.run('data/customer_updates.csv')
print(f'Unique customers: {len(good)}')
"
```
