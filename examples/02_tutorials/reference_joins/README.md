# Reference Joins

Enrich your data by joining with lookup/reference tables.

## What You'll Learn

1. **Links** - Declare external reference datasets
2. **SQL Joins** - Enrich data in transformations
3. **Referential Integrity** - Validate foreign keys exist

## Files

```
reference_joins/
├── contract.yaml
├── data/
│   ├── customers.csv           # Main data
│   ├── dim_geography.csv       # Geography lookup
│   └── marketing_opt_outs.csv  # Opt-out list
└── run.py
```

## The Pattern

```
customers.csv ──┬──→ [Join Geography] ──→ [Join Opt-outs] ──→ Enriched Output
                │
dim_geography.csv
marketing_opt_outs.csv
```

## Contract Breakdown

```yaml
# Declare reference datasets
links:
  - name: dim_geography
    path: ./data/dim_geography.csv
    type: csv
  - name: marketing_opt_outs
    path: ./data/marketing_opt_outs.csv
    type: csv

# Use them in transformations
transformations:
  - sql: |
      SELECT
        src.*,
        COALESCE(geo.region, 'Unknown') AS region,
        COALESCE(opt.opted_out, 'false') AS opted_out,
        src.first_name || ' ' || src.last_name AS full_name
      FROM source src
      LEFT JOIN dim_geography geo ON src.country_id = geo.country_id
      LEFT JOIN marketing_opt_outs opt ON src.email = opt.email
    phase: post
```

## Run It

```bash
cd reference_joins
python run.py
```

Or in Python:
```python
from lakelogic import DataProcessor

proc = DataProcessor(contract="contract.yaml")
good_df, bad_df = proc.run("data/customers.csv")

# Check enriched columns
print(good_df.select(["email", "region", "opted_out", "full_name"]))
```

## Use Cases

| Use Case | Reference Table |
|----------|-----------------|
| Geography enrichment | Country/region dimensions |
| Compliance filtering | Opt-out/blocklists |
| Code lookups | Status codes, categories |
| Referential integrity | Validate FK exists |

## Next Steps

- [03_patterns/](../../03_patterns/) - Data engineering recipes
- [bronze_quality_gate](../../03_patterns/bronze_quality_gate/) - Quality at ingestion
