# API Reference

## DataProcessor

```python
from lakeguard import DataProcessor

processor = DataProcessor(
    engine="polars",
    contract="contract.yaml"
)

good_df, bad_df = processor.run(df)
```

### Constructor

- `engine` (str): `polars`, `pandas`, `duckdb`, or `spark`
- `contract` (str | Path | dict | DataContract): YAML path or dict

### run(df)

Executes the contract against a DataFrame.

Returns:
- `good_df`: validated, transformed records
- `bad_df`: quarantined records with error reasons

## DataContract

Use a YAML file or a dict. Key sections:

- `model.fields`: schema and types
- `schema_policy`: behavior for unknown fields
- `quality.row_rules`: row‑level checks
- `quality.dataset_rules`: aggregate checks
- `transformations`: rename, derive, lookup
- `quarantine`: quarantine + notifications
