# External Python Logic

Extend LakeGuard with custom Python functions or Jupyter notebooks.

## When to Use

- Complex business logic that can't be expressed in SQL
- ML model scoring
- API calls during transformation
- Reusable Python libraries

## Files

```
external_python_logic/
├── contract_python.yaml      # Uses Python function
├── contract_notebook.yaml    # Uses Jupyter notebook
├── data/
│   └── sales.csv
├── gold/
│   └── build_sales_gold.py   # Custom logic
├── run_python.py
└── run_notebook.py
```

## Option 1: Python Function

### Contract
```yaml
external_logic:
  type: python
  path: ./gold/build_sales_gold.py
  entrypoint: build_sales_gold
```

### Python File (gold/build_sales_gold.py)
```python
import polars as pl

def build_sales_gold(df: pl.DataFrame) -> pl.DataFrame:
    """Custom business logic for Gold layer."""
    return (
        df
        .with_columns([
            (pl.col("amount") * 1.1).alias("amount_with_tax"),
            pl.col("sale_date").dt.month().alias("sale_month"),
        ])
        .filter(pl.col("amount") > 100)
    )
```

## Option 2: Jupyter Notebook

### Contract
```yaml
external_logic:
  type: notebook
  path: ./gold/build_sales_gold.ipynb
```

The notebook receives `df` as input and must output `df` as the result.

## Run It

```python
from lakeguard import DataProcessor

# Python function approach
proc = DataProcessor(contract="contract_python.yaml")
good_df, bad_df = proc.run("data/sales.csv")

# Notebook approach
proc = DataProcessor(contract="contract_notebook.yaml")
good_df, bad_df = proc.run("data/sales.csv")
```

## Best Practices

1. **Keep it simple**: If SQL can do it, use SQL
2. **Type hints**: Use Polars DataFrame type hints
3. **Idempotent**: Function should produce same output for same input
4. **No side effects**: Don't write files or make API calls that can't be retried
5. **Error handling**: Raise exceptions, don't return partial results
