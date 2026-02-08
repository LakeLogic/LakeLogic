# External Python Logic

Extend LakeGuard with custom Python functions or Jupyter notebooks.

## When to Use

- Complex business logic that can't be expressed in SQL
- ML model scoring
- API calls during transformation
- Reusable Python libraries

## Files

```
examples/03_patterns/external_python_logic/
├── contract_python.yaml
├── contract_notebook.yaml
├── README.md
├── data/
│   └── sales.csv
├── gold/
│   └── build_sales_gold.py
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

## Full Contract Example

```yaml
version: 1.0.0
info:
  title: Sales Gold (Python External Logic)

dataset: silver_pos_sales

model:
  fields:
    - name: sale_id
      type: int
      required: true
    - name: sale_date
      type: date
    - name: amount
      type: double
    - name: salesperson_id
      type: int

quality:
  row_rules:
    - name: positive_amount
      sql: "amount > 0"
      category: correctness

external_logic:
  type: python
  path: ./gold/build_sales_gold.py
  entrypoint: build_sales_gold

materialization:
  strategy: overwrite
  target_path: output/gold_fact_sales
  format: csv
```

## Run It

```bash
cd examples/03_patterns/external_python_logic

# Python function approach
python run_python.py

# Notebook approach
python run_notebook.py
```

## Best Practices

1. **Keep it simple**: If SQL can do it, use SQL
2. **Type hints**: Use Polars DataFrame type hints
3. **Idempotent**: Function should produce same output for same input
4. **No side effects**: Don't write files or make API calls that can't be retried
5. **Error handling**: Raise exceptions, don't return partial results
