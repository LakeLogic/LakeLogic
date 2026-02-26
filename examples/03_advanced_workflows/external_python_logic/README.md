# External Python Logic

Extend LakeLogic with custom logic using notebooks.

## When to Use

- Complex business logic that cannot be expressed in SQL
- Model scoring and feature engineering
- Multi-step transformations outside the contract

## Files

external_python_logic/
- contract_python.yaml      # Notebook-based external logic (legacy name)
- contract_notebook.yaml    # Notebook-based external logic
- external_python_logic_demo.ipynb
- data/sales.csv
- gold/sales_gold.ipynb

## Contract (Notebook)

```yaml
external_logic:
  type: notebook
  path: ./gold/sales_gold.ipynb
  output_path: output/gold_fact_sales.csv
  output_format: csv
```

The notebook receives df as input and must output df as the result.

## Run It

```python
from lakelogic import DataProcessor

proc = DataProcessor(contract="contract_notebook.yaml")
good_df, bad_df = proc.run("data/sales.csv")
```

## Best Practices

1. Keep logic focused and deterministic
2. Avoid side effects in the notebook
3. Use clear column naming and documentation
4. Validate outputs before materialization
