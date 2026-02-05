# Example: External Gold Logic (Python/Notebook)

This example shows how to offload Gold processing to a dedicated Python script or notebook and reference it from a contract.

## Files

- Data: `examples/external_logic/data/sales.csv`
- Python contract: `examples/external_logic/contract_python.yaml`
- Notebook contract: `examples/external_logic/contract_notebook.yaml`
- Python logic: `examples/external_logic/gold/build_sales_gold.py`
- Notebook logic: `examples/external_logic/gold/sales_gold.ipynb`
- Runners: `examples/external_logic/run_python.py`, `examples/external_logic/run_notebook.py`

## Python External Logic

```yaml
external_logic:
  type: python
  path: ./gold/build_sales_gold.py
  entrypoint: build_sales_gold
```

The function receives the validated dataframe and should return a dataframe.

## Notebook External Logic

```yaml
external_logic:
  type: notebook
  path: ./gold/sales_gold.ipynb
  output_path: output/gold_fact_sales.csv
  output_format: csv
```

The notebook receives `LAKEGUARD_PARAMS` including:
- `lakeguard_input_path`
- `lakeguard_output_path`

> Note: Notebook execution requires `lakeguard[notebook]`.

## Run It

```bash
cd examples/external_logic
python run_python.py
# or
python run_notebook.py
```

Outputs are written to `examples/external_logic/output/`.
