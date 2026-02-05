# Quickstart

This is the fastest path from install to a successful run using the built-in example.

## 1. Install

```bash
# With all engines
uv pip install "lakeguard[all]"
# or
pip install "lakeguard[all]"
```

## 2. Run the Example

```bash
cd examples/quickstart
python run.py
```

You should see two outputs in `examples/quickstart`:

- `good_customers.csv`
- `bad_customers.csv`

The sample data intentionally includes invalid rows so you can see quarantine in action.

## 3. Try Another Engine

```bash
lakeguard run --engine duckdb --contract examples/quickstart/contract.yaml --source examples/quickstart/data/customers.csv
```

## 4. Next Steps

- Learn how contracts work in [How It Works](concepts.md)
- Review an end-to-end playbook in [Playbooks Overview](playbooks.md)
- See a production-ready ingestion pattern in [Ingestion (Bronze)](examples/ingestion.md)
- Use table adapters in [Warehouse Adapters](warehouse_adapters.md)
