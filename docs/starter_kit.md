# Starter Kit: Bronze -> Silver -> Gold

This starter kit walks through a realistic, end-to-end flow using the built-in examples.

## 1. Bronze Gate (Schema + Quality)

Use the ingestion example to validate raw data and quarantine schema drift.

```bash
cd examples/ingestion
python run.py
```

Outputs:

- `good_crm.csv`
- `bad_crm.csv`

## 2. Silver Transform (Clean + Enrich)

Use the quickstart contract to clean, dedupe, and enrich customer data.

```bash
cd examples/quickstart
python run.py
```

Outputs:

- `good_customers.csv`
- `bad_customers.csv`

## 3. Gold Build (External Logic)

Use the external logic example to run a dedicated Python module for Gold processing.

```bash
cd examples/external_logic
python run_python.py
```

Outputs:

- `output/gold_fact_sales.csv`

## Next Steps

- Learn contract structure in [How It Works](concepts.md)
- See production playbooks in [Playbooks Overview](playbooks.md)
- Explore warehouse execution in [Warehouse Adapters](warehouse_adapters.md)
