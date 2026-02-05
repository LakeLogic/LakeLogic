# Playground (Quickstart)

The playground is the fastest way to see LakeGuard working end-to-end with a real contract and real data.

For more scenarios, see the Playbooks section in the docs.

## What's Included

- `examples/quickstart/contract.yaml` - full contract (schema, transformations, rules)
- `examples/quickstart/data/customers.csv` - raw input data
- `examples/quickstart/data/dim_geography.csv` - lookup reference table
- `examples/quickstart/data/marketing_opt_outs.csv` - compliance reference table
- `examples/quickstart/run.py` - runnable demo script

## Run It

```bash
cd examples/quickstart
python run.py
```

You'll get:
- `good_customers.csv` - clean, transformed records
- `bad_customers.csv` - quarantined records with error reasons

## What To Look For

- Row-level rules are enforced (email format, membership tier, opt-outs).
- Lookups and derived fields are applied (`country_name`, `full_name`).
- Bad records are isolated without crashing the run.

> Notifications are disabled in the playground contract by default. Enable and configure them in `examples/quickstart/contract.yaml` if you want live alerts.
