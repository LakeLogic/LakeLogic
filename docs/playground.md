# Playground

The playground is the fastest way to see LakeGuard working end‑to‑end with a real contract and real data.

## What’s Included

- `playground/contract.yaml` — full contract (schema, transformations, rules)
- `playground/customers.csv` — raw input data
- `playground/dim_geography.csv` — lookup reference table
- `playground/marketing_opt_outs.csv` — compliance reference table
- `playground/run_demo.py` — runnable demo script

## Run It

```bash
cd playground
python run_demo.py
```

You’ll get:
- `good_customers.csv` — clean, transformed records
- `bad_customers.csv` — quarantined records with error reasons

## What To Look For

- Row‑level rules are enforced (email format, membership tier, opt‑outs).
- Lookups and derived fields are applied (`country_name`, `full_name`).
- Bad records are isolated without crashing the run.
