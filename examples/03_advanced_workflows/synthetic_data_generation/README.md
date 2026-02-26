# Synthetic Data Generation

Generate contract-aware synthetic data — no real data required.

## Business Scenario

Your new Orders pipeline is fully defined but the upstream source system won't deliver
real data for two weeks. `DataGenerator` lets you:

- **Start immediately** — the contract YAML is the data spec
- **Stress-test quarantine** — inject intentionally invalid rows and verify they're caught
- **Benchmark at scale** — generate 100k+ rows locally to validate pipeline performance

## Files

| File | Description |
|---|---|
| `synthetic_data_generation.ipynb` | End-to-end walkthrough notebook |
| `contract_orders.yaml` | Sample orders contract with types, accepted_values, and range constraints |
| `data/` | Output directory (generated at runtime) |

## Quick Start

```python
from lakelogic import DataGenerator

gen = DataGenerator("contract_orders.yaml", seed=42)

# 1,000 valid rows
df = gen.generate(rows=1_000)

# 500 rows with 15% intentionally breaking quality rules
df_mixed = gen.generate(rows=500, invalid_ratio=0.15)

# Save to disk
gen.save(df, "data/orders_sample.parquet", format="parquet")
```

## CLI

```bash
lakelogic generate --contract contract_orders.yaml --rows 1000 --output data/orders.parquet

# With 15% bad rows to verify quarantine
lakelogic generate --contract contract_orders.yaml --rows 500 \
    --invalid-ratio 0.15 --format csv --output data/orders_mixed.csv
```

## What the Generator Respects

| Contract field | Behaviour |
|---|---|
| `type` | Generates matching Python/Polars type |
| `required: true` | Never null |
| `accepted_values` | Samples from the allowed list |
| `range` (min/max) | Stays within the defined bounds |
| Field names (email, name, phone) | Faker generates realistic values |
| `--invalid-ratio` | Deliberately breaks rules for quarantine testing |
