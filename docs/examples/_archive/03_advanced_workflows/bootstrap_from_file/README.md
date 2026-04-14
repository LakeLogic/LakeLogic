# Bootstrap: Contracts from Existing Files

Generate production-ready LakeLogic contract YAML files automatically from any
CSV, Parquet, JSON, or Excel data file — no manual schema authoring required.

## Business Scenario

Your data engineering team inherits **three years of historical event data** from a
third-party provider — 40+ columns, no documentation, no schema definition.
You need to onboard these files into your Bronze layer within a sprint.

Writing contracts by hand means:
- Reading column headers, guessing types, documenting meaning
- No quality rules — so unknown distributions slip through undetected
- Hours of tedious YAML authoring before a single row can be processed

`infer_contract()` reads the file, infers every column's type, flags low-cardinality
accepted values, detects required fields, and suggests numeric range bounds — generating
a complete contract draft in one line.

## Business Value

| Without Bootstrap | With Bootstrap |
|---|---|
| Hours of manual YAML authoring | Contract inferred in seconds |
| Types guessed by hand, often wrong | Polars-native type inference — exact |
| No quality rules on day one | `suggest_rules=True` auto-detects constraints |
| PII discovered late (compliance risk) | `detect_pii=True` flags columns at ingest |
| One-off scripts per file format | Unified API: CSV, Parquet, JSON, NDJSON, Excel |
| Contract disconnected from data | Contract can chain directly into DataGenerator |

## Files

| File | Description |
|---|---|
| `bootstrap_from_file.ipynb` | End-to-end walkthrough notebook |
| `data/` | Sample landing zone files (generated at runtime) |
| `contracts/` | Output contracts (written by the notebook) |

## Quick Start

```python
from lakelogic import infer_contract

# Infer schema + quality rules from a CSV
draft = infer_contract("data/events.csv", suggest_rules=True)

# Inspect the contract YAML
draft.show()

# Save to disk — now usable by DataProcessor, DataGenerator, CLI, registry
draft.save("contracts/bronze_events.yaml")

# Or skip the file and chain straight into a generator
df = infer_contract("data/events.csv").to_generator(seed=42).generate(rows=5_000)
```

## What the Bootstrap Infers

| Data Pattern | Generated Contract Element |
|---|---|
| Column with < 1% nulls | `required: true` |
| String column with ≤ 20 distinct values | `accepted_values` quality rule |
| Numeric column | `range` rule with observed min/max |
| Column unique across all rows | `unique` dataset rule |
| Nested column names (`pricing__price`) | Preserved as-is (Bronze flattening) |
| PII values (`--detect-pii`) | `pii: true`, `classification: email` etc. |

## CLI (Bootstrap a Full Landing Zone)

```bash
# Scan a folder, generate one contract per entity, write a registry
lakelogic bootstrap \
  --landing data/landing \
  --output-dir contracts/bronze \
  --registry contracts/bronze/_registry.yaml \
  --format json \
  --suggest-rules

# Sync mode: add new entities without overwriting existing contracts
lakelogic bootstrap \
  --landing data/landing \
  --output-dir contracts/bronze \
  --registry contracts/bronze/_registry.yaml \
  --sync
```
