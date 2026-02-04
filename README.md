# LakeGuard

**The Quality Gate for your Data Lakehouse.**

LakeGuard is a SQL-first, declarative data contract runtime that keeps your data clean, validated, and enriched as it moves through Bronze, Silver, and Gold layers.

[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://your-username.github.io/lakeguard)
[![License](https://img.shields.io/badge/license-Apache%202.0-green)](LICENSE)

---

## Why LakeGuard?

In a Data Lakehouse, raw data (Bronze) is often messy. Moving it to Silver and Gold usually requires writing complex, custom code that is hard to maintain and inconsistent across different engines (Spark vs. Polars).

LakeGuard solves this by providing **One Contract** that runs on **Any Engine**:

1. **Define Once**: Use YAML to declare your schema, quality rules, and business logic.
2. **Execute Anywhere**: Run the same contract on Spark, Polars, DuckDB, or Pandas.
3. **Governance Built-in**: Automatically isolate bad data into **Quarantine** with detailed failure reasons.

## Key Features

- **SQL-First Logic**: Use the SQL expressions you already know for transformations and quality rules.
- **Schema Enforcement**: Type casting, required fields, and unknown-field handling.
- **Intelligent Quarantine**: Records that fail rules are detoured, tagged with error messages, and saved for correction.
- **Referential Integrity**: Validate keys against dimensions using local reference tables.
- **Notifications (Demo)**: Built-in adapters log alerts for quarantine and rule failures.

## Installation

```bash
# Get the full engine suite
uv pip install "lakeguard[all]"

# Or just use Polars for local speed
uv pip install "lakeguard[polars]"
```

See the full installation guide in `docs/installation.md`.

## Quick Start

```python
import polars as pl
from lakeguard import DataProcessor

# 1. Load your Bronze (Raw) data
df = pl.read_csv("bronze_customers.csv")

# 2. Run the Quality Gate
processor = DataProcessor(engine="polars", contract="silver_customers.yaml")
good_df, bad_df = processor.run(df)

# good_df -> Ready for Silver Layer
# bad_df  -> Sent to Quarantine
```

## Playground

Try the runnable end-to-end demo in `docs/playground.md`.

## Documentation

Visit our documentation for:
- Concepts: `docs/concepts.md`
- Reprocessing: `docs/reprocessing.md`
- Playground: `docs/playground.md`
- CLI: `docs/cli.md`

## Contributing

See `docs/installation.md#developer-installation` to get started.

---
*License: Apache-2.0*
