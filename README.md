# LakeGuard 🛡️

**The Quality Gate for your Data Lakehouse.** 

LakeGuard is a SQL-First, declarative framework that ensures your data is clean, validated, and enriched as it moves through **Bronze, Silver, and Gold** layers.

[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://your-username.github.io/lakeguard)
[![License](https://img.shields.io/badge/license-Apache%202.0-green)](LICENSE)

---

## 🚀 Why LakeGuard?

In a Data Lakehouse, raw data (Bronze) is often messy. Moving it to Silver and Gold usually requires writing complex, custom code that is hard to maintain and inconsistent across different engines (Spark vs. Polars).

LakeGuard solves this by providing **One Contract** that runs on **Any Engine**:

1.  **Define Once**: Use ODCS-aligned YAML to declare your schema, quality rules, and business logic.
2.  **Execute Anywhere**: Run the same contract on Spark, Polars, DuckDB, or Pandas.
3.  **Governance Built-in**: Automatically isolate bad data into **Quarantine** with detailed failure reasons.

## ✨ Key Features

- 🏗️ **Lakehouse Strategies**: Built-in support for SCD Type 1 (Merge), SCD Type 2 (History), and Fact table materialization.
- 🧪 **SQL-First Logic**: Use the SQL expressions you already know for transformations and quality rules.
- 🛑 **Intelligent Quarantine**: Records that fail rules are detoured, tagged with error messages, and saved for correction.
- 🛡️ **Referential Integrity**: Validate keys against dimensions (e.g., ensure `product_id` exists) with support for "Unknown" mapping.
- ⚡ **UV Powered**: Optimized for the fastest Python tooling and standards.

## 📦 Installation

```bash
# Get the full engine suite
uv pip install "lakeguard[all]"

# Or just use Polars for local speed
uv pip install "lakeguard[polars]"
```
See the [Full Installation Guide](docs/installation.md) for more options.

## 🏁 Quick Start

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

## 📚 Documentation
Visit our [Interactive Guide](https://your-username.github.io/lakeguard) for:
- [The Medallion Architecture Guide](docs/concepts.md)
- [Handling Late Arriving Data](docs/reprocessing.md)
- [Example: Sales Fact Populations](docs/examples/sales_gold.md)
- [CLI Reference](api.md)

## 🤝 Contributing
We love contributors! Take a look at our [Developer Guide](docs/installation.md#developer-installation) to get started with `uv`.

---
*License: Apache-2.0*
