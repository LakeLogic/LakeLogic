# Quickstart

This is the fastest path from install to a successful run.

## 1. Install

```bash
# With all engines (recommended for testing)
pip install "lakelogic[all]"

# Or using uv (faster)
uv pip install "lakelogic[all]"
```

## 2. Run Your First Contract

```bash
cd examples/01_quickstart
lakelogic run --contract users_contract.yaml --source data/sample_customers.csv
```

You should see output showing:

- Good records that passed validation
- Quarantined records with error reasons

## 3. Try the Interactive Tutorial

Open the Jupyter notebook for a guided walkthrough:

```bash
cd examples/01_quickstart
jupyter notebook 01_hello_world.ipynb
```

## 4. Explore More Examples

The examples are organized by topic:

```text
examples/
├── 01_quickstart/         # Start here (5 min)
├── 02_core_patterns/      # Essential patterns
├── 03_advanced_workflows/  # Complex pipelines
├── 04_compliance_governance/ # Regulatory checks
└── _archive/              # Untested/legacy examples
```

## 5. Try Another Engine

LakeLogic supports multiple engines. Try DuckDB:

```bash
lakelogic run --engine duckdb \
  --contract examples/01_quickstart/users_contract.yaml \
  --source examples/01_quickstart/data/sample_customers.csv
```

## 6. Bootstrap a Contract

Generate a contract from existing data:

```bash
lakelogic bootstrap \
  --landing examples/03_advanced_workflows/insurance_elt/data/bronze \
  --output-dir my_contracts \
  --registry my_contracts/_registry.yaml \
  --format csv \
  --pattern "*.csv"
```

## Next Steps

- [How It Works](concepts.md) - Understand the medallion architecture
- [Patterns](playbooks.md) - Common data engineering recipes
- [CLI Reference](cli.md) - Full command documentation
- [Warehouse Adapters](warehouse_adapters.md) - Snowflake, BigQuery, Spark
