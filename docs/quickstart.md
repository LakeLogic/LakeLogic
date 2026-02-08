# Quickstart

This is the fastest path from install to a successful run.

## 1. Install

```bash
# With all engines (recommended for testing)
pip install "lakeguard[all]"

# Or using uv (faster)
uv pip install "lakeguard[all]"
```

## 2. Run Your First Contract

```bash
cd examples/01_getting_started/basic_validation
lakeguard run --contract contract.yaml --source data/sample_customers.csv
```

You should see output showing:

- Good records that passed validation
- Quarantined records with error reasons

## 3. Try the Interactive Tutorial

Open the Jupyter notebook for a guided walkthrough:

```bash
cd examples/01_getting_started/basic_validation
jupyter notebook tutorial.ipynb
```

## 4. Explore More Examples

The examples are organized by skill level:

```text
examples/
├── 01_getting_started/    # Start here (5 min)
├── 02_tutorials/          # Core concepts (30 min)
├── 03_patterns/           # Real-world recipes
├── 04_features/           # Advanced capabilities
├── 05_production/         # Production templates
└── 06_integrations/       # Orchestrator templates
```

## 5. Try Another Engine

LakeGuard supports multiple engines. Try DuckDB:

```bash
lakeguard run --engine duckdb \
  --contract examples/01_getting_started/basic_validation/contract.yaml \
  --source examples/01_getting_started/basic_validation/data/sample_customers.csv
```

## 6. Bootstrap a Contract

Generate a contract from existing data:

```bash
lakeguard bootstrap \
  --landing examples/05_production/insurance_elt/data/bronze \
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
