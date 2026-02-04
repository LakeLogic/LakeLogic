# CLI Reference

LakeGuard ships with a simple CLI for validating files with a contract.

## Basic Usage

```bash
lakeguard run --contract contract.yaml --source data.csv --engine polars
```

## Options

- `--contract, -c`: Path to the YAML contract
- `--source, -s`: Input file (CSV or Parquet)
- `--engine, -e`: Engine (`polars`, `pandas`, `duckdb`, `spark`)
- `--output-good`: Save good records to CSV
- `--output-bad`: Save quarantined records to CSV
- `--verbose, -v`: Enable debug logs

## Example

```bash
lakeguard run \
  --contract examples/customer_onboarding/contract.yaml \
  --source examples/customer_onboarding/customers.csv \
  --engine polars \
  --output-good good.csv \
  --output-bad bad.csv
```
