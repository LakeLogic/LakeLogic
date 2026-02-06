# CLI Reference

LakeGuard ships with a simple CLI for validating files with a contract.

## Basic Usage

```bash
lakeguard run --contract contract.yaml --source data.csv
```

For warehouse engines, pass a table name:

```bash
lakeguard run --engine snowflake --contract contract.yaml --source table:ANALYTICS.SILVER.CUSTOMERS
```

## Options

- `--contract, -c`: Path to the YAML contract
- `--source, -s`: Input file (CSV or Parquet; Delta/Iceberg with Spark + `server.format`) or a table name for Snowflake/BigQuery engines
- `--engine, -e`: (Optional) Force an engine (`polars`, `pandas`, `duckdb`, `spark`, `snowflake`, `bigquery`). If omitted, LakeGuard discovery is used.
- `--output-good`: Save good records to CSV/Parquet (or write to Delta/Iceberg via `--materialize`)
- `--output-bad`: Save quarantined records to CSV/Parquet
- `--output-format`: `csv` or `parquet` (defaults to CSV or inferred from file extension)
- `--materialize`: Write good data to the contract materialization target
- `--materialize-target`: Override the materialization target path
- `--verbose, -v`: Enable debug logs

> Note: When using the Spark engine, `--output-good/--output-bad` are written with the Spark writer and may create a directory with part files (standard Spark behavior).

## Example (Auto-Engine)

```bash
lakeguard run \
  --contract examples/customer_onboarding/contract.yaml \
  --source examples/customer_onboarding/data/customers.csv \
  --output-good good.csv \
  --output-bad bad.csv \
  --materialize
```

## Pipeline Driver

The registry-driven driver is exposed as `lakeguard-driver` and orchestrates bronze/silver/gold layers.

```bash
lakeguard-driver \
  --registry examples/insurance_elt/contracts/insurance/_registry.yaml \
  --reference-registry examples/insurance_elt/contracts/shared/reference/_registry.yaml \
  --gold-registry examples/insurance_elt/contracts/insurance/warehouse/_registry.yaml \
  --layers reference,bronze,silver,gold \
  --window last_success
```

### Driver Options (Highlights)

- `--summary-path`: Write a per-run summary JSON (metrics + per-contract status).
- `--summary-table`: Write a pipeline summary row to a table backend.
- `--summary-backend`: `spark`, `duckdb`, `sqlite`, `snowflake`, or `bigquery` for summary tables (Snowflake/BigQuery use environment credentials).
- `--summary-database`: Database path for `duckdb/sqlite` summary tables.
- `--summary-table-format`: Spark table format (default `delta`).
- `--summary-merge-on-run-id` / `--no-summary-merge-on-run-id`: Control Spark summary upserts.
- `--metrics-path`: Write a metrics JSON payload for monitoring.
- `--metrics-backend`: `statsd` or `prometheus`.
- `--metrics-host`: StatsD host or Prometheus bind host (default `127.0.0.1` for StatsD, `0.0.0.0` for Prometheus).
- `--metrics-port`: StatsD port (default `8125`) or Prometheus port (default `9100`).
- `--metrics-prefix`: StatsD metric prefix (default `lakeguard`).
- `--metrics-tags`: Comma-separated tags, e.g. `env=prod,team=data`.
- `--continue-on-error`: Keep running other contracts even if one fails.
- `--window range --window-start-date YYYY-MM-DD --window-end-date YYYY-MM-DD`: Explicit window.
- `--reprocess-date` or `--reprocess-start-date/--reprocess-end-date`: Late-arriving data replay.
- `--entities`: Run only specific entities without editing the registry.
- `--contracts`: Run specific contract paths directly.
