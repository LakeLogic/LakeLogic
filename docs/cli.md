# CLI Reference

The LakeLogic CLI is the high-efficiency entry point for enforcing your data contracts. It is designed for **Speed-to-Production** and **Engine Portability**.

## Strategic Value

- **Developer Productivity:** Bootstrap production-ready contracts from raw data in seconds.
- **Infrastructure Optionality:** Use the `--engine` flag to swap between Polars (local speed) and Spark (cluster scale) with zero code changes.
- **Audit Readiness:** Every execution generates a run summary for instant reconciliation.

## Commands

### 🟢 `lakelogic run`

Validates a source dataset against a contract.

```bash
lakelogic run --contract contract.yaml --source data.csv
```

### CLI Help

```bash
lakelogic help
lakelogic help bootstrap
```

This prints short usage guidance and examples directly in the terminal.

Python helper:

```bash
python -c "import lakelogic; lakelogic.help()"
python -c "import lakelogic; lakelogic.driver.help()"
python -c "import lakelogic; lakelogic.bootstrap.help()"
python -c "import lakelogic; lakelogic.policy_packs.help()"
python -c "import lakelogic; lakelogic.observability.help()"
```

For warehouse engines, pass a table name:

```bash
lakelogic run --engine snowflake --contract contract.yaml --source table:ANALYTICS.SILVER.CUSTOMERS
```

## Options

- `--contract, -c`: Path to the YAML contract
- `--source, -s`: Input file (CSV or Parquet; Delta/Iceberg with Spark + `server.format`) or a table name for Snowflake/BigQuery engines
- `--engine, -e`: (Optional) Force an engine (`polars`, `pandas`, `duckdb`, `spark`, `snowflake`, `bigquery`). If omitted, LakeLogic discovery is used.
- `--stage`: Apply contract stage overrides (e.g., `bronze`, `silver`) from a top-level `stages` block.
- `--output-good`: Save good records to CSV/Parquet (or write to Delta/Iceberg via `--materialize`)
- `--output-bad`: Save quarantined records to CSV/Parquet
- `--output-format`: `csv` or `parquet` (defaults to CSV or inferred from file extension)
- `--materialize`: Write good data to the contract materialization target
- `--materialize-target`: Override the materialization target path
- `--verbose, -v`: Enable debug logs

> Note: When using the Spark engine, `--output-good/--output-bad` are written with the Spark writer and may create a directory with part files (standard Spark behavior).

## Example (Auto-Engine)

```bash
lakelogic run \
  --contract examples/customer_onboarding/contract.yaml \
  --source examples/customer_onboarding/data/customers.csv \
  --output-good good.csv \
  --output-bad bad.csv \
  --materialize
```

## Pipeline Driver

The registry-driven driver is exposed as `lakelogic-driver` and orchestrates bronze/silver/gold layers.

```bash
lakelogic-driver \
  --registry examples/insurance_elt/contracts/insurance/_registry.yaml \
  --reference-registry examples/insurance_elt/contracts/shared/reference/_registry.yaml \
  --gold-registry examples/insurance_elt/contracts/insurance/warehouse/_registry.yaml \
  --layers reference,bronze,silver,gold \
  --window last_success
```

### 🛠️ `lakelogic bootstrap`

Generate starter contracts and a registry from a landing zone. This is the **Governance Accelerator** for Day 1 compliance.

Generate starter contracts and a registry from a landing zone:

```bash
lakelogic bootstrap \
  --landing examples/insurance_elt/data/bronze \
  --output-dir examples/insurance_elt/bootstrap_contracts \
  --registry examples/insurance_elt/bootstrap_contracts/_registry.yaml \
  --format csv \
  --pattern "*.csv"
```

Use `--sync` to align an existing registry with new landing data:

```bash
lakelogic bootstrap \
  --landing examples/insurance_elt/data/bronze \
  --output-dir examples/insurance_elt/bootstrap_contracts \
  --registry examples/insurance_elt/bootstrap_contracts/_registry.yaml \
  --format csv \
  --pattern "*.csv" \
  --sync
```

## Driver Options (Highlights)

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
- `--metrics-prefix`: StatsD metric prefix (default `lakelogic`).
- `--metrics-tags`: Comma-separated tags, e.g. `env=prod,team=data`.
- `--set`: Override contract fields at runtime (repeatable).
- `--policy-pack`: Apply a policy pack by name.
- `--policy-pack-dir`: Directory containing policy packs.
- `--state-path`: State file for partial resume.
- `--resume`: Resume from last successful state.
- `--retries`: Retry count for transient failures.
- `--retry-backoff`: Initial retry backoff in seconds.
- `--retry-max-delay`: Max retry delay in seconds.
- `--approval-required`: Require approvals on drift/quarantine thresholds.
- `--approval-file`: Approval file path to bypass approval gates.
- `--cache-references`: Cache reference datasets across runs.
- `--backfill-start-date`: Backfill start date (YYYY-MM-DD).
- `--backfill-end-date`: Backfill end date (YYYY-MM-DD).
- `--backfill-granularity`: Backfill granularity (`day` or `week`).
- `--continue-on-error`: Keep running other contracts even if one fails.
- `--window range --window-start-date YYYY-MM-DD --window-end-date YYYY-MM-DD`: Explicit window.
- `--reprocess-date` or `--reprocess-start-date/--reprocess-end-date`: Late-arriving data replay.
- `--entities`: Run only specific entities without editing the registry.
- `--contracts`: Run specific contract paths directly.
