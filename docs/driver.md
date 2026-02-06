# LakeGuard Driver

The registry-driven driver runs Bronze -> Silver -> Gold pipelines with a single CLI.
It is designed for production orchestration: parallel execution, incremental windows, reprocessing, and observability outputs.

## Why It Matters

- **Standardization**: One driver for all domains and systems, with consistent behavior.
- **Operational Safety**: Enforces upstream freshness and captures failures without silent partial loads.
- **Cost Control**: Run local engines for smaller loads and Spark only where needed.
- **Observability**: Per-run summaries and metrics are produced for dashboards and alerting.

## Core Concepts

- **Registries** define which contracts run per layer and whether they are enabled.
- **Contracts** define the source and load mode (full/incremental/cdc).
- **Upstream** dependencies let the driver gate downstream runs when freshness is not met.

## Basic Usage

```bash
lakeguard-driver \
  --registry examples/insurance_elt/contracts/insurance/_registry.yaml \
  --reference-registry examples/insurance_elt/contracts/shared/reference/_registry.yaml \
  --gold-registry examples/insurance_elt/contracts/insurance/warehouse/_registry.yaml \
  --layers reference,bronze,silver,gold \
  --window last_success
```

## Run Only One Entity

```bash
lakeguard-driver \
  --registry examples/insurance_elt/contracts/insurance/_registry.yaml \
  --reference-registry examples/insurance_elt/contracts/shared/reference/_registry.yaml \
  --gold-registry examples/insurance_elt/contracts/insurance/warehouse/_registry.yaml \
  --layers bronze,silver,gold \
  --entities policies
```

## Incremental Window (Range)

```bash
lakeguard-driver \
  --registry examples/insurance_elt/contracts/insurance/_registry.yaml \
  --reference-registry examples/insurance_elt/contracts/shared/reference/_registry.yaml \
  --gold-registry examples/insurance_elt/contracts/insurance/warehouse/_registry.yaml \
  --layers bronze,silver,gold \
  --window range \
  --window-start-date 2026-02-01 \
  --window-end-date 2026-02-05
```

## Reprocess (Late Arriving Data)

```bash
lakeguard-driver \
  --registry examples/insurance_elt/contracts/insurance/_registry.yaml \
  --reference-registry examples/insurance_elt/contracts/shared/reference/_registry.yaml \
  --gold-registry examples/insurance_elt/contracts/insurance/warehouse/_registry.yaml \
  --layers silver,gold \
  --reprocess-start-date 2026-02-01 \
  --reprocess-end-date 2026-02-05
```

## Observability Outputs

Write a per-run summary row into a table and emit metrics:

```bash
lakeguard-driver \
  --registry examples/insurance_elt/contracts/insurance/_registry.yaml \
  --reference-registry examples/insurance_elt/contracts/shared/reference/_registry.yaml \
  --gold-registry examples/insurance_elt/contracts/insurance/warehouse/_registry.yaml \
  --layers reference,bronze,silver,gold \
  --window last_success \
  --summary-table lakeguard.pipeline_runs \
  --summary-backend duckdb \
  --summary-database examples/insurance_elt/output/run_logs/lakeguard_pipeline_runs.duckdb \
  --metrics-path examples/insurance_elt/output/run_logs/pipeline_metrics.json
```

Prometheus scraping:

```bash
lakeguard-driver \
  --registry examples/insurance_elt/contracts/insurance/_registry.yaml \
  --reference-registry examples/insurance_elt/contracts/shared/reference/_registry.yaml \
  --gold-registry examples/insurance_elt/contracts/insurance/warehouse/_registry.yaml \
  --metrics-backend prometheus \
  --metrics-host 0.0.0.0 \
  --metrics-port 9100
```

## Orchestrator Templates

See the job templates for Airflow, Prefect, Dagster, Databricks, Synapse, Fabric, ADF, and AWS:

- `docs/job_templates.md`

## How It Decides Incremental vs Full

When `--window last_success` is used:

- If a run log table exists and a prior run is found, the driver runs incremental from that timestamp.
- If the log table is missing or empty, it falls back to a full load and records the reason.

This keeps pipelines safe and predictable in production.
