# LakeLogic Driver

The registry-driven driver runs Bronze -> Silver -> Gold pipelines with a single CLI.
It is designed for production orchestration: parallel execution, incremental windows, reprocessing, and observability outputs.

## Why It Matters

- **Standardization**: One driver for all domains and systems, with consistent behavior.
- **Operational Safety**: Enforces upstream freshness and captures failures without silent partial loads.
- **Cost Control**: Run local engines for smaller loads and Spark only where needed.
- **Observability**: Per-run summaries and metrics are produced for dashboards and alerting.

!!! tip "Business Value"
    - Reduce pipeline incident rates by enforcing upstream freshness and safe reprocessing.
    - Cut compute costs by avoiding unnecessary Spark runs for smaller workloads.
    - Improve auditability with per-run summaries, metrics, and run log tables.
    - Shorten recovery time by reprocessing only affected windows instead of full reloads.

!!! info "Business Viability (High)"
    **Estimated viability:** 7.5-8.5 / 10  
    **Why it works:** Clear pain point (bad data in medallion pipelines), strong differentiation (engine-agnostic + registry-driven orchestration), and production-ready observability.  
    **Risks:** Crowded data quality market, buyers expect tight orchestration and warehouse integrations.  
    **Best-fit buyers:** Lakehouse teams with Bronze/Silver/Gold workflows, cost-sensitive platforms, governance-heavy orgs.

## Core Concepts

- **Registries** define which contracts run per layer and whether they are enabled.
- **Contracts** define the source and load mode (full/incremental/cdc).
- **Upstream** dependencies let the driver gate downstream runs when freshness is not met.

## Basic Usage

```bash
lakelogic-driver \
  --registry examples/insurance_elt/contracts/insurance/_registry.yaml \
  --reference-registry examples/insurance_elt/contracts/shared/reference/_registry.yaml \
  --gold-registry examples/insurance_elt/contracts/insurance/warehouse/_registry.yaml \
  --layers reference,bronze,silver,gold \
  --window last_success
```
This runs the full Insurance ELT pipeline using registry-defined contracts and uses run-log state for incremental execution.

## Run Only One Entity

```bash
lakelogic-driver \
  --registry examples/insurance_elt/contracts/insurance/_registry.yaml \
  --reference-registry examples/insurance_elt/contracts/shared/reference/_registry.yaml \
  --gold-registry examples/insurance_elt/contracts/insurance/warehouse/_registry.yaml \
  --layers bronze,silver,gold \
  --entities policies
```
This limits execution to the `policies` entity without changing registry files.

## Run-Level Overrides

Use `--set` to override contract fields at runtime:

```bash
lakelogic-driver \
  --registry examples/insurance_elt/contracts/insurance/_registry.yaml \
  --layers bronze \
  --set source.path=examples/insurance_elt/data/bronze \
  --set source.pattern="claims_cdc_2026-02-06.csv"
```
This overrides the source path and file pattern for a one-off run, without editing YAML.

**Use case**: Run a one-off hotfix for a single file without editing any contract YAML.

## Incremental Window (Range)

```bash
lakelogic-driver \
  --registry examples/insurance_elt/contracts/insurance/_registry.yaml \
  --reference-registry examples/insurance_elt/contracts/shared/reference/_registry.yaml \
  --gold-registry examples/insurance_elt/contracts/insurance/warehouse/_registry.yaml \
  --layers bronze,silver,gold \
  --window range \
  --window-start-date 2026-02-01 \
  --window-end-date 2026-02-05
```
This processes only data in the requested date range (end date inclusive).

**Use case**: Rebuild a known bad period after a source-system outage (e.g., Feb 1-5).

## Backfill Planner

Generate daily or weekly windows and run them sequentially:

```bash
lakelogic-driver \
  --registry examples/insurance_elt/contracts/insurance/_registry.yaml \
  --layers bronze,silver \
  --backfill-start-date 2026-02-01 \
  --backfill-end-date 2026-02-07 \
  --backfill-granularity day
```
This generates daily windows and executes them sequentially for controlled backfills.

**Use case**: Backfill a newly onboarded system for the last 90 days without manual scripting.

## Reprocess (Late Arriving Data)

```bash
lakelogic-driver \
  --registry examples/insurance_elt/contracts/insurance/_registry.yaml \
  --reference-registry examples/insurance_elt/contracts/shared/reference/_registry.yaml \
  --gold-registry examples/insurance_elt/contracts/insurance/warehouse/_registry.yaml \
  --layers silver,gold \
  --reprocess-start-date 2026-02-01 \
  --reprocess-end-date 2026-02-05
```
This reprocesses the selected date window and overwrites affected partitions.

**Use case**: Late arriving claims for a specific week after a vendor delay.

## Partial Resume

Persist state so a failed run can resume without re-running successful contracts:

```bash
lakelogic-driver \
  --registry examples/insurance_elt/contracts/insurance/_registry.yaml \
  --layers bronze,silver,gold \
  --state-path logs/driver_state.json \
  --resume
```
This skips already completed contracts and resumes from the last failed step.

**Use case**: Bronze and Silver succeeded, Gold failed. Resume only Gold after fixing a rule.

## Observability Outputs

Write a per-run summary row into a table and emit metrics:

```bash
lakelogic-driver \
  --registry examples/insurance_elt/contracts/insurance/_registry.yaml \
  --reference-registry examples/insurance_elt/contracts/shared/reference/_registry.yaml \
  --gold-registry examples/insurance_elt/contracts/insurance/warehouse/_registry.yaml \
  --layers reference,bronze,silver,gold \
  --window last_success \
  --summary-table lakelogic.pipeline_runs \
  --summary-backend duckdb \
  --summary-database examples/insurance_elt/output/run_logs/lakelogic_pipeline_runs.duckdb \
  --metrics-path examples/insurance_elt/output/run_logs/pipeline_metrics.json
```
This writes pipeline summaries to DuckDB and emits a metrics JSON file for monitoring.

**Use case**: Feed pipeline summaries into a governance dashboard and alert on failed runs.

## Policy Packs

Apply standardized rules and defaults across layers:

```bash
lakelogic-driver \
  --registry examples/insurance_elt/contracts/insurance/_registry.yaml \
  --layers silver \
  --policy-pack baseline_silver \
  --policy-pack-dir policy_packs
```
This applies a standardized Silver policy pack for consistent rules and SLOs.

You can also set `metadata.policy_pack` inside a contract.

**Use case**: Enforce corporate quality baselines across all Silver datasets with one flag.

Policy packs can also include shared `transformations` (with `transformations_mode` and
`{stage}_transformations`), which are merged into each contract.

## Approval Gates

Require explicit approval when schema drift or quarantine ratio breaches:

```bash
lakelogic-driver \
  --registry examples/insurance_elt/contracts/insurance/_registry.yaml \
  --layers gold \
  --approval-required \
  --approval-file approvals/2026-02-06.ok
```
This enforces approval gates before publishing Gold outputs when thresholds are breached.

**Use case**: Require sign-off before publishing Gold data when quarantine ratio spikes.

## Lifecycle Window Rule

Validate that event timestamps fall within a customer lifecycle:

```yaml
quality:
  row_rules:
    - lifecycle_window:
        event_ts: event_ts
        event_key: customer_id
        reference: customers
        reference_key: customer_id
        start_field: start_date
        end_field: end_date
        end_default: "9999-12-31"
```
This catches events that occur before a customer starts or after they end.

## Upstream Freshness Policy

Allow non-critical upstreams to be stale within a grace window:

```yaml
metadata:
  upstream_policy: warn
  upstream_grace_hours: 6
```
This allows upstreams to be stale within a grace window while continuing execution.

**Use case**: Allow non-critical reference data to be up to 6 hours stale while still running facts.

## Reference Cache

Cache lookup datasets across contracts to reduce re-reads:

```bash
lakelogic-driver \
  --registry examples/insurance_elt/contracts/insurance/_registry.yaml \
  --layers silver \
  --cache-references
```
This caches reference datasets in memory to avoid repeated reads during the run.

**Use case**: Reuse large reference tables across multiple Silver contracts in a single run.

## Bootstrap Contracts

Generate starter contracts and a registry from a landing zone:

```bash
lakelogic bootstrap \
  --landing examples/insurance_elt/data/bronze \
  --output-dir examples/insurance_elt/bootstrap_contracts \
  --registry examples/insurance_elt/bootstrap_contracts/_registry.yaml \
  --format csv \
  --pattern "*.csv"
```
This scans the landing zone, infers schema, and generates starter contracts plus a registry.

**Use case**: Onboard a brand-new system when only landing files exist.

Prometheus scraping:

```bash
lakelogic-driver \
  --registry examples/insurance_elt/contracts/insurance/_registry.yaml \
  --reference-registry examples/insurance_elt/contracts/shared/reference/_registry.yaml \
  --gold-registry examples/insurance_elt/contracts/insurance/warehouse/_registry.yaml \
  --metrics-backend prometheus \
  --metrics-host 0.0.0.0 \
  --metrics-port 9100
```
This starts a Prometheus `/metrics` endpoint for scraping.

## Orchestrator Templates

See the job templates for Airflow, Prefect, Dagster, Databricks, Synapse, Fabric, ADF, and AWS:

- `docs/job_templates.md`

## How It Decides Incremental vs Full

When `--window last_success` is used:

- If a run log table exists and a prior run is found, the driver runs incremental from that timestamp.
- If the log table is missing or empty, it falls back to a full load and records the reason.

This keeps pipelines safe and predictable in production.
