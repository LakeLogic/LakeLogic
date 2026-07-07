# Job Templates

These templates show how to call `lakelogic-driver` in major orchestrators.
Update paths, credentials, and schedules to match your environment.

## State & Scheduling Notes

- Use your orchestrator for schedules, retries, and alerting.
- Set `--window last_success` to pick up where the prior run left off (uses run log tables).
- Add `--summary-path` to emit a per-run JSON summary that you can archive or parse into metrics.
- Add `--summary-table` with `--summary-backend` to write run summaries into a table for dashboards (Spark, Snowflake, BigQuery, DuckDB, SQLite).
- Add `--metrics-path` for a lightweight metrics payload or `--metrics-backend statsd` for real-time monitoring.
- Use `--metrics-backend prometheus` to expose a `/metrics` endpoint for scraping.
- Use `--continue-on-error` if you want a best-effort run that reports all failures in one pass.

## Airflow

File: `examples/job_templates/airflow_dag.py`

## Prefect

File: `examples/job_templates/prefect_flow.py`

## Dagster

File: `examples/job_templates/dagster_job.py`

## Databricks Jobs

File: `examples/job_templates/databricks_job.json`

## Azure Synapse Pipelines

File: `examples/job_templates/synapse_pipeline.json`

## Microsoft Fabric Pipelines

File: `examples/job_templates/fabric_pipeline.json`

## Azure Data Factory

File: `examples/job_templates/adf_pipeline.json`

## AWS Glue

File: `examples/job_templates/aws_glue_job.py`

## AWS Step Functions

File: `examples/job_templates/aws_step_functions.json`
