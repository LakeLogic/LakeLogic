# Job Templates

This folder contains runnable templates for orchestrating LakeGuard pipelines using the registry-driven driver.
Each template calls `lakeguard-driver` with the registry paths for the target environment.
You can add `--summary-path`, `--summary-table`, or `--metrics-path` for observability outputs.
For Prometheus scraping, run with `--metrics-backend prometheus`.

Files:
- airflow_dag.py
- prefect_flow.py
- dagster_job.py
- databricks_job.json
- synapse_pipeline.json
- fabric_pipeline.json
- adf_pipeline.json
- aws_glue_job.py
- aws_step_functions.json
