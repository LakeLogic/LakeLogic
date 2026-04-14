# Orchestration Templates

Notebook-driven templates for orchestrating LakeLogic pipelines.

## Available Examples

- airflow/airflow_demo.ipynb
- prefect/prefect_demo.ipynb
- dagster/dagster_demo.ipynb
- databricks_jobs/databricks_demo.ipynb
- azure_data_factory/adf_demo.ipynb
- fabric_pipelines/fabric_pipeline_demo.ipynb
- aws_glue/aws_glue_demo.ipynb
- aws_step_functions/aws_step_functions_demo.ipynb

## Usage Pattern

All templates call the LakeLogic driver with a registry.

```bash
lakelogic-driver \
  --registry contracts/_registry.yaml \
  --layers bronze,silver \
  --summary-path output/run_summary.json
```

## Notes

- Add --metrics-backend prometheus for Prometheus scraping
- Use --summary-table or --summary-path for run summaries
