# Integrations

Connect LakeGuard to your orchestration platform.

## Examples

### [job_templates/](job_templates/)

Ready-to-use templates for:

| Platform | File |
|----------|------|
| **Apache Airflow** | `airflow_dag.py` |
| **Prefect** | `prefect_flow.py` |
| **Dagster** | `dagster_job.py` |
| **Databricks Jobs** | `databricks_job.json` |
| **Azure Data Factory** | `adf_pipeline.json` |
| **Azure Synapse** | `synapse_pipeline.json` |
| **Microsoft Fabric** | `fabric_pipeline.json` |
| **AWS Glue** | `aws_glue_job.py` |
| **AWS Step Functions** | `aws_step_functions.json` |

---

## Usage Pattern

All templates follow the same pattern:

```bash
# Run the LakeGuard driver with your registry
lakeguard-driver \
  --registry contracts/_registry.yaml \
  --layers bronze,silver \
  --summary-path output/run_summary.json
```

## Prerequisites

1. Complete the [insurance_elt](../05_production/insurance_elt/) example to understand registries
2. Have LakeGuard installed in your orchestration environment
3. Configure your registry and contracts

## Next Steps

See each template's inline documentation for platform-specific setup.
