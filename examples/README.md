# LakeLogic Examples

Hands-on examples organized by learning stage and integration type.

## Structure

- 01_quickstart/ - 5-10 minute wins
- 02_core_patterns/ - essential modeling patterns
- 03_data_sources/ - databases, APIs, streaming, files
- 04_cloud_platforms/ - Databricks, Fabric, Azure Synapse
- 05_orchestration/ - Airflow, Prefect, Dagster, and more
- 06_advanced_workflows/ - real-world scenarios
- 07_production/ - CI/CD, secrets management, monitoring
- 08_compliance_governance/ - HIPAA, PII masking, FSI audit packs

## Quick Start

```bash
# 1. Install LakeLogic
pip install lakelogic

# 2. Run your first example
cd examples/01_quickstart/basic_validation
lakelogic run --contract contract.yaml --source data/sample_customers.csv
```

## Example Structure

Most examples follow this layout:

```
example_name/
- README.md          # What this example teaches
- contract.yaml      # The data contract
- data/              # Sample input data
- run.py             # Optional runner
```

## Where to Go Next

- Start with 01_quickstart/ if you are new to LakeLogic
- Move to 02_core_patterns/ for core modeling patterns
- Use 03_data_sources/ to connect to databases, APIs, and streaming
- Explore 04_cloud_platforms/ for platform-specific integrations
- Use 05_orchestration/ templates to schedule pipelines
- See 06_advanced_workflows/ for end-to-end scenarios
- Use 07_production/ for CI/CD and secrets
