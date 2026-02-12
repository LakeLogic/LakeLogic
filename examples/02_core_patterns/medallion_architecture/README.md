# Medallion Architecture

Learn the Bronze -> Silver pipeline pattern used in modern lakehouses.

## What You'll Learn

1. Stages for different processing layers
2. Bronze layer ingestion
3. Silver layer validation
4. Materialization to Parquet

## Files

medallion_architecture/
- contract.yaml
- data/crm_export.csv
- quickstart_tutorial.ipynb

## The Pattern

Raw CSV -> Bronze stage -> bronze.parquet -> Silver stage -> silver.parquet

## Run It

```python
from lakelogic import DataProcessor

# Run Bronze stage
proc = DataProcessor(contract="contract.yaml", stage="bronze")
bronze_good, bronze_bad = proc.run("data/crm_export.csv")

# Run Silver stage (reads Bronze output)
proc = DataProcessor(contract="contract.yaml")
silver_good, silver_bad = proc.run()
```

Or open quickstart_tutorial.ipynb.

## Next Steps

- ../reference_joins/ - Enrich data with lookup tables
- ../dedup_survivorship/ - Handle duplicates
