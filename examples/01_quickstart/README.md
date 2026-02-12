# Quickstart

Your first LakeLogic experience. Start here.

## Examples

### basic_validation/
Time: 5 minutes

The smallest end-to-end contract with quality rules and quarantine output.

```bash
cd basic_validation
lakelogic run --contract contract.yaml --source data/sample_customers.csv
```

### database_extraction/
Time: 10 minutes

Extract data from a local SQLite database and write to Delta format.

```bash
cd database_extraction
lakelogic run --contract users_contract.yaml
```

## Next Steps

- ../02_core_patterns/medallion_architecture/ - Bronze and Silver stages
- ../02_core_patterns/reference_joins/ - Lookups and enrichment
- ../03_data_sources/streaming/sse_wikimedia/ - Streaming quickstart
