# Core Patterns

Essential building blocks for data quality and modeling.

## Examples

- medallion_architecture/ - Bronze and Silver stage pattern
- reference_joins/ - Enrich data with lookup tables
- bronze_quality_gate/ - Validate at ingestion
- dedup_survivorship/ - Keep the best record per key
- scd2_dimension/ - Track history with SCD Type 2
- soft_delete/ - Handle logical deletes safely

## When to Use Each Pattern

- Ingest raw data safely: bronze_quality_gate
- Resolve duplicates: dedup_survivorship
- Track history: scd2_dimension
- Enrich records: reference_joins
- Model stages: medallion_architecture
- Keep tombstones: soft_delete

## Prerequisites

Complete 01_quickstart/ first.

## Next Steps

- 03_data_sources/ for connectors and ingestion
- 06_advanced_workflows/ for end-to-end scenarios
