# Patterns

Common data engineering recipes. Pick what you need.

## Examples

### [bronze_quality_gate/](bronze_quality_gate/)
Apply quality rules at the Bronze layer. Useful when you want to reject obviously bad data before it enters your lakehouse.

### [dedup_survivorship/](dedup_survivorship/)
Handle duplicate records with survivorship rules. Keep the most recent record per key using ROW_NUMBER().

### [scd2_dimension/](scd2_dimension/)
Build Slowly Changing Dimension Type 2 tables. Track historical changes with effective dates and current flags.

### [late_arriving_reprocess/](late_arriving_reprocess/)
Safely backfill partitions when late-arriving data comes in. Uses `overwrite_partition_safe` strategy.

### [external_python_logic/](external_python_logic/)
Extend LakeLogic with custom Python functions or Jupyter notebooks for complex business logic.

---

## When to Use Each Pattern

| Problem | Pattern |
|---------|---------|
| Reject garbage at ingestion | bronze_quality_gate |
| Multiple records per key | dedup_survivorship |
| Track historical changes | scd2_dimension |
| Backfill without data loss | late_arriving_reprocess |
| Complex business logic | external_python_logic |

## Prerequisites

Complete [02_tutorials/](../02_tutorials/) to understand the basics first.

## Next Steps

- [04_features/](../04_features/) - Notifications and secrets
- [05_production/](../05_production/) - Full production examples
