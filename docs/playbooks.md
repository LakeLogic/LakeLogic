# Patterns

Patterns are reusable recipes for common data engineering problems. Each pattern includes:

- A working contract YAML
- Sample data you can run locally
- A README explaining when and how to use it

Note: Some patterns use SQL window functions (e.g., `ROW_NUMBER`) and are best run with the DuckDB or Spark engine.

## Available Patterns

| Pattern | What it Solves | Location |
| --- | --- | --- |
| [Bronze Quality Gate](patterns/bronze_quality_gate.md) | Reject bad data at ingestion | `examples/03_patterns/bronze_quality_gate/` |
| [Dedup & Survivorship](patterns/dedup_survivorship.md) | Handle duplicate records | `examples/03_patterns/dedup_survivorship/` |
| [SCD2 Dimension](patterns/scd2_dimension.md) | Track historical changes | `examples/03_patterns/scd2_dimension/` |
| [Late Arriving Reprocess](patterns/late_arriving_reprocess.md) | Safe partition backfill | `examples/03_patterns/late_arriving_reprocess/` |
| [External Python Logic](patterns/external_python_logic.md) | Custom Python/notebook hooks | `examples/03_patterns/external_python_logic/` |

## When to Use Each Pattern

| Problem | Pattern |
| --- | --- |
| Reject garbage at ingestion | Bronze Quality Gate |
| Multiple records per key | Dedup & Survivorship |
| Track historical changes | SCD2 Dimension |
| Backfill without data loss | Late Arriving Reprocess |
| Complex business logic | External Python Logic |

## See Also

- [Tutorials](examples/01_hello_world.ipynb) - Start with the basics first
- [Production Examples](https://github.com/lakelogic/LakeLogic/tree/main/examples) - Complete end-to-end pipeline
