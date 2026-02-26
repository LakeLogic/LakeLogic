# Self-Healing Data Pipelines

Implement autonomous, multi-layer reprocessing for late-arriving data.

## Components

- **silver_contract.yaml**: Historical partitioning and truth correction.
- **gold_contract.yaml**: Incremental reacting aggregates (The Product).
- **demo_self_healing.ipynb**: Interactive walkthrough of the "T+3" scenario.
- **data/**: Sample datasets for initial and late arrivals.

## When to Use This Pattern

- **Handle Delivery Latency**: When source data arrives days after the actual event date.
- **Automate Backfills**: Re-calculate business aggregates without manual intervention.
- **Ensure Data Consistency**: Keep Gold products in sync with historical Silver corrections.
- **Idempotent Reprocessing**: Safely re-run any batch without creating duplicates.

## Prerequisites

- Complete **02_core_patterns/medallion_architecture** to understand multi-stage contracts.
- Basic understanding of **Lineage** metadata columns.

## Next Steps

- **03_data_sources/streaming/** for real-time versions of this pattern.
- **05_orchestration/** to schedule these self-healing jobs.
