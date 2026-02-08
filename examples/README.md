# LakeGuard Examples

Learn LakeGuard through hands-on examples, organized by skill level.

---

## Learning Path

### Level 1: First Contract (5 minutes)
Start here. Run your first data quality check.

```
01_getting_started/basic_validation/
```

### Level 2: Core Concepts (30 minutes)
Understand the medallion architecture and reference data.

```
02_tutorials/
├── medallion_architecture/   # Bronze → Silver pipeline
└── reference_joins/          # Link tables and enrichment
```

### Level 3: Real Patterns (pick what you need)
Common data engineering recipes.

```
03_patterns/
├── bronze_quality_gate/      # Quality checks at ingestion
├── dedup_survivorship/       # Handle duplicate records
├── scd2_dimension/           # Slowly changing dimensions
├── late_arriving_reprocess/  # Safe partition backfill
└── external_python_logic/    # Custom Python/notebook hooks
```

### Level 4: Production Ready
Alerts, secrets, and complete examples.

```
04_features/
└── notifications_and_secrets/   # Slack, Teams, email alerts

05_production/
├── contract_template/           # Full production-grade template
└── insurance_elt/               # Complete multi-entity example
```

### Level 5: Integrate with Your Stack
Connect LakeGuard to your orchestrator.

```
06_integrations/
└── job_templates/               # Airflow, Dagster, Prefect, etc.
```

---

## Quick Start

```bash
# 1. Install LakeGuard
pip install lakeguard

# 2. Run your first example
cd examples/01_getting_started/basic_validation
lakeguard run --contract contract.yaml --source data/sample_customers.csv

# 3. Explore the output
# Good records pass, bad records go to quarantine with error reasons
```

---

## Example Structure

Each example contains:

```
example_name/
├── README.md          # What this example teaches
├── contract.yaml      # The data contract
├── data/              # Sample input data
└── run.py             # (optional) Python script to run
```

---

## Need Help?

- [Documentation](https://LineageLogic.github.io/LakeGuard)
- [GitHub Issues](https://github.com/LineageLogic/LakeGuard/issues)
- [GitHub Discussions](https://github.com/LineageLogic/LakeGuard/discussions)
