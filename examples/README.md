# LakeLogic Examples

Hands-on examples organized by learning stage and integration type.

## Structure

- `01_quickstart/` — 5–10 minute wins
- `02_core_patterns/` — essential modeling patterns
- `03_advanced_workflows/` — real-world scenarios
- `04_compliance_governance/` — HIPAA, PII masking
- `_archive/` — untested examples (data sources, cloud platforms, orchestration, production)

## Quick Start

```bash
# 1. Install LakeLogic
pip install lakelogic

# 2. Run your first example
cd examples/01_quickstart
lakelogic run --contract users_contract.yaml --source data/sample_customers.csv
```

## Example Structure

Most examples follow this layout:

```
example_name/
├── README.md          # What this example teaches
├── contract.yaml      # The data contract
├── data/              # Sample input data
└── playbook.ipynb     # Interactive notebook
```

## Where to Go Next

1. Start with `01_quickstart/` if you are new to LakeLogic
2. Move to `02_core_patterns/` for medallion architecture, SCD2, deduplication, and reference joins
3. See `03_advanced_workflows/` for end-to-end scenarios (insurance ELT, GDPR, streaming, synthetic data, **AI contract enrichment**)
4. Use `04_compliance_governance/` for HIPAA and PII masking patterns

> **Archived examples** in `_archive/` cover data sources, cloud platforms, orchestration, and production but have not been fully tested yet.
