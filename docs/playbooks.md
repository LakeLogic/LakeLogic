# Playbooks

Playbooks are end-to-end, real-world scenarios. Each playbook ships with:

- A small dataset you can run locally
- A production-style contract YAML
- A notebook walkthrough that executes the contract

Note: Some playbooks use SQL window functions (e.g., `ROW_NUMBER`) and are best run with the DuckDB or Spark engine.

## Available Playbooks

| Playbook | What it Tests | Assets |
| --- | --- | --- |
| Bronze Quality Gate | Schema enforcement, row rules, lineage injection | `examples/playbooks/bronze_quality_gate/contract.yaml`, `examples/playbooks/bronze_quality_gate/data/raw_signups.csv`, `examples/playbooks/bronze_quality_gate/playbook.ipynb` |
| Customer Onboarding | Dedup, lookups, enrichment, opt-out flagging | `examples/playbooks/customer_onboarding/contract.yaml`, `examples/playbooks/customer_onboarding/data/*.csv`, `examples/playbooks/customer_onboarding/playbook.ipynb` |
| Dedup & Survivorship | Deduplicate by most recent update, derive status | `examples/playbooks/dedup_survivorship/contract.yaml`, `examples/playbooks/dedup_survivorship/data/customer_updates.csv`, `examples/playbooks/dedup_survivorship/playbook.ipynb` |
| Late Arriving Reprocess | Partitioned materialization with overwrite partition | `examples/playbooks/late_arriving_reprocess/contract.yaml`, `examples/playbooks/late_arriving_reprocess/data/*.csv`, `examples/playbooks/late_arriving_reprocess/playbook.ipynb` |
| SCD2 Dimension | Historical tracking with SCD2 materialization | `examples/playbooks/scd2_dimension/contract.yaml`, `examples/playbooks/scd2_dimension/data/*.csv`, `examples/playbooks/scd2_dimension/playbook.ipynb` |
