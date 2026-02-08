# Production

Production-grade contracts and complete examples.

## Examples

### [contract_template/](contract_template/)
A full-featured contract template with:
- Complete schema with PII classification
- Multi-join transformations
- Row and dataset quality rules
- SLA definitions
- Quarantine configuration

Use this as a starting point for your production contracts.

### [insurance_elt/](insurance_elt/)
A complete multi-entity ELT pipeline for an insurance company:
- 3 source systems (Policy Admin, Claims, Billing)
- Bronze → Silver → Gold layers
- Reference data management
- Registry-driven orchestration
- Jupyter notebook walkthrough

This is the flagship example showing LakeLogic at scale.

---

## Prerequisites

Complete the tutorials and patterns first:
- [01_getting_started/](../01_getting_started/)
- [02_tutorials/](../02_tutorials/)
- [03_patterns/](../03_patterns/)

## Next Steps

- [06_integrations/](../06_integrations/) - Connect to Airflow, Dagster, etc.
