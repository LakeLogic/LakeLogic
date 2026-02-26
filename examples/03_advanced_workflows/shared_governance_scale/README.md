# Shared Governance at Scale

This example demonstrates how to apply shared transformations and validation across many tables,
while still keeping a contract-per-table approach. It shows how to use policy packs for shared
cleanup logic, a base template to standardize metadata and quality rules, and advanced structured
transformations (pivot/unpivot) across dozens of contracts.

## Business Scenario

You are onboarding 500+ operational tables into Bronze and Silver. You need:
- consistent cleansing rules (trim, lower, shared not-null checks)
- shared validation baselines (row-count bounds, strict schema policy)
- soft-delete normalization across many tables
- fast, parallel execution without manual contract duplication

## Value Proposition

- One policy pack enforces shared transforms and row rules across every contract
- One base template standardizes metadata and baseline quality checks
- Soft-delete normalization stays consistent across sources
- Structured pivot/unpivot transformations keep analytics output consistent
- Registry-driven parallel execution scales to hundreds of entities

## Files

shared_governance_scale/
- contracts/_registry.yaml
- contracts/_shared/base_silver.yaml
- contracts/bronze/*.yaml (13 entities)
- contracts/silver/*.yaml (13 entities)
- policy_packs/shared_standard.yaml
- data/*.csv (13 entities)
- shared_governance_scale.ipynb

## Step 1: Apply Shared Template to All Silver Contracts

```bash
python scripts/apply_contract_template.py \
  --base-template examples/06_advanced_workflows/shared_governance_scale/contracts/_shared/base_silver.yaml \
  --registry examples/06_advanced_workflows/shared_governance_scale/contracts/_registry.yaml \
  --stage silver \
  --list-merge-keys transformations,quality.row_rules,quality.dataset_rules \
  --list-mode append \
  --soft-delete
```

This standardizes schema policy, lineage, and dataset-level rules for all Silver contracts.
`--soft-delete` auto-detects `operation`, `deleted_at`, and `is_deleted` columns and injects a shared
soft-delete mapping (applies to `events` and `clicks` in this sample data).
Use `--entity-include` or `--entity-exclude` to target specific subsets (regex supported).

### Python API (No CLI Required)

```python
from pathlib import Path

from lakelogic.tools.template_apply import apply_contract_template

apply_contract_template(
    base_template=Path("examples/06_advanced_workflows/shared_governance_scale/contracts/_shared/base_silver.yaml"),
    registry=Path("examples/06_advanced_workflows/shared_governance_scale/contracts/_registry.yaml"),
    stage="silver",
    list_merge_keys=["transformations", "quality.row_rules", "quality.dataset_rules"],
    list_mode="append",
    soft_delete=True,
)
```

## Step 2: Run in Parallel with Shared Policy Pack

```bash
lakelogic-driver \
  --registry examples/06_advanced_workflows/shared_governance_scale/contracts/_registry.yaml \
  --layers bronze,silver \
  --policy-pack shared_standard \
  --policy-pack-dir examples/06_advanced_workflows/shared_governance_scale/policy_packs \
  --max-workers 8
```

## Step 3: Grouping at Scale (Optional)

Run only specific entities without editing the registry:

```bash
lakelogic-driver \
  --registry examples/06_advanced_workflows/shared_governance_scale/contracts/_registry.yaml \
  --layers silver \
  --entities users,events,sessions,purchases \
  --policy-pack shared_standard \
  --policy-pack-dir examples/06_advanced_workflows/shared_governance_scale/policy_packs
```

## Step 4: Pivot + Unpivot (Structured Transformations)

- `silver_feature_flags.yaml` uses `pivot` to create per-user feature flag columns.
- `silver_daily_metrics.yaml` uses `unpivot` to normalize wide metrics into long format.

## Notebook

Open `shared_governance_scale.ipynb` for a guided walk-through.
