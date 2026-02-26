# Insurance ELT Pipeline

A complete multi-entity ELT pipeline demonstrating LakeLogic at scale.

## Overview

This example models a realistic insurance company data platform:

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Policy Admin   │     │  Claims System  │     │    Billing      │
│    (Source)     │     │    (Source)     │     │    (Source)     │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                         BRONZE LAYER                             │
│  bronze_policies    bronze_claims    bronze_payments             │
└─────────────────────────────────────────────────────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│                         SILVER LAYER                             │
│  silver_policies    silver_claims    silver_payments             │
│                 + Reference Data (states, coverage, etc.)        │
└─────────────────────────────────────────────────────────────────┘
         │                       │                       │
         └───────────────────────┼───────────────────────┘
                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│                          GOLD LAYER                              │
│           gold_dim_policyholders    gold_fact_claims             │
└─────────────────────────────────────────────────────────────────┘
```

## Files

```
insurance_elt/
├── contracts/
│   ├── insurance/
│   │   ├── _registry.yaml              # Entity registry
│   │   ├── policy_admin/
│   │   │   ├── bronze/
│   │   │   └── silver/
│   │   ├── claims_system/
│   │   │   ├── bronze/
│   │   │   └── silver/
│   │   ├── billing/
│   │   │   ├── bronze/
│   │   │   └── silver/
│   │   └── warehouse/
│   │       └── gold/
│   └── shared/
│       └── reference/                  # Shared reference data
├── data/
│   ├── bronze/                         # Raw CDC files
│   └── reference/                      # Lookup tables
├── output/                             # Materialized tables
└── lakelogic_driver.ipynb              # Interactive walkthrough
```

## Registry-Driven Processing

The `_registry.yaml` defines all entities and their contract paths:

```yaml
entries:
  - entity: policies
    enabled: true
    contracts:
      bronze: policy_admin/bronze/bronze_policy_admin_policies_v1.yaml
      silver: policy_admin/silver/silver_policies_v1.yaml

  - entity: claims
    enabled: true
    contracts:
      bronze: claims_system/bronze/bronze_claims_claims_v1.yaml
      silver: claims_system/silver/silver_claims_v1.yaml

  - entity: payments
    enabled: true
    contracts:
      bronze: billing/bronze/bronze_billing_payments_v1.yaml
      silver: billing/silver/silver_payments_v1.yaml
```

## Run with Driver

```bash
# Process all Bronze layers
lakelogic-driver --registry contracts/insurance/_registry.yaml --layers bronze

# Process all Silver layers
lakelogic-driver --registry contracts/insurance/_registry.yaml --layers silver

# Process specific entity
lakelogic-driver --registry contracts/insurance/_registry.yaml --entities claims --layers silver
```

## Key Concepts Demonstrated

| Concept | Where |
|---------|-------|
| Multi-entity registry | `contracts/insurance/_registry.yaml` |
| Bronze → Silver flow | `policy_admin/bronze/` → `policy_admin/silver/` |
| Reference data | `contracts/shared/reference/` |
| CDC processing | `data/bronze/*_cdc.csv` |
| Gold aggregations | `warehouse/gold/` |

## Run the Notebook

Open `lakelogic_driver.ipynb` for an interactive walkthrough of the entire pipeline.

## Scaling This Pattern

1. **Add new source systems**: Create new folders under `contracts/insurance/`
2. **Add new entities**: Add entries to `_registry.yaml`
3. **Add Gold tables**: Create contracts in `warehouse/gold/`
4. **Orchestrate**: Use templates from `05_orchestration/`
