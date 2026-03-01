# LakeLogic Examples

Hands-on examples organized by learning stage. Each example is a self-contained notebook that runs locally, on Colab, or on Databricks with a single setup cell.

## Structure

| Folder | What It Covers |
|---|---|
| `01_quickstart/` | Your first pipeline in 5 minutes |
| `02_core_patterns/` | Essential modeling patterns for any lakehouse |
| `03_compliance_governance/` | HIPAA & GDPR policy enforcement, PII masking |
| `_archive/` | Untested examples — available once validated |

## Quick Start

```bash
# 1. Install LakeLogic
pip install lakelogic

# 2. Run your first example
cd examples/01_quickstart
jupyter notebook 01_hello_world.ipynb
```

## Available Examples

### 01_quickstart
| Notebook | What You'll Learn |
|---|---|
| `01_hello_world.ipynb` | Ingest a remote CSV, apply quality rules, inspect good/bad rows |
| `02_database_governance.ipynb` | Extract from SQLite, validate against a contract, quarantine bad records |
| `03_dbt_pii_quality.ipynb` | Convert a dbt schema to a LakeLogic contract with PII detection |

### 02_core_patterns
| Folder | What You'll Learn |
|---|---|
| `bronze_quality_gate/` | Stop bad data at ingestion — schema enforcement + row rules |
| `dedup_survivorship/` | Deduplicate records and elect a survivor using configurable rules |
| `medallion_architecture/` | Full Bronze → Silver → Gold pipeline in one contract chain |
| `reference_joins/` | Enrich records by joining a reference table inside the contract |
| `scd2_dimension/` | Preserve full history with versioned rows (Slowly Changing Dimension Type 2) |
| `soft_delete/` | Flag CDC deletes instead of removing rows — GDPR & audit friendly |

### 03_compliance_governance
| Folder | What You'll Learn |
|---|---|
| `hipaa_gdpr_pii_masking/` | HIPAA & GDPR Policy Packs, automated PII masking, audit-ready quarantine |

## Example Layout

Most examples follow this structure:

```
example_name/
├── README.md          # What this example teaches
├── contract.yaml      # The data contract
├── data/              # Sample input data
└── playbook.ipynb     # Interactive notebook — runs locally or on Colab
```

## Where to Go Next

1. **New to LakeLogic?** Start with `01_quickstart/01_hello_world.ipynb`
2. **Building a lakehouse?** Work through `02_core_patterns/` — bronze gate → dedup → SCD2 → soft delete
3. **Regulated industry?** Go straight to `03_compliance_governance/hipaa_gdpr_pii_masking/`