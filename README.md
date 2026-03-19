# LakeLogic

**Your data estate. Under Contract.**

[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://LakeLogic.github.io/LakeLogic/)
[![PyPI](https://img.shields.io/pypi/v/lakelogic?logo=pypi&logoColor=white)](https://pypi.org/project/lakelogic/)
[![Downloads](https://img.shields.io/pypi/dm/lakelogic)](https://pypi.org/project/lakelogic/)
[![Python](https://img.shields.io/badge/python-3.9+-blue?logo=python&logoColor=white)](https://www.python.org)
[![License](https://img.shields.io/badge/license-Apache%202.0-green)](LICENSE)

Catch breaking data changes before they reach production. One YAML contract. Any engine.
Every row validated, quarantined, or promoted — automatically.

---

## 🌐 Data Mesh Alignment

LakeLogic is built for the decentralized data estate, directly supporting the four pillars of Data Mesh:

| Pillar | How LakeLogic Delivers |
| :--- | :--- |
| **Domain Ownership** | Contracts are owned and defined by domain teams (e.g., CRM, Finance) who know the data best. |
| **Data as a Product** | Contracts serve as the explicit "product interface," guaranteeing quality for consumers. |
| **Self-Serve Platform** | A standardized runtime that any team can use to deploy quality gates without infra silos. |
| **Federated Governance** | Global standards (e.g., PII masking) are defined centrally but enforced locally at every layer. |

---

## Quick Start (60 Seconds)

```bash
pip install "lakelogic[all]"
```

### 1. Bootstrap a contract

```bash
lakelogic bootstrap --landing data/ --output contracts/ --ai
```

*Scans data, infers schemas, detects PII, and generates rules using AI.*

### 2. Run the quality gate

```bash
lakelogic run --contract contracts/customers.yaml --source data/customers.csv
```

### 3. Or use Python directly

```python
from lakelogic import DataProcessor

result = DataProcessor("contract.yaml").run_source()
print(f"Valid: {result.good_count}  |  Quarantined: {result.bad_count}")
```

---

## Contract Example

This single YAML file replaces hundreds of lines of validation code:

```yaml
# REQUIRED: Contract version for compatibility tracking
version: "1.0"

# REQUIRED: Metadata — who owns this data and where it lives in the org
info:
  title: Silver Customers                 # Human-readable name for logs and monitoring
  owner: data-team                        # Team responsible for this contract
  domain: CRM                             # Data mesh domain (CRM, Finance, Marketing...)
  system: Salesforce                      # Source system this data originates from
  classification: "confidential"          # Data sensitivity: public | internal | confidential | restricted
  status: "production"                    # Lifecycle stage: development | staging | production | deprecated

# OPTIONAL: Custom tags for governance, cost tracking, and SLA enforcement
metadata:
  pii_present: true                       # Flags this dataset as containing personal data
  retention_days: 2555                    # Operational retention policy (7 years) — used by automated purge jobs
  sla_tier: "tier1"                       # SLA priority: tier1 = critical (< 4hr response)

# REQUIRED: Schema definition — expected columns, types, and constraints
# Field descriptions serve two purposes:
#   1. Business documentation — so analysts understand each field without asking
#   2. LLM context — used by `lakelogic bootstrap --ai` to generate smarter rules
model:
  fields:
    - name: customer_id
      type: integer
      required: true                      # Generates automatic NOT NULL quality rule
      description: "Unique identifier for each customer record"
    - name: email
      type: string
      pii: true                           # Marks as personally identifiable — enables auto-masking
      description: "Primary email address used for account login and communications"
    - name: revenue
      type: float
      description: "Lifetime revenue attributed to this customer in base currency"
    - name: status
      type: string
      description: "Current account state: active, churned, or pending onboarding"

# OPTIONAL: Schema evolution and unknown field handling
schema_policy:
  evolution: "strict"                     # Schema change behavior: strict | compatible | allow
  unknown_fields: "quarantine"            # Unknown columns: quarantine | drop | allow

# REQUIRED: Where to load data from (supports files, S3, ADLS, databases)
source:
  type: landing                           # Acquisition pattern: landing (files) | table (DB) | stream (Kafka)
  path: "data/customers/*.csv"            # Glob pattern — also supports s3://, abfss://, Unity Catalog tables
  load_mode: incremental                  # Only process new/changed data: full | incremental | cdc

# OPTIONAL: Reference data for joins and enrichment
links:
  - name: "dim_countries"                  # Logical name used in lookup/join transformations
    path: "./reference/countries.parquet"   # File path, S3 URI, or Unity Catalog table
    type: "parquet"                         # Format: parquet | csv | table
    broadcast: true                        # Broadcast join for small dimensions (Spark)

# OPTIONAL: Environment-specific overrides (activate via LAKELOGIC_ENV)
environments:
  dev:
    path: "dev/customers"                  # Cheaper storage for development
    format: "parquet"
  prod:
    path: "s3://prod-lake/silver/customers"
    format: "delta"

# OPTIONAL: Data transformations — pre (before validation) and post (after validation)
transformations:
  - rename:                               # Fix source naming drift before schema checks
      from: "cust_id"
      to: "customer_id"
    phase: "pre"                          # PRE = applied before quality rules run
  - deduplicate:                          # Keep most recent record per business key
      columns: ["customer_id"]
      order_by: "updated_at"
  - sql: |                                # Full SQL for complex enrichment logic
      SELECT *, UPPER(status) as status_code,
        revenue * 0.1 as tax_estimate
      FROM source
    phase: "post"                         # POST = applied after validation, on good data only

# OPTIONAL: Quality rules — rows that fail are quarantined, not silently dropped
quality:
  row_rules:                              # Row-level: each row evaluated independently
    - sql: "customer_id IS NOT NULL AND email IS NOT NULL"   # Completeness check
    - sql: "status IN ('active', 'churned', 'pending')"     # Enum validation
    - sql: "revenue >= 0"                                    # Range validation
    - sql: "email LIKE '%@%.%'"                              # Format validation
  dataset_rules:                          # Dataset-level: aggregate checks on all good rows
    - unique: "customer_id"               # No duplicate business keys

# OPTIONAL: Data provenance and audit trail
lineage:
  enabled: true                           # Stamps every row with run_id, source path, timestamps

# REQUIRED: Output — where and how to write validated data
materialization:
  strategy: merge                         # Write mode: overwrite | append | merge (upsert)
  target_path: "silver/customers"         # Destination path (also supports Unity Catalog table names)
  format: delta                           # Storage format: delta | parquet | iceberg | csv
  merge_keys: [customer_id]              # Business keys for merge/upsert operations
  partition_by:                           # Partition columns for query performance
    - "country"
    - "created_date"
  cluster_by: ["customer_id"]            # Clustering columns (Delta/Iceberg optimization)
  reprocess_policy: "overwrite_partition" # Idempotent re-runs: overwrite_partition | append | fail

# OPTIONAL: Soft deletes — GDPR "right to erasure" without losing audit trail
soft_deletes:
  enabled: true                           # Mark rows as deleted instead of hard-deleting
  flag_field: "_is_deleted"               # Boolean column added to target table
  reason_field: "_delete_reason"          # e.g. "GDPR request", "duplicate"
  timestamp_field: "_deleted_at"          # When the deletion was recorded

# OPTIONAL: Quarantine — isolate failed rows with error reasons for replay
quarantine:
  enabled: true                           # If false, pipeline hard-fails on any quality error
  target: "quarantine/customers"          # Where bad rows are written (with _lakelogic_errors column)
  notifications:                          # Alert channels when rows are quarantined
    - target: "https://hooks.slack.com/services/YOUR/WEBHOOK"  # Slack, Teams, email auto-detected
      on_events: ["quarantine", "failure", "schema_drift"]

# OPTIONAL: Service Level Objectives — data reliability monitoring
service_levels:
  freshness:
    threshold: "24h"                      # Data must be refreshed within this window
    field: "updated_at"                   # Timestamp field to check staleness against
  availability:
    threshold: 99.9                       # % of runs that must produce valid output

# OPTIONAL: Regulatory compliance metadata — used for audit-ready reports
compliance:
  gdpr:
    applicable: true                      # Whether GDPR applies to this dataset
    legal_basis: "legitimate_interest"    # Art. 6(1) lawful basis for processing
    purpose: "Customer engagement tracking"  # Why this data is processed (Art. 5(1)(b))
    retention_period: "24 months"         # Legal retention limit for PII — separate from operational retention
  eu_ai_act:
    applicable: false                     # Whether EU AI Act applies (for ML feature datasets)
```

> [!TIP]
> **[View the Complete Contract Reference](docs/contract_template.md)** for every available configuration option.

---

## Architecture

LakeLogic enforces Data Contracts as quality gates across the Medallion Architecture (Bronze → Silver → Gold).

![LakeLogic Architecture](docs/assets/lakelogic_architecture.png)

Each layer uses its own contract:

| Layer | Role | Guarantee |
| :--- | :--- | :--- |
| **Bronze** | Capture everything raw, no validation | Immutable record of source |
| **Silver** | Full validation, business rules, dedup | Trusted, queryable data |
| **Gold** | Aggregations, KPIs, ML features | Analytics-ready datasets |
| **Quarantine** | Failed rows isolated with error reasons | Nothing silently dropped |

**Key Guarantee:** `source_count = good_count + bad_count` — 100% reconciliation, always.

---

## Business Impact

| Benefit | Detail |
| :--- | :--- |
| **Cut Compute Spend by 80%** | Not every job needs Spark. Run maintenance tasks on Polars or DuckDB locally. |
| **Guaranteed Integrity** | Dirty data goes to quarantine — dashboards are never poisoned. |
| **Full Transparency** | Trace any KPI back to raw source records and the contract that validated them. |
| **Parallel Development** | Two engineers work on two tables simultaneously without touching the same file. |
| **Easier Debugging** | Logs tell you exactly which module failed — no searching through monster scripts. |

---

## Data Mesh Alignment

LakeLogic directly supports the four pillars of **Data Mesh**:

- **Domain Ownership** — Contracts are owned by the teams who know the data best.
- **Data as a Product** — Contracts serve as the explicit "product interface" guaranteeing quality.
- **Self-Serve Platform** — Any team can deploy quality gates without infra silos.
- **Federated Governance** — Global standards defined centrally, enforced locally at every layer.

---

## Examples

The [examples](https://github.com/LakeLogic/LakeLogic/tree/main/examples) directory contains runnable notebooks:

| Folder | What You'll Learn |
| :--- | :--- |
| [`01_quickstart/`](examples/01_quickstart/) | Remote CSV ingestion, database governance |
| [`02_core_patterns/`](examples/02_core_patterns/) | Bronze quality gate, medallion architecture, SCD2, deduplication, soft deletes |
| [`03_compliance_governance/`](examples/03_compliance_governance/) | HIPAA & GDPR Policy Packs, automated PII masking, audit-ready quarantine |

---

## Documentation

- **[Full Docs](https://LakeLogic.github.io/LakeLogic)** — Guides and API reference
- **[Architecture Overview](docs/architecture_diagram.md)** — Medallion with Quality Gates
- **[Contract Reference](docs/contract_template.md)** — Full YAML field reference
- **[Governance at Scale](docs/organization.md)** — Organizing 1,000s of contracts
- **[CLI Reference](https://LakeLogic.github.io/LakeLogic/cli/)** — Command-line usage
- **[Changelog](https://github.com/LakeLogic/LakeLogic/blob/main/CHANGELOG.md)** — Release history

## Technical Capabilities

- **Engine Agnostic** — Auto-optimizes for Spark, Polars, DuckDB, or Pandas
- **Incremental-First** — Built-in watermarking, CDC, and file-mtime tracking
- **SQL-First Rules** — Define business logic in the language your team already speaks
- **Automatic Lineage** — Every row stamped with Run IDs and source paths
- **100% Reconciliation** — Mathematically guaranteed: `source = good + bad`

## Contributing

See `CONTRIBUTING.md` to get started, or `docs/installation.md#developer-installation` for environment setup.

---

### License

Apache-2.0
