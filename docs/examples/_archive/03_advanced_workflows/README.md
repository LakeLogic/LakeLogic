# Advanced Workflows

Complex end-to-end scenarios and real-world data engineering patterns.

## Examples

- **late_arriving_reprocess/** — Self-healing pipelines for out-of-order data
- **external_python_logic/** — Custom logic via notebooks
- **payments_lifecycle/** — Multi-entity lifecycle and SCD2
- **insurance_elt/** — Full end-to-end industry scenario
- **shared_governance_scale/** — Shared transformations and validation at scale
- **gdpr_compliance/** — GDPR Right-to-Erasure and PII masking
- **date_dimension/** — Date dimension generator with fiscal calendars and holidays
- **partitioned_merge/** — Partition-aware merge materialization
- **polars_streaming/** — Process files larger than memory via Polars streaming
- **env_promotion/** ⭐ — Promote contracts dev → test → prod with one env var
- **multi_tenant_isolation/** ⭐ — Per-tenant path isolation for B2B SaaS platforms

## When to Use Each Pattern

| Pattern | Use when you need to… |
|---|---|
| `late_arriving_reprocess` | Self-heal aggregates after out-of-order data |
| `external_python_logic` | Run custom Python or notebooks inside a contract |
| `payments_lifecycle` | Track stateful events with SCD2 customer history |
| `insurance_elt` | Full industry ELT template with reference data |
| `shared_governance_scale` | Enforce shared rules across 500+ tables |
| `gdpr_compliance` | Data subject erasure and PII masking |
| `date_dimension` | Build dim_date with fiscal calendars and holidays |
| `partitioned_merge` | Merge into partitioned Parquet/Delta tables |
| `polars_streaming` | Process files larger than available RAM |
| `env_promotion` | Deploy the same contracts to dev / test / prod |
| `multi_tenant_isolation` | Isolate data per customer in a B2B SaaS platform |

## Prerequisites

- Complete 02_core_patterns/ to understand building blocks
- Familiarity with the Data as a Product vision

## Next Steps

- 07_production/ for monitoring and deployment
- 08_compliance_governance/ for broader governance patterns
- docs/patterns/ for deeper theory
