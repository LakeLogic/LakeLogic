# Multi-Tenant Data Isolation

Enforce per-tenant data isolation across a SaaS platform's shared data lake — one contract per tenant, one `LAKELOGIC_ENV` per environment, zero cross-tenant leakage.

## Business Scenario

You operate a **B2B SaaS analytics platform** serving 50+ enterprise clients. Each tenant's data is stored in a shared ADLS Gen2 account but partitioned by tenant namespace:

```
abfss://events@myaccount.dfs.core.windows.net/
  tenants/
    acme-corp/
      bronze/   silver/   gold/
    globex-ltd/
      bronze/   silver/   gold/
    initech/
      bronze/   silver/   gold/
```

**The problem**: Contracts are identical in shape across all tenants (same schema, same quality rules, same transformations). But:

- Each tenant must only see its own data
- Tenant-specific paths are different in dev (local) vs. test (shared test ADLS) vs. prod (tenant ADLS namespaces)
- You want **one contract template** per layer, not 50 × 3 = 150 YAML files

## Value Proposition

- ☑️ **One contract template per layer** — `bronze_events.yaml`, `silver_events.yaml`, `gold_events.yaml`
- ☑️ **Per-tenant `environments` block** — different ADLS path per tenant namespace
- ☑️ **`TENANT_ID` + `LAKELOGIC_ENV`** — two env vars control everything at runtime
- ☑️ **Schema + quality rules shared** — enforced consistently across all tenants
- ☑️ **Zero cross-tenant data leakage** — each contract run is fully path-isolated
- ☑️ **Onboarding a new tenant** = add one environments entry, re-run the pipeline

## How It Works

Each contract carries environments keyed as `{env}-{tenant}`:

```yaml
server:
  type: adls
  path: abfss://events@myaccount.dfs.core.windows.net/tenants/prod-default/bronze/

environments:
  dev-acme:
    path: ./data/dev/acme/bronze_events.csv
    format: csv
  dev-globex:
    path: ./data/dev/globex/bronze_events.csv
    format: csv
  test-acme:
    path: abfss://events@myaccount.dfs.core.windows.net/test/acme/bronze/
  test-globex:
    path: abfss://events@myaccount.dfs.core.windows.net/test/globex/bronze/
  prod-acme:
    path: abfss://events@myaccount.dfs.core.windows.net/prod/acme/bronze/
  prod-globex:
    path: abfss://events@myaccount.dfs.core.windows.net/prod/globex/bronze/
```

At runtime, set `LAKELOGIC_ENV={env}-{tenant}`:

```bash
# Run for ACME in dev
export LAKELOGIC_ENV=dev-acme
lakelogic-driver --registry contracts/_registry.yaml --layers bronze,silver,gold

# Run for Globex in prod
export LAKELOGIC_ENV=prod-globex
lakelogic-driver --registry contracts/_registry.yaml --layers bronze,silver,gold
```

## Files

```
multi_tenant_isolation/
├── README.md
├── contracts/
│   ├── _registry.yaml
│   ├── bronze_events.yaml          # Raw clickstream events (Bronze)
│   ├── silver_events.yaml          # Cleaned + enriched events (Silver)
│   └── gold_tenant_kpi.yaml        # Per-tenant KPI aggregation (Gold)
├── data/
│   ├── dev/
│   │   ├── acme/events_raw.csv
│   │   └── globex/events_raw.csv
│   └── quarantine/
└── multi_tenant_isolation.ipynb    # Interactive walkthrough
```

## Onboarding a New Tenant

1. Add entries in the `environments` block of each contract:
   ```yaml
   environments:
     dev-newtenant:
       path: ./data/dev/newtenant/events_raw.csv
       format: csv
     prod-newtenant:
       path: abfss://events@myaccount.dfs.core.windows.net/prod/newtenant/bronze/
   ```
2. Create the tenant's seed data in `data/dev/newtenant/`
3. Run: `LAKELOGIC_ENV=dev-newtenant lakelogic-driver ...`

No schema changes, no policy changes, no new YAML files needed.

## Compliance Note

Tenant path isolation ensures:
- No ADLS access policy can accidentally expose one tenant's data to another
- Each pipeline run writes exactly to one tenant's namespace
- Audit logs reference `LAKELOGIC_ENV` — giving full per-tenant lineage

## Next Steps

- `08_compliance_governance/hipaa_pii_masking/` — add per-tenant PII masking
- `06_advanced_workflows/gdpr_compliance/` — per-tenant GDPR erasure
- `07_production/ci_cd/` — multi-tenant CI/CD orchestration
