# Production Contract Template

A full-featured contract template for production use.

## What's Included

This template demonstrates every major LakeLogic feature:

1. **Metadata** - Title, description, owner, contact
2. **Environment Overrides** - Dev vs production paths
3. **Reference Links** - External lookup tables
4. **Schema** - Fields with PII classification
5. **Transformations** - Pre and post SQL
6. **Quality Rules** - Row-level and dataset-level
7. **SLAs** - Freshness and availability
8. **Quarantine** - Error capture configuration

## Files

```
contract_template/
├── contract.yaml
├── data/
│   ├── customers.csv
│   ├── dim_geography.csv
│   └── marketing_opt_outs.csv
└── run.py
```

## Contract Sections

### Metadata
```yaml
info:
  title: Customer Master Data
  version: 1.1.0
  description: |
    Unified customer profile data including PII and membership details.
  owner: CRM & Growth Team
  contact:
    name: Sarah Connor (CRM Lead)
    email: sarah.connor@example.com
```

### Environment Overrides
```yaml
server:
  type: s3
  path: s3://data-lake/customers/master/
  format: parquet

environments:
  dev:
    path: ./data/customers.csv
    format: csv
```

### PII Classification
```yaml
model:
  fields:
    - name: email
      type: string
      pii: true
      classification: sensitive
```

### Dataset Rules
```yaml
quality:
  dataset_rules:
    - name: unique_emails
      sql: "SELECT count(email) - count(distinct email) FROM silver_crm_customers"
      must_be_less_than: 1

    - name: min_customer_base
      sql: "SELECT count(*) FROM silver_crm_customers"
      must_be_greater_than: 1000
```

### SLAs
```yaml
service_levels:
  freshness:
    threshold: 24h
    field: updated_at
  availability:
    threshold: 99.9%
```

## Run It

```bash
cd contract_template
python run.py
```

## Use as Template

1. Copy this folder to your project
2. Rename `contract.yaml` to match your dataset
3. Update schema, rules, and transformations
4. Test with sample data
5. Deploy to production
