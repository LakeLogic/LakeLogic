# LakeLogic Bootstrap

The bootstrap command generates **starter contracts + a registry** by scanning a landing zone.
It accelerates onboarding for new systems when no contracts exist yet.

## When to Use It

- **New system onboarding** with only raw landing files available.
- **Legacy data dump** where schemas are unknown.
- **Rapid POC** to validate whether LakeLogic fits a data source.

## Basic Usage

```bash
lakelogic bootstrap \
  --landing data/landing/new_system \
  --output-dir contracts/new_system \
  --registry contracts/new_system/_registry.yaml \
  --format csv \
  --pattern "*.csv"
```
This scans the landing zone, infers a schema for each entity, writes contracts, and creates a registry with all entities enabled.

## Profiling and PII Detection

Generate profiles and detect PII during bootstrap:

```bash
lakelogic bootstrap \
  --landing data/landing/new_system \
  --output-dir contracts/new_system \
  --registry contracts/new_system/_registry.yaml \
  --format csv \
  --pattern "*.csv" \
  --profile \
  --detect-pii \
  --suggest-rules
```
This writes a profile report per entity and annotates detected PII fields in the contract.

## Sync Mode (Keep Contracts in Sync)

If new files or entities appear later, you can sync the registry without re-creating everything:

```bash
lakelogic bootstrap \
  --landing data/landing/new_system \
  --output-dir contracts/new_system \
  --registry contracts/new_system/_registry.yaml \
  --format csv \
  --pattern "*.csv" \
  --sync
```
This adds new entities to the registry but leaves existing contracts unchanged.

### Update Existing Schemas

```bash
lakelogic bootstrap \
  --landing data/landing/new_system \
  --output-dir contracts/new_system \
  --registry contracts/new_system/_registry.yaml \
  --format csv \
  --pattern "*.csv" \
  --sync \
  --sync-update-schema
```
This appends any newly discovered columns to existing contract schemas.

### Overwrite Existing Contracts

```bash
lakelogic bootstrap \
  --landing data/landing/new_system \
  --output-dir contracts/new_system \
  --registry contracts/new_system/_registry.yaml \
  --format csv \
  --pattern "*.csv" \
  --sync \
  --sync-overwrite
```
This regenerates contracts for existing entities, useful when the initial schema was incomplete.

### What It Generates

- One contract per detected entity
- A `_registry.yaml` with all entities enabled
- Bronze defaults (schema + quarantine + materialization)

## Entity Discovery Rules

1. **Folder per entity** (preferred)
   - `landing/customers/*.csv` -> entity `customers`
   - `landing/orders/*.csv` -> entity `orders`

2. **File prefix fallback**
   - `customers_2026-02-05.csv` -> entity `customers`
   - `orders_2026-02-05.csv` -> entity `orders`

## Real World Examples

### Example 1 - New CRM Feed

Landing zone:

```
landing/crm/customers/*.csv
landing/crm/contacts/*.csv
```

Bootstrap:

```bash
lakelogic bootstrap \
  --landing landing/crm \
  --output-dir contracts/crm \
  --registry contracts/crm/_registry.yaml \
  --format csv \
  --pattern "*.csv"
```
This generates `bronze_customers.yaml`, `bronze_contacts.yaml`, and a registry for the CRM system.

Result:
- `bronze_customers.yaml`
- `bronze_contacts.yaml`
- `_registry.yaml` with both entities enabled

### Example 2 - Vendor Drop (Parquet)

Landing zone:

```
landing/vendor/events/*.parquet
landing/vendor/accounts/*.parquet
```

Bootstrap:

```bash
lakelogic bootstrap \
  --landing landing/vendor \
  --output-dir contracts/vendor \
  --registry contracts/vendor/_registry.yaml \
  --format parquet \
  --pattern "*.parquet"
```
This infers schema from Parquet files and creates a contract per entity folder.

### Example 3 - One-off Data Dump

Landing zone:

```
landing/audit/users_2026-02-05.csv
landing/audit/roles_2026-02-05.csv
```

Bootstrap:

```bash
lakelogic bootstrap \
  --landing landing/audit \
  --output-dir contracts/audit \
  --registry contracts/audit/_registry.yaml \
  --format csv \
  --pattern "*.csv"
```
This bootstraps contracts from file prefixes when there are no entity subfolders.

## Notes and Next Steps

- Bootstrap contracts are **starter templates**. You can add quality rules, transformations, and lineage.
- For production, attach policy packs or shared rules for consistent governance.
- You can immediately run with:

```bash
lakelogic-driver --registry contracts/new_system/_registry.yaml --layers bronze
```
This runs the generated Bronze contracts immediately after bootstrap.

## Common Pitfalls

- **Mixed schemas in one folder**: If a landing folder contains multiple schemas, the inferred contract may be too narrow. Consider splitting by entity or file prefix.
- **Inconsistent file naming**: Bootstrap relies on folder names or file prefixes to identify entities. Standardize naming to avoid merging unrelated datasets.
- **Non-representative samples**: Schema inference uses sample rows. If early files are atypical, inferred types may be wrong.
- **Nested JSON**: JSON with nested structures may require manual adjustments after bootstrap.

## Requirements

For profiling and PII detection, install the optional dependencies:

```bash
pip install "lakelogic[profiling]"
```
