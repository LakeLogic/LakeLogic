# LakeLogic Bootstrap

The bootstrap command generates **starter contracts + a registry** by scanning a landing zone.
It accelerates onboarding for new systems when no contracts exist yet.

## When to Use It

- **New system onboarding** with only raw landing files available.
- **Legacy data dump** where schemas are unknown.
- **Rapid POC** to validate whether LakeLogic fits a data source.
- **Database onboarding** — generate contracts from an existing catalog, schema, or warehouse.

## Bootstrap Sources

| Source | Command | What It Does |
| :--- | :--- | :--- |
| **Landing zone (files)** | `--landing data/` | Scans CSV/JSON/Parquet files, infers schemas |
| **Unity Catalog** | `--catalog catalog.schema` | Reads `INFORMATION_SCHEMA` from Databricks |
| **Snowflake** | `--database ANALYTICS.PUBLIC` | Introspects Snowflake schema |
| **PostgreSQL** | `--database postgresql://...` | Reads `information_schema.columns` |
| **BigQuery** | `--database project.dataset` | Reads BigQuery dataset metadata |
| **DuckDB** | `--database duckdb:///path.db` | Reads DuckDB information schema |

## File-Based Bootstrap

### Basic Usage

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

---

## Database & Catalog Bootstrap

Generate contracts directly from an existing database schema. LakeLogic introspects the catalog, reads column metadata (types, nullability, comments), and produces one contract per table — no data files required.

### Unity Catalog (Databricks)

```bash
lakelogic bootstrap \
  --catalog retail_prod.marketing \
  --output-dir contracts/marketing \
  --registry contracts/marketing/_system.yaml \
  --detect-pii \
  --suggest-rules \
  --ai
```

This connects to your Databricks workspace via the configured Spark session and:

1. Lists all tables in `retail_prod.marketing`
2. Reads column types, nullability, and column comments from `INFORMATION_SCHEMA`
3. Generates one contract per table with proper type mappings
4. Optionally runs AI to suggest quality rules from column names and comments
5. Creates a `_system.yaml` registry listing all discovered entities

### Snowflake

```bash
lakelogic bootstrap \
  --database "snowflake://user@account/ANALYTICS/PUBLIC" \
  --output-dir contracts/analytics \
  --registry contracts/analytics/_system.yaml \
  --detect-pii
```

### PostgreSQL / MySQL

```bash
lakelogic bootstrap \
  --database "postgresql://user:pass@host:5432/mydb?schema=public" \
  --output-dir contracts/mydb \
  --registry contracts/mydb/_system.yaml
```

### DuckDB (Local)

```bash
lakelogic bootstrap \
  --database "duckdb:///path/to/analytics.db" \
  --output-dir contracts/analytics \
  --registry contracts/analytics/_system.yaml
```

### What Catalog Bootstrap Generates

For each table, the generated contract includes:

- **Schema**: All columns with correct types mapped to LakeLogic types (`string`, `long`, `double`, `timestamp`, etc.)
- **Required fields**: Columns with `NOT NULL` constraints marked as `required: true`
- **Descriptions**: Column comments imported as `description:` fields
- **PII detection**: Columns matching PII patterns (email, phone, SSN) annotated with `pii: true`
- **Primary keys**: Extracted from catalog constraints and set as `primary_key`
- **Suggested rules**: AI-generated quality rules when `--ai` is enabled

### Example Output

```yaml
# Auto-generated by: lakelogic bootstrap --catalog retail_prod.marketing
version: 1.0.0
info:
  title: "Silver Marketing Sessions"
  table_name: "silver_google_analytics_sessions"
  target_layer: "silver"
  domain: "marketing"
  system: "google_analytics"

model:
  fields:
    - name: "session_id"
      type: "string"
      required: true
      description: "Unique session identifier from GA4"
    - name: "user_id"
      type: "string"
      pii: true
      description: "Pseudonymous user ID"
    - name: "session_start"
      type: "timestamp"
      required: true
    - name: "page_views"
      type: "integer"
    - name: "bounce_rate"
      type: "double"

primary_key: ["session_id"]

quality:
  row_rules:
    - not_null: "session_id"
    - not_null: "session_start"
    - sql: "page_views >= 0"
  dataset_rules:
    - unique: "session_id"
```

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
