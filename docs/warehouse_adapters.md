# Warehouse Adapters (Snowflake & BigQuery)

LakeGuard can execute contracts directly inside Snowflake and BigQuery using SQL pushdown.
These adapters are **table-only** (no file staging). They are ideal when the data already lives in the warehouse.

## Install Extras

If you are installing from source (this repo), use editable extras:

```bash
pip install -e ".[snowflake]"
pip install -e ".[bigquery]"
```

If you are installing from a package index, use the package name:

```bash
pip install "lakeguard[snowflake]"
pip install "lakeguard[bigquery]"
```

## How It Works

- `--engine snowflake` or `--engine bigquery` runs the contract in-warehouse.
- `--source` expects a **table name** (or use `metadata.source_table`). You can also prefix with `table:`.
- Links must be **tables** (use `links[].table` or `path: table:...`).
- LakeGuard creates **temporary tables/views** for intermediate steps.

## Authentication and Secrets

LakeGuard resolves `metadata` values with `env:VAR` or `${ENV:VAR}`. This means you can
use your platform's secret manager to inject environment variables, without putting
secrets in the contract.

- **Snowflake**: Provide connection fields in `metadata` or environment variables:
  `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_WAREHOUSE`,
  `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_ROLE`.
- **Snowflake secret vaults**: Not called directly by LakeGuard yet. Use your
  orchestrator or platform to surface the secret into an environment variable and
  reference it with `env:SNOWFLAKE_PASSWORD`.
- **BigQuery**: Uses Application Default Credentials (ADC). Set
  `GOOGLE_APPLICATION_CREDENTIALS` for a service account JSON if needed, and optionally
  `GOOGLE_CLOUD_PROJECT` or `metadata.bigquery_project`.

### Secret Manager Patterns (Examples)

LakeGuard does not call vendor secret APIs directly. Instead, pull the secret using
your platform's mechanism and export it as an environment variable.

GitHub Actions:

```yaml
env:
  SNOWFLAKE_PASSWORD: ${{ secrets.SNOWFLAKE_PASSWORD }}
  SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
  SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
```

Databricks (notebook/job):

```python
import os
os.environ["SNOWFLAKE_PASSWORD"] = dbutils.secrets.get(scope="lakeguard", key="snowflake_password")
```

Kubernetes:

```yaml
env:
  - name: SNOWFLAKE_PASSWORD
    valueFrom:
      secretKeyRef:
        name: lakeguard-secrets
        key: snowflake_password
```

Azure Key Vault (via App Service / Azure Functions settings or a startup step):

```bash
export SNOWFLAKE_PASSWORD="$(az keyvault secret show --vault-name my-kv --name snowflake-password --query value -o tsv)"
```

AWS Secrets Manager (via a startup step):

```bash
export SNOWFLAKE_PASSWORD="$(aws secretsmanager get-secret-value --secret-id snowflake/password --query SecretString --output text)"
```

## Snowflake Usage

### Minimal Contract

```yaml
version: "1.0.0"
dataset: silver_crm_customers

metadata:
  snowflake_account: env:SNOWFLAKE_ACCOUNT
  snowflake_user: env:SNOWFLAKE_USER
  snowflake_password: env:SNOWFLAKE_PASSWORD
  snowflake_warehouse: env:SNOWFLAKE_WAREHOUSE
  snowflake_database: ANALYTICS
  snowflake_schema: SILVER
  source_table: ANALYTICS.SILVER.CRM_CUSTOMERS

quality:
  row_rules:
    - not_null: email
    - regex_match:
        field: email
        pattern: "^[^@]+@[^@]+\\.[^@]+$"
  dataset_rules:
    - null_ratio:
        field: email
        max: 0.05

transformations:
  - trim:
      fields: ["email"]
  - lower:
      fields: ["email"]
```

### CLI

```bash
lakeguard run --engine snowflake --contract contract.yaml --source ANALYTICS.SILVER.CRM_CUSTOMERS
```

### Python

```python
from lakeguard import DataProcessor

processor = DataProcessor(engine="snowflake", contract="contract.yaml")
source_df, good_df, bad_df = processor.run_source("ANALYTICS.SILVER.CRM_CUSTOMERS")
```

## BigQuery Usage

### Minimal Contract

```yaml
version: "1.0.0"
dataset: silver_crm_customers

metadata:
  bigquery_project: my-project-id
  source_table: my-project-id.silver.crm_customers

quality:
  row_rules:
    - not_null: email
  dataset_rules:
    - unique: customer_id

transformations:
  - deduplicate:
      on: ["email"]
      sort_by: ["created_at"]
      order: desc
```

### CLI

```bash
lakeguard run --engine bigquery --contract contract.yaml --source my-project-id.silver.crm_customers
```

### Python

```python
from lakeguard import DataProcessor

processor = DataProcessor(engine="bigquery", contract="contract.yaml")
source_df, good_df, bad_df = processor.run_source("my-project-id.silver.crm_customers")
```

## Table Links (Lookups)

Table links are required for Snowflake/BigQuery. You can declare them two ways:

```yaml
links:
  - name: dim_geography
    table: ANALYTICS.DIM.GEOGRAPHY
    type: table
```

Or:

```yaml
links:
  - name: dim_geography
    path: table:ANALYTICS.DIM.GEOGRAPHY
```

## Permissions & Caveats

- The adapter uses **temporary tables/views** for each step.
- The Snowflake/BigQuery role must allow creating temp objects.
- Warehouse adapters are **not auto-discovered**. Always set `engine`.
- File paths (CSV/Parquet) are **not** supported here; use Spark/Polars/DuckDB for file-based runs.

If you need staged file ingestion into the warehouse, use Spark or a warehouse-native load step before running LakeGuard.
