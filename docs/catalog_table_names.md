# Catalog Table Name Support (Unity Catalog, Fabric, Synapse)
<!-- markdownlint-disable MD013 -->

LakeLogic supports 3-part table names for Unity Catalog, Fabric LakeDB, and Synapse Analytics. Table names like `catalog.schema.table` are automatically resolved to their underlying Delta Lake storage paths — no Spark required.

---

## Quick Start

### Unity Catalog (Databricks) — Fully Automatic

Unity Catalog table names are **auto-detected** by the processor when `DATABRICKS_HOST` is set:

```python
from lakelogic import DataProcessor

# Automatically resolves main.default.customers → s3://bucket/.../customers/
processor = DataProcessor(engine="polars", contract="contracts/customers.yaml")
good_df, bad_df = processor.run_source("main.default.customers")
```

**Requirements:**
```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
```

### Fabric LakeDB (Microsoft)

Fabric uses **predictable OneLake paths** — no API call needed for resolution.
Use `resolve_catalog_path()` with `platform="fabric"`:

```python
from lakelogic.engines.unity_catalog import resolve_catalog_path

# Resolve table name → OneLake path
path = resolve_catalog_path("myworkspace.sales_lakehouse.customers", platform="fabric")
# → abfss://myworkspace@onelake.dfs.fabric.microsoft.com/sales_lakehouse.Lakehouse/Tables/customers/

# Then use with processor
from lakelogic import DataProcessor

processor = DataProcessor(engine="polars", contract="contracts/customers.yaml")
good_df, bad_df = processor.run_source(path)
```

### Synapse Analytics (Azure)

Synapse uses **predictable ADLS Gen2 paths** — no API call needed for resolution.
Use `resolve_catalog_path()` with `platform="synapse"`:

```python
import os
from lakelogic.engines.unity_catalog import resolve_catalog_path

os.environ["SYNAPSE_STORAGE_ACCOUNT"] = "mysynapsestorage"

# Resolve table name → ADLS path
path = resolve_catalog_path("salesdb.dbo.customers", platform="synapse")
# → abfss://salesdb@mysynapsestorage.dfs.core.windows.net/dbo/customers/

# Then use with processor
from lakelogic import DataProcessor

processor = DataProcessor(engine="polars", contract="contracts/customers.yaml")
good_df, bad_df = processor.run_source(path)
```

---

## Auto-Detection Behavior

The processor calls `resolve_catalog_path(path)` **without** a `platform` parameter.
The auto-detection logic in the code is:

| Condition | Resolved As |
|-----------|------------|
| `DATABRICKS_HOST` env var is set | Unity Catalog (API call) |
| No env vars set | Tries Unity Catalog, falls back to raw path |
| `platform="fabric"` passed explicitly | Fabric LakeDB (no API, predictable path) |
| `platform="synapse"` passed explicitly | Synapse Analytics (no API, predictable path) |

> **Note:** The processor currently only auto-detects Unity Catalog.
> For Fabric and Synapse, resolve the path first with `resolve_catalog_path(..., platform="fabric"|"synapse")`,
> then pass the resolved storage path to `processor.run_source()`.

---

## Permissions Model

LakeLogic uses a **2-step permission model**:

1. **Metadata Access** (resolve table name → storage path)
   - Unity Catalog: requires Databricks API credentials
   - Fabric/Synapse: not required (predictable paths)

2. **Data Access** (read/write Delta Lake files)
   - Requires cloud storage permissions (S3, Azure Blob, GCS)
   - Used for all data operations

---

## Required Permissions by Platform

### Unity Catalog (Databricks)

**Metadata Access:**
- Databricks `SELECT`, `USE CATALOG`, and `USE SCHEMA` permissions
- Setup:
```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
```
- Or pass directly:
```python
from lakelogic.engines.unity_catalog import UnityCatalogResolver

resolver = UnityCatalogResolver(host="https://your-workspace.cloud.databricks.com", token="dapi...")
```

**Data Access (AWS S3):**
```python
from lakelogic.engines.delta_adapter import DeltaAdapter

adapter = DeltaAdapter(
    storage_options={"AWS_REGION": "us-west-2", "AWS_ACCESS_KEY_ID": "AKIA...", "AWS_SECRET_ACCESS_KEY": "..."}
)
df = adapter.read("main.default.customers")
```

Required IAM: `s3:GetObject`, `s3:ListBucket` on the Unity Catalog storage bucket.

**Data Access (Azure ADLS):**
```python
adapter = DeltaAdapter(
    storage_options={"AZURE_STORAGE_ACCOUNT_NAME": "your_account", "AZURE_STORAGE_ACCOUNT_KEY": "..."}
)
```

Required role: **Storage Blob Data Reader** on the storage account.

---

### Fabric LakeDB (Microsoft)

**Metadata Access:** Not required — paths follow a predictable pattern:
```
workspace.lakehouse.table
  → abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse.Lakehouse/Tables/table/
```

**Data Access (OneLake):**
- Requires Azure credentials and **Contributor** or **Reader** role on workspace
- Setup:
```python
from lakelogic.engines.delta_adapter import DeltaAdapter

adapter = DeltaAdapter(storage_options={"AZURE_STORAGE_ACCOUNT_NAME": "onelake", "AZURE_STORAGE_ACCOUNT_KEY": "..."})
df = adapter.read("myworkspace.sales_lakehouse.customers")
```

**Azure AD (recommended):**
```python
from azure.identity import DefaultAzureCredential

credential = DefaultAzureCredential()
token = credential.get_token("https://storage.azure.com/.default")

adapter = DeltaAdapter(storage_options={"AZURE_STORAGE_ACCOUNT_NAME": "onelake", "BEARER_TOKEN": token.token})
```

---

### Synapse Analytics (Azure)

**Metadata Access:** Not required — paths follow a predictable pattern:
```
database.schema.table
  → abfss://database@{STORAGE_ACCOUNT}.dfs.core.windows.net/schema/table/
```

Requires `SYNAPSE_STORAGE_ACCOUNT` env var:
```bash
export SYNAPSE_STORAGE_ACCOUNT="mysynapsestorage"
```

**Data Access (ADLS Gen2):**
- Requires **Storage Blob Data Reader** role
```python
from lakelogic.engines.delta_adapter import DeltaAdapter

adapter = DeltaAdapter(
    storage_options={"AZURE_STORAGE_ACCOUNT_NAME": "mysynapsestorage", "AZURE_STORAGE_ACCOUNT_KEY": "..."}
)
df = adapter.read("salesdb.dbo.customers")
```

---

## Permission Comparison

| Platform | Metadata Access | Data Access | Credentials Required |
|----------|-----------------|-------------|---------------------|
| **Unity Catalog** | Required (Databricks API) | Required (S3/Azure/GCS) | `DATABRICKS_HOST` + `DATABRICKS_TOKEN` + cloud creds |
| **Fabric LakeDB** | Not required (predictable paths) | Required (OneLake) | Azure credentials only |
| **Synapse Analytics** | Not required (predictable paths) | Required (ADLS Gen2) | `SYNAPSE_STORAGE_ACCOUNT` + Azure creds |

---

## Configuration Examples

### Unity Catalog (Full Setup)

```bash
# .env file
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi...
AWS_REGION=us-west-2
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
```

```python
from lakelogic import DataProcessor

# Credentials loaded from environment — auto-resolves UC table names
processor = DataProcessor(engine="polars", contract="contracts/customers.yaml")
good_df, bad_df = processor.run_source("main.default.customers")
```

### Fabric LakeDB (Full Setup)

```bash
# .env file
AZURE_STORAGE_ACCOUNT_NAME=onelake
AZURE_STORAGE_ACCOUNT_KEY=...
```

```python
from lakelogic.engines.unity_catalog import resolve_catalog_path
from lakelogic import DataProcessor

path = resolve_catalog_path("myworkspace.sales_lakehouse.customers", platform="fabric")
processor = DataProcessor(engine="polars", contract="contracts/customers.yaml")
good_df, bad_df = processor.run_source(path)
```

### Synapse Analytics (Full Setup)

```bash
# .env file
SYNAPSE_STORAGE_ACCOUNT=mysynapsestorage
AZURE_STORAGE_ACCOUNT_NAME=mysynapsestorage
AZURE_STORAGE_ACCOUNT_KEY=...
```

```python
from lakelogic.engines.unity_catalog import resolve_catalog_path
from lakelogic import DataProcessor

path = resolve_catalog_path("salesdb.dbo.customers", platform="synapse")
processor = DataProcessor(engine="polars", contract="contracts/customers.yaml")
good_df, bad_df = processor.run_source(path)
```

---

## Usage Examples

### Contract YAML with Unity Catalog

```yaml
version: 1.0.0
dataset: bronze_customers
server:
  type: delta
  path: main.default.customers  # Unity Catalog table name
  format: delta

model:
  fields:
    - name: id
      type: integer
    - name: name
      type: string

quality:
  row_rules:
    - not_null: id
```

```python
from lakelogic import DataProcessor

processor = DataProcessor(engine="polars", contract="contracts/customers.yaml")
good_df, bad_df = processor.run_source()  # Uses path from contract
```

### Explicit Platform Resolution

```python
from lakelogic.engines.unity_catalog import resolve_catalog_path

# Fabric (auto-resolved, no API call)
path = resolve_catalog_path("myworkspace.sales_lakehouse.customers", platform="fabric")
# → abfss://myworkspace@onelake.dfs.fabric.microsoft.com/sales_lakehouse.Lakehouse/Tables/customers/

# Synapse (auto-resolved, no API call)
path = resolve_catalog_path("salesdb.dbo.customers", platform="synapse")
# → abfss://salesdb@mysynapsestorage.dfs.core.windows.net/dbo/customers/
```

### DeltaAdapter for Ad-Hoc Access

```python
from lakelogic.engines.delta_adapter import DeltaAdapter

adapter = DeltaAdapter()

# Read
df = adapter.read("main.default.customers")

# Write
adapter.write(df, "s3://bucket/output/", mode="append")

# Merge (upsert)
stats = adapter.merge(target_path="s3://bucket/table/", source_df=new_data, merge_key="id")
```

### Cache Table Paths for Performance

```python
from lakelogic.engines.unity_catalog import get_unity_catalog_resolver

resolver = get_unity_catalog_resolver()

# First call: API request to Databricks
path1 = resolver.resolve_table("main.default.customers")

# Second call: cached (no API request)
path2 = resolver.resolve_table("main.default.customers")
```

---

## Troubleshooting

### Unity Catalog table not found

```
ValueError: Failed to resolve Unity Catalog table main.default.customers: Table not found
```

1. Check credentials: `echo $DATABRICKS_HOST && echo $DATABRICKS_TOKEN`
2. Verify table exists: `SHOW TABLES IN main.default;`
3. Check permissions: `SHOW GRANTS ON TABLE main.default.customers;`

### Fabric table access denied

```
AuthorizationPermissionMismatch: This request is not authorized
```

1. Check Azure credentials
2. Verify workspace access (Contributor or Reader role)
3. Use Azure AD authentication instead of account key

### Synapse storage account not set

```
SYNAPSE_STORAGE_ACCOUNT environment variable not set
```

```bash
export SYNAPSE_STORAGE_ACCOUNT="mysynapsestorage"
```

---

## Best Practices

1. **Use environment variables for credentials** — never hardcode secrets
2. **Azure AD for Fabric/Synapse** — more secure than storage account keys
3. **Leverage caching** — `UnityCatalogResolver` caches paths automatically
4. **Use `DeltaAdapter` for ad-hoc work** — the processor uses delta-rs automatically for pipeline runs

---

## Summary

| Platform | Table Name Format | Auto-Detected by Processor | Resolution Method |
|----------|-------------------|---------------------------|-------------------|
| **Unity Catalog** | `catalog.schema.table` | ✅ Yes (when `DATABRICKS_HOST` set) | Databricks API call |
| **Fabric LakeDB** | `workspace.lakehouse.table` | ❌ No (use `resolve_catalog_path(..., platform="fabric")`) | Predictable path pattern |
| **Synapse Analytics** | `database.schema.table` | ❌ No (use `resolve_catalog_path(..., platform="synapse")`) | Predictable path pattern |

---

*Last Updated: March 2026*
