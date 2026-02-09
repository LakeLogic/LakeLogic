# Catalog Table Name Support (Unity Catalog, Fabric, Synapse)

**LakeLogic now supports 3-part table names for Unity Catalog, Fabric LakeDB, and Synapse Analytics!**

Use familiar table names like `catalog.schema.table` instead of full storage paths. LakeLogic automatically resolves them to the underlying Delta Lake storage locations.

---

## 🚀 **Quick Start**

### **Unity Catalog (Databricks)**

```python
from lakelogic import DataProcessor

# Use Unity Catalog table name directly
processor = DataProcessor(engine="polars", contract="contracts/customers.yaml")
good_df, bad_df = processor.run_source("main.default.customers")

# No need for full S3 path!
# LakeLogic automatically resolves to: s3://bucket/unity-catalog/main/default/customers/
```

### **Fabric LakeDB (Microsoft)**

```python
from lakelogic import DataProcessor

# Use Fabric table name directly
processor = DataProcessor(engine="polars", contract="contracts/customers.yaml")
good_df, bad_df = processor.run_source("myworkspace.sales_lakehouse.customers")

# No need for full OneLake path!
# LakeLogic automatically resolves to: abfss://myworkspace@onelake.dfs.fabric.microsoft.com/sales_lakehouse.Lakehouse/Tables/customers/
```

### **Synapse Analytics (Azure)**

```python
import os
from lakelogic import DataProcessor

# Set storage account (required for Synapse)
os.environ["SYNAPSE_STORAGE_ACCOUNT"] = "mysynapsestorage"

# Use Synapse table name directly
processor = DataProcessor(engine="polars", contract="contracts/customers.yaml")
good_df, bad_df = processor.run_source("salesdb.dbo.customers")

# No need for full ADLS path!
# LakeLogic automatically resolves to: abfss://salesdb@mysynapsestorage.dfs.core.windows.net/dbo/customers/
```

---

## 🔐 **Permissions Model**

### **How Permissions Work**

**LakeLogic uses a 2-step permission model:**

1. **Metadata Access** (resolve table name → storage path)
   - Requires Unity Catalog/Fabric/Synapse API permissions
   - Only needed once per table (results are cached)

2. **Data Access** (read/write Delta Lake files)
   - Requires cloud storage permissions (S3, Azure Blob, GCS)
   - Used for all data operations

---

## 📋 **Required Permissions by Platform**

### **Unity Catalog (Databricks)**

#### **Step 1: Metadata Access**

**Required:**
- Databricks workspace access
- Unity Catalog `SELECT` permission on table
- Unity Catalog `USE CATALOG` and `USE SCHEMA` permissions

**Setup:**
```bash
# Set environment variables
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."  # Personal Access Token

# Or pass directly to resolver
from lakelogic.engines.unity_catalog import UnityCatalogResolver
resolver = UnityCatalogResolver(
    host="https://your-workspace.cloud.databricks.com",
    token="dapi..."
)
```

**Grant permissions (SQL):**
```sql
-- Grant catalog/schema access
GRANT USE CATALOG ON CATALOG main TO `user@example.com`;
GRANT USE SCHEMA ON SCHEMA main.default TO `user@example.com`;

-- Grant table access
GRANT SELECT ON TABLE main.default.customers TO `user@example.com`;
```

#### **Step 2: Data Access (Cloud Storage)**

**AWS S3:**
```python
from lakelogic.engines.delta_adapter import DeltaAdapter

adapter = DeltaAdapter(storage_options={
    "AWS_REGION": "us-west-2",
    "AWS_ACCESS_KEY_ID": "AKIA...",
    "AWS_SECRET_ACCESS_KEY": "..."
})

# Read Unity Catalog table
df = adapter.read("main.default.customers")
```

**Required IAM permissions:**
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::your-bucket/unity-catalog/*"
      ]
    }
  ]
}
```

**Azure ADLS:**
```python
adapter = DeltaAdapter(storage_options={
    "AZURE_STORAGE_ACCOUNT_NAME": "your_account",
    "AZURE_STORAGE_ACCOUNT_KEY": "..."
})
```

**Required Azure permissions:**
- **Storage Blob Data Reader** role on the storage account

---

### **Fabric LakeDB (Microsoft)**

#### **Step 1: Metadata Access**

**Not required!** Fabric table names follow a predictable pattern:
```
workspace.lakehouse.table → abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse.Lakehouse/Tables/table/
```

LakeLogic resolves this **without API calls** (no credentials needed for resolution).

#### **Step 2: Data Access (OneLake)**

**Required:**
- OneLake access via Azure credentials
- **Contributor** or **Reader** role on the Fabric workspace

**Setup:**
```python
from lakelogic.engines.delta_adapter import DeltaAdapter

adapter = DeltaAdapter(storage_options={
    "AZURE_STORAGE_ACCOUNT_NAME": "onelake",
    "AZURE_STORAGE_ACCOUNT_KEY": "..."  # Or use Azure AD auth
})

# Read Fabric table
df = adapter.read("myworkspace.sales_lakehouse.customers")
```

**Azure AD Authentication (Recommended):**
```python
from azure.identity import DefaultAzureCredential
from deltalake import DeltaTable

# Use Azure AD for authentication
credential = DefaultAzureCredential()
token = credential.get_token("https://storage.azure.com/.default")

adapter = DeltaAdapter(storage_options={
    "AZURE_STORAGE_ACCOUNT_NAME": "onelake",
    "BEARER_TOKEN": token.token
})
```

---

### **Synapse Analytics (Azure)**

#### **Step 1: Metadata Access**

**Not required!** Synapse table names follow a predictable pattern:
```
database.schema.table → abfss://database@{STORAGE_ACCOUNT}.dfs.core.windows.net/schema/table/
```

**Setup:**
```bash
# Set storage account (required)
export SYNAPSE_STORAGE_ACCOUNT="mysynapsestorage"
```

#### **Step 2: Data Access (ADLS Gen2)**

**Required:**
- ADLS Gen2 access via Azure credentials
- **Storage Blob Data Reader** role on the storage account

**Setup:**
```python
from lakelogic.engines.delta_adapter import DeltaAdapter

adapter = DeltaAdapter(storage_options={
    "AZURE_STORAGE_ACCOUNT_NAME": "mysynapsestorage",
    "AZURE_STORAGE_ACCOUNT_KEY": "..."
})

# Read Synapse table
df = adapter.read("salesdb.dbo.customers")
```

---

## 📊 **Permission Comparison**

| Platform | Metadata Access | Data Access | Credentials Required |
|----------|-----------------|-------------|---------------------|
| **Unity Catalog** | ✅ Required (Databricks API) | ✅ Required (S3/Azure/GCS) | DATABRICKS_HOST, DATABRICKS_TOKEN + Cloud creds |
| **Fabric LakeDB** | ❌ Not required (predictable paths) | ✅ Required (OneLake) | Azure credentials only |
| **Synapse Analytics** | ❌ Not required (predictable paths) | ✅ Required (ADLS Gen2) | SYNAPSE_STORAGE_ACCOUNT + Azure creds |

---

## 🔧 **Configuration Examples**

### **Unity Catalog (Full Setup)**

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

# Credentials loaded from environment
processor = DataProcessor(engine="polars", contract="contracts/customers.yaml")
good_df, bad_df = processor.run_source("main.default.customers")
```

---

### **Fabric LakeDB (Full Setup)**

```bash
# .env file
AZURE_STORAGE_ACCOUNT_NAME=onelake
AZURE_STORAGE_ACCOUNT_KEY=...
# Or use Azure AD (no key needed)
```

```python
from lakelogic import DataProcessor

# Credentials loaded from environment
processor = DataProcessor(engine="polars", contract="contracts/customers.yaml")
good_df, bad_df = processor.run_source("myworkspace.sales_lakehouse.customers")
```

---

### **Synapse Analytics (Full Setup)**

```bash
# .env file
SYNAPSE_STORAGE_ACCOUNT=mysynapsestorage
AZURE_STORAGE_ACCOUNT_NAME=mysynapsestorage
AZURE_STORAGE_ACCOUNT_KEY=...
```

```python
from lakelogic import DataProcessor

# Credentials loaded from environment
processor = DataProcessor(engine="polars", contract="contracts/customers.yaml")
good_df, bad_df = processor.run_source("salesdb.dbo.customers")
```

---

## 🎯 **Usage Examples**

### **Example 1: Unity Catalog with Contract YAML**

**Contract (`contracts/customers.yaml`):**
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

**Python:**
```python
from lakelogic import DataProcessor

processor = DataProcessor(engine="polars", contract="contracts/customers.yaml")
good_df, bad_df = processor.run_source()  # Uses path from contract

print(f"Good: {len(good_df)}, Bad: {len(bad_df)}")
```

---

### **Example 2: Fabric LakeDB with Explicit Platform**

```python
from lakelogic.engines.unity_catalog import resolve_catalog_path

# Explicit platform hint
path = resolve_catalog_path("myworkspace.sales_lakehouse.customers", platform="fabric")
print(path)
# abfss://myworkspace@onelake.dfs.fabric.microsoft.com/sales_lakehouse.Lakehouse/Tables/customers/

# Use with Delta adapter
from lakelogic.engines.delta_adapter import DeltaAdapter
adapter = DeltaAdapter()
df = adapter.read("myworkspace.sales_lakehouse.customers")
```

---

### **Example 3: Multi-Platform Support**

```python
from lakelogic import DataProcessor

# Unity Catalog
processor = DataProcessor(engine="polars", contract="contracts/uc_customers.yaml")
uc_good, uc_bad = processor.run_source("main.default.customers")

# Fabric LakeDB
processor = DataProcessor(engine="polars", contract="contracts/fabric_customers.yaml")
fabric_good, fabric_bad = processor.run_source("myworkspace.sales_lakehouse.customers")

# Synapse Analytics
processor = DataProcessor(engine="polars", contract="contracts/synapse_customers.yaml")
synapse_good, synapse_bad = processor.run_source("salesdb.dbo.customers")
```

---

## 🔍 **Troubleshooting**

### **Problem: Unity Catalog table not found**

**Error:**
```
ValueError: Failed to resolve Unity Catalog table main.default.customers: Table not found
```

**Solution:**
1. Check Databricks credentials:
   ```bash
   echo $DATABRICKS_HOST
   echo $DATABRICKS_TOKEN
   ```

2. Verify table exists:
   ```sql
   SHOW TABLES IN main.default;
   ```

3. Check permissions:
   ```sql
   SHOW GRANTS ON TABLE main.default.customers;
   ```

---

### **Problem: Fabric table access denied**

**Error:**
```
AuthorizationPermissionMismatch: This request is not authorized to perform this operation
```

**Solution:**
1. Check Azure credentials
2. Verify workspace access (Contributor or Reader role)
3. Use Azure AD authentication instead of account key

---

### **Problem: Synapse storage account not set**

**Error:**
```
SYNAPSE_STORAGE_ACCOUNT environment variable not set
```

**Solution:**
```bash
export SYNAPSE_STORAGE_ACCOUNT="mysynapsestorage"
```

---

## 💡 **Best Practices**

### **1. Use Environment Variables for Credentials**

```bash
# .env file
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi...
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
```

```python
# Load from environment (automatic)
from lakelogic import DataProcessor
processor = DataProcessor(engine="polars", contract="contracts/customers.yaml")
```

---

### **2. Use Azure AD for Fabric/Synapse (More Secure)**

```python
from azure.identity import DefaultAzureCredential

# Azure AD authentication (no keys in code)
credential = DefaultAzureCredential()
token = credential.get_token("https://storage.azure.com/.default")

adapter = DeltaAdapter(storage_options={
    "AZURE_STORAGE_ACCOUNT_NAME": "onelake",
    "BEARER_TOKEN": token.token
})
```

---

### **3. Cache Table Paths for Performance**

```python
from lakelogic.engines.unity_catalog import get_unity_catalog_resolver

# Resolver caches table paths automatically
resolver = get_unity_catalog_resolver()

# First call: API request
path1 = resolver.resolve_table("main.default.customers")

# Second call: cached (no API request)
path2 = resolver.resolve_table("main.default.customers")
```

---

## 📚 **Summary**

**LakeLogic supports 3-part table names for:**
- ✅ **Unity Catalog** (Databricks): `catalog.schema.table`
- ✅ **Fabric LakeDB** (Microsoft): `workspace.lakehouse.table`
- ✅ **Synapse Analytics** (Azure): `database.schema.table`

**Permissions:**
- **Unity Catalog:** Databricks API + Cloud storage
- **Fabric LakeDB:** Azure credentials only (no API)
- **Synapse Analytics:** Azure credentials only (no API)

**Usage:**
```python
from lakelogic import DataProcessor

# Unity Catalog
processor = DataProcessor(engine="polars", contract="customers.yaml")
good_df, bad_df = processor.run_source("main.default.customers")

# Fabric LakeDB
good_df, bad_df = processor.run_source("myworkspace.sales_lakehouse.customers")

# Synapse Analytics
good_df, bad_df = processor.run_source("salesdb.dbo.customers")
```

---

*Last Updated: February 2026*
