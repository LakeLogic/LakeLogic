# Delta Lake & Catalog Examples

This directory contains example contracts demonstrating **Spark-free Delta Lake** operations and **catalog table name** support.

---

## 🚀 **Quick Start**

### **Unity Catalog (Databricks)**

```bash
# Set credentials
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."
export AWS_ACCESS_KEY_ID="AKIA..."
export AWS_SECRET_ACCESS_KEY="..."

# Run contract
lakelogic run \
  --engine polars \
  --contract unity_catalog_contract.yaml \
  --source main.default.customers
```

### **Fabric LakeDB (Microsoft)**

```bash
# Set credentials
export AZURE_STORAGE_ACCOUNT_NAME="onelake"
export AZURE_STORAGE_ACCOUNT_KEY="..."

# Run contract
lakelogic run \
  --engine polars \
  --contract fabric_lakedb_contract.yaml \
  --source myworkspace.sales_lakehouse.transactions
```

### **Synapse Analytics (Azure)**

```bash
# Set credentials
export SYNAPSE_STORAGE_ACCOUNT="mysynapsestorage"
export AZURE_STORAGE_ACCOUNT_NAME="mysynapsestorage"
export AZURE_STORAGE_ACCOUNT_KEY="..."

# Run contract
lakelogic run \
  --engine polars \
  --contract synapse_analytics_contract.yaml \
  --source inventorydb.dbo.stock_levels
```

---

## 📋 **Example Contracts**

### **1. Unity Catalog Contract** (`unity_catalog_contract.yaml`)

**Features:**
- ✅ Unity Catalog table names (`catalog.schema.table`)
- ✅ Delta Lake read/write (Spark-free)
- ✅ Automatic path resolution
- ✅ Email validation with regex
- ✅ Duplicate detection
- ✅ Materialization to Unity Catalog tables

**Usage:**
```python
from lakelogic import DataProcessor

processor = DataProcessor(
    engine="polars",
    contract="unity_catalog_contract.yaml"
)

# Use Unity Catalog table name directly
good_df, bad_df = processor.run_source("main.default.customers")
```

---

### **2. Fabric LakeDB Contract** (`fabric_lakedb_contract.yaml`)

**Features:**
- ✅ Fabric table names (`workspace.lakehouse.table`)
- ✅ OneLake Delta Lake access (Spark-free)
- ✅ Automatic OneLake path resolution
- ✅ Calculated fields (total_amount)
- ✅ Business rule validation
- ✅ Materialization to Fabric tables

**Usage:**
```python
from lakelogic import DataProcessor

processor = DataProcessor(
    engine="polars",
    contract="fabric_lakedb_contract.yaml"
)

# Use Fabric table name directly
good_df, bad_df = processor.run_source("myworkspace.sales_lakehouse.transactions")
```

---

### **3. Synapse Analytics Contract** (`synapse_analytics_contract.yaml`)

**Features:**
- ✅ Synapse table names (`database.schema.table`)
- ✅ ADLS Gen2 Delta Lake access (Spark-free)
- ✅ Automatic ADLS path resolution
- ✅ Inventory reconciliation logic
- ✅ Strict quality thresholds
- ✅ Materialization to Synapse tables

**Usage:**
```python
from lakelogic import DataProcessor

processor = DataProcessor(
    engine="polars",
    contract="synapse_analytics_contract.yaml"
)

# Use Synapse table name directly
good_df, bad_df = processor.run_source("inventorydb.dbo.stock_levels")
```

---

## 🔐 **Permissions Setup**

### **Unity Catalog**

**Required:**
1. Databricks workspace access
2. Unity Catalog `SELECT` permission
3. Cloud storage access (S3/Azure/GCS)

**Grant permissions:**
```sql
GRANT USE CATALOG ON CATALOG main TO `user@example.com`;
GRANT USE SCHEMA ON SCHEMA main.default TO `user@example.com`;
GRANT SELECT ON TABLE main.default.customers TO `user@example.com`;
```

---

### **Fabric LakeDB**

**Required:**
1. Fabric workspace access (Contributor or Reader role)
2. Azure credentials for OneLake

**No SQL grants needed** - permissions managed via Fabric workspace roles.

---

### **Synapse Analytics**

**Required:**
1. ADLS Gen2 access (Storage Blob Data Reader role)
2. `SYNAPSE_STORAGE_ACCOUNT` environment variable

**No SQL grants needed** - permissions managed via Azure RBAC.

---

## 🎯 **Key Benefits**

### **No Spark Required**
- ✅ 10-100x faster for small/medium data
- ✅ No JVM overhead
- ✅ Simple installation (`pip install "lakelogic[delta]"`)

### **Familiar Table Names**
- ✅ Use `catalog.schema.table` instead of full paths
- ✅ Automatic resolution to storage paths
- ✅ Works across Unity Catalog, Fabric, Synapse

### **Full Delta Lake Support**
- ✅ Read/Write operations
- ✅ Atomic MERGE (upsert)
- ✅ Time travel
- ✅ Vacuum & optimize

---

## 📚 **Learn More**

- **[Delta Lake Support](../../docs/delta_lake_support.md)** - Complete Delta-RS guide
- **[Catalog Table Names](../../docs/catalog_table_names.md)** - Permissions and setup
- **[Main README](../../README.md)** - LakeLogic overview

---

## 🔧 **Troubleshooting**

### **Unity Catalog table not found**
```bash
# Check credentials
echo $DATABRICKS_HOST
echo $DATABRICKS_TOKEN

# Verify table exists
databricks-sql --query "SHOW TABLES IN main.default"
```

### **Fabric access denied**
```bash
# Use Azure AD authentication (more secure)
az login
# Credentials are automatically picked up
```

### **Synapse storage account not set**
```bash
# Set required environment variable
export SYNAPSE_STORAGE_ACCOUNT="mysynapsestorage"
```

---

*Last Updated: February 2026*
