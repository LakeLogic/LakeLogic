# Automatic Cloud Credential Resolution

LakeLogic automatically resolves cloud credentials for Delta Lake operations. You don't need to manually configure `storage_options` - credentials are automatically detected from:

- **Azure AD** (DefaultAzureCredential)
- **AWS IAM roles** and environment variables
- **GCP service accounts** and Application Default Credentials
- **Environment variables** for all clouds

---

## 🎯 **How It Works**

When you use `DeltaAdapter` or `DataProcessor`, LakeLogic automatically:

1. **Detects the cloud provider** from the path (s3://, abfss://, gs://)
2. **Resolves credentials** using the appropriate method
3. **Caches tokens** (e.g., Azure AD) for performance
4. **Falls back** to environment variables if automatic resolution fails

---

## ✅ **Before (Manual Configuration)**

```python
# OLD WAY - Manual credential configuration
from azure.identity import DefaultAzureCredential
from lakelogic.engines.delta_adapter import DeltaAdapter

# Manually get Azure AD token
credential = DefaultAzureCredential()
token = credential.get_token("https://storage.azure.com/.default")

# Manually configure storage options
adapter = DeltaAdapter(storage_options={"AZURE_STORAGE_ACCOUNT_NAME": "onelake", "BEARER_TOKEN": token.token})

df = adapter.read("abfss://workspace@onelake.dfs.fabric.microsoft.com/...")
```

---

## ✨ **After (Automatic Resolution)**

```python
# NEW WAY - Automatic credential resolution
from lakelogic.engines.delta_adapter import DeltaAdapter

# Just create the adapter - credentials are resolved automatically!
adapter = DeltaAdapter()

# Read Fabric LakeDB table - Azure AD token is automatically acquired
df = adapter.read("myworkspace.sales_lakehouse.transactions")
```

**That's it!** No manual token acquisition, no storage_options configuration.

---

## 📋 **Examples by Platform**

### **Unity Catalog (Databricks)**

```python
import os
from lakelogic import DataProcessor

# Set Databricks credentials (required)
os.environ["DATABRICKS_HOST"] = "https://your-workspace.cloud.databricks.com"
os.environ["DATABRICKS_TOKEN"] = "dapi..."

# Set cloud storage credentials (AWS example)
os.environ["AWS_ACCESS_KEY_ID"] = "AKIA..."
os.environ["AWS_SECRET_ACCESS_KEY"] = "..."

# Use Unity Catalog table name - credentials are resolved automatically!
processor = DataProcessor(engine="polars", contract="customers.yaml")
good_df, bad_df = processor.run_source("main.default.customers")
```

---

### **Fabric LakeDB (Microsoft)**

```python
from lakelogic.engines.delta_adapter import DeltaAdapter

# Option 1: Azure AD (Recommended) - just login
# az login

# Option 2: Account Key
# os.environ["AZURE_STORAGE_ACCOUNT_KEY"] = "..."

# Use Fabric table name - Azure AD token is automatically acquired!
adapter = DeltaAdapter()
df = adapter.read("myworkspace.sales_lakehouse.transactions")
```

---

### **Synapse Analytics (Azure)**

```python
import os
from lakelogic import DataProcessor

# Set storage account (required)
os.environ["SYNAPSE_STORAGE_ACCOUNT"] = "mysynapsestorage"

# Azure AD credentials are automatically acquired (az login)
# Or set account key: os.environ["AZURE_STORAGE_ACCOUNT_KEY"] = "..."

# Use Synapse table name - credentials are resolved automatically!
processor = DataProcessor(engine="polars", contract="inventory.yaml")
good_df, bad_df = processor.run_source("inventorydb.dbo.stock_levels")
```

---

### **Delta Lake on S3 (AWS)**

```python
from lakelogic.engines.delta_adapter import DeltaAdapter

# Option 1: AWS IAM role (EC2, Lambda, ECS) - no configuration needed!
# Option 2: Environment variables
# os.environ["AWS_ACCESS_KEY_ID"] = "AKIA..."
# os.environ["AWS_SECRET_ACCESS_KEY"] = "..."

# Read Delta table - AWS credentials are automatically resolved!
adapter = DeltaAdapter()
df = adapter.read("s3://my-bucket/delta-table/")
```

---

### **Delta Lake on GCS (GCP)**

```python
from lakelogic.engines.delta_adapter import DeltaAdapter

# Option 1: Service account
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/path/to/service-account.json"

# Option 2: Application Default Credentials
# gcloud auth application-default login

# Read Delta table - GCP credentials are automatically resolved!
adapter = DeltaAdapter()
df = adapter.read("gs://my-bucket/delta-table/")
```

---

## 🔧 **Credential Resolution Priority**

### **Azure (Fabric, Synapse, Unity Catalog on Azure)**

1. User-provided `storage_options` (if any)
2. Environment variable: `AZURE_STORAGE_ACCOUNT_KEY`
3. **Azure AD** (DefaultAzureCredential) - **Automatic!**

### **AWS (Unity Catalog on AWS, S3)**

1. User-provided `storage_options` (if any)
2. Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
3. **AWS IAM role** (boto3 credential chain) - **Automatic!**

### **GCP (Unity Catalog on GCP, GCS)**

1. User-provided `storage_options` (if any)
2. Environment variable: `GOOGLE_APPLICATION_CREDENTIALS`
3. **Application Default Credentials** - **Automatic!**

---

## 🎛️ **Advanced: Disable Automatic Resolution**

If you want to manually control credentials:

```python
from lakelogic.engines.delta_adapter import DeltaAdapter

# Disable automatic credential resolution
adapter = DeltaAdapter(
    storage_options={"AWS_ACCESS_KEY_ID": "AKIA...", "AWS_SECRET_ACCESS_KEY": "..."},
    auto_resolve_credentials=False,  # Disable automatic resolution
)

df = adapter.read("s3://bucket/table/")
```

---

## 🔐 **Security Best Practices**

### **✅ Recommended: Use Automatic Resolution**

```python
# Azure AD (no keys in code!)
az login
adapter = DeltaAdapter()

# AWS IAM role (no keys in code!)
# Automatically uses EC2 instance role
adapter = DeltaAdapter()

# GCP service account (no keys in code!)
gcloud auth application-default login
adapter = DeltaAdapter()
```

### **❌ Avoid: Hardcoding Credentials**

```python
# DON'T DO THIS - credentials in code!
adapter = DeltaAdapter(
    storage_options={
        "AWS_ACCESS_KEY_ID": "AKIA...",  # ❌ Hardcoded
        "AWS_SECRET_ACCESS_KEY": "...",  # ❌ Hardcoded
    }
)
```

---

## 📚 **Learn More**

- **[Ingestion & Cloud Setup](ingestion.md)** - Detailed setup for each platform
- **[Delta Lake Support](delta_lake_support.md)** - Complete Delta-RS guide
- **[Catalog Table Names](catalog_table_names.md)** - Unity Catalog, Fabric, Synapse

---

*Last Updated: February 2026*
