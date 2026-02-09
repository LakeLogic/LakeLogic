# Cloud Authentication Setup

Complete guide for setting up cloud authentication for Delta Lake operations across Unity Catalog, Fabric LakeDB, and Synapse Analytics.

---

## 📦 **Installation**

```bash
# Install Delta Lake support with all cloud authentication libraries
pip install "lakelogic[delta]"
```

**Includes:**
- ✅ `deltalake>=0.15.0` - Delta Lake library (Spark-free)
- ✅ `azure-identity>=1.15.0` - Azure AD authentication
- ✅ `azure-storage-blob>=12.19.0` - Azure Blob Storage
- ✅ `databricks-sdk>=0.18.0` - Unity Catalog metadata access
- ✅ `boto3>=1.28.0` - AWS S3 access
- ✅ `google-cloud-storage>=2.10.0` - GCP GCS access

---

## ☁️ **Platform-Specific Setup**

### **Unity Catalog (Databricks)**

#### **Installation**
```bash
pip install "lakelogic[delta]"
```

#### **Credentials Setup**

**Step 1: Databricks Workspace Access**
```bash
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export DATABRICKS_TOKEN="dapi..."  # Personal Access Token
```

**Step 2: Cloud Storage Access** (depends on Unity Catalog backend)

**AWS S3:**
```bash
export AWS_REGION="us-west-2"
export AWS_ACCESS_KEY_ID="AKIA..."
export AWS_SECRET_ACCESS_KEY="..."
```

**Azure ADLS:**
```bash
export AZURE_STORAGE_ACCOUNT_NAME="your_account"
export AZURE_STORAGE_ACCOUNT_KEY="..."

# Or use Azure AD (recommended)
az login
```

**GCP GCS:**
```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

# Or use gcloud CLI
gcloud auth application-default login
```

#### **Python Example**
```python
import os
from lakelogic import DataProcessor

# Set credentials
os.environ["DATABRICKS_HOST"] = "https://your-workspace.cloud.databricks.com"
os.environ["DATABRICKS_TOKEN"] = "dapi..."
os.environ["AWS_ACCESS_KEY_ID"] = "AKIA..."
os.environ["AWS_SECRET_ACCESS_KEY"] = "..."

# Use Unity Catalog table name
processor = DataProcessor(engine="polars", contract="customers.yaml")
good_df, bad_df = processor.run_source("main.default.customers")
```

---

### **Fabric LakeDB (Microsoft)**

#### **Installation**
```bash
pip install "lakelogic[delta]"
```

#### **Credentials Setup**

**Option 1: Azure AD (Recommended)**
```bash
# Login with Azure CLI
az login

# Credentials are automatically picked up
```

**Option 2: Account Key**
```bash
export AZURE_STORAGE_ACCOUNT_NAME="onelake"
export AZURE_STORAGE_ACCOUNT_KEY="..."
```

#### **Python Example (Azure AD)**
```python
from azure.identity import DefaultAzureCredential
from lakelogic.engines.delta_adapter import DeltaAdapter

# Get Azure AD token
credential = DefaultAzureCredential()
token = credential.get_token("https://storage.azure.com/.default")

# Create adapter with Azure AD auth
adapter = DeltaAdapter(storage_options={
    "AZURE_STORAGE_ACCOUNT_NAME": "onelake",
    "BEARER_TOKEN": token.token
})

# Read Fabric table
df = adapter.read("myworkspace.sales_lakehouse.transactions")
```

#### **Python Example (Account Key)**
```python
import os
from lakelogic import DataProcessor

# Set credentials
os.environ["AZURE_STORAGE_ACCOUNT_NAME"] = "onelake"
os.environ["AZURE_STORAGE_ACCOUNT_KEY"] = "..."

# Use Fabric table name
processor = DataProcessor(engine="polars", contract="transactions.yaml")
good_df, bad_df = processor.run_source("myworkspace.sales_lakehouse.transactions")
```

---

### **Synapse Analytics (Azure)**

#### **Installation**
```bash
pip install "lakelogic[delta]"
```

#### **Credentials Setup**

**Required: Storage Account**
```bash
export SYNAPSE_STORAGE_ACCOUNT="mysynapsestorage"
```

**Option 1: Azure AD (Recommended)**
```bash
# Login with Azure CLI
az login

# Credentials are automatically picked up
```

**Option 2: Account Key**
```bash
export AZURE_STORAGE_ACCOUNT_NAME="mysynapsestorage"
export AZURE_STORAGE_ACCOUNT_KEY="..."
```

#### **Python Example**
```python
import os
from lakelogic import DataProcessor

# Set credentials
os.environ["SYNAPSE_STORAGE_ACCOUNT"] = "mysynapsestorage"
os.environ["AZURE_STORAGE_ACCOUNT_NAME"] = "mysynapsestorage"
os.environ["AZURE_STORAGE_ACCOUNT_KEY"] = "..."

# Use Synapse table name
processor = DataProcessor(engine="polars", contract="inventory.yaml")
good_df, bad_df = processor.run_source("inventorydb.dbo.stock_levels")
```

---

## 🔐 **Authentication Methods**

### **Azure AD (Recommended for Azure)**

**Benefits:**
- ✅ More secure (no keys in code)
- ✅ Centralized access management
- ✅ Automatic token refresh

**Setup:**
```bash
# Install Azure Identity
pip install azure-identity

# Login
az login
```

**Python:**
```python
from azure.identity import DefaultAzureCredential
from lakelogic.engines.delta_adapter import DeltaAdapter

credential = DefaultAzureCredential()
token = credential.get_token("https://storage.azure.com/.default")

adapter = DeltaAdapter(storage_options={
    "AZURE_STORAGE_ACCOUNT_NAME": "your_account",
    "BEARER_TOKEN": token.token
})
```

---

### **AWS IAM Roles (Recommended for AWS)**

**Benefits:**
- ✅ No credentials in code
- ✅ Automatic credential rotation
- ✅ Fine-grained permissions

**Setup:**
```bash
# Configure AWS CLI
aws configure

# Or use IAM role (EC2, Lambda, ECS)
# Credentials are automatically picked up
```

**Python:**
```python
from lakelogic.engines.delta_adapter import DeltaAdapter

# No explicit credentials needed (uses IAM role)
adapter = DeltaAdapter()
df = adapter.read("s3://bucket/table/")
```

---

### **GCP Service Accounts (Recommended for GCP)**

**Benefits:**
- ✅ No credentials in code
- ✅ Centralized access management
- ✅ Audit logging

**Setup:**
```bash
# Set service account key
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

# Or use gcloud CLI
gcloud auth application-default login
```

**Python:**
```python
from lakelogic.engines.delta_adapter import DeltaAdapter

# No explicit credentials needed (uses service account)
adapter = DeltaAdapter()
df = adapter.read("gs://bucket/table/")
```

---

## 🔧 **Environment Variables Reference**

### **Unity Catalog (Databricks)**
```bash
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi...
```

### **AWS S3**
```bash
AWS_REGION=us-west-2
AWS_ACCESS_KEY_ID=AKIA...
AWS_SECRET_ACCESS_KEY=...
```

### **Azure Blob/ADLS**
```bash
AZURE_STORAGE_ACCOUNT_NAME=your_account
AZURE_STORAGE_ACCOUNT_KEY=...
```

### **GCP GCS**
```bash
GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account.json
```

### **Synapse Analytics**
```bash
SYNAPSE_STORAGE_ACCOUNT=mysynapsestorage
AZURE_STORAGE_ACCOUNT_NAME=mysynapsestorage
AZURE_STORAGE_ACCOUNT_KEY=...
```

---

## 📚 **Learn More**

- **[Delta Lake Support](delta_lake_support.md)** - Complete Delta-RS guide
- **[Catalog Table Names](catalog_table_names.md)** - Permissions and setup
- **[Examples](../examples/04_features/)** - Interactive notebooks

---

*Last Updated: February 2026*
