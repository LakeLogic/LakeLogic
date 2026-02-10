# Database CDC (Change Data Capture) Guide

Complete guide for extracting data from databases using LakeLogic's CDC connectors with automatic credential resolution.

---

## 🎯 **Overview**

LakeLogic provides automatic CDC extraction from:
- **Azure SQL Database** (Azure AD authentication)
- **PostgreSQL** (Azure, AWS RDS, GCP Cloud SQL)
- **MySQL** (Azure, AWS RDS, GCP Cloud SQL)
- **MongoDB** (Azure Cosmos DB, MongoDB Atlas)

**Features:**
- ✅ Automatic credential resolution (Azure AD, IAM, etc.)
- ✅ Full table extraction
- ✅ Incremental extraction (watermark-based)
- ✅ Schema detection
- ✅ Connection pooling
- ✅ Retry logic

---

## 📦 **Installation**

```bash
# All database connectors
pip install "lakelogic[databases]"

# Specific databases
pip install "lakelogic[azuresql]"      # Azure SQL
pip install "lakelogic[postgresql]"    # PostgreSQL
pip install "lakelogic[mysql]"         # MySQL
pip install "lakelogic[mongodb]"       # MongoDB
```

---

## 🔷 **Azure SQL Database**

### **Automatic Azure AD Authentication**

```python
from lakelogic.engines.database_connectors import AzureSQLConnector

# Azure AD authentication (automatic!)
# Just run: az login
connector = AzureSQLConnector(
    server="myserver.database.windows.net",
    database="production_db"
)

# Extract data
df = connector.extract_full("dbo.customers")
print(f"Extracted {len(df)} customers")

connector.close()
```

### **SQL Authentication (Manual)**

```python
connector = AzureSQLConnector(
    server="myserver.database.windows.net",
    database="production_db",
    username="admin",
    password="...",
    auto_resolve_credentials=False
)
```

### **Incremental CDC Extraction**

```python
# First run - full extraction
df_full = connector.extract_full("dbo.orders")
max_watermark = df_full["updated_at"].max()

# Subsequent runs - incremental only
df_incremental = connector.extract_incremental(
    table="dbo.orders",
    watermark_column="updated_at",
    last_watermark=max_watermark
)

print(f"Extracted {len(df_incremental)} new/updated records")
```

### **Schema Detection**

```python
schema = connector.get_schema("dbo.customers")
print(schema)
# {'customer_id': 'int', 'name': 'nvarchar', 'created_at': 'datetime2'}
```

---

## 🐘 **PostgreSQL**

### **Azure PostgreSQL with Azure AD**

```python
from lakelogic.engines.database_connectors import PostgreSQLConnector

# Azure AD authentication (automatic for Azure PostgreSQL!)
connector = PostgreSQLConnector(
    host="myserver.postgres.database.azure.com",
    database="production_db"
)

df = connector.extract_full("public.orders")
```

### **AWS RDS PostgreSQL**

```python
connector = PostgreSQLConnector(
    host="mydb.abc123.us-west-2.rds.amazonaws.com",
    database="production_db",
    username="admin",
    password="..."
)
```

### **GCP Cloud SQL PostgreSQL**

```python
connector = PostgreSQLConnector(
    host="10.1.2.3",  # Private IP
    database="production_db",
    username="postgres",
    password="..."
)
```

### **Incremental Extraction**

```python
df_incremental = connector.extract_incremental(
    table="public.orders",
    watermark_column="updated_at",
    last_watermark="2026-02-08T00:00:00Z"
)
```

---

## 🔄 **Complete CDC Pipeline**

### **Bronze → Silver Pipeline**

```python
from lakelogic.engines.database_connectors import AzureSQLConnector
from lakelogic.engines.delta_adapter import DeltaAdapter
from lakelogic import DataProcessor

# Step 1: Extract incremental data
connector = AzureSQLConnector(
    server="myserver.database.windows.net",
    database="production_db"
)

df_incremental = connector.extract_incremental(
    table="dbo.orders",
    watermark_column="updated_at",
    last_watermark="2026-02-08T00:00:00Z"
)

print(f"Extracted {len(df_incremental)} incremental records")
connector.close()

# Step 2: Validate with LakeLogic contract
processor = DataProcessor(
    engine="polars",
    contract="contracts/orders_contract.yaml"
)

good_df, bad_df = processor.run(df_incremental)
print(f"Good: {len(good_df)}, Quarantined: {len(bad_df)}")

# Step 3: Write to Bronze layer (Delta Lake)
delta_adapter = DeltaAdapter()

delta_adapter.write(
    df=good_df,
    path="s3://datalake/bronze/orders/",
    mode="append",
    partition_by=["order_date"]
)

# Step 4: MERGE to Silver layer (deduplication)
merge_stats = delta_adapter.merge(
    target_path="s3://datalake/silver/orders/",
    source_df=good_df,
    merge_key="order_id"
)

print(f"Updated: {merge_stats['num_updated']}, Inserted: {merge_stats['num_inserted']}")
```

---

## 📋 **Contract Template for CDC**

```yaml
# azuresql_cdc_contract.yaml
version: 1.0.0
dataset: bronze_orders
description: "CDC from Azure SQL Database"

source:
  type: database
  connector: azuresql
  server: myserver.database.windows.net
  database: production_db
  table: dbo.orders
  
  # CDC configuration
  cdc:
    enabled: true
    watermark_column: updated_at
    watermark_type: timestamp

model:
  fields:
    - name: order_id
      type: integer
    - name: customer_id
      type: integer
    - name: total_amount
      type: decimal
    - name: status
      type: string
    - name: updated_at
      type: timestamp

quality:
  row_rules:
    - name: order_id_not_null
      rule: not_null
      column: order_id
      severity: error
    
    - name: total_amount_positive
      rule: sql
      expression: "total_amount > 0"
      severity: error

materialization:
  good:
    enabled: true
    path: s3://datalake/bronze/orders/
    format: delta
    mode: append
  
  quarantine:
    enabled: true
    path: s3://datalake/quarantine/orders/
    format: delta
    mode: append
```

---

## ⏰ **Scheduled CDC Pipeline**

### **Production Pattern**

```python
def run_cdc_pipeline(
    server: str,
    database: str,
    table: str,
    watermark_column: str,
    contract_path: str,
    bronze_path: str,
    silver_path: str
):
    """
    Scheduled CDC pipeline (run hourly/daily).
    """
    
    # Get last watermark from metadata store
    last_watermark = get_last_watermark(table)  # Your metadata store
    
    # Extract incremental
    connector = AzureSQLConnector(server=server, database=database)
    df = connector.extract_incremental(
        table=table,
        watermark_column=watermark_column,
        last_watermark=last_watermark
    )
    connector.close()
    
    if len(df) == 0:
        return  # No new data
    
    # Validate
    processor = DataProcessor(engine="polars", contract=contract_path)
    good_df, bad_df = processor.run(df)
    
    # Write to Bronze
    delta_adapter = DeltaAdapter()
    delta_adapter.write(df=good_df, path=bronze_path, mode="append")
    
    # MERGE to Silver
    merge_stats = delta_adapter.merge(
        target_path=silver_path,
        source_df=good_df,
        merge_key="order_id"
    )
    
    # Update watermark
    new_watermark = df[watermark_column].max()
    save_watermark(table, new_watermark)  # Your metadata store
    
    return merge_stats


# Schedule with cron or Airflow
# 0 * * * * python run_cdc.py  # Every hour
```

---

## 🔧 **Multi-Table CDC**

```python
tables_config = [
    {
        "table": "dbo.orders",
        "watermark_column": "updated_at",
        "contract": "contracts/orders.yaml",
        "bronze_path": "s3://datalake/bronze/orders/",
        "silver_path": "s3://datalake/silver/orders/",
        "merge_key": "order_id"
    },
    {
        "table": "dbo.customers",
        "watermark_column": "updated_at",
        "contract": "contracts/customers.yaml",
        "bronze_path": "s3://datalake/bronze/customers/",
        "silver_path": "s3://datalake/silver/customers/",
        "merge_key": "customer_id"
    }
]

connector = AzureSQLConnector(server="...", database="...")
delta_adapter = DeltaAdapter()

for config in tables_config:
    # Extract
    df = connector.extract_incremental(
        table=config["table"],
        watermark_column=config["watermark_column"],
        last_watermark="2026-02-08T00:00:00Z"
    )
    
    # Validate
    processor = DataProcessor(engine="polars", contract=config["contract"])
    good_df, bad_df = processor.run(df)
    
    # Write to Bronze
    delta_adapter.write(df=good_df, path=config["bronze_path"], mode="append")
    
    # MERGE to Silver
    delta_adapter.merge(
        target_path=config["silver_path"],
        source_df=good_df,
        merge_key=config["merge_key"]
    )

connector.close()
```

---

## 🚨 **Troubleshooting**

### **Problem: Azure AD authentication fails**

```bash
# Solution: Login with Azure CLI
az login

# Verify login
az account show
```

### **Problem: ODBC driver not found**

```bash
# Windows: Download from Microsoft
# https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

# Linux (Ubuntu/Debian)
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

### **Problem: Connection timeout**

```python
# Increase connection timeout
connector = AzureSQLConnector(
    server="...",
    database="...",
    connection_string="...;Connection Timeout=60;"
)
```

---

## 📚 **Learn More**

- **[Automatic Credentials](automatic_credentials.md)** - Credential resolution guide
- **[Delta Lake Support](delta_lake_support.md)** - Delta Lake operations
- **[Examples](../examples/04_features/)** - Complete examples

---

*Last Updated: February 2026*
