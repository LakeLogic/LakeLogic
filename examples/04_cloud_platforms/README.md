# Cloud Data Platform Examples

Examples for connecting to and processing data within major cloud data platforms.

---

## 📁 **Structure**

```
04_cloud_platforms/
├── databricks/         # Databricks (Unity Catalog, Delta Lake)
├── synapse/            # Azure Synapse Analytics
├── fabric/             # Microsoft Fabric (OneLake, Lakehouse)
├── snowflake/          # Snowflake Data Cloud
├── bigquery/           # Google Cloud BigQuery
└── README.md
```

---

## 🚀 **Quick Start by Platform**

### **1. Databricks (Interactive)**
**Focus:** Unity Catalog integration and Delta Lake optimization.  
**Notebook:** `04_cloud_platforms/databricks/databricks_unity_catalog.ipynb`

---

### **2. Microsoft Fabric (Interactive)**
**Focus:** OneLake integration and Shortcuts.  
**Notebook:** `04_cloud_platforms/fabric/fabric_lakehouse.ipynb`

---

### **3. Azure Synapse**
**Focus:** Dedicated SQL Pools and Serverless SQL.

```bash
cd 04_cloud_platforms/synapse
python synapse_analytics_example.py
```

---

### **4. Snowflake**
**Focus:** SQL-based ingestion and stage management.

```bash
cd 04_cloud_platforms/snowflake
python snowflake_example.py
```

---

### **5. Google BigQuery**
**Focus:** BigLake and cloud-native warehousing.

```bash
cd 04_cloud_platforms/bigquery
python bigquery_example.py
```

---

## 📦 **Installation**

### **Individual Platforms**
```bash
# Databricks
pip install "lakelogic[databricks]"

# Microsoft Fabric / Synapse
pip install "lakelogic[azure]"

# Snowflake
pip install "lakelogic[snowflake]"

# BigQuery
pip install "lakelogic[bigquery]"
```

---

## 🔐 **Authentication Patterns**

### **Databricks (Personal Access Token)**
```yaml
source:
  type: cloud_platform
  connector: databricks
  connection:
    host: adb-123456789.0.azuredatabricks.net
    http_path: /sql/1.0/endpoints/abcde12345
    access_token: ${SECRET:databricks_token}
```

### **Microsoft Fabric (Service Principal)**
```yaml
source:
  type: cloud_platform
  connector: fabric
  connection:
    workspace_id: your-workspace-id
    lakehouse_id: your-lakehouse-id
    # Uses Azure AD authentication automatically
```

---

## 📚 **Documentation**

- **Cloud Connectors:** `lakelogic/engines/cloud_connectors.py`
- **Unity Catalog Guide:** `docs/unity_catalog_integration.md`
- **OneLake Shortcuts:** `docs/microsoft_fabric_onelake.md`

---

*Last Updated: February 9, 2026*
