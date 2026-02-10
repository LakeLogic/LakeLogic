# API & Integration Examples

Examples for connecting to REST APIs, SFTP servers, and other external data sources.

---

## 📁 **Structure**

```
05_apis/
├── rest_api/           # RESTful JSON/XML APIs
├── sftp/               # Secure File Transfer Protocol
├── custom_connector/   # Building your own Python-based connector
└── README.md
```

---

## 🚀 **Quick Start**

### **1. REST API**
**Focus:** Handling authentication (Bearer, API Key) and pagination.

```bash
cd 05_apis/rest_api
python rest_api_example.py
```

---

### **2. SFTP**
**Focus:** Securely downloading and processing CSV/Parquet files from remote servers.

```bash
cd 05_apis/sftp
python sftp_example.py
```

---

## 📦 **Installation**

### **Individual Connectors**
```bash
# REST API (Standard requests)
pip install "lakelogic[api]"

# SFTP (Paramiko)
pip install "lakelogic[sftp]"
```

---

## 🔐 **Example Patterns**

### **Custom API with Bearer Token**
```yaml
source:
  type: api
  connector: rest
  endpoint: "https://api.example.com/v1/data"
  method: GET
  headers:
    Authorization: "Bearer ${SECRET:api_token}"
  pagination:
    type: offset_limit
```

---

*Last Updated: February 9, 2026*
