# Database Examples

Database connector examples organized by database type.

---

## 📁 **Structure**

```
03_databases/
├── postgresql/         # PostgreSQL
├── mysql/              # MySQL / MariaDB
├── sqlserver/          # SQL Server (on-premises)
├── azure_sql/          # Azure SQL Database
├── oracle/             # Oracle Database
├── mongodb/            # MongoDB
├── sqlite/             # SQLite
└── README.md
```

---

## 🚀 **Quick Start by Database**

### **1. PostgreSQL**
```bash
cd 03_databases/postgresql
python postgres_example.py
```

**Use cases:**
- General-purpose relational database
- OLTP workloads
- JSON support

---

### **2. MySQL**
```bash
cd 03_databases/mysql
python mysql_example.py
```

**Use cases:**
- Web applications
- E-commerce
- High read workloads

---

### **3. SQL Server**
```bash
cd 03_databases/sqlserver
python sqlserver_example.py
```

**Use cases:**
- Enterprise applications
- Windows environments
- Business intelligence

---

### **4. Azure SQL Database**
```bash
cd 03_databases/azure_sql
python azure_sql_example.py
```

**Use cases:**
- Cloud-native applications
- Managed database service
- Automatic scaling

---

### **5. Oracle**
```bash
cd 03_databases/oracle
python oracle_example.py
```

**Use cases:**
- Enterprise applications
- Mission-critical workloads
- Large-scale OLTP

---

### **6. MongoDB**
```bash
cd 03_databases/mongodb
python mongodb_example.py
```

**Use cases:**
- Document storage
- Flexible schemas
- Real-time analytics

---

### **7. SQLite**
```bash
cd 03_databases/sqlite
python sqlite_example.py
```

**Use cases:**
- Embedded databases
- Local development
- Small applications

---

## 📦 **Installation**

### **All Databases**
```bash
pip install "lakelogic[all]"
```

### **Individual Databases**
```bash
# PostgreSQL
pip install "lakelogic[postgresql]"

# MySQL
pip install "lakelogic[mysql]"

# SQL Server
pip install "lakelogic[sqlserver]"

# Oracle
pip install "lakelogic[oracle]"

# MongoDB
pip install "lakelogic[mongodb]"

# SQLite (included in Python)
pip install lakelogic
```

---

## 🔐 **Authentication**

### **PostgreSQL / MySQL / SQL Server**
```yaml
source:
  type: database
  connector: postgresql  # or mysql, sqlserver
  connection:
    host: localhost
    port: 5432
    database: mydb
    username: user
    password: ${SECRET:db_password}  # From Key Vault
```

### **Azure SQL Database**
```yaml
source:
  type: database
  connector: azure_sql
  connection:
    server: myserver.database.windows.net
    database: mydb
    # Uses Azure AD authentication automatically
```

### **Oracle**
```yaml
source:
  type: database
  connector: oracle
  connection:
    host: localhost
    port: 1521
    service_name: ORCL
    username: user
    password: ${SECRET:oracle_password}
```

### **MongoDB**
```yaml
source:
  type: database
  connector: mongodb
  connection:
    connection_string: mongodb://localhost:27017
    database: mydb
```

---

## 🎯 **Choosing a Database**

| Use Case | Recommended Database |
|----------|---------------------|
| **General-purpose OLTP** | PostgreSQL, MySQL |
| **Enterprise applications** | SQL Server, Oracle |
| **Cloud-native** | Azure SQL Database |
| **Document storage** | MongoDB |
| **Embedded/local** | SQLite |
| **High read workloads** | MySQL, PostgreSQL |
| **Complex transactions** | Oracle, SQL Server |

---

## 📚 **Documentation**

- **Database Connectors:** `lakelogic/engines/database_connectors.py`
- **Credential Management:** `docs/automatic_credentials.md`
- **Contract Examples:** Each database folder has example contracts

---

## 🔧 **Common Patterns**

### **1. Full Table Extract**
```yaml
source:
  type: database
  connector: postgresql
  query: "SELECT * FROM customers"
```

### **2. Incremental Load**
```yaml
source:
  type: database
  connector: sqlserver
  query: "SELECT * FROM orders WHERE updated_at > '${LAST_RUN_TIME}'"
```

### **3. CDC (Change Data Capture)**
```yaml
source:
  type: database
  connector: azure_sql
  cdc:
    enabled: true
    table: customers
```

---

*Last Updated: February 9, 2026*
