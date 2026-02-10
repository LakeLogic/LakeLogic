# Database Quickstart Tutorial

**Time:** 10 minutes  
**Level:** Beginner  
**Goal:** Extract data from a SQL database into Delta Lake.

---

## 🎯 **What You'll Learn**

1. Define a database connection in a contract.
2. Run a LakeLogic extraction.
3. Verify the output in Delta Lake format.

---

## 🚀 **Step 1: Setup Local Database**

For this tutorial, we will use **SQLite** (included with Python) so you don't need to install a database server.

Create a file called `setup_db.py` to create some sample data:

```python
import sqlite3

conn = sqlite3.connect('example.db')
c = conn.cursor()
c.execute('CREATE TABLE IF NOT EXISTS users (id INTEGER, name TEXT, email TEXT)')
c.execute('INSERT INTO users VALUES (1, "Alice", "alice@example.com")')
c.execute('INSERT INTO users VALUES (2, "Bob", "bob@example.com")')
conn.commit()
conn.close()
print("✅ Local database setup complete!")
```

---

## 📝 **Step 2: Create Your Contract**

Create `database_contract.yaml`:

```yaml
version: 1.0.0
dataset: local_users

source:
  type: database
  connector: sqlite
  connection:
    database: example.db
  query: "SELECT * FROM users"

model:
  fields:
    - name: id
      type: integer
    - name: name
      type: string
    - name: email
      type: string

materialization:
  bronze:
    enabled: true
    path: ./data/bronze/users/
    format: delta
```

---

## ▶️ **Step 3: Run the Extraction**

Create `run_extraction.py`:

```python
from lakelogic.core.data_processor import DataProcessor
import yaml

# Load the contract
with open('database_contract.yaml', 'r') as f:
    contract = yaml.safe_load(f)

# Run LakeLogic
processor = DataProcessor(contract)
processor.run()

print("✅ Data extracted successfully to ./data/bronze/users/")
```

---

## 🎉 **Check Your Results**

Go to your `./data/bronze/users/` folder. You will see:
- `_delta_log/` - Transaction logs.
- `.parquet` files - Your data.

---

## 🚀 **Next Steps**

Connect to a real database:
- **PostgreSQL:** `examples/03_databases/postgresql/`
- **SQL Server:** `examples/03_databases/sqlserver/`
- **Azure SQL:** `examples/03_databases/azure_sql/`

---

*Last Updated: February 9, 2026*
