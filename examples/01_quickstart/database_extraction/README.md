# Database Quickstart Tutorial

Time: 10 minutes
Level: Beginner
Goal: Extract data from a SQL database into Delta Lake.

## What You'll Learn

1. Define a database connection in a contract
2. Run a LakeLogic extraction
3. Verify the output in Delta format

## Step 1: Setup Local Database

Use SQLite (included with Python). Run this once to create sample data:

```python
import sqlite3

conn = sqlite3.connect('example.db')
c = conn.cursor()
c.execute('CREATE TABLE IF NOT EXISTS users (id INTEGER, name TEXT, email TEXT)')
c.execute('DELETE FROM users')
c.execute('INSERT INTO users VALUES (1, "Alice", "alice@example.com")')
c.execute('INSERT INTO users VALUES (2, "Bob", "bob@example.com")')
conn.commit()
conn.close()
print("Local database setup complete")
```

## Step 2: Use the Contract

The file users_contract.yaml is already provided in this folder.

## Step 3: Run the Extraction

```bash
lakelogic run --contract users_contract.yaml
```

## Check Your Results

Go to ./data/bronze/users/. You will see:
- _delta_log/ transaction logs
- .parquet files for your data

## Next Steps

Connect to a real database:
- examples/03_data_sources/databases/sqlserver/
- examples/03_data_sources/databases/azure_sql/
- examples/03_data_sources/databases/oracle/

Planned placeholders:
- examples/03_data_sources/databases/postgres/
- examples/03_data_sources/databases/sqlite/
