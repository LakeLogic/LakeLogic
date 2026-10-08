# Reading databases

`databases.ipynb` runs one contract file per source from [`contracts/`](contracts/):

| Contract | Source | Load | Needs |
|---|---|---|---|
| `rides_sqlite.yaml` | SQLite file | incremental | nothing |
| `rides_postgres.yaml` | PostgreSQL | incremental | `POSTGRES_URI` |
| `rides_azuresql.yaml` | Azure SQL / SQL Server | incremental, chunked (`fetch_size`) | `AZURE_SQL_URI`, ODBC Driver 18 |
| `rides_azuresql_cdc.yaml` | SQL Server change data capture | cdc (inserts, updates, deletes) | CDC enabled; Azure SQL S3+ |
| `rides_mongodb.yaml` | MongoDB, Atlas, Cosmos DB (MongoDB API), DocumentDB | incremental, server filter | `MONGO_URI` |

Connection strings are environment variables (`path: env:POSTGRES_URI`); a contract never holds a password.
Each contract has its own run log file, which remembers its watermark between runs.

`seed.py` writes the test data only. `make_notebook.py` rebuilds the notebook.
`db_scenarios.py` and `mongo_scenarios.py` are the fuller check suites (`DB=postgres|azuresql`, `ENGINE=polars|duckdb`).
