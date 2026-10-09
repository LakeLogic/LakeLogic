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

# Writing to databases (dlt)

`dlt_materialization.ipynb` writes contract output into databases through dlt, using the contracts in
[`dlt_targets/`](dlt_targets/):

| Contract | Target |
|---|---|
| `customers_to_database.yaml` | `format: dlt`: the table and its quarantine live in the database (merge on `primary_key`) |
| `customers_lake_and_database.yaml` | Delta in the lake, plus a `secondary_targets` copy in the database |

It runs each section against DuckDB (always) and against Postgres (`PG_HOST`, `PG_USER`, `PG_PASSWORD`) and
Azure SQL / SQL Server (`AZSQL_HOST`, `AZSQL_USER`, `AZSQL_PASSWORD`) when those are set. Sections: merge,
overwrite, the lake-plus-database copy, the run log in the database, and a wrong password that never shows up
in the error. The last cell drops the `lakelogic_dlt_demo` schema.

`make_dlt_notebook.py` rebuilds the notebook. `materialize_to_postgres.py` is the short script version, which
leaves its table in place to look at.
