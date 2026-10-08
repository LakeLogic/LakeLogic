"""Builds databases.ipynb (run once; the notebook is committed)."""

import json
from pathlib import Path

cells = []


def md(t):
    cells.append({"cell_type": "markdown", "metadata": {}, "source": t})


def code(t):
    cells.append({"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [], "source": t})


md("""# Reading databases with LakeLogic

Every run here is **one contract file** in [`contracts/`](contracts/): where the data is, how to read it
(full, incremental or change data capture), the schema, the rules, and where good and bad rows go.
The notebook only seeds test data and calls `run_source()`.

| Contract | Source | Needs |
|---|---|---|
| `rides_sqlite.yaml` | SQLite file | nothing — runs anywhere |
| `rides_postgres.yaml` | PostgreSQL | `POSTGRES_URI` |
| `rides_azuresql.yaml` | Azure SQL / SQL Server, chunked | `AZURE_SQL_URI`, ODBC Driver 18 |
| `rides_azuresql_cdc.yaml` | SQL Server change data capture | `AZURE_SQL_URI`, CDC enabled (Azure SQL S3+) |
| `rides_mongodb.yaml` | MongoDB / Atlas / Cosmos DB (MongoDB API) / DocumentDB | `MONGO_URI` |

Connection strings (passwords included) are **environment variables**: a contract says `path: env:POSTGRES_URI`
and never holds a secret. A section whose variable is not set is skipped.

Each source holds the same data: 200 good rides, 10 with a negative fare, 5 with no city. Then 20 new rides
arrive and 5 are refunded. An incremental contract must read 215 rows, then only those 25, then nothing.

Needs `pip install "lakelogic[polars]" deltalake`, plus `psycopg2-binary` (Postgres), `pyodbc` (SQL Server),
`pymongo` (MongoDB). Local MongoDB: `docker run -d --name lakelogic-mongo-dev -p 27017:27017 mongo:7`.""")

code("""import os
import shutil
import sys
from pathlib import Path

LAKELOGIC_SRC = None   # a local lakelogic checkout, if you are not using the released package
if LAKELOGIC_SRC:
    sys.path.insert(0, LAKELOGIC_SRC)
ENGINE = os.environ.get("ENGINE", "polars")   # polars | duckdb | spark
os.environ.setdefault("MONGO_URI", "mongodb://localhost:27017")

import polars as pl
from loguru import logger

from lakelogic import DataProcessor
from lakelogic.core.run_log import write_run_log
import seed   # test data only — LakeLogic reads through the contract

logger.remove()
pl.Config.set_tbl_rows(8)
pl.Config.set_tbl_width_chars(160)
shutil.rmtree("data", ignore_errors=True)   # a clean start: no run log, no bronze
Path("data").mkdir()


def as_polars(df):
    if isinstance(df, pl.DataFrame):
        return df
    return df.pl() if hasattr(df, "pl") else pl.from_pandas(df.toPandas())


def run(contract: str):
    \"\"\"What a scheduled pipeline does with one contract: read, validate, write, log.\"\"\"
    proc = DataProcessor(engine=ENGINE, contract=contract)
    good, bad = proc.run_source()[:2]
    proc.materialize(good, bad)
    write_run_log(proc.last_report, proc.contract, engine_name=ENGINE)  # remembers the watermark
    good, bad = as_polars(good), as_polars(bad)
    reasons = (bad["_lakelogic_errors"].explode().value_counts(sort=True)
               if bad.height and "_lakelogic_errors" in bad.columns else None)
    print(f"read {good.height + bad.height} rows: {good.height} good, {bad.height} quarantined")
    if reasons is not None:
        print(reasons)
    return good, bad


def show(contract: str):
    print(Path(contract).read_text())""")

md("""## SQLite — incremental by `updated_at`

The contract:""")
code('show("contracts/rides_sqlite.yaml")')
code("""seed.seed("sqlite")
good, bad = run("contracts/rides_sqlite.yaml")      # first run: everything
good.head(3)""")
code("""seed.changes("sqlite")
good, bad = run("contracts/rides_sqlite.yaml")      # 20 new + 5 refunded
good.filter(pl.col("status") == "refunded")""")
code("""run("contracts/rides_sqlite.yaml");                 # nothing changed: nothing read""")

md("""## PostgreSQL

Set `POSTGRES_URI`, e.g. `postgresql://user:password@host:5432/lakelogic_test?sslmode=require`.""")
code('show("contracts/rides_postgres.yaml")')
code("""if os.environ.get("POSTGRES_URI"):
    seed.seed("postgres")
    run("contracts/rides_postgres.yaml")
    seed.changes("postgres")
    run("contracts/rides_postgres.yaml")
    run("contracts/rides_postgres.yaml")
else:
    print("POSTGRES_URI not set — skipped")""")

md("""## Azure SQL / SQL Server — chunked reads

Set `AZURE_SQL_URI`, e.g. `mssql://user:password@server.database.windows.net:1433/lakelogic_test?encrypt=true`.
`fetch_size` reads the table in chunks.""")
code('show("contracts/rides_azuresql.yaml")')
code("""if os.environ.get("AZURE_SQL_URI"):
    seed.seed("azuresql")
    run("contracts/rides_azuresql.yaml")
    seed.changes("azuresql")
    run("contracts/rides_azuresql.yaml")
    run("contracts/rides_azuresql.yaml")
else:
    print("AZURE_SQL_URI not set — skipped")""")

md("""## SQL Server change data capture

CDC reads the database's own change log, so it sees **deletes** too, and needs no `updated_at` column.
Each row carries `_lakelogic_cdc_op` (insert / update / delete). Azure SQL needs the S3 tier or higher,
so this section only runs when `RUN_CDC=1`: enable CDC on `rides` first (see the contract's header).""")
code('show("contracts/rides_azuresql_cdc.yaml")')
code("""if os.environ.get("AZURE_SQL_URI") and os.environ.get("RUN_CDC") == "1":
    good, bad = run("contracts/rides_azuresql_cdc.yaml")
    print(good["_lakelogic_cdc_op"].value_counts())
else:
    print("RUN_CDC=1 and AZURE_SQL_URI needed — skipped")""")

md("""## MongoDB (also Atlas, Cosmos DB's MongoDB API, DocumentDB)

`dataset` is the collection. Nested `customer.name` becomes `customer_name`; the `items` array stays JSON text.
`options.filter` is a MongoDB query the server applies. One document has `fare: "abc"` and one has no
customer — both are quarantined, with the reason.""")
code('show("contracts/rides_mongodb.yaml")')
code("""try:
    seed.seed("mongodb")
except Exception as exc:
    print(f"MongoDB not reachable at {os.environ['MONGO_URI']} — skipped ({type(exc).__name__})")
else:
    good, bad = run("contracts/rides_mongodb.yaml")
    display(good.select("ride_id", "customer_name", "customer_tier", "fare", "items").head(3))
    seed.changes("mongodb")
    run("contracts/rides_mongodb.yaml")
    run("contracts/rides_mongodb.yaml")""")

nb = {
    "cells": cells,
    "metadata": {"kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
                 "language_info": {"name": "python"}},
    "nbformat": 4,
    "nbformat_minor": 5,
}
for c in nb["cells"]:
    c["source"] = c["source"].splitlines(keepends=True)
Path(__file__).with_name("databases.ipynb").write_text(json.dumps(nb, indent=1), encoding="utf-8")
print("wrote databases.ipynb")
