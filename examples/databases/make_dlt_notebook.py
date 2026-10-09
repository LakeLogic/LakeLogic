"""Builds dlt_materialization.ipynb (run once; the notebook is committed)."""

import json
from pathlib import Path

cells = []


def md(t):
    cells.append({"cell_type": "markdown", "metadata": {}, "source": t})


def code(t):
    cells.append({"cell_type": "code", "metadata": {}, "execution_count": None, "outputs": [], "source": t})


md("""# Writing to databases with dlt

LakeLogic can write a contract's output into a relational database (Postgres, SQL Server / Azure SQL,
and any other [dlt destination](https://dlthub.com/docs/dlt-ecosystem/destinations/)) in two ways:

| Contract | What it does |
|---|---|
| [`dlt_targets/customers_to_database.yaml`](dlt_targets/customers_to_database.yaml) | `format: dlt` — the table lives in the database; failing rows go to a quarantine table beside it |
| [`dlt_targets/customers_lake_and_database.yaml`](dlt_targets/customers_lake_and_database.yaml) | `format: delta` in the lake, plus a `secondary_targets` copy in the database |

Each section runs against every database you have configured:

| Database | Set these environment variables | Needs |
|---|---|---|
| DuckDB file | nothing — always runs | `pip install "lakelogic[dlt]"` |
| Postgres | `PG_HOST`, `PG_USER`, `PG_PASSWORD` (database `lakelogic_test`) | `pip install "lakelogic[dlt-postgres]"` |
| Azure SQL / SQL Server | `AZSQL_HOST`, `AZSQL_USER`, `AZSQL_PASSWORD` (database `lakelogic_test`) | `pip install "lakelogic[dlt-mssql]"` + ODBC Driver 18 |

The contracts never hold a password: `dlt_credentials: env:DLT_TARGET_URL`. The notebook builds that URL from
the variables above for each database in turn. In production use `keyvault://vault/secret` the same way.

Everything is written to the schema `lakelogic_dlt_demo`. The last cell drops it.""")

code("""import os
import sys
from pathlib import Path
from urllib.parse import quote_plus

LAKELOGIC_SRC = None   # a local lakelogic checkout, if you are not using the released package
if LAKELOGIC_SRC:
    sys.path.insert(0, LAKELOGIC_SRC)

import polars as pl
import yaml
from loguru import logger

from lakelogic import DataProcessor
from lakelogic.core.models import DataContract
from lakelogic.core.run_log import write_run_log

logger.remove()
pl.Config.set_tbl_rows(10)
Path("data").mkdir(exist_ok=True)
SCHEMA = "lakelogic_dlt_demo"

# Every database we can reach: name -> (dlt destination, connection URL, how to query it back).
TARGETS = {"duckdb": ("duckdb", str(Path("data/dlt_demo.duckdb").absolute()))}
if os.environ.get("PG_HOST"):
    TARGETS["postgres"] = ("postgres",
        f"postgresql://{os.environ['PG_USER']}:{quote_plus(os.environ['PG_PASSWORD'])}"
        f"@{os.environ['PG_HOST']}:5432/lakelogic_test?sslmode=require")
if os.environ.get("AZSQL_HOST"):
    TARGETS["azure_sql"] = ("mssql",
        f"mssql://{os.environ['AZSQL_USER']}:{quote_plus(os.environ['AZSQL_PASSWORD'])}"
        f"@{os.environ['AZSQL_HOST']}:1433/lakelogic_test?driver=ODBC+Driver+18+for+SQL+Server")
print("Writing to:", ", ".join(TARGETS))


def contract_for(path: str, target: str) -> DataContract:
    \"\"\"The contract file, pointed at one database. Only the destination changes.\"\"\"
    destination, url = TARGETS[target]
    os.environ["DLT_TARGET_URL"] = url            # the contract reads env:DLT_TARGET_URL
    doc = yaml.safe_load(Path(path).read_text())
    blocks = [doc["materialization"], doc.get("quarantine") or {}, *doc["materialization"].get("secondary_targets", [])]
    for block in blocks:
        if "dlt_destination" in block:
            block["dlt_destination"] = destination
    return DataContract.model_validate(doc)


def query(target: str, sql: str) -> pl.DataFrame:
    \"\"\"Read straight from the database — no LakeLogic involved — to see what really landed.\"\"\"
    destination, url = TARGETS[target]
    if destination == "duckdb":
        import duckdb
        with duckdb.connect(url) as con:
            return con.sql(sql).pl()
    if destination == "postgres":
        import psycopg2
        con = psycopg2.connect(url)
    else:
        import pyodbc
        con = pyodbc.connect(
            f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={os.environ['AZSQL_HOST']},1433;"
            f"DATABASE=lakelogic_test;UID={os.environ['AZSQL_USER']};PWD={os.environ['AZSQL_PASSWORD']};Encrypt=yes;")
    try:
        cur = con.cursor()
        cur.execute(sql)
        cols = [c[0] for c in cur.description]
        return pl.DataFrame([dict(zip(cols, r)) for r in cur.fetchall()])
    finally:
        con.close()


# Day 1: four customers, one with a negative lifetime value (breaks a rule -> quarantine).
DAY1 = pl.DataFrame({"customer_id": [1, 2, 3, 4], "name": ["Alice", "Bob", "Chidi", "Eve"],
                     "city": ["London", "Leeds", "Lagos", "Paris"], "lifetime_value": [120.0, 80.5, 42.0, -5.0]})
# Day 2: Bob changed, Dana is new.
DAY2 = pl.DataFrame({"customer_id": [2, 5], "name": ["Bob Smith", "Dana"],
                     "city": ["Leeds", "Dublin"], "lifetime_value": [95.0, 10.0]})""")

md("""## 1. The database is the target (`format: dlt`), with merge

Day 1 loads three good customers; Eve breaks `lifetime_value_not_negative` and goes to
`customers_quarantine`. Day 2 merges on `primary_key`: Bob is updated in place and Dana is added —
four rows, no duplicates. `primary_key` also adds the `customer_id_unique` check to the run.""")

code("""for target in TARGETS:
    contract = contract_for("dlt_targets/customers_to_database.yaml", target)
    for day, rows in (("day 1", DAY1), ("day 2", DAY2)):
        proc = DataProcessor(contract=contract, engine="polars")
        good, bad = proc.run(rows)[:2]
        written = proc.materialize(good, bad)
        print(f"{target:9} {day}: {good.height} good -> {written['target']}, {bad.height} quarantined")
    checks = {r["name"]: "pass" if r["passed"] else "FAIL" for r in proc.last_report.get("dataset_rules") or []}
    print(f"{'':9} dataset checks: {checks}")
    print(query(target, f"SELECT customer_id, name, city, lifetime_value FROM {SCHEMA}.customers ORDER BY customer_id"))
    print(query(target, f"SELECT customer_id, name, lifetime_value FROM {SCHEMA}.customers_quarantine"))""")

md("""## 2. Overwrite replaces the table

`strategy: overwrite` maps to dlt's `replace`: after loading day 2 only its two rows remain.""")

code("""for target in TARGETS:
    contract = contract_for("dlt_targets/customers_to_database.yaml", target)
    contract.materialization.strategy = "overwrite"
    proc = DataProcessor(contract=contract, engine="polars")
    good, bad = proc.run(DAY2)[:2]
    proc.materialize(good, bad)
    n = query(target, f"SELECT COUNT(*) AS n FROM {SCHEMA}.customers")["n"][0]
    print(f"{target:9} after overwrite: {n} rows (expected 2)")""")

md("""## 3. Lake first, database copy second (`secondary_targets`)

The main write is a Delta table in `data/lake/`. The same rows are then copied into
`customers_reporting_copy` in the database. Secondary targets used to run only after Delta; they now run
after every format.""")

code("""for target in TARGETS:
    contract = contract_for("dlt_targets/customers_lake_and_database.yaml", target)
    contract.materialization.target_path = f"data/lake/{target}/silver_customers"
    proc = DataProcessor(contract=contract, engine="polars")
    good, bad = proc.run(DAY1.filter(pl.col("lifetime_value") >= 0))[:2]
    written = proc.materialize(good, bad)
    copy = written.get("secondary_writes") or []
    print(f"{target:9} lake: {written.get('rows_written')} rows; copies: {[(c['target'], c.get('rows_written', c.get('error'))) for c in copy]}")
    print(query(target, f"SELECT customer_id, name FROM {SCHEMA}.customers_reporting_copy ORDER BY customer_id"))""")

md("""## 4. The run log in the database

Set `run_log_backend: dlt` and every run's report (counts, timings, status) is a row in the database too.""")

code("""for target in TARGETS:
    contract = contract_for("dlt_targets/customers_to_database.yaml", target)
    contract.metadata = {"run_log_backend": "dlt", "run_log_table": "pipeline_runs",
                         "dlt_destination": TARGETS[target][0], "dlt_credentials": "env:DLT_TARGET_URL",
                         "dlt_dataset_name": SCHEMA}
    proc = DataProcessor(contract=contract, engine="polars")
    proc.run(DAY1)
    write_run_log(proc.last_report, proc.contract, engine_name="polars")
    print(target, query(target, f"SELECT contract, counts_good, counts_quarantined FROM {SCHEMA}.pipeline_runs"))""")

md("""## 5. Safe failure: a wrong password never shows up

A failed write raises a clear error, and the password is removed from the message.""")

code("""import re

for target, (destination, url) in TARGETS.items():
    if destination == "duckdb":
        continue
    good_url = url
    TARGETS[target] = (destination, re.sub(r"(://[^:/@]+:)[^@]+@", r"\\1wrong-password-123@", url))
    try:
        proc = DataProcessor(contract=contract_for("dlt_targets/customers_to_database.yaml", target), engine="polars")
        good, bad = proc.run(DAY2)[:2]
        proc.materialize(good, bad)
    except ValueError as err:
        message = str(err)
        print(f"{target}: failed as expected; password in message: {'wrong-password-123' in message}")
        print("  ", message[:160])
    finally:
        TARGETS[target] = (destination, good_url)""")

md("""## Clean up

Drops the `lakelogic_dlt_demo` schema from each database. Skip this cell to keep the tables and look at them
in your SQL client.""")

code("""for target, (destination, url) in TARGETS.items():
    if destination == "duckdb":
        Path(url).unlink(missing_ok=True)
        continue
    if destination == "postgres":
        import psycopg2
        con = psycopg2.connect(url)
        con.cursor().execute(f"DROP SCHEMA IF EXISTS {SCHEMA} CASCADE")
    else:
        import pyodbc
        con = pyodbc.connect(
            f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={os.environ['AZSQL_HOST']},1433;"
            f"DATABASE=lakelogic_test;UID={os.environ['AZSQL_USER']};PWD={os.environ['AZSQL_PASSWORD']};Encrypt=yes;")
        cur = con.cursor()
        cur.execute(f"SELECT name FROM sys.tables WHERE schema_id = SCHEMA_ID('{SCHEMA}')")
        for (t,) in cur.fetchall():
            cur.execute(f"DROP TABLE {SCHEMA}.[{t}]")
        cur.execute(f"IF SCHEMA_ID('{SCHEMA}') IS NOT NULL DROP SCHEMA {SCHEMA}")
    con.commit()
    con.close()
    print(f"{target}: dropped {SCHEMA}")""")

nb = {
    "cells": cells,
    "metadata": {
        "kernelspec": {"display_name": "Python 3", "language": "python", "name": "python3"},
        "language_info": {"name": "python"},
    },
    "nbformat": 4,
    "nbformat_minor": 5,
}
Path(__file__).with_name("dlt_materialization.ipynb").write_text(json.dumps(nb, indent=1), encoding="utf-8")
print("wrote dlt_materialization.ipynb")
