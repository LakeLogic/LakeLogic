"""Write a contract's output into Postgres through dlt, then read it back.

Run (PowerShell), from the lakelogic repo:

    $env:PG_HOST = "psql-lakelogic-dev-4e08b8.postgres.database.azure.com"
    $env:PG_USER = "llpgadmin"
    $env:PG_PASSWORD = (az keyvault secret show --vault-name kv-lakelogic-dev-4e08b8 `
                        -n postgres-admin-password --query value -o tsv `
                        --subscription "Visual Studio Premium with MSDN")
    .venv\\Scripts\\python examples\\databases\\materialize_to_postgres.py

The table is left in place (schema ``lakelogic_demo``, table ``customers``) so you can look at it
in any SQL client. Run it again to see ``merge`` update Bob and add Dana instead of duplicating.
"""

import os
from urllib.parse import quote_plus

import polars as pl
import psycopg2

from lakelogic.core.materialization import materialize_dataframe
from lakelogic.core.models import DataContract

url = (
    f"postgresql://{os.environ['PG_USER']}:{quote_plus(os.environ['PG_PASSWORD'])}"
    f"@{os.environ['PG_HOST']}:5432/lakelogic_test?sslmode=require"
)
os.environ["DEMO_PG_URL"] = url  # the contract only names the variable, never the password

contract = DataContract.model_validate(
    {
        "version": "1.0",
        "dataset": "customers",
        "primary_key": ["customer_id"],
        "model": {
            "fields": [
                {"name": "customer_id", "type": "integer"},
                {"name": "name", "type": "string"},
                {"name": "city", "type": "string"},
            ]
        },
        "materialization": {
            "strategy": "merge",
            "format": "dlt",
            "dlt_destination": "postgres",
            "dlt_credentials": "env:DEMO_PG_URL",
            "dlt_dataset_name": "lakelogic_demo",
        },
    }
)

first = pl.DataFrame(
    {"customer_id": [1, 2, 3], "name": ["Alice", "Bob", "Chidi"], "city": ["London", "Leeds", "Lagos"]}
)
second = pl.DataFrame({"customer_id": [2, 4], "name": ["Bob Smith", "Dana"], "city": ["Leeds", "Dublin"]})

for label, rows in (("first load", first), ("second load (merge)", second)):
    result = materialize_dataframe(rows, contract)
    print(f"{label}: {result['rows_written']} rows -> {result['target']}")

con = psycopg2.connect(url)
cur = con.cursor()
cur.execute("SELECT customer_id, name, city FROM lakelogic_demo.customers ORDER BY customer_id")
print("\nlakelogic_demo.customers in Postgres:")
for row in cur.fetchall():
    print("  ", row)
con.close()
