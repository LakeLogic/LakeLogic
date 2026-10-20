"""``lakelogic check`` — check a table against its contract's rules and report it (#570).

Runs where the data lives, from the client's own scheduler (cron, Airflow, Databricks Jobs,
Fabric pipelines), so no connection from LakeLogic to the data platform is needed. Only counts
leave the table.
"""

from __future__ import annotations

import json
from typing import Optional

import typer


def check(
    contract: str = typer.Argument(..., help="Path to the contract YAML."),
    table: Optional[str] = typer.Option(None, "--table", help="Table to read (default: the contract's info.table_name)."),
    data: Optional[str] = typer.Option(None, "--data", help="A local file to check with DuckDB (parquet / csv) instead of a table."),
    endpoint: Optional[str] = typer.Option(None, "--endpoint", envvar="LAKELOGIC_OBSERVATORY_ENDPOINT",
                                           help="LakeLogic run-log ingest URL. Omitted: print only."),
    api_key: Optional[str] = typer.Option(None, "--api-key", envvar="LAKELOGIC_API_KEY", help="LakeLogic API key."),
    environment: Optional[str] = typer.Option(None, "--environment", help="Environment to report (dev, prod…)."),
    dataset: Optional[str] = typer.Option(None, "--dataset", help="Dataset name to report under (default: the contract's)."),
    results_table: Optional[str] = typer.Option(None, "--results-table", envvar="LAKELOGIC_QUALITY_CHECKS_TABLE",
                                                help="Also append the per-rule results to this table (e.g. catalog.lakelogic.lakelogic_quality_checks)."),
) -> None:
    """Check a table against its contract's rules; send the result to LakeLogic when an endpoint is set."""
    from lakelogic.core.rule_check import check_contract

    kwargs = {}
    target = table
    if data:
        import duckdb

        con = duckdb.connect()
        reader = "read_csv_auto" if data.lower().endswith(".csv") else "read_parquet"
        con.execute(f"CREATE VIEW _checked AS SELECT * FROM {reader}('{data}')")
        kwargs["duckdb_con"] = con
        target = "_checked"
    else:
        try:
            from pyspark.sql import SparkSession

            kwargs["spark"] = SparkSession.builder.getOrCreate()
        except Exception as exc:  # noqa: BLE001
            raise typer.BadParameter(f"No Spark session available ({exc}); pass --data for a local file.")
    out = check_contract(contract, table=target, endpoint=endpoint, api_key=api_key, environment=environment,
                         dataset=dataset, results_table=results_table, **kwargs)
    rows = out["result"]["rule_results"]
    for r in rows:
        tail = f" {r['failed_rows']} of {r['rows_evaluated']} rows fail" if r.get("failed_rows") else ""
        typer.echo(f"{r['status']:>13}  {r['name']}{tail}")
    typer.echo(json.dumps({"rules": len(rows), "failed": sum(1 for r in rows if r['status'] == 'failed'),
                           "sent": out["sent"], "results_table": out["results_table"],
                           "results_table_error": out["results_table_error"]}))
