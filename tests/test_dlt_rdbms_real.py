"""Materialize to real relational databases through dlt: Postgres and SQL Server / Azure SQL.

Runs only when the connection settings are in the environment (the same names as
``examples/databases/db_scenarios.py``); otherwise every test is skipped:

    PG_HOST / PG_USER / PG_PASSWORD           (database lakelogic_test)
    AZSQL_HOST / AZSQL_USER / AZSQL_PASSWORD  (database lakelogic_test; ODBC Driver 18)

Each test writes to its own schema and drops it afterwards. Credentials reach the contract only
as ``env:VAR`` references, the way a production contract should hold them.
"""

from __future__ import annotations

import os
import re
import uuid
from urllib.parse import quote_plus

import polars as pl
import pytest

from lakelogic.core.materialization import materialize_dataframe
from lakelogic.core.models import DataContract

pytest.importorskip("dlt")

ROWS = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"], "amount": [1.5, 2.5, 3.5]})
CHANGED = pl.DataFrame({"id": [2, 4], "name": ["b2", "d"], "amount": [20.0, 4.0]})


def _postgres():
    if not os.environ.get("PG_HOST"):
        return None
    pytest.importorskip("psycopg2")
    url = (
        f"postgresql://{os.environ['PG_USER']}:{quote_plus(os.environ['PG_PASSWORD'])}"
        f"@{os.environ['PG_HOST']}:5432/lakelogic_test?sslmode=require"
    )
    return {"destination": "postgres", "url": url}


def _azure_sql():
    if not os.environ.get("AZSQL_HOST"):
        return None
    pytest.importorskip("pyodbc")
    url = (
        f"mssql://{os.environ['AZSQL_USER']}:{quote_plus(os.environ['AZSQL_PASSWORD'])}"
        f"@{os.environ['AZSQL_HOST']}:1433/lakelogic_test?driver=ODBC+Driver+18+for+SQL+Server"
    )
    return {"destination": "mssql", "url": url}


TARGETS = {"postgres": _postgres, "azure_sql": _azure_sql}


@pytest.fixture(params=list(TARGETS))
def db(request, monkeypatch):
    target = TARGETS[request.param]()
    if target is None:
        pytest.skip(f"{request.param}: connection settings not in the environment")
    monkeypatch.setenv("LL_TEST_DB_URL", target["url"])
    schema = f"ll_dlt_{uuid.uuid4().hex[:8]}"
    yield {**target, "schema": schema, "name": request.param}
    _drop_schema(target, schema)


def _connect(target):
    if target["destination"] == "postgres":
        import psycopg2

        return psycopg2.connect(target["url"])
    import pyodbc

    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={os.environ['AZSQL_HOST']},1433;DATABASE=lakelogic_test;"
        f"UID={os.environ['AZSQL_USER']};PWD={os.environ['AZSQL_PASSWORD']};Encrypt=yes;"
    )


def _rows(target, schema, table):
    con = _connect(target)
    try:
        cur = con.cursor()
        cur.execute(f'SELECT id, name, amount FROM "{schema}"."{table}" ORDER BY id')
        return [tuple(r) for r in cur.fetchall()]
    finally:
        con.close()


def _drop_schema(target, schema):
    try:
        con = _connect(target)
        cur = con.cursor()
        if target["destination"] == "postgres":
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        else:
            cur.execute(f"SELECT name FROM sys.tables WHERE schema_id = SCHEMA_ID('{schema}')")
            for (t,) in cur.fetchall():
                cur.execute(f'DROP TABLE "{schema}"."{t}"')
            cur.execute(f"IF SCHEMA_ID('{schema}') IS NOT NULL DROP SCHEMA \"{schema}\"")
        con.commit()
        con.close()
    except Exception:  # noqa: BLE001 - cleanup is best-effort
        pass


def _contract(mat, primary_key=None):
    return DataContract.model_validate(
        {
            "version": "1.0",
            "dataset": "customers",
            "model": {
                "fields": [
                    {"name": "id", "type": "integer"},
                    {"name": "name", "type": "string"},
                    {"name": "amount", "type": "double"},
                ]
            },
            **({"primary_key": primary_key} if primary_key else {}),
            "materialization": mat,
        }
    )


def _dlt(db, **extra):
    return {
        "dlt_destination": db["destination"],
        "dlt_credentials": "env:LL_TEST_DB_URL",
        "dlt_dataset_name": db["schema"],
        **extra,
    }


def test_primary_dlt_target_writes_to_the_database_the_contract_names(db, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    r = materialize_dataframe(ROWS, _contract({"strategy": "append", "format": "dlt", **_dlt(db)}))
    assert r["target"] == f"{db['destination']}:{db['schema']}.customers" and r["rows_written"] == 3
    assert _rows(db, db["schema"], "customers") == [(1, "a", 1.5), (2, "b", 2.5), (3, "c", 3.5)]
    assert not list(tmp_path.rglob("*.duckdb"))  # nothing silently landed in a local file


def test_merge_updates_and_inserts_on_the_primary_key(db):
    contract = _contract({"strategy": "merge", "format": "dlt", **_dlt(db)}, primary_key=["id"])
    materialize_dataframe(ROWS, contract)
    materialize_dataframe(CHANGED, contract)
    assert _rows(db, db["schema"], "customers") == [(1, "a", 1.5), (2, "b2", 20.0), (3, "c", 3.5), (4, "d", 4.0)]


def test_overwrite_replaces_the_table(db):
    contract = _contract({"strategy": "overwrite", "format": "dlt", **_dlt(db)})
    materialize_dataframe(ROWS, contract)
    materialize_dataframe(CHANGED, contract)
    assert _rows(db, db["schema"], "customers") == [(2, "b2", 20.0), (4, "d", 4.0)]


@pytest.mark.parametrize("lake_format", ["parquet", "delta"])
def test_secondary_target_runs_after_any_lake_format(db, tmp_path, lake_format):
    r = materialize_dataframe(
        ROWS,
        _contract(
            {
                "strategy": "append",
                "format": lake_format,
                "target_path": str(tmp_path / "lake"),
                "secondary_targets": [
                    {"format": "dlt", "table_name": "customers_copy", "fail_on_error": True, **_dlt(db)}
                ],
            }
        ),
    )
    assert [w["rows_written"] for w in r["secondary_writes"]] == [3]
    assert len(_rows(db, db["schema"], "customers_copy")) == 3


def test_a_failed_write_never_shows_the_password(db, monkeypatch):
    bad = re.sub(r"(://[^:/@]+:)[^@]+@", r"wrong-pass-123@", db["url"])
    assert bad != db["url"]
    monkeypatch.setenv("LL_TEST_DB_URL", bad)
    with pytest.raises(ValueError) as err:
        materialize_dataframe(ROWS, _contract({"strategy": "append", "format": "dlt", **_dlt(db)}))
    assert "wrong-pass-123" not in str(err.value)


def _count(target, schema, table):
    con = _connect(target)
    try:
        cur = con.cursor()
        cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}"')
        return cur.fetchone()[0]
    finally:
        con.close()


def test_quarantined_rows_land_in_the_database(db):
    from lakelogic.core.models import Quarantine
    from lakelogic.core.quarantine import materialize_quarantine

    contract = _contract({"strategy": "append", "format": "parquet", "target_path": "unused"})
    contract.quarantine = Quarantine(
        target="unused",
        format="dlt",
        table="bad_rows",
        dlt_destination=db["destination"],
        dlt_credentials="env:LL_TEST_DB_URL",
        dlt_dataset_name=db["schema"],
    )
    result = materialize_quarantine(ROWS, contract)
    assert result["target"] == f"{db['destination']}:{db['schema']}.bad_rows" and result["rows_written"] == 3
    assert _count(db, db["schema"], "bad_rows") == 3


def test_run_log_lands_in_the_database(db):
    from lakelogic.core.run_log import _write_run_log_table

    contract = _contract({"strategy": "append", "format": "parquet", "target_path": "unused"})
    contract.metadata = {
        "run_log_backend": "dlt",
        "run_log_table": "pipeline_runs",
        "dlt_destination": db["destination"],
        "dlt_credentials": "env:LL_TEST_DB_URL",
        "dlt_dataset_name": db["schema"],
    }
    report = {
        "pipeline_run_id": "p-1",
        "run_id": "r-1",
        "timestamp": "2026-10-09T12:00:00+00:00",
        "contract": "customers",
        "status": "success",
        "counts_good": 3,
    }
    assert (
        _write_run_log_table(report, contract, engine_name="polars")
        == f"{db['destination']}:{db['schema']}.pipeline_runs"
    )
    assert _count(db, db["schema"], "pipeline_runs") == 1
