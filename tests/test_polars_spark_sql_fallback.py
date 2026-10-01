"""A transform written in Spark SQL runs on the polars engine.

Contracts are authored in Spark SQL. `to_date(...)` is unsupported by Polars SQL and absent
from DuckDB, so the DuckDB fallback failed too, the transform was skipped, `kpi_date` came out
null and every row of the RideFlow revenue and acquisition marts was quarantined
(2026-10-01). The fallback now translates Spark SQL to DuckDB — only after the SQL as written
has failed, so SQL that already runs is never rewritten.
"""

from datetime import date, datetime

import polars as pl

from lakelogic import DataProcessor


def _contract(sql):
    return {
        "version": "1.0",
        "info": {"title": "revenue daily", "target_layer": "gold"},
        "model": {"fields": [
            {"name": "kpi_date", "type": "date", "required": True},
            {"name": "amount", "type": "double"},
        ]},
        "transformations": [{"phase": "pre", "sql": sql}],
        "quality": {"row_rules": [{"name": "kpi_date_required", "sql": '"kpi_date" IS NOT NULL'}]},
    }


def _rows():
    return pl.DataFrame({
        "created_at": [datetime(2026, 9, 30, 8, 15), datetime(2026, 10, 1, 23, 59)],
        "amount": [12.5, 7.0],
    })


def test_spark_to_date_runs_on_polars():
    sql = "SELECT to_date(created_at) AS kpi_date, CAST(amount AS double) AS amount FROM source"
    result = DataProcessor(_contract(sql), engine="polars").run(_rows())
    assert result.bad.height == 0
    assert sorted(result.good["kpi_date"].to_list()) == [date(2026, 9, 30), date(2026, 10, 1)]


def test_sql_that_already_runs_is_not_rewritten(monkeypatch):
    """Translation would turn CAST(... AS TIMESTAMP) into TIMESTAMPTZ; SQL that runs as
    written must never be translated."""
    import sqlglot

    calls = []
    real = sqlglot.transpile
    monkeypatch.setattr(sqlglot, "transpile", lambda *a, **k: calls.append(a) or real(*a, **k))
    sql = "SELECT CAST(created_at AS DATE) AS kpi_date, amount FROM source"
    result = DataProcessor(_contract(sql), engine="polars").run(_rows())
    assert result.good.height == 2
    assert not [c for c in calls if c and "CAST(created_at AS DATE)" in str(c[0])]
