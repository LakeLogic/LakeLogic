"""A milestone-order rule compares a DATE milestone with a TIMESTAMP one on every engine.

`arrival_date >= created_at` (DATE vs TIMESTAMP) was an error on BigQuery, which does not
compare the two types; the generated rule casts both sides to TIMESTAMP (seen live on GCP).
"""
from __future__ import annotations

import datetime as dt

import pytest

SQL = ("(arrival_date IS NULL) OR (created_at IS NULL) OR "
       "(CAST(arrival_date AS TIMESTAMP) >= CAST(created_at AS TIMESTAMP))")
ROWS = {
    "arrival_date": [dt.date(2026, 1, 3), dt.date(2026, 1, 1), None],
    "created_at": [dt.datetime(2026, 1, 2, 9), dt.datetime(2026, 1, 2, 9), dt.datetime(2026, 1, 2, 9)],
}
EXPECTED = [True, False, True]


def test_polars_evaluates_the_milestone_rule():
    pl = pytest.importorskip("polars")
    out = pl.SQLContext(t=pl.DataFrame(ROWS)).execute(f"SELECT {SQL} AS ok FROM t", eager=True)
    assert out["ok"].to_list() == EXPECTED


def test_duckdb_evaluates_the_milestone_rule():
    duckdb = pytest.importorskip("duckdb")
    pd = pytest.importorskip("pandas")
    t = pd.DataFrame(ROWS)  # noqa: F841 - read by DuckDB by name
    assert [r[0] for r in duckdb.sql(f"SELECT {SQL} AS ok FROM t").fetchall()] == EXPECTED


def test_the_generated_rule_is_the_one_tested_here():
    import inspect

    from lakelogic.core import processor

    assert "CAST({end_col} AS TIMESTAMP) >= CAST({start_col} AS TIMESTAMP)" in inspect.getsource(processor)
