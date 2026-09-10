"""SLOValidator can read Snowflake — a fourth engine, not a duckdb lookalike.

WHY THIS EXISTS. The validator supported Spark, polars and duckdb. Databricks and Fabric
both hand it a live Spark session; Snowflake has none — its runtime drives a
`snowflake.connector` connection — so `run_checks()` returned [] from every check.

AND THE FAILURE WAS SILENT. A check that cannot read yields no result rather than an
error, so the report came back EMPTY AND PASSING: a green tick over nothing measured.
That is the one outcome this class must never produce, which is why the connection gets
a real branch rather than being duck-typed into `duckdb_con` — both expose `.execute()`,
but the duckdb branch scans FILES (`delta_scan(path)`) and Snowflake has no files.
"""

from __future__ import annotations

import pytest

from lakelogic.core.slo import SLOValidator


class _Cursor:
    def __init__(self, owner):
        self.owner = owner
        self.closed = False

    def execute(self, sql):
        self.owner.queries.append(" ".join(sql.split()))
        if self.owner.fail_on and self.owner.fail_on in sql:
            raise RuntimeError("no such column")
        return self

    def fetchone(self):
        return self.owner.rows.pop(0) if self.owner.rows else None

    def close(self):
        self.closed = True
        self.owner.closed_cursors += 1


class _Conn:
    """Just enough `snowflake.connector` to answer one query."""

    def __init__(self, rows=None, fail_on=None):
        self.rows = list(rows or [])
        self.queries = []
        self.fail_on = fail_on
        self.closed_cursors = 0

    def cursor(self):
        return _Cursor(self)


class _Registry:
    domain = "marketplace"
    system = "rideflow"


@pytest.fixture
def validator():
    return SLOValidator(_Registry(), snowflake_con=_Conn())


# ── The safety property, first ──────────────────────────────────────────────
def test_no_engine_is_still_refused():
    """THE INVARIANT. A validator that cannot read anything must not silently pass —
    every check bails out and says so."""
    v = SLOValidator(_Registry())
    assert v._has_engine() is False


def test_a_snowflake_connection_counts_as_an_engine():
    assert SLOValidator(_Registry(), snowflake_con=_Conn())._has_engine() is True


def test_the_other_engines_still_count():
    assert SLOValidator(_Registry(), spark=object())._has_engine() is True
    assert SLOValidator(_Registry(), polars=True)._has_engine() is True
    assert SLOValidator(_Registry(), duckdb_con=object())._has_engine() is True


# ── The query seam ──────────────────────────────────────────────────────────
def test_it_runs_the_query_and_returns_the_row(validator):
    validator.snowflake_con.rows = [("2026-09-09 12:00:00",)]
    assert validator._snowflake_fetchone("SELECT MAX(updated_at) FROM t") == ("2026-09-09 12:00:00",)


def test_no_rows_is_none_not_an_error(validator):
    """An empty table is a real answer — "nothing to measure" — not a failure."""
    assert validator._snowflake_fetchone("SELECT MAX(updated_at) FROM t") is None


def test_the_cursor_is_always_closed(validator):
    validator.snowflake_con.rows = [(1,)]
    validator._snowflake_fetchone("SELECT 1")
    assert validator.snowflake_con.closed_cursors == 1


def test_the_cursor_is_closed_even_when_the_query_raises():
    """A leaked cursor per failed check would exhaust the session on a wide estate."""
    conn = _Conn(fail_on="BROKEN")
    v = SLOValidator(_Registry(), snowflake_con=conn)
    with pytest.raises(RuntimeError):
        v._snowflake_fetchone("SELECT BROKEN FROM t")
    assert conn.closed_cursors == 1


def test_identifiers_are_left_unquoted(validator):
    """Snowflake folds unquoted identifiers to UPPERCASE and the mesh is uppercase
    throughout, so `MAX(updated_at)` resolves to the real `UPDATED_AT`. Quoting them
    here would make every column lookup fail."""
    validator.snowflake_con.rows = [(None,)]
    validator._snowflake_fetchone("SELECT MAX(updated_at) AS latest_ts FROM silver_trips")

    sent = validator.snowflake_con.queries[0]
    assert '"' not in sent
    assert "MAX(updated_at)" in sent


# ── It reads TABLES, not files ──────────────────────────────────────────────
def test_the_source_module_queries_a_table_not_a_path():
    """The duckdb branch scans files (`delta_scan('<path>')`). Snowflake holds the
    medallion in real tables — reusing the file branch is the mistake this guards."""
    import inspect

    src = inspect.getsource(SLOValidator)
    branch = src[src.index("elif self.snowflake_con:"):]
    branch = branch[: branch.index("elif self.duckdb_con:")]

    assert "FROM {table_name}" in branch
    assert "delta_scan" not in branch
    assert "parquet_scan" not in branch


def test_the_row_count_branch_returns_a_named_row():
    """The consumer reads `row[check_field]`. A Spark Row supports that; a DB-API tuple
    does not, and returning one raw raises `TypeError: tuple indices must be integers`
    on the first real row."""
    import inspect

    src = inspect.getsource(SLOValidator.check_row_counts)
    branch = src[src.index("elif self.snowflake_con:"):]
    branch = branch[: branch.index("elif self.duckdb_con:")]

    assert "row = {" in branch
    assert "check_field: result[0]" in branch
    # Indexed defensively, so a run log predating the provenance columns still answers.
    assert "if len(result) > 2 else None" in branch


def test_the_wide_select_falls_back():
    """A run log written before `pipeline_run_id`/`run_id` existed would fail the wide
    SELECT. Letting that surface would turn a correct verdict into "NO DATA"."""
    import inspect

    src = inspect.getsource(SLOValidator.check_row_counts)
    branch = src[src.index("elif self.snowflake_con:"):]
    branch = branch[: branch.index("elif self.duckdb_con:")]

    assert "except Exception:" in branch
    assert branch.count("_snowflake_fetchone") == 2
