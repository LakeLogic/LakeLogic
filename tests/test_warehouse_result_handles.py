"""The warehouse engines must expose WHERE their results live, not just the rows.

Snowflake and BigQuery push every stage of a contract down into the warehouse:
transforms, schema application, row rules and the good/bad split all run as SQL,
and the accepted and quarantined sets end up as real tables there. Both engines
then called ``fetch_pandas_all()`` / ``to_dataframe()`` and returned only pandas.

That made the final step the scalability cliff: a run that computed correctly on a
billion rows inside the warehouse then had to funnel the entire result through the
driver's memory. And it could not be avoided — the table names were locals, so even
a caller holding the session open had no way to name the tables and read them
server-side.

These tests pin the seam that makes the server-side path possible:

* ``good_table`` / ``bad_table`` survive the call, so a caller can do
  ``CREATE TABLE target AS SELECT * FROM {adapter.good_table}``;
* they are set BEFORE the fetch, so a fetch that dies of the very size the warning
  is about still leaves the results reachable;
* a large fetch warns, and the warning names the escape hatch rather than just
  reporting the problem.
"""

from __future__ import annotations

import sys
import types

import pytest

from lakelogic.core.models import DataContract


def _contract() -> DataContract:
    return DataContract(
        version="1.0.0",
        dataset="customers",
        model={"fields": [{"name": "id", "type": "int"}]},
    )


def _capture_warnings(fn):
    """Run *fn*, returning (result, [warning messages])."""
    from loguru import logger

    records: list[str] = []
    sink_id = logger.add(lambda m: records.append(str(m)), level="WARNING", format="{message}")
    try:
        return fn(), records
    finally:
        logger.remove(sink_id)


# ── Snowflake ────────────────────────────────────────────────────────────────


class _FakeSnowflakeCursor:
    def __init__(self, rows: int):
        self._rows = rows
        self.queries: list[str] = []

    def execute(self, sql: str):
        self.queries.append(sql)
        return self

    def fetchone(self):
        return (self._rows,)

    def fetch_pandas_all(self):
        import pandas as pd

        return pd.DataFrame({"id": []})

    def close(self):
        pass


class _FakeSnowflakeConn:
    def __init__(self, rows: int):
        self._cursor = _FakeSnowflakeCursor(rows)

    def cursor(self):
        return self._cursor


def _snowflake_adapter():
    from lakelogic.engines.snowflake import SnowflakeAdapter

    return SnowflakeAdapter(_contract())


def test_snowflake_counts_before_fetching_so_the_warning_precedes_the_failure():
    """The COUNT must be issued before the SELECT *.

    A warning emitted after ``fetch_pandas_all()`` would be silent in exactly the
    case it exists for: the fetch that runs out of memory never returns to log it.
    """
    adapter = _snowflake_adapter()
    conn = _FakeSnowflakeConn(rows=5_000_000)

    _, warnings = _capture_warnings(lambda: adapter._fetch_dataframe(conn, "LG_GOOD_X"))

    queries = conn._cursor.queries
    assert queries[0].strip().upper().startswith("SELECT COUNT(*)"), queries
    assert any(q.strip().upper().startswith("SELECT *") for q in queries), queries
    assert queries.index([q for q in queries if q.strip().upper().startswith("SELECT *")][0]) > 0

    assert len(warnings) == 1, warnings
    assert "5,000,000" in warnings[0]


def test_snowflake_warning_names_the_escape_hatch_not_just_the_problem():
    """A warning that only reports the cliff, offering nothing to do about it, is
    nagging. It must name the server-side route."""
    adapter = _snowflake_adapter()
    _, warnings = _capture_warnings(
        lambda: adapter._fetch_dataframe(_FakeSnowflakeConn(rows=2_000_000), "LG_GOOD_X")
    )

    msg = warnings[0]
    assert "set_shared_connection" in msg
    assert "good_table" in msg
    assert "LAKELOGIC_SNOWFLAKE_FETCH_WARN_ROWS" in msg


def test_snowflake_small_fetch_is_silent():
    adapter = _snowflake_adapter()
    _, warnings = _capture_warnings(
        lambda: adapter._fetch_dataframe(_FakeSnowflakeConn(rows=10), "LG_GOOD_X")
    )
    assert warnings == []


def test_snowflake_threshold_is_configurable(monkeypatch):
    monkeypatch.setenv("LAKELOGIC_SNOWFLAKE_FETCH_WARN_ROWS", "5")
    adapter = _snowflake_adapter()
    _, warnings = _capture_warnings(
        lambda: adapter._fetch_dataframe(_FakeSnowflakeConn(rows=10), "LG_GOOD_X")
    )
    assert len(warnings) == 1
    assert "currently 5" in warnings[0]


def test_snowflake_count_failure_never_breaks_a_working_run():
    """The advisory is best-effort. If COUNT(*) fails the fetch must still happen —
    a diagnostic must not be able to take down the thing it is diagnosing."""

    class _ExplodingCursor(_FakeSnowflakeCursor):
        def execute(self, sql: str):
            if sql.strip().upper().startswith("SELECT COUNT(*)"):
                raise RuntimeError("count denied")
            return super().execute(sql)

    conn = _FakeSnowflakeConn(rows=0)
    conn._cursor = _ExplodingCursor(0)

    out, warnings = _capture_warnings(
        lambda: _snowflake_adapter()._fetch_dataframe(conn, "LG_GOOD_X")
    )
    assert out is not None
    assert warnings == []


def test_snowflake_exposes_result_handles_as_class_attributes():
    """The names must exist on the adapter even before a run, so callers can rely on
    the attribute rather than probing with getattr."""
    from lakelogic.engines.snowflake import SnowflakeAdapter

    assert hasattr(SnowflakeAdapter, "good_table")
    assert hasattr(SnowflakeAdapter, "bad_table")


def test_snowflake_publishes_handles_before_the_fetch():
    """Set BEFORE fetching: if the fetch dies of the size the warning is about, the
    caller must still be able to reach the results sitting in the warehouse."""
    import inspect

    from lakelogic.engines.snowflake import SnowflakeAdapter

    src = inspect.getsource(SnowflakeAdapter.execute)
    assert src.index("self.good_table = good_table") < src.index("good_df = self._fetch_dataframe")


# ── BigQuery ─────────────────────────────────────────────────────────────────


class _FakeRowIterator:
    def __init__(self, total_rows: int):
        self.total_rows = total_rows
        self.to_dataframe_called = False

    def to_dataframe(self):
        self.to_dataframe_called = True
        import pandas as pd

        return pd.DataFrame({"id": []})


class _FakeBQJob:
    def __init__(self, rows: _FakeRowIterator):
        self._rows = rows

    def result(self):
        return self._rows


class _FakeBQClient:
    def __init__(self, total_rows: int):
        self.rows = _FakeRowIterator(total_rows)

    def query(self, sql, job_config=None):
        return _FakeBQJob(self.rows)


def _bigquery_adapter():
    from lakelogic.engines.bigquery import BigQueryAdapter

    return BigQueryAdapter(_contract())


def test_bigquery_warns_before_materialising_rows():
    """``total_rows`` is known from the job result before any row is converted, so
    the warning costs nothing extra AND lands before the expensive step."""
    adapter = _bigquery_adapter()
    client = _FakeBQClient(total_rows=9_000_000)

    _, warnings = _capture_warnings(lambda: adapter._fetch_dataframe(client, "tmp_good"))

    assert len(warnings) == 1, warnings
    assert "9,000,000" in warnings[0]
    assert client.rows.to_dataframe_called  # still returns the frame


def test_bigquery_warning_names_the_escape_hatch():
    adapter = _bigquery_adapter()
    _, warnings = _capture_warnings(
        lambda: adapter._fetch_dataframe(_FakeBQClient(total_rows=2_000_000), "tmp_good")
    )
    msg = warnings[0]
    assert "good_table" in msg
    assert "_session_id" in msg
    assert "LAKELOGIC_BIGQUERY_FETCH_WARN_ROWS" in msg


def test_bigquery_small_fetch_is_silent():
    adapter = _bigquery_adapter()
    _, warnings = _capture_warnings(
        lambda: adapter._fetch_dataframe(_FakeBQClient(total_rows=10), "tmp_good")
    )
    assert warnings == []


def test_bigquery_threshold_is_configurable(monkeypatch):
    monkeypatch.setenv("LAKELOGIC_BIGQUERY_FETCH_WARN_ROWS", "5")
    adapter = _bigquery_adapter()
    _, warnings = _capture_warnings(
        lambda: adapter._fetch_dataframe(_FakeBQClient(total_rows=10), "tmp_good")
    )
    assert len(warnings) == 1
    assert "currently 5" in warnings[0]


def test_bigquery_exposes_result_handles_as_class_attributes():
    from lakelogic.engines.bigquery import BigQueryAdapter

    assert hasattr(BigQueryAdapter, "good_table")
    assert hasattr(BigQueryAdapter, "bad_table")


def test_bigquery_publishes_handles_before_the_fetch():
    import inspect

    from lakelogic.engines.bigquery import BigQueryAdapter

    src = inspect.getsource(BigQueryAdapter.execute)
    assert src.index("self.good_table = good_table") < src.index("good_df = self._fetch_dataframe")
