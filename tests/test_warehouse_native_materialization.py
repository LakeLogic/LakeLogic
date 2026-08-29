"""Warehouse engines must be able to write results WITHOUT a driver round trip.

Snowflake and BigQuery push every stage of a contract down into the warehouse, so
the accepted rows end up as a table there. The default materialization path then
does: warehouse -> fetch_pandas_all()/to_dataframe() -> pandas -> dlt -> warehouse.
The data is dragged out through the driver and pushed straight back to where it
came from, bounded by driver memory the whole way.

``materialize_native()`` writes it in place with a CTAS/INSERT from the result
table, so the rows never leave the warehouse. These tests pin the SQL it emits, and
pin the two ways it must refuse rather than write something wrong.
"""

from __future__ import annotations

import pytest

from lakelogic.core.models import DataContract


def _contract() -> DataContract:
    return DataContract(
        version="1.0.0",
        dataset="orders",
        model={"fields": [{"name": "id", "type": "int"}]},
    )


# ── Snowflake ────────────────────────────────────────────────────────────────


class _SFCursor:
    def __init__(self, log):
        self.log = log

    def execute(self, sql, *a, **kw):
        self.log.append(sql)
        return self

    def fetchone(self):
        return (42,)

    def close(self):
        pass


class _SFConn:
    def __init__(self):
        self.sql: list[str] = []

    def cursor(self):
        return _SFCursor(self.sql)

    def commit(self):
        pass


@pytest.fixture
def sf_shared():
    """Install a shared connection and always clear it — the adapter holds it as
    CLASS state, so a leak here would silently reconfigure later tests."""
    from lakelogic.engines.snowflake import SnowflakeAdapter

    conn = _SFConn()
    SnowflakeAdapter.set_shared_connection(conn)
    try:
        yield conn
    finally:
        SnowflakeAdapter.set_shared_connection(None)


def _sf_adapter():
    from lakelogic.engines.snowflake import SnowflakeAdapter

    a = SnowflakeAdapter(_contract())
    a.good_table = "LG_GOOD_ABC123"
    return a


def test_snowflake_overwrite_is_a_server_side_ctas(sf_shared):
    out = _sf_adapter().materialize_native("ANALYTICS.GOLD_ORDERS", strategy="overwrite")

    ctas = [s for s in sf_shared.sql if "CREATE OR REPLACE TABLE" in s]
    assert len(ctas) == 1, sf_shared.sql
    assert "SELECT * FROM LG_GOOD_ABC123" in ctas[0]
    # The point of the exercise: the rows are never named as literals/parameters,
    # because they are never in this process.
    assert out["server_side"] is True
    assert out["rows_written"] == 42


def test_snowflake_append_inserts_rather_than_replacing(sf_shared):
    _sf_adapter().materialize_native("ANALYTICS.GOLD_ORDERS", strategy="append")

    assert any(s.startswith("INSERT INTO") for s in sf_shared.sql), sf_shared.sql
    assert not any("CREATE OR REPLACE" in s for s in sf_shared.sql)


def test_snowflake_refuses_an_unimplemented_strategy(sf_shared):
    """merge/scd2 are not implemented. Silently writing an append instead would be a
    DATA error — the wrong rows in the target — not a slow path."""
    with pytest.raises(NotImplementedError) as exc:
        _sf_adapter().materialize_native("T", strategy="merge")
    assert "merge" in str(exc.value)
    assert sf_shared.sql == []  # nothing was written


def test_snowflake_refuses_without_a_shared_connection():
    """With an engine-owned connection the TEMP result tables are dropped when
    execute() returns. Writing then would produce an EMPTY target and report
    success, so it must raise instead."""
    from lakelogic.engines.snowflake import SnowflakeAdapter

    SnowflakeAdapter.set_shared_connection(None)
    with pytest.raises(RuntimeError) as exc:
        _sf_adapter().materialize_native("T")
    assert "shared connection" in str(exc.value)


def test_snowflake_refuses_before_a_run(sf_shared):
    from lakelogic.engines.snowflake import SnowflakeAdapter

    adapter = SnowflakeAdapter(_contract())  # good_table still None
    with pytest.raises(RuntimeError) as exc:
        adapter.materialize_native("T")
    assert "good_table" in str(exc.value)
    assert sf_shared.sql == []


# ── BigQuery ─────────────────────────────────────────────────────────────────


class _BQJob:
    def __init__(self, rows):
        self._rows = rows

    def result(self):
        return self._rows


class _BQClient:
    def __init__(self):
        self.sql: list[str] = []

    def query(self, sql, job_config=None):
        self.sql.append(sql)
        return _BQJob([[7]])


def _bq_adapter(client):
    from lakelogic.engines.bigquery import BigQueryAdapter

    a = BigQueryAdapter(_contract())
    a.good_table = "lg_good_abc123"
    a._session_id = "sess-1"
    a._get_client = lambda: client  # type: ignore[method-assign]
    return a


def test_bigquery_overwrite_is_a_server_side_ctas():
    client = _BQClient()
    out = _bq_adapter(client).materialize_native("proj.ds.gold_orders", strategy="overwrite")

    ctas = [s for s in client.sql if "CREATE OR REPLACE TABLE" in s]
    assert len(ctas) == 1, client.sql
    assert "SELECT * FROM lg_good_abc123" in ctas[0]
    assert out["server_side"] is True
    assert out["rows_written"] == 7


def test_bigquery_append_inserts_rather_than_replacing():
    client = _BQClient()
    _bq_adapter(client).materialize_native("proj.ds.gold_orders", strategy="append")

    assert any(s.startswith("INSERT INTO") for s in client.sql), client.sql
    assert not any("CREATE OR REPLACE" in s for s in client.sql)


def test_bigquery_refuses_an_unimplemented_strategy():
    client = _BQClient()
    with pytest.raises(NotImplementedError):
        _bq_adapter(client).materialize_native("t", strategy="scd2")
    assert client.sql == []


def test_bigquery_refuses_before_a_run():
    from lakelogic.engines.bigquery import BigQueryAdapter

    client = _BQClient()
    adapter = BigQueryAdapter(_contract())
    adapter._get_client = lambda: client  # type: ignore[method-assign]
    with pytest.raises(RuntimeError) as exc:
        adapter.materialize_native("t")
    assert "good_table" in str(exc.value)
    assert client.sql == []


# ── the property both engines share ──────────────────────────────────────────


def test_neither_engine_converts_the_result_to_pandas(sf_shared):
    """The whole point. If either implementation reached for a dataframe, the round
    trip it exists to remove would still be happening."""
    import ast
    import inspect
    import textwrap

    from lakelogic.engines.bigquery import BigQueryAdapter
    from lakelogic.engines.snowflake import SnowflakeAdapter

    for fn in (SnowflakeAdapter.materialize_native, BigQueryAdapter.materialize_native):
        # Assert on CODE, not prose. Both docstrings NAME fetch_pandas_all/to_dataframe
        # to explain the round trip they remove, so a substring check over the raw
        # source finds the very words it is looking for and fails on the
        # documentation. Parse and drop the docstring, then check what executes.
        tree = ast.parse(textwrap.dedent(inspect.getsource(fn)))
        body = tree.body[0].body
        if (
            body
            and isinstance(body[0], ast.Expr)
            and isinstance(body[0].value, ast.Constant)
            and isinstance(body[0].value.value, str)
        ):
            body = body[1:]  # drop the docstring
        code = "\n".join(ast.unparse(node) for node in body)

        assert "fetch_pandas_all" not in code
        assert "to_dataframe" not in code
        assert "_fetch_dataframe" not in code
