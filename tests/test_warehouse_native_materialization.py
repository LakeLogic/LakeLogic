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
    #: A result's columns: (name, type_code, ..., precision, scale). Target and source alike.
    description = [("ID", 0, None, None, 38, 0, True), ("NAME", 2, None, None, None, None, True)]
    rowcount = 2

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
    """scd2 is not implemented. Silently writing an append instead would be a
    DATA error — the wrong rows in the target — not a slow path."""
    with pytest.raises(NotImplementedError) as exc:
        _sf_adapter().materialize_native("T", strategy="scd2")
    assert "scd2" in str(exc.value)
    assert sf_shared.sql == []  # nothing was written


def test_snowflake_merge_upserts_on_the_primary_key(sf_shared):
    """A merge contract was overwritten on Snowflake (write_pandas overwrite=True), 2026-11-02."""
    from lakelogic.engines.snowflake import SnowflakeAdapter

    a = SnowflakeAdapter(
        DataContract(
            version="1.0.0", dataset="orders", primary_key=["id"], model={"fields": [{"name": "id", "type": "int"}]}
        )
    )
    a.good_table = "LG_GOOD_X"
    a.materialize_native("ANALYTICS.ORDERS", strategy="merge")
    merge = next(s for s in sf_shared.sql if s.startswith("MERGE INTO"))
    assert 'ON t."ID" = s."ID"' in merge and 'UPDATE SET t."NAME" = s."NAME"' in merge
    assert 'QUALIFY ROW_NUMBER() OVER (PARTITION BY "ID"' in merge  # one row per key
    assert 'INSERT ("ID", "NAME")' in merge


def test_snowflake_adds_a_column_the_target_lacks(sf_shared, monkeypatch):
    from lakelogic.engines.snowflake import SnowflakeAdapter

    cols = iter([_SFCursor.description, _SFCursor.description[:1]])  # source, then target

    class Cur(_SFCursor):
        def execute(self, sql, *a, **kw):
            self.log.append(sql)
            if "LIMIT 0" in sql and not sql.startswith("CREATE"):
                self.description = next(cols)
            return self

    monkeypatch.setattr(sf_shared, "cursor", lambda: Cur(sf_shared.sql))
    a = _sf_adapter()
    a.write_in_place(sf_shared, "SELECT * FROM LG_GOOD_X", "ANALYTICS.ORDERS", "append")
    assert any(s.startswith('ALTER TABLE ANALYTICS.ORDERS ADD COLUMN "NAME" VARCHAR') for s in sf_shared.sql)


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


def test_known_columns_save_the_source_lookup(sf_shared):
    """The runtime already knows its columns; the write used to look them up again (2026-11-02)."""
    a = _sf_adapter()
    a.write_in_place(
        sf_shared,
        "SELECT * FROM LG_GOOD_X",
        "ANALYTICS.ORDERS",
        "append",
        columns=[("ID", "NUMBER(38,0)"), ("NAME", "VARCHAR")],
    )
    assert not any("FROM (SELECT * FROM LG_GOOD_X) LIMIT 0" in s for s in sf_shared.sql)
    assert sum(1 for s in sf_shared.sql if "LIMIT 0" in s) == 1  # the target's columns only


def test_an_existing_target_is_not_created_again(sf_shared):
    _sf_adapter().write_in_place(sf_shared, "SELECT * FROM LG_GOOD_X", "ANALYTICS.ORDERS", "append")
    assert not any(s.startswith("CREATE TABLE") for s in sf_shared.sql)


def test_a_merge_updates_only_rows_whose_business_columns_changed(sf_shared):
    """Every matched row was updated, restamping _lakelogic_processed_at, so an incremental
    reader downstream saw the whole table as new each run (2026-11-02)."""
    a = _sf_adapter()
    a.contract = DataContract(version="1.0.0", dataset="orders", primary_key=["id"],
                              model={"fields": [{"name": "id", "type": "int"}]})
    a.write_in_place(sf_shared, "SELECT * FROM LG_GOOD_X", "ANALYTICS.ORDERS", "merge", merge_keys=["id"],
                     columns=[("ID", "NUMBER"), ("NAME", "VARCHAR"), ("_LAKELOGIC_PROCESSED_AT", "VARCHAR")])
    merge = next(s for s in sf_shared.sql if s.startswith("MERGE INTO"))
    assert 'WHEN MATCHED AND (t."NAME" IS DISTINCT FROM s."NAME") THEN UPDATE' in merge
    assert 'IS DISTINCT FROM s."_LAKELOGIC_PROCESSED_AT"' not in merge


def _incremental_adapter(target):
    from lakelogic.engines.snowflake import SnowflakeAdapter

    a = SnowflakeAdapter(DataContract(
        version="1.0.0", dataset="facts", model={"fields": [{"name": "id", "type": "int"}]},
        source={"type": "table", "path": "table:DB.SILVER.ORDERS", "load_mode": "incremental",
                "watermark_field": "_lakelogic_processed_at"},
        metadata={"run_log_table": "DB.LAKELOGIC.LAKELOGIC_RUN_LOG"}))
    a.incremental_target = target
    return a


def test_an_incremental_source_reads_only_rows_newer_than_the_target(sf_shared):
    """An append fact re-appended all of silver every run on Snowflake: 400 rows for 100."""
    view = _incremental_adapter('"DB"."GOLD"."FACTS"')._incremental_source(sf_shared, "DB.SILVER.ORDERS")
    assert 'SELECT MAX("_LAKELOGIC_PROCESSED_AT") FROM "DB"."GOLD"."FACTS"' in sf_shared.sql
    made = next(s for s in sf_shared.sql if s.startswith("CREATE OR REPLACE TEMP VIEW"))
    assert view in made and "FROM DB.SILVER.ORDERS WHERE TO_VARCHAR(\"_LAKELOGIC_PROCESSED_AT\") > '42'" in made


def test_a_full_source_or_an_unknown_target_reads_everything(sf_shared):
    assert _incremental_adapter(None)._incremental_source(sf_shared, "DB.SILVER.ORDERS") == "DB.SILVER.ORDERS"
    full = _incremental_adapter('"DB"."GOLD"."FACTS"')
    full.contract.source.load_mode = "full"
    assert full._incremental_source(sf_shared, "DB.SILVER.ORDERS") == "DB.SILVER.ORDERS"
    assert sf_shared.sql == []


def test_a_missing_target_is_a_first_run(sf_shared, monkeypatch):
    def boom(self, sql, *a, **kw):
        raise RuntimeError("Object 'DB.GOLD.FACTS' does not exist")

    monkeypatch.setattr(_SFCursor, "execute", boom)
    a = _incremental_adapter('"DB"."GOLD"."FACTS"')
    assert a._incremental_source(sf_shared, "DB.SILVER.ORDERS") == "DB.SILVER.ORDERS"


def test_a_landing_source_is_never_filtered(sf_shared):
    """Bronze reads a landing table with no watermark column; filtering it failed every bronze
    contract with "invalid identifier '_LAKELOGIC_PROCESSED_AT'" (2026-11-02)."""
    a = _incremental_adapter('"DB"."BRONZE"."ORDERS"')
    a.contract.source.type = "landing"
    a.contract.source.path = "@DB.FILES.LANDING/orders"
    assert a._incremental_source(sf_shared, "DB.FILES.LL_LANDING_ORDERS") == "DB.FILES.LL_LANDING_ORDERS"
    assert sf_shared.sql == []
