"""Merge stays inside the warehouse on Snowflake and BigQuery (2026-11-18).

Both engines could write in place, but the processor allowed only append/overwrite/replace
onto that path, so every merge was fetched into pandas and shipped back through dlt. Snowflake
already had MERGE INTO; BigQuery gets one of the same shape.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from lakelogic.core.models import DataContract
from lakelogic.core.processor import DataProcessor


class _Result(list):
    def __init__(self, rows, schema):
        super().__init__(rows)
        self.schema = schema


class _BQClient:
    def __init__(self, cols):
        self.sql: list[str] = []
        self._schema = [SimpleNamespace(name=c) for c in cols]

    def query(self, sql, job_config=None):
        self.sql.append(sql)
        return SimpleNamespace(result=lambda: _Result([[3]], self._schema))


def _bq(cols, primary_key=("id",)):
    from lakelogic.engines.bigquery import BigQueryAdapter

    contract = DataContract(version="1.0.0", dataset="orders", primary_key=list(primary_key),
                            model={"fields": [{"name": c, "type": "string"} for c in cols]})
    a = BigQueryAdapter(contract)
    a.good_table = "lg_good_abc"
    a._session_id = "sess-1"
    client = _BQClient(cols)
    a._get_client = lambda: client  # type: ignore[method-assign]
    return a, client


def test_bigquery_merge_is_one_statement_inside_bigquery():
    a, client = _bq(["id", "name", "amount", "_lakelogic_processed_at"])
    out = a.materialize_native("proj.ds.orders", strategy="merge")
    merge = next(s for s in client.sql if s.startswith("MERGE"))
    assert "ON t.`id` = s.`id`" in merge
    assert "QUALIFY ROW_NUMBER() OVER (PARTITION BY `id`" in merge               # one row per key
    assert "t.`name` IS DISTINCT FROM s.`name`" in merge                          # only changed rows
    assert "_lakelogic_processed_at` IS DISTINCT FROM" not in merge               # not a change
    assert "`_lakelogic_processed_at` = s.`_lakelogic_processed_at`" in merge     # but still refreshed
    assert any(s.startswith("CREATE TABLE IF NOT EXISTS") for s in client.sql)    # target created
    assert out["server_side"] is True and out["strategy"] == "merge"


def test_bigquery_merge_needs_a_primary_key():
    a, client = _bq(["id", "name"], primary_key=())
    with pytest.raises(ValueError, match="primary_key"):
        a.materialize_native("proj.ds.orders", strategy="merge")
    assert not any(s.startswith("MERGE") for s in client.sql)


class _Adapter:
    """A warehouse adapter that declares what it can write in place."""

    def __init__(self, strategies):
        self._NATIVE_WRITE_STRATEGIES = set(strategies)
        self.good_table = "LG_GOOD"
        self.calls = []

    def materialize_native(self, target_table, strategy="overwrite"):
        self.calls.append(strategy)
        return {"target": target_table, "rows_written": 1, "strategy": strategy, "server_side": True}


def _processor(adapter, strategy):
    p = object.__new__(DataProcessor)
    p.adapter = adapter
    p.engine_name = "snowflake"
    p.contract = SimpleNamespace(materialization=SimpleNamespace(strategy=strategy, target_path="table:DB.S.T"))
    return p


def test_merge_goes_native_when_the_engine_declares_it():
    adapter = _Adapter({"append", "overwrite", "replace", "merge", "upsert"})
    out = _processor(adapter, "merge")._try_native_materialize("table:DB.S.T")
    assert out is not None and adapter.calls == ["merge"]


def test_scd2_still_goes_to_the_dlt_path():
    adapter = _Adapter({"append", "overwrite", "replace", "merge", "upsert"})
    assert _processor(adapter, "scd2")._try_native_materialize("table:DB.S.T") is None
    assert adapter.calls == []


def test_both_warehouse_engines_declare_merge():
    from lakelogic.engines.bigquery import BigQueryAdapter
    from lakelogic.engines.snowflake import SnowflakeAdapter

    for engine in (SnowflakeAdapter, BigQueryAdapter):
        assert {"merge", "upsert"} <= engine._NATIVE_WRITE_STRATEGIES
