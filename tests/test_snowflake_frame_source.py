"""Snowflake runs a contract whose source is a frame, not a table: what external logic hands
over (a generated date dimension). Before 2026-11-01 the engine refused it with "Snowflake
adapter requires a source table name", so gold_dim_date could not run on Snowflake."""

from __future__ import annotations

import sys
import types

import pandas as pd
import polars as pl
import pytest

from lakelogic.core.models import DataContract
from lakelogic.engines.snowflake import SnowflakeAdapter, _as_pandas


def _adapter() -> SnowflakeAdapter:
    return SnowflakeAdapter(
        DataContract(version="1.0.0", dataset="gold_dim_date", model={"fields": [{"name": "date_key", "type": "int"}]})
    )


def test_frames_of_every_kind_become_pandas():
    assert list(_as_pandas(pl.DataFrame({"a": [1]})).columns) == ["a"]
    assert list(_as_pandas(pl.DataFrame({"a": [1]}).lazy()).columns) == ["a"]
    assert isinstance(_as_pandas(pd.DataFrame({"a": [1]})), pd.DataFrame)
    assert _as_pandas("RAW.TABLE") is None and _as_pandas(None) is None


def test_a_frame_is_loaded_into_a_temporary_table(monkeypatch):
    calls = {}

    def write_pandas(conn, frame, name, **kw):
        calls.update(name=name, rows=len(frame), **kw)
        return True, 1, len(frame), None

    mod = types.ModuleType("snowflake.connector.pandas_tools")
    mod.write_pandas = write_pandas
    monkeypatch.setitem(sys.modules, "snowflake.connector.pandas_tools", mod)

    name = _adapter()._upload_frame(object(), pd.DataFrame({"date_key": [20260101, 20260102]}))
    assert name.startswith("LG_SRC_") and calls["name"] == name and calls["rows"] == 2
    assert calls["table_type"] == "temporary" and calls["quote_identifiers"] is False


def test_neither_a_table_nor_a_frame_is_still_refused():
    with pytest.raises(ValueError, match="source table name or a DataFrame"):
        _adapter().execute(None)


def test_a_snowflake_log_table_is_widened_once_per_process():
    """936 of a 26-contract run's 1,600 statements were the same ADD COLUMN checks (2026-11-01)."""
    from lakelogic.core import run_log

    class Cur:
        def __init__(self):
            self.sql = []

        def execute(self, q):
            self.sql.append(q)

    run_log._SNOWFLAKE_TABLES_READY.discard("db.lakelogic.t")
    cur = Cur()
    cols = [("a", "VARCHAR"), ("b", "FLOAT")]
    run_log._ensure_snowflake_table(cur, "DB.LAKELOGIC.T", cols)
    run_log._ensure_snowflake_table(cur, "db.lakelogic.t", cols)
    assert len(cur.sql) == 3  # CREATE + 2 ALTERs, then nothing


def test_backtick_table_names_are_read_as_snowflake_names():
    """Shared contracts link `rideflow_dev_reference`.gold.x; Snowflake has no backticks (2026-11-02)."""
    from lakelogic.engines.snowflake import _table_name

    assert _table_name("`rideflow_dev_reference`.gold.gold_dim_city") == "rideflow_dev_reference.gold.gold_dim_city"
    assert _table_name('"DB"."S"."T"') == '"DB"."S"."T"'
    assert _adapter()._resolve_source_table("table:`db`.s.t") == "db.s.t"


def test_the_dataset_view_is_made_only_when_its_sql_names_it():
    from lakelogic.engines.snowflake import SnowflakeAdapter

    plain = _adapter()
    assert not plain._sql_mentions("gold_dim_date")
    named = SnowflakeAdapter(
        DataContract(
            version="1.0.0",
            dataset="silver_trips",
            model={"fields": [{"name": "id", "type": "int"}]},
            transformations=[{"phase": "post", "sql": "SELECT * FROM silver_trips WHERE id > 0"}],
        )
    )
    assert named._sql_mentions("silver_trips")


def test_a_caller_can_keep_results_in_the_warehouse():
    from lakelogic.core.processor import DataProcessor

    proc = DataProcessor.__new__(DataProcessor)
    proc.adapter = _adapter()
    proc.adapter.keep_results_in_warehouse = True
    proc._set_defer_fetch(False, None)
    assert proc.adapter.defer_fetch is True
