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
    return SnowflakeAdapter(DataContract(version="1.0.0", dataset="gold_dim_date",
                                         model={"fields": [{"name": "date_key", "type": "int"}]}))


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
