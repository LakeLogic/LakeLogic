"""Polars / DuckDB (delta-rs): an existing Delta table follows the contract's model.

A declared column the table lacks is added in place; a column the contract dropped is kept; a type
change (delta-rs cannot change a type in place) stops the run and names `lakelogic migrate`, which
rewrites with a strict cast in one commit.
"""

import polars as pl
import pytest
from deltalake import DeltaTable, write_deltalake

from lakelogic.core.materialization import materialize_dataframe
from lakelogic.core.models import DataContract
from lakelogic.core.schema_alignment import SchemaChangeNeedsMigration, delta_table_types, migrate_delta_rs_table


def _contract(fields, strategy="append"):
    return DataContract(
        **{
            "version": "1.0",
            "info": {"title": "orders"},
            "primary_key": ["order_id"],
            "model": {"fields": [{"name": n, "type": t} for n, t in fields]},
            "materialization": {"strategy": strategy, "format": "delta"},
        }
    )


@pytest.fixture()
def table(tmp_path):
    path = (tmp_path / "orders").as_posix()
    write_deltalake(path, pl.DataFrame({"order_id": ["o1"], "code": ["10"], "old_col": ["x"]}).to_arrow())
    return path


@pytest.mark.parametrize("strategy", ["append", "merge"])
def test_a_declared_column_is_added_and_a_dropped_one_kept(table, strategy, caplog):
    contract = _contract([("order_id", "string"), ("code", "string"), ("channel", "string")], strategy)
    df = pl.DataFrame({"order_id": ["o2"], "code": ["20"], "channel": ["web"]})
    materialize_dataframe(df, contract, target_path=table, output_format="delta", engine_name="polars")

    types = delta_table_types(DeltaTable(table))
    assert types["channel"] == "string" and "old_col" in types
    rows = {r["order_id"]: r for r in pl.read_delta(table).to_dicts()}
    assert rows["o2"]["channel"] == "web" and rows["o1"]["old_col"] == "x"


def test_a_type_change_stops_the_run_and_leaves_the_table(table):
    contract = _contract([("order_id", "string"), ("code", "int"), ("old_col", "string")])
    df = pl.DataFrame({"order_id": ["o2"], "code": [20], "old_col": ["y"]}, schema_overrides={"code": pl.Int32})
    version = DeltaTable(table).version()
    with pytest.raises(SchemaChangeNeedsMigration, match="lakelogic migrate"):
        materialize_dataframe(df, contract, target_path=table, output_format="delta", engine_name="polars")
    assert DeltaTable(table).version() == version


def test_migrate_converts_every_row_to_the_contract_type(table):
    out = migrate_delta_rs_table(table, _contract([("order_id", "string"), ("code", "int"), ("old_col", "string")]))
    assert out["changed"] == ["code: string -> int"] and out["previous_version"] == 0
    assert delta_table_types(DeltaTable(table))["code"] == "int"
    assert pl.read_delta(table)["code"].to_list() == [10]


def test_migrate_fails_on_a_value_that_does_not_convert_and_writes_nothing(tmp_path):
    path = (tmp_path / "bad").as_posix()
    write_deltalake(path, pl.DataFrame({"order_id": ["o1", "o2"], "code": ["10", "N/A"]}).to_arrow())
    with pytest.raises(Exception, match="(?i)N/A|invalid"):
        migrate_delta_rs_table(path, _contract([("order_id", "string"), ("code", "int")]))
    assert DeltaTable(path).version() == 0 and pl.read_delta(path)["code"].to_list() == ["10", "N/A"]


def test_duckdb_writes_the_added_column_too(table):
    import duckdb

    contract = _contract([("order_id", "string"), ("code", "string"), ("channel", "string")])
    rel = duckdb.sql("SELECT 'o3' AS order_id, '30' AS code, 'app' AS channel")
    materialize_dataframe(rel, contract, target_path=table, output_format="delta", engine_name="duckdb")
    rows = {r["order_id"]: r for r in pl.read_delta(table).to_dicts()}
    assert rows["o3"]["channel"] == "app" and rows["o1"]["channel"] is None
