"""A `key=value` landing folder becomes a real column — when the contract declares it.

A lake that lands `orders/country=GB/...` carries the country in the PATH, not in the files.
Every reader here produced no `country` column, so a contract that declared the field logged
`schema drift: missing=['country']` on every run, and a `materialization.partition_by:
[country]` was pruned at write time — the layout the data arrived in could not survive being
read, and the write silently landed unpartitioned.

Narrow by design: only a key the contract already DECLARES becomes a column, so a contract
that says nothing about its folders reads exactly as it did before.
"""

from __future__ import annotations

import polars as pl
import pytest

from lakelogic import DataProcessor
from lakelogic.core.processor import DataProcessor as _DP


def _landing(tmp_path, fmt: str):
    root = tmp_path / "orders"
    rows = {"GB": 2, "DE": 1}
    for country, n in rows.items():
        d = root / f"country={country}"
        d.mkdir(parents=True)
        frame = pl.DataFrame(
            {
                "order_id": [f"{country}-{i}" for i in range(n)],
                "amount": [10.0] * n,
            }
        )
        if fmt == "parquet":
            frame.write_parquet(d / "part.parquet")
        else:
            frame.write_csv(d / "part.csv")
    return root


def _contract(root, fmt: str, *, declare: bool):
    fields = [{"name": "order_id", "type": "string"}, {"name": "amount", "type": "float"}]
    if declare:
        fields.append({"name": "country", "type": "string"})
    return {
        "version": "1.0.0",
        "info": {"title": "bronze_orders", "target_layer": "bronze"},
        "dataset": "bronze_orders",
        "model": {"fields": fields},
        "source": {"type": "landing", "path": str(root), "format": fmt},
    }


@pytest.mark.parametrize("engine", ["polars", "duckdb"])
@pytest.mark.parametrize("fmt", ["parquet", "csv"])
def test_a_declared_path_key_arrives_as_a_column_with_the_right_value_per_row(tmp_path, engine, fmt):
    root = _landing(tmp_path, fmt)
    good, _bad = DataProcessor(engine=engine, contract=_contract(root, fmt, declare=True)).run_source()

    assert "country" in good.columns
    # Per ROW, not per read: a single read across both folders cannot say which rows are which.
    by_country = {str(r["order_id"]): str(r["country"]) for r in good.select(["order_id", "country"]).to_dicts()}
    assert by_country == {"GB-0": "GB", "GB-1": "GB", "DE-0": "DE"}


@pytest.mark.parametrize("engine", ["polars", "duckdb"])
def test_a_contract_that_declares_nothing_about_its_folders_is_unchanged(tmp_path, engine):
    # The whole safety argument: no new column, so no new drift and nothing to re-approve.
    root = _landing(tmp_path, "parquet")
    good, _bad = DataProcessor(engine=engine, contract=_contract(root, "parquet", declare=False)).run_source()
    assert "country" not in good.columns


def test_the_path_reader_takes_every_key_and_ignores_what_is_not_one():
    assert _DP._path_key_values("landing/orders/country=GB/dt=2026-09-11/part.parquet") == {
        "country": "GB",
        "dt": "2026-09-11",
    }
    # A bare folder is not a stated key, and neither is a file that happens to contain "=".
    assert _DP._path_key_values("landing/orders/2026/09/part.parquet") == {}
    assert _DP._path_key_values("landing/orders/country=/part.parquet") == {}
