"""SCD2 on the polars/delta-rs path: typed intervals, versions in time order, no false warnings.

A fresh-agent test (2026-11-18) found `effective_from` / `effective_to` stored as TEXT in two
shapes ("1900-01-01" and "2026-02-01T00:00:00"), `_version` numbered against time because it
sorted that text, and six "in the table but not in the contract" warnings on every load after
the first, for columns SCD2 itself writes.
"""

from __future__ import annotations

import pytest

pl = pytest.importorskip("polars")
pytest.importorskip("deltalake")


def _contract(target: str) -> dict:
    return {
        "version": "1.0.0",
        "dataset": "customers",
        "info": {"title": "Customers", "target_layer": "silver"},
        "primary_key": ["customer_id"],
        "model": {
            "fields": [
                {"name": "customer_id", "type": "integer"},
                {"name": "name", "type": "string"},
                {"name": "address", "type": "string"},
                {"name": "updated_at", "type": "timestamp"},
            ]
        },
        "materialization": {
            "strategy": "scd2",
            "format": "delta",
            "target_path": target,
            "scd2": {"track_columns": ["name", "address"], "timestamp_field": "updated_at"},
        },
    }


def _load(tmp_path, name: str, rows: str):
    from lakelogic import DataProcessor

    (tmp_path / name).write_text("customer_id,name,address,updated_at\n" + rows, encoding="utf-8")
    proc = DataProcessor(_contract(str(tmp_path / "dim")), engine="polars")
    result = proc.run_source(str(tmp_path / name))
    proc.materialize(result.good, result.bad)


def test_two_loads_give_typed_ordered_history_without_false_warnings(tmp_path):
    from loguru import logger

    warnings: list = []
    sink = logger.add(lambda m: warnings.append(m.record["message"]), level="WARNING")
    try:
        _load(tmp_path, "c1.csv", "1,Ann,1 High St,2026-01-01T00:00:00\n2,Bob,2 Low Rd,2026-01-01T00:00:00\n")
        _load(tmp_path, "c2.csv", "1,Ann,9 New Ave,2026-02-01T00:00:00\n2,Bob,2 Low Rd,2026-02-01T00:00:00\n")
    finally:
        logger.remove(sink)

    dim = pl.read_delta(str(tmp_path / "dim")).sort(["customer_id", "_version"])
    assert dim.schema["effective_from"] == pl.Datetime("us")
    assert dim.schema["effective_to"] == pl.Datetime("us")
    ann = dim.filter(pl.col("customer_id") == 1)
    assert ann["address"].to_list() == ["1 High St", "9 New Ave"]  # version 1 is the oldest
    assert ann["is_current"].to_list() == [False, True]
    assert not [w for w in warnings if "not in the contract" in w], warnings
