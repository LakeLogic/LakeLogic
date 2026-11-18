"""The input is read in its own format, not the output's (2026-11-18).

With no `source` block, `materialization.format: delta` made `run_source("orders.csv")` open the
CSV as a Delta table ("Invalid table location … orders.csv"), found by a fresh-agent test.
"""
from __future__ import annotations

import pytest

from lakelogic.core.processor import _format_from_extension


@pytest.mark.parametrize("path,fmt", [
    ("data/orders.csv", "csv"), ("x.parquet", "parquet"), ("x.jsonl", "ndjson"), ("x.csv.gz", "csv"),
    ("lake/orders_delta", None), ("landing/*.csv", "csv"), ("x.unknown", None),
])
def test_extension_names_the_format(path, fmt):
    assert _format_from_extension(path) == fmt


def test_csv_is_read_as_csv_when_output_is_delta(tmp_path):
    pytest.importorskip("deltalake")
    from lakelogic import DataProcessor

    (tmp_path / "orders.csv").write_text("order_id,amount\n1,10\n2,20\n", encoding="utf-8")
    contract = {
        "version": "1.0.0",
        "info": {"title": "orders"},
        "dataset": "orders",
        "primary_key": ["order_id"],
        "model": {"fields": [{"name": "order_id", "type": "integer"}, {"name": "amount", "type": "integer"}]},
        "materialization": {"strategy": "merge", "format": "delta", "target_path": str(tmp_path / "out")},
    }
    good, bad = DataProcessor(contract, engine="polars").run_source(str(tmp_path / "orders.csv"))
    assert len(good) == 2 and len(bad) == 0
