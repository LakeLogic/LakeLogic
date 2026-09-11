"""A DATE partition column names its folder ``col=YYYY-MM-DD`` on every format.

The Parquet (non-Delta) writer partitions through pandas, which has no date dtype, so a polars
``Date`` arrived as a midnight ``Timestamp`` and the folder became
``order_date=2026-09-10_00_00_00`` — while the Delta path wrote ``order_date=2026-09-10`` for
the same contract. Found while proving the Build Centre's derived-date partitioning
(``derive order_date = CAST(order_ts AS DATE)`` + ``partition_by [country_code, order_date]``).
"""

from __future__ import annotations

import os
from datetime import datetime

import polars as pl
import pytest

from lakelogic import DataProcessor


def _contract(target: str, fmt: str) -> dict:
    return {
        "version": "1.0.0",
        "info": {"title": "silver_orders", "target_layer": "silver"},
        "dataset": "silver_orders",
        "model": {"fields": [
            {"name": "order_id", "type": "string"},
            {"name": "country_code", "type": "string"},
            {"name": "order_ts", "type": "timestamp"},
            {"name": "order_date", "type": "date"},
        ]},
        "transformations": [
            {"phase": "pre", "derive": {"field": "order_date", "sql": "CAST(order_ts AS DATE)"}},
        ],
        "materialization": {"strategy": "append", "format": fmt,
                            "partition_by": ["country_code", "order_date"], "target_path": target},
    }


DATA = pl.DataFrame({
    "order_id": ["1", "2", "3"],
    "country_code": ["GB", "DE", "GB"],
    "order_ts": [datetime(2026, 9, 10, 8, 0), datetime(2026, 9, 10, 9, 0), datetime(2026, 9, 11, 12, 0)],
})


def _leaf_dirs(root: str) -> list:
    out = []
    for d, dirs, files in os.walk(root):
        rel = os.path.relpath(d, root).replace(os.sep, "/")
        if files and not dirs and rel != "." and not rel.startswith("_delta_log"):
            out.append(rel)
    return sorted(out)


EXPECTED = [
    "country_code=DE/order_date=2026-09-10",
    "country_code=GB/order_date=2026-09-10",
    "country_code=GB/order_date=2026-09-11",
]


@pytest.mark.parametrize("engine", ["polars", "duckdb"])
@pytest.mark.parametrize("fmt", ["parquet", "delta"])
def test_date_partition_folders_are_plain_dates(tmp_path, engine, fmt):
    target = str(tmp_path / f"{engine}_{fmt}")
    DataProcessor(engine=engine, contract=_contract(target, fmt)).run(
        DATA, materialize=True, materialize_target=target
    )
    assert _leaf_dirs(target) == EXPECTED


def test_a_timestamp_partition_column_is_not_rewritten_as_a_date(tmp_path):
    # Only columns the contract declares `date` are rendered as dates — a (discouraged) raw
    # timestamp partition keeps its time, rather than silently merging a day's rows into one folder.
    from lakelogic.core.materialization import _date_partition_columns
    from lakelogic.core.models import DataContract

    c = DataContract(**_contract(str(tmp_path), "parquet"))
    assert _date_partition_columns(c, ["country_code", "order_date", "order_ts"]) == frozenset({"order_date"})
