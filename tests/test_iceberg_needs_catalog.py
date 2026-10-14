"""Iceberg is a catalog format: a path target is refused before anything is written.

A bare path failed on every engine anyway (DuckDB has no Iceberg COPY; Spark resolves an Iceberg
write by table name), each in its own words. Now one message names the fix.
"""

import polars as pl
import pytest

from lakelogic import DataProcessor
from lakelogic.core.materialization import IcebergNeedsCatalog


@pytest.mark.parametrize("engine", ["polars", "duckdb"])
def test_an_iceberg_path_target_is_refused_with_the_fix(tmp_path, engine):
    contract = {
        "version": "1.0.0",
        "info": {"title": "Trips", "table_name": "trips"},
        "model": {"fields": [{"name": "id", "type": "integer"}]},
        "materialization": {"strategy": "append", "format": "iceberg"},
    }
    target = tmp_path / "silver_trips"
    with pytest.raises(IcebergNeedsCatalog, match=r"table:<catalog>\.<namespace>\.<table>"):
        DataProcessor(engine=engine, contract=contract).run(
            pl.DataFrame({"id": [1]}), materialize=True, materialize_target=str(target)
        )
    assert not target.exists()  # nothing written
