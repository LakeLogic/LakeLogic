"""An Iceberg table DIRECTORY is a source on Polars and DuckDB (``source.type: iceberg``).

It used to go to the file reader and fail with "expected a file path; ... is a directory".
"""

import polars as pl
import pytest

pytest.importorskip("pyiceberg")

from lakelogic import DataProcessor  # noqa: E402
from lakelogic.core.processor import _is_iceberg_table_dir, _read_iceberg_table_dir  # noqa: E402


def _table(tmp_path, batches):
    """A local Iceberg table appended once per batch; returns its directory."""
    from pyiceberg.catalog.sql import SqlCatalog

    warehouse = tmp_path / "warehouse"
    warehouse.mkdir()
    catalog = SqlCatalog(
        "t",
        uri=f"sqlite:///{(tmp_path / 'catalog.db').as_posix()}",
        warehouse=warehouse.as_uri(),
        **{"py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO"},
    )
    catalog.create_namespace("n")
    first = pl.DataFrame(batches[0]).to_arrow()
    table = catalog.create_table("n.trips", schema=first.schema)
    for batch in batches:
        table.append(pl.DataFrame(batch).to_arrow())
    return warehouse / "n" / "trips"


def test_reads_the_newest_snapshot_not_the_first(tmp_path):
    # Two appends write two metadata files. Reading the first would return one row.
    path = _table(tmp_path, [{"id": [1], "city": ["LON"]}, {"id": [2], "city": ["NYC"]}])
    assert _is_iceberg_table_dir(str(path))
    assert sorted(_read_iceberg_table_dir(str(path))["id"].to_list()) == [1, 2]


def test_a_plain_directory_is_not_an_iceberg_table(tmp_path):
    (tmp_path / "metadata").mkdir()
    assert not _is_iceberg_table_dir(str(tmp_path))
    assert not _is_iceberg_table_dir("abfss://container@account.dfs.core.windows.net/t")


@pytest.mark.parametrize("engine", ["polars", "duckdb"])
def test_iceberg_source_runs_end_to_end(tmp_path, engine):
    path = _table(tmp_path, [{"id": [1, 2], "city": ["LON", "NYC"]}])
    contract = {
        "version": "1.0.0",
        "info": {"title": "Trips", "table_name": "trips"},
        "source": {"type": "iceberg", "path": str(path), "format": "iceberg"},
        "model": {"fields": [{"name": "id", "type": "integer"}, {"name": "city", "type": "string"}]},
    }
    good, bad = DataProcessor(engine=engine, contract=contract).run_source(str(path))
    rows = good.to_dicts() if hasattr(good, "to_dicts") else good.pl().to_dicts()
    assert sorted(r["id"] for r in rows) == [1, 2]
