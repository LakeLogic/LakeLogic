"""A path target's existence is asked of Spark, not the local disk.

``Path("abfss://...").exists()`` is always False, so a merge into an existing cloud table was
treated as a first write and overwrote the table with only the incoming batch.
"""

import types

from lakelogic.core.materialization import _spark_path_holds_table


def _spark(loads: bool):
    def load(path):
        if not loads:
            raise RuntimeError("PATH_NOT_FOUND")
        return types.SimpleNamespace(schema=["id"])

    reader = types.SimpleNamespace(format=lambda fmt: types.SimpleNamespace(load=load))
    return types.SimpleNamespace(read=reader)


def test_an_existing_cloud_table_is_found():
    assert _spark_path_holds_table(_spark(loads=True), "iceberg", "abfss://c@a.dfs.core.windows.net/silver/trips")


def test_an_empty_cloud_path_is_not_a_table():
    assert not _spark_path_holds_table(_spark(loads=False), "parquet", "s3://bucket/silver/trips")


def test_a_local_path_is_still_checked_on_disk(tmp_path):
    assert _spark_path_holds_table(_spark(loads=False), "parquet", str(tmp_path))
    assert not _spark_path_holds_table(_spark(loads=True), "parquet", str(tmp_path / "missing"))
