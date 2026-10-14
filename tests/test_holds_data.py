"""Append, merge and SCD2 ask the STORAGE whether a cloud target already holds data.

``URIPath.exists`` is always False, so an append or merge to a Parquet/CSV file in cloud storage
used to replace it with only the new rows. "Could not tell" must stop the run, never mean "empty".
"""

import types

import pytest

from lakelogic.core import materialization as m


def _fs(exists=None, error=None):
    def url_to_fs(uri):
        if error:
            raise error
        return types.SimpleNamespace(exists=lambda path: exists), uri

    return url_to_fs


def test_an_existing_cloud_file_is_found(monkeypatch):
    import fsspec

    monkeypatch.setattr(fsspec.core, "url_to_fs", _fs(exists=True))
    assert m._holds_data(m.URIPath("abfss://c@a.dfs.core.windows.net/silver/trips/data.parquet"))


def test_a_missing_cloud_file_is_not_data(monkeypatch):
    import fsspec

    monkeypatch.setattr(fsspec.core, "url_to_fs", _fs(exists=False))
    assert not m._holds_data(m.URIPath("s3://bucket/silver/trips/data.parquet"))


def test_storage_that_cannot_be_asked_stops_the_run(monkeypatch):
    import fsspec

    monkeypatch.setattr(fsspec.core, "url_to_fs", _fs(error=PermissionError("no credentials")))
    with pytest.raises(RuntimeError, match="rather than risk overwriting"):
        m._holds_data(m.URIPath("gs://bucket/silver/trips/data.parquet"))


def test_a_local_target_is_checked_on_disk(tmp_path):
    (tmp_path / "data.parquet").write_bytes(b"x")
    assert m._holds_data(tmp_path / "data.parquet")
    assert not m._holds_data(tmp_path / "missing.parquet")
