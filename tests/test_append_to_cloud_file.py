"""An append to a Parquet/CSV file in cloud storage reads the existing file by its URI, with the
writer's credentials (2026-10-22: the second run of an append to an ADLS landing zone failed with
`Object does not have a .read() method`)."""

import polars as pl
import pytest

from lakelogic.core import materialization as m


@pytest.mark.parametrize("fmt,reader", [("parquet", "read_parquet"), ("csv", "read_csv")])
def test_append_reads_the_existing_cloud_file_by_uri_with_credentials(monkeypatch, fmt, reader):
    target = m.URIPath(f"abfss://landing@acct.dfs.core.windows.net/erp/orders/data.{fmt}")
    seen, written = {}, {}

    def fake_read(path, **kw):
        seen["path"], seen["kw"] = path, kw
        return pl.DataFrame({"id": [1]})

    monkeypatch.setattr(pl, reader, fake_read)
    monkeypatch.setattr(m, "_holds_data", lambda _t: True)
    monkeypatch.setattr(m, "_build_storage_options", lambda _o: {"account_name": "acct", "account_key": "k"})
    monkeypatch.setattr(m, "_write_frame", lambda df, *_a, **_k: written.setdefault("rows", df.height))

    rows = m._append_without_pandas(pl.DataFrame({"id": [2, 3]}), target, fmt)
    assert seen["path"] == str(target) and isinstance(seen["path"], str)
    assert seen["kw"] == {"storage_options": {"account_name": "acct", "account_key": "k"}}
    assert rows == 3 and written["rows"] == 3


def test_a_local_append_is_unchanged(tmp_path):
    target = tmp_path / "data.parquet"
    pl.DataFrame({"id": [1]}).write_parquet(target)
    assert m._append_without_pandas(pl.DataFrame({"id": [2]}), target, "parquet") == 2
    assert pl.read_parquet(target)["id"].to_list() == [1, 2]


@pytest.mark.parametrize("value,expected", [(20, 20), ("20", 20), (" 50000 ", 50000), (None, None)])
def test_a_database_read_count_written_as_text_is_a_number(value, expected):
    from lakelogic.core.processor import _whole_number

    assert _whole_number(value, "fetch_size") == expected


@pytest.mark.parametrize("value", ["twenty", "1.5", True])
def test_a_database_read_count_that_is_not_a_whole_number_is_refused(value):
    from lakelogic.core.processor import _whole_number

    with pytest.raises(ValueError, match="fetch_size must be a whole number"):
        _whole_number(value, "fetch_size")
