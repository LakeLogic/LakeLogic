"""Reading a file's schema must not read its rows — and must not lie about types.

This module exists because a consumer was about to reimplement a subset of it: a probe that
split the first CSV line and parsed the first JSON record by hand. That approach can only
ever report every column as a string, silently disagrees with the framework about which
extensions are Excel, and has no route to cloud storage at all.
"""
from __future__ import annotations

import json

import pytest

pl = pytest.importorskip("polars")

from lakelogic.core.file_schema import (
    DATA_EXTENSIONS,
    FORMAT_EXTENSIONS,
    format_of,
    probe_schema,
)


@pytest.fixture
def files(tmp_path):
    df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"], "amt": [1.5, 2.5]})
    df.write_parquet(tmp_path / "a.parquet")
    df.write_csv(tmp_path / "a.csv")
    (tmp_path / "a.ndjson").write_text(
        '{"id": 1, "name": "a", "nested": {"x": 1}}\n{"id": 2, "name": "b"}\n'
    )
    (tmp_path / "a.json").write_text(json.dumps([{"id": 1, "name": "a"}]))
    return tmp_path


# ── format identification ───────────────────────────────────────────────── #


@pytest.mark.parametrize(
    "name,expected",
    [
        ("x.parquet", "parquet"),
        ("x.csv", "csv"),
        ("x.tsv", "csv"),
        ("x.json", "json"),
        ("x.ndjson", "json"),
        ("x.jsonl", "json"),
        ("x.xlsx", "excel"),
        ("PART-0001.PARQUET", "parquet"),
    ],
)
def test_format_of_identifies_what_it_can_read(name, expected):
    assert format_of(name) == expected


@pytest.mark.parametrize("name", ["x.avro", "x.orc", "x.txt", "x", "x.xls"])
def test_unreadable_extensions_are_named_as_such(name):
    """`.xls` is deliberately absent: polars reads it only through an optional engine, and
    claiming support that depends on one is worse than naming the gap."""
    assert format_of(name) is None


def test_every_declared_extension_is_reachable():
    """A format in the table with no extension in the flattened list is unreachable — the
    kind of gap that looks supported in code review and fails in use."""
    for exts in FORMAT_EXTENSIONS.values():
        for ext in exts:
            assert ext in DATA_EXTENSIONS


# ── schemas ─────────────────────────────────────────────────────────────── #


@pytest.mark.parametrize("name,fmt", [("a.parquet", "parquet"), ("a.csv", "csv")])
def test_it_reports_real_types_not_strings(files, name, fmt):
    """The reason for reusing the framework instead of parsing a header line: a header row
    carries names, never types, so a hand-rolled probe reports `id` as a string."""
    result = probe_schema(str(files / name))
    assert result.file_format == fmt
    types = {c.name: c.data_type for c in result.columns}
    assert types["id"].startswith("Int")
    assert types["amt"].startswith("Float")


def test_columns_keep_their_file_order(files):
    result = probe_schema(str(files / "a.parquet"))
    assert [c.name for c in result.columns] == ["id", "name", "amt"]
    assert [c.ordinal for c in result.columns] == [1, 2, 3]


def test_ndjson_keeps_a_nested_object_as_a_nested_type(files):
    """Flattening is a contract decision — how deep, what separator — so discovery reports
    the shape it found rather than inventing `nested_x`."""
    result = probe_schema(str(files / "a.ndjson"))
    nested = next(c for c in result.columns if c.name == "nested")
    assert nested.data_type.startswith("Struct")


def test_a_json_array_is_read_too(files):
    """An API dump writes one array; an event stream writes one object per line. Both land,
    and the extension does not distinguish them."""
    result = probe_schema(str(files / "a.json"))
    assert [c.name for c in result.columns] == ["id", "name"]


def test_types_can_be_turned_off(files):
    """A caller building all-string bronze wants exactly that — but it must be asked for."""
    result = probe_schema(str(files / "a.csv"), infer_types=False)
    assert {c.data_type for c in result.columns} == {"string"}


# ── failure is under-reporting, never a crash ───────────────────────────── #


def test_an_unreadable_extension_returns_none(files):
    assert probe_schema(str(files / "a.avro")) is None


def test_a_missing_file_returns_none_rather_than_raising(files):
    """A scan walks thousands of files. One that vanished mid-walk must not abort it."""
    assert probe_schema(str(files / "gone.parquet")) is None


def test_a_corrupt_file_returns_none_rather_than_raising(files):
    (files / "bad.parquet").write_bytes(b"not a parquet file")
    assert probe_schema(str(files / "bad.parquet")) is None


def test_an_empty_file_is_not_a_dataset(files):
    """Zero columns reported as a dataset produces a contract with nothing in it."""
    (files / "empty.csv").write_text("")
    assert probe_schema(str(files / "empty.csv")) is None


def test_no_row_count_is_invented(files):
    """Counting CSV lines means reading the file. Unknown is the honest answer, and an
    estimate presented as a count is worse than none."""
    assert probe_schema(str(files / "a.csv")).row_count is None
    assert probe_schema(str(files / "a.ndjson")).row_count is None


def test_parquet_reports_the_row_count_it_already_states(files):
    """Parquet carries the count in its FOOTER, so reporting it reads metadata, not rows —
    and dropping it would lose a fact the file hands over for free. A consumer of this
    module regressed exactly that way when its own probe was replaced."""
    assert probe_schema(str(files / "a.parquet")).row_count == 2


def test_the_bytes_reader_reports_the_same_count(files):
    from lakelogic.core.file_schema import probe_schema_bytes

    result = probe_schema_bytes((files / "a.parquet").read_bytes(), file_name="a.parquet")
    assert result.row_count == 2


# ── credentials ─────────────────────────────────────────────────────────── #


def test_storage_options_are_passed_through_not_read_from_the_environment(files, monkeypatch):
    """A multi-tenant caller holds one tenant's credential at a time. Reading it from the
    process environment is how the next tenant's scan inherits it."""
    seen = {}

    def fake_scan_parquet(path, **kwargs):
        seen.update(kwargs)
        return pl.scan_parquet(path)

    monkeypatch.setattr(pl, "scan_parquet", fake_scan_parquet)
    probe_schema(str(files / "a.parquet"), storage_options={"account_key": "k"})
    assert seen["storage_options"] == {"account_key": "k"}


def test_no_storage_options_means_none_are_sent(files, monkeypatch):
    """A local path must not be handed an empty options dict — some readers treat its
    presence as a request for remote access."""
    seen = {}

    def fake_scan_parquet(path, **kwargs):
        seen.update(kwargs)
        return pl.scan_parquet(path)

    monkeypatch.setattr(pl, "scan_parquet", fake_scan_parquet)
    probe_schema(str(files / "a.parquet"))
    assert "storage_options" not in seen


# ── the same read, from bytes ───────────────────────────────────────────── #


@pytest.mark.parametrize("name", ["a.parquet", "a.csv", "a.ndjson", "a.json"])
def test_bytes_and_path_agree(files, name):
    """Two entry points, one answer. A caller that reaches storage through its own
    credentialed client must not get a different schema from one that passes a URI."""
    from lakelogic.core.file_schema import probe_schema_bytes

    by_path = probe_schema(str(files / name))
    by_bytes = probe_schema_bytes((files / name).read_bytes(), file_name=name)
    assert [(c.name, c.data_type) for c in by_path.columns] == [
        (c.name, c.data_type) for c in by_bytes.columns
    ]
    assert by_path.file_format == by_bytes.file_format


def test_bytes_uses_the_name_only_to_pick_a_reader(files):
    """The buffer carries no extension, so the name is the only signal — and a name this
    module cannot read must not be guessed at."""
    from lakelogic.core.file_schema import probe_schema_bytes

    assert probe_schema_bytes(b"anything", file_name="x.avro") is None


def test_empty_bytes_are_not_a_dataset():
    from lakelogic.core.file_schema import probe_schema_bytes

    assert probe_schema_bytes(b"", file_name="a.csv") is None


def test_corrupt_bytes_return_none_rather_than_raising():
    """A scan walks thousands of objects; one bad file must not abort it."""
    from lakelogic.core.file_schema import probe_schema_bytes

    assert probe_schema_bytes(b"not a parquet", file_name="a.parquet") is None
