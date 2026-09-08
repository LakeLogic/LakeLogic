"""Whether a JSON file holds one value or one per line is a property of the DATA.

THE LIVE FAILURE — silent data loss, no error anywhere:

    landing/rideflow/trips/dt=2026-09-08/hr=13/
        batch_00_a1b2c3.json   20 rows
        batch_01_d4e5f6.json   20 rows
        ... ten files, 200 rows

    bronze_rideflow_trips   succeeded   src=10   good=10   qtn=0

`multiLine=true` tells Spark to parse each FILE as a single JSON value. Against JSON Lines it
takes the first object and discards the rest — ten files, ten rows, 190 gone. Bronze reported
success, silver and gold processed the ten, and every screen was green.

The option was hardcoded on Spark. Polars, meanwhile, has always AUTO-DETECTED the shape (see
`_read_json_flat`), so the same contract read 200 rows locally and 10 on Fabric — with the
local dry run passing. Two engines, one contract, two answers.

Both engines now behave the same way: an explicit `source.options.multiLine` wins, and
absent one, the shape is read from the data. Pinned end-to-end by conformance cases
OLC-S-002 / OLC-S-003, which run the same rows in both layouts on duckdb, polars and Spark.
"""

import inspect
import json

import pytest

from lakelogic.core import processor
from lakelogic.core.processor import DataProcessor


def _json_branch() -> str:
    src = inspect.getsource(processor)
    start = src.index('elif fmt == "json":')
    return src[start : start + 2600]


# ── the contract decides, when it says so ────────────────────────────────────────────────


def test_the_option_is_not_hardcoded():
    branch = _json_branch()
    assert 'reader.option("multiLine", "true")' not in branch, (
        "hardcoded again — the data must decide, and the contract must be able to override"
    )
    assert 'getattr(self.contract.source, "options"' in branch


def test_both_spellings_are_accepted():
    """A contract author will write one or the other, and being wrong is invisible."""
    branch = _json_branch()
    assert '"multiLine"' in branch and '"multiline"' in branch
    assert '"true" if _multiline else "false"' in branch


def test_silence_means_read_the_data_not_assume_a_shape():
    """The default USED to be a hardcoded `true`, which is what truncated the landing zone.
    It is now a sniff — the same answer polars has always produced — so the two engines agree
    on a contract that declares nothing, which is most of them."""
    branch = _json_branch()
    assert "_multiline is None" in branch
    assert "_json_is_one_value_per_file" in branch


# ── the sniff reads one character of one file ────────────────────────────────────────────


def _sniff(tmp_path, body: str) -> bool:
    fp = tmp_path / "part_00.json"
    fp.write_text(body, encoding="utf-8")
    proc = DataProcessor.__new__(DataProcessor)
    return processor.DataProcessor._json_is_one_value_per_file(proc, [str(fp)], None)


def test_an_array_file_is_one_value_per_file(tmp_path):
    assert _sniff(tmp_path, json.dumps([{"a": 1}, {"a": 2}])) is True


def test_a_json_lines_file_is_not(tmp_path):
    assert _sniff(tmp_path, '{"a": 1}\n{"a": 2}\n') is False


def test_leading_whitespace_does_not_fool_it(tmp_path):
    assert _sniff(tmp_path, '\n\n   [{"a": 1}]') is True


def test_an_unreadable_file_falls_back_to_the_previous_behaviour():
    """A sniff that cannot read must not quietly change how every existing estate is parsed."""
    proc = DataProcessor.__new__(DataProcessor)
    assert processor.DataProcessor._json_is_one_value_per_file(
        proc, ["/does/not/exist.json"], None
    ) is True


def test_nothing_to_sniff_falls_back_too():
    proc = DataProcessor.__new__(DataProcessor)
    assert processor.DataProcessor._json_is_one_value_per_file(proc, [], None) is True


# ── the warning stays where a future editor will read it ─────────────────────────────────


def test_the_cost_of_getting_it_wrong_is_written_down():
    """A future reader deciding to 'simplify' this back to a constant needs to see what that
    costs — the failure is invisible, so the comment is the only warning."""
    branch = _json_branch()
    assert "silently discards" in branch
    assert "TEN rows" in branch


def test_the_sniff_explains_itself():
    doc = inspect.getdoc(DataProcessor._json_is_one_value_per_file) or ""
    assert "OLC-S-003" in doc, "point at the conformance case that reproduces it"
