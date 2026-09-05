"""Rows that disappear must be counted, and named.

A silver contract read 4,572 rows and wrote 3,166. The run log recorded both
numbers and nothing else, so 1,406 rows vanished with no explanation — which
looks identical to data loss even though a declared ``deduplicate:`` was doing
exactly its job.

Two separate defects were behind that:

1. ``counts_dropped`` was computed, put on the run-log row, and then **silently
   discarded on write** because no table schema had the column. Same for
   ``counts_aggregated``. The writer built values nobody stored.
2. Even stored, the number is undifferentiated: it cannot say whether a dedup, a
   filter, or a bug removed the rows.

The first test below is the general guard: it fails if ANY field the writer puts
on the row is missing from ANY backend's schema. That is the class of bug, not
the instance — the same shape as `error_traceback`, which had to be added to six
places by hand and would have been caught here.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

from lakelogic.core import run_log as rl

SOURCE = Path(rl.__file__).read_text(encoding="utf-8")


def _row_fields() -> set:
    """Field names the writer puts on a run-log row."""
    block = re.search(r'"counts_source": counts\.get\("source"\),(.*?)\n    \}', SOURCE, re.S)
    assert block, "could not locate the run-log row dict — has it been restructured?"
    return set(re.findall(r'^\s*"([a-z_0-9]+)":', block.group(1), re.M)) | {"counts_source"}


# Every backend that must be able to STORE a row, and how its schema names columns.
BACKENDS = {
    "spark StructType": r'StructField\("([a-z_0-9]+)"',
    "spark ALTER TABLE": r'\("([a-z_0-9]+)", "(?:STRING|BIGINT|DOUBLE|BOOLEAN|TIMESTAMP)"\)',
    "postgres ALTER TABLE": r'\("([a-z_0-9]+)", "(?:TEXT|INTEGER|REAL|BOOLEAN|TIMESTAMP)"\)',
    "arrow schema": r'\("([a-z_0-9]+)", pa\.',
}

# Counts are the fields this change is about; assert on them specifically so a
# regression names the column rather than dumping a 35-field diff.
COUNT_FIELDS = {
    "counts_source", "counts_total", "counts_good", "counts_quarantined",
    "counts_aggregated", "counts_dropped", "counts_deduplicated", "counts_filtered",
}


@pytest.mark.parametrize("backend,pattern", sorted(BACKENDS.items()))
def test_every_count_the_writer_emits_has_a_column_to_land_in(backend, pattern):
    """The guard for the whole class: a field with nowhere to go is data loss."""
    known = set(re.findall(pattern, SOURCE))
    emitted = _row_fields() & COUNT_FIELDS
    missing = sorted(emitted - known)
    assert not missing, (
        f"{backend} has no column for {missing}. The writer puts these on the row, so "
        f"they are computed and then silently dropped at write time — exactly how "
        f"counts_dropped and counts_aggregated were lost."
    )


def test_the_new_counts_are_emitted_at_all():
    fields = _row_fields()
    for name in ("counts_dropped", "counts_deduplicated", "counts_filtered"):
        assert name in fields, f"{name} is not written to the run-log row"


# ── attribution ──────────────────────────────────────────────────────────────

class _Trans:
    """A stand-in for a contract transformation; only truthiness per attr matters."""

    def __init__(self, **kw):
        for attr in ("deduplicate", "deduplicate_by_latest", "filter",
                     "sql", "rollup", "pivot", "unnest", "explode"):
            setattr(self, attr, kw.get(attr))


class _Contract:
    def __init__(self, *transformations):
        self.transformations = list(transformations)


def _attribute(dropped, *transformations):
    from lakelogic.core.processor import DataProcessor

    proc = object.__new__(DataProcessor)          # no __init__: this is a pure helper
    proc.contract = _Contract(*transformations)
    return DataProcessor._attribute_dropped_rows(proc, dropped)


def test_a_lone_deduplicate_owns_every_dropped_row():
    """The real case: one declared dedup, so 1,406 rows are explained."""
    assert _attribute(1406, _Trans(deduplicate={"by": ["request_id"]})) == {"deduplicated": 1406}


def test_deduplicate_by_latest_is_attributed_the_same_way():
    assert _attribute(12, _Trans(deduplicate_by_latest={"by": ["id"]})) == {"deduplicated": 12}


def test_a_lone_filter_owns_every_dropped_row():
    assert _attribute(30, _Trans(filter={"sql": "amount > 0"})) == {"filtered": 30}


def test_two_removers_leave_the_split_unmeasured():
    """Unmeasured is not zero. With a dedup AND a filter, counts alone cannot say
    which removed what, so we report neither rather than inventing a split."""
    assert _attribute(50, _Trans(deduplicate={"by": ["id"]}), _Trans(filter={"sql": "x"})) == {}


def test_an_opaque_transform_blocks_attribution():
    """A SQL transform can change the row count invisibly, so a dedup sitting
    beside one can no longer be credited with the whole drop."""
    assert _attribute(50, _Trans(deduplicate={"by": ["id"]}), _Trans(sql="select * from source")) == {}


def test_nothing_is_claimed_when_nothing_was_dropped():
    assert _attribute(0, _Trans(deduplicate={"by": ["id"]})) == {}
    assert _attribute(None, _Trans(deduplicate={"by": ["id"]})) == {}


def test_no_transformations_means_no_attribution():
    assert _attribute(99) == {}


def test_attribution_never_raises_on_a_malformed_contract():
    """This runs on the reporting path; a bad contract must not lose the run log."""
    from lakelogic.core.processor import DataProcessor

    proc = object.__new__(DataProcessor)
    proc.contract = object()  # no `transformations` attribute at all
    assert DataProcessor._attribute_dropped_rows(proc, 5) == {}


# ── the wire format ──────────────────────────────────────────────────────────

def test_the_telemetry_payload_carries_the_drop_counts():
    """The platform showed `rows_input` 4,572 and `rows_output` 3,166 with no way
    to explain the gap, because the payload never sent one.

    They travel in `metadata`, not as top-level keys: the receiving DTO validates
    and DISCARDS undeclared top-level fields — the same way it silently dropped
    `error_traceback` for months until its schema learned the name. Metadata is a
    free-form dict on both ends, so this reaches the platform without needing a
    migration first.
    """
    payload_block = re.search(r'payload = \{(.*?)\n                \}', SOURCE, re.S)
    assert payload_block, "could not locate the telemetry payload"
    body = payload_block.group(1)
    for key in ('"rows_dropped"', '"rows_deduplicated"', '"rows_filtered"'):
        assert key in body, f"{key} is not sent to the platform"


# ── build provenance ─────────────────────────────────────────────────────────

def test_the_version_is_stamped_on_successful_runs_too():
    """`lakelogic_version` was written only by capture_failure(), so it was NULL on
    every green run — the exact inverse of what it is for.

    "Which build is running in production" is a question about successful runs, and
    "did this regress after the upgrade?" needs the version on the runs BEFORE the
    failure. Observed on Databricks: a whole table of successful runs with a null
    version column.
    """
    row_block = re.search(r'"lakelogic_version":([^\n]*)', SOURCE)
    assert row_block, "lakelogic_version is no longer written to the row"
    assert "_lakelogic_version()" in row_block.group(1), (
        "lakelogic_version falls back to nothing on a successful run — it must be "
        "stamped unconditionally, not only by capture_failure()"
    )


def test_capture_failure_still_supplies_its_own_version():
    """The failure path keeps working; the fallback must not have replaced it."""
    from lakelogic.core.run_log import capture_failure

    try:
        raise ValueError("boom")
    except ValueError as exc:
        captured = capture_failure(exc)
    assert captured["lakelogic_version"], "capture_failure no longer reports a version"
    assert captured["error_message"] == "boom"
