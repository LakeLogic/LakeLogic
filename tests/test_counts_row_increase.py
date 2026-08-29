"""A run that emits more rows than it read must say so.

`pre_transform_dropped` is `source - total` floored at zero, so a run that
INCREASED its row count reported `dropped: 0` and nothing else. A join on a
non-unique key doubles revenue while every individual row still validates —
the counts were structurally unable to show it.

This mirrors the treatment row REDUCTION already had: a decrease explained by a
declared aggregation is reclassified as `aggregated_rows`, and only an
unexplained decrease stays a finding. An increase now works the same way.
"""
from __future__ import annotations

import types

import pandas as pd

from lakelogic.core.processor import DataProcessor


def _counts(source_rows: int, good_rows: int):
    proc = object.__new__(DataProcessor)
    proc.engine_name = "pandas"
    src = pd.DataFrame({"id": range(source_rows)})
    good = pd.DataFrame({"id": range(good_rows)})
    bad = pd.DataFrame({"id": []})
    return proc._compute_counts(src, good, bad)


def test_a_fan_out_is_reported_not_clamped_away():
    """10 rows in, 20 out — the extra 10 must be visible."""
    counts = _counts(source_rows=10, good_rows=20)

    assert counts["pre_transform_added"] == 10, "the row increase must be surfaced"
    assert counts["pre_transform_dropped"] == 0, "dropped stays clamped for existing consumers"


def test_row_reduction_still_reports_dropped_and_no_increase():
    counts = _counts(source_rows=20, good_rows=10)

    assert counts["pre_transform_dropped"] == 10
    assert counts["pre_transform_added"] is None, "a decrease is not an increase"


def test_balanced_run_reports_neither():
    counts = _counts(source_rows=10, good_rows=10)

    assert counts["pre_transform_dropped"] == 0
    assert counts["pre_transform_added"] is None
