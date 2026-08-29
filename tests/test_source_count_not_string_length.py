"""A table NAME must never be counted as if it were a dataset.

The warehouse engines (snowflake, bigquery) take their source as a STRING — a table
name — because the data never enters the process. The run-log counter called
``len()`` on whatever it was given, so it measured the length of that string and
reported it as the source row count.

Observed live, on real warehouses, from a table containing THREE rows:

    Snowflake, source "PUBLIC.LL_SRC"  (13 chars)
      -> Run complete | Source: 13 | Total: 3 | Pre-Transform Dropped: 10
    BigQuery, source "project-d124a213-149c-42f2-860.ll_probe.src"  (43 chars)
      -> Run complete | Source: 43 | Total: 3 | Pre-Transform Dropped: 40

Both numbers are fiction, and neither is distinguishable from a real measurement.
The derived "Pre-Transform Dropped" is worse: it invents data loss that never
happened, on the very log a data engineer reads to decide whether a run was sound.

Unmeasured must read as unmeasured.
"""

from __future__ import annotations

import pytest

pl = pytest.importorskip("polars")

from lakelogic.core.models import DataContract
from lakelogic.core.processor import DataProcessor


def _processor() -> DataProcessor:
    return DataProcessor(
        {
            "version": "1.0.0",
            "dataset": "orders",
            "model": {"fields": [{"name": "id", "type": "int"}]},
        },
        engine="polars",
    )


def _counts(source, good, bad):
    return _processor()._compute_counts(source, good, bad)


GOOD = pl.DataFrame({"id": [1, 2]})
BAD = pl.DataFrame({"id": [3]})


@pytest.mark.parametrize(
    "table_name",
    [
        "PUBLIC.LL_SRC",  # 13 chars — the exact Snowflake case
        "project-d124a213-149c-42f2-860.ll_probe.src",  # 43 — the BigQuery case
        "t",  # 1 char: would have looked like a plausible 1-row source
        "",
    ],
)
def test_a_table_name_source_reports_unmeasured_not_its_length(table_name):
    counts = _counts(table_name, GOOD, BAD)

    assert counts["source"] is None, (
        f"source counted the NAME: got {counts['source']} for {table_name!r} "
        f"(len={len(table_name)})"
    )
    # ...and nothing derived from it may be invented either.
    assert counts["pre_transform_dropped"] in (None, 0)
    assert counts.get("pre_transform_added") is None


def test_the_real_rows_are_still_counted_when_the_source_is_a_name():
    """Refusing to count the source must not blind the rest of the log."""
    counts = _counts("PUBLIC.LL_SRC", GOOD, BAD)
    assert counts["good"] == 2
    assert counts["quarantined"] == 1
    assert counts["total"] == 3


def test_a_real_frame_source_is_still_counted():
    """The guard must not disable source counting for engines that DO pass a frame —
    that would trade a wrong number for a missing one."""
    counts = _counts(pl.DataFrame({"id": [1, 2, 3, 4, 5]}), GOOD, BAD)
    assert counts["source"] == 5
    assert counts["pre_transform_dropped"] == 2  # 5 read, 3 emitted


def test_bytes_are_also_not_a_dataset():
    assert _counts(b"PUBLIC.LL_SRC", GOOD, BAD)["source"] is None


def test_none_source_remains_unmeasured():
    assert _counts(None, GOOD, BAD)["source"] is None
