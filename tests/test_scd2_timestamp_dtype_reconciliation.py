"""An SCD2 merge must produce ONE representation per timestamp column, not two.

`_scd2_frames` writes a pandas Timestamp into the SCD2 control columns from two independent
places:

  * cutting a NEW version sets `effective_from` from the contract's `timestamp_field`, so it
    inherits that source column's dtype — normally datetime64;
  * CLOSING the superseded row sets `effective_to`.

The rows already in the table come back from Delta as STRINGS whenever the table stores them
as strings, which it does whenever the first load used the `'1900-01-01'` default. Concatenate
the two and the column becomes `object` holding both, and the write dies at the very last step:

    ("Expected bytes, got a 'Timestamp' object",
     'Conversion failed for column effective_to with type object')

It only bites when a version is actually CUT, which is what disguised it as a
table-specific problem. On one real run of the RideFlow estate:

    dim_driver   existing=284  result=290   6 versions cut   -> crashed
    dim_rider    existing=734  result=734   0 versions cut   -> passed

`dim_rider` was one changed row away from the identical failure.

Both paths are covered here, separately, because fixing only the concat moved the error from
`effective_from` to `effective_to` — the close path writes somewhere else entirely.

The assertion is `pa.Table.from_pandas`, not a dtype check: that call is what actually failed
in production, so it is what the test should exercise.
"""

from __future__ import annotations

import pandas as pd
import pytest

from lakelogic.core.materialization import _scd2_frames

pa = pytest.importorskip("pyarrow")


def _cfg() -> dict:
    """The RideFlow dim_driver shape: a tracked column, and a source timestamp field."""
    return {
        "surrogate_key": "driver_sk",
        "surrogate_key_strategy": "hash",
        "timestamp_field": "source_updated_at",
        "effective_from_field": "effective_from",
        "effective_to_field": "effective_to",
        "current_flag_field": "is_current",
        "version_column": "version_number",
        "track_columns": ["status"],
    }


def _existing_with_string_timestamps() -> pd.DataFrame:
    """What Delta hands back when the table stores these columns as strings."""
    return pd.DataFrame(
        [
            {
                "driver_id": "d1",
                "status": "active",
                "source_updated_at": pd.Timestamp("2026-01-01"),
                "effective_from": "1900-01-01",
                "effective_to": "9999-12-31",
                "is_current": True,
            }
        ]
    )


def _incoming_with_a_change() -> pd.DataFrame:
    """A tracked column changed, so a new version must be cut."""
    return pd.DataFrame(
        [
            {
                "driver_id": "d1",
                "status": "suspended",  # tracked -> cuts a version
                "source_updated_at": pd.Timestamp("2026-09-11T04:35:00"),
            }
        ]
    )


def test_the_merged_frame_converts_to_arrow():
    """The end-to-end guard: this is the exact call that failed in production."""
    merged = _scd2_frames(_existing_with_string_timestamps(), _incoming_with_a_change(), ["driver_id"], _cfg())
    # Would raise ArrowTypeError("Expected bytes, got a 'Timestamp' object") before the fix.
    pa.Table.from_pandas(merged, preserve_index=False)


def test_a_version_really_was_cut():
    """Guard on the guard. If no version is cut there is nothing to concatenate and the
    Arrow conversion passes for the wrong reason — which is precisely why dim_rider looked
    healthy while carrying the same defect."""
    merged = _scd2_frames(_existing_with_string_timestamps(), _incoming_with_a_change(), ["driver_id"], _cfg())
    assert len(merged) > 1, "no new version was cut, so this test proves nothing"


@pytest.mark.parametrize("column", ["effective_from", "effective_to"])
def test_neither_timestamp_column_mixes_types(column):
    """Both columns, separately. Fixing only the concat path moved the failure from
    `effective_from` to `effective_to`."""
    merged = _scd2_frames(_existing_with_string_timestamps(), _incoming_with_a_change(), ["driver_id"], _cfg())
    kinds = {type(v).__name__ for v in merged[column].dropna()}
    assert len(kinds) == 1, f"{column} holds mixed types: {sorted(kinds)}"


def test_a_table_storing_real_timestamps_is_left_alone():
    """Normalising to strings must not be imposed on a table that already stores datetimes —
    that would be a schema change, not a fix."""
    existing = _existing_with_string_timestamps()
    existing["effective_from"] = pd.to_datetime(existing["effective_from"])
    existing["effective_to"] = pd.to_datetime(existing["effective_to"])

    merged = _scd2_frames(existing, _incoming_with_a_change(), ["driver_id"], _cfg())
    assert not any(isinstance(v, str) for v in merged["effective_from"].dropna())
    pa.Table.from_pandas(merged, preserve_index=False)
