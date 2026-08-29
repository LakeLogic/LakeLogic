"""SCD2 without `track_columns` must not cut a version on an unchanged re-run.

`_scd2_frames` falls back to "compare every non-key, non-control column" when a contract
declares no `track_columns`. That fallback was built with

    set(existing.columns) | set(incoming.columns) - set(primary_key) - scd2_control

where `-` binds tighter than `|`, so the exclusions applied only to `incoming` and the
primary key and the SCD2 control columns came back in through `existing`. With
`effective_from` in the compare set, the stored row's sentinel could never equal the
incoming change date, every comparison reported "changed", and an identical re-run cut a
fresh version of every row — unbounded history churn from a contract that did nothing
wrong.
"""
from __future__ import annotations

import pandas as pd

from lakelogic.core.materialization import _scd2_frames


def _cfg() -> dict:
    return {
        "effective_from_field": "effective_from",
        "effective_to_field": "effective_to",
        "current_flag_field": "is_current",
    }


def test_unchanged_rerun_cuts_no_new_version_without_track_columns():
    existing = pd.DataFrame([{
        "customer_id": "c1", "name": "Ada", "effective_from": "1900-01-01",
        "effective_to": "9999-12-31", "is_current": True,
    }])
    incoming = pd.DataFrame([{"customer_id": "c1", "name": "Ada"}])

    out = _scd2_frames(existing, incoming, ["customer_id"], _cfg())
    frame = out[0] if isinstance(out, tuple) else out

    current = frame[frame["is_current"].astype(bool)]
    assert len(frame) == 1, (
        f"an unchanged re-run cut a new version — {len(frame)} rows for one key:\n{frame}"
    )
    assert len(current) == 1


def test_a_real_change_still_cuts_a_version_without_track_columns():
    """The fix must not silence genuine change detection."""
    existing = pd.DataFrame([{
        "customer_id": "c1", "name": "Ada", "effective_from": "1900-01-01",
        "effective_to": "9999-12-31", "is_current": True,
    }])
    incoming = pd.DataFrame([{"customer_id": "c1", "name": "Grace"}])

    out = _scd2_frames(existing, incoming, ["customer_id"], _cfg())
    frame = out[0] if isinstance(out, tuple) else out

    assert len(frame) == 2, f"a changed attribute must cut a version:\n{frame}"
    assert set(frame["name"]) == {"Ada", "Grace"}
