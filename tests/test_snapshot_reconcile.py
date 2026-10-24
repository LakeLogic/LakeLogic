"""Deletes detected from full snapshots: `materialization.deletion: {strategy: snapshot_reconcile}`.

A source that sends its COMPLETE current records every day (a bank's daily settlement file over
SFTP, a nightly full extract) never says what was deleted. A record the table holds but today's
full load lacks has been deleted at source. Each test runs real loads through
`materialize_dataframe` and reads the table back.
"""

from __future__ import annotations

import polars as pl
import pytest

from lakelogic.core.materialization import SnapshotReconcileRefused, materialize_dataframe
from lakelogic.core.models import DataContract


def _contract(target, *, soft=True, ratio=0.5, scope=None):
    mat = {
        "strategy": "merge",
        "format": "parquet",
        "target_path": str(target),
        "deletion": {
            "strategy": "snapshot_reconcile",
            "max_delete_ratio": ratio,
            **({"scope": scope} if scope else {}),
        },
    }
    if soft:
        mat.update(
            soft_delete_column="is_deleted",
            soft_delete_time_column="deleted_at",
            soft_delete_reason_column="delete_reason",
        )
    return DataContract.model_validate(
        {
            "version": "1.0.0",
            "info": {"title": "settlements", "owner": "payments"},
            "primary_key": ["id"],
            "source": {"type": "landing", "path": "unused", "format": "csv"},
            "model": {
                "fields": [
                    {"name": "id", "type": "string"},
                    {"name": "status", "type": "string"},
                    {"name": "country", "type": "string"},
                ]
            },
            "materialization": mat,
        }
    )


def _load(target, rows, **kw):
    df = pl.DataFrame(rows, schema={"id": pl.Utf8, "status": pl.Utf8, "country": pl.Utf8}, orient="row")
    materialize_dataframe(df, _contract(target, **kw), engine_name="polars")
    return pl.read_parquet(target).sort("id")


def _state(table):
    return {r["id"]: (r["status"], bool(r["is_deleted"])) for r in table.iter_rows(named=True)}


DAY1 = [("1", "paid", "GB"), ("2", "pending", "GB"), ("3", "paid", "GB")]


def test_a_record_missing_from_the_full_file_is_flagged_deleted_with_its_data_kept(tmp_path):
    target = tmp_path / "settlements.parquet"
    _load(target, DAY1)
    day2 = _load(target, [("1", "paid", "GB"), ("2", "refunded", "GB")])  # 3 gone, 2 updated
    assert _state(day2) == {"1": ("paid", False), "2": ("refunded", False), "3": ("paid", True)}
    gone = day2.filter(pl.col("id") == "3").row(0, named=True)
    assert gone["delete_reason"] == "missing_from_snapshot" and gone["deleted_at"]


def test_a_deleted_record_that_comes_back_is_restored(tmp_path):
    target = tmp_path / "settlements.parquet"
    _load(target, DAY1)
    _load(target, DAY1[:2])
    day3 = _load(target, DAY1)
    assert _state(day3)["3"] == ("paid", False)


def test_an_already_deleted_record_is_not_counted_again(tmp_path):
    target = tmp_path / "settlements.parquet"
    _load(target, DAY1)
    _load(target, DAY1[:2])
    # Same snapshot again: record 3 is already deleted, so nothing new is missing.
    again = _load(target, DAY1[:2], ratio=0.01)
    assert _state(again)["3"] == ("paid", True)


def test_a_truncated_file_stops_the_run_and_deletes_nothing(tmp_path):
    target = tmp_path / "settlements.parquet"
    _load(target, DAY1)
    with pytest.raises(SnapshotReconcileRefused, match="2 of 3"):
        _load(target, DAY1[:1], ratio=0.2)
    assert not pl.read_parquet(target)["is_deleted"].cast(pl.Boolean).fill_null(False).any()


def test_without_a_soft_delete_column_the_record_is_removed(tmp_path):
    target = tmp_path / "settlements.parquet"
    _load(target, DAY1, soft=False)
    day2 = _load(target, DAY1[:2], soft=False)
    assert sorted(day2["id"].to_list()) == ["1", "2"]


def test_scope_reconciles_only_what_the_load_covers(tmp_path):
    """One file per country: today's GB file says nothing about FR."""
    target = tmp_path / "settlements.parquet"
    _load(target, DAY1 + [("4", "paid", "FR")], scope=["country"])
    day2 = _load(target, [("1", "paid", "GB"), ("2", "paid", "GB")], scope=["country"])
    assert _state(day2)["3"][1] is True  # GB, missing from the GB file
    assert _state(day2)["4"][1] is False  # FR, not in this load's scope


def test_the_first_load_has_nothing_to_compare_with(tmp_path):
    target = tmp_path / "settlements.parquet"
    assert len(_load(target, DAY1)) == 3


def test_it_needs_merge_and_a_key():
    base = {
        "version": "1.0.0",
        "info": {"title": "t", "owner": "o"},
        "model": {"fields": [{"name": "id", "type": "string"}]},
    }
    with pytest.raises(Exception, match="merge"):
        DataContract.model_validate(
            {
                **base,
                "primary_key": ["id"],
                "materialization": {"strategy": "append", "deletion": {"strategy": "snapshot_reconcile"}},
            }
        )
