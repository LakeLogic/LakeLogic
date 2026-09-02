"""When the output goes straight back to the warehouse, don't bring it home first.

Snowflake/BigQuery push everything down, then ``execute()`` fetches the result into
pandas. If the caller is only going to write those rows back into the same
warehouse, that fetch is pure waste — and it is the step bounded by driver memory,
so it is what fails first on a large run.

``defer_fetch`` skips it. The cost is that ``run()`` then returns EMPTY frames, so
the decision must be narrow (only when the write is definitely going server-side)
and the run METADATA must stay truthful — counting the empty frames would report
"Good: 0" for a run that wrote millions of rows, which reads as "produced nothing".
"""

from __future__ import annotations

import pytest

from lakelogic.core.processor import DataProcessor


class _WarehouseAdapter:
    """Stands in for snowflake/bigquery: has both defer_fetch and a native writer."""

    defer_fetch = False
    deferred_counts = None

    def materialize_native(self, target_table, strategy="overwrite"):
        return {"target": target_table, "rows_written": 1, "server_side": True}


class _PlainAdapter:
    """polars/duckdb/spark: no defer_fetch attribute at all."""


def _processor(adapter, *, strategy="overwrite", target="table:DB.T"):
    p = object.__new__(DataProcessor)
    p.adapter = adapter
    p.engine_name = "snowflake"
    p.contract = type("C", (), {"materialization": type("M", (), {"strategy": strategy, "target_path": target})()})()
    return p


# ── when the fetch is skipped ────────────────────────────────────────────────


def test_defers_when_the_write_is_going_server_side():
    a = _WarehouseAdapter()
    _processor(a)._set_defer_fetch(True, "table:DB.T")
    assert a.defer_fetch is True


# ── when it must NOT be skipped (the caller needs real frames) ───────────────


def test_does_not_defer_without_materialize():
    """run() with materialize=False returns data the caller asked for. Skipping the
    fetch there would hand back empty frames and lose the run's output."""
    a = _WarehouseAdapter()
    _processor(a)._set_defer_fetch(False, None)
    assert a.defer_fetch is False


@pytest.mark.parametrize("strategy", ["merge", "scd2"])
def test_does_not_defer_for_strategies_that_use_the_normal_write_path(strategy):
    """merge/scd2 go through dlt, which needs the frames. Deferring would starve it."""
    a = _WarehouseAdapter()
    _processor(a, strategy=strategy)._set_defer_fetch(True, "table:DB.T")
    assert a.defer_fetch is False


@pytest.mark.parametrize("target", ["s3://b/p", "gs://b/p", "/local/out.parquet", "out.delta"])
def test_does_not_defer_for_file_targets(target):
    """A file target genuinely needs the rows in-process to be written."""
    a = _WarehouseAdapter()
    _processor(a, target=target)._set_defer_fetch(True, target)
    assert a.defer_fetch is False


def test_engines_without_the_concept_are_untouched():
    """polars/duckdb/spark have no fetch to skip; the helper must not invent state."""
    a = _PlainAdapter()
    _processor(a)._set_defer_fetch(True, "table:DB.T")
    assert not hasattr(a, "defer_fetch")


def test_a_previous_runs_decision_never_leaks():
    """Adapters are reused across runs. A stale defer_fetch would silently blank the
    output of a later run that genuinely needed its rows."""
    a = _WarehouseAdapter()
    a.defer_fetch = True
    a.deferred_counts = {"good": 999, "bad": 999}

    _processor(a)._set_defer_fetch(False, None)  # this run wants its data

    assert a.defer_fetch is False
    assert a.deferred_counts is None


# ── the metadata must stay true ──────────────────────────────────────────────


def test_counts_come_from_the_warehouse_not_the_empty_frames():
    """The whole risk of this feature. The frames are empty ON PURPOSE, so counting
    them would log "Good: 0 | Quarantine: 0" for a run that wrote millions — worse
    than a wrong number, because it reads as a failed run."""
    pl = pytest.importorskip("polars")

    p = DataProcessor(
        {
            "version": "1.0.0",
            "dataset": "orders",
            "model": {"fields": [{"name": "id", "type": "int"}]},
        },
        engine="polars",
    )
    p.adapter.deferred_counts = {"good": 5_000_000, "bad": 12}

    counts = p._compute_counts(None, pl.DataFrame(), pl.DataFrame())

    assert counts["good"] == 5_000_000
    assert counts["quarantined"] == 12
    assert counts["total"] == 5_000_012


def test_counts_use_the_frames_when_nothing_was_deferred():
    """The override must not fire on a normal run."""
    pl = pytest.importorskip("polars")

    p = DataProcessor(
        {
            "version": "1.0.0",
            "dataset": "orders",
            "model": {"fields": [{"name": "id", "type": "int"}]},
        },
        engine="polars",
    )
    counts = p._compute_counts(None, pl.DataFrame({"id": [1, 2]}), pl.DataFrame({"id": [3]}))

    assert counts["good"] == 2
    assert counts["quarantined"] == 1
