"""A run that rejects every row it read is not a success.

THE LIVE FAILURE, from a Fabric run:

    bronze_rideflow_trips   succeeded  src=10 good=10 qtn=0
    silver_rideflow_trips   succeeded  src=10 good=0  qtn=10    ← 100% rejected
    gold_fact_trips         succeeded  src=0  good=0  qtn=0     ← nothing to read

Three green layers and an empty estate. `fail_on_quarantine` cannot cover this: it raises on a
SINGLE bad row, so any estate that deliberately seeds invalid rows must leave it off — and then
the opposite extreme reports clean too.
"""

import inspect

from lakelogic.pipeline import runner


def _status_block():
    src = inspect.getsource(runner)
    start = src.index('            _status = "success"')
    return src[start : start + 2000]


def test_a_fully_quarantined_run_gets_its_own_status():
    block = _status_block()
    assert "all_rows_quarantined" in block
    # It must be reached when the GOOD frame is empty but the bad one is not — the case that
    # `no_new_rows` (both empty) does not cover.
    assert "elif is_good_empty:" in block


def test_it_is_not_folded_into_success_or_no_new_rows():
    """Three distinct outcomes, because they need three different responses: data landed,
    nothing arrived, everything arrived and was rejected."""
    block = _status_block()
    ordered = [
        block.index('_status = "success"'),
        block.index('_status = "no_new_rows"'),
        block.index('_status = "all_rows_quarantined"'),
    ]
    assert ordered == sorted(ordered)
    assert len(set(ordered)) == 3


def test_the_operator_is_told_where_the_reason_is():
    """The rows are in quarantine with an `error_reason`; a status alone sends someone
    hunting."""
    block = _status_block()
    assert "quarantined EVERY row" in block
    assert "quarantine table carries the reason" in block


# ── the verdict has to survive the trip to the RUN LOG ───────────────────────────────────
#
# `_status` was computed correctly and then discarded: the run log wrote a literal
# "succeeded" for every entity that did not raise. The engine knew; the table that governs
# the estate did not.


def test_the_run_log_does_not_report_a_fully_quarantined_run_as_a_success():
    assert runner.run_log_status("all_rows_quarantined") == "partial"


def test_partial_still_emits():
    """`partial` and not a new status value, because `emit_on` filters on this enum — an
    unrecognised value would silently stop the telemetry for the runs worth seeing."""
    from lakelogic.core import run_log as run_log_module

    default_emit_on = ["success", "partial", "failed"]
    assert runner.run_log_status("all_rows_quarantined") in default_emit_on
    assert 'emit_on", ["success", "partial", "failed"]' in inspect.getsource(run_log_module)


def test_the_ordinary_outcomes_are_unchanged():
    assert runner.run_log_status("success") == "succeeded"
    assert runner.run_log_status("no_new_rows") == "succeeded"


def test_the_run_log_write_uses_the_mapping_rather_than_a_literal():
    """The defect was a hardcoded string next to a variable that already held the answer."""
    src = inspect.getsource(runner)
    assert '_report["status"] = run_log_status(_status)' in src
    assert '_report["status"] = "succeeded"' not in src
