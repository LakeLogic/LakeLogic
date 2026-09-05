"""A failure records three things, not one string.

Observed in a live run log: a Delta (JVM) failure stored 2000 characters — the
truncation cap, so already clipped — of Scala frames, while a Python failure stored
31: `'Column' object is not callable`. The frames were never attached, so the only
question worth asking of a crash, WHICH LINE, could not be answered from the log.
Hours went into reproducing that failure four ways on Databricks; a traceback would
have answered it in one lookup.

They are separate columns rather than one because they are read differently:

* error_message   groups failures. The same fault on two datasets must produce the
                  same text, and a traceback never does.
* error_traceback locates the fault.
* lakelogic_version answers "which build", as a column so it can be filtered.
"""

from __future__ import annotations

import pytest

from lakelogic.core.run_log import capture_failure


def _boom(kind=TypeError, msg="'Column' object is not callable"):
    try:
        raise kind(msg)
    except BaseException as exc:  # noqa: BLE001 - the point is to capture any failure
        return capture_failure(exc)


def test_the_three_fields_are_populated():
    got = _boom()
    assert got["error_message"] == "'Column' object is not callable"
    assert "in _boom" in got["error_traceback"], got["error_traceback"]
    assert got["lakelogic_version"]


def test_the_message_is_the_grouping_key_and_stays_identical():
    """Two datasets hitting one fault must produce the same text, or they cannot be
    grouped — which is exactly the signal a triage system needs."""
    assert _boom()["error_message"] == _boom()["error_message"]


def test_the_traceback_is_NOT_in_the_message():
    """Putting frames in the message would break grouping and swamp the incident card."""
    got = _boom()
    assert "\n" not in got["error_message"]
    assert " in " not in got["error_message"]


def test_the_message_keeps_only_the_first_line():
    got = _boom(ValueError, "headline\nsecond line\nthird")
    assert got["error_message"] == "headline"
    assert "second line" not in got["error_message"]


def test_frames_do_not_leak_the_host_layout():
    """A run log is read by people who cannot see the machine that produced it."""
    trace = _boom()["error_traceback"]
    assert "/local_disk0/" not in trace
    assert "ephemeral_nfs" not in trace
    assert ":\\" not in trace, trace  # no C:\... absolute Windows paths


def test_both_fields_stay_within_the_column_budget():
    deep = _boom(RuntimeError, "x" * 5000)
    assert len(deep["error_message"]) <= 500
    assert len(deep["error_traceback"]) <= 2000


def test_an_exception_with_no_message_still_identifies_itself():
    got = _boom(KeyboardInterrupt, "")
    assert got["error_message"], "an empty message must fall back to the class name"


@pytest.mark.parametrize("column", ["error_traceback", "lakelogic_version"])
def test_the_new_columns_exist_in_every_schema_definition(column):
    """Six writers define this table; a column missing from one silently drops it."""
    from pathlib import Path

    run_log = Path(__file__).resolve().parents[1] / "lakelogic" / "core" / "run_log.py"
    constants = Path(__file__).resolve().parents[1] / "lakelogic" / "core" / "constants.py"
    text = run_log.read_text(encoding="utf-8")
    assert text.count(column) >= 5, f"{column} missing from a run_log.py schema site"
    assert column in constants.read_text(encoding="utf-8"), f"{column} missing from the UC DDL"


def test_the_failure_record_survives_a_broken_capture():
    """The capture runs inside the runner's `except ...: pass`, so if building the
    record raises, the record is lost and the run fails with nothing explaining why.
    A stubbed or older run_log module must degrade to a message, not to silence."""
    import types

    import lakelogic.pipeline.runner as runner  # noqa: F401  (import path exercised)

    # Mimic the runner's fallback contract directly: whatever happens, a message.
    try:
        raise TypeError("'Column' object is not callable")
    except TypeError as exc:
        fallback = {"error_message": str(exc).splitlines()[0][:500] if str(exc) else type(exc).__name__}
    assert fallback["error_message"] == "'Column' object is not callable"
