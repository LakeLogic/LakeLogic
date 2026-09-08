"""An evaluation is only interpretable against the promise that applied when it ran.

THE DEFECT: quality and schedule reached the platform as a verdict with no target.
Measured on a live estate, 7 days:

    quality  -> {"pass": true}  and nothing else            (252 evaluations)
    schedule -> {"pass": false, "seconds": 2376.0}          (no deadline)

So the Service Levels page rendered "breached — no measurement reported" and could not
say what "good" was supposed to mean. Freshness, volume and retention all carry observed
AND declared; these two carried observed only — and quality did not even carry that,
because the section is built from freshness's fields and quality populates none of them.
Retention had this exact bug and it was fixed the same way (`run_log.py`, "Retention
reached the platform as `{"pass": true}` and nothing else").

WHY THE TARGET IS STAMPED, NOT JOINED
The floor lives in the domain config, and the config changes. Looking it up at read time
would mean raising a floor from 0.95 to 0.99 retroactively fails every evaluation that
met the promise in force when it ran. The target belongs to the evidence, for the same
reason `contract_version` and `contract_fingerprint` are stamped on every run log.
"""

from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from lakelogic.core.run_log import emit_slo_report  # noqa: E402
from lakelogic.core.slo import SLOValidator, SLOCheckResult  # noqa: E402


class _Quality:
    def __init__(self, min_good_ratio=0.99, max_quarantine_ratio=0.01, by_severity=None):
        self.min_good_ratio = min_good_ratio
        self.max_quarantine_ratio = max_quarantine_ratio
        self.by_severity = by_severity or {}


def _counts(total=100, good=55, quarantined=45):
    return {"total": total, "good": good, "quarantined": quarantined}


# ── Quality: the floor the verdict was reached against ───────────────────────
def test_a_quality_breach_states_the_floor_it_missed():
    """THE DEFECT. `passed=False` with no floor is not actionable."""
    [result] = SLOValidator._evaluate_quality_counts("pipeline", _counts(), _Quality(0.99))

    assert result.passed is False
    assert result.quality_ratio == 0.55
    assert result.quality_min_ratio == 0.99, "the promise this was judged against"


def test_a_passing_quality_check_carries_it_too():
    """"Which is fine, and by how much" is the question asked BEFORE a breach."""
    [result] = SLOValidator._evaluate_quality_counts("pipeline", _counts(good=100, quarantined=0), _Quality(0.99))

    assert result.passed is True
    assert result.quality_min_ratio == 0.99


def test_both_sides_of_the_gate_are_reported():
    """`passed` ANDs two conditions. Reporting only the good ratio leaves a
    quarantine-driven failure looking inexplicable."""
    [result] = SLOValidator._evaluate_quality_counts("pipeline", _counts(), _Quality(0.5, 0.01))

    # good_ratio 0.55 clears the 0.5 floor; the 45% quarantine rate is what failed it.
    assert result.quality_ratio == 0.55
    assert result.quality_quarantine_ratio == 0.45
    assert result.quality_max_quarantine_ratio == 0.01
    assert result.passed is False


def test_total_data_loss_still_names_the_floor():
    """The 0% branch returns early and had to be fixed separately — the kind of second
    path that gets missed."""
    [result] = SLOValidator._evaluate_quality_counts("pipeline", _counts(good=0, quarantined=100), _Quality(0.9))

    assert result.quality_ratio == 0.0
    assert result.quality_min_ratio == 0.9


def test_no_quality_slo_means_no_invented_floor():
    """A domain that never opted into gating must not be reported as having a target."""
    [result] = SLOValidator._evaluate_quality_counts("pipeline", _counts(good=0, quarantined=100), None)

    assert result.passed is True, "visible, but not a breach of an agreement nobody made"
    assert result.quality_min_ratio is None


# ── The wire format ─────────────────────────────────────────────────────────
# The fields existing on the model is NOT the fix — `quality_ratio` was already there and
# was never serialised. These assert the payload that actually leaves the process.
class _Registry:
    domain = "marketplace"
    system = "rideflow"
    observatory = {
        "enabled": True,
        "endpoint": "https://example.invalid/api/v1/operations/run-logs/ingest",
        "api_key": "k",
    }


@pytest.fixture
def posted(monkeypatch):
    bodies = []

    class _Resp:
        status_code = 202
        text = ""

    def _post(url, json=None, headers=None, timeout=None):  # noqa: A002
        bodies.append(json)
        return _Resp()

    import requests

    monkeypatch.setattr(requests, "post", _post)
    return bodies


def _section(bodies, kind):
    assert bodies, "nothing was posted"
    return (bodies[0].get("metadata", {}).get("slo_json") or {}).get(kind, {})


def test_the_quality_section_reaches_the_wire_with_both_numbers(posted):
    """THE DEFECT, at the boundary: this section used to be `{"pass": false}` alone."""
    emit_slo_report(_Registry(), [SLOCheckResult(
        layer="silver", entity="silver_trips", check_type="quality",
        status="fail", passed=False, quality_ratio=0.55, quality_min_ratio=0.99,
        quality_quarantine_ratio=0.45, quality_max_quarantine_ratio=0.01,
    )], environment="dev")

    section = _section(posted, "quality")
    assert section["good_ratio"] == 0.55
    assert section["min_good_ratio"] == 0.99, "the floor, on the wire"
    assert section["quarantine_ratio"] == 0.45
    assert section["max_quarantine_ratio"] == 0.01
    assert section["pass"] is False


def test_the_schedule_section_carries_the_deadline(posted):
    """`seconds` with no deadline is a number and no promise."""
    emit_slo_report(_Registry(), [SLOCheckResult(
        layer="schedule", entity="pipeline", check_type="schedule",
        status="late", passed=False, delay_minutes=39.6,
        schedule_deadline_utc="06:00",
    )], environment="dev")

    section = _section(posted, "schedule")
    assert section["deadline_utc"] == "06:00"
    assert section["seconds"] == 39.6 * 60.0


def test_a_check_with_no_declared_target_adds_no_keys(posted):
    """A domain that declared nothing must not gain an invented floor of None — the
    platform reads a present key as a reported measurement."""
    emit_slo_report(_Registry(), [SLOCheckResult(
        layer="silver", entity="silver_trips", check_type="quality",
        status="warn", passed=True,
    )], environment="dev")

    section = _section(posted, "quality")
    assert "good_ratio" not in section
    assert "min_good_ratio" not in section
