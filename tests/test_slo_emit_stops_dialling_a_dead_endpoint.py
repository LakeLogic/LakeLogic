"""One dead endpoint is ONE problem, not one per entity.

`emit_slo_report` posts one row per entity — the shape the pipeline uses, because
Data Products is keyed by dataset. But `write_run_log` posts once per invocation,
so a dead Observatory costs it a single 3s timeout and a single warning. The same
code in a 13-entity loop stalled the service-level notebook ~40s and printed the
identical "Observatory unreachable" line 13 times, which reads as 13 faults.

Once the transport has failed, the remaining rows are spooled with no network call.
A 4xx is different: it is about that one payload, so the loop keeps going.
"""

from __future__ import annotations

import types

import pytest

from lakelogic.core import run_log


def _post_timeout(_n):
    raise OSError("read timed out")


class _Result:
    def __init__(self, entity):
        self.entity, self.check_type, self.passed = entity, "freshness", True
        self.delay_minutes = self.slo_max_minutes = self.row_count = None
        self.slo_min_rows = self.slo_max_rows = self.layer = None


@pytest.fixture()
def wired(monkeypatch):
    cfg = {"enabled": True, "endpoint": "https://obs.invalid/ingest", "api_key": "k"}
    monkeypatch.setattr(run_log, "_lakelogic_version", lambda: "test")
    import lakelogic.core.observatory_spool as spool

    monkeypatch.setattr(spool, "resolve_observatory_config", lambda _x: cfg)
    spooled: list = []
    monkeypatch.setattr(spool, "spool_payload", lambda c, p: spooled.append(p))
    monkeypatch.setattr(spool, "flush_spool", lambda *a, **k: None)
    return spooled


def _registry():
    return types.SimpleNamespace(observatory={}, domain="marketplace", system="rideflow")


def _emit(monkeypatch, post, entities=("a", "b", "c")):
    calls = []

    def _post(*args, **kwargs):
        calls.append(kwargs)
        return post(len(calls))

    monkeypatch.setattr("requests.post", _post)
    accepted = run_log.emit_slo_report(_registry(), [_Result(e) for e in entities], environment="dev")
    return accepted, calls


def test_a_timeout_stops_the_loop_dialling(monkeypatch, wired):
    """THE DEFECT: every entity paid its own 3s timeout."""

    def _post(_n):
        raise OSError("read timed out")

    accepted, calls = _emit(monkeypatch, _post)
    assert accepted == 0
    # One entity, two attempts (the cold-start retry), then the breaker stops the
    # loop — NOT one 3s stall per entity, which is what the notebook actually did.
    assert len(calls) == 2, "only the first entity should reach the network"


def test_the_rows_are_still_buffered_after_the_breaker_trips(monkeypatch, wired):
    """Not dialling must not mean dropping — every row is spooled for a later run."""

    def _post(_n):
        raise OSError("read timed out")

    _emit(monkeypatch, _post)
    assert len(wired) == 3


def test_one_warning_not_one_per_entity(monkeypatch, wired):
    """loguru, not stdlib logging — caplog sees nothing here, so sink it directly."""
    from loguru import logger as _logger

    lines: list = []
    sink = _logger.add(lines.append, level="WARNING")
    try:
        _emit(monkeypatch, _post_timeout)
    finally:
        _logger.remove(sink)
    assert len([x for x in lines if "unreachable" in x]) == 1


def test_a_server_error_also_trips_the_breaker(monkeypatch, wired):
    """503 is about the server, so the next entity would hit the same wall.

    No retry here: the call RETURNED, so there was no cold start to absorb.
    """

    def _post(_n):
        return types.SimpleNamespace(status_code=503, text="down")

    accepted, calls = _emit(monkeypatch, _post)
    assert (accepted, len(calls)) == (0, 1)
    assert len(wired) == 3


def test_a_rejected_payload_does_not_stop_the_others(monkeypatch, wired):
    """400 is about THAT payload; the remaining entities must still be tried."""

    def _post(_n):
        return types.SimpleNamespace(status_code=400, text="bad field")

    accepted, calls = _emit(monkeypatch, _post)
    assert (accepted, len(calls)) == (0, 3)
    assert wired == [], "a 4xx is not retryable, so it is not buffered"


def test_a_healthy_endpoint_sends_every_row(monkeypatch, wired):
    def _post(_n):
        return types.SimpleNamespace(status_code=200, text="ok")

    accepted, calls = _emit(monkeypatch, _post)
    assert (accepted, len(calls)) == (3, 3)


# ── The pipeline's environment gating, which this path did not honour ─────────


def test_an_out_of_scope_environment_is_not_pushed(monkeypatch, wired):
    """`write_run_log` honours `environments`; this path pushed from everywhere."""
    import lakelogic.core.observatory_spool as spool

    monkeypatch.setattr(
        spool,
        "resolve_observatory_config",
        lambda _x: {
            "enabled": True,
            "endpoint": "https://obs.invalid/ingest",
            "environments": ["prod"],
        },
    )
    calls = []
    monkeypatch.setattr("requests.post", lambda *a, **k: calls.append(k))
    accepted = run_log.emit_slo_report(_registry(), [_Result("a")], environment="dev")
    assert (accepted, calls) == (0, [])


def test_an_in_scope_environment_is_pushed(monkeypatch, wired):
    import lakelogic.core.observatory_spool as spool

    monkeypatch.setattr(
        spool,
        "resolve_observatory_config",
        lambda _x: {
            "enabled": True,
            "endpoint": "https://obs.invalid/ingest",
            "environments": ["dev", "prod"],
        },
    )
    monkeypatch.setattr("requests.post", lambda *a, **k: types.SimpleNamespace(status_code=200, text="ok"))
    assert run_log.emit_slo_report(_registry(), [_Result("a")], environment="dev") == 1


def test_a_passing_check_is_still_sent(monkeypatch, wired):
    """`emit_on` must NOT transfer: a met objective is the evidence, not noise."""
    import lakelogic.core.observatory_spool as spool

    monkeypatch.setattr(
        spool,
        "resolve_observatory_config",
        lambda _x: {
            "enabled": True,
            "endpoint": "https://obs.invalid/ingest",
            "emit_on": ["failed"],
        },
    )
    monkeypatch.setattr("requests.post", lambda *a, **k: types.SimpleNamespace(status_code=200, text="ok"))
    passing = _Result("a")
    assert passing.passed is True
    assert run_log.emit_slo_report(_registry(), [passing], environment="dev") == 1


# ── The budget, and the retry that must happen before the breaker trips ──────


def test_a_cold_first_attempt_is_retried_before_condemning_the_endpoint(monkeypatch, wired):
    """DNS + TLS + tunnel wake-up on the first call must not spool the whole report."""
    state = {"n": 0}

    def _post(*a, **k):
        state["n"] += 1
        if state["n"] == 1:
            raise OSError("read timed out")
        return types.SimpleNamespace(status_code=200, text="ok")

    monkeypatch.setattr("requests.post", _post)
    accepted = run_log.emit_slo_report(_registry(), [_Result("a"), _Result("b")], environment="dev")
    assert accepted == 2, "the cold first attempt should not have tripped the breaker"


def test_two_failures_in_a_row_still_trip_the_breaker(monkeypatch, wired):
    """The retry softens a hiccup; it must not defeat the breaker on a real outage."""
    calls = []

    def _post(*a, **k):
        calls.append(1)
        raise OSError("read timed out")

    monkeypatch.setattr("requests.post", _post)
    run_log.emit_slo_report(_registry(), [_Result(x) for x in "abcd"], environment="dev")
    assert len(calls) == 2, "one entity, two attempts, then stop"


def test_the_budget_is_not_the_pipelines_three_seconds(monkeypatch, wired):
    """Reporting IS this job's purpose, so it does not inherit the pipeline's
    do-not-block-the-load budget."""
    seen = {}

    def _post(*a, **k):
        seen.update(k)
        return types.SimpleNamespace(status_code=200, text="ok")

    monkeypatch.setattr("requests.post", _post)
    run_log.emit_slo_report(_registry(), [_Result("a")], environment="dev")
    assert seen["timeout"] > 3.0


def test_the_budget_is_caller_configurable(monkeypatch, wired):
    seen = {}

    def _post(*a, **k):
        seen.update(k)
        return types.SimpleNamespace(status_code=200, text="ok")

    monkeypatch.setattr("requests.post", _post)
    run_log.emit_slo_report(_registry(), [_Result("a")], environment="dev", timeout=42.0)
    assert seen["timeout"] == 42.0
