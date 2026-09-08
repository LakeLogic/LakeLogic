"""The posted rows must say which check RUN they belong to.

WHY. `emit_slo_report` splits results one row per entity — right for Data Products,
which is keyed by dataset — but that left the platform with N independent posts and
nothing joining them. It could not say "14 of 51 objectives breached", because each row
sees one entity and never the 51; and it could not send one notification per check run
instead of one per failing entity, which is the difference between 15 messages and 114.

`run_checks()` has minted a `check_run_id` all along; it just never left the Delta table.
"""

import json

import pytest

from lakelogic.core.run_log import emit_slo_report


class _Result:
    def __init__(self, entity, check_type="freshness", passed=True):
        self.entity = entity
        self.check_type = check_type
        self.passed = passed
        self.layer = "silver"


class _Report:
    """Shaped like `SLOReport` — what `run_checks()` returns."""

    def __init__(self, results, check_run_id):
        self.results = results
        self.check_run_id = check_run_id


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
    """Capture the bodies instead of sending them."""
    bodies = []

    class _Resp:
        status_code = 202
        text = ""

    def _post(url, json=None, headers=None, timeout=None):
        bodies.append(json)
        return _Resp()

    import requests

    monkeypatch.setattr(requests, "post", _post)
    return bodies


RESULTS = [
    _Result("silver_trips", passed=False),
    _Result("silver_riders", passed=True),
    _Result("gold_dim_driver", passed=True),
]


def test_every_row_carries_the_check_run_id(posted):
    """THE GAP: without this the platform cannot group the posts back together."""
    emit_slo_report(_Registry(), _Report(RESULTS, "run-abc"), environment="dev")

    assert len(posted) == 3
    assert {b["metadata"]["check_run_id"] for b in posted} == {"run-abc"}


def test_every_row_carries_the_runs_own_totals(posted):
    """The denominator only exists on THIS side of the wire — each posted row sees one
    entity. "14 of 51" is unanswerable without it."""
    emit_slo_report(_Registry(), _Report(RESULTS, "run-abc"), environment="dev")

    meta = posted[0]["metadata"]
    assert meta["check_run_objectives"] == 3
    assert meta["check_run_breached"] == 1


def test_a_bare_results_list_still_works(posted):
    """Every existing caller passes `report.results`. They must keep working, with the
    id simply absent rather than the call failing."""
    emit_slo_report(_Registry(), RESULTS, environment="dev")

    assert len(posted) == 3
    assert posted[0]["metadata"]["check_run_id"] is None
    # The totals do not depend on the report object.
    assert posted[0]["metadata"]["check_run_objectives"] == 3


def test_an_explicit_check_run_id_wins(posted):
    """A caller holding only the list can still pass the id."""
    emit_slo_report(_Registry(), RESULTS, environment="dev", check_run_id="run-xyz")

    assert posted[0]["metadata"]["check_run_id"] == "run-xyz"


def test_one_row_per_entity_is_unchanged(posted):
    """The split is deliberate — Data Products is keyed by dataset, so a single
    combined row could not say which product met its objective. Adding the grouping id
    must not tempt anyone into collapsing it."""
    emit_slo_report(_Registry(), _Report(RESULTS, "run-abc"), environment="dev")

    assert [b["contract_name"] for b in posted] == [
        "silver_trips",
        "silver_riders",
        "gold_dim_driver",
    ]
    assert json.dumps(posted[0]["metadata"]["slo_json"])  # still per-entity evidence
