"""Erasure evidence reaches the platform, and never carries a subject identifier.

THE LIVE GAP (Fabric, 2026-09-14): `_execute_gdpr_pass` reported through `RemoteObserver`
(nothing ingests it) and wrote to the Databricks-only `/Workspace/Shared/lakelogic_logs`, so the
platform's GDPR Evidence page showed no erasure events whatever ran. Events now go to the
platform's privacy-action ingest through the same observatory configuration run logs use.
"""

import json

import pytest

from lakelogic.core import privacy_evidence as pe
from lakelogic.core.run_log import emit_slo_report

IDS = ["RDR-0001", "RDR-0002", "RDR-0002"]


def _event(**overrides):
    kwargs = dict(
        framework="gdpr",
        action="nullify",
        dry_run=False,
        contract_name="silver_riders",
        subject_column="rider_id",
        subject_ids=IDS,
        columns=["email", "full_name"],
        rows_affected=2,
        case_ref="DSR-1042",
        run_id="run-1",
        engine="spark",
    )
    kwargs.update(overrides)
    return pe.build_privacy_action_event(**kwargs)


def test_subject_ids_become_a_count_and_a_digest_and_nothing_else():
    event = _event()
    text = json.dumps(event)
    assert "RDR-0001" not in text and "RDR-0002" not in text
    assert event["subject_count"] == 2 and len(event["subjects_sha256"]) == 64
    assert event["columns"] == ["email", "full_name"]
    assert event["verification"]["verified"] is True and len(event["verification"]["evidence_digest"]) == 64


def test_a_dry_run_is_a_plan_with_a_generated_case_ref_and_no_verification():
    event = _event(dry_run=True, case_ref=None)
    assert event["mode"] == "dry_run" and event["case_ref"] == "dry-run-run-1"
    assert event["verification"] == {**event["verification"], "method": "none", "verified": False}


@pytest.mark.parametrize(
    "bad",
    [
        {"subject_ids": ["RDR-0001"]},
        {"asset": {"identifier_values": ["RDR-0001"]}},
    ],
)
def test_a_prohibited_field_anywhere_is_rejected(bad):
    event = {**_event(), **bad}
    with pytest.raises(ValueError, match="must not carry"):
        pe.validate_privacy_action_event(event)


def test_a_subject_value_that_leaks_into_any_field_is_rejected():
    with pytest.raises(ValueError, match="subject identifier"):
        _event(columns=["RDR-0001"])


@pytest.mark.parametrize(
    "endpoint, expected",
    [
        (
            "https://api.lakelogic.io/api/v1/operations/run-logs/ingest",
            "https://api.lakelogic.io/api/v1/compliance/privacy-actions/ingest",
        ),
        (
            "https://x.ngrok-free.dev/api/v1/operations/run-logs/ingest",
            "https://x.ngrok-free.dev/api/v1/compliance/privacy-actions/ingest",
        ),
        (None, None),
    ],
)
def test_the_ingest_endpoint_derives_from_the_run_log_endpoint(monkeypatch, endpoint, expected):
    monkeypatch.delenv(pe.ENV_PRIVACY_ENDPOINT, raising=False)
    assert pe.privacy_ingest_endpoint(endpoint) == expected


class _Resp:
    def __init__(self, status=202):
        self.status_code = status
        self.text = ""


OBSERVATORY = {"enabled": True, "endpoint": "https://h/api/v1/operations/run-logs/ingest", "api_key": "k"}


def test_events_are_posted_with_the_observatory_key(monkeypatch):
    import requests

    sent = []
    monkeypatch.setattr(
        requests,
        "post",
        lambda url, json=None, headers=None, timeout=None: sent.append((url, json, headers)) or _Resp(),
    )
    accepted = pe.emit_privacy_action_events(None, [_event(), _event()], observatory=OBSERVATORY)
    assert accepted == 2
    assert all(url.endswith("/compliance/privacy-actions/ingest") for url, _, _ in sent)
    assert all(headers["X-API-Key"] == "k" for _, _, headers in sent)


def test_a_platform_that_is_down_never_raises(monkeypatch):
    import requests

    def boom(*_a, **_k):
        raise ConnectionError("down")

    monkeypatch.setattr(requests, "post", boom)
    assert pe.emit_privacy_action_events(None, [_event()], observatory=OBSERVATORY) == 0


class _Result:
    def __init__(self):
        self.entity, self.layer, self.check_type, self.passed = "silver_riders", "silver", "retention", False
        self.retention_age_minutes, self.retention_limit_minutes, self.retention_period = 200000, 129600, "P90D"


class _Registry:
    observatory = OBSERVATORY
    domain, system = "marketplace", "rideflow"


def test_the_retention_check_reports_as_its_own_record_type(monkeypatch):
    import requests

    sent = []
    monkeypatch.setattr(
        requests, "post", lambda url, json=None, headers=None, timeout=None: sent.append(json) or _Resp()
    )
    emit_slo_report(_Registry(), [_Result()], environment="dev", record_type="retention_check", engine="retention")
    (payload,) = sent
    assert payload["engine"] == "retention" and payload["metadata"]["record_type"] == "retention_check"
    assert payload["metadata"]["slo_json"]["retention"]["period"] == "P90D"
