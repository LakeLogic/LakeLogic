from __future__ import annotations

import types

from lakelogic.core import observer


def test_remote_observer_respects_env_guards(monkeypatch):
    report = {"counts": {}, "slos": {}}
    monkeypatch.delenv("LAKELOGIC_REMOTE_OBSERVER", raising=False)
    remote = observer.RemoteObserver("https://api.example")
    assert remote.report(report) is None

    monkeypatch.setenv("LAKELOGIC_REMOTE_OBSERVER", "true")
    monkeypatch.setenv("LAKELOGIC_OFFLINE", "true")
    remote = observer.RemoteObserver("https://api.example")
    assert remote.report(report) is None


def test_remote_observer_requires_url_and_swallows_errors(monkeypatch):
    debug_messages = []
    monkeypatch.setenv("LAKELOGIC_REMOTE_OBSERVER", "true")
    monkeypatch.delenv("LAKELOGIC_OFFLINE", raising=False)
    monkeypatch.delenv("LINEAGELOGIC_REPORT_URL", raising=False)
    monkeypatch.setattr(observer.logger, "debug", debug_messages.append)

    remote = observer.RemoteObserver()
    remote.report({"counts": {}, "slos": {}})
    assert any("LINEAGELOGIC_REPORT_URL not set" in message for message in debug_messages)

    class FailingClient:
        def __init__(self, timeout):
            self.timeout = timeout

        def __enter__(self):
            raise RuntimeError("network down")

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(observer.httpx, "Client", FailingClient)
    remote = observer.RemoteObserver("https://api.example")
    remote.report({"counts": {}, "slos": {}})
    assert any("Remote reporting skipped: network down" in message for message in debug_messages)


def test_remote_observer_posts_payload_and_headers(monkeypatch):
    monkeypatch.setenv("LAKELOGIC_REMOTE_OBSERVER", "true")
    monkeypatch.delenv("LAKELOGIC_OFFLINE", raising=False)
    monkeypatch.setenv("LINEAGELOGIC_API_KEY", "secret")
    sent = {}
    debug_messages = []
    monkeypatch.setattr(observer.logger, "debug", debug_messages.append)

    class FakeClient:
        def __init__(self, timeout):
            sent["timeout"] = timeout

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def post(self, url, json, headers):
            sent["url"] = url
            sent["json"] = json
            sent["headers"] = headers
            return types.SimpleNamespace(status_code=200)

    monkeypatch.setattr(observer.httpx, "Client", FakeClient)
    report = {
        "run_id": "run-1",
        "pipeline_run_id": "pipe-1",
        "contract": "contract.yaml",
        "dataset": "orders",
        "stage": "silver",
        "engine": "polars",
        "timestamp": "2026-04-19T00:00:00Z",
        "domain": "sales",
        "system": "crm",
        "data_layer": "silver",
        "source_path": "landing/orders.csv",
        "counts": {
            "source": 10,
            "total": 9,
            "good": 8,
            "quarantined": 1,
            "pre_transform_dropped": 1,
            "quarantine_ratio": 0.1,
        },
        "row_rule_failures": [{"name": "not_null"}],
        "dataset_rules": [{"name": "unique_id"}],
        "schema_drift": {"unexpected": ["new_col"]},
        "slos": {"freshness": "pass"},
        "duration_ms": 321,
        "estimated_cost": 1.23,
        "cost_currency": "USD",
        "cost_confidence": "high",
    }

    remote = observer.RemoteObserver("https://api.example/report")
    remote.report(report)

    assert sent["timeout"] == 2.0
    assert sent["url"] == "https://api.example/report"
    assert sent["headers"]["Authorization"] == "Bearer secret"
    assert sent["headers"]["X-LakeLogic-Version"] == observer.__version__
    assert sent["json"]["metrics"]["good"] == 8
    assert sent["json"]["cost"]["currency"] == "USD"
    assert any("Successfully reported metrics" in message for message in debug_messages)