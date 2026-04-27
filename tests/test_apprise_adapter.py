from __future__ import annotations

import builtins
import sys
import types

import pytest

from lakelogic.notifications import apprise_adapter as aa


def test_collect_targets_requires_values_and_resolves_entries(monkeypatch):
    warnings = []
    monkeypatch.setattr(aa.logger, "warning", warnings.append)
    monkeypatch.setattr(aa, "_resolve_value", lambda url, config: "" if url == "drop" else f"resolved:{url}")

    with pytest.raises(ValueError):
        aa.AppriseAdapter._collect_targets({})

    targets = aa.AppriseAdapter._collect_targets({"target": "one", "targets": ["two", "drop", None]})
    assert targets == ["resolved:one", "resolved:two"]
    assert any("resolved to empty" in message for message in warnings)


def test_apprise_adapter_send_detects_notify_type_and_attachments(monkeypatch):
    sent = {}
    infos = []
    warnings = []
    monkeypatch.setattr(aa.logger, "info", infos.append)
    monkeypatch.setattr(aa.logger, "warning", warnings.append)

    class FakeAttachment:
        def __init__(self):
            self.paths = []

        def add(self, path):
            self.paths.append(path)

    class FakeApprise:
        def __init__(self):
            self.added = []

        def add(self, url):
            self.added.append(url)

        def notify(self, title, body, notify_type, attach):
            sent["title"] = title
            sent["body"] = body
            sent["notify_type"] = notify_type
            sent["attach"] = attach.paths if attach else None
            return False

    fake_apprise = types.SimpleNamespace(
        Apprise=FakeApprise,
        AppriseAttachment=FakeAttachment,
        NotifyType=types.SimpleNamespace(FAILURE="failure", WARNING="warning", SUCCESS="success", INFO="info"),
    )
    monkeypatch.setitem(sys.modules, "apprise", fake_apprise)

    adapter = aa.AppriseAdapter({"target": "slack://one", "targets": ["mailto://two"]})
    adapter.send("body", subject="Failure breach", attach=["report.json"])
    adapter.send("body", subject="Quarantine warning")
    adapter.send("body", subject="Completed successfully")
    adapter.send("body", subject="Neutral update")
    adapter.send("body", subject="Routine update", notify_type="custom")

    assert sent["title"] == "Routine update"
    assert sent["notify_type"] == "custom"
    assert sent["attach"] is None
    assert any("Sending Apprise notification to 2 target(s)" in message for message in infos)
    assert any("Apprise returned failure" in message for message in warnings)


def test_apprise_adapter_send_requires_package(monkeypatch):
    original_import = builtins.__import__

    def blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "apprise":
            raise ImportError("missing")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", blocked_import)
    adapter = aa.AppriseAdapter({"target": "slack://one"})

    with pytest.raises(ImportError):
        adapter.send("body")