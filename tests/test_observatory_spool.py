"""Tests for Observatory telemetry spool + bounded retry (resilience hardening).

Covers: quarantine sample is never persisted, failed pushes are buffered,
replay deletes on success / drops 4xx / stops on 5xx & network error, and the
ring-buffer + TTL caps hold.
"""
from __future__ import annotations

import json
import os
import time

from lakelogic.core import observatory_spool as sp


# --------------------------------------------------------------------------- #
# fakes                                                                        #
# --------------------------------------------------------------------------- #
class _Resp:
    def __init__(self, status_code):
        self.status_code = status_code
        self.text = f"status={status_code}"


class _FakeRequests:
    """Configurable fake for requests.post — a status sequence or an exception."""
    def __init__(self, statuses=None, raise_exc=False):
        self.statuses = list(statuses or [])
        self.raise_exc = raise_exc
        self.calls = 0

    def post(self, *a, **k):
        self.calls += 1
        if self.raise_exc:
            raise ConnectionError("saas unreachable")
        code = self.statuses.pop(0) if self.statuses else 200
        return _Resp(code)


def _cfg(tmp_path, **spool):
    base = {"dir": str(tmp_path)}
    base.update(spool)
    return {"enabled": True, "spool": base}


def _payload(run_id="run-1"):
    return {
        "contract_name": "gold_trips",
        "status": "failed",
        "metadata": {"run_id": run_id, "domain": "marketplace"},
        "quarantined_rows": [{"id": 1, "pii_email": "a@b.com"}],  # must NOT be spooled
    }


def _files(tmp_path):
    return sorted(tmp_path.glob("*.json"))


# --------------------------------------------------------------------------- #
# privacy                                                                      #
# --------------------------------------------------------------------------- #
def test_strip_removes_quarantine_sample():
    safe = sp.strip_for_spool(_payload())
    assert "quarantined_rows" not in safe
    assert safe["contract_name"] == "gold_trips"
    assert safe["_spooled"] is True


def test_spooled_file_never_contains_quarantine_rows(tmp_path):
    assert sp.spool_payload(_cfg(tmp_path), _payload()) is True
    files = _files(tmp_path)
    assert len(files) == 1
    data = json.loads(files[0].read_text())
    assert "quarantined_rows" not in data
    assert data["metadata"]["run_id"] == "run-1"


# --------------------------------------------------------------------------- #
# spool basics                                                                 #
# --------------------------------------------------------------------------- #
def test_spool_disabled_writes_nothing(tmp_path):
    cfg = {"enabled": True, "spool": {"dir": str(tmp_path), "enabled": False}}
    assert sp.spool_payload(cfg, _payload()) is False
    assert _files(tmp_path) == []


def test_ring_buffer_cap_enforced(tmp_path):
    cfg = _cfg(tmp_path, max_files=3)
    for i in range(6):
        sp.spool_payload(cfg, _payload(f"run-{i}"))
        time.sleep(0.01)  # distinct mtimes for deterministic oldest-drop
    assert len(_files(tmp_path)) <= 3


def test_ttl_purges_stale_files(tmp_path):
    cfg = _cfg(tmp_path, ttl_days=7)
    sp.spool_payload(cfg, _payload("old"))
    stale = _files(tmp_path)[0]
    old = time.time() - 8 * 86400  # older than 7d
    os.utime(stale, (old, old))
    sp._enforce_caps(cfg, tmp_path)
    assert _files(tmp_path) == []


# --------------------------------------------------------------------------- #
# flush / replay                                                               #
# --------------------------------------------------------------------------- #
def test_flush_replays_and_deletes_on_success(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    sp.spool_payload(cfg, _payload("a"))
    sp.spool_payload(cfg, _payload("b"))
    fake = _FakeRequests(statuses=[200, 200])
    monkeypatch.setattr(sp, "requests", fake)
    sent = sp.flush_spool(cfg, "https://saas/ingest", {"X-API-Key": "k"})
    assert sent == 2
    assert _files(tmp_path) == []


def test_flush_stops_on_server_error_keeps_files(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    sp.spool_payload(cfg, _payload("a"))
    sp.spool_payload(cfg, _payload("b"))
    fake = _FakeRequests(statuses=[503, 200])  # first still failing → stop
    monkeypatch.setattr(sp, "requests", fake)
    sent = sp.flush_spool(cfg, "https://saas/ingest", {})
    assert sent == 0
    assert len(_files(tmp_path)) == 2  # nothing dropped; retried later
    assert fake.calls == 1  # stopped after the first failure


def test_flush_drops_4xx(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    sp.spool_payload(cfg, _payload("bad"))
    fake = _FakeRequests(statuses=[400])  # bad payload/auth → drop, don't retry forever
    monkeypatch.setattr(sp, "requests", fake)
    sent = sp.flush_spool(cfg, "https://saas/ingest", {})
    assert sent == 0
    assert _files(tmp_path) == []


def test_flush_keeps_files_on_network_error(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    sp.spool_payload(cfg, _payload("a"))
    fake = _FakeRequests(raise_exc=True)
    monkeypatch.setattr(sp, "requests", fake)
    sent = sp.flush_spool(cfg, "https://saas/ingest", {})
    assert sent == 0
    assert len(_files(tmp_path)) == 1


def test_flush_noop_when_spool_empty(tmp_path, monkeypatch):
    fake = _FakeRequests()
    monkeypatch.setattr(sp, "requests", fake)
    assert sp.flush_spool(_cfg(tmp_path), "https://saas/ingest", {}) == 0
    assert fake.calls == 0


# --------------------------------------------------------------------------- #
# CLI: `lakelogic observatory flush`                                          #
# --------------------------------------------------------------------------- #
def test_cli_flush_drains_spool(tmp_path, monkeypatch):
    from typer.testing import CliRunner
    from lakelogic.cli.main import app

    cfg = _cfg(tmp_path)
    sp.spool_payload(cfg, _payload("a"))
    sp.spool_payload(cfg, _payload("b"))
    monkeypatch.setattr(sp, "requests", _FakeRequests(statuses=[200, 200]))

    result = CliRunner().invoke(
        app,
        ["observatory", "flush", "--endpoint", "https://saas/ingest",
         "--api-key", "llc_sk_x", "--spool-dir", str(tmp_path)],
    )
    assert result.exit_code == 0
    assert "Replayed 2" in result.stdout
    assert _files(tmp_path) == []


def test_cli_flush_requires_endpoint(tmp_path):
    from typer.testing import CliRunner
    from lakelogic.cli.main import app

    result = CliRunner().invoke(app, ["observatory", "flush", "--spool-dir", str(tmp_path)])
    assert result.exit_code != 0  # BadParameter — no endpoint configured


# --------------------------------------------------------------------------- #
# B1: frictionless connection — env-var config resolution                     #
# --------------------------------------------------------------------------- #
def _clear_cloud_env(monkeypatch):
    for k in ("LAKELOGIC_CLOUD_API_KEY", "LAKELOGIC_CLOUD_ENDPOINT"):
        monkeypatch.delenv(k, raising=False)


def test_resolve_env_key_alone_connects_to_hosted_default(monkeypatch):
    _clear_cloud_env(monkeypatch)
    monkeypatch.setenv("LAKELOGIC_CLOUD_API_KEY", "llc_sk_abc123")
    cfg = sp.resolve_observatory_config(None)  # NO yaml at all
    assert cfg["enabled"] is True
    assert cfg["api_key"] == "llc_sk_abc123"
    assert cfg["endpoint"] == sp.DEFAULT_CLOUD_ENDPOINT
    # env-connected path defaults the quarantine sample on (feeds diagnosis)
    assert cfg["include_quarantine_sample"] is True


def test_resolve_custom_endpoint_env(monkeypatch):
    _clear_cloud_env(monkeypatch)
    monkeypatch.setenv("LAKELOGIC_CLOUD_API_KEY", "llc_sk_x")
    monkeypatch.setenv("LAKELOGIC_CLOUD_ENDPOINT", "https://stage.example/ingest")
    cfg = sp.resolve_observatory_config(None)
    assert cfg["endpoint"] == "https://stage.example/ingest"


def test_resolve_explicit_disable_is_honored(monkeypatch):
    _clear_cloud_env(monkeypatch)
    monkeypatch.setenv("LAKELOGIC_CLOUD_API_KEY", "llc_sk_x")
    cfg = sp.resolve_observatory_config({"enabled": False})
    assert cfg["enabled"] is False  # YAML opt-out always wins


def test_resolve_yaml_key_takes_precedence_over_env(monkeypatch):
    _clear_cloud_env(monkeypatch)
    monkeypatch.setenv("LAKELOGIC_CLOUD_API_KEY", "env_key")
    cfg = sp.resolve_observatory_config({"api_key": "yaml_key", "endpoint": "https://y/ingest"})
    assert cfg["api_key"] == "yaml_key"
    # explicit YAML path does NOT auto-enable samples (metadata-only default)
    assert "include_quarantine_sample" not in cfg


def test_resolve_env_interpolation(monkeypatch):
    _clear_cloud_env(monkeypatch)
    monkeypatch.setenv("MY_SECRET", "llc_sk_interp")
    cfg = sp.resolve_observatory_config({"enabled": True, "endpoint": "https://y/ingest",
                                         "api_key": "${MY_SECRET}"})
    assert cfg["api_key"] == "llc_sk_interp"


def test_resolve_interpolation_unset_is_inert(monkeypatch):
    _clear_cloud_env(monkeypatch)
    cfg = sp.resolve_observatory_config({"api_key": "${NOPE_UNSET}", "endpoint": "https://y/ingest"})
    assert cfg["api_key"] == ""  # committed placeholder stays inert until env is set


def test_resolve_nothing_configured_is_disabled(monkeypatch):
    _clear_cloud_env(monkeypatch)
    cfg = sp.resolve_observatory_config(None)
    assert not cfg.get("enabled")
    assert not cfg.get("api_key")


def test_cli_status_reports_connected(monkeypatch, tmp_path):
    from typer.testing import CliRunner
    from lakelogic.cli.main import app
    _clear_cloud_env(monkeypatch)
    monkeypatch.setenv("LAKELOGIC_CLOUD_API_KEY", "llc_sk_status123456")
    result = CliRunner().invoke(app, ["observatory", "status", "--spool-dir", str(tmp_path)])
    assert result.exit_code == 0
    assert "connected : yes" in result.stdout
    assert "llc_sk" in result.stdout  # masked key shown
    assert "llc_sk_status123456" not in result.stdout  # full key never printed


def test_cli_status_reports_not_connected(monkeypatch, tmp_path):
    from typer.testing import CliRunner
    from lakelogic.cli.main import app
    _clear_cloud_env(monkeypatch)
    result = CliRunner().invoke(app, ["observatory", "status", "--spool-dir", str(tmp_path)])
    assert result.exit_code == 0
    assert "connected : no" in result.stdout
