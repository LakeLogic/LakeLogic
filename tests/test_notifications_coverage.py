from __future__ import annotations

import base64
import builtins
import json
import os
import sys
import types
from pathlib import Path
from urllib.error import URLError

import pytest

from lakelogic.notifications import base as nb


def test_get_list_none():
    ownership = {"contacts": [{"name": "A", "email": None}]}
    res = nb.resolve_ownership_contacts(ownership, "failure")
    assert len(res) == 0


def test_notification_adapter_base_send():
    class DummyAdapter(nb.NotificationAdapter):
        def send(self, message: str, subject: str = "LakeLogic Alert"):
            super().send(message, subject)

    DummyAdapter({}).send("msg")


def test_keyvault_missing_deps(monkeypatch):
    monkeypatch.setitem(sys.modules, "azure", None)
    with pytest.raises(ValueError, match="Azure Key Vault dependencies not installed"):
        nb._resolve_keyvault_secret("my-secret", {"key_vault_url": "https://fake.vault.azure.net/"})


def test_keyvault_exception(monkeypatch):
    fake_identity = types.ModuleType("azure.identity")
    fake_identity.DefaultAzureCredential = lambda: None
    monkeypatch.setitem(sys.modules, "azure.identity", fake_identity)
    
    fake_secrets = types.ModuleType("azure.keyvault.secrets")
    class FakeClient:
        def __init__(self, **kwargs): pass
        def get_secret(self, name): raise RuntimeError("kv-boom")
    fake_secrets.SecretClient = FakeClient
    monkeypatch.setitem(sys.modules, "azure.keyvault.secrets", fake_secrets)
    
    with pytest.raises(ValueError, match="kv-boom"):
        nb._resolve_keyvault_secret("my-secret", {"key_vault_url": "https://fake.vault.azure.net/"})


def test_aws_missing_deps(monkeypatch):
    monkeypatch.setitem(sys.modules, "boto3", None)
    with pytest.raises(ValueError, match="AWS Secrets Manager dependencies not installed"):
        nb._resolve_aws_secret("my-secret", {"aws_region": "us-east-1"})


def test_aws_cached_and_no_profile_and_binary(monkeypatch):
    fake_boto3 = types.ModuleType("boto3")
    
    class FakeClient:
        def __init__(self, **kwargs): pass
        def get_secret_value(self, SecretId):
            if SecretId == "str-secret":
                return {"SecretString": "str-value"}
            if SecretId == "bin-secret":
                return {"SecretBinary": base64.b64encode(b"bin-value")}
            if SecretId == "empty-secret":
                return {}
            raise RuntimeError("aws-boom")
            
    fake_session_mod = types.ModuleType("boto3.session")
    fake_session_mod.Session = lambda profile_name=None: types.SimpleNamespace(
        client=lambda service, region_name, endpoint_url: FakeClient()
    )
    fake_boto3.session = fake_session_mod
    monkeypatch.setitem(sys.modules, "boto3", fake_boto3)
    
    # Test string secret
    res = nb._resolve_aws_secret("str-secret", {"aws_region": "us-east-1"})
    assert res == "str-value"
    
    # Test binary secret
    res = nb._resolve_aws_secret("bin-secret", {"aws_region": "us-east-1"})
    assert res == "bin-value"
    
    # Test cached
    assert nb._resolve_aws_secret("bin-secret", {"aws_region": "us-east-1"}) == "bin-value"
    
    # Test profile and empty secret
    res = nb._resolve_aws_secret("empty-secret", {"aws_region": "us-east-1", "aws_profile": "my-prof"})
    assert res is None
    
    # Test exception
    with pytest.raises(ValueError, match="aws-boom"):
        nb._resolve_aws_secret("my-secret", {"aws_region": "us-east-1"})


def test_gcp_missing_deps_and_missing_project(monkeypatch):
    monkeypatch.delenv("GCP_PROJECT", raising=False)
    monkeypatch.delenv("GOOGLE_CLOUD_PROJECT", raising=False)
    monkeypatch.setitem(sys.modules, "google", None)
    with pytest.raises(ValueError, match="GCP Secret Manager dependencies not installed"):
        nb._resolve_gcp_secret("my-secret", {"gcp_project": "test-project"})
        
    fake_google = types.ModuleType("google")
    fake_cloud = types.ModuleType("google.cloud")
    fake_secretmanager = types.ModuleType("google.cloud.secretmanager")
    
    fake_cloud.secretmanager = fake_secretmanager
    fake_google.cloud = fake_cloud
    
    monkeypatch.setitem(sys.modules, "google", fake_google)
    monkeypatch.setitem(sys.modules, "google.cloud", fake_cloud)
    monkeypatch.setitem(sys.modules, "google.cloud.secretmanager", fake_secretmanager)
    
    with pytest.raises(ValueError, match="GCP project not provided"):
        nb._resolve_gcp_secret("my-secret", {})


def test_gcp_cached_and_exception(monkeypatch):
    fake_google = types.ModuleType("google")
    fake_cloud = types.ModuleType("google.cloud")
    fake_secretmanager = types.ModuleType("google.cloud.secretmanager")
    
    class FakeClient:
        def access_secret_version(self, name):
            if "fail" in name:
                raise RuntimeError("gcp-boom")
            return types.SimpleNamespace(payload=types.SimpleNamespace(data=b"gcp-value"))
            
    fake_secretmanager.SecretManagerServiceClient = FakeClient
    fake_cloud.secretmanager = fake_secretmanager
    fake_google.cloud = fake_cloud
    
    monkeypatch.setitem(sys.modules, "google", fake_google)
    monkeypatch.setitem(sys.modules, "google.cloud", fake_cloud)
    monkeypatch.setitem(sys.modules, "google.cloud.secretmanager", fake_secretmanager)
    
    # Test success
    res = nb._resolve_gcp_secret("my-secret", {"gcp_project": "test-project"})
    assert res == "gcp-value"
    
    # Test cached
    assert nb._resolve_gcp_secret("my-secret", {"gcp_project": "test-project"}) == "gcp-value"
    
    # Test exception
    with pytest.raises(ValueError, match="gcp-boom"):
        nb._resolve_gcp_secret("fail-secret", {"gcp_project": "test-project"})


def test_vault_missing_deps_and_url(monkeypatch):
    monkeypatch.setitem(sys.modules, "hvac", None)
    with pytest.raises(ValueError, match="HashiCorp Vault dependencies not installed"):
        nb._resolve_vault_secret("my-secret", {"vault_url": "http://vault"})
        
    fake_hvac = types.ModuleType("hvac")
    monkeypatch.setitem(sys.modules, "hvac", fake_hvac)
    
    with pytest.raises(ValueError, match="Vault URL not provided"):
        nb._resolve_vault_secret("my-secret", {})


def test_vault_cached_and_exception(monkeypatch):
    fake_hvac = types.ModuleType("hvac")
    
    class FakeClient:
        def __init__(self, **kwargs):
            self.secrets = types.SimpleNamespace(
                kv=types.SimpleNamespace(
                    v1=types.SimpleNamespace(
                        read_secret=self.read_secret_v1
                    )
                )
            )
            
        def read_secret_v1(self, path):
            if "fail" in path:
                raise RuntimeError("vault-boom")
            return {"data": {"myfield": "vault-value"}}
            
    fake_hvac.Client = FakeClient
    monkeypatch.setitem(sys.modules, "hvac", fake_hvac)
    
    # Test success (V1 with field)
    res = nb._resolve_vault_secret("my-secret#myfield", {"vault_url": "http://vault", "vault_kv_version": "1"})
    assert res == "vault-value"
    
    # Test cached
    assert nb._resolve_vault_secret("my-secret#myfield", {"vault_url": "http://vault", "vault_kv_version": "1"}) == "vault-value"
    
    # Test exception
    with pytest.raises(ValueError, match="vault-boom"):
        nb._resolve_vault_secret("fail-secret", {"vault_url": "http://vault", "vault_kv_version": "1"})


def test_resolve_env_only():
    assert nb._resolve_env_only(None) is None
    assert nb._resolve_env_only("just text") == "just text"
    
    os.environ["DUMMY_ENV"] = "abc"
    assert nb._resolve_env_only("env:DUMMY_ENV") == "abc"
    assert nb._resolve_env_only("${ENV:DUMMY_ENV}") == "abc"
    
    assert nb._resolve_env_only("env:MISSING_ENV") is None


def test_local_secrets(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "cryptography.fernet", None)
    with pytest.raises(ValueError, match="Local secrets require cryptography"):
        nb._load_local_secrets({"secrets_file": str(tmp_path / "missing.enc"), "secrets_key": "some-key"})
        
    fake_fernet_mod = types.ModuleType("cryptography.fernet")
    class FakeFernetError(Exception): pass
    class FakeFernet:
        def __init__(self, key):
            if key == b"bad-key": raise FakeFernetError()
        def decrypt(self, token):
            if token == b"bad-token": raise FakeFernetError()
            if token == b"not-json": return b"not-json"
            if token == b"not-dict": return b'["list"]'
            return b'{"my-secret": "local-value"}'
            
    fake_fernet_mod.Fernet = FakeFernet
    fake_fernet_mod.InvalidToken = FakeFernetError
    monkeypatch.setitem(sys.modules, "cryptography.fernet", fake_fernet_mod)
    
    # Write files
    enc_path = tmp_path / "secrets.enc"
    enc_path.write_bytes(b"good-token")
    bad_path = tmp_path / "bad.enc"
    bad_path.write_bytes(b"bad-token")
    not_json_path = tmp_path / "not-json.enc"
    not_json_path.write_bytes(b"not-json")
    not_dict_path = tmp_path / "not-dict.enc"
    not_dict_path.write_bytes(b"not-dict")
    
    monkeypatch.delitem(nb._LOCAL_SECRET_CACHE, (str(enc_path), "good-key"), raising=False)
    
    # Test success
    res = nb._resolve_local_secret("my-secret", {"secrets_file": str(enc_path), "secrets_key": "good-key"})
    assert res == "local-value"
    
    # Test base_path resolution
    res2 = nb._resolve_local_secret("my-secret", {"secrets_file": "secrets.enc", "secrets_key": "good-key", "_base_path": str(tmp_path)})
    assert res2 == "local-value"
    
    # Test missing secrets_file
    with pytest.raises(ValueError, match="secrets_file is required"):
        nb._load_local_secrets({})
        
    # Test missing secrets_key
    monkeypatch.delenv("LAKELOGIC_SECRETS_KEY", raising=False)
    with pytest.raises(ValueError, match="secrets_key is required"):
        nb._load_local_secrets({"secrets_file": str(enc_path)})
        
    # Test file not found
    with pytest.raises(ValueError, match="Local secrets file not found"):
        nb._load_local_secrets({"secrets_file": str(tmp_path / "nope.enc"), "secrets_key": "good-key"})
    
    # Test missing secret
    with pytest.raises(ValueError, match="not found in secrets file"):
        nb._resolve_local_secret("missing", {"secrets_file": str(enc_path), "secrets_key": "good-key"})
        
    # Test invalid token
    with pytest.raises(ValueError, match="Invalid secrets_key"):
        nb._load_local_secrets({"secrets_file": str(bad_path), "secrets_key": "good-key"})
        
    # Test not JSON
    with pytest.raises(ValueError, match="not valid JSON"):
        nb._load_local_secrets({"secrets_file": str(not_json_path), "secrets_key": "good-key"})
        
    # Test not dict
    with pytest.raises(ValueError, match="must be a JSON object"):
        nb._load_local_secrets({"secrets_file": str(not_dict_path), "secrets_key": "good-key"})


def test_missing_env_warning():
    res = nb._resolve_value("env:MISSING_VAR_LOG_ME", {})
    assert res is None
    res = nb._resolve_value("${ENV:MISSING_VAR_LOG_ME2}", {})
    assert res is None


def test_validate_invalid_type():
    with pytest.raises(ValueError, match="Unsupported notification type"):
        nb.validate_notification_config("invalid_type", {})


def test_jinja_template_exceptions_1(monkeypatch):
    monkeypatch.setitem(sys.modules, "jinja2", None)
    with pytest.raises(ValueError, match="Notification templates require 'jinja2'"):
        nb._render_jinja_template("hi", {}, "test")


def test_jinja_template_exceptions_2():
    with pytest.raises(ValueError, match="Failed to render notification"):
        nb._render_jinja_template("{{ missing_var }}", {}, "test")


def test_jinja_template_exceptions_3():
    with pytest.raises(ValueError, match="Failed to render notification"):
        nb._render_jinja_template("{% extends 'base.html' %}{{ missing_var }}", {}, "test")


def test_load_builtin_unknown():
    assert nb._load_builtin_template("non_existent_event_12345") is None


def test_jinja_standalone_full():
    context = {
        "event": "quarantine",
        "message": "Hello",
        "contract": {"title": "T", "version": "1", "dataset": "D", "domain": "dom", "system": "sys", "owner": "own"}
    }
    res = nb._render_jinja_standalone(context)
    assert "• *Domain:* dom" in res
    assert "• *System:* sys" in res
    assert "• *Owner:* own" in res


def test_adapters_missing_configs():
    # SMTP
    ad = nb.SMTPAdapter({})
    ad.send("msg")  # Should just skip and log warning
    
    # SendGrid
    ad = nb.SendGridAdapter({})
    ad.send("msg")
    # SendGrid with name
    ad = nb.SendGridAdapter({"api_key": "x", "from_email": "x", "target": "x", "from_name": "MyName"})
    with monkeypatch_urlopen():
        ad.send("msg")
        
    # Slack
    ad = nb.SlackAdapter({})
    ad.send("msg")
    
    # Teams
    ad = nb.TeamsAdapter({})
    ad.send("msg")
    
    # Webhook
    ad = nb.WebhookAdapter({})
    ad.send("msg")


import contextlib
@contextlib.contextmanager
def monkeypatch_urlopen():
    original = nb.urlopen
    class DummyResp:
        def __enter__(self): return self
        def __exit__(self, *args): pass
        def read(self): return b""
    nb.urlopen = lambda req, timeout: DummyResp()
    try:
        yield
    finally:
        nb.urlopen = original


def test_console_adapter_encode_error(monkeypatch, capsys):
    class BadStdout:
        def __init__(self):
            self.buffer = types.SimpleNamespace(
                write=lambda b: None,
                flush=lambda: None
            )
            self.encoding = "cp1252"
        def flush(self): pass
        def write(self, s): raise UnicodeEncodeError("ascii", "", 0, 1, "test")
        
    monkeypatch.setattr(sys, "stdout", BadStdout())
    
    ad = nb.ConsoleAdapter({})
    ad.send("hello 🚀") # Should use buffer.write fallback


def test_validate_console():
    nb.validate_notification_config('console', {})
