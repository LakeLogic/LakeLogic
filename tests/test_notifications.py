import json
import sys
import types

import pytest

from lakelogic.notifications import base as nb
from lakelogic.notifications.base import (
    ConsoleAdapter,
    SendGridAdapter,
    SMTPAdapter,
    get_notification_adapter,
    resolve_config_secrets,
    resolve_ownership_contacts,
)


def test_env_resolution(monkeypatch):
    monkeypatch.setenv("SMTP_PASS", "secret-value")
    config = {"smtp_password": "${ENV:SMTP_PASS}", "target": "alerts@example.com"}
    resolved = resolve_config_secrets(config)
    assert resolved["smtp_password"] == "secret-value"


def test_env_prefix_resolution(monkeypatch):
    monkeypatch.setenv("SENDGRID_API_KEY", "sg-key")
    config = {"api_key": "env:SENDGRID_API_KEY"}
    resolved = resolve_config_secrets(config)
    assert resolved["api_key"] == "sg-key"


def test_non_secret_passthrough():
    config = {"target": "https://hooks.slack.com/services/xxx"}
    resolved = resolve_config_secrets(config)
    assert resolved["target"] == "https://hooks.slack.com/services/xxx"


def test_validation_missing_fields():
    config = {"type": "smtp", "target": "alerts@example.com"}
    with pytest.raises(ValueError):
        get_notification_adapter("smtp", config)


def test_local_secrets_resolution(tmp_path, monkeypatch):
    cryptography = pytest.importorskip("cryptography.fernet")
    from cryptography.fernet import Fernet

    secrets = {"smtp_password": "secret-local"}
    key = Fernet.generate_key()
    token = Fernet(key).encrypt(json.dumps(secrets).encode("utf-8"))

    secrets_file = tmp_path / "secrets.enc"
    secrets_file.write_bytes(token)

    monkeypatch.setenv("LAKELOGIC_SECRETS_KEY", key.decode("utf-8"))
    config = {
        "smtp_password": "local:smtp_password",
        "secrets_file": str(secrets_file),
        "smtp_host": "smtp.example.com",
        "from_email": "lakelogic@example.com",
        "target": "alerts@example.com",
    }

    resolved = resolve_config_secrets(config)
    assert resolved["smtp_password"] == "secret-local"


def test_email_adapter_upgrades_to_sendgrid_when_env_present(monkeypatch):
    monkeypatch.setenv("SENDGRID_API_KEY", "sg-key")
    monkeypatch.setenv("EMAIL_FROM_ADDRESS", "alerts@lakelogic.dev")
    monkeypatch.setenv("EMAIL_FROM_NAME", "LakeLogic Alerts")

    adapter = get_notification_adapter("email", {"target": "ops@example.com"})

    assert isinstance(adapter, SendGridAdapter)
    assert adapter.config["api_key"] == "sg-key"
    assert adapter.config["from_email"] == "alerts@lakelogic.dev"
    assert adapter.config["from_name"] == "LakeLogic Alerts"
    assert adapter.config["target"] == "ops@example.com"


def test_resolve_ownership_contacts_filters_roles_and_non_actionable_targets():
    ownership = {
        "contacts": [
            {
                "name": "OnCall",
                "role": "platform",
                "email": ["platform@example.com", "backup@example.com"],
                "slack": ["#platform-alerts", "https://hooks.slack.com/services/real"],
                "teams": "https://teams.example/webhook",
                "webhook": "https://ops.example/webhook",
            },
            {
                "name": "Analyst",
                "role": "analytics",
                "email": "analytics@example.com",
            },
        ]
    }

    channels = resolve_ownership_contacts(ownership, "failure", roles=["platform"])

    assert [channel["type"] for channel in channels] == [
        "email",
        "email",
        "slack",
        "teams",
        "webhook",
    ]
    assert all(channel["target"] != "#platform-alerts" for channel in channels)
    assert all(channel["_source"] == "ownership.contacts[OnCall]" for channel in channels)


def test_post_json_sends_payload_and_merges_headers(monkeypatch):
    captured = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            captured["read_called"] = True
            return b"ok"

    def fake_urlopen(request, timeout=0):
        captured["url"] = request.full_url
        captured["data"] = request.data
        captured["headers"] = dict(request.header_items())
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(nb, "urlopen", fake_urlopen)
    nb._post_json("https://example.test/hook", {"ok": True}, headers={"Authorization": "Bearer token"})

    assert captured["url"] == "https://example.test/hook"
    assert json.loads(captured["data"].decode("utf-8")) == {"ok": True}
    assert captured["headers"]["Content-type"] == "application/json"
    assert captured["headers"]["Authorization"] == "Bearer token"
    assert captured["timeout"] == 10
    assert captured["read_called"] is True


def test_keyvault_secret_resolution_and_cache(monkeypatch):
    calls = []

    class FakeSecretClient:
        def __init__(self, vault_url=None, credential=None):
            self.vault_url = vault_url
            self.credential = credential

        def get_secret(self, secret_name):
            calls.append(secret_name)
            return types.SimpleNamespace(value=f"kv:{secret_name}")

    monkeypatch.setenv("AZURE_KEY_VAULT_URL", "https://vault.example")
    monkeypatch.setitem(sys.modules, "azure.identity", types.SimpleNamespace(DefaultAzureCredential=lambda: "cred"))
    monkeypatch.setitem(sys.modules, "azure.keyvault.secrets", types.SimpleNamespace(SecretClient=FakeSecretClient))
    nb._SECRET_CACHE.clear()

    config = {}
    assert nb._resolve_keyvault_secret("smtp-pass", config) == "kv:smtp-pass"
    assert nb._resolve_keyvault_secret("smtp-pass", config) == "kv:smtp-pass"
    assert calls == ["smtp-pass"]

    monkeypatch.delenv("AZURE_KEY_VAULT_URL", raising=False)
    with pytest.raises(ValueError, match="Key Vault URL not provided"):
        nb._resolve_keyvault_secret("other", {})


def test_aws_gcp_and_vault_secret_resolution(monkeypatch):
    class FakeSession:
        def __init__(self, profile_name=None):
            self.profile_name = profile_name

        def client(self, service_name, region_name=None, endpoint_url=None):
            assert service_name == "secretsmanager"
            return types.SimpleNamespace(get_secret_value=lambda SecretId: {"SecretBinary": b"c2VjcmV0"})

    monkeypatch.setitem(sys.modules, "boto3", types.SimpleNamespace(session=types.SimpleNamespace(Session=FakeSession)))
    nb._SECRET_CACHE.clear()
    assert nb._resolve_aws_secret("smtp/pass", {"aws_region": "eu-west-1"}) == "secret"

    class FakeSecretManagerClient:
        def access_secret_version(self, name):
            return types.SimpleNamespace(payload=types.SimpleNamespace(data=b"gcp-secret"))

    monkeypatch.setenv("GOOGLE_CLOUD_PROJECT", "demo-project")
    monkeypatch.setitem(
        sys.modules,
        "google.cloud",
        types.SimpleNamespace(secretmanager=types.SimpleNamespace(SecretManagerServiceClient=FakeSecretManagerClient)),
    )
    assert nb._resolve_gcp_secret("smtp-pass", {}) == "gcp-secret"
    assert nb._resolve_gcp_secret("projects/x/secrets/y/versions/latest", {}) == "gcp-secret"

    class FakeHvacClient:
        def __init__(self, url=None, token=None):
            self.secrets = types.SimpleNamespace(
                kv=types.SimpleNamespace(
                    v1=types.SimpleNamespace(read_secret=lambda path: {"data": {"plain": "v1-secret"}}),
                    v2=types.SimpleNamespace(
                        read_secret_version=lambda path: {"data": {"data": {"field": "vault-secret"}}}
                    ),
                )
            )

    monkeypatch.setenv("VAULT_ADDR", "https://vault.local")
    monkeypatch.setitem(sys.modules, "hvac", types.SimpleNamespace(Client=FakeHvacClient))
    assert nb._resolve_vault_secret("secret/path#field", {}) == "vault-secret"
    assert nb._resolve_vault_secret("secret/path", {"vault_kv_version": "1"}) == '{"plain": "v1-secret"}'


def test_local_secret_loading_and_error_paths(tmp_path, monkeypatch):
    cryptography = pytest.importorskip("cryptography.fernet")
    from cryptography.fernet import Fernet

    key = Fernet.generate_key().decode("utf-8")
    secrets_file = tmp_path / "local.enc"
    secrets_file.write_bytes(Fernet(key.encode("utf-8")).encrypt(json.dumps({"smtp": "pw"}).encode("utf-8")))
    nb._LOCAL_SECRET_CACHE.clear()

    loaded = nb._load_local_secrets({"secrets_file": str(secrets_file), "secrets_key": key})
    assert loaded == {"smtp": "pw"}
    assert nb._resolve_local_secret("smtp", {"secrets_file": str(secrets_file), "secrets_key": key}) == "pw"

    with pytest.raises(ValueError, match="secrets_file is required"):
        nb._load_local_secrets({})
    with pytest.raises(ValueError, match="secrets_key is required"):
        nb._load_local_secrets({"secrets_file": str(secrets_file)})
    with pytest.raises(ValueError, match="not found"):
        nb._load_local_secrets({"secrets_file": str(tmp_path / "missing.enc"), "secrets_key": key})
    with pytest.raises(ValueError, match="not found"):
        nb._resolve_local_secret("absent", {"secrets_file": str(secrets_file), "secrets_key": key})


def test_resolve_value_dispatch_and_warning(monkeypatch):
    warnings = []
    monkeypatch.setattr(nb.logger, "warning", warnings.append)
    monkeypatch.setattr(nb, "_resolve_keyvault_secret", lambda secret, config: f"kv:{secret}")
    monkeypatch.setattr(nb, "_resolve_aws_secret", lambda secret, config: f"aws:{secret}")
    monkeypatch.setattr(nb, "_resolve_gcp_secret", lambda secret, config: f"gcp:{secret}")
    monkeypatch.setattr(nb, "_resolve_vault_secret", lambda secret, config: f"vault:{secret}")
    monkeypatch.setattr(nb, "_resolve_local_secret", lambda secret, config: f"local:{secret}")
    monkeypatch.setenv("FOUND_ENV", "env-value")

    assert nb._resolve_value("env:FOUND_ENV", {}) == "env-value"
    assert nb._resolve_value("${ENV:FOUND_ENV}", {}) == "env-value"
    assert nb._resolve_value("keyvault:abc", {}) == "kv:abc"
    assert nb._resolve_value("${AZURE_KEY_VAULT:abc}", {}) == "kv:abc"
    assert nb._resolve_value("aws:name", {}) == "aws:name"
    assert nb._resolve_value("${AWS_SECRETS_MANAGER:name}", {}) == "aws:name"
    assert nb._resolve_value("gcp:name", {}) == "gcp:name"
    assert nb._resolve_value("${GCP_SECRET_MANAGER:name}", {}) == "gcp:name"
    assert nb._resolve_value("vault:path#field", {}) == "vault:path#field"
    assert nb._resolve_value("${HASHICORP_VAULT:path#field}", {}) == "vault:path#field"
    assert nb._resolve_value("local:key", {}) == "local:key"
    assert nb._resolve_value("${LOCAL_SECRET:key}", {}) == "local:key"
    assert nb._resolve_value(42, {}) == 42
    assert nb._resolve_value("${ENV:MISSING_ENV}", {}) is None
    assert any("Env var not found: MISSING_ENV" in message for message in warnings)


def test_validation_helpers_and_template_path_resolution(tmp_path):
    with pytest.raises(ValueError, match="smtp_password is required"):
        nb.validate_notification_config(
            "smtp",
            {
                "smtp_host": "smtp.example.com",
                "from_email": "from@example.com",
                "target": "to@example.com",
                "smtp_username": "user",
            },
        )
    with pytest.raises(ValueError, match="Unsupported notification type"):
        nb.validate_notification_config("pagerduty", {"target": "x"})

    assert nb._first_present({"a": "", "b": "value"}, ["a", "b"]) == "value"
    assert nb._first_present({"a": ""}, ["a", "b"]) is None
    assert nb._resolve_template_path("body.md", {"_base_path": str(tmp_path)}).parent == tmp_path


def test_render_notification_content_with_files_builtin_and_standalone_fallback(tmp_path, monkeypatch):
    subject_file = tmp_path / "subject.j2"
    body_file = tmp_path / "body.j2"
    subject_file.write_text("Alert for {{ contract.title }}", encoding="utf-8")
    body_file.write_text("{{ message }} from {{ template_context_value }}", encoding="utf-8")

    config = {
        "_base_path": str(tmp_path),
        "subject_template_file": "subject.j2",
        "message_template_file": "body.j2",
        "template_context": {"template_context_value": "template"},
    }
    message, subject = nb.render_notification_content(
        config,
        "Body",
        subject="Default",
        context={"contract": {"title": "Orders", "version": "1.0", "dataset": "bronze_orders"}},
    )
    assert subject == "Alert for Orders"
    assert message == "Body from template"

    builtin_message, builtin_subject = nb.render_notification_content(
        {},
        "Pipeline failed",
        context={
            "event": "failure",
            "run_id": "r1",
            "engine": "polars",
            "timestamp_utc": "2024-03-10T00:00:00Z",
            "source_path": "abfss://container@storageacct.dfs.core.windows.net/raw/orders",
            "contract": {"title": "Orders", "version": "1.0", "dataset": "bronze_orders"},
        },
    )
    assert builtin_subject == "LakeLogic Alert"
    assert "Pipeline failed" in builtin_message
    assert "s***" in builtin_message

    monkeypatch.setattr(nb, "_render_jinja_template", lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("boom")))
    fallback_message, _ = nb.render_notification_content(
        {"message_template": "{{ message }}"},
        "Fallback body",
        context={
            "event": "failure",
            "run_id": "r1",
            "engine": "duckdb",
            "contract": {"title": "Orders", "version": "1.0", "dataset": "bronze_orders"},
        },
    )
    assert "Fallback body" in fallback_message
    assert "Run ID" in fallback_message


def test_render_notification_content_error_paths(tmp_path):
    with pytest.raises(ValueError, match="template_context must be an object"):
        nb.render_notification_content({"template_context": "bad"}, "message")

    with pytest.raises(ValueError, match="subject_template_file not found"):
        nb.render_notification_content({"subject_template_file": str(tmp_path / "missing.j2")}, "message")

    with pytest.raises(ValueError, match="message_template_file not found"):
        nb.render_notification_content({"message_template_file": str(tmp_path / "missing-body.j2")}, "message")


def test_adapters_send_paths(monkeypatch):
    smtp_events = []

    class FakeSMTP:
        def __init__(self, host, port, timeout=0):
            smtp_events.append(("connect", host, port, timeout))

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def starttls(self):
            smtp_events.append("starttls")

        def login(self, username, password):
            smtp_events.append(("login", username, password))

        def send_message(self, msg):
            smtp_events.append(("send", msg["To"], msg["Subject"], msg.get_payload()))

    post_calls = []
    print_calls = []
    monkeypatch.setattr(nb.smtplib, "SMTP", FakeSMTP)
    monkeypatch.setattr(nb, "_post_json", lambda url, payload, headers=None: post_calls.append((url, payload, headers)))
    monkeypatch.setattr("builtins.print", lambda block: print_calls.append(block))

    SMTPAdapter(
        {
            "smtp_host": "smtp.example.com",
            "smtp_port": 2525,
            "smtp_username": "user",
            "smtp_password": "pass",
            "from_email": "from@example.com",
            "target": "to@example.com",
        }
    ).send("hello", "subject")
    assert smtp_events[0] == ("connect", "smtp.example.com", 2525, 10)
    assert "starttls" in smtp_events
    assert ("login", "user", "pass") in smtp_events
    assert any(event[0] == "send" for event in smtp_events if isinstance(event, tuple))

    nb.SendGridAdapter({"api_key": "sg", "from_email": "from@example.com", "target": "to@example.com"}).send(
        "body", "subject"
    )
    nb.SlackAdapter({"target": "https://slack.example"}).send("body", "subject")
    nb.TeamsAdapter({"target": "https://teams.example"}).send("body", "subject")
    nb.WebhookAdapter({"target": "https://hook.example"}).send("body", "subject")
    ConsoleAdapter({}).send("body", "subject")

    assert post_calls[0][0] == "https://api.sendgrid.com/v3/mail/send"
    assert post_calls[1][1] == {"text": "subject\nbody"}
    assert post_calls[2][1] == {"title": "subject", "text": "body"}
    assert post_calls[3][1] == {"subject": "subject", "message": "body"}
    assert any("[LAKELOGIC NOTIFICATION]" in block for block in print_calls)


def test_get_notification_adapter_console_and_unsupported(monkeypatch):
    monkeypatch.delenv("SENDGRID_API_KEY", raising=False)
    adapter = get_notification_adapter("console", {})
    assert isinstance(adapter, ConsoleAdapter)

    with pytest.raises(ValueError, match="Unsupported notification type"):
        get_notification_adapter("pagerduty", {"target": "x"})
