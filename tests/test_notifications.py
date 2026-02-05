import os
import json
import pytest
from pathlib import Path
from lakeguard.notifications.base import resolve_config_secrets, get_notification_adapter


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

    monkeypatch.setenv("LAKEGUARD_SECRETS_KEY", key.decode("utf-8"))
    config = {
        "smtp_password": "local:smtp_password",
        "secrets_file": str(secrets_file),
        "smtp_host": "smtp.example.com",
        "from_email": "lakeguard@example.com",
        "target": "alerts@example.com",
    }

    resolved = resolve_config_secrets(config)
    assert resolved["smtp_password"] == "secret-local"
