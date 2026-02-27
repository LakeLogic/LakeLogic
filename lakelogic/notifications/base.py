from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Tuple
from loguru import logger
import base64
import json
import os
import re
import smtplib
from pathlib import Path
from email.mime.text import MIMEText
from urllib.request import Request, urlopen


class NotificationAdapter(ABC):
    """
    Base class for all notification adapters.
    """
    def __init__(self, config: Dict[str, Any]):
        """
        Initialize an adapter with resolved configuration.

        Args:
            config: Notification configuration.
        """
        self.config = config

    @abstractmethod
    def send(self, message: str, subject: str = "LakeLogic Alert"):
        """
        Send a notification message.

        Args:
            message: Body content.
            subject: Message subject.
        """
        pass


def _post_json(url: str, payload: Dict[str, Any], headers: Optional[Dict[str, str]] = None) -> None:
    """
    Post a JSON payload to a webhook URL.

    Args:
        url: Destination URL.
        payload: JSON payload.
        headers: Optional headers.
    """
    body = json.dumps(payload).encode("utf-8")
    req_headers = {"Content-Type": "application/json"}
    if headers:
        req_headers.update(headers)
    req = Request(url, data=body, headers=req_headers, method="POST")
    with urlopen(req, timeout=10) as resp:
        _ = resp.read()

_ENV_PATTERN = re.compile(r"^\${ENV:([A-Z0-9_]+)}$")
_KEYVAULT_PATTERN = re.compile(r"^\${AZURE_KEY_VAULT:([A-Za-z0-9_\-]+)}$")
_AWS_SM_PATTERN = re.compile(r"^\${AWS_SECRETS_MANAGER:([A-Za-z0-9_\-./]+)}$")
_GCP_SM_PATTERN = re.compile(r"^\${GCP_SECRET_MANAGER:([A-Za-z0-9_\-./]+)}$")
_VAULT_PATTERN = re.compile(r"^\${HASHICORP_VAULT:([A-Za-z0-9_\-./#]+)}$")
_LOCAL_PATTERN = re.compile(r"^\${LOCAL_SECRET:([A-Za-z0-9_\-./]+)}$")

_SECRET_CACHE: Dict[Tuple[Any, ...], Optional[str]] = {}
_LOCAL_SECRET_CACHE: Dict[Tuple[str, str], Dict[str, Any]] = {}


def _cache_get(key: Tuple[Any, ...]) -> Optional[str]:
    """
    Get a cached secret value.

    Args:
        key: Cache key tuple.

    Returns:
        Cached secret value or None.
    """
    return _SECRET_CACHE.get(key)


def _cache_set(key: Tuple[Any, ...], value: Optional[str]) -> None:
    """
    Cache a secret value.

    Args:
        key: Cache key tuple.
        value: Secret value to cache.
    """
    _SECRET_CACHE[key] = value


def _resolve_keyvault_secret(secret_name: str, config: Dict[str, Any]) -> Optional[str]:
    """
    Resolve a secret from Azure Key Vault.

    Args:
        secret_name: Secret name.
        config: Notification config.

    Returns:
        Secret value.
    """
    vault_url = config.get("key_vault_url") or os.getenv("AZURE_KEY_VAULT_URL")
    if not vault_url:
        raise ValueError("Azure Key Vault URL not provided (key_vault_url or AZURE_KEY_VAULT_URL).")

    cache_key = ("azure_kv", vault_url, secret_name)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient
    except Exception:
        raise ValueError("Azure Key Vault dependencies not installed. Install azure-identity and azure-keyvault-secrets.")

    try:
        client = SecretClient(vault_url=vault_url, credential=DefaultAzureCredential())
        secret = client.get_secret(secret_name)
        _cache_set(cache_key, secret.value)
        return secret.value
    except Exception as e:
        raise ValueError(f"Failed to retrieve Key Vault secret '{secret_name}': {e}")


def _resolve_aws_secret(secret_name: str, config: Dict[str, Any]) -> Optional[str]:
    """
    Resolve a secret from AWS Secrets Manager.

    Args:
        secret_name: Secret name.
        config: Notification config.

    Returns:
        Secret value.
    """
    try:
        import boto3
    except Exception:
        raise ValueError("AWS Secrets Manager dependencies not installed. Install boto3.")

    region = config.get("aws_region") or os.getenv("AWS_REGION")
    profile = config.get("aws_profile") or os.getenv("AWS_PROFILE")
    endpoint_url = config.get("aws_endpoint_url")
    cache_key = ("aws_sm", region, profile, endpoint_url, secret_name)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        if profile:
            session = boto3.session.Session(profile_name=profile)
        else:
            session = boto3.session.Session()
        client = session.client("secretsmanager", region_name=region, endpoint_url=endpoint_url)
        resp = client.get_secret_value(SecretId=secret_name)
        if "SecretString" in resp:
            _cache_set(cache_key, resp["SecretString"])
            return resp["SecretString"]
        if "SecretBinary" in resp:
            decoded = base64.b64decode(resp["SecretBinary"]).decode("utf-8")
            _cache_set(cache_key, decoded)
            return decoded
    except Exception as e:
        raise ValueError(f"Failed to retrieve AWS secret '{secret_name}': {e}")
    return None


def _resolve_gcp_secret(secret_name: str, config: Dict[str, Any]) -> Optional[str]:
    """
    Resolve a secret from GCP Secret Manager.

    Args:
        secret_name: Secret name.
        config: Notification config.

    Returns:
        Secret value.
    """
    try:
        from google.cloud import secretmanager
    except Exception:
        raise ValueError("GCP Secret Manager dependencies not installed. Install google-cloud-secret-manager.")

    project_id = config.get("gcp_project") or os.getenv("GCP_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    version = config.get("gcp_version") or "latest"

    # If secret_name is not a full resource, build one.
    if not secret_name.startswith("projects/"):
        if not project_id:
            raise ValueError("GCP project not provided (gcp_project or GOOGLE_CLOUD_PROJECT).")
        resource_name = f"projects/{project_id}/secrets/{secret_name}/versions/{version}"
    else:
        resource_name = secret_name

    cache_key = ("gcp_sm", resource_name)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        client = secretmanager.SecretManagerServiceClient()
        response = client.access_secret_version(name=resource_name)
        value = response.payload.data.decode("utf-8")
        _cache_set(cache_key, value)
        return value
    except Exception as e:
        raise ValueError(f"Failed to retrieve GCP secret '{secret_name}': {e}")


def _resolve_vault_secret(secret_ref: str, config: Dict[str, Any]) -> Optional[str]:
    """
    Resolve a secret from HashiCorp Vault.

    Args:
        secret_ref: Secret path (optionally with #field).
        config: Notification config.

    Returns:
        Secret value.
    """
    try:
        import hvac
    except Exception:
        raise ValueError("HashiCorp Vault dependencies not installed. Install hvac.")

    vault_url = config.get("vault_url") or os.getenv("VAULT_ADDR")
    token = config.get("vault_token") or os.getenv("VAULT_TOKEN")
    if not vault_url:
        raise ValueError("Vault URL not provided (vault_url or VAULT_ADDR).")

    # Optional field selector: secret/path#field
    if "#" in secret_ref:
        path, field = secret_ref.split("#", 1)
    else:
        path, field = secret_ref, None

    kv_version = str(config.get("vault_kv_version") or "2")
    cache_key = ("vault", vault_url, kv_version, secret_ref)
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    try:
        client = hvac.Client(url=vault_url, token=token)
        if kv_version == "1":
            secret = client.secrets.kv.v1.read_secret(path=path)
            data = secret.get("data", {})
        else:
            secret = client.secrets.kv.v2.read_secret_version(path=path)
            data = secret.get("data", {}).get("data", {})

        if field:
            value = data.get(field)
        else:
            value = json.dumps(data)
        _cache_set(cache_key, value)
        return value
    except Exception as e:
        raise ValueError(f"Failed to retrieve Vault secret '{secret_ref}': {e}")


def _resolve_env_only(value: Optional[str]) -> Optional[str]:
    """
    Resolve env:VAR patterns without other secret providers.

    Args:
        value: Raw value.

    Returns:
        Resolved value.
    """
    if value is None:
        return None
    if value.startswith("env:"):
        return os.getenv(value[4:].strip())
    match = _ENV_PATTERN.match(value)
    if match:
        return os.getenv(match.group(1))
    return value


def _load_local_secrets(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Load and decrypt local secrets from an encrypted file.

    Args:
        config: Notification config.

    Returns:
        Decrypted secrets dict.
    """
    secrets_file = config.get("secrets_file")
    if not secrets_file:
        raise ValueError("secrets_file is required for local secrets.")

    path = Path(secrets_file)
    base_path = config.get("_base_path")
    if not path.is_absolute() and base_path:
        path = Path(base_path) / path

    secrets_key = config.get("secrets_key") or os.getenv("LAKELOGIC_SECRETS_KEY")
    secrets_key = _resolve_env_only(secrets_key)
    if not secrets_key:
        raise ValueError("secrets_key is required for local secrets (or set LAKELOGIC_SECRETS_KEY).")

    cache_key = (str(path), secrets_key)
    cached = _LOCAL_SECRET_CACHE.get(cache_key)
    if cached is not None:
        return cached

    try:
        from cryptography.fernet import Fernet, InvalidToken
    except Exception:
        raise ValueError("Local secrets require cryptography. Install cryptography.")

    if not path.exists():
        raise ValueError(f"Local secrets file not found: {path}")

    token = path.read_bytes()
    try:
        fernet = Fernet(secrets_key.encode("utf-8"))
        decrypted = fernet.decrypt(token)
    except InvalidToken as e:
        raise ValueError("Invalid secrets_key for local secrets file.") from e

    try:
        payload = json.loads(decrypted.decode("utf-8"))
    except Exception as e:
        raise ValueError("Local secrets file is not valid JSON.") from e

    if not isinstance(payload, dict):
        raise ValueError("Local secrets file must be a JSON object (key/value).")

    _LOCAL_SECRET_CACHE[cache_key] = payload
    return payload


def _resolve_local_secret(secret_name: str, config: Dict[str, Any]) -> Optional[str]:
    """
    Resolve a secret from the local encrypted secrets file.

    Args:
        secret_name: Secret key.
        config: Notification config.

    Returns:
        Secret value.
    """
    secrets = _load_local_secrets(config)
    if secret_name not in secrets:
        raise ValueError(f"Local secret '{secret_name}' not found in secrets file.")
    return secrets.get(secret_name)


def _resolve_value(value: Any, config: Dict[str, Any]) -> Any:
    """
    Resolve a configuration value, supporting multiple secret providers.

    Args:
        value: Raw value.
        config: Notification config.

    Returns:
        Resolved value.
    """
    if not isinstance(value, str):
        return value

    # env:VAR_NAME or ${ENV:VAR_NAME}
    if value.startswith("env:"):
        var_name = value[4:].strip()
        resolved = os.getenv(var_name)
        if resolved is None:
            logger.warning(f"Env var not found: {var_name}")
        return resolved

    match = _ENV_PATTERN.match(value)
    if match:
        var_name = match.group(1)
        resolved = os.getenv(var_name)
        if resolved is None:
            logger.warning(f"Env var not found: {var_name}")
        return resolved

    # keyvault:secret-name or ${AZURE_KEY_VAULT:secret-name}
    if value.startswith("keyvault:"):
        secret_name = value[len("keyvault:"):].strip()
        return _resolve_keyvault_secret(secret_name, config)

    match = _KEYVAULT_PATTERN.match(value)
    if match:
        secret_name = match.group(1)
        return _resolve_keyvault_secret(secret_name, config)

    # aws secrets manager
    if value.startswith("aws:"):
        secret_name = value[len("aws:"):].strip()
        return _resolve_aws_secret(secret_name, config)
    match = _AWS_SM_PATTERN.match(value)
    if match:
        secret_name = match.group(1)
        return _resolve_aws_secret(secret_name, config)

    # gcp secret manager
    if value.startswith("gcp:"):
        secret_name = value[len("gcp:"):].strip()
        return _resolve_gcp_secret(secret_name, config)
    match = _GCP_SM_PATTERN.match(value)
    if match:
        secret_name = match.group(1)
        return _resolve_gcp_secret(secret_name, config)

    # hashicorp vault
    if value.startswith("vault:"):
        secret_ref = value[len("vault:"):].strip()
        return _resolve_vault_secret(secret_ref, config)
    match = _VAULT_PATTERN.match(value)
    if match:
        secret_ref = match.group(1)
        return _resolve_vault_secret(secret_ref, config)

    # local encrypted secrets file
    if value.startswith("local:"):
        secret_name = value[len("local:"):].strip()
        return _resolve_local_secret(secret_name, config)
    match = _LOCAL_PATTERN.match(value)
    if match:
        secret_name = match.group(1)
        return _resolve_local_secret(secret_name, config)

    return value


def resolve_config_secrets(config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Resolve all secret placeholders in a config dict.

    Args:
        config: Notification config.

    Returns:
        Resolved config dict.
    """
    resolved: Dict[str, Any] = {}
    for key, value in config.items():
        resolved[key] = _resolve_value(value, config)
    return resolved


def _require_fields(notif_type: str, config: Dict[str, Any], fields: list[str]) -> None:
    """
    Ensure required fields exist for a notification type.

    Args:
        notif_type: Notification type.
        config: Notification config.
        fields: Required field names.
    """
    missing = [f for f in fields if not config.get(f)]
    if missing:
        raise ValueError(f"{notif_type} notification missing required fields: {', '.join(missing)}")


def validate_notification_config(notif_type: str, config: Dict[str, Any]) -> None:
    """
    Validate notification configuration by type.

    Args:
        notif_type: Notification type.
        config: Notification config.
    """
    notif = notif_type.lower()

    if notif in ["smtp", "email"]:
        _require_fields(notif, config, ["smtp_host", "from_email", "target"])
        if config.get("smtp_username") and not config.get("smtp_password"):
            raise ValueError("smtp_password is required when smtp_username is provided.")
    elif notif == "sendgrid":
        _require_fields(notif, config, ["api_key", "from_email", "target"])
    elif notif in ["slack", "teams", "webhook"]:
        _require_fields(notif, config, ["target"])
    else:
        raise ValueError(f"Unsupported notification type: {notif_type}")


def _first_present(config: Dict[str, Any], keys: list[str]) -> Optional[str]:
    """
    Return the first non-empty string value from a list of keys.

    Args:
        config: Notification config.
        keys: Candidate key names in priority order.

    Returns:
        The first non-empty string value, else None.
    """
    for key in keys:
        value = config.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _resolve_template_path(path_value: str, config: Dict[str, Any]) -> Path:
    """
    Resolve a template file path using contract-relative base path when present.

    Args:
        path_value: Raw path from config.
        config: Notification config.

    Returns:
        Absolute or relative Path resolved against _base_path when available.
    """
    path = Path(path_value)
    base_path = config.get("_base_path")
    if not path.is_absolute() and base_path:
        path = Path(base_path) / path
    return path


def _render_jinja_template(template_text: str, context: Dict[str, Any], label: str) -> str:
    """
    Render a Jinja2 template with strict undefined handling.

    When the template contains ``{% extends %}`` directives, a
    ``FileSystemLoader`` is used with the built-in templates directory
    so ``_base.md.j2`` (and other shared templates) can be resolved.

    Args:
        template_text: Template source string.
        context: Rendering context.
        label: Human-readable label for error messages.

    Returns:
        Rendered string.
    """
    try:
        from jinja2 import Environment, FileSystemLoader, StrictUndefined
    except Exception as e:
        raise ValueError(
            "Notification templates require 'jinja2'. Install with: pip install jinja2"
        ) from e

    templates_dir = Path(__file__).parent / "templates"

    # Use FileSystemLoader when template uses {% extends %} so _base.md.j2
    # can be resolved.  Otherwise use a simple from_string approach.
    if "{% extends" in template_text and templates_dir.is_dir():
        env = Environment(
            loader=FileSystemLoader(str(templates_dir)),
            autoescape=False,
            undefined=StrictUndefined,
        )
        try:
            tmpl = env.from_string(template_text)
            return tmpl.render(**context)
        except Exception as e:
            raise ValueError(f"Failed to render notification {label}: {e}") from e
    else:
        env = Environment(autoescape=False, undefined=StrictUndefined)
        try:
            return env.from_string(template_text).render(**context)
        except Exception as e:
            raise ValueError(f"Failed to render notification {label}: {e}") from e


def _load_builtin_template(event: str) -> Optional[str]:
    """
    Load a built-in default Jinja2 template for a given event.

    Templates are shipped inside ``lakelogic/notifications/templates/``
    and named ``{event}.md.j2``.  Returns ``None`` when no built-in
    template exists for the event.

    Args:
        event: Event name (e.g. ``quarantine``, ``failure``, ``schema_drift``).

    Returns:
        Template source string, or None.
    """
    templates_dir = Path(__file__).parent / "templates"
    # Normalise event name — convert spaces/hyphens to underscores, lowercase
    safe_name = event.strip().lower().replace("-", "_").replace(" ", "_")
    template_path = templates_dir / f"{safe_name}.md.j2"
    if template_path.is_file():
        return template_path.read_text(encoding="utf-8")
    return None


def render_notification_content(
    config: Dict[str, Any],
    message: str,
    subject: str = "LakeLogic Alert",
    context: Optional[Dict[str, Any]] = None,
) -> Tuple[str, str]:
    """
    Render optional subject/message templates for any notification channel.

    Template resolution priority (highest wins):

    1. ``message_template_file`` / ``subject_template_file`` — user file
    2. ``message_template`` / ``subject_template`` — inline Jinja2 string
    3. Built-in default — ``notifications/templates/{event}.md.j2``
    4. Raw ``message`` / ``subject`` — plain string fallback

    Supported keys:
    - subject_template / subject_template_file
    - message_template / message_template_file
    - body_template / body_template_file (aliases for message templates)
    - template_context (dict merged into render context)

    Args:
        config: Notification configuration.
        message: Default message body.
        subject: Default subject.
        context: Optional runtime context values.

    Returns:
        Tuple of (rendered_message, rendered_subject).
    """
    merged_context: Dict[str, Any] = {"message": message, "subject": subject}
    if context:
        merged_context.update(context)

    template_context = config.get("template_context")
    if template_context is not None:
        if not isinstance(template_context, dict):
            raise ValueError("template_context must be an object/map.")
        merged_context.update(template_context)

    rendered_subject = subject
    rendered_message = message

    subject_template = _first_present(config, ["subject_template"])
    subject_template_file = _first_present(config, ["subject_template_file"])
    message_template = _first_present(config, ["message_template", "body_template"])
    message_template_file = _first_present(config, ["message_template_file", "body_template_file"])

    if subject_template_file:
        subject_path = _resolve_template_path(subject_template_file, config)
        if not subject_path.exists():
            raise ValueError(f"subject_template_file not found: {subject_path}")
        subject_template = subject_path.read_text(encoding="utf-8")

    if subject_template:
        rendered_subject = _render_jinja_template(subject_template, merged_context, "subject template")
        merged_context["subject"] = rendered_subject

    if message_template_file:
        message_path = _resolve_template_path(message_template_file, config)
        if not message_path.exists():
            raise ValueError(f"message_template_file not found: {message_path}")
        message_template = message_path.read_text(encoding="utf-8")

    # Fall back to built-in default template when no custom template is provided
    if not message_template:
        event = merged_context.get("event", "")
        if event:
            builtin = _load_builtin_template(event)
            if builtin:
                message_template = builtin
                logger.debug(f"Using built-in template for event '{event}'")

    if message_template:
        try:
            rendered_message = _render_jinja_template(message_template, merged_context, "message template")
        except Exception as e:
            # If built-in template uses {% extends %} and Jinja2 FileSystemLoader
            # isn't configured, fall back to a non-extending render.
            logger.debug(f"Template render with extends failed, trying standalone: {e}")
            rendered_message = _render_jinja_standalone(merged_context)

    return rendered_message, rendered_subject


def _render_jinja_standalone(context: Dict[str, Any]) -> str:
    """
    Render a simple standalone notification when {% extends %} isn't available.

    This produces a clean markdown message without needing the Jinja2
    FileSystemLoader, which the built-in templates normally require.
    """
    event = context.get("event", "alert")
    message = context.get("message", "")
    contract = context.get("contract", {})
    title = contract.get("title", "Unknown")
    version = contract.get("version", "?")

    _ICONS = {
        "quarantine": "⚠️",
        "failure": "🔴",
        "schema_drift": "🟡",
        "success": "✅",
        "sla_breach": "🕐",
    }
    icon = _ICONS.get(event, "ℹ️")

    lines = [
        f"**LakeLogic** — {title} (v{version})",
        "",
        f"## {icon} {event.replace('_', ' ').title()}",
        "",
        message,
        "",
        "| Detail | Value |",
        "|:-------|:------|",
        f"| Run ID | `{context.get('run_id', 'N/A')}` |",
        f"| Pipeline Run | `{context.get('pipeline_run_id') or 'N/A'}` |",
        f"| Engine | {context.get('engine', 'N/A')} |",
        f"| Source | `{context.get('source_path') or 'N/A'}` |",
        f"| Time (UTC) | {context.get('timestamp_utc', 'N/A')} |",
    ]

    if contract.get("domain"):
        lines.append(f"| Domain | {contract['domain']} |")
    if contract.get("system"):
        lines.append(f"| System | {contract['system']} |")
    if contract.get("owner"):
        lines.append(f"| Owner | {contract['owner']} |")

    lines.extend(["", "---", f"Run ID: `{context.get('run_id', 'N/A')}` | Engine: {context.get('engine', 'N/A')} | {context.get('timestamp_utc', '')}"])

    return "\n".join(lines)


class SMTPAdapter(NotificationAdapter):
    """SMTP-based notification adapter."""
    def send(self, message: str, subject: str = "LakeLogic Alert"):
        """
        Send an SMTP email message.

        Args:
            message: Body content.
            subject: Email subject.
        """
        host = self.config.get("smtp_host")
        port = int(self.config.get("smtp_port", 587))
        username = self.config.get("smtp_username")
        password = self.config.get("smtp_password")
        from_email = self.config.get("from_email")
        to_email = self.config.get("target")
        use_tls = bool(self.config.get("use_tls", True))

        if not host or not from_email or not to_email:
            logger.warning("SMTPAdapter missing smtp_host, from_email, or target; skipping send.")
            return

        msg = MIMEText(message)
        msg["Subject"] = subject
        msg["From"] = from_email
        msg["To"] = to_email

        logger.info(f"Sending SMTP email to {to_email} via {host}:{port}")
        with smtplib.SMTP(host, port, timeout=10) as server:
            if use_tls:
                server.starttls()
            if username and password:
                server.login(username, password)
            server.send_message(msg)


class SendGridAdapter(NotificationAdapter):
    """SendGrid-based notification adapter."""
    def send(self, message: str, subject: str = "LakeLogic Alert"):
        """
        Send a SendGrid email message.

        Args:
            message: Body content.
            subject: Email subject.
        """
        api_key = self.config.get("api_key")
        from_email = self.config.get("from_email")
        to_email = self.config.get("target")

        if not api_key or not from_email or not to_email:
            logger.warning("SendGridAdapter missing api_key, from_email, or target; skipping send.")
            return

        payload = {
            "personalizations": [{"to": [{"email": to_email}]}],
            "from": {"email": from_email},
            "subject": subject,
            "content": [{"type": "text/plain", "value": message}],
        }

        logger.info(f"Sending SendGrid email to {to_email}")
        _post_json(
            "https://api.sendgrid.com/v3/mail/send",
            payload,
            headers={"Authorization": f"Bearer {api_key}"}
        )


class SlackAdapter(NotificationAdapter):
    """Slack webhook notification adapter."""
    def send(self, message: str, subject: str = "LakeLogic Alert"):
        """
        Send a Slack webhook message.

        Args:
            message: Body content.
            subject: Message subject.
        """
        url = self.config.get("target")
        if not url:
            logger.warning("SlackAdapter missing target (webhook URL); skipping send.")
            return
        payload = {"text": f"{subject}\n{message}"}
        logger.info("Sending Slack message")
        _post_json(url, payload)


class TeamsAdapter(NotificationAdapter):
    """Microsoft Teams webhook notification adapter."""
    def send(self, message: str, subject: str = "LakeLogic Alert"):
        """
        Send a Microsoft Teams webhook message.

        Args:
            message: Body content.
            subject: Message subject.
        """
        url = self.config.get("target")
        if not url:
            logger.warning("TeamsAdapter missing target (webhook URL); skipping send.")
            return
        payload = {"text": f"{subject}\n{message}"}
        logger.info("Sending Teams message")
        _post_json(url, payload)


class WebhookAdapter(NotificationAdapter):
    """Generic webhook notification adapter."""
    def send(self, message: str, subject: str = "LakeLogic Alert"):
        """
        Send a generic webhook message.

        Args:
            message: Body content.
            subject: Message subject.
        """
        url = self.config.get("target")
        if not url:
            logger.warning("WebhookAdapter missing target URL; skipping send.")
            return
        payload = {"subject": subject, "message": message}
        logger.info("Sending Webhook")
        _post_json(url, payload)


def get_notification_adapter(notif_type: str, config: Dict[str, Any]) -> NotificationAdapter:
    """
    Create a notification adapter for a given type.

    Args:
        notif_type: Notification type identifier.
        config: Notification config.

    Returns:
        Initialized NotificationAdapter.
    """
    # Lazy-import Apprise adapter to avoid hard dependency
    from lakelogic.notifications.apprise_adapter import AppriseAdapter

    adapters = {
        "smtp": SMTPAdapter,
        "sendgrid": SendGridAdapter,
        "slack": SlackAdapter,
        "teams": TeamsAdapter,
        "email": SMTPAdapter,  # fallback
        "webhook": WebhookAdapter,
        "apprise": AppriseAdapter,
    }
    adapter_class = adapters.get(notif_type.lower())
    if not adapter_class:
        raise ValueError(f"Unsupported notification type: {notif_type}")
    resolved_config = resolve_config_secrets(config)
    # Apprise handles its own validation (URL-based)
    if notif_type.lower() != "apprise":
        validate_notification_config(notif_type, resolved_config)
    return adapter_class(resolved_config)
