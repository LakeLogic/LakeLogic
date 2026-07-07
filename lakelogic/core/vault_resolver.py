"""
Vault key resolver for LakeLogic PII encryption.

Parses ``pii_vault`` URI strings from data contracts and fetches the
encryption key from the appropriate backend.  Keys are cached for the
lifetime of the process to avoid repeated API calls.

Supported URI schemes
---------------------
``keyvault://vault-name/secret-name``
    Azure Key Vault secret.  Requires ``azure-identity`` and
    ``azure-keyvault-secrets`` packages.  Authenticates via
    ``DefaultAzureCredential`` (Managed Identity on Databricks,
    CLI/env locally).

``databricks://scope-name/key-name``
    Databricks secret scope.  Only works inside a Databricks
    notebook/job.  Uses ``dbutils.secrets.get()``.

``env://VARIABLE_NAME``
    Environment variable.  Reads ``os.environ[VARIABLE_NAME]``.

``vault://path/to/secret``
    HashiCorp Vault (stub — logs a warning, falls back to env).

``aws-kms://alias/key-name``
    AWS KMS / Secrets Manager (stub — logs a warning, falls back to env).

Fallback
--------
If no ``pii_vault`` URI is provided, or if the resolved provider fails,
the resolver falls back to ``os.environ.get("LAKELOGIC_PII_KEY", "")``.

Usage::

    from lakelogic.core.vault_resolver import resolve_encryption_key

    # From a contract field:
    key = resolve_encryption_key("keyvault://mycompany-pii/lakelogic-pii-key")

    # Auto-resolve from contract (picks the first pii_vault URI found):
    key = resolve_encryption_key_from_contract(contract)
"""

from __future__ import annotations

import os
from typing import Any, Dict, Optional

from loguru import logger

# ── In-memory cache ─────────────────────────────────────────────────────────
# Keyed by the full URI string.  Survives for the process lifetime so
# repeated calls (e.g. per-partition) don't trigger API calls.
_KEY_CACHE: Dict[str, str] = {}

_ENV_FALLBACK_VAR = "LAKELOGIC_PII_KEY"


# ── Public API ──────────────────────────────────────────────────────────────


def resolve_encryption_key(pii_vault_uri: Optional[str] = None) -> str:
    """Resolve an encryption key from a ``pii_vault`` URI.

    Parameters
    ----------
    pii_vault_uri : str, optional
        URI string from the contract's ``pii_vault`` field.
        If ``None`` or empty, falls back to the ``LAKELOGIC_PII_KEY``
        environment variable.

    Returns
    -------
    str
        The resolved encryption key (plaintext).  Empty string if
        no key could be resolved.

    Examples
    --------
    >>> resolve_encryption_key("keyvault://mycompany-pii/lakelogic-pii-key")
    'my-secret-key-value'

    >>> resolve_encryption_key("databricks://pii-scope/encryption-key")
    'databricks-stored-key'

    >>> resolve_encryption_key("env://MY_CUSTOM_KEY")
    'value-from-env'

    >>> resolve_encryption_key(None)  # falls back to LAKELOGIC_PII_KEY
    ''
    """
    if not pii_vault_uri or not pii_vault_uri.strip():
        return os.environ.get(_ENV_FALLBACK_VAR, "")

    uri = pii_vault_uri.strip()

    # Check cache
    if uri in _KEY_CACHE:
        logger.debug(f"Vault key cache hit for '{uri}'")
        return _KEY_CACHE[uri]

    try:
        key = _resolve(uri)
        if key:
            _KEY_CACHE[uri] = key
            # Log success without revealing the key
            logger.info(f"Resolved encryption key from '{_safe_uri(uri)}' ({len(key)} chars)")
            return key
        else:
            logger.warning(
                f"Vault resolver returned empty key for '{_safe_uri(uri)}' — falling back to ${_ENV_FALLBACK_VAR}"
            )
            return os.environ.get(_ENV_FALLBACK_VAR, "")

    except Exception as exc:
        logger.warning(f"Failed to resolve key from '{_safe_uri(uri)}': {exc} — falling back to ${_ENV_FALLBACK_VAR}")
        return os.environ.get(_ENV_FALLBACK_VAR, "")


def resolve_encryption_key_from_contract(contract: Any) -> str:
    """Auto-resolve the encryption key from a contract's PII field annotations.

    Scans ``model.fields`` for the first field with a ``pii_vault`` value
    and resolves it.  All PII fields in a contract are expected to use
    the same vault key — if multiple different URIs are found, a warning
    is logged.

    Parameters
    ----------
    contract : DataContract
        The contract to scan.

    Returns
    -------
    str
        Resolved encryption key.
    """
    vault_uris: set = set()
    model = getattr(contract, "model", None)
    if model:
        for field in getattr(model, "fields", []) or []:
            vault = getattr(field, "pii_vault", None)
            if vault:
                vault_uris.add(vault.strip())

    if not vault_uris:
        return os.environ.get(_ENV_FALLBACK_VAR, "")

    if len(vault_uris) > 1:
        logger.warning(
            f"Multiple pii_vault URIs found in contract: {vault_uris}. "
            f"Using the first one. All PII fields should reference the same key."
        )

    return resolve_encryption_key(next(iter(vault_uris)))


def clear_cache() -> None:
    """Clear the in-memory key cache (useful for testing / key rotation)."""
    _KEY_CACHE.clear()
    logger.debug("Vault key cache cleared")


# ── URI Parsing & Dispatch ──────────────────────────────────────────────────


def _safe_uri(uri: str) -> str:
    """Return a log-safe version of the URI (no secrets)."""
    # Don't log the actual env var value
    if uri.startswith("env://"):
        return uri
    # For vault URIs, show the scheme + vault name but mask the key name
    parts = uri.split("/")
    if len(parts) >= 3:
        return f"{parts[0]}//{parts[2]}/***"
    return uri.split("://")[0] + "://***"


def _resolve(uri: str) -> Optional[str]:
    """Dispatch to the appropriate provider based on URI scheme."""
    scheme = uri.split("://")[0].lower() if "://" in uri else ""

    if scheme == "keyvault":
        return _resolve_azure_keyvault(uri)
    elif scheme == "databricks":
        return _resolve_databricks(uri)
    elif scheme == "env":
        return _resolve_env(uri)
    elif scheme == "vault":
        return _resolve_hashicorp(uri)
    elif scheme in ("aws-kms", "aws-secrets", "aws"):
        return _resolve_aws(uri)
    else:
        logger.warning(
            f"Unknown pii_vault scheme '{scheme}' in '{uri}'. "
            f"Supported: keyvault://, databricks://, env://, vault://, aws-kms://"
        )
        return None


# ── Azure Key Vault ─────────────────────────────────────────────────────────


def _resolve_azure_keyvault(uri: str) -> Optional[str]:
    """Fetch a secret from Azure Key Vault.

    URI formats::

        keyvault://vault-name/secret-name
        keyvault://vault-name/secret-name/version
        keyvault://vault-name.vault.azure.net/secret-name   (full URL also accepted)

    Requires:
        pip install azure-identity azure-keyvault-secrets

    Authentication: ``DefaultAzureCredential`` which supports:
        - Managed Identity (Databricks, Azure VMs, AKS)
        - Azure CLI (``az login``)
        - Environment variables (AZURE_CLIENT_ID, etc.)
        - VS Code / IntelliJ credentials
    """
    # Parse: keyvault://vault-name/secret-name[/version]
    path = uri.split("://", 1)[1] if "://" in uri else uri
    parts = path.split("/")
    if len(parts) < 2:
        raise ValueError(f"Invalid keyvault URI: '{uri}'. Expected: keyvault://vault-name/secret-name")

    vault_name = parts[0]
    secret_name = parts[1]
    version = parts[2] if len(parts) > 2 else None

    # Handle both short name and full URL:
    #   keyvault://kv-mycompany/secret       → https://kv-mycompany.vault.azure.net
    #   keyvault://kv-mycompany.vault.azure.net/secret → https://kv-mycompany.vault.azure.net
    if ".vault.azure.net" in vault_name:
        vault_url = f"https://{vault_name}"
    else:
        vault_url = f"https://{vault_name}.vault.azure.net"

    try:
        from azure.identity import DefaultAzureCredential
        from azure.keyvault.secrets import SecretClient
    except ImportError:
        raise ImportError("Azure Key Vault integration requires: pip install azure-identity azure-keyvault-secrets")

    credential = DefaultAzureCredential()
    client = SecretClient(vault_url=vault_url, credential=credential)

    secret = client.get_secret(secret_name, version=version)
    logger.debug(f"Fetched secret '{secret_name}' from Azure Key Vault '{vault_url}'")
    return secret.value


# ── Databricks Secret Scope ────────────────────────────────────────────────


def _resolve_databricks(uri: str) -> Optional[str]:
    """Fetch a secret from a Databricks secret scope.

    URI format: ``databricks://scope-name/key-name``

    Only works inside a Databricks notebook or job where ``dbutils``
    is available in the global scope.
    """
    path = uri.split("://", 1)[1] if "://" in uri else uri
    parts = path.split("/")
    if len(parts) < 2:
        raise ValueError(f"Invalid databricks URI: '{uri}'. Expected: databricks://scope-name/key-name")

    scope = parts[0]
    key_name = parts[1]

    # Try to get dbutils from the global scope (Databricks runtime)
    try:
        # Method 1: Direct global (notebook environment)
        import builtins

        dbutils = getattr(builtins, "dbutils", None)

        # Method 2: pyspark.dbutils
        if dbutils is None:
            try:
                from pyspark.dbutils import DBUtils
                from pyspark.sql import SparkSession

                spark = SparkSession.getActiveSession()
                if spark:
                    dbutils = DBUtils(spark)
            except (ImportError, Exception):
                pass

        # Method 3: IPython global
        if dbutils is None:
            try:
                from IPython import get_ipython

                ip = get_ipython()
                if ip:
                    dbutils = ip.user_ns.get("dbutils")
            except (ImportError, Exception):
                pass

        if dbutils is None:
            raise RuntimeError(
                "dbutils not available — Databricks secret scopes only work inside a Databricks notebook or job."
            )

        value = dbutils.secrets.get(scope=scope, key=key_name)
        logger.debug(f"Fetched secret '{key_name}' from Databricks scope '{scope}'")
        return value

    except Exception as exc:
        raise RuntimeError(f"Failed to fetch Databricks secret '{scope}/{key_name}': {exc}") from exc


# ── Environment Variable ───────────────────────────────────────────────────


def _resolve_env(uri: str) -> Optional[str]:
    """Read a key from an environment variable.

    URI format: ``env://VARIABLE_NAME``
    """
    var_name = uri.split("://", 1)[1] if "://" in uri else uri
    value = os.environ.get(var_name)
    if value is None:
        logger.warning(f"Environment variable '{var_name}' not set")
    return value


# ── HashiCorp Vault (stub) ─────────────────────────────────────────────────


def _resolve_hashicorp(uri: str) -> Optional[str]:
    """Fetch a secret from HashiCorp Vault.

    URI format: ``vault://path/to/secret``

    Requires: pip install hvac
    Uses VAULT_ADDR and VAULT_TOKEN environment variables.
    """
    path = uri.split("://", 1)[1] if "://" in uri else uri

    try:
        import hvac
    except ImportError:
        logger.warning("HashiCorp Vault integration requires: pip install hvac. Falling back to environment variable.")
        return None

    vault_addr = os.environ.get("VAULT_ADDR")
    vault_token = os.environ.get("VAULT_TOKEN")

    if not vault_addr:
        logger.warning("VAULT_ADDR not set — cannot connect to HashiCorp Vault")
        return None

    try:
        client = hvac.Client(url=vault_addr, token=vault_token)
        # Try KV v2 first, fall back to v1
        try:
            response = client.secrets.kv.v2.read_secret_version(path=path)
            data = response["data"]["data"]
        except Exception:
            response = client.secrets.kv.v1.read_secret(path=path)
            data = response["data"]

        # Return the first value, or "key" if it exists
        return data.get("key") or data.get("value") or next(iter(data.values()))

    except Exception as exc:
        logger.warning(f"HashiCorp Vault read failed for '{path}': {exc}")
        return None


# ── AWS KMS / Secrets Manager (stub) ────────────────────────────────────────


def _resolve_aws(uri: str) -> Optional[str]:
    """Fetch a secret from AWS Secrets Manager.

    URI format: ``aws-kms://secret-name`` or ``aws-secrets://secret-name``

    Requires: pip install boto3
    """
    secret_name = uri.split("://", 1)[1] if "://" in uri else uri

    try:
        import boto3
    except ImportError:
        logger.warning("AWS integration requires: pip install boto3. Falling back to environment variable.")
        return None

    try:
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_name)
        return response.get("SecretString")

    except Exception as exc:
        logger.warning(f"AWS Secrets Manager read failed for '{secret_name}': {exc}")
        return None
