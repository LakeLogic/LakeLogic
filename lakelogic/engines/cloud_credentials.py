"""
Automatic cloud credential resolution for Delta Lake operations.

This module automatically detects and configures cloud credentials for:
- Azure (Fabric LakeDB, Synapse Analytics, Unity Catalog on Azure)
  Auth methods supported (in priority order):
    1. User-provided BEARER_TOKEN, AZURE_STORAGE_ACCOUNT_KEY, or SAS_TOKEN
    2. Service Principal  — AZURE_CLIENT_ID + AZURE_CLIENT_SECRET + AZURE_TENANT_ID
       (pass explicitly or via environment variables)
    3. Account key       — AZURE_STORAGE_ACCOUNT_KEY env var
    4. Azure AD          — DefaultAzureCredential (az login / managed identity)
- AWS (Unity Catalog on AWS, Delta Lake on S3)
- GCP (Unity Catalog on GCP, Delta Lake on GCS)

Users don't need to manually configure storage_options - credentials are
automatically resolved from environment variables, CLI tools, or IAM roles.
"""

import os
import threading
from typing import Optional, Dict, Any
from loguru import logger


class CloudCredentialResolver:
    """
    Automatically resolve cloud credentials for Delta Lake operations.
    
    Supports:
    - Azure AD (DefaultAzureCredential)
    - AWS IAM roles and credentials
    - GCP service accounts
    - Environment variables
    """
    
    def __init__(
        self,
        use_key_vault: bool = True,
        azure_client_id: Optional[str] = None,
        azure_client_secret: Optional[str] = None,
        azure_tenant_id: Optional[str] = None,
    ):
        """
        Initialize credential resolver.

        Args:
            use_key_vault: Automatically resolve credentials from Key Vaults (default: True)
                - Azure Key Vault
                - AWS Secrets Manager
                - GCP Secret Manager
            azure_client_id: Azure service principal application (client) ID.
                Falls back to the ``AZURE_CLIENT_ID`` environment variable.
            azure_client_secret: Azure service principal client secret.
                Falls back to the ``AZURE_CLIENT_SECRET`` environment variable.
            azure_tenant_id: Azure AD tenant ID.
                Falls back to the ``AZURE_TENANT_ID`` environment variable.

        Example — service principal::

            resolver = CloudCredentialResolver(
                azure_client_id="...",
                azure_client_secret="...",
                azure_tenant_id="...",
            )
            options = resolver.resolve_storage_options(
                "abfss://silver@myaccount.dfs.core.windows.net/prod/orders/"
            )
        """
        self._lock = threading.Lock()
        self._azure_token = None
        self._azure_token_expiry = None
        self.use_key_vault = use_key_vault
        self._secret_cache = {}  # Cache secrets to avoid repeated API calls

        # Service principal credentials — explicit args take precedence over env vars
        self._azure_client_id     = azure_client_id     or os.getenv("AZURE_CLIENT_ID")
        self._azure_client_secret = azure_client_secret or os.getenv("AZURE_CLIENT_SECRET")
        self._azure_tenant_id     = azure_tenant_id     or os.getenv("AZURE_TENANT_ID")
    
    def resolve_storage_options(
        self,
        path: str,
        storage_options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Automatically resolve storage options based on path.
        
        Args:
            path: Storage path (s3://, abfss://, gs://, etc.)
            storage_options: Optional user-provided storage options (takes precedence)
        
        Returns:
            Resolved storage options dictionary
        
        Example:
            >>> resolver = CloudCredentialResolver()
            >>> 
            >>> # Azure - automatically uses Azure AD
            >>> options = resolver.resolve_storage_options("abfss://...")
            >>> 
            >>> # AWS - automatically uses IAM role or env vars
            >>> options = resolver.resolve_storage_options("s3://...")
            >>> 
            >>> # GCP - automatically uses service account
            >>> options = resolver.resolve_storage_options("gs://...")
        """
        # Start with user-provided options (if any)
        resolved_options = storage_options.copy() if storage_options else {}
        
        # Detect cloud provider from path
        if path.startswith("abfss://") or path.startswith("az://"):
            # Azure Blob/ADLS
            resolved_options = self._resolve_azure_credentials(resolved_options)
        
        elif path.startswith("s3://") or path.startswith("s3a://"):
            # AWS S3
            resolved_options = self._resolve_aws_credentials(resolved_options)
        
        elif path.startswith("gs://") or path.startswith("gcs://"):
            # GCP GCS
            resolved_options = self._resolve_gcp_credentials(resolved_options)
        
        return resolved_options
    
    def _resolve_azure_credentials(self, options: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve Azure credentials automatically.

        Priority:
        1. User-provided options (BEARER_TOKEN, AZURE_STORAGE_ACCOUNT_KEY, AZURE_STORAGE_SAS_TOKEN)
        2. Service Principal (ClientSecretCredential) via client_id + client_secret + tenant_id
        3. Account key via AZURE_STORAGE_ACCOUNT_KEY env var
        4. Azure AD (DefaultAzureCredential — covers az login, managed identity, workload identity)
        """
        # ── Priority 1: explicit user-supplied credential ────────────────────
        if "BEARER_TOKEN" in options or "AZURE_STORAGE_ACCOUNT_KEY" in options:
            logger.debug("Using user-provided Azure token/account-key credential")
            return options

        if "AZURE_STORAGE_SAS_TOKEN" in options:
            logger.debug("Using user-provided Azure SAS token")
            return options

        # Check for SAS token in env var
        sas_token = os.getenv("AZURE_STORAGE_SAS_TOKEN")
        if sas_token:
            logger.debug("Using Azure SAS token from environment")
            options["AZURE_STORAGE_SAS_TOKEN"] = sas_token
            return options

        # Propagate account name (needed by some Delta-RS backends)
        account_name = options.get("AZURE_STORAGE_ACCOUNT_NAME") or os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
        if account_name:
            options["AZURE_STORAGE_ACCOUNT_NAME"] = account_name

        # ── Priority 2: service principal (client_id + client_secret + tenant_id) ──
        if self._azure_client_id and self._azure_client_secret and self._azure_tenant_id:
            token = self._get_sp_token()
            if token:
                options["BEARER_TOKEN"] = token
                return options

        # ── Priority 3: account key from env var ─────────────────────────────
        account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
        if account_key:
            logger.debug("Using Azure account key from environment")
            options["AZURE_STORAGE_ACCOUNT_KEY"] = account_key
            return options

        # ── Priority 4: DefaultAzureCredential (az login / managed identity) ─
        try:
            from azure.identity import DefaultAzureCredential
            from datetime import datetime, timedelta

            with self._lock:
                if self._azure_token and self._azure_token_expiry:
                    if datetime.now() < self._azure_token_expiry:
                        logger.debug("Using cached Azure AD token (DefaultAzureCredential)")
                        options["BEARER_TOKEN"] = self._azure_token
                        return options

                logger.debug("Acquiring Azure AD token via DefaultAzureCredential")
                credential = DefaultAzureCredential()
                token = credential.get_token("https://storage.azure.com/.default")

                self._azure_token = token.token
                self._azure_token_expiry = datetime.now() + timedelta(minutes=50)

            options["BEARER_TOKEN"] = self._azure_token
            logger.info("✅ Azure AD authentication successful (DefaultAzureCredential)")
            return options

        except ImportError:
            logger.warning(
                "azure-identity not installed. Install with: pip install azure-identity\n"
                "Falling back to unauthenticated access (public containers only)."
            )
        except Exception as e:
            logger.warning(f"Azure AD (DefaultAzureCredential) failed: {e}")

        return options

    def _get_sp_token(self, scope: str = "https://storage.azure.com/.default") -> Optional[str]:
        """
        Acquire a bearer token using service-principal credentials
        (ClientSecretCredential — tenant_id + client_id + client_secret).

        Returns the token string, or None on failure.
        """
        from datetime import datetime, timedelta

        with self._lock:
            if self._azure_token and self._azure_token_expiry:
                if datetime.now() < self._azure_token_expiry:
                    logger.debug("Using cached Azure SP bearer token")
                    return self._azure_token

        try:
            from azure.identity import ClientSecretCredential

            logger.debug(
                f"Acquiring Azure bearer token via service principal "
                f"(client_id={self._azure_client_id}, tenant={self._azure_tenant_id})"
            )
            credential = ClientSecretCredential(
                tenant_id=self._azure_tenant_id,
                client_id=self._azure_client_id,
                client_secret=self._azure_client_secret,
            )
            token = credential.get_token(scope)

            with self._lock:
                self._azure_token = token.token
                self._azure_token_expiry = datetime.now() + timedelta(minutes=50)

            logger.info(
                f"✅ Azure service-principal authentication successful "
                f"(client_id={self._azure_client_id})"
            )
            return token.token

        except ImportError:
            logger.warning(
                "azure-identity not installed. Install with: pip install azure-identity"
            )
        except Exception as e:
            logger.warning(f"Service-principal authentication failed: {e}")

        return None
    
    def _resolve_aws_credentials(self, options: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve AWS credentials automatically.
        
        Priority:
        1. User-provided options (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        2. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
        3. AWS IAM role (boto3 default credential chain)
        """
        # If user already provided credentials, use them
        if "AWS_ACCESS_KEY_ID" in options and "AWS_SECRET_ACCESS_KEY" in options:
            logger.debug("Using user-provided AWS credentials")
            return options
        
        # Try environment variables
        access_key = os.getenv("AWS_ACCESS_KEY_ID")
        secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION")
        
        if access_key and secret_key:
            logger.debug("Using AWS credentials from environment")
            options["AWS_ACCESS_KEY_ID"] = access_key
            options["AWS_SECRET_ACCESS_KEY"] = secret_key
            if region:
                options["AWS_REGION"] = region
            return options
        
        # Try AWS IAM role (boto3 will handle this automatically)
        try:
            import boto3
            
            # Test if we can get credentials from boto3
            session = boto3.Session()
            credentials = session.get_credentials()
            
            if credentials:
                logger.debug("Using AWS IAM role credentials")
                options["AWS_ACCESS_KEY_ID"] = credentials.access_key
                options["AWS_SECRET_ACCESS_KEY"] = credentials.secret_key
                if credentials.token:
                    options["AWS_SESSION_TOKEN"] = credentials.token
                
                # Get region
                if not region:
                    region = session.region_name
                if region:
                    options["AWS_REGION"] = region
                
                logger.info("✅ AWS IAM role authentication successful")
                return options
        
        except ImportError:
            logger.warning(
                "boto3 not installed. Install with: pip install boto3\n"
                "Falling back to environment variables."
            )
        except Exception as e:
            logger.warning(f"AWS credential resolution failed: {e}\nFalling back to environment variables.")
        
        return options
    
    def _resolve_gcp_credentials(self, options: Dict[str, Any]) -> Dict[str, Any]:
        """
        Resolve GCP credentials automatically.
        
        Priority:
        1. User-provided options (GOOGLE_SERVICE_ACCOUNT)
        2. Environment variable (GOOGLE_APPLICATION_CREDENTIALS)
        3. GCP Application Default Credentials
        """
        # If user already provided service account, use it
        if "GOOGLE_SERVICE_ACCOUNT" in options:
            logger.debug("Using user-provided GCP service account")
            return options
        
        # Try environment variable
        service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        if service_account_path:
            logger.debug("Using GCP service account from environment")
            options["GOOGLE_SERVICE_ACCOUNT"] = service_account_path
            return options
        
        # Try Application Default Credentials
        try:
            from google.auth import default
            
            credentials, project = default()
            logger.debug("Using GCP Application Default Credentials")
            
            # For Delta-RS, we need to provide the service account file path
            # If using ADC, we can't easily get the file path, so we'll rely on
            # the environment variable or user-provided path
            logger.info("✅ GCP Application Default Credentials available")
            
            # Note: Delta-RS requires explicit service account file path
            # ADC works for google-cloud-storage but not for Delta-RS directly
            logger.warning(
                "GCP Application Default Credentials detected, but Delta-RS requires "
                "explicit service account file path. Set GOOGLE_APPLICATION_CREDENTIALS "
                "environment variable."
            )
        
        except ImportError:
            logger.warning(
                "google-auth not installed. Install with: pip install google-auth\n"
                "Falling back to environment variables."
            )
        except Exception as e:
            logger.warning(f"GCP credential resolution failed: {e}\nFalling back to environment variables.")
        
        return options
    
    def get_secret(
        self,
        secret_name: str,
        vault_url: Optional[str] = None,
        cloud_provider: Optional[str] = None
    ) -> Optional[str]:
        """
        Get secret from Key Vault (Azure, AWS, GCP).
        
        Args:
            secret_name: Name of the secret
            vault_url: Optional vault URL (Azure Key Vault URL, AWS region, GCP project)
            cloud_provider: Optional cloud provider ("azure", "aws", "gcp")
                If not specified, will try to auto-detect
        
        Returns:
            Secret value or None if not found
        
        Example:
            >>> resolver = CloudCredentialResolver()
            >>> 
            >>> # Azure Key Vault
            >>> password = resolver.get_secret(
            ...     "db-password",
            ...     vault_url="https://myvault.vault.azure.net/"
            ... )
            >>> 
            >>> # AWS Secrets Manager
            >>> api_key = resolver.get_secret(
            ...     "api-key",
            ...     vault_url="us-west-2",
            ...     cloud_provider="aws"
            ... )
        """
        if not self.use_key_vault:
            logger.debug("Key Vault resolution disabled")
            return None
        
        # Check cache
        cache_key = f"{cloud_provider}:{vault_url}:{secret_name}"
        with self._lock:
            if cache_key in self._secret_cache:
                logger.debug(f"Using cached secret: {secret_name}")
                return self._secret_cache[cache_key]
        
        # Auto-detect cloud provider if not specified
        if not cloud_provider:
            if vault_url and "vault.azure.net" in vault_url:
                cloud_provider = "azure"
            elif os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION"):
                cloud_provider = "aws"
            elif os.getenv("GOOGLE_CLOUD_PROJECT"):
                cloud_provider = "gcp"
        
        # Resolve secret
        secret_value = None
        
        if cloud_provider == "azure":
            secret_value = self._get_azure_key_vault_secret(secret_name, vault_url)
        elif cloud_provider == "aws":
            secret_value = self._get_aws_secret(secret_name, vault_url)
        elif cloud_provider == "gcp":
            secret_value = self._get_gcp_secret(secret_name, vault_url)
        else:
            logger.warning(f"Unknown cloud provider: {cloud_provider}")
        
        # Cache secret
        if secret_value:
            with self._lock:
                self._secret_cache[cache_key] = secret_value
        
        return secret_value
    
    def _get_azure_key_vault_secret(
        self,
        secret_name: str,
        vault_url: Optional[str] = None,
    ) -> Optional[str]:
        """
        Get a secret from Azure Key Vault.

        Auth priority:
        1. Service principal (ClientSecretCredential) if client_id / client_secret /
           tenant_id were supplied to this resolver instance or via env vars.
        2. DefaultAzureCredential (az login, managed identity, workload identity).
        """
        try:
            from azure.keyvault.secrets import SecretClient

            # Resolve vault URL
            if not vault_url:
                vault_url = os.getenv("AZURE_KEY_VAULT_URL")
            if not vault_url:
                logger.warning(
                    "Azure Key Vault URL not provided. "
                    "Pass vault_url or set AZURE_KEY_VAULT_URL."
                )
                return None

            # Choose credential
            if self._azure_client_id and self._azure_client_secret and self._azure_tenant_id:
                from azure.identity import ClientSecretCredential
                credential = ClientSecretCredential(
                    tenant_id=self._azure_tenant_id,
                    client_id=self._azure_client_id,
                    client_secret=self._azure_client_secret,
                )
                logger.debug(
                    f"Key Vault: using service-principal credential "
                    f"(client_id={self._azure_client_id})"
                )
            else:
                from azure.identity import DefaultAzureCredential
                credential = DefaultAzureCredential()
                logger.debug("Key Vault: using DefaultAzureCredential")

            client = SecretClient(vault_url=vault_url, credential=credential)
            secret = client.get_secret(secret_name)
            logger.info(f"✅ Retrieved secret '{secret_name}' from Azure Key Vault ({vault_url})")
            return secret.value

        except ImportError:
            logger.warning(
                "Azure Key Vault libraries not installed. "
                "Install with: pip install azure-keyvault-secrets azure-identity"
            )
            return None
        except Exception as e:
            logger.warning(f"Failed to retrieve secret '{secret_name}' from Azure Key Vault: {e}")
            return None
    
    def _get_aws_secret(
        self,
        secret_name: str,
        region: Optional[str] = None
    ) -> Optional[str]:
        """Get secret from AWS Secrets Manager."""
        try:
            import boto3
            import json
            
            # Get region from environment if not provided
            if not region:
                region = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"
            
            logger.debug(f"Retrieving secret '{secret_name}' from AWS Secrets Manager")
            
            client = boto3.client('secretsmanager', region_name=region)
            response = client.get_secret_value(SecretId=secret_name)
            
            # Secrets can be string or binary
            if 'SecretString' in response:
                secret_value = response['SecretString']
                
                # Try to parse as JSON (AWS often stores secrets as JSON)
                try:
                    secret_dict = json.loads(secret_value)
                    # If it's a dict with a single key, return that value
                    if len(secret_dict) == 1:
                        secret_value = list(secret_dict.values())[0]
                except json.JSONDecodeError:
                    pass  # Not JSON, use as-is
                
                logger.info(f"✅ Retrieved secret '{secret_name}' from AWS Secrets Manager")
                return secret_value
            else:
                # Binary secret
                import base64
                secret_value = base64.b64decode(response['SecretBinary']).decode('utf-8')
                logger.info(f"✅ Retrieved secret '{secret_name}' from AWS Secrets Manager")
                return secret_value
        
        except ImportError:
            logger.warning("boto3 not installed. Install with: pip install boto3")
            return None
        except Exception as e:
            logger.warning(f"Failed to retrieve secret from AWS Secrets Manager: {e}")
            return None
    
    def _get_gcp_secret(
        self,
        secret_name: str,
        project_id: Optional[str] = None
    ) -> Optional[str]:
        """Get secret from GCP Secret Manager."""
        try:
            from google.cloud import secretmanager
            
            # Get project ID from environment if not provided
            if not project_id:
                project_id = os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT")
            
            if not project_id:
                logger.warning("GCP project ID not provided")
                return None
            
            logger.debug(f"Retrieving secret '{secret_name}' from GCP Secret Manager")
            
            client = secretmanager.SecretManagerServiceClient()
            
            # Build the resource name (use latest version)
            name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
            
            response = client.access_secret_version(request={"name": name})
            secret_value = response.payload.data.decode('UTF-8')
            
            logger.info(f"✅ Retrieved secret '{secret_name}' from GCP Secret Manager")
            return secret_value
        
        except ImportError:
            logger.warning(
                "GCP Secret Manager library not installed. "
                "Install with: pip install google-cloud-secret-manager"
            )
            return None
        except Exception as e:
            logger.warning(f"Failed to retrieve secret from GCP Secret Manager: {e}")
            return None
    
    def clear_cache(self):
        """Clear cached credentials and secrets."""
        with self._lock:
            self._azure_token = None
            self._azure_token_expiry = None
            self._secret_cache = {}
        logger.debug("Cleared credential and secret cache")


# Global credential resolver instance
_global_resolver: Optional[CloudCredentialResolver] = None
_resolver_lock = threading.Lock()


def get_credential_resolver() -> CloudCredentialResolver:
    """
    Get or create the global credential resolver.
    
    Returns:
        Global CloudCredentialResolver instance
    
    Example:
        >>> from lakelogic.engines.cloud_credentials import get_credential_resolver
        >>> resolver = get_credential_resolver()
        >>> options = resolver.resolve_storage_options("abfss://...")
    """
    global _global_resolver
    if _global_resolver is not None:
        return _global_resolver
    with _resolver_lock:
        # Double-checked locking
        if _global_resolver is None:
            _global_resolver = CloudCredentialResolver()
    return _global_resolver


def resolve_storage_options(
    path: str,
    storage_options: Optional[Dict[str, Any]] = None,
    azure_client_id: Optional[str] = None,
    azure_client_secret: Optional[str] = None,
    azure_tenant_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Convenience function to resolve storage options for a given path.

    For most scenarios the global resolver (which reads env vars automatically)
    is sufficient.  Pass ``azure_client_id`` / ``azure_client_secret`` /
    ``azure_tenant_id`` explicitly when you need per-call service-principal
    credentials (e.g. multi-tenant scenarios).

    Args:
        path: Storage path (``s3://``, ``abfss://``, ``gs://``, etc.)
        storage_options: Optional user-provided storage options (highest priority).
        azure_client_id: Service principal client ID (overrides env var).
        azure_client_secret: Service principal client secret (overrides env var).
        azure_tenant_id: Azure AD tenant ID (overrides env var).

    Returns:
        Resolved storage options dictionary suitable for Polars / Delta-RS.

    Examples::

        from lakelogic.engines.cloud_credentials import resolve_storage_options

        # Auto-resolve via az login / managed identity / env vars
        options = resolve_storage_options(
            "abfss://silver@myaccount.dfs.core.windows.net/prod/orders/"
        )

        # Explicit service principal
        options = resolve_storage_options(
            "abfss://silver@myaccount.dfs.core.windows.net/prod/orders/",
            azure_client_id="<app-id>",
            azure_client_secret="<secret>",
            azure_tenant_id="<tenant-id>",
        )

        # AWS IAM role (auto-resolved via boto3)
        options = resolve_storage_options("s3://bucket/table/")
    """
    # If caller passes SP credentials explicitly, use a one-shot resolver
    if azure_client_id or azure_client_secret or azure_tenant_id:
        resolver = CloudCredentialResolver(
            azure_client_id=azure_client_id,
            azure_client_secret=azure_client_secret,
            azure_tenant_id=azure_tenant_id,
        )
        return resolver.resolve_storage_options(path, storage_options)

    # Otherwise use the global (env-var-aware) singleton
    return get_credential_resolver().resolve_storage_options(path, storage_options)


# ── Databricks secret integration ─────────────────────────────────────────────

def _get_dbutils():
    """
    Retrieve the Databricks ``dbutils`` object from the active IPython shell.

    Raises ``RuntimeError`` with a clear message when called outside of a
    Databricks notebook / job runtime.
    """
    try:
        import IPython
        shell = IPython.get_ipython()
        if shell is not None and hasattr(shell, "user_ns") and "dbutils" in shell.user_ns:
            return shell.user_ns["dbutils"]
    except ImportError:
        pass

    # Fallback: try the pyspark context (Databricks Connect / job clusters)
    try:
        from pyspark.dbutils import DBUtils  # type: ignore
        from pyspark.sql import SparkSession  # type: ignore
        spark = SparkSession.getActiveSession()
        if spark:
            return DBUtils(spark)
    except Exception:
        pass

    raise RuntimeError(
        "dbutils is not available. "
        "DatabricksSecretResolver must be called inside a Databricks notebook or job. "
        "Outside Databricks, pass credentials directly to CloudCredentialResolver "
        "or set the corresponding environment variables."
    )


class DatabricksSecretResolver:
    """
    Build a :class:`CloudCredentialResolver` from Databricks secret scopes.

    Fetches cloud credentials from ``dbutils.secrets.get()`` (which is backed
    by Azure Key Vault, AWS Secrets Manager, or manually managed scopes) and
    returns a fully configured :class:`CloudCredentialResolver` ready for use
    with :class:`~lakelogic.engines.delta_adapter.DeltaAdapter`,
    :class:`~lakelogic.core.processor.DataProcessor`, and every other
    LakeLogic component that accepts ``storage_options``.

    Supported clouds
    ----------------
    ``"azure"``
        Reads ``AZURE_CLIENT_ID``, ``AZURE_CLIENT_SECRET``, ``AZURE_TENANT_ID``
        from the secret scope and uses Azure service-principal auth.

    ``"aws"``
        Reads ``AWS_ACCESS_KEY_ID``, ``AWS_SECRET_ACCESS_KEY``, ``AWS_REGION``
        and injects them as environment variables so boto3 / Delta-RS pick them
        up automatically.

    ``"gcp"``
        Reads ``GCP_SERVICE_ACCOUNT_KEY`` (full JSON content) and
        ``GOOGLE_CLOUD_PROJECT``, writes the key to a temp file, and sets
        ``GOOGLE_APPLICATION_CREDENTIALS`` accordingly.

    Examples
    --------
    **Azure (Key Vault-backed scope):**

    .. code-block:: python

        from lakelogic.engines.cloud_credentials import DatabricksSecretResolver

        resolver = DatabricksSecretResolver.for_cloud("azure", scope="lakelogic")
        options  = resolver.resolve_storage_options(
            "abfss://silver@myaccount.dfs.core.windows.net/orders/"
        )

    **AWS:**

    .. code-block:: python

        resolver = DatabricksSecretResolver.for_cloud("aws", scope="lakelogic-aws")
        options  = resolver.resolve_storage_options("s3://my-bucket/silver/orders/")

    **GCP:**

    .. code-block:: python

        resolver = DatabricksSecretResolver.for_cloud("gcp", scope="lakelogic-gcp")
        options  = resolver.resolve_storage_options("gs://my-bucket/silver/orders/")

    **All three using default scope names:**

    .. code-block:: python

        azure_resolver = DatabricksSecretResolver.for_cloud("azure")
        aws_resolver   = DatabricksSecretResolver.for_cloud("aws")
        gcp_resolver   = DatabricksSecretResolver.for_cloud("gcp")
    """

    # Default secret scope names per cloud — override via ``scope`` argument
    _DEFAULT_SCOPES = {
        "azure": "lakelogic",
        "aws":   "lakelogic-aws",
        "gcp":   "lakelogic-gcp",
    }

    # Secret key names to look up in each scope
    _SECRET_KEYS = {
        "azure": {
            "client_id":     "AZURE_CLIENT_ID",
            "client_secret": "AZURE_CLIENT_SECRET",
            "tenant_id":     "AZURE_TENANT_ID",
        },
        "aws": {
            "access_key_id":     "AWS_ACCESS_KEY_ID",
            "secret_access_key": "AWS_SECRET_ACCESS_KEY",
            "region":            "AWS_REGION",
        },
        "gcp": {
            "service_account_key": "GCP_SERVICE_ACCOUNT_KEY",
            "project":             "GOOGLE_CLOUD_PROJECT",
        },
    }

    @classmethod
    def for_cloud(
        cls,
        cloud: str,
        scope: Optional[str] = None,
        dbutils=None,
    ) -> "CloudCredentialResolver":
        """
        Return a :class:`CloudCredentialResolver` configured from Databricks
        secrets for the given cloud provider.

        Args:
            cloud:   Cloud provider — ``"azure"``, ``"aws"``, or ``"gcp"``.
            scope:   Databricks secret scope name.  Defaults to
                     ``"lakelogic"`` (Azure), ``"lakelogic-aws"`` (AWS), or
                     ``"lakelogic-gcp"`` (GCP).
            dbutils: Explicit ``dbutils`` object.  When ``None`` (default),
                     the object is auto-detected from the active notebook /
                     job session.

        Returns:
            A configured :class:`CloudCredentialResolver`.

        Raises:
            ValueError:  Unknown cloud provider.
            RuntimeError: Called outside a Databricks runtime without an
                          explicit ``dbutils`` argument.
        """
        cloud = cloud.lower()
        if cloud not in cls._DEFAULT_SCOPES:
            raise ValueError(
                f"Unknown cloud {cloud!r}. "
                f"Valid options: {list(cls._DEFAULT_SCOPES)}"
            )

        scope = scope or cls._DEFAULT_SCOPES[cloud]
        dbu   = dbutils or _get_dbutils()

        handler = {
            "azure": cls._build_azure,
            "aws":   cls._build_aws,
            "gcp":   cls._build_gcp,
        }[cloud]

        return handler(dbu, scope)

    # ── per-cloud builders ─────────────────────────────────────────────────

    @staticmethod
    def _build_azure(dbu, scope: str) -> "CloudCredentialResolver":
        keys = DatabricksSecretResolver._SECRET_KEYS["azure"]
        client_id     = dbu.secrets.get(scope, keys["client_id"])
        client_secret = dbu.secrets.get(scope, keys["client_secret"])
        tenant_id     = dbu.secrets.get(scope, keys["tenant_id"])
        logger.info(
            f"DatabricksSecretResolver: loaded Azure SP credentials "
            f"(scope={scope!r}, client_id={client_id})"
        )
        return CloudCredentialResolver(
            azure_client_id=client_id,
            azure_client_secret=client_secret,
            azure_tenant_id=tenant_id,
        )

    @staticmethod
    def _build_aws(dbu, scope: str) -> "CloudCredentialResolver":
        import os
        keys = DatabricksSecretResolver._SECRET_KEYS["aws"]
        os.environ["AWS_ACCESS_KEY_ID"]     = dbu.secrets.get(scope, keys["access_key_id"])
        os.environ["AWS_SECRET_ACCESS_KEY"] = dbu.secrets.get(scope, keys["secret_access_key"])
        region = dbu.secrets.get(scope, keys["region"])
        if region:
            os.environ["AWS_REGION"] = region
        logger.info(
            f"DatabricksSecretResolver: loaded AWS credentials from "
            f"scope={scope!r} into environment"
        )
        return CloudCredentialResolver()   # boto3 chain reads env vars

    @staticmethod
    def _build_gcp(dbu, scope: str) -> "CloudCredentialResolver":
        import os, tempfile
        keys = DatabricksSecretResolver._SECRET_KEYS["gcp"]
        sa_json  = dbu.secrets.get(scope, keys["service_account_key"])
        project  = dbu.secrets.get(scope, keys["project"])

        # Delta-RS / google-cloud-storage require a file path, not inline JSON
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False, prefix="lakelogic_sa_"
        ) as f:
            f.write(sa_json)
            sa_path = f.name

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = sa_path
        if project:
            os.environ["GOOGLE_CLOUD_PROJECT"] = project
        logger.info(
            f"DatabricksSecretResolver: loaded GCP service account "
            f"(scope={scope!r}, project={project})"
        )
        return CloudCredentialResolver()   # google-auth reads GOOGLE_APPLICATION_CREDENTIALS


def from_databricks(
    cloud: str,
    scope: Optional[str] = None,
    dbutils=None,
) -> "CloudCredentialResolver":
    """
    Convenience top-level function — equivalent to
    ``DatabricksSecretResolver.for_cloud(...)``.

    Builds a :class:`CloudCredentialResolver` by pulling cloud credentials
    from a Databricks secret scope (backed by Azure Key Vault, AWS Secrets
    Manager, or a manually created scope).

    Args:
        cloud:   ``"azure"``, ``"aws"``, or ``"gcp"``.
        scope:   Databricks secret scope name (uses sensible defaults).
        dbutils: Explicit ``dbutils`` — auto-detected when ``None``.

    Returns:
        Configured :class:`CloudCredentialResolver`.

    Examples
    --------
    .. code-block:: python

        from lakelogic.engines.cloud_credentials import from_databricks
        from lakelogic.engines.delta_adapter import DeltaAdapter

        # Azure — reads AZURE_CLIENT_ID / SECRET / TENANT_ID from "lakelogic" scope
        resolver = from_databricks("azure")
        options  = resolver.resolve_storage_options(
            "abfss://silver@myaccount.dfs.core.windows.net/orders/"
        )
        df = DeltaAdapter(storage_options=options).read(
            "abfss://silver@myaccount.dfs.core.windows.net/orders/"
        )

        # AWS — reads AWS_ACCESS_KEY_ID / SECRET / REGION from "lakelogic-aws" scope
        resolver = from_databricks("aws")
        options  = resolver.resolve_storage_options("s3://my-bucket/silver/orders/")

        # GCP — reads GCP_SERVICE_ACCOUNT_KEY / GOOGLE_CLOUD_PROJECT from "lakelogic-gcp"
        resolver = from_databricks("gcp")
        options  = resolver.resolve_storage_options("gs://my-bucket/silver/orders/")

        # Custom scope name
        resolver = from_databricks("azure", scope="my-custom-scope")
    """
    return DatabricksSecretResolver.for_cloud(cloud, scope=scope, dbutils=dbutils)
