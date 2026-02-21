"""
Automatic cloud credential resolution for Delta Lake operations.

This module automatically detects and configures cloud credentials for:
- Azure (Fabric LakeDB, Synapse Analytics, Unity Catalog on Azure)
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
    
    def __init__(self, use_key_vault: bool = True):
        """
        Initialize credential resolver.
        
        Args:
            use_key_vault: Automatically resolve credentials from Key Vaults (default: True)
                - Azure Key Vault
                - AWS Secrets Manager
                - GCP Secret Manager
        """
        self._lock = threading.Lock()
        self._azure_token = None
        self._azure_token_expiry = None
        self.use_key_vault = use_key_vault
        self._secret_cache = {}  # Cache secrets to avoid repeated API calls
    
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
        1. User-provided options (BEARER_TOKEN, AZURE_STORAGE_ACCOUNT_KEY)
        2. Environment variables (AZURE_STORAGE_ACCOUNT_KEY)
        3. Azure AD (DefaultAzureCredential)
        """
        # If user already provided token or key, use it
        if "BEARER_TOKEN" in options or "AZURE_STORAGE_ACCOUNT_KEY" in options:
            logger.debug("Using user-provided Azure credentials")
            return options
        
        # Check for account name (required)
        account_name = options.get("AZURE_STORAGE_ACCOUNT_NAME") or os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
        if account_name:
            options["AZURE_STORAGE_ACCOUNT_NAME"] = account_name
        
        # Try environment variable (account key)
        account_key = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
        if account_key:
            logger.debug("Using Azure account key from environment")
            options["AZURE_STORAGE_ACCOUNT_KEY"] = account_key
            return options
        
        # Try Azure AD (DefaultAzureCredential) — protected by lock to avoid
        # concurrent threads racing to refresh the cached token.
        try:
            from azure.identity import DefaultAzureCredential
            from datetime import datetime, timedelta
            
            with self._lock:
                # Check if we have a cached token that's still valid
                if self._azure_token and self._azure_token_expiry:
                    if datetime.now() < self._azure_token_expiry:
                        logger.debug("Using cached Azure AD token")
                        options["BEARER_TOKEN"] = self._azure_token
                        return options
                
                # Get new token
                logger.debug("Acquiring Azure AD token via DefaultAzureCredential")
                credential = DefaultAzureCredential()
                token = credential.get_token("https://storage.azure.com/.default")
                
                # Cache token (expires in 1 hour, refresh after 50 minutes)
                self._azure_token = token.token
                self._azure_token_expiry = datetime.now() + timedelta(minutes=50)
            
            options["BEARER_TOKEN"] = token.token
            logger.info("✅ Azure AD authentication successful")
            return options
        
        except ImportError:
            logger.warning(
                "Azure Identity not installed. Install with: pip install azure-identity\n"
                "Falling back to environment variables."
            )
        except Exception as e:
            logger.warning(f"Azure AD authentication failed: {e}\nFalling back to environment variables.")
        
        return options
    
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
        vault_url: Optional[str] = None
    ) -> Optional[str]:
        """Get secret from Azure Key Vault."""
        try:
            from azure.keyvault.secrets import SecretClient
            from azure.identity import DefaultAzureCredential
            
            # Get vault URL from environment if not provided
            if not vault_url:
                vault_url = os.getenv("AZURE_KEY_VAULT_URL")
            
            if not vault_url:
                logger.warning("Azure Key Vault URL not provided")
                return None
            
            logger.debug(f"Retrieving secret '{secret_name}' from Azure Key Vault")
            
            credential = DefaultAzureCredential()
            client = SecretClient(vault_url=vault_url, credential=credential)
            
            secret = client.get_secret(secret_name)
            logger.info(f"✅ Retrieved secret '{secret_name}' from Azure Key Vault")
            
            return secret.value
        
        except ImportError:
            logger.warning(
                "Azure Key Vault libraries not installed. "
                "Install with: pip install azure-keyvault-secrets azure-identity"
            )
            return None
        except Exception as e:
            logger.warning(f"Failed to retrieve secret from Azure Key Vault: {e}")
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
    storage_options: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Convenience function to resolve storage options.
    
    Args:
        path: Storage path (s3://, abfss://, gs://, etc.)
        storage_options: Optional user-provided storage options
    
    Returns:
        Resolved storage options dictionary
    
    Example:
        >>> from lakelogic.engines.cloud_credentials import resolve_storage_options
        >>> 
        >>> # Automatically resolves Azure AD credentials
        >>> options = resolve_storage_options("abfss://workspace@onelake.dfs.fabric.microsoft.com/...")
        >>> 
        >>> # Automatically resolves AWS IAM role
        >>> options = resolve_storage_options("s3://bucket/table/")
    """
    resolver = get_credential_resolver()
    return resolver.resolve_storage_options(path, storage_options)
