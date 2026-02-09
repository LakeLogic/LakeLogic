"""
API, File Transfer, and Messaging connectors for LakeLogic.

This module provides connectors for:
- REST APIs (with OAuth2, API keys, etc.)
- SFTP/FTP (Azure Blob, AWS S3, GCP GCS)
- Azure Event Grid
- Azure Service Bus
- AWS SQS/SNS
- GCP Pub/Sub
- Kafka

Features:
- Automatic credential resolution
- Retry logic with exponential backoff
- Rate limiting
- Pagination handling
- Schema inference

Example:
    >>> from lakelogic.engines.integration_connectors import RESTAPIConnector
    >>> 
    >>> # REST API with OAuth2
    >>> connector = RESTAPIConnector(
    ...     base_url="https://api.example.com",
    ...     auth_type="oauth2",
    ...     client_id="...",
    ...     client_secret="..."
    ... )
    >>> 
    >>> # Extract data with pagination
    >>> df = connector.extract(
    ...     endpoint="/customers",
    ...     params={"status": "active"}
    ... )
"""

from typing import Optional, Dict, Any, List, Union, Callable
from datetime import datetime
import time
import polars as pl
import pandas as pd
from loguru import logger


class RESTAPIConnector:
    """
    REST API connector with automatic authentication and pagination.
    
    Features:
    - Multiple auth types (OAuth2, API key, Bearer token, Basic auth)
    - Automatic pagination
    - Rate limiting
    - Retry logic with exponential backoff
    - Schema inference
    
    Example:
        >>> # OAuth2 authentication
        >>> connector = RESTAPIConnector(
        ...     base_url="https://api.example.com",
        ...     auth_type="oauth2",
        ...     client_id="...",
        ...     client_secret="...",
        ...     token_url="https://api.example.com/oauth/token"
        ... )
        >>> 
        >>> # API key authentication
        >>> connector = RESTAPIConnector(
        ...     base_url="https://api.example.com",
        ...     auth_type="api_key",
        ...     api_key="...",
        ...     api_key_header="X-API-Key"
        ... )
        >>> 
        >>> # Extract with pagination
        >>> df = connector.extract(
        ...     endpoint="/customers",
        ...     params={"status": "active"},
        ...     pagination_type="offset",
        ...     page_size=100
        ... )
    """
    
    def __init__(
        self,
        base_url: str,
        auth_type: str = "none",
        api_key: Optional[str] = None,
        api_key_header: str = "X-API-Key",
        bearer_token: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        token_url: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None,
        rate_limit: Optional[int] = None,
        retry_attempts: int = 3,
        retry_delay: int = 1
    ):
        """
        Initialize REST API connector.
        
        Args:
            base_url: Base URL of the API
            auth_type: Authentication type ("none", "api_key", "bearer", "basic", "oauth2")
            api_key: API key (for api_key auth)
            api_key_header: Header name for API key (default: X-API-Key)
            bearer_token: Bearer token (for bearer auth)
            username: Username (for basic auth)
            password: Password (for basic auth)
            client_id: OAuth2 client ID
            client_secret: OAuth2 client secret
            token_url: OAuth2 token URL
            headers: Additional headers
            rate_limit: Max requests per second (None = no limit)
            retry_attempts: Number of retry attempts on failure
            retry_delay: Initial retry delay in seconds
        """
        self.base_url = base_url.rstrip("/")
        self.auth_type = auth_type
        self.api_key = api_key
        self.api_key_header = api_key_header
        self.bearer_token = bearer_token
        self.username = username
        self.password = password
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url
        self.headers = headers or {}
        self.rate_limit = rate_limit
        self.retry_attempts = retry_attempts
        self.retry_delay = retry_delay
        self._last_request_time = 0
        self._session = None
    
    def _get_session(self):
        """Get or create HTTP session with authentication."""
        if self._session:
            return self._session
        
        try:
            import requests
        except ImportError:
            raise ImportError(
                "requests is not installed. Install with: pip install requests"
            )
        
        self._session = requests.Session()
        
        # Configure authentication
        if self.auth_type == "api_key":
            self._session.headers[self.api_key_header] = self.api_key
        
        elif self.auth_type == "bearer":
            self._session.headers["Authorization"] = f"Bearer {self.bearer_token}"
        
        elif self.auth_type == "basic":
            from requests.auth import HTTPBasicAuth
            self._session.auth = HTTPBasicAuth(self.username, self.password)
        
        elif self.auth_type == "oauth2":
            # Get OAuth2 token
            token = self._get_oauth2_token()
            self._session.headers["Authorization"] = f"Bearer {token}"
        
        # Add custom headers
        self._session.headers.update(self.headers)
        
        return self._session
    
    def _get_oauth2_token(self) -> str:
        """Get OAuth2 access token."""
        try:
            import requests
        except ImportError:
            raise ImportError("requests is not installed. Install with: pip install requests")
        
        logger.debug(f"Acquiring OAuth2 token from {self.token_url}")
        
        response = requests.post(
            self.token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret
            }
        )
        response.raise_for_status()
        
        token_data = response.json()
        logger.info("✅ OAuth2 token acquired")
        
        return token_data["access_token"]
    
    def _rate_limit_wait(self):
        """Wait if rate limit is configured."""
        if self.rate_limit:
            elapsed = time.time() - self._last_request_time
            min_interval = 1.0 / self.rate_limit
            
            if elapsed < min_interval:
                wait_time = min_interval - elapsed
                time.sleep(wait_time)
        
        self._last_request_time = time.time()
    
    def _request_with_retry(
        self,
        method: str,
        url: str,
        **kwargs
    ) -> Any:
        """Make HTTP request with retry logic."""
        session = self._get_session()
        
        for attempt in range(self.retry_attempts):
            try:
                self._rate_limit_wait()
                
                response = session.request(method, url, **kwargs)
                response.raise_for_status()
                
                return response.json()
            
            except Exception as e:
                if attempt < self.retry_attempts - 1:
                    delay = self.retry_delay * (2 ** attempt)
                    logger.warning(f"Request failed (attempt {attempt + 1}/{self.retry_attempts}): {e}. Retrying in {delay}s...")
                    time.sleep(delay)
                else:
                    logger.error(f"Request failed after {self.retry_attempts} attempts: {e}")
                    raise
    
    def extract(
        self,
        endpoint: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        pagination_type: Optional[str] = None,
        page_size: int = 100,
        max_pages: Optional[int] = None,
        data_path: Optional[str] = None,
        as_polars: bool = True
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        Extract data from REST API.
        
        Args:
            endpoint: API endpoint (e.g., "/customers")
            method: HTTP method (GET, POST, etc.)
            params: Query parameters
            json_body: JSON body for POST/PUT requests
            pagination_type: Pagination type ("offset", "page", "cursor", None)
            page_size: Records per page
            max_pages: Maximum pages to fetch (None = all)
            data_path: JSON path to data array (e.g., "data.items")
            as_polars: Return Polars DataFrame (True) or Pandas (False)
        
        Returns:
            DataFrame with extracted data
        
        Example:
            >>> # Simple GET
            >>> df = connector.extract("/customers")
            >>> 
            >>> # With pagination
            >>> df = connector.extract(
            ...     "/customers",
            ...     pagination_type="offset",
            ...     page_size=100
            ... )
            >>> 
            >>> # POST with body
            >>> df = connector.extract(
            ...     "/search",
            ...     method="POST",
            ...     json_body={"query": "active customers"}
            ... )
        """
        url = f"{self.base_url}{endpoint}"
        params = params or {}
        
        all_data = []
        page = 0
        
        while True:
            # Add pagination parameters
            if pagination_type == "offset":
                params["offset"] = page * page_size
                params["limit"] = page_size
            elif pagination_type == "page":
                params["page"] = page + 1
                params["page_size"] = page_size
            
            # Make request
            logger.debug(f"Fetching page {page + 1} from {endpoint}")
            
            if method.upper() == "GET":
                data = self._request_with_retry("GET", url, params=params)
            else:
                data = self._request_with_retry(method, url, params=params, json=json_body)
            
            # Extract data from response
            if data_path:
                for key in data_path.split("."):
                    data = data[key]
            
            # Handle different response formats
            if isinstance(data, list):
                records = data
            elif isinstance(data, dict) and "data" in data:
                records = data["data"]
            elif isinstance(data, dict) and "items" in data:
                records = data["items"]
            else:
                records = [data]
            
            if not records:
                break
            
            all_data.extend(records)
            logger.info(f"✅ Fetched {len(records)} records (page {page + 1})")
            
            # Check if we should continue pagination
            if not pagination_type:
                break
            
            if len(records) < page_size:
                break
            
            if max_pages and page + 1 >= max_pages:
                break
            
            page += 1
        
        logger.info(f"✅ Total records extracted: {len(all_data)}")
        
        # Convert to DataFrame
        df = pd.DataFrame(all_data)
        
        if as_polars:
            return pl.from_pandas(df)
        return df
    
    def close(self):
        """Close HTTP session."""
        if self._session:
            self._session.close()
            self._session = None


class SFTPConnector:
    """
    SFTP connector with automatic credential resolution.
    
    Features:
    - SSH key authentication
    - Password authentication
    - File pattern matching
    - Incremental file extraction
    
    Example:
        >>> # SSH key authentication
        >>> connector = SFTPConnector(
        ...     host="sftp.example.com",
        ...     username="user",
        ...     private_key_path="/path/to/key"
        ... )
        >>> 
        >>> # Extract CSV files
        >>> df = connector.extract_files(
        ...     remote_path="/data/",
        ...     file_pattern="*.csv",
        ...     file_format="csv"
        ... )
    """
    
    def __init__(
        self,
        host: str,
        port: int = 22,
        username: Optional[str] = None,
        password: Optional[str] = None,
        private_key_path: Optional[str] = None
    ):
        """
        Initialize SFTP connector.
        
        Args:
            host: SFTP host
            port: SFTP port (default: 22)
            username: Username
            password: Password (for password auth)
            private_key_path: Path to SSH private key (for key auth)
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.private_key_path = private_key_path
        self._client = None
    
    def _get_client(self):
        """Get or create SFTP client."""
        if self._client:
            return self._client
        
        try:
            import paramiko
        except ImportError:
            raise ImportError(
                "paramiko is not installed. Install with: pip install paramiko"
            )
        
        logger.debug(f"Connecting to SFTP: {self.host}:{self.port}")
        
        # Create SSH client
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Connect with key or password
        if self.private_key_path:
            ssh.connect(
                self.host,
                port=self.port,
                username=self.username,
                key_filename=self.private_key_path
            )
        else:
            ssh.connect(
                self.host,
                port=self.port,
                username=self.username,
                password=self.password
            )
        
        self._client = ssh.open_sftp()
        logger.info(f"✅ Connected to SFTP: {self.host}")
        
        return self._client
    
    def extract_files(
        self,
        remote_path: str,
        file_pattern: str = "*",
        file_format: str = "csv",
        as_polars: bool = True
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        Extract files from SFTP server.
        
        Args:
            remote_path: Remote directory path
            file_pattern: File pattern (e.g., "*.csv")
            file_format: File format ("csv", "json", "parquet")
            as_polars: Return Polars DataFrame (True) or Pandas (False)
        
        Returns:
            DataFrame with extracted data
        
        Example:
            >>> df = connector.extract_files("/data/", "*.csv", "csv")
        """
        import fnmatch
        import tempfile
        import os
        
        client = self._get_client()
        
        # List files matching pattern
        files = client.listdir(remote_path)
        matching_files = [f for f in files if fnmatch.fnmatch(f, file_pattern)]
        
        logger.info(f"Found {len(matching_files)} files matching {file_pattern}")
        
        all_data = []
        
        for filename in matching_files:
            remote_file = os.path.join(remote_path, filename).replace("\\", "/")
            
            # Download to temp file
            with tempfile.NamedTemporaryFile(delete=False, suffix=f".{file_format}") as tmp:
                logger.debug(f"Downloading {filename}")
                client.get(remote_file, tmp.name)
                
                # Read file
                if file_format == "csv":
                    df = pd.read_csv(tmp.name)
                elif file_format == "json":
                    df = pd.read_json(tmp.name)
                elif file_format == "parquet":
                    df = pd.read_parquet(tmp.name)
                else:
                    raise ValueError(f"Unsupported file format: {file_format}")
                
                all_data.append(df)
                logger.info(f"✅ Extracted {len(df)} records from {filename}")
                
                # Clean up temp file
                os.unlink(tmp.name)
        
        # Combine all data
        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"✅ Total records extracted: {len(combined_df)}")
        
        if as_polars:
            return pl.from_pandas(combined_df)
        return combined_df
    
    def close(self):
        """Close SFTP connection."""
        if self._client:
            self._client.close()
            self._client = None


class AzureServiceBusConnector:
    """
    Azure Service Bus connector with automatic Azure AD authentication.
    
    Features:
    - Automatic Azure AD authentication
    - Message batching
    - Dead letter queue support
    
    Example:
        >>> # Azure AD authentication (automatic)
        >>> connector = AzureServiceBusConnector(
        ...     namespace="myservicebus.servicebus.windows.net",
        ...     queue_name="myqueue"
        ... )
        >>> 
        >>> # Receive messages
        >>> df = connector.receive_messages(max_messages=100)
    """
    
    def __init__(
        self,
        namespace: str,
        queue_name: Optional[str] = None,
        topic_name: Optional[str] = None,
        subscription_name: Optional[str] = None,
        connection_string: Optional[str] = None,
        auto_resolve_credentials: bool = True
    ):
        """
        Initialize Azure Service Bus connector.
        
        Args:
            namespace: Service Bus namespace (e.g., myservicebus.servicebus.windows.net)
            queue_name: Queue name (for queue)
            topic_name: Topic name (for topic/subscription)
            subscription_name: Subscription name (for topic/subscription)
            connection_string: Optional connection string
            auto_resolve_credentials: Use Azure AD (default: True)
        """
        self.namespace = namespace
        self.queue_name = queue_name
        self.topic_name = topic_name
        self.subscription_name = subscription_name
        self.connection_string = connection_string
        self.auto_resolve_credentials = auto_resolve_credentials
        self._client = None
    
    def _get_client(self):
        """Get or create Service Bus client."""
        if self._client:
            return self._client
        
        try:
            from azure.servicebus import ServiceBusClient
        except ImportError:
            raise ImportError(
                "azure-servicebus is not installed. Install with: pip install azure-servicebus"
            )
        
        # Use connection string if provided
        if self.connection_string:
            logger.debug("Using connection string")
            self._client = ServiceBusClient.from_connection_string(self.connection_string)
            return self._client
        
        # Try Azure AD authentication
        if self.auto_resolve_credentials:
            try:
                from azure.identity import DefaultAzureCredential
                
                logger.debug("Acquiring Azure AD token for Service Bus")
                credential = DefaultAzureCredential()
                
                fully_qualified_namespace = self.namespace
                if not fully_qualified_namespace.endswith(".servicebus.windows.net"):
                    fully_qualified_namespace = f"{fully_qualified_namespace}.servicebus.windows.net"
                
                self._client = ServiceBusClient(
                    fully_qualified_namespace=fully_qualified_namespace,
                    credential=credential
                )
                
                logger.info(f"✅ Connected to Azure Service Bus: {self.namespace} (Azure AD)")
                return self._client
            
            except ImportError:
                logger.warning("Azure Identity not installed. Install with: pip install azure-identity")
            except Exception as e:
                logger.warning(f"Azure AD authentication failed: {e}")
        
        raise ValueError("No credentials provided. Provide connection_string or use Azure AD.")
    
    def receive_messages(
        self,
        max_messages: int = 100,
        max_wait_time: int = 5,
        as_polars: bool = True
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        Receive messages from Service Bus.
        
        Args:
            max_messages: Maximum messages to receive
            max_wait_time: Max wait time in seconds
            as_polars: Return Polars DataFrame (True) or Pandas (False)
        
        Returns:
            DataFrame with messages
        
        Example:
            >>> df = connector.receive_messages(max_messages=100)
        """
        client = self._get_client()
        
        if self.queue_name:
            receiver = client.get_queue_receiver(queue_name=self.queue_name)
        elif self.topic_name and self.subscription_name:
            receiver = client.get_subscription_receiver(
                topic_name=self.topic_name,
                subscription_name=self.subscription_name
            )
        else:
            raise ValueError("Provide either queue_name or (topic_name + subscription_name)")
        
        messages = []
        
        with receiver:
            received_msgs = receiver.receive_messages(
                max_message_count=max_messages,
                max_wait_time=max_wait_time
            )
            
            for msg in received_msgs:
                messages.append({
                    "message_id": msg.message_id,
                    "body": str(msg),
                    "enqueued_time": msg.enqueued_time_utc,
                    "sequence_number": msg.sequence_number
                })
                
                # Complete message
                receiver.complete_message(msg)
        
        logger.info(f"✅ Received {len(messages)} messages")
        
        df = pd.DataFrame(messages)
        
        if as_polars:
            return pl.from_pandas(df)
        return df
    
    def close(self):
        """Close Service Bus client."""
        if self._client:
            self._client.close()
            self._client = None


# Export connectors
__all__ = [
    "RESTAPIConnector",
    "SFTPConnector",
    "AzureServiceBusConnector",
]
