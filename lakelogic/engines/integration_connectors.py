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

import time
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import polars as pl
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
        retry_delay: int = 1,
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
            raise ImportError("requests is not installed. Install with: pip install requests")

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
                "client_secret": self.client_secret,
            },
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

    def _request_with_retry(self, method: str, url: str, **kwargs) -> Any:
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
                    delay = self.retry_delay * (2**attempt)
                    logger.warning(
                        f"Request failed (attempt {attempt + 1}/{self.retry_attempts}): {e}. Retrying in {delay}s..."
                    )
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
        as_polars: bool = True,
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
    SFTP connector built on AsyncSSH.

    Features:
    - SSH key and password authentication
    - Host-key VERIFICATION by default (see below)
    - File pattern matching
    - Incremental extraction by modification time

    Why AsyncSSH rather than paramiko: the paramiko implementation set
    ``AutoAddPolicy()``, which silently accepts ANY unknown host key. That disables
    host-key verification altogether and leaves a credentialed transfer from a
    partner system open to a machine-in-the-middle. AsyncSSH verifies against
    ``known_hosts`` by default, so the safe behaviour is the one you get without
    asking. Verification can be waived explicitly with ``known_hosts=None``, which is
    a visible decision in the contract rather than a hidden default.

    The API stays synchronous — callers and the contract-driven source path are
    sync — with the event loop confined to this class.

    Example:
        >>> connector = SFTPConnector(host="sftp.example.com", username="u",
        ...                           private_key_path="~/.ssh/id_ed25519")
        >>> df = connector.extract_files("/data/", "*.csv", "csv")
    """

    def __init__(
        self,
        host: str,
        port: int = 22,
        username: Optional[str] = None,
        password: Optional[str] = None,
        private_key_path: Optional[str] = None,
        known_hosts: Any = "",
    ):
        """
        Args:
            host: SFTP host
            port: SFTP port (default 22)
            username: Username
            password: Password (used when no private key is given)
            private_key_path: Path to a private key
            known_hosts: Passed to AsyncSSH. "" (default) means the user's
                ~/.ssh/known_hosts. Pass None to DISABLE host-key verification —
                accepted, but logged as a warning, because it re-opens the hole the
                paramiko version had.
        """
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.private_key_path = private_key_path
        self.known_hosts = known_hosts
        if known_hosts is None:
            logger.warning(
                f"SFTP host-key verification DISABLED for {host}. The server is not "
                "authenticated, so this connection can be intercepted. Set known_hosts "
                "to a path (or leave the default) to verify."
            )

    def _connect_kwargs(self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {
            "host": self.host,
            "port": self.port,
            "known_hosts": self.known_hosts,
        }
        if self.username:
            kwargs["username"] = self.username
        if self.private_key_path:
            kwargs["client_keys"] = [self.private_key_path]
        elif self.password:
            kwargs["password"] = self.password
        return kwargs

    async def _afetch(
        self,
        remote_path: str,
        file_pattern: str,
        dest_dir: str,
        modified_since: Optional[float] = None,
    ) -> List[str]:
        """Download every matching file; return the local paths.

        ``modified_since`` (epoch seconds) skips files whose mtime is not newer,
        so a polled drop-folder re-downloads only what has arrived since the last
        run. The mtime comes from the server's own stat, not from the local clock.
        """
        import fnmatch
        import os

        try:
            import asyncssh
        except ImportError:
            raise ImportError("asyncssh is not installed. Install with: pip install asyncssh")

        local_paths: List[str] = []
        async with asyncssh.connect(**self._connect_kwargs()) as conn:
            async with conn.start_sftp_client() as sftp:
                names = await sftp.listdir(remote_path)
                matching = sorted(n for n in names if n not in (".", "..") and fnmatch.fnmatch(n, file_pattern))

                skipped = 0
                for name in matching:
                    remote_file = f"{remote_path.rstrip('/')}/{name}"
                    if modified_since is not None:
                        attrs = await sftp.stat(remote_file)
                        mtime = getattr(attrs, "mtime", None)
                        if mtime is not None and mtime <= modified_since:
                            skipped += 1
                            continue
                    local_file = os.path.join(dest_dir, name)
                    await sftp.get(remote_file, local_file)
                    local_paths.append(local_file)

                # Say what was skipped: "0 new files" and "the pattern is wrong" look
                # identical otherwise, and one of them is a broken pipeline.
                logger.info(
                    f"{len(matching)} file(s) matched {file_pattern} in {remote_path}; "
                    f"downloaded {len(local_paths)}"
                    + (f", skipped {skipped} not modified since watermark" if skipped else "")
                )
        return local_paths

    def fetch_files(
        self,
        remote_path: str,
        file_pattern: str = "*",
        dest_dir: Optional[str] = None,
        modified_since: Optional[float] = None,
    ) -> List[str]:
        """Download matching files and return their local paths (sync wrapper)."""
        import asyncio
        import tempfile

        dest = dest_dir or tempfile.mkdtemp(prefix="lakelogic-sftp-")
        coro = lambda: self._afetch(remote_path, file_pattern, dest, modified_since)  # noqa: E731
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro())
        # Already inside a loop (notebook/async host): run in a worker thread so we
        # never call asyncio.run() on a running loop.
        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, coro()).result()

    def extract_files(
        self,
        remote_path: str,
        file_pattern: str = "*",
        file_format: str = "csv",
        as_polars: bool = True,
        modified_since: Optional[float] = None,
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """Extract matching files from the SFTP server into one DataFrame."""
        readers = {"csv": pd.read_csv, "json": pd.read_json, "parquet": pd.read_parquet}
        if file_format not in readers:
            raise ValueError(f"Unsupported file format: {file_format}. Supported: {sorted(readers)}")

        local_paths = self.fetch_files(remote_path, file_pattern, modified_since=modified_since)
        if not local_paths:
            # Empty is a legitimate outcome for a polled drop-folder, but it must be
            # visible rather than silently yielding an empty frame.
            logger.warning(f"No files matched {file_pattern} in {remote_path}; returning an empty frame.")
            empty = pd.DataFrame()
            return pl.from_pandas(empty) if as_polars else empty

        frames = []
        for path in local_paths:
            df = readers[file_format](path)
            logger.info(f"Extracted {len(df)} records from {path}")
            frames.append(df)

        combined = pd.concat(frames, ignore_index=True)
        logger.info(f"Total records extracted: {len(combined)}")
        return pl.from_pandas(combined) if as_polars else combined

    async def _aput(self, local_paths: List[str], remote_dir: str, atomic: bool = True) -> List[str]:
        """Upload files; return the final remote paths.

        Atomic by default: each file goes up as ``<name>.tmp`` and is renamed into
        place once the transfer completes. A partner polling the drop-folder would
        otherwise read a half-written file and process a truncated batch — the
        failure is silent on their side and looks like missing data on ours.
        """
        import os

        try:
            import asyncssh
        except ImportError:
            raise ImportError("asyncssh is not installed. Install with: pip install asyncssh")

        remote_paths: List[str] = []
        async with asyncssh.connect(**self._connect_kwargs()) as conn:
            async with conn.start_sftp_client() as sftp:
                for local in local_paths:
                    name = os.path.basename(local)
                    final = f"{remote_dir.rstrip('/')}/{name}"
                    staged = f"{final}.tmp" if atomic else final
                    await sftp.put(local, staged)
                    if atomic:
                        # Overwrite a leftover from a previous failed run rather than
                        # letting the rename fail on an existing name.
                        try:
                            await sftp.remove(final)
                        except Exception:
                            pass
                        await sftp.rename(staged, final)
                    remote_paths.append(final)
                    logger.info(f"Uploaded {name} -> {final}" + (" (atomic rename)" if atomic else ""))
        return remote_paths

    def put_files(self, local_paths: List[str], remote_dir: str, atomic: bool = True) -> List[str]:
        """Upload local files to *remote_dir* (sync wrapper). Returns remote paths."""
        import asyncio

        coro = lambda: self._aput(local_paths, remote_dir, atomic)  # noqa: E731
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro())

        import concurrent.futures

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            return pool.submit(asyncio.run, coro()).result()

    def close(self):
        """No persistent connection is held — each transfer opens and closes its own."""
        return None


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
        auto_resolve_credentials: bool = True,
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
            raise ImportError("azure-servicebus is not installed. Install with: pip install azure-servicebus")

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
                    credential=credential,
                )

                logger.info(f"✅ Connected to Azure Service Bus: {self.namespace} (Azure AD)")
                return self._client

            except ImportError:
                logger.warning("Azure Identity not installed. Install with: pip install azure-identity")
            except Exception as e:
                logger.warning(f"Azure AD authentication failed: {e}")

        raise ValueError("No credentials provided. Provide connection_string or use Azure AD.")

    def receive_messages(
        self, max_messages: int = 100, max_wait_time: int = 5, as_polars: bool = True
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
                topic_name=self.topic_name, subscription_name=self.subscription_name
            )
        else:
            raise ValueError("Provide either queue_name or (topic_name + subscription_name)")

        messages = []

        with receiver:
            received_msgs = receiver.receive_messages(max_message_count=max_messages, max_wait_time=max_wait_time)

            for msg in received_msgs:
                messages.append(
                    {
                        "message_id": msg.message_id,
                        "body": str(msg),
                        "enqueued_time": msg.enqueued_time_utc,
                        "sequence_number": msg.sequence_number,
                    }
                )

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
