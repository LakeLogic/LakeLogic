"""
Catalog integration for LakeLogic (Unity Catalog, Fabric LakeDB, Synapse).

This module provides utilities to resolve catalog table names to their underlying
Delta Lake storage paths, enabling Spark-free access using Delta-RS.

Supported Platforms:
- **Unity Catalog** (Databricks): catalog.schema.table
- **Fabric LakeDB** (Microsoft): workspace.lakehouse.table
- **Synapse Analytics** (Azure): database.schema.table

Features:
- Resolve table names to storage paths
- Support for multiple cloud platforms
- Cache table metadata for performance
- Works with Delta-RS for Spark-free operations

Example:
    >>> from lakelogic.engines.unity_catalog import resolve_catalog_path
    >>>
    >>> # Unity Catalog (Databricks)
    >>> path = resolve_catalog_path("main.default.customers")
    >>>
    >>> # Fabric LakeDB (Microsoft)
    >>> path = resolve_catalog_path("myworkspace.sales_lakehouse.customers")
    >>>
    >>> # Synapse Analytics (Azure)
    >>> path = resolve_catalog_path("salesdb.dbo.customers")
    >>>
    >>> # Resolve table name to path
    >>> path = resolver.resolve_table("main.default.customers")
    >>> print(path)
    >>> # s3://bucket/unity-catalog/main/default/customers/
    >>>
    >>> # Use with Delta-RS
    >>> from lakelogic.engines.delta_adapter import DeltaAdapter
    >>> adapter = DeltaAdapter()
    >>> df = adapter.read(path)
"""

import os
from typing import Dict, Optional

from loguru import logger


class UnityCatalogResolver:
    """
    Resolve Unity Catalog table names to Delta Lake storage paths.

    Enables Spark-free access to Unity Catalog tables by resolving
    3-part table names (catalog.schema.table) to their underlying
    storage locations.
    """

    def __init__(
        self,
        host: Optional[str] = None,
        token: Optional[str] = None,
        use_databricks_sdk: bool = True,
    ):
        """
        Initialize Unity Catalog resolver.

        Args:
            host: Databricks workspace URL (e.g., "https://your-workspace.cloud.databricks.com")
                  If None, reads from DATABRICKS_HOST env var
            token: Databricks personal access token
                   If None, reads from DATABRICKS_TOKEN env var
            use_databricks_sdk: Use Databricks SDK for resolution (True) or manual API calls (False)

        Example:
            >>> resolver = UnityCatalogResolver(
            ...     host="https://your-workspace.cloud.databricks.com",
            ...     token="YOUR_TOKEN"
            ... )
        """
        self.host = host or os.getenv("DATABRICKS_HOST")
        self.token = token or os.getenv("DATABRICKS_TOKEN")
        self.use_databricks_sdk = use_databricks_sdk
        self._cache: Dict[str, str] = {}

        # Check if credentials are available
        if not self.host or not self.token:
            logger.warning(
                "Unity Catalog credentials not found. Set DATABRICKS_HOST and DATABRICKS_TOKEN "
                "environment variables or pass them to UnityCatalogResolver."
            )

    def is_unity_catalog_table(self, path: str) -> bool:
        """
        Check if path is a Unity Catalog table name (catalog.schema.table).

        Args:
            path: Path or table name to check

        Returns:
            True if path is a Unity Catalog table name, False otherwise

        Example:
            >>> resolver.is_unity_catalog_table("main.default.customers")
            True
            >>> resolver.is_unity_catalog_table("s3://bucket/table/")
            False
        """
        # Unity Catalog table names have exactly 3 parts separated by dots
        # and don't contain path separators or URI schemes
        if "/" in path or "\\" in path or "://" in path:
            return False

        parts = path.split(".")
        return len(parts) == 3 and all(part.strip() for part in parts)

    def resolve_table(self, table_name: str) -> str:
        """
        Resolve Unity Catalog table name to Delta Lake storage path.

        Args:
            table_name: Unity Catalog table name (catalog.schema.table)

        Returns:
            Delta Lake storage path (e.g., s3://bucket/unity-catalog/catalog/schema/table/)

        Raises:
            ValueError: If table name is invalid or credentials are missing
            ImportError: If Databricks SDK is not installed

        Example:
            >>> path = resolver.resolve_table("main.default.customers")
            >>> print(path)
            s3://bucket/unity-catalog/main/default/customers/
        """
        # Check cache first
        if table_name in self._cache:
            logger.debug(f"Using cached path for {table_name}")
            return self._cache[table_name]

        # Validate table name
        if not self.is_unity_catalog_table(table_name):
            raise ValueError(f"Invalid Unity Catalog table name: {table_name}. Expected format: catalog.schema.table")

        # Check credentials
        if not self.host or not self.token:
            raise ValueError(
                "Unity Catalog credentials not configured. Set DATABRICKS_HOST and DATABRICKS_TOKEN "
                "environment variables or pass them to UnityCatalogResolver."
            )

        # Resolve using Databricks SDK or REST API
        if self.use_databricks_sdk:
            path = self._resolve_with_sdk(table_name)
        else:
            path = self._resolve_with_api(table_name)

        # Cache the result
        self._cache[table_name] = path
        logger.info(f"Resolved Unity Catalog table {table_name} -> {path}")

        return path

    def _resolve_with_sdk(self, table_name: str) -> str:
        """
        Resolve table using Databricks SDK.

        Args:
            table_name: Unity Catalog table name

        Returns:
            Delta Lake storage path

        Raises:
            ImportError: If Databricks SDK is not installed
        """
        try:
            from databricks.sdk import WorkspaceClient
        except ImportError:
            raise ImportError("Databricks SDK is not installed. Install with: pip install databricks-sdk")

        # Create workspace client
        w = WorkspaceClient(host=self.host, token=self.token)

        # Get table metadata
        try:
            table = w.tables.get(full_name=table_name)
            storage_location = table.storage_location

            if not storage_location:
                raise ValueError(
                    f"Table {table_name} does not have a storage location. It may be a managed table or view."
                )

            return storage_location

        except Exception as e:
            raise ValueError(f"Failed to resolve Unity Catalog table {table_name}: {e}")

    def _resolve_with_api(self, table_name: str) -> str:
        """
        Resolve table using Unity Catalog REST API.

        Args:
            table_name: Unity Catalog table name

        Returns:
            Delta Lake storage path
        """
        import requests

        # Parse table name
        catalog, schema, table = table_name.split(".")

        # Call Unity Catalog API
        url = f"{self.host}/api/2.1/unity-catalog/tables/{catalog}/{schema}/{table}"
        headers = {"Authorization": f"Bearer {self.token}"}

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            data = response.json()
            storage_location = data.get("storage_location")

            if not storage_location:
                raise ValueError(
                    f"Table {table_name} does not have a storage location. It may be a managed table or view."
                )

            return storage_location

        except requests.exceptions.RequestException as e:
            raise ValueError(f"Failed to resolve Unity Catalog table {table_name}: {e}")

    def clear_cache(self):
        """Clear the table path cache."""
        self._cache.clear()
        logger.debug("Cleared Unity Catalog table path cache")


# Global resolver instance (lazy initialization)
_global_resolver: Optional[UnityCatalogResolver] = None


def get_unity_catalog_resolver() -> UnityCatalogResolver:
    """
    Get or create the global Unity Catalog resolver.

    Returns:
        Global UnityCatalogResolver instance

    Example:
        >>> from lakelogic.engines.unity_catalog import get_unity_catalog_resolver
        >>> resolver = get_unity_catalog_resolver()
        >>> path = resolver.resolve_table("main.default.customers")
    """
    global _global_resolver
    if _global_resolver is None:
        _global_resolver = UnityCatalogResolver()
    return _global_resolver


def resolve_unity_catalog_path(path: str) -> str:
    """
    Resolve Unity Catalog table name to storage path, or return path unchanged.

    This is a convenience function that automatically detects Unity Catalog
    table names and resolves them, while passing through regular paths unchanged.

    Args:
        path: Unity Catalog table name (catalog.schema.table) or regular path

    Returns:
        Delta Lake storage path if Unity Catalog table, otherwise original path

    Example:
        >>> # Unity Catalog table name -> resolved to path
        >>> path = resolve_unity_catalog_path("main.default.customers")
        >>> print(path)
        s3://bucket/unity-catalog/main/default/customers/
        >>>
        >>> # Regular path -> unchanged
        >>> path = resolve_unity_catalog_path("s3://bucket/table/")
        >>> print(path)
        s3://bucket/table/
    """
    resolver = get_unity_catalog_resolver()

    # Check if it's a Unity Catalog table name
    if resolver.is_unity_catalog_table(path):
        try:
            return resolver.resolve_table(path)
        except Exception as e:
            logger.warning(f"Failed to resolve Unity Catalog table {path}: {e}")
            logger.warning("Treating as regular path")
            return path

    # Not a Unity Catalog table, return unchanged
    return path


def resolve_catalog_path(path: str, platform: Optional[str] = None) -> str:
    """
    Resolve catalog table name to storage path (Unity Catalog, Fabric, Synapse).

    Automatically detects the platform based on table name format or uses
    the specified platform. Supports:
    - Unity Catalog (Databricks): catalog.schema.table
    - Fabric LakeDB (Microsoft): workspace.lakehouse.table
    - Synapse Analytics (Azure): database.schema.table

    Args:
        path: Table name (3-part: catalog.schema.table) or regular path
        platform: Optional platform hint ("unity", "fabric", "synapse")

    Returns:
        Delta Lake storage path if catalog table, otherwise original path

    Example:
        >>> # Unity Catalog (auto-detected)
        >>> path = resolve_catalog_path("main.default.customers")
        >>>
        >>> # Fabric LakeDB (auto-detected or explicit)
        >>> path = resolve_catalog_path("myworkspace.sales_lakehouse.customers")
        >>> path = resolve_catalog_path("myworkspace.sales_lakehouse.customers", platform="fabric")
        >>>
        >>> # Synapse Analytics (explicit)
        >>> path = resolve_catalog_path("salesdb.dbo.customers", platform="synapse")
        >>>
        >>> # Regular path (unchanged)
        >>> path = resolve_catalog_path("s3://bucket/table/")
    """
    # Check if it's a path (not a table name)
    if "/" in path or "\\" in path or "://" in path:
        return path

    # Check if it's a 3-part table name
    parts = path.split(".")
    if len(parts) != 3:
        return path

    # Determine platform
    if platform == "unity" or (platform is None and os.getenv("DATABRICKS_HOST")):
        # Unity Catalog (Databricks)
        return resolve_unity_catalog_path(path)

    elif platform == "fabric":
        # Fabric LakeDB (Microsoft)
        return _resolve_fabric_table(path)

    elif platform == "synapse":
        # Synapse Analytics (Azure)
        return _resolve_synapse_table(path)

    else:
        # Try Unity Catalog first (most common)
        try:
            return resolve_unity_catalog_path(path)
        except Exception:
            # Fall back to treating as regular path
            logger.debug(f"Could not resolve {path} as Unity Catalog table, treating as regular path")
            return path


def _resolve_fabric_table(table_name: str) -> str:
    """
    Resolve Fabric LakeDB table name to OneLake storage path.

    Args:
        table_name: Fabric table name (workspace.lakehouse.table)

    Returns:
        OneLake storage path

    Example:
        >>> path = _resolve_fabric_table("myworkspace.sales_lakehouse.customers")
        >>> print(path)
        abfss://myworkspace@onelake.dfs.fabric.microsoft.com/sales_lakehouse.Lakehouse/Tables/customers/
    """
    workspace, lakehouse, table = table_name.split(".")

    # Fabric OneLake path format
    # abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Tables/<table>/
    path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/{lakehouse}.Lakehouse/Tables/{table}/"

    logger.info(f"Resolved Fabric LakeDB table {table_name} -> {path}")
    return path


def _resolve_synapse_table(table_name: str) -> str:
    """
    Resolve Synapse Analytics table name to ADLS storage path.

    Note: This requires SYNAPSE_STORAGE_ACCOUNT environment variable to be set.

    Args:
        table_name: Synapse table name (database.schema.table)

    Returns:
        ADLS storage path

    Example:
        >>> os.environ["SYNAPSE_STORAGE_ACCOUNT"] = "mysynapsestorage"
        >>> path = _resolve_synapse_table("salesdb.dbo.customers")
        >>> print(path)
        abfss://salesdb@mysynapsestorage.dfs.core.windows.net/dbo/customers/
    """
    database, schema, table = table_name.split(".")

    # Get storage account from environment
    storage_account = os.getenv("SYNAPSE_STORAGE_ACCOUNT")
    if not storage_account:
        logger.warning(
            "SYNAPSE_STORAGE_ACCOUNT environment variable not set. "
            "Cannot resolve Synapse table name. Treating as regular path."
        )
        return table_name

    # Synapse ADLS path format
    # abfss://<database>@<storage_account>.dfs.core.windows.net/<schema>/<table>/
    path = f"abfss://{database}@{storage_account}.dfs.core.windows.net/{schema}/{table}/"

    logger.info(f"Resolved Synapse Analytics table {table_name} -> {path}")
    return path
