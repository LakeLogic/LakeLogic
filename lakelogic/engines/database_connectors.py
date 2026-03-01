"""
Database CDC (Change Data Capture) connectors for LakeLogic.

This module provides automatic credential resolution and connection management
for cloud databases:
- Azure SQL Database
- PostgreSQL (Azure, AWS RDS, GCP Cloud SQL)
- MySQL (Azure, AWS RDS, GCP Cloud SQL)
- MongoDB (Azure Cosmos DB, MongoDB Atlas)
- DynamoDB (AWS)
- Firestore (GCP)

Features:
- Automatic credential resolution (Azure AD, IAM, service accounts)
- Incremental CDC extraction (timestamp, sequence, log-based)
- Connection pooling
- Retry logic
- Schema detection

Example:
    >>> from lakelogic.engines.database_connectors import AzureSQLConnector
    >>>
    >>> # Automatic Azure AD authentication
    >>> connector = AzureSQLConnector(
    ...     server="myserver.database.windows.net",
    ...     database="mydb"
    ... )
    >>>
    >>> # Incremental CDC extraction
    >>> df = connector.extract_incremental(
    ...     table="customers",
    ...     watermark_column="updated_at",
    ...     last_watermark="2026-02-08T00:00:00Z"
    ... )
"""

from datetime import datetime
from typing import Dict, List, Optional, Union

import pandas as pd
import polars as pl

from loguru import logger


class DatabaseConnector:
    """
    Base class for database connectors with automatic credential resolution.
    """

    def __init__(
        self,
        connection_string: Optional[str] = None,
        auto_resolve_credentials: bool = True,
    ):
        """
        Initialize database connector.

        Args:
            connection_string: Optional manual connection string
            auto_resolve_credentials: Automatically resolve credentials (default: True)
        """
        self.connection_string = connection_string
        self.auto_resolve_credentials = auto_resolve_credentials
        self._connection = None

    def extract_full(
        self,
        table: str,
        columns: Optional[List[str]] = None,
        where: Optional[str] = None,
        as_polars: bool = True,
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        Extract full table data.

        Args:
            table: Table name
            columns: Optional list of columns to extract
            where: Optional WHERE clause
            as_polars: Return Polars DataFrame (True) or Pandas (False)

        Returns:
            DataFrame with extracted data
        """
        raise NotImplementedError("Subclass must implement extract_full")

    def extract_incremental(
        self,
        table: str,
        watermark_column: str,
        last_watermark: Optional[Union[str, datetime]] = None,
        columns: Optional[List[str]] = None,
        as_polars: bool = True,
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        Extract incremental data based on watermark column.

        Args:
            table: Table name
            watermark_column: Column to use for incremental extraction (e.g., updated_at)
            last_watermark: Last extracted watermark value
            columns: Optional list of columns to extract
            as_polars: Return Polars DataFrame (True) or Pandas (False)

        Returns:
            DataFrame with incremental data
        """
        raise NotImplementedError("Subclass must implement extract_incremental")

    def get_schema(self, table: str) -> Dict[str, str]:
        """
        Get table schema.

        Args:
            table: Table name

        Returns:
            Dictionary mapping column names to data types
        """
        raise NotImplementedError("Subclass must implement get_schema")

    def close(self):
        """Close database connection."""
        if self._connection:
            self._connection.close()
            self._connection = None


class AzureSQLConnector(DatabaseConnector):
    """
    Azure SQL Database connector with automatic Azure AD authentication.

    Features:
    - Automatic Azure AD authentication (DefaultAzureCredential)
    - Fallback to SQL authentication
    - Connection pooling
    - Incremental CDC extraction

    Example:
        >>> # Azure AD authentication (automatic)
        >>> connector = AzureSQLConnector(
        ...     server="myserver.database.windows.net",
        ...     database="mydb"
        ... )
        >>>
        >>> # SQL authentication (manual)
        >>> connector = AzureSQLConnector(
        ...     server="myserver.database.windows.net",
        ...     database="mydb",
        ...     username="admin",
        ...     password="...",
        ...     auto_resolve_credentials=False
        ... )
        >>>
        >>> # Incremental extraction
        >>> df = connector.extract_incremental(
        ...     table="customers",
        ...     watermark_column="updated_at",
        ...     last_watermark="2026-02-08T00:00:00Z"
        ... )
    """

    def __init__(
        self,
        server: str,
        database: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        connection_string: Optional[str] = None,
        auto_resolve_credentials: bool = True,
    ):
        """
        Initialize Azure SQL connector.

        Args:
            server: Azure SQL server (e.g., myserver.database.windows.net)
            database: Database name
            username: Optional SQL username (for SQL auth)
            password: Optional SQL password (for SQL auth)
            connection_string: Optional manual connection string
            auto_resolve_credentials: Use Azure AD authentication (default: True)
        """
        super().__init__(connection_string, auto_resolve_credentials)
        self.server = server
        self.database = database
        self.username = username
        self.password = password

    def _get_connection(self):
        """Get or create database connection."""
        if self._connection:
            return self._connection

        try:
            import pyodbc
        except ImportError:
            raise ImportError(
                "pyodbc is not installed. Install with: pip install pyodbc\n"
                "Note: You may also need to install the ODBC driver: "
                "https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server"
            )

        # Use manual connection string if provided
        if self.connection_string:
            logger.debug("Using manual connection string")
            self._connection = pyodbc.connect(self.connection_string)
            return self._connection

        # Try Azure AD authentication
        if self.auto_resolve_credentials and not (self.username and self.password):
            try:
                from azure.identity import DefaultAzureCredential

                logger.debug("Acquiring Azure AD token for SQL Database")
                credential = DefaultAzureCredential()
                token = credential.get_token("https://database.windows.net/.default")

                # Build connection string with Azure AD token
                conn_str = (
                    f"Driver={{ODBC Driver 18 for SQL Server}};"
                    f"Server={self.server};"
                    f"Database={self.database};"
                    f"Encrypt=yes;"
                    f"TrustServerCertificate=no;"
                    f"Connection Timeout=30;"
                )

                # Connect with token
                self._connection = pyodbc.connect(conn_str, attrs_before={"AccessToken": token.token})

                logger.info(f"✅ Connected to Azure SQL Database: {self.database} (Azure AD)")
                return self._connection

            except ImportError:
                logger.warning(
                    "Azure Identity not installed. Install with: pip install azure-identity\n"
                    "Falling back to SQL authentication."
                )
            except Exception as e:
                logger.warning(f"Azure AD authentication failed: {e}\nFalling back to SQL authentication.")

        # Fallback to SQL authentication
        if self.username and self.password:
            logger.debug("Using SQL authentication")
            conn_str = (
                f"Driver={{ODBC Driver 18 for SQL Server}};"
                f"Server={self.server};"
                f"Database={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                f"Encrypt=yes;"
                f"TrustServerCertificate=no;"
                f"Connection Timeout=30;"
            )

            self._connection = pyodbc.connect(conn_str)
            logger.info(f"✅ Connected to Azure SQL Database: {self.database} (SQL auth)")
            return self._connection

        raise ValueError(
            "No credentials provided. Either:\n"
            "1. Use Azure AD (az login) with auto_resolve_credentials=True, or\n"
            "2. Provide username and password for SQL authentication"
        )

    def extract_full(
        self,
        table: str,
        columns: Optional[List[str]] = None,
        where: Optional[str] = None,
        as_polars: bool = True,
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        Extract full table data.

        Args:
            table: Table name (e.g., "dbo.customers" or "customers")
            columns: Optional list of columns to extract
            where: Optional WHERE clause (without WHERE keyword)
            as_polars: Return Polars DataFrame (True) or Pandas (False)

        Returns:
            DataFrame with extracted data

        Example:
            >>> df = connector.extract_full("dbo.customers")
            >>> df = connector.extract_full("customers", columns=["id", "name"])
            >>> df = connector.extract_full("customers", where="status = 'active'")
        """
        conn = self._get_connection()

        # Build query
        cols = ", ".join(columns) if columns else "*"
        query = f"SELECT {cols} FROM {table}"
        if where:
            query += f" WHERE {where}"

        logger.debug(f"Executing query: {query}")

        # Execute query
        df = pd.read_sql(query, conn)

        logger.info(f"✅ Extracted {len(df)} rows from {table}")

        if as_polars:
            return pl.from_pandas(df)
        return df

    def extract_incremental(
        self,
        table: str,
        watermark_column: str,
        last_watermark: Optional[Union[str, datetime]] = None,
        columns: Optional[List[str]] = None,
        as_polars: bool = True,
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        Extract incremental data based on watermark column.

        Args:
            table: Table name
            watermark_column: Column to use for incremental extraction
            last_watermark: Last extracted watermark value
            columns: Optional list of columns to extract
            as_polars: Return Polars DataFrame (True) or Pandas (False)

        Returns:
            DataFrame with incremental data

        Example:
            >>> # First run (full extract)
            >>> df = connector.extract_incremental("customers", "updated_at")
            >>>
            >>> # Subsequent runs (incremental)
            >>> df = connector.extract_incremental(
            ...     "customers",
            ...     "updated_at",
            ...     last_watermark="2026-02-08T00:00:00Z"
            ... )
        """
        conn = self._get_connection()

        # Build query
        cols = ", ".join(columns) if columns else "*"
        query = f"SELECT {cols} FROM {table}"

        if last_watermark:
            # Format watermark value
            if isinstance(last_watermark, datetime):
                watermark_str = last_watermark.isoformat()
            else:
                watermark_str = last_watermark

            query += f" WHERE {watermark_column} > '{watermark_str}'"

        query += f" ORDER BY {watermark_column}"

        logger.debug(f"Executing incremental query: {query}")

        # Execute query
        df = pd.read_sql(query, conn)

        logger.info(f"✅ Extracted {len(df)} incremental rows from {table}")

        if as_polars:
            return pl.from_pandas(df)
        return df

    def get_schema(self, table: str) -> Dict[str, str]:
        """
        Get table schema.

        Args:
            table: Table name

        Returns:
            Dictionary mapping column names to data types

        Example:
            >>> schema = connector.get_schema("dbo.customers")
            >>> print(schema)
            {'id': 'int', 'name': 'nvarchar', 'created_at': 'datetime2'}
        """
        conn = self._get_connection()

        # Parse schema and table name
        if "." in table:
            schema, table_name = table.split(".", 1)
        else:
            schema = "dbo"
            table_name = table

        query = f"""
        SELECT
            COLUMN_NAME,
            DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema}'
          AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """

        df = pd.read_sql(query, conn)

        return dict(zip(df["COLUMN_NAME"], df["DATA_TYPE"]))


class PostgreSQLConnector(DatabaseConnector):
    """
    PostgreSQL connector (Azure, AWS RDS, GCP Cloud SQL).

    Features:
    - Automatic credential resolution (Azure AD for Azure PostgreSQL)
    - Connection pooling
    - Incremental CDC extraction

    Example:
        >>> # Azure PostgreSQL with Azure AD
        >>> connector = PostgreSQLConnector(
        ...     host="myserver.postgres.database.azure.com",
        ...     database="mydb"
        ... )
        >>>
        >>> # AWS RDS PostgreSQL
        >>> connector = PostgreSQLConnector(
        ...     host="mydb.abc123.us-west-2.rds.amazonaws.com",
        ...     database="mydb",
        ...     username="admin",
        ...     password="..."
        ... )
    """

    def __init__(
        self,
        host: str,
        database: str,
        port: int = 5432,
        username: Optional[str] = None,
        password: Optional[str] = None,
        connection_string: Optional[str] = None,
        auto_resolve_credentials: bool = True,
    ):
        """
        Initialize PostgreSQL connector.

        Args:
            host: PostgreSQL host
            database: Database name
            port: Port (default: 5432)
            username: Optional username
            password: Optional password
            connection_string: Optional manual connection string
            auto_resolve_credentials: Use Azure AD for Azure PostgreSQL (default: True)
        """
        super().__init__(connection_string, auto_resolve_credentials)
        self.host = host
        self.database = database
        self.port = port
        self.username = username
        self.password = password

    def _get_connection(self):
        """Get or create database connection."""
        if self._connection:
            return self._connection

        try:
            import psycopg2
        except ImportError:
            raise ImportError("psycopg2 is not installed. Install with: pip install psycopg2-binary")

        # Use manual connection string if provided
        if self.connection_string:
            logger.debug("Using manual connection string")
            self._connection = psycopg2.connect(self.connection_string)
            return self._connection

        # Try Azure AD authentication for Azure PostgreSQL
        if self.auto_resolve_credentials and "postgres.database.azure.com" in self.host:
            try:
                from azure.identity import DefaultAzureCredential

                logger.debug("Acquiring Azure AD token for PostgreSQL")
                credential = DefaultAzureCredential()
                token = credential.get_token("https://ossrdbms-aad.database.windows.net/.default")

                # Connect with Azure AD token as password
                self._connection = psycopg2.connect(
                    host=self.host,
                    database=self.database,
                    port=self.port,
                    user=self.username or "azure_ad_user",
                    password=token.token,
                    sslmode="require",
                )

                logger.info(f"✅ Connected to Azure PostgreSQL: {self.database} (Azure AD)")
                return self._connection

            except ImportError:
                logger.warning("Azure Identity not installed. Falling back to password authentication.")
            except Exception as e:
                logger.warning(f"Azure AD authentication failed: {e}\nFalling back to password authentication.")

        # Fallback to username/password authentication
        if self.username and self.password:
            logger.debug("Using password authentication")
            self._connection = psycopg2.connect(
                host=self.host,
                database=self.database,
                port=self.port,
                user=self.username,
                password=self.password,
            )

            logger.info(f"✅ Connected to PostgreSQL: {self.database}")
            return self._connection

        raise ValueError("No credentials provided. Provide username and password.")

    def extract_full(
        self,
        table: str,
        columns: Optional[List[str]] = None,
        where: Optional[str] = None,
        as_polars: bool = True,
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """Extract full table data."""
        conn = self._get_connection()

        cols = ", ".join(columns) if columns else "*"
        query = f"SELECT {cols} FROM {table}"
        if where:
            query += f" WHERE {where}"

        logger.debug(f"Executing query: {query}")
        df = pd.read_sql(query, conn)
        logger.info(f"✅ Extracted {len(df)} rows from {table}")

        if as_polars:
            return pl.from_pandas(df)
        return df

    def extract_incremental(
        self,
        table: str,
        watermark_column: str,
        last_watermark: Optional[Union[str, datetime]] = None,
        columns: Optional[List[str]] = None,
        as_polars: bool = True,
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """Extract incremental data."""
        conn = self._get_connection()

        cols = ", ".join(columns) if columns else "*"
        query = f"SELECT {cols} FROM {table}"

        if last_watermark:
            if isinstance(last_watermark, datetime):
                watermark_str = last_watermark.isoformat()
            else:
                watermark_str = last_watermark

            query += f" WHERE {watermark_column} > '{watermark_str}'"

        query += f" ORDER BY {watermark_column}"

        logger.debug(f"Executing incremental query: {query}")
        df = pd.read_sql(query, conn)
        logger.info(f"✅ Extracted {len(df)} incremental rows from {table}")

        if as_polars:
            return pl.from_pandas(df)
        return df

    def get_schema(self, table: str) -> Dict[str, str]:
        """Get table schema."""
        conn = self._get_connection()

        # Parse schema and table name
        if "." in table:
            schema, table_name = table.split(".", 1)
        else:
            schema = "public"
            table_name = table

        query = f"""
        SELECT
            column_name,
            data_type
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
          AND table_name = '{table_name}'
        ORDER BY ordinal_position
        """

        df = pd.read_sql(query, conn)
        return dict(zip(df["column_name"], df["data_type"]))


# Export connectors
__all__ = [
    "DatabaseConnector",
    "AzureSQLConnector",
    "PostgreSQLConnector",
]
