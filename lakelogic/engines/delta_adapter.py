"""
Delta Lake adapter using Delta-RS for Spark-free Delta operations.

This module enables Delta Lake read/write/merge operations for non-Spark engines
(Polars, DuckDB, Pandas) without requiring JVM or Spark installation.

Features:
- Read Delta tables without Spark
- Write Delta tables without Spark
- Atomic MERGE operations (upsert) without Spark
- Time travel support
- Vacuum operations
- Works with Unity Catalog, Fabric LakeDB, Synapse, AWS, Azure, GCP

Example:
    >>> from lakelogic.engines.delta_adapter import DeltaAdapter
    >>> adapter = DeltaAdapter()
    >>>
    >>> # Read Delta table
    >>> df = adapter.read("s3://bucket/unity-catalog/table/")
    >>>
    >>> # Write Delta table
    >>> adapter.write(df, "s3://bucket/output/", mode="append")
    >>>
    >>> # MERGE (upsert)
    >>> adapter.merge(
    ...     target_path="s3://bucket/table/",
    ...     source_df=new_data,
    ...     merge_key="id"
    ... )
"""

from typing import Any, Dict, List, Optional, Union

import pandas as pd
import polars as pl


class DeltaAdapter:
    """
    Spark-free Delta Lake adapter using Delta-RS.

    Enables Delta Lake operations for Polars, DuckDB, and Pandas without
    requiring Spark or JVM installation.
    """

    def __init__(
        self,
        storage_options: Optional[Dict[str, str]] = None,
        auto_resolve_credentials: bool = True,
    ):
        """
        Initialize Delta adapter.

        Args:
            storage_options: Cloud storage credentials (AWS, Azure, GCP).
                If not provided and auto_resolve_credentials=True, credentials will be
                automatically resolved from environment variables, Azure AD, AWS IAM, or GCP ADC.
                Examples:
                - AWS: {"AWS_REGION": "us-west-2", "AWS_ACCESS_KEY_ID": "...", "AWS_SECRET_ACCESS_KEY": "..."}
                - Azure: {"AZURE_STORAGE_ACCOUNT_NAME": "...", "AZURE_STORAGE_ACCOUNT_KEY": "..."}
                - GCP: {"GOOGLE_SERVICE_ACCOUNT": "..."}
            auto_resolve_credentials: Automatically resolve cloud credentials (default: True).
                Set to False to disable automatic credential resolution.
        """
        self.storage_options = storage_options or {}
        self.auto_resolve_credentials = auto_resolve_credentials

        # Check if deltalake is installed
        try:
            import importlib.util

            self._delta_available = importlib.util.find_spec("deltalake") is not None
        except Exception:
            self._delta_available = False

    def _ensure_delta_available(self):
        """Raise error if Delta-RS is not installed."""
        if not self._delta_available:
            raise ImportError(
                "Delta-RS is not installed. Install with: pip install 'lakelogic[delta]' or pip install deltalake"
            )

    def read(
        self,
        path: str,
        version: Optional[int] = None,
        timestamp: Optional[str] = None,
        columns: Optional[List[str]] = None,
        as_polars: bool = True,
    ) -> Union[pl.DataFrame, pd.DataFrame]:
        """
        Read Delta table without Spark.

        Args:
            path: Path to Delta table (local, s3://, abfss://, gs://) or Unity Catalog table name (catalog.schema.table)
            version: Optional version number for time travel
            timestamp: Optional timestamp for time travel (ISO 8601)
            columns: Optional list of columns to read
            as_polars: Return Polars DataFrame (True) or Pandas (False)

        Returns:
            Polars or Pandas DataFrame

        Example:
            >>> adapter = DeltaAdapter()
            >>>
            >>> # Unity Catalog table name
            >>> df = adapter.read("main.default.customers")
            >>>
            >>> # Regular path
            >>> df = adapter.read("s3://bucket/table/")
            >>>
            >>> # Time travel
            >>> df_v1 = adapter.read("main.default.customers", version=1)
            >>> df_yesterday = adapter.read("s3://bucket/table/", timestamp="2026-02-08T00:00:00Z")
        """
        self._ensure_delta_available()
        from deltalake import DeltaTable

        from lakelogic.engines.catalog_resolver import resolve_catalog_path
        from lakelogic.engines.cloud_credentials import resolve_storage_options

        # Resolve catalog table names (Unity Catalog, Fabric, Synapse)
        resolved_path = resolve_catalog_path(path)

        # Automatically resolve cloud credentials if enabled
        storage_options = self.storage_options
        if self.auto_resolve_credentials:
            storage_options = resolve_storage_options(resolved_path, storage_options)

        # Read Delta table
        dt = DeltaTable(resolved_path, storage_options=storage_options, version=version)

        # Time travel by timestamp
        if timestamp:
            dt.load_as_version(timestamp)

        # Convert to DataFrame
        if as_polars:
            import polars as pl

            df = pl.from_arrow(dt.to_pyarrow_table())
            if columns:
                df = df.select(columns)
            return df
        else:
            df = dt.to_pandas()
            if columns:
                df = df[columns]
            return df

    def write(
        self,
        df: Union[pl.DataFrame, pd.DataFrame],
        path: str,
        mode: str = "append",
        partition_by: Optional[List[str]] = None,
        schema_mode: str = "merge",
    ) -> None:
        """
        Write Delta table without Spark.

        Args:
            df: Polars or Pandas DataFrame to write
            path: Path to Delta table (local, s3://, abfss://, gs://)
            mode: Write mode ("append", "overwrite", "error", "ignore")
            partition_by: Optional list of columns to partition by
            schema_mode: Schema evolution mode ("merge", "overwrite")

        Example:
            >>> adapter = DeltaAdapter()
            >>> df = pl.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
            >>>
            >>> # Append
            >>> adapter.write(df, "s3://bucket/table/", mode="append")
            >>>
            >>> # Overwrite
            >>> adapter.write(df, "s3://bucket/table/", mode="overwrite")
            >>>
            >>> # Partitioned
            >>> adapter.write(df, "s3://bucket/table/", partition_by=["date"])
        """
        self._ensure_delta_available()
        from deltalake import write_deltalake

        from lakelogic.engines.cloud_credentials import resolve_storage_options

        # Convert Polars to Pandas if needed
        if isinstance(df, pl.DataFrame):
            df = df.to_pandas()

        # Automatically resolve cloud credentials if enabled
        storage_options = self.storage_options
        if self.auto_resolve_credentials:
            storage_options = resolve_storage_options(path, storage_options)

        # Write Delta table
        # The pyarrow engine does not support schema_mode='merge' in older deltalake versions; use rust engine if available.
        kwargs = {
            "mode": mode,
            "storage_options": storage_options,
            "partition_by": partition_by,
            "schema_mode": schema_mode,
        }
        
        import inspect
        sig = inspect.signature(write_deltalake)
        if "engine" in sig.parameters:
            kwargs["engine"] = "rust" if schema_mode == "merge" else "pyarrow"

        write_deltalake(path, df, **kwargs)

    def merge(
        self,
        target_path: str,
        source_df: Union[pl.DataFrame, pd.DataFrame],
        merge_key: Union[str, List[str]],
        update_columns: Optional[List[str]] = None,
        insert_columns: Optional[List[str]] = None,
        matched_update: bool = True,
        not_matched_insert: bool = True,
    ) -> Dict[str, int]:
        """
        Atomic MERGE (upsert) operation without Spark.

        Args:
            target_path: Path to target Delta table
            source_df: Source DataFrame with new/updated records
            merge_key: Column(s) to match on (e.g., "id" or ["id", "date"])
            update_columns: Columns to update on match (None = all)
            insert_columns: Columns to insert on no match (None = all)
            matched_update: Update matched records (True) or skip (False)
            not_matched_insert: Insert unmatched records (True) or skip (False)

        Returns:
            Dict with merge statistics (num_updated, num_inserted, num_deleted)

        Example:
            >>> adapter = DeltaAdapter()
            >>> new_data = pl.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
            >>>
            >>> # MERGE (upsert)
            >>> stats = adapter.merge(
            ...     target_path="s3://bucket/table/",
            ...     source_df=new_data,
            ...     merge_key="id"
            ... )
            >>> print(f"Updated: {stats['num_updated']}, Inserted: {stats['num_inserted']}")
        """
        self._ensure_delta_available()
        from deltalake import DeltaTable

        from lakelogic.engines.cloud_credentials import resolve_storage_options

        # Automatically resolve cloud credentials if enabled
        storage_options = self.storage_options
        if self.auto_resolve_credentials:
            storage_options = resolve_storage_options(target_path, storage_options)

        # Read target table
        dt = DeltaTable(target_path, storage_options=storage_options)

        # Convert Polars to Pandas if needed
        if isinstance(source_df, pl.DataFrame):
            source_df = source_df.to_pandas()

        # Build merge predicate
        if isinstance(merge_key, str):
            merge_key = [merge_key]

        predicate = " AND ".join([f"target.{key} = source.{key}" for key in merge_key])

        # Build merge operation
        merge_builder = dt.merge(
            source=source_df,
            predicate=predicate,
            source_alias="source",
            target_alias="target",
        )

        # Configure update/insert
        if matched_update:
            if update_columns:
                updates = {col: f"source.{col}" for col in update_columns}
                merge_builder = merge_builder.when_matched_update(updates)
            else:
                merge_builder = merge_builder.when_matched_update_all()

        if not_matched_insert:
            if insert_columns:
                inserts = {col: f"source.{col}" for col in insert_columns}
                merge_builder = merge_builder.when_not_matched_insert(inserts)
            else:
                merge_builder = merge_builder.when_not_matched_insert_all()

        # Execute merge
        result = merge_builder.execute()

        # Return statistics
        return {
            "num_updated": result.get("num_updated_rows", 0),
            "num_inserted": result.get("num_inserted_rows", 0),
            "num_deleted": result.get("num_deleted_rows", 0),
        }

    def vacuum(
        self,
        path: str,
        retention_hours: int = 168,  # 7 days
        dry_run: bool = True,
    ) -> List[str]:
        """
        Vacuum old Delta table files.

        Args:
            path: Path to Delta table
            retention_hours: Retention period in hours (default: 168 = 7 days)
            dry_run: Preview files to delete without deleting (True) or delete (False)

        Returns:
            List of files that were (or would be) deleted

        Example:
            >>> adapter = DeltaAdapter()
            >>>
            >>> # Preview files to delete
            >>> files = adapter.vacuum("s3://bucket/table/", dry_run=True)
            >>> print(f"Would delete {len(files)} files")
            >>>
            >>> # Actually delete
            >>> adapter.vacuum("s3://bucket/table/", dry_run=False)
        """
        self._ensure_delta_available()
        from deltalake import DeltaTable

        from lakelogic.engines.cloud_credentials import resolve_storage_options

        # Automatically resolve cloud credentials if enabled
        storage_options = self.storage_options
        if self.auto_resolve_credentials:
            storage_options = resolve_storage_options(path, storage_options)

        dt = DeltaTable(path, storage_options=storage_options)
        deleted_files = dt.vacuum(retention_hours=retention_hours, dry_run=dry_run)

        return deleted_files

    def get_history(self, path: str, limit: Optional[int] = None) -> pl.DataFrame:
        """
        Get Delta table history (commits).

        Args:
            path: Path to Delta table
            limit: Optional limit on number of commits to return

        Returns:
            Polars DataFrame with commit history

        Example:
            >>> adapter = DeltaAdapter()
            >>> history = adapter.get_history("s3://bucket/table/", limit=10)
            >>> print(history)
        """
        self._ensure_delta_available()
        from deltalake import DeltaTable

        from lakelogic.engines.cloud_credentials import resolve_storage_options

        # Automatically resolve cloud credentials if enabled
        storage_options = self.storage_options
        if self.auto_resolve_credentials:
            storage_options = resolve_storage_options(path, storage_options)

        dt = DeltaTable(path, storage_options=storage_options)
        history = dt.history(limit=limit)

        return pl.DataFrame(history)

    def optimize(
        self,
        path: str,
        target_size: int = 134217728,  # 128MB
    ) -> Dict[str, Any]:
        """
        Optimize Delta table (compact small files).

        Args:
            path: Path to Delta table
            target_size: Target file size in bytes (default: 128MB)

        Returns:
            Dict with optimization statistics

        Example:
            >>> adapter = DeltaAdapter()
            >>> stats = adapter.optimize("s3://bucket/table/")
            >>> print(f"Compacted {stats['num_files_added']} files")
        """
        self._ensure_delta_available()
        from deltalake import DeltaTable

        from lakelogic.engines.cloud_credentials import resolve_storage_options

        # Automatically resolve cloud credentials if enabled
        storage_options = self.storage_options
        if self.auto_resolve_credentials:
            storage_options = resolve_storage_options(path, storage_options)

        dt = DeltaTable(path, storage_options=storage_options)
        result = dt.optimize.compact(target_size=target_size)

        return {
            "num_files_added": result.metrics.get("numFilesAdded", 0),
            "num_files_removed": result.metrics.get("numFilesRemoved", 0),
            "total_files_skipped": result.metrics.get("totalFilesSkipped", 0),
        }


def is_delta_table(path: str) -> bool:
    """
    Check if path is a Delta table.

    Args:
        path: Path to check

    Returns:
        True if path is a Delta table, False otherwise

    Example:
        >>> if is_delta_table("s3://bucket/table/"):
        ...     print("This is a Delta table")
    """
    try:
        from deltalake import DeltaTable

        DeltaTable(path)
        return True
    except Exception:
        return False
