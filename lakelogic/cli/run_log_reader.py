"""
Run‑log reader — resolve last-success timestamps from configured run-log tables.

Extracted from ``driver.py`` to keep the pipeline driver module focused on
orchestration logic.
"""

from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from lakelogic.core.models import DataContract
from loguru import logger


class RunLogReader:
    """Read last-success timestamps from configured run log tables."""

    def __init__(self, engine: str) -> None:
        self.engine = engine

    def last_success(self, contract: DataContract) -> Optional[datetime]:
        """
        Return the last successful run timestamp for a contract from run log tables.

        Args:
            contract: DataContract instance.

        Returns:
            Timestamp of last run or None if not found.
        """
        timestamp, _ = self.last_success_info(contract)
        return timestamp

    def last_success_info(self, contract: DataContract) -> Tuple[Optional[datetime], Optional[str]]:
        """
        Return last-success timestamp with a reason code when missing.

        Args:
            contract: DataContract instance.

        Returns:
            Tuple of (timestamp, reason).
        """
        metadata = contract.metadata or {}
        table_name = metadata.get("run_log_table")
        if not table_name:
            return None, "no_run_log_table"

        backend = (metadata.get("run_log_backend") or ("spark" if self.engine == "spark" else "duckdb")).lower()

        if backend == "spark":
            return self._read_spark(table_name, contract)
        if backend == "duckdb":
            return self._read_duckdb(table_name, contract, metadata)
        if backend == "sqlite":
            return self._read_sqlite(table_name, contract, metadata)
        return None, "unsupported_backend"

    def _contract_key(self, contract: DataContract) -> str:
        """
        Resolve the run-log contract key.

        Args:
            contract: DataContract instance.

        Returns:
            Identifier used in run log tables.
        """
        if contract.info and contract.info.title:
            return contract.info.title
        if contract.dataset:
            return contract.dataset
        return "unknown"

    def _read_spark(self, table_name: str, contract: DataContract) -> Tuple[Optional[datetime], Optional[str]]:
        """
        Read last-success timestamp from a Spark run log table.

        Args:
            table_name: Spark table name.
            contract: DataContract instance.

        Returns:
            Timestamp or None.
        """
        try:
            from pyspark.sql import SparkSession
        except Exception:
            return None, "spark_unavailable"
        spark = SparkSession.builder.getOrCreate()
        if not spark.catalog.tableExists(table_name):
            return None, "run_log_table_missing"
        key = self._contract_key(contract)
        df = spark.sql(f"SELECT MAX(timestamp) AS last_ts FROM {table_name} WHERE contract = '{key}'")
        rows = df.collect()
        if not rows:
            return None, "run_log_entry_missing"
        value = rows[0]["last_ts"]
        if not value:
            return None, "run_log_entry_missing"
        return self._parse_timestamp(value), None

    def _read_duckdb(self, table_name: str, contract: DataContract, metadata: Dict[str, str]) -> Tuple[Optional[datetime], Optional[str]]:
        """
        Read last-success timestamp from a DuckDB run log table.

        Args:
            table_name: Table name.
            contract: DataContract instance.
            metadata: Contract metadata.

        Returns:
            Timestamp or None.
        """
        try:
            import duckdb
        except Exception:
            return None, "duckdb_unavailable"

        base_path = getattr(contract, "_base_path", None)
        db_path = metadata.get("run_log_database") or "logs/lakelogic_run_logs.duckdb"
        db_path = self._resolve_path(db_path, base_path)
        if not db_path.exists():
            return None, "run_log_db_missing"

        key = self._contract_key(contract)
        con = duckdb.connect(database=str(db_path))
        try:
            try:
                result = con.execute(
                    f"SELECT MAX(timestamp) FROM {table_name} WHERE contract = ?",
                    [key],
                ).fetchone()
            except Exception:
                return None, "run_log_table_missing"
        finally:
            con.close()
        if not result or not result[0]:
            return None, "run_log_entry_missing"
        return self._parse_timestamp(result[0]), None

    def _read_sqlite(self, table_name: str, contract: DataContract, metadata: Dict[str, str]) -> Tuple[Optional[datetime], Optional[str]]:
        """
        Read last-success timestamp from a SQLite run log table.

        Args:
            table_name: Table name.
            contract: DataContract instance.
            metadata: Contract metadata.

        Returns:
            Timestamp or None.
        """
        import sqlite3

        base_path = getattr(contract, "_base_path", None)
        db_path = metadata.get("run_log_database") or "logs/lakelogic_run_logs.sqlite"
        db_path = self._resolve_path(db_path, base_path)
        if not db_path.exists():
            return None, "run_log_db_missing"

        key = self._contract_key(contract)
        conn = sqlite3.connect(str(db_path))
        try:
            try:
                cur = conn.execute(
                    f"SELECT MAX(timestamp) FROM {table_name} WHERE contract = ?",
                    (key,),
                )
                result = cur.fetchone()
            except Exception:
                return None, "run_log_table_missing"
        finally:
            conn.close()
        if not result or not result[0]:
            return None, "run_log_entry_missing"
        return self._parse_timestamp(result[0]), None

    @staticmethod
    def _parse_timestamp(value) -> Optional[datetime]:
        """
        Parse a timestamp value into a datetime.

        Args:
            value: Timestamp-like value.

        Returns:
            datetime or None.
        """
        if isinstance(value, datetime):
            return value
        try:
            return datetime.fromisoformat(str(value))
        except Exception:
            return None

    @staticmethod
    def _resolve_path(raw: str, base: Optional[Path]) -> Path:
        """
        Resolve a raw path against a base directory.

        Args:
            raw: Raw path string.
            base: Base directory.

        Returns:
            Resolved Path.
        """
        path = Path(raw)
        if not path.is_absolute() and base:
            path = base / path
        return path
