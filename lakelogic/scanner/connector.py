"""
Scanner connectors — connect to Delta, Unity Catalog, DuckDB data sources
and expose a uniform interface for table discovery and metadata reads.

All connectors are read-only. No row-level data is read — only aggregates
(MIN, MAX, COUNT) and catalog metadata. Safe for VPC-restricted environments.
"""

from __future__ import annotations

import datetime
import fnmatch
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from loguru import logger

from lakelogic.scanner.config import ConnectionConfig, DiscoveryConfig

# ── Data classes ──────────────────────────────────────────────────────────────


@dataclass
class ScannedTable:
    catalog: str
    schema: str
    table: str
    storage_path: Optional[str] = None  # cloud/local path for Delta reads
    engine: str = "delta"  # delta | duckdb | snowflake | bigquery

    @property
    def full_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.table}"

    @property
    def layer(self) -> str:
        """Infer medallion layer from schema or table name."""
        combined = f"{self.schema}.{self.table}".lower()
        for layer in ("bronze", "silver", "gold"):
            if layer in combined:
                return layer
        return self.schema


@dataclass
class HistoryEntry:
    timestamp: datetime.datetime
    operation: str
    num_output_rows: Optional[int] = None


@dataclass
class TableMetadata:
    table: ScannedTable
    num_rows: Optional[int] = None
    size_bytes: Optional[int] = None
    last_modified: Optional[datetime.datetime] = None
    schema_fields: List[Dict[str, Any]] = field(default_factory=list)
    # [{"name": "id", "type": "long", "nullable": True}, ...]
    history: List[HistoryEntry] = field(default_factory=list)
    # Last N operations from Delta transaction log


# ── Base connector ────────────────────────────────────────────────────────────


class BaseConnector(ABC):
    @abstractmethod
    def connect(self) -> None:
        """Validate credentials and connectivity. Raises on failure."""

    @abstractmethod
    def discover(self, config: DiscoveryConfig) -> List[ScannedTable]:
        """Return all tables matching the discovery config."""

    @abstractmethod
    def get_metadata(self, table: ScannedTable) -> TableMetadata:
        """Return metadata for a single table — no row-level data."""

    def query_min_timestamp(self, table: ScannedTable, columns: List[str]) -> Optional[datetime.datetime]:
        """
        Try each candidate column and return MIN(col) for the first one that
        exists and parses. Returns None if no column resolves.
        Default implementation returns None — connectors override where supported.
        """
        return None

    @staticmethod
    def _matches_exclude(name: str, patterns: List[str]) -> bool:
        return any(fnmatch.fnmatch(name, pat) for pat in patterns)


# ── Delta connector ───────────────────────────────────────────────────────────


class DeltaConnector(BaseConnector):
    """
    Reads Delta tables directly from cloud or local storage using the
    deltalake Python library. No Spark required.
    """

    def __init__(self, config: ConnectionConfig):
        self.storage_root = (config.storage_root or "").rstrip("/")
        self._storage_options: Optional[Dict[str, str]] = None
        self._credentials = config.credentials or "env"

    def _get_storage_options(self) -> Optional[Dict[str, str]]:
        if self._storage_options is not None:
            return self._storage_options
        try:
            from lakelogic.core.paths import enrich_azure_storage_options
            from lakelogic.engines.cloud_credentials import resolve_storage_options

            opts = resolve_storage_options(self.storage_root)
            self._storage_options = enrich_azure_storage_options(opts)
        except Exception:
            self._storage_options = {}
        return self._storage_options

    def connect(self) -> None:
        if not self.storage_root:
            raise ValueError("DeltaConnector requires connection.storage_root")
        try:
            from deltalake import DeltaTable  # noqa: F401
        except ImportError:
            raise ImportError("Install deltalake: pip install lakelogic[delta]")
        logger.info(f"DeltaConnector: storage root = {self.storage_root}")

    def discover(self, config: DiscoveryConfig) -> List[ScannedTable]:
        """Walk storage_root and find directories containing _delta_log/."""
        tables: List[ScannedTable] = []
        root = self.storage_root

        if root.startswith(("abfss://", "s3://", "gs://")):
            tables = self._discover_cloud(root, config)
        else:
            tables = self._discover_local(root, config)

        if config.max_tables:
            tables = tables[: config.max_tables]

        logger.info(f"DeltaConnector: discovered {len(tables)} tables under {root}")
        return tables

    def _discover_local(self, root: str, config: DiscoveryConfig) -> List[ScannedTable]:
        from pathlib import Path

        tables: List[ScannedTable] = []
        root_path = Path(root)
        if not root_path.exists():
            logger.warning(f"Storage root does not exist: {root}")
            return []

        for delta_log in root_path.rglob("_delta_log"):
            table_path = delta_log.parent
            parts = table_path.relative_to(root_path).parts
            if len(parts) == 0:
                continue

            schema = parts[-2] if len(parts) >= 2 else "default"
            table = parts[-1]

            if self._matches_exclude(table, config.exclude_patterns):
                continue
            if config.include_schemas and schema not in config.include_schemas:
                continue

            tables.append(
                ScannedTable(
                    catalog=root_path.name,
                    schema=schema,
                    table=table,
                    storage_path=str(table_path),
                    engine="delta",
                )
            )
        return tables

    def _discover_cloud(self, root: str, config: DiscoveryConfig) -> List[ScannedTable]:
        try:
            import fsspec
        except ImportError:
            logger.warning("Install fsspec for cloud discovery: pip install fsspec adlfs")
            return []

        tables: List[ScannedTable] = []
        storage_opts = self._get_storage_options() or {}

        try:
            protocol = root.split("://")[0]
            fs = fsspec.filesystem(protocol, **storage_opts)
            # Walk two levels looking for _delta_log directories
            try:
                entries = fs.ls(root, detail=False)
            except Exception as e:
                logger.warning(f"Cannot list {root}: {e}")
                return []

            for schema_path in entries:
                schema_name = schema_path.rstrip("/").split("/")[-1]
                if self._matches_exclude(schema_name, config.exclude_patterns):
                    continue
                if config.include_schemas and schema_name not in config.include_schemas:
                    continue
                try:
                    table_entries = fs.ls(schema_path, detail=False)
                except Exception:
                    continue
                for table_path in table_entries:
                    table_name = table_path.rstrip("/").split("/")[-1]
                    if self._matches_exclude(table_name, config.exclude_patterns):
                        continue
                    delta_log_path = f"{table_path}/_delta_log"
                    if fs.exists(delta_log_path):
                        full_path = f"{protocol}://{table_path}" if not table_path.startswith(protocol) else table_path
                        tables.append(
                            ScannedTable(
                                catalog=root.split("://")[1].split("/")[0],
                                schema=schema_name,
                                table=table_name,
                                storage_path=full_path,
                                engine="delta",
                            )
                        )
        except Exception as exc:
            logger.warning(f"Cloud discovery failed: {exc}")

        return tables

    def get_metadata(self, table: ScannedTable) -> TableMetadata:
        from deltalake import DeltaTable

        path = table.storage_path or f"{self.storage_root}/{table.schema}/{table.table}"
        storage_opts = self._get_storage_options()

        try:
            dt = DeltaTable(path, storage_options=storage_opts or None)
        except Exception as exc:
            logger.warning(f"Cannot open Delta table {path}: {exc}")
            return TableMetadata(table=table)

        # Schema
        schema_fields = [{"name": f.name, "type": str(f.type), "nullable": f.nullable} for f in dt.schema().fields]

        # Detail — num_rows, size, last_modified
        num_rows: Optional[int] = None
        size_bytes: Optional[int] = None
        last_modified: Optional[datetime.datetime] = None

        try:
            dt.metadata()
            # deltalake exposes these via protocol/add files
            files = dt.get_add_actions(flatten=True).to_pydict()
            if files.get("size_bytes"):
                size_bytes = sum(files["size_bytes"])
            if files.get("num_records"):
                num_rows = sum(r for r in files["num_records"] if r is not None)
            if files.get("modification_time"):
                max_mtime = max(files["modification_time"])
                last_modified = datetime.datetime.fromtimestamp(max_mtime / 1000, tz=datetime.timezone.utc)
        except Exception:
            pass  # get_add_actions not available in all versions

        # Fallback: last_modified from history
        history: List[HistoryEntry] = []
        try:
            raw_history = dt.history(limit=20)
            for entry in raw_history:
                ts = entry.get("timestamp")
                if ts:
                    ts_dt = datetime.datetime.fromtimestamp(ts / 1000, tz=datetime.timezone.utc)
                    if last_modified is None:
                        last_modified = ts_dt
                    op_metrics = entry.get("operationMetrics", {})
                    history.append(
                        HistoryEntry(
                            timestamp=ts_dt,
                            operation=entry.get("operation", ""),
                            num_output_rows=int(op_metrics.get("numOutputRows", 0) or 0) or None,
                        )
                    )
        except Exception:
            pass

        return TableMetadata(
            table=table,
            num_rows=num_rows,
            size_bytes=size_bytes,
            last_modified=last_modified,
            schema_fields=schema_fields,
            history=history,
        )

    def query_min_timestamp(self, table: ScannedTable, columns: List[str]) -> Optional[datetime.datetime]:
        try:
            import duckdb

            path = table.storage_path or f"{self.storage_root}/{table.schema}/{table.table}"
            con = duckdb.connect()
            for col in columns:
                try:
                    res = con.execute(f"SELECT MIN(TRY_CAST({col} AS TIMESTAMP)) FROM delta_scan('{path}')").fetchone()
                    if res and res[0] is not None:
                        ts = res[0]
                        if hasattr(ts, "tzinfo") and ts.tzinfo is None:
                            ts = ts.replace(tzinfo=datetime.timezone.utc)
                        return ts
                except Exception:
                    continue
            con.close()
        except ImportError:
            pass
        return None


# ── DuckDB connector ──────────────────────────────────────────────────────────


class DuckDBConnector(BaseConnector):
    """
    Scans local Delta or Parquet files via DuckDB. Ideal for local dev and CI.
    """

    def __init__(self, config: ConnectionConfig):
        self.root = config.path or config.storage_root or "."
        self._con = None

    def connect(self) -> None:
        import duckdb

        self._con = duckdb.connect()
        logger.info(f"DuckDBConnector: root = {self.root}")

    def _con_(self):
        if self._con is None:
            import duckdb

            self._con = duckdb.connect()
        return self._con

    def discover(self, config: DiscoveryConfig) -> List[ScannedTable]:
        from pathlib import Path

        root_path = Path(self.root)
        tables: List[ScannedTable] = []

        # Find Delta tables (_delta_log) or bare Parquet dirs
        for delta_log in root_path.rglob("_delta_log"):
            table_path = delta_log.parent
            parts = table_path.relative_to(root_path).parts
            schema = parts[-2] if len(parts) >= 2 else "default"
            table = parts[-1]
            if self._matches_exclude(table, config.exclude_patterns):
                continue
            if config.include_schemas and schema not in config.include_schemas:
                continue
            tables.append(
                ScannedTable(
                    catalog=root_path.name,
                    schema=schema,
                    table=table,
                    storage_path=str(table_path),
                    engine="duckdb",
                )
            )

        if config.max_tables:
            tables = tables[: config.max_tables]
        logger.info(f"DuckDBConnector: discovered {len(tables)} tables")
        return tables

    def get_metadata(self, table: ScannedTable) -> TableMetadata:
        path = table.storage_path or f"{self.root}/{table.schema}/{table.table}"
        con = self._con_()
        schema_fields: List[Dict[str, Any]] = []
        num_rows: Optional[int] = None
        last_modified: Optional[datetime.datetime] = None

        try:
            desc = con.execute(f"DESCRIBE SELECT * FROM delta_scan('{path}') LIMIT 0").fetchall()
            schema_fields = [{"name": r[0], "type": r[1], "nullable": True} for r in desc]
            row = con.execute(f"SELECT COUNT(*) FROM delta_scan('{path}')").fetchone()
            if row:
                num_rows = row[0]
        except Exception:
            try:
                parquet_glob = f"{path}/**/*.parquet"
                desc = con.execute(f"DESCRIBE SELECT * FROM parquet_scan('{parquet_glob}') LIMIT 0").fetchall()
                schema_fields = [{"name": r[0], "type": r[1], "nullable": True} for r in desc]
                row = con.execute(f"SELECT COUNT(*) FROM parquet_scan('{parquet_glob}')").fetchone()
                if row:
                    num_rows = row[0]
            except Exception as exc:
                logger.debug(f"DuckDB metadata failed for {path}: {exc}")

        # last_modified from filesystem
        from pathlib import Path as _Path

        p = _Path(path)
        if p.exists():
            mtime = p.stat().st_mtime
            last_modified = datetime.datetime.fromtimestamp(mtime, tz=datetime.timezone.utc)

        return TableMetadata(
            table=table,
            num_rows=num_rows,
            last_modified=last_modified,
            schema_fields=schema_fields,
        )

    def query_min_timestamp(self, table: ScannedTable, columns: List[str]) -> Optional[datetime.datetime]:
        path = table.storage_path or f"{self.root}/{table.schema}/{table.table}"
        con = self._con_()
        for col in columns:
            for scan_fn in (f"delta_scan('{path}')", f"parquet_scan('{path}/**/*.parquet')"):
                try:
                    res = con.execute(f"SELECT MIN(TRY_CAST({col} AS TIMESTAMP)) FROM {scan_fn}").fetchone()
                    if res and res[0] is not None:
                        ts = res[0]
                        if hasattr(ts, "tzinfo") and ts.tzinfo is None:
                            ts = ts.replace(tzinfo=datetime.timezone.utc)
                        return ts
                    break
                except Exception:
                    continue
        return None


# ── Unity Catalog connector ───────────────────────────────────────────────────


class UnityCatalogConnector(BaseConnector):
    """
    Connects to Databricks Unity Catalog via the REST API.
    Uses httpx (already a core dep) — no Spark or Databricks SDK required.
    """

    _API = "/api/2.1/unity-catalog"

    def __init__(self, config: ConnectionConfig):
        self.host = (config.host or "").rstrip("/")
        self.token = config.token or ""
        self.catalog = config.catalog or ""
        self._headers = {"Authorization": f"Bearer {self.token}"}

    def connect(self) -> None:
        if not self.host or not self.token:
            raise ValueError("UnityCatalogConnector requires connection.host and connection.token")
        try:
            import httpx

            r = httpx.get(
                f"{self.host}{self._API}/catalogs/{self.catalog}",
                headers=self._headers,
                timeout=10,
            )
            r.raise_for_status()
            logger.info(f"UnityCatalogConnector: connected to {self.host} catalog={self.catalog}")
        except Exception as exc:
            raise ConnectionError(f"Unity Catalog connection failed: {exc}") from exc

    def _get(self, path: str, params: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        import httpx

        r = httpx.get(
            f"{self.host}{self._API}{path}",
            headers=self._headers,
            params=params or {},
            timeout=30,
        )
        r.raise_for_status()
        return r.json()

    def discover(self, config: DiscoveryConfig) -> List[ScannedTable]:
        tables: List[ScannedTable] = []

        # List schemas
        try:
            schemas_resp = self._get("/schemas", {"catalog_name": self.catalog})
            schemas = [s["name"] for s in schemas_resp.get("schemas", [])]
        except Exception as exc:
            logger.warning(f"Failed to list schemas in {self.catalog}: {exc}")
            return []

        for schema in schemas:
            if self._matches_exclude(schema, config.exclude_patterns):
                continue
            if config.include_schemas and schema not in config.include_schemas:
                continue

            try:
                tables_resp = self._get(
                    "/tables",
                    {"catalog_name": self.catalog, "schema_name": schema},
                )
                for t in tables_resp.get("tables", []):
                    name = t.get("name", "")
                    if self._matches_exclude(name, config.exclude_patterns):
                        continue
                    tables.append(
                        ScannedTable(
                            catalog=self.catalog,
                            schema=schema,
                            table=name,
                            storage_path=t.get("storage_location"),
                            engine="unity_catalog",
                        )
                    )
            except Exception as exc:
                logger.warning(f"Failed to list tables in {self.catalog}.{schema}: {exc}")

            if config.max_tables and len(tables) >= config.max_tables:
                break

        logger.info(f"UnityCatalogConnector: discovered {len(tables)} tables")
        return tables[: config.max_tables] if config.max_tables else tables

    def get_metadata(self, table: ScannedTable) -> TableMetadata:
        try:
            resp = self._get(f"/tables/{self.catalog}.{table.schema}.{table.table}")
        except Exception as exc:
            logger.warning(f"Failed to get metadata for {table.full_name}: {exc}")
            return TableMetadata(table=table)

        # Schema from columns
        schema_fields = [
            {
                "name": c.get("name", ""),
                "type": c.get("type_text", c.get("type_name", "")),
                "nullable": c.get("nullable", True),
            }
            for c in resp.get("columns", [])
        ]

        # Timestamps
        last_modified: Optional[datetime.datetime] = None
        updated_at = resp.get("updated_at")
        if updated_at:
            last_modified = datetime.datetime.fromtimestamp(
                updated_at / 1000 if updated_at > 1e10 else updated_at,
                tz=datetime.timezone.utc,
            )

        # Row count from table properties if available
        num_rows: Optional[int] = None
        props = resp.get("properties", {})
        if "numRows" in props:
            try:
                num_rows = int(props["numRows"])
            except (ValueError, TypeError):
                pass

        return TableMetadata(
            table=table,
            num_rows=num_rows,
            last_modified=last_modified,
            schema_fields=schema_fields,
        )

    def query_min_timestamp(self, table: ScannedTable, columns: List[str]) -> Optional[datetime.datetime]:
        # Requires Spark or DBSQL — delegate to DeltaConnector if storage_location available
        if table.storage_path:
            delta_cfg_mock = type(
                "C",
                (),
                {
                    "storage_root": table.storage_path,
                    "credentials": "env",
                },
            )()
            try:
                dc = DeltaConnector(delta_cfg_mock)
                return dc.query_min_timestamp(table, columns)
            except Exception:
                pass
        return None


# ── Factory ───────────────────────────────────────────────────────────────────


def build_connector(config: ConnectionConfig) -> BaseConnector:
    """Instantiate the correct connector for the configured type."""
    t = config.type.lower().replace("-", "_")
    if t == "delta":
        return DeltaConnector(config)
    if t in ("unity_catalog", "databricks"):
        return UnityCatalogConnector(config)
    if t == "duckdb":
        return DuckDBConnector(config)
    raise ValueError(f"Unknown connection type '{config.type}'. Supported: delta, unity_catalog, duckdb")
