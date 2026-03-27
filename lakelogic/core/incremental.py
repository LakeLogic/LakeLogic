"""
lakelogic.core.incremental
--------------------------
Resolves the time boundary for incremental layer-to-layer processing.

Given a source layer (e.g. Bronze) that may have accumulated multiple days of
partitions, this module answers: **which partitions have NOT yet been processed
into the target layer (Silver / Gold)?**

Five strategies — all produce the same ``Boundary(from_dt, to_dt)`` output:

  1. ``max_target`` *(default)* — query MAX(watermark_field) from the target
                         Delta table.  Processes everything newer than that
                         high-water mark.  Self-healing: if silver is manually
                         modified, the next run re-reads based on the timestamp
                         boundary, not a version pointer.

  2. ``pipeline_log``  — query a pipeline audit log table for the last
                         successful run.  More reliable than watermark when the
                         target table can be partially overwritten.

  3. ``manifest``      — read a JSON manifest file listing already-processed
                         partition values.  Lightweight, no Spark required.

  4. ``date_range``    — explicit from/to dates or a human-readable lookback
                         string (e.g. ``"7 days"``, ``"3 hours"``).
                         Useful for ad-hoc backfills and Databricks Widgets.

  5. ``delta_version`` — use Delta transaction log versions to identify new
                         commits.  Fastest for large tables: reads only the
                         specific Parquet files added/changed between versions.
                         State is stored in target table TBLPROPERTIES
                         (``lakelogic.last_source_version``).

Default strategy
~~~~~~~~~~~~~~~~
When ``watermark_strategy`` is not specified in the contract source block,
``max_target`` is used.  When ``watermark_field`` is also omitted,
the processor defaults to ``_lakelogic_processed_at`` (stamped by lineage).

Choosing a strategy
~~~~~~~~~~~~~~~~~~~
==============  ===================  ==========================================
Strategy        Best for             Notes
==============  ===================  ==========================================
max_target      Most batch pipelines Self-healing; safe with external edits
pipeline_log    Partial overwrites   Needs a log table; most reliable
manifest        No-Spark pipelines   Lightweight JSON; good for Polars/Pandas
date_range      Backfills & widgets  Explicit control; not automated
delta_version   Large streaming      Fastest; Spark-only; gap-free capture
==============  ===================  ==========================================

Usage
-----
::

    from lakelogic.core.incremental import IncrementalBoundary

    # Strategy 1 — watermark on target table (DEFAULT)
    boundary = IncrementalBoundary.from_max_target(
        target_path="abfss://silver@.../listings",
        watermark_field="_lakelogic_processed_at",
    )

    # Strategy 2 — pipeline log
    boundary = IncrementalBoundary.from_pipeline_log(
        log_table="pipeline_runs",
        pipeline_name="bronze_to_silver_zoopla_listings",
    )

    # Strategy 3 — manifest file
    boundary = IncrementalBoundary.from_manifest(
        manifest_path="abfss://meta@.../manifests/bronze_to_silver.json",
        partition_field="_snapshot_date",
    )

    # Strategy 4a — explicit date range
    boundary = IncrementalBoundary.from_date_range(
        from_date="2024-03-01",
        to_date="2024-03-31",
    )

    # Strategy 4b — lookback string
    boundary = IncrementalBoundary.from_lookback("7 days")
    boundary = IncrementalBoundary.from_lookback("3 hours")
    boundary = IncrementalBoundary.from_lookback("1 month")

    # Strategy 5 — Delta table version tracking
    boundary = IncrementalBoundary.from_delta_version(
        source_path="table:catalog.bronze.sessions",
        target_path="table:catalog.silver.sessions",
    )
    # First run:  reads all versions from 0
    # Next runs:  reads only versions > last stored version
    # State:      stored in target TBLPROPERTIES('lakelogic.last_source_version')
    # Safeguards: detects source rollback (from_v > to_v) and auto-resets

    # Use the boundary to filter source data
    bronze_df = (
        spark.read.format("delta").load(BRONZE_ROOT)
             .filter(boundary.spark_filter("_lakelogic_processed_at"))
    )
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import logging

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Boundary dataclass — unified output of all strategies
# ---------------------------------------------------------------------------


@dataclass
class Boundary:
    """
    Resolved incremental processing window.

    Attributes
    ----------
    from_dt : datetime
        Inclusive start of the window (UTC).
    to_dt : datetime
        Inclusive end of the window (UTC).
    strategy : str
        Name of the strategy that resolved this boundary.
    partition_filters : dict
        Static (non-temporal) partition values to AND into every filter.
        Example: ``{"country": "GB", "region": "south"}``
    metadata : dict
        Strategy-specific metadata (watermark value found, manifest entries, etc.)
    """

    from_dt: datetime
    to_dt: datetime
    strategy: str
    partition_filters: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Convenience properties
    @property
    def from_date(self) -> date:
        return self.from_dt.date()

    @property
    def to_date(self) -> date:
        return self.to_dt.date()

    @property
    def from_iso(self) -> str:
        return self.from_dt.isoformat()

    @property
    def to_iso(self) -> str:
        return self.to_dt.isoformat()

    # ------------------------------------------------------------------
    # Spark filter helpers
    # ------------------------------------------------------------------

    def spark_filter(
        self,
        field: Optional[str] = None,
        *,
        date_parts: Optional[Union[List[str], Dict[str, str]]] = None,
    ) -> str:
        """
        Return a Spark SQL filter string covering the resolved time window.

        Supports three field layouts — pick the one that matches your table:

        **Single date/timestamp column** (most common)::

            # Table has: _snapshot_date DATE
            df.filter(boundary.spark_filter("_snapshot_date"))
            # → "_snapshot_date >= '2024-03-01' AND _snapshot_date <= '2024-03-31'"

        **Split date-part columns as a list** (year, month, day)::

            # Table has: year INT, month INT, day INT
            df.filter(boundary.spark_filter(date_parts=["year", "month", "day"]))
            # → "MAKE_DATE(year, month, day) >= '2024-03-01'
            #    AND MAKE_DATE(year, month, day) <= '2024-03-31'"

        **Split date-part columns as a dict** (custom column names)::

            # Table has: partition_year INT, partition_month INT, partition_day INT
            df.filter(boundary.spark_filter(date_parts={
                "year": "partition_year",
                "month": "partition_month",
                "day": "partition_day",
            }))

        **Mixed: static + temporal partitions**::

            # source.partition_filters: {country: GB}
            boundary = IncrementalBoundary.from_lookback(
                "7 days",
                partition_filters={"country": "GB"},
            )
            df.filter(boundary.spark_filter("_snapshot_date"))
            # → "country = 'GB' AND _snapshot_date >= '2024-...' AND ..."

        Parameters
        ----------
        field : str, optional
            Single date/timestamp column name.
        date_parts : list of str or dict, optional
            Split date columns.  List → ``[year_col, month_col, day_col]``.
            Dict → ``{"year": col, "month": col, "day": col}``.
        """
        parts: List[str] = []

        # ── Static partition filters (non-temporal) ───────────────────────────
        for col, val in self.partition_filters.items():
            if isinstance(val, str):
                parts.append(f"{col} = '{val}'")
            else:
                parts.append(f"{col} = {val}")

        # ── Temporal filter ───────────────────────────────────────────────────
        if field is not None:
            parts.append(f"{field} >= '{self.from_iso}'")
            parts.append(f"{field} <= '{self.to_iso}'")

        elif date_parts is not None:
            # Normalise to dict
            if isinstance(date_parts, list):
                if len(date_parts) == 3:
                    dp = {
                        "year": date_parts[0],
                        "month": date_parts[1],
                        "day": date_parts[2],
                    }
                elif len(date_parts) == 2:
                    dp = {"year": date_parts[0], "month": date_parts[1]}
                else:
                    raise ValueError(
                        f"date_parts list must have 2 or 3 elements [year, month, day], "
                        f"got {len(date_parts)}: {date_parts}"
                    )
            else:
                dp = date_parts

            if "day" in dp:
                make_date = f"MAKE_DATE({dp['year']}, {dp['month']}, {dp['day']})"
                parts.append(f"{make_date} >= '{self.from_date}'")
                parts.append(f"{make_date} <= '{self.to_date}'")
            elif "month" in dp:
                # Year+month only — match whole months in range
                y_col, m_col = dp["year"], dp["month"]
                parts.append(
                    f"({y_col} > {self.from_date.year} OR "
                    f"({y_col} = {self.from_date.year} AND {m_col} >= {self.from_date.month}))"
                )
                parts.append(
                    f"({y_col} < {self.to_date.year} OR "
                    f"({y_col} = {self.to_date.year} AND {m_col} <= {self.to_date.month}))"
                )
            else:
                raise ValueError("date_parts must include at least 'year' and 'month' keys")

        else:
            raise ValueError("Provide either field='col_name' or date_parts=['year','month','day']")

        return " AND ".join(parts)

    # ------------------------------------------------------------------
    # Polars filter helpers
    # ------------------------------------------------------------------

    def polars_filter(
        self,
        field: Optional[str] = None,
        *,
        date_parts: Optional[Union[List[str], Dict[str, str]]] = None,
    ):
        """
        Return a Polars boolean expression covering the resolved time window.

        Mirrors :meth:`spark_filter` but returns a Polars expression rather than
        a SQL string.

        Examples::

            # Single date column
            df.filter(boundary.polars_filter("_snapshot_date"))

            # Split date parts
            df.filter(boundary.polars_filter(date_parts=["year", "month", "day"]))

            # Custom column names
            df.filter(boundary.polars_filter(date_parts={
                "year": "partition_year", "month": "partition_month", "day": "partition_day"
            }))
        """
        import polars as pl

        expr = pl.lit(True)

        # ── Static partition filters ──────────────────────────────────────────
        for col, val in self.partition_filters.items():
            expr = expr & (pl.col(col) == val)

        # ── Temporal filter ───────────────────────────────────────────────────
        if field is not None:
            expr = expr & (pl.col(field).cast(pl.Date) >= self.from_date)
            expr = expr & (pl.col(field).cast(pl.Date) <= self.to_date)

        elif date_parts is not None:
            if isinstance(date_parts, list):
                if len(date_parts) == 3:
                    dp = {
                        "year": date_parts[0],
                        "month": date_parts[1],
                        "day": date_parts[2],
                    }
                elif len(date_parts) == 2:
                    dp = {"year": date_parts[0], "month": date_parts[1]}
                else:
                    raise ValueError(f"date_parts list must have 2 or 3 elements, got {len(date_parts)}")
            else:
                dp = date_parts

            if "day" in dp:
                # Reconstruct date: year*10000 + month*100 + day as integer key
                # Polars doesn't have MAKE_DATE directly — use date arithmetic
                y, m, d = pl.col(dp["year"]), pl.col(dp["month"]), pl.col(dp["day"])
                # Integer key: YYYYMMDD — faithful date ordering without MAKE_DATE
                date_key = y * 10_000 + m * 100 + d
                from_key = self.from_date.year * 10_000 + self.from_date.month * 100 + self.from_date.day
                to_key = self.to_date.year * 10_000 + self.to_date.month * 100 + self.to_date.day
                expr = expr & (date_key >= from_key) & (date_key <= to_key)
            elif "month" in dp:
                y_col, m_col = pl.col(dp["year"]), pl.col(dp["month"])
                ym_key = y_col * 100 + m_col
                from_ym = self.from_date.year * 100 + self.from_date.month
                to_ym = self.to_date.year * 100 + self.to_date.month
                expr = expr & (ym_key >= from_ym) & (ym_key <= to_ym)
            else:
                raise ValueError("date_parts must include at least 'year' and 'month' keys")

        else:
            raise ValueError("Provide either field='col_name' or date_parts=['year','month','day']")

        return expr

    def __repr__(self) -> str:
        pf = f", partition_filters={self.partition_filters}" if self.partition_filters else ""
        return f"Boundary(strategy={self.strategy!r}, from={self.from_date}, to={self.to_date}{pf})"


# ---------------------------------------------------------------------------
# Lookback parser
# ---------------------------------------------------------------------------

_LOOKBACK_UNITS = {
    "second": 1,
    "seconds": 1,
    "sec": 1,
    "secs": 1,
    "minute": 60,
    "minutes": 60,
    "min": 60,
    "mins": 60,
    "hour": 3600,
    "hours": 3600,
    "hr": 3600,
    "hrs": 3600,
    "day": 86400,
    "days": 86400,
    "week": 604800,
    "weeks": 604800,
    # Months and years are approximate
    "month": 86400 * 30,
    "months": 86400 * 30,
    "year": 86400 * 365,
    "years": 86400 * 365,
}


def _parse_lookback(lookback: str) -> timedelta:
    """
    Parse a human-readable lookback string into a ``timedelta``.

    Supported formats::

        "15 minutes"  "3 hours"  "7 days"  "2 weeks"  "1 month"  "1 year"
        "30mins"      "6hrs"     "90"      (bare integer → seconds)

    Parameters
    ----------
    lookback : str
        Human-readable duration string.

    Returns
    -------
    timedelta
    """
    lookback = lookback.strip().lower()

    # Bare integer → seconds
    if lookback.isdigit():
        return timedelta(seconds=int(lookback))

    m = re.match(r"^(\d+(?:\.\d+)?)\s*([a-z]+)$", lookback)
    if not m:
        raise ValueError(
            f"Cannot parse lookback string: {lookback!r}. Expected e.g. '7 days', '3 hours', '30 mins', '1 month'."
        )
    amount = float(m.group(1))
    unit = m.group(2)

    if unit not in _LOOKBACK_UNITS:
        raise ValueError(
            f"Unknown time unit {unit!r} in lookback {lookback!r}. Supported: {sorted(set(_LOOKBACK_UNITS.values()))}."
        )
    return timedelta(seconds=amount * _LOOKBACK_UNITS[unit])


# ---------------------------------------------------------------------------
# IncrementalBoundary — factory class for all 4 strategies
# ---------------------------------------------------------------------------


class IncrementalBoundary:
    """
    Factory class that resolves incremental processing windows.

    All ``from_*`` class methods return a :class:`Boundary` object.

    Design decision — why a single ``Boundary`` output?
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Every strategy answers the same question: "process source data where
    ``watermark_field`` is between X and Y".  By normalising to a ``Boundary``
    dataclass, the pipeline code stays the same regardless of strategy:

    ::

        boundary  = IncrementalBoundary.from_max_target(...)   # or any other
        source_df = spark.read.format("delta").load(BRONZE_PATH) \\
                         .filter(boundary.spark_filter("_snapshot_date"))
    """

    # ── Strategy 1: Max watermark on the target table ─────────────────────────

    @classmethod
    def from_max_target(
        cls,
        target_path: str,
        watermark_field: str,
        *,
        to_dt: Optional[datetime] = None,
        default_from: Optional[Union[datetime, str]] = None,
    ) -> Boundary:
        """
        Strategy: query ``MAX(watermark_field)`` from the target Delta table.

        Processes source data where ``watermark_field > max_value_in_target``.
        If the target table is empty or doesn't exist, uses ``default_from``
        (or 90 days ago as a safe fallback).

        Parameters
        ----------
        target_path : str
            Delta table path (ADLS / local).
        watermark_field : str
            Column name to take MAX of (e.g. ``_snapshot_date``).
        to_dt : datetime, optional
            Upper bound — defaults to NOW (UTC).
        default_from : datetime or ISO string, optional
            Fallback lower bound when target is empty. Default: 90 days ago.

        Example
        -------
        ::

            boundary = IncrementalBoundary.from_max_target(
                target_path="abfss://silver@acct.dfs.core.windows.net/zoopla/listings",
                watermark_field="_snapshot_date",
            )
            # If Silver has data up to 2024-03-05 →
            # boundary.from_date == date(2024, 3, 6)
            # boundary.to_date   == date.today()
        """
        try:
            from pyspark.sql import SparkSession
            import pyspark.sql.functions as F

            spark = SparkSession.getActiveSession()
            if spark is None:
                raise RuntimeError("No active Spark session")

            df = spark.read.format("delta").load(target_path)
            max_val = df.agg(F.max(watermark_field)).collect()[0][0]

            if max_val is None:
                raise ValueError("Target table is empty")

            # Convert to datetime
            if isinstance(max_val, datetime):
                from_dt = max_val + timedelta(seconds=1)
            elif isinstance(max_val, date):
                from_dt = datetime(max_val.year, max_val.month, max_val.day) + timedelta(days=1)
            else:
                from_dt = datetime.fromisoformat(str(max_val)) + timedelta(days=1)

            meta = {"watermark_value": str(max_val), "target_path": target_path}

        except Exception as exc:
            # Target missing / empty — use default_from
            if default_from is not None:
                from_dt = datetime.fromisoformat(default_from) if isinstance(default_from, str) else default_from
            else:
                from_dt = datetime.now(timezone.utc) - timedelta(days=90)
            meta = {"fallback_reason": str(exc), "target_path": target_path}

        _to = to_dt or datetime.now(timezone.utc)
        return Boundary(from_dt=from_dt, to_dt=_to, strategy="max_target", metadata=meta)

    # ── Strategy 1b: Delta Table Version (Snapshot) ───────────────────────────

    @classmethod
    def from_delta_version(
        cls,
        source_path: str,
        target_path: str,
        *,
        to_version: Optional[int] = None,
        default_version: int = 0,
    ) -> Boundary:
        """
        Strategy: query processing window based on Delta table versions.

        Retrieves the 'lakelogic.last_source_version' from the target Delta
        table's properties.  If not found, starts from `default_version`.

        Parameters
        ----------
        source_path : str
            Source Delta table path.
        target_path : str
            Target Delta table path (where we store the state).
        to_version : int, optional
            Upper bound — defaults to the current source version.
        default_version : int, optional
            Fallback version if no state is found. Default: 0.
        """
        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.getActiveSession()
            if spark is None:
                raise RuntimeError("No active Spark session")

            # 1. Resolve starting version from target properties
            try:
                # We use SHOW TBLPROPERTIES which is standard in Spark/Databricks
                target_name = target_path[6:] if target_path.startswith("table:") else f"delta.`{target_path}`"
                props_df = spark.sql(f"SHOW TBLPROPERTIES {target_name}")
                props = {row["key"]: row["value"] for row in props_df.collect()}
                last_ver_str = props.get("lakelogic.last_source_version")
                last_version = int(last_ver_str) if last_ver_str else None
            except Exception:
                last_version = None

            # 2. Resolve target version from source history
            source_name = source_path[6:] if source_path.startswith("table:") else f"delta.`{source_path}`"
            curr_version = spark.sql(f"DESCRIBE HISTORY {source_name} LIMIT 1").collect()[0]["version"]

            from_v = (last_version + 1) if last_version is not None else default_version
            to_v = to_version if to_version is not None else curr_version

            # Handle source rollback or target state beyond source history
            if from_v > to_v:
                logger.warning(
                    f"Last processed version ({last_version}) is >= current source version ({curr_version}). "
                    f"Possible source rollback or target state drift. Resetting to {to_v} (nothing to process)."
                )
                from_v = to_v

            meta = {
                "from_version": from_v,
                "to_version": to_v,
                "last_processed_version": last_version,
                "source_path": source_path,
                "target_path": target_path,
            }

            # Map version numbers to dummy datetimes (Boundary requires datetimes)
            # We use the epoch + version-seconds as a stable-ish representation
            return Boundary(
                from_dt=datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=from_v),
                to_dt=datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=to_v),
                strategy="delta_version",
                metadata=meta,
            )

        except Exception as exc:
            # Fallback to full load if Spark/Delta check fails
            logger.warning(f"Delta version resolution failed for {source_path}: {exc}")
            return Boundary(
                from_dt=datetime(1900, 1, 1, tzinfo=timezone.utc),
                to_dt=datetime.now(timezone.utc),
                strategy="delta_version",
                metadata={"error": str(exc), "from_version": 0},
            )

    # ── Strategy 2: Pipeline log / audit table ────────────────────────────────

    @classmethod
    def from_pipeline_log(
        cls,
        pipeline_name: str,
        *,
        log_table: str = "pipeline_runs",
        dataset: Optional[str] = None,
        data_layer: Optional[str] = None,
        domain: Optional[str] = None,
        system: Optional[str] = None,
        to_dt: Optional[datetime] = None,
        default_from: Optional[Union[datetime, str]] = None,
    ) -> Boundary:
        """
        Strategy: query the LakeLogic ``_run_logs`` table for the last
        successful run of a specific contract/dataset.

        When ``dataset`` is provided (recommended), the method queries the
        ``_run_logs`` table written by ``run_log.py`` using the ``dataset``
        column (which holds the actual target table name, e.g.
        ``bronze_google_analytics_events``).  Additional precision filters
        (``data_layer``, ``domain``, ``system``) are applied when available.

        Falls back to the legacy ``pipeline_runs`` table query (using
        ``pipeline_name`` + ``processed_through``) when ``dataset`` is not
        provided, for backward compatibility.

        Parameters
        ----------
        pipeline_name : str
            Registered name of the pipeline — used only in legacy fallback.
        log_table : str
            Spark/Databricks table name for the run log (e.g.
            ``"`catalog`.domain._run_logs"``).
        dataset : str, optional
            Target table name stored in the ``dataset`` column of
            ``_run_logs`` (e.g. ``"bronze_google_analytics_events"``).
        data_layer : str, optional
            Layer filter (``bronze``, ``silver``, ``gold``).
        domain : str, optional
            Domain filter (e.g. ``"marketing"``).
        system : str, optional
            System filter (e.g. ``"google_analytics"``).
        to_dt : datetime, optional
            Upper bound — defaults to NOW (UTC).
        default_from : datetime or ISO string, optional
            Fallback when no successful run exists.

        Example
        -------
        ::

            boundary = IncrementalBoundary.from_pipeline_log(
                pipeline_name="bronze_google_analytics_events",
                log_table="`lakelogic-lakehouse-dev-001`.marketing._run_logs",
                dataset="bronze_google_analytics_events",
                data_layer="bronze",
                domain="marketing",
                system="google_analytics",
            )
        """
        try:
            from pyspark.sql import SparkSession
            import pyspark.sql.functions as F

            spark = SparkSession.getActiveSession()
            if spark is None:
                raise RuntimeError("No active Spark session")

            if dataset:
                # ── Modern path: query _run_logs by dataset column ────────
                filt = (
                    (F.col("dataset") == dataset) & (F.col("stage") != "no_new_data") & (F.col("stage") != "reprocess")
                )
                if data_layer:
                    filt = filt & (F.col("data_layer") == data_layer)
                if domain:
                    filt = filt & (F.col("domain") == domain)
                if system:
                    filt = filt & (F.col("system") == system)

                row = (
                    spark.table(log_table)
                    .filter(filt)
                    .agg(F.max("max_watermark_value").alias("last_watermark"), F.max("timestamp").alias("last_success"))
                    .collect()[0]
                )
                last_success_str = row["last_watermark"] or row["last_success"]
                if last_success_str is None:
                    raise ValueError(
                        f"No successful run found in {log_table} for dataset={dataset!r} data_layer={data_layer!r}"
                    )

                # timestamp is stored as ISO string in _run_logs
                if isinstance(last_success_str, str):
                    last_success = datetime.fromisoformat(last_success_str.replace("Z", "+00:00"))
                elif isinstance(last_success_str, datetime):
                    last_success = last_success_str
                else:
                    last_success = datetime.fromisoformat(str(last_success_str))

                from_dt = last_success + timedelta(seconds=1)
                meta = {
                    "last_success": str(last_success),
                    "dataset": dataset,
                    "data_layer": data_layer or "",
                    "domain": domain or "",
                    "system": system or "",
                    "log_table": log_table,
                }
            else:
                # ── Legacy path: query pipeline_runs by pipeline_name ─────
                row = (
                    spark.table(log_table)
                    .filter((F.col("pipeline_name") == pipeline_name) & (F.col("status") == "success"))
                    .agg(F.max("processed_through").alias("last_success"))
                    .collect()[0]
                )
                last_success = row["last_success"]
                if last_success is None:
                    raise ValueError(f"No successful run found for {pipeline_name!r}")

                from_dt = (
                    last_success + timedelta(seconds=1)
                    if isinstance(last_success, datetime)
                    else datetime(last_success.year, last_success.month, last_success.day) + timedelta(days=1)
                )
                meta = {
                    "last_success": str(last_success),
                    "pipeline_name": pipeline_name,
                }

        except Exception as exc:
            if default_from is not None:
                from_dt = datetime.fromisoformat(default_from) if isinstance(default_from, str) else default_from
            else:
                from_dt = datetime.now(timezone.utc) - timedelta(days=90)
            meta = {"fallback_reason": str(exc), "pipeline_name": pipeline_name}

        _to = to_dt or datetime.now(timezone.utc)
        return Boundary(from_dt=from_dt, to_dt=_to, strategy="pipeline_log", metadata=meta)

    # ── Strategy 3: Manifest file ─────────────────────────────────────────────

    @classmethod
    def from_manifest(
        cls,
        manifest_path: str,
        partition_field: str = "_snapshot_date",
        *,
        to_dt: Optional[datetime] = None,
        default_from: Optional[Union[datetime, str]] = None,
    ) -> Boundary:
        """
        Strategy: read a JSON manifest file listing already-processed partition values.

        The manifest is a JSON file with the schema::

            {
              "pipeline": "bronze_to_silver_zoopla_listings",
              "processed_partitions": ["2024-03-01", "2024-03-02", "2024-03-03"],
              "last_updated": "2024-03-03T02:15:00Z"
            }

        The boundary ``from_dt`` is set to the day AFTER the latest processed partition.

        Parameters
        ----------
        manifest_path : str
            Path to the manifest JSON file (local path, ADLS path, or S3 URI).
            For cloud paths: pass a local temp path after downloading with your
            cloud SDK, or use ``dbutils.fs.cp`` on Databricks.
        partition_field : str
            For documentation only — not used at runtime (manifest stores the values).
        to_dt : datetime, optional
            Upper bound — defaults to NOW (UTC).

        Example manifest file
        ---------------------
        ::

            {
              "pipeline": "bronze_to_silver_zoopla",
              "processed_partitions": ["2024-03-01", "2024-03-02"],
              "last_updated": "2024-03-02T03:00:00Z"
            }

        Example usage
        -------------
        ::

            boundary = IncrementalBoundary.from_manifest(
                manifest_path="/dbfs/mnt/meta/manifests/bronze_to_silver_zoopla.json"
            )
            # Manifest shows last processed = '2024-03-02'
            # boundary.from_date == date(2024, 3, 3)
        """
        try:
            p = Path(manifest_path)
            if not p.exists():
                raise FileNotFoundError(f"Manifest not found: {manifest_path}")

            data = json.loads(p.read_text(encoding="utf-8"))
            partitions: List[str] = data.get("processed_partitions", [])

            if not partitions:
                raise ValueError("Manifest has no processed_partitions")

            last_partition = max(partitions)  # ISO string comparison works for dates
            last_dt = (
                datetime.fromisoformat(last_partition)
                if "T" in last_partition
                else datetime.strptime(last_partition, "%Y-%m-%d")
            )
            from_dt = last_dt + timedelta(days=1)
            meta = {
                "manifest_path": manifest_path,
                "last_partition": last_partition,
                "total_processed": len(partitions),
            }

        except Exception as exc:
            if default_from is not None:
                from_dt = datetime.fromisoformat(default_from) if isinstance(default_from, str) else default_from
            else:
                from_dt = datetime.now(timezone.utc) - timedelta(days=90)
            meta = {"fallback_reason": str(exc), "manifest_path": manifest_path}

        _to = to_dt or datetime.now(timezone.utc)
        return Boundary(from_dt=from_dt, to_dt=_to, strategy="manifest", metadata=meta)

    @classmethod
    def update_manifest(
        cls,
        manifest_path: str,
        pipeline: str,
        new_partitions: List[str],
    ) -> None:
        """
        Append newly-processed partition values to a manifest file.

        Call this AFTER a successful pipeline run to record what was processed.

        Example
        -------
        ::

            IncrementalBoundary.update_manifest(
                manifest_path="/dbfs/mnt/meta/manifests/bronze_to_silver_zoopla.json",
                pipeline="bronze_to_silver_zoopla",
                new_partitions=["2024-03-03", "2024-03-04"],
            )
        """
        p = Path(manifest_path)
        if p.exists():
            data = json.loads(p.read_text(encoding="utf-8"))
        else:
            data = {"pipeline": pipeline, "processed_partitions": []}

        existing = set(data.get("processed_partitions", []))
        existing.update(new_partitions)
        data["processed_partitions"] = sorted(existing)
        data["last_updated"] = datetime.now(timezone.utc).isoformat() + "Z"

        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(data, indent=2), encoding="utf-8")

    # ── Strategy 4a: Explicit date range ─────────────────────────────────────

    @classmethod
    def from_date_range(
        cls,
        from_date: Union[str, date, datetime],
        to_date: Optional[Union[str, date, datetime]] = None,
        *,
        partition_filters: Optional[Dict[str, Any]] = None,
    ) -> Boundary:
        """
        Strategy: explicit from/to date range.

        Ideal for Databricks Widget parameters or ad-hoc backfills.

        Parameters
        ----------
        from_date : str, date, or datetime
            Inclusive start (e.g. ``"2024-03-01"`` or a ``datetime`` object).
        to_date : str, date, or datetime, optional
            Inclusive end. Defaults to NOW (UTC).
        partition_filters : dict, optional
            Static (non-temporal) partition values to AND into every filter.
            Example: ``{"country": "GB"}``

        Example
        -------
        ::

            # Databricks Widget pattern
            from_date = dbutils.widgets.get("from_date")   # "2024-03-01"
            to_date   = dbutils.widgets.get("to_date")     # "2024-03-31"
            boundary  = IncrementalBoundary.from_date_range(from_date, to_date)

            # Backfill March 2024, GB only
            boundary = IncrementalBoundary.from_date_range(
                "2024-03-01", "2024-03-31",
                partition_filters={"country": "GB"},
            )
            df.filter(boundary.spark_filter(date_parts=["year", "month", "day"]))
            # → "country = 'GB' AND MAKE_DATE(year,month,day) BETWEEN ..."
        """

        def _to_dt(v) -> datetime:
            if isinstance(v, datetime):
                return v
            if isinstance(v, date):
                return datetime(v.year, v.month, v.day)
            return datetime.fromisoformat(str(v))

        from_dt = _to_dt(from_date)
        to_dt = _to_dt(to_date) if to_date is not None else datetime.now(timezone.utc)
        return Boundary(
            from_dt=from_dt,
            to_dt=to_dt,
            strategy="date_range",
            partition_filters=partition_filters or {},
            metadata={"from_date": str(from_date), "to_date": str(to_date or "now")},
        )

    # ── Strategy 4b: Lookback string ─────────────────────────────────────────

    @classmethod
    def from_lookback(
        cls,
        lookback: str,
        *,
        reference_dt: Optional[datetime] = None,
        partition_filters: Optional[Dict[str, Any]] = None,
    ) -> Boundary:
        """
        Strategy: human-readable lookback from NOW (or a reference point).

        Parameters
        ----------
        lookback : str
            Duration string. Examples:

            ============= ===========
            String        Window
            ============= ===========
            ``"15 mins"`` last 15 minutes
            ``"3 hours"`` last 3 hours
            ``"7 days"``  last 7 days
            ``"2 weeks"`` last 2 weeks
            ``"1 month"`` last ~30 days
            ``"1 year"``  last ~365 days
            ============= ===========

        reference_dt : datetime, optional
            Anchor point. Defaults to ``datetime.now(timezone.utc)``.
        partition_filters : dict, optional
            Static (non-temporal) partition values to AND into every filter.
            Example: ``{"country": "GB", "region": "south"}``

        Examples
        --------
        ::

            # Last 7 days, all partitions
            boundary = IncrementalBoundary.from_lookback("7 days")

            # Last 7 days, GB only (table partitioned by country, year, month, day)
            boundary = IncrementalBoundary.from_lookback(
                "7 days",
                partition_filters={"country": "GB"},
            )
            df.filter(boundary.spark_filter(date_parts=["year", "month", "day"]))
            # → "country = 'GB' AND MAKE_DATE(year, month, day) >= '...' AND ..."

            # Near-real-time micro-batch
            boundary = IncrementalBoundary.from_lookback("30 mins")
        """
        delta = _parse_lookback(lookback)
        to_dt = reference_dt or datetime.now(timezone.utc)
        from_dt = to_dt - delta
        return Boundary(
            from_dt=from_dt,
            to_dt=to_dt,
            strategy="lookback",
            partition_filters=partition_filters or {},
            metadata={"lookback": lookback, "delta_seconds": delta.total_seconds()},
        )

    # ── Convenience: resolve from contract SourceConfig ────────────────────────

    @classmethod
    def from_contract(
        cls,
        contract_path: str,
        *,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        lookback: Optional[str] = None,
        target_path: Optional[str] = None,
        pipeline_name: Optional[str] = None,
        manifest_path: Optional[str] = None,
        partition_filters: Optional[Dict[str, Any]] = None,
    ) -> Boundary:
        """
        Resolve boundary from a LakeLogic contract ``source`` block.

        Runtime overrides always take precedence, enabling Databricks Widget /
        CLI parameterisation and the **per-partition pipeline pattern**.

        Partition filter merging
        ------------------------
        ``partition_filters`` at runtime is **merged on top of** any
        ``partition_filters`` declared in the contract.  Runtime values win.
        This lets you bake defaults into the contract and add the country/
        region at runtime, OR put nothing in the contract and supply
        everything at runtime.

        Per-country pipeline patterns
        -----------------------------
        ::

            # Pattern A — country-specific contract (one YAML per country)
            #   bronze_listings_gb.yaml: partition_filters: {country: GB}
            boundary = IncrementalBoundary.from_contract(
                "contracts/bronze_listings_gb.yaml",
                lookback="7 days",
            )

            # Pattern B — generic contract, country at runtime
            for country in ["GB", "DE", "FR", "ES"]:
                boundary = IncrementalBoundary.from_contract(
                    "contracts/bronze_listings.yaml",
                    lookback="7 days",
                    partition_filters={"country": country},
                )
                (bronze_df
                    .filter(boundary.spark_filter(date_parts=["year", "month", "day"]))
                    .write.format("delta")
                    .save(f"{SILVER_ROOT}/{country}")
                )

            # Pattern C — Databricks Widgets
            boundary = IncrementalBoundary.from_contract(
                CONTRACT_PATH,
                lookback=dbutils.widgets.get("lookback"),
                partition_filters={"country": dbutils.widgets.get("country")},
            )
        """
        import yaml as _yaml

        data = _yaml.safe_load(Path(contract_path).read_text(encoding="utf-8"))
        src = data.get("source") or {}

        strategy = src.get("watermark_strategy", "max_target")
        wm_field = src.get("watermark_field", "_snapshot_date")
        contract_lookback = src.get("lookback")

        # Merge: contract is base, runtime partition_filters wins on conflicts
        merged_pf: Dict[str, Any] = {**src.get("partition_filters", {})}
        if partition_filters:
            merged_pf.update(partition_filters)

        if lookback:
            return cls.from_lookback(lookback, partition_filters=merged_pf or None)

        if from_date:
            return cls.from_date_range(from_date, to_date, partition_filters=merged_pf or None)

        if strategy == "lookback":
            lb = contract_lookback or "7 days"
            return cls.from_lookback(lb, partition_filters=merged_pf or None)

        if strategy == "pipeline_log":
            log_table = src.get("pipeline_log_table", "pipeline_runs")
            p_name = pipeline_name or src.get("pipeline_name", Path(contract_path).stem)
            b = cls.from_pipeline_log(
                p_name,
                log_table=log_table,
                dataset=src.get("dataset"),
                data_layer=src.get("data_layer"),
                domain=src.get("domain"),
                system=src.get("system"),
            )
            b.partition_filters = merged_pf
            return b

        if strategy == "manifest":
            mp = manifest_path or src.get("manifest_path", "")
            b = cls.from_manifest(mp, partition_field=wm_field)
            b.partition_filters = merged_pf
            return b

        if strategy == "date_range":
            fd = from_date or src.get("from_date")
            td = to_date or src.get("to_date")
            return cls.from_date_range(fd, td, partition_filters=merged_pf or None)

        if strategy == "delta_version":
            tp = target_path or src.get("target_path", "")
            sp = src.get("path", "")
            return cls.from_delta_version(sp, tp)

        tp = target_path or src.get("target_path", "")
        b = cls.from_max_target(tp, watermark_field=wm_field)
        b.partition_filters = merged_pf
        return b

    # ── Convenience: resolve from SourceConfig model ────────────────────────

    @classmethod
    def from_source_config(cls, source_config, **overrides) -> Boundary:
        """
        Resolve boundary from a ``SourceConfig`` Pydantic model instance.

        Supports a ``partition_filters`` override kwarg using the same merge
        semantics as :meth:`from_contract` (runtime wins over contract).

        Examples
        --------
        ::

            from lakelogic.core.models import DataContract
            contract = DataContract.from_yaml("bronze_listings.yaml")

            # Per-country pipeline
            for country in ["GB", "DE", "FR"]:
                boundary = IncrementalBoundary.from_source_config(
                    contract.source,
                    lookback="7 days",
                    partition_filters={"country": country},
                )
                bronze_df.filter(
                    boundary.spark_filter(date_parts=["year", "month", "day"])
                ).write.format("delta").save(f"{SILVER_ROOT}/{country}")
        """
        cfg = source_config.model_dump() if hasattr(source_config, "model_dump") else vars(source_config)

        # Extract partition_filters separately — needs merge, not plain replace
        runtime_pf: Dict[str, Any] = overrides.pop("partition_filters", None) or {}
        cfg.update({k: v for k, v in overrides.items() if v is not None})

        merged_pf: Dict[str, Any] = {**cfg.get("partition_filters", {})}
        merged_pf.update(runtime_pf)

        lookback = cfg.get("lookback")
        from_date = cfg.get("from_date")
        to_date = cfg.get("to_date")
        strategy = cfg.get("watermark_strategy", "max_target")
        wm_field = cfg.get("watermark_field", "_snapshot_date")

        if lookback:
            return cls.from_lookback(lookback, partition_filters=merged_pf or None)
        if from_date:
            return cls.from_date_range(from_date, to_date, partition_filters=merged_pf or None)
        if strategy == "lookback":
            return cls.from_lookback(
                cfg.get("lookback_default", "7 days"),
                partition_filters=merged_pf or None,
            )
        if strategy == "pipeline_log":
            b = cls.from_pipeline_log(
                cfg.get("pipeline_name", "unknown"),
                log_table=cfg.get("pipeline_log_table", "pipeline_runs"),
                dataset=cfg.get("dataset"),
                data_layer=cfg.get("data_layer"),
                domain=cfg.get("domain"),
                system=cfg.get("system"),
            )
            b.partition_filters = merged_pf
            return b
        if strategy == "manifest":
            b = cls.from_manifest(cfg.get("manifest_path", ""), partition_field=wm_field)
            b.partition_filters = merged_pf
            return b
        if strategy == "date_range":
            return cls.from_date_range(
                cfg.get("from_date"),
                cfg.get("to_date"),
                partition_filters=merged_pf or None,
            )
        if strategy == "delta_version":
            return cls.from_delta_version(
                cfg.get("path", ""),
                cfg.get("target_path", ""),
            )
        b = cls.from_max_target(cfg.get("target_path", ""), watermark_field=wm_field)
        b.partition_filters = merged_pf
        return b
