"""
SLO (Service Level Objective) computation for LakeLogic.

Provides freshness and availability metrics that can be evaluated
against contract-defined thresholds.
"""

import re
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def compute_slos(
    contract: Any,
    good_df: Any,
    counts: Dict[str, Optional[int]],
    engine_name: str,
) -> Dict[str, Any]:
    """
    Compute freshness and availability SLO metrics.

    Args:
        contract: DataContract instance.
        good_df: Validated dataframe.
        counts: Count metrics for the run.
        engine_name: Current engine name.

    Returns:
        Dict containing SLO results.
    """
    slos: Dict[str, Any] = {}
    svc = contract.service_levels
    if not svc:
        return slos

    if svc.freshness:
        slos["freshness"] = _compute_freshness(good_df, svc.freshness, engine_name)

    if svc.availability is not None:
        slos["availability"] = _compute_availability(good_df, counts, svc.availability, engine_name)

    return slos


def _parse_duration_seconds(value: Any) -> Optional[float]:
    """
    Parse a duration string to seconds.

    Args:
        value: Duration (e.g., 24h, 30m) or numeric hours.

    Returns:
        Duration in seconds, if parseable.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        # Interpret numeric freshness threshold as hours
        return float(value) * 3600.0
    if isinstance(value, str):
        text = value.strip().lower()
        if not text:
            return None
        try:
            match = re.match(r"^(\d+(?:\.\d+)?)([smhdw])$", text)
            if not match:
                return None
            amount = float(match.group(1))
            unit = match.group(2)
            multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
            return amount * multipliers[unit]
        except Exception:
            return None
    return None


def _get_max_timestamp(df: Any, field: str, engine_name: str) -> Optional[datetime]:
    """
    Get the max timestamp from a dataframe column.

    Args:
        df: Engine dataframe.
        field: Column name to inspect.
        engine_name: Current engine name.

    Returns:
        Max timestamp or None.
    """
    if not field:
        return None
    try:
        import polars as pl

        if isinstance(df, pl.DataFrame):
            if field not in df.columns:
                return None
            value = df.select(pl.col(field).max()).to_series()[0]
            return _coerce_datetime(value)
    except Exception:
        pass

    try:
        import pandas as pd

        if isinstance(df, pd.DataFrame):
            if field not in df.columns:
                return None
            value = df[field].max()
            return _coerce_datetime(value)
    except Exception:
        pass

    if engine_name == "spark":
        try:
            from pyspark.sql import functions as F

            if field not in df.columns:
                return None
            value = df.agg(F.max(field).alias("max_value")).collect()[0][0]
            return _coerce_datetime(value)
        except Exception:
            return None

    return None


def _coerce_datetime(value: Any) -> Optional[datetime]:
    """
    Coerce a value to a timezone-aware datetime.

    Args:
        value: Input value (datetime/string).

    Returns:
        Datetime in UTC or None.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        import pandas as pd

        ts = pd.to_datetime(value, utc=True, errors="coerce")
        if ts is not None and ts is not pd.NaT:
            return ts.to_pydatetime()
    except Exception:
        pass
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None
    return None


def _compute_freshness(good_df: Any, freshness_obj: Any, engine_name: str) -> Dict[str, Any]:
    """
    Compute freshness metrics from the newest record timestamp.

    Args:
        good_df: Validated dataframe.
        freshness_obj: Service level definition.
        engine_name: Current engine name.

    Returns:
        Dict containing freshness metrics.
    """
    result: Dict[str, Any] = {"passed": None}
    field = None
    threshold = None
    if hasattr(freshness_obj, "field"):
        field = getattr(freshness_obj, "field", None)
        threshold = getattr(freshness_obj, "threshold", None)
    elif isinstance(freshness_obj, dict):
        field = freshness_obj.get("field")
        threshold = freshness_obj.get("threshold")
    elif isinstance(freshness_obj, str):
        threshold = freshness_obj

    threshold_seconds = _parse_duration_seconds(threshold)
    if threshold_seconds is not None:
        result["threshold_seconds"] = threshold_seconds

    max_ts = _get_max_timestamp(good_df, field, engine_name) if field else None
    if max_ts is None:
        return result

    now = datetime.now(timezone.utc)
    age_seconds = (now - max_ts).total_seconds()
    result["max_timestamp"] = max_ts.isoformat()
    result["age_seconds"] = age_seconds
    if threshold_seconds is not None:
        result["passed"] = age_seconds <= threshold_seconds
    return result


def _compute_availability(
    good_df: Any,
    counts: Dict[str, Optional[int]],
    availability_obj: Any,
    engine_name: str,
) -> Dict[str, Any]:
    """
    Compute availability metrics based on non-null ratio or good/total.

    Args:
        good_df: Validated dataframe.
        counts: Count metrics.
        availability_obj: Service level definition.
        engine_name: Current engine name.

    Returns:
        Dict containing availability metrics.
    """
    result: Dict[str, Any] = {"passed": None}
    field = None
    threshold = None

    if hasattr(availability_obj, "field"):
        field = getattr(availability_obj, "field", None)
        threshold = getattr(availability_obj, "threshold", None)
    elif isinstance(availability_obj, dict):
        field = availability_obj.get("field")
        threshold = availability_obj.get("threshold")
    else:
        threshold = availability_obj

    ratio = None
    if field:
        ratio = _non_null_ratio(good_df, field, engine_name)
    else:
        total = counts.get("total")
        good = counts.get("good")
        if total and good is not None:
            ratio = good / total

    result["ratio"] = ratio

    if threshold is not None:
        try:
            threshold_val = float(threshold)
        except Exception:
            threshold_val = None
        if threshold_val is not None:
            if threshold_val > 1:
                threshold_val = threshold_val / 100.0
            result["threshold"] = threshold_val
            if ratio is not None:
                result["passed"] = ratio >= threshold_val

    return result


def _non_null_ratio(df: Any, field: str, engine_name: str) -> Optional[float]:
    """
    Calculate the non-null ratio for a given column.

    Args:
        df: Engine dataframe.
        field: Column name.
        engine_name: Current engine name.

    Returns:
        Ratio of non-null values.
    """
    try:
        import polars as pl

        if isinstance(df, pl.DataFrame):
            if field not in df.columns:
                return None
            total = df.height
            if total == 0:
                return None
            non_null = df.select(pl.col(field).is_not_null().sum()).to_series()[0]
            return float(non_null) / float(total)
    except Exception:
        pass

    try:
        import pandas as pd

        if isinstance(df, pd.DataFrame):
            if field not in df.columns:
                return None
            total = len(df)
            if total == 0:
                return None
            non_null = df[field].notna().sum()
            return float(non_null) / float(total)
    except Exception:
        pass

    if engine_name == "spark":
        try:
            from pyspark.sql import functions as F

            if field not in df.columns:
                return None
            # Single aggregation: compute total and non-null count together
            # Avoids two separate .count() calls (each triggering full DAG)
            result = df.agg(F.count("*").alias("total"), F.count(F.col(field)).alias("non_null")).first()
            total = result["total"]
            non_null = result["non_null"]
            if total == 0:
                return None
            return float(non_null) / float(total)
        except Exception:
            return None

    return None
