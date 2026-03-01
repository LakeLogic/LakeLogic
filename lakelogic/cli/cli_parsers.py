"""
CLI argument parsers — standalone functions for parsing driver CLI arguments.

Extracted from ``driver.py`` to keep the pipeline driver module focused on
orchestration logic.
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from loguru import logger


def parse_layers(raw: str, strict: bool = False) -> List[str]:
    """
    Parse a comma-separated layer list.

    Args:
        raw: Raw layer string.
        strict: Whether to enforce a strict allowed ordering.

    Returns:
        List of layers.
    """
    allowed = ["bronze", "silver", "gold", "reference"]
    aliases = {"ref": "reference"}
    layers = [lyr.strip().lower() for lyr in raw.split(",") if lyr.strip()]
    layers = [aliases.get(lyr, lyr) for lyr in layers]

    if not layers:
        raise ValueError("No layers specified.")
    invalid = [lyr for lyr in layers if lyr not in allowed]
    if invalid:
        raise ValueError(f"Invalid layer(s): {','.join(invalid)}. Allowed: {','.join(allowed)}")

    if strict:
        order_map = {name: idx for idx, name in enumerate(allowed)}
        positions = [order_map[lyr] for lyr in layers if lyr in order_map]
        if positions != sorted(positions):
            raise ValueError(f"Invalid layer order: {','.join(allowed)}. Got: {','.join(layers)}")

    return layers


def parse_entities(raw: Optional[str]) -> Optional[Set[str]]:
    """
    Parse comma-separated entity filters.

    Args:
        raw: Raw entity string.

    Returns:
        Set of entity names or None.
    """
    if not raw:
        return None
    return {e.strip() for e in raw.split(",") if e.strip()}


def parse_contracts(raw: Optional[str]) -> Optional[Set[Path]]:
    """
    Parse comma-separated contract paths into absolute paths.

    Args:
        raw: Raw contract path string.

    Returns:
        Set of resolved Paths or None.
    """
    if not raw:
        return None
    return {Path(c.strip()).resolve() for c in raw.split(",") if c.strip()}


def parse_metrics_tags(raw: Optional[str]) -> Dict[str, str]:
    """
    Parse comma-separated key=value tags for metrics.

    Args:
        raw: Raw tag string.

    Returns:
        Dict of tag keys to values.
    """
    if not raw:
        return {}
    tags = {}
    for pair in raw.split(","):
        pair = pair.strip()
        if "=" in pair:
            key, value = pair.split("=", 1)
            tags[key.strip()] = value.strip()
        else:
            logger.warning(f"Ignoring unparseable tag: {pair}")
    return tags


def parse_overrides(values: Optional[List[str]]) -> Dict[str, Any]:
    """
    Parse --set key=value overrides into a dict.
    """
    if not values:
        return {}
    overrides: Dict[str, Any] = {}
    for item in values:
        if "=" not in item:
            logger.warning(f"Ignoring unparseable override: {item}")
            continue
        key, value = item.split("=", 1)
        key = key.strip()
        value = value.strip()
        if value.lower() == "true":
            value = True
        elif value.lower() == "false":
            value = False
        elif value.isdigit():
            value = int(value)
        else:
            try:
                value = float(value)
            except ValueError:
                pass
        overrides[key] = value
    return overrides


def build_backfill_windows(start: str, end: str, granularity: str) -> list:
    """
    Build a list of windows for backfill execution.
    """
    from lakelogic.cli.driver import Window

    start_dt = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_dt = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    if end_dt < start_dt:
        raise ValueError("Backfill end date must be on or after start date.")

    delta = timedelta(days=1) if granularity == "day" else timedelta(weeks=1)
    windows = []
    current = start_dt
    while current <= end_dt:
        window_end = current + delta
        windows.append(Window(current, window_end, f"backfill_{current.strftime('%Y%m%d')}"))
        current = window_end
    return windows


def parse_window(
    raw: str,
    window_start_date: Optional[str],
    window_end_date: Optional[str],
    reprocess_date: Optional[str],
    reprocess_start_date: Optional[str],
    reprocess_end_date: Optional[str],
) -> Tuple:
    """
    Parse window and reprocess parameters.

    Args:
        raw: Window selector.
        window_start_date: Optional start date string.
        window_end_date: Optional end date string.
        reprocess_date: Optional date string.
        reprocess_start_date: Optional start date string.
        reprocess_end_date: Optional end date string.

    Returns:
        Tuple of (Window, reprocess flag).
    """
    from lakelogic.cli.driver import Window

    if reprocess_date or reprocess_start_date or reprocess_end_date:
        if reprocess_date:
            start = datetime.strptime(reprocess_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            end = start + timedelta(days=1)
        else:
            if not reprocess_start_date or not reprocess_end_date:
                raise ValueError("Both --reprocess-start-date and --reprocess-end-date are required.")
            start = datetime.strptime(reprocess_start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            end = datetime.strptime(reprocess_end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
            if end <= start:
                raise ValueError("Reprocess end date must be on or after start date.")
        return Window(start, end, "reprocess"), True

    if raw == "none":
        return Window(None, None, "full"), False
    if raw == "yesterday":
        now = datetime.now(timezone.utc)
        start = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        return Window(start, end, "yesterday"), False
    if raw == "range":
        if not window_start_date or not window_end_date:
            raise ValueError("Both --window-start-date and --window-end-date are required for window=range.")
        start = datetime.strptime(window_start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end = datetime.strptime(window_end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
        if end <= start:
            raise ValueError("Window end date must be on or after start date.")
        return Window(start, end, "range"), False
    return Window(None, None, "last_success"), False
