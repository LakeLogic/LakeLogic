"""What a profiling run reads, and how its results add up.

The one definition of profiling *scope* and *summary*, shared by the ``lakelogic profile`` CLI,
a profiling job in your own platform, and the LakeLogic service:

**Which tables.** ``paths`` (a folder in ADLS, S3 or GCS, or a catalog/schema in a warehouse)
plus ``tables`` by name. :func:`under_path` is the one rule for both: whole segments only, so
``raw/fin`` does not hold ``raw/finance/x``. A choice larger than the limit is refused with its
size, never silently cut.

**How much of each.** ``full`` reads every row. ``sample`` reads a number of rows or a
percentage of each table, newest first (by the table's timestamp column) or at random. A table
with no timestamp column is sampled at random, and the result says so.

**What it adds up to.** Per-table metrics, worst-first ordering, the run summary and the change
since the previous run. Not-measured columns are counted, never zeroed.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from lakelogic.core import profile_findings as pf

MODE_FULL = "full"
MODE_SAMPLE = "sample"
ORDER_NEWEST = "newest"
ORDER_RANDOM = "random"
UNIT_ROWS = "rows"
UNIT_PERCENT = "percent"

SAMPLE_ROWS_MIN = 1_000
SAMPLE_ROWS_MAX = 1_000_000
SAMPLE_ROWS_DEFAULT = 100_000
SAMPLE_PERCENT_MIN = 0.1
SAMPLE_PERCENT_MAX = 100.0
SAMPLE_PERCENT_DEFAULT = 5.0

#: Most tables one run may read, and most paths it may name.
MAX_TABLES = 500
MAX_PATHS = 50

#: A column more than this % empty counts against the table; a null shift of at least this many
#: points between runs is reported as a change.
EMPTY_THRESHOLD_PCT = pf.EMPTY_THRESHOLD_PCT
NULL_SHIFT_POINTS = 5.0

DEFAULT_SETTINGS: Dict[str, Any] = {
    "tables": None,
    "paths": None,
    "mode": MODE_FULL,
    "sample_rows": SAMPLE_ROWS_DEFAULT,
    "sample_order": ORDER_NEWEST,
    "sample_unit": UNIT_ROWS,
    "sample_percent": SAMPLE_PERCENT_DEFAULT,
}

_FRESHNESS_NAME = re.compile(r"(updated|modified|event|created|timestamp|ingest|loaded)", re.I)


# ─────────────────────────────────────────────────────────────────────────────
# Settings
# ─────────────────────────────────────────────────────────────────────────────


def _norm_path(p: Any) -> str:
    return str(p or "").strip().strip("/").strip()


def normalize_settings(raw: Optional[Dict[str, Any]], *, max_tables: int = MAX_TABLES) -> Dict[str, Any]:
    """Validated settings. Raises ``ValueError`` with a reason a person can act on."""
    raw = dict(raw or {})
    mode = str(raw.get("mode") or MODE_FULL).lower()
    if mode not in (MODE_FULL, MODE_SAMPLE):
        raise ValueError("Choose a full scan or a sample.")
    order = str(raw.get("sample_order") or ORDER_NEWEST).lower()
    if order not in (ORDER_NEWEST, ORDER_RANDOM):
        raise ValueError("A sample is drawn newest first or at random.")
    rows = raw.get("sample_rows")
    try:
        rows = SAMPLE_ROWS_DEFAULT if rows in (None, "") else int(rows)
    except (TypeError, ValueError):
        raise ValueError("The sample size must be a whole number of rows.")
    unit = str(raw.get("sample_unit") or UNIT_ROWS).lower()
    if unit not in (UNIT_ROWS, UNIT_PERCENT):
        raise ValueError("A sample is a number of rows or a percentage.")
    pct = raw.get("sample_percent")
    try:
        pct = SAMPLE_PERCENT_DEFAULT if pct in (None, "") else float(pct)
    except (TypeError, ValueError):
        raise ValueError("The sample percentage must be a number.")
    if mode == MODE_SAMPLE and unit == UNIT_ROWS and not (SAMPLE_ROWS_MIN <= rows <= SAMPLE_ROWS_MAX):
        raise ValueError(f"A sample is {SAMPLE_ROWS_MIN:,} to {SAMPLE_ROWS_MAX:,} rows.")
    if mode == MODE_SAMPLE and unit == UNIT_PERCENT and not (SAMPLE_PERCENT_MIN <= pct <= SAMPLE_PERCENT_MAX):
        raise ValueError(f"A sample is {SAMPLE_PERCENT_MIN:g}% to {SAMPLE_PERCENT_MAX:g}% of each table.")
    paths = raw.get("paths")
    if paths is not None:
        paths = list(dict.fromkeys(_norm_path(p) for p in paths if _norm_path(p))) or None
        if paths and len(paths) > MAX_PATHS:
            raise ValueError(f"Choose at most {MAX_PATHS} paths.")
    tables = raw.get("tables")
    if tables is not None:
        tables = list(dict.fromkeys(str(t).strip() for t in tables if str(t).strip())) or None
        if tables and len(tables) > max_tables:
            raise ValueError(f"Choose at most {max_tables} tables.")
    return {
        "tables": tables,
        "paths": paths,
        "mode": mode,
        "sample_rows": rows,
        "sample_order": order,
        "sample_unit": unit,
        "sample_percent": pct,
    }


def under_path(table: str, path: str) -> bool:
    """Is ``table`` at or under ``path``? One rule for every source: a folder in ADLS, S3 or GCS
    (``raw/finance`` holds ``raw/finance/invoices``) and a catalog or schema in a warehouse
    (``main.sales`` holds ``main.sales.orders``). Whole segments only."""
    t, p = table.strip("/"), _norm_path(path)
    return bool(p) and (t == p or t.startswith(p + "/") or t.startswith(p + "."))


def choose_tables(every: Sequence[str], settings: Dict[str, Any], max_tables: int = MAX_TABLES) -> List[str]:
    """The tables a run reads: everything under the chosen ``paths`` plus the chosen ``tables``
    (each must be one of the source's own), or the first ``max_tables`` when nothing is chosen.
    An unknown name, or a path that holds nothing, is refused; a choice larger than the limit is
    refused with its size, never silently cut."""
    chosen = settings.get("tables") or []
    paths = settings.get("paths") or []
    if not chosen and not paths:
        return list(every)[:max_tables]
    known = set(every)
    unknown = [t for t in chosen if t not in known]
    if unknown:
        raise ValueError(f"Not tables of this source: {', '.join(unknown[:5])}")
    empty = [p for p in paths if not any(under_path(t, p) for t in every)]
    if empty:
        raise ValueError(f"No tables under: {', '.join(empty[:5])}")
    picked = set(chosen) | {t for t in every if any(under_path(t, p) for p in paths)}
    names = [t for t in every if t in picked]
    if len(names) > max_tables:
        raise ValueError(
            f"{len(names):,} tables chosen; the limit per run is {max_tables:,}. "
            f"Choose a narrower path, or split the run."
        )
    return names


def sampling_label(settings: Dict[str, Any]) -> str:
    if settings.get("mode") == MODE_SAMPLE:
        how = "newest first" if settings.get("sample_order") == ORDER_NEWEST else "at random"
        if settings.get("sample_unit") == UNIT_PERCENT:
            return f"sample of {float(settings['sample_percent']):g}% of rows, {how}"
        return f"sample of {int(settings['sample_rows']):,} rows, {how}"
    return "full scan"


def freshness_column(schema: Sequence[Tuple[str, str]]) -> Optional[str]:
    """The column a "newest first" sample orders by: a timestamp named like updated/event/created,
    else any timestamp, else none."""
    temporal = [n for n, t in schema if pf.is_temporal_type(t) and not pf.is_system_column(n)]
    named = [n for n in temporal if _FRESHNESS_NAME.search(n)]
    return (named or temporal or [None])[0]


def sample_sql(
    from_sql: str,
    schema: Sequence[Tuple[str, str]],
    settings: Dict[str, Any],
    quote: Callable[[str], str],
    *,
    dialect: str,
) -> Tuple[str, str]:
    """``(select, selection)`` drawing the sample from ``from_sql``. ``selection`` says what was
    read (``newest 10% by event_at``, ``1,000 random rows``)."""
    col = freshness_column(schema) if settings.get("sample_order") == ORDER_NEWEST else None
    why = "" if settings.get("sample_order") == ORDER_RANDOM else " (no timestamp column to order by)"
    duck = dialect == "duckdb"
    if settings.get("sample_unit") == UNIT_PERCENT:
        p = float(settings["sample_percent"])
        if col:
            # The newest p% of rows: one window pass, same SQL in DuckDB, Databricks and Snowflake.
            return (
                f"SELECT * FROM {from_sql} QUALIFY row_number() OVER (ORDER BY {quote(col)} DESC NULLS LAST)"
                f" <= ceil(count(*) OVER () * {p} / 100)",
                f"newest {p:g}% by {col}",
            )
        if duck:
            return (f"SELECT * FROM {from_sql} USING SAMPLE {p} PERCENT (bernoulli, 42)", f"{p:g}% at random{why}")
        return (f"SELECT * FROM {from_sql} TABLESAMPLE ({p} PERCENT) REPEATABLE (42)", f"{p:g}% at random{why}")
    n = int(settings["sample_rows"])
    if col:
        return (f"SELECT * FROM {from_sql} ORDER BY {quote(col)} DESC NULLS LAST LIMIT {n}", f"newest first by {col}")
    if duck:
        return (f"SELECT * FROM {from_sql} USING SAMPLE reservoir({n} ROWS) REPEATABLE (42)", f"at random{why}")
    return (f"SELECT * FROM {from_sql} TABLESAMPLE ({n} ROWS) REPEATABLE (42)", f"{n:,} random rows{why}")


# ─────────────────────────────────────────────────────────────────────────────
# Results
# ─────────────────────────────────────────────────────────────────────────────


def table_metrics(doc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Per-table figures derived from ONE profile document."""
    cols = list((doc or {}).get("columns") or [])
    measured = [c for c in cols if c.get("null_pct") is not None]
    with_nulls = [c for c in measured if float(c["null_pct"]) > 0]
    over = [c for c in measured if float(c["null_pct"]) > EMPTY_THRESHOLD_PCT]
    return {
        "row_count": (doc or {}).get("row_count"),
        "freshness": (doc or {}).get("freshness"),
        "columns_total": len(cols),
        "columns_measured": len(measured),
        "columns_not_measured": len(cols) - len(measured),
        "columns_with_nulls": len(with_nulls),
        "pct_columns_with_nulls": (round(100.0 * len(with_nulls) / len(measured), 1) if measured else None),
        "columns_over_threshold": len(over),
        "sensitive_columns": sum(1 for c in cols if c.get("sensitive")),
        "sampling": (doc or {}).get("sampling"),
    }


def worst_first_key(entry: Dict[str, Any]) -> Tuple:
    """Most confirmed issues, then most potential issues, then the completeness figures."""
    m = entry.get("metrics") or {}
    unmeasured = entry.get("status") != "profiled" or m.get("pct_columns_with_nulls") is None
    kinds = entry.get("finding_counts") or {}
    return (
        1 if unmeasured else 0,
        -(kinds.get("confirmed") or 0),
        -(kinds.get("potential") or 0),
        -(m.get("columns_over_threshold") or 0),
        -(m.get("pct_columns_with_nulls") or 0.0),
        str(entry.get("name") or ""),
    )


def order_worst_first(entries: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(entries, key=worst_first_key)


def summarize(
    tables: Sequence[Dict[str, Any]], caps: Optional[Dict[str, Any]] = None, *, sources_found: Optional[int] = None
) -> Dict[str, Any]:
    """The run's headline figures. Not-measured columns are COUNTED, never zeroed."""
    profiled = [t for t in tables if t.get("status") == "profiled"]
    metrics = [table_metrics(t.get("profile")) for t in profiled]
    methods = sorted(
        {
            str(((t.get("profile") or {}).get("sampling") or {}).get("method"))
            for t in profiled
            if (t.get("profile") or {}).get("sampling")
        }
    )
    return {
        "tables_found": sources_found if sources_found is not None else len(tables),
        "tables_profiled": len(profiled),
        "tables_failed": sum(1 for t in tables if t.get("status") == "failed"),
        "tables_skipped": sum(1 for t in tables if t.get("status") == "skipped"),
        "tables_over_cap": max(0, (sources_found or 0) - len(tables)) if sources_found is not None else 0,
        "columns_total": sum(m["columns_total"] for m in metrics),
        "columns_measured": sum(m["columns_measured"] for m in metrics),
        "columns_not_measured": sum(m["columns_not_measured"] for m in metrics),
        "columns_over_threshold": sum(m["columns_over_threshold"] for m in metrics),
        "threshold_pct": EMPTY_THRESHOLD_PCT,
        "sensitive_columns": sum(m["sensitive_columns"] for m in metrics),
        "sampling_methods": methods,
        "findings": pf.count_by_kind(f for t in profiled for f in t.get("findings") or []),
        "tables_with_contract": sum(1 for t in profiled if t.get("contract")),
    }


def diff(
    latest: Optional[Dict[str, Any]], previous: Optional[Dict[str, Any]], *, points: float = NULL_SHIFT_POINTS
) -> Optional[Dict[str, Any]]:
    """Change since the previous run; ``None`` when there is no previous one."""
    if not latest or not previous:
        return None
    prev = {t["name"]: t for t in (previous.get("tables") or []) if t.get("status") == "profiled"}
    out: List[Dict[str, Any]] = []
    for t in latest.get("tables") or []:
        p = prev.get(t.get("name"))
        if t.get("status") != "profiled" or p is None:
            continue
        a, b = (p.get("profile") or {}), (t.get("profile") or {})
        rc_a, rc_b = a.get("row_count"), b.get("row_count")
        old_cols = {c["name"]: c for c in a.get("columns") or []}
        shifts = []
        for c in b.get("columns") or []:
            o = old_cols.get(c["name"])
            if not o or o.get("null_pct") is None or c.get("null_pct") is None:
                continue
            delta = round(float(c["null_pct"]) - float(o["null_pct"]), 2)
            if abs(delta) >= points:
                shifts.append(
                    {"column": c["name"], "before": o["null_pct"], "after": c["null_pct"], "delta_points": delta}
                )
        out.append(
            {
                "name": t["name"],
                "row_count_before": rc_a,
                "row_count_after": rc_b,
                "row_count_change": (rc_b - rc_a) if (rc_a is not None and rc_b is not None) else None,
                "null_shifts": shifts,
            }
        )
    names_prev = set(prev)
    names_now = {t["name"] for t in latest.get("tables") or [] if t.get("status") == "profiled"}
    return {
        "previous_id": previous.get("id"),
        "previous_started_at": previous.get("started_at"),
        "points": points,
        "tables": out,
        "tables_added": sorted(names_now - names_prev),
        "tables_removed": sorted(names_prev - names_now),
    }
