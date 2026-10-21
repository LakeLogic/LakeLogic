"""Key discovery: which column, or combination of columns, identifies a row -- from the data.

For an estate with no contracts, nothing declares a table's key, and guessing one column
(``profile_findings.candidate_key``) misses composite keys: a table keyed on
``(order_id, line_no)`` then reads as "repeated order_id values". This finds the smallest
combination that is unique, by measurement, without an LLM:

1. **Candidates.** Non-system, non-sensitive columns with no empty values, ranked: id-like names
   (``id``, ``*_id``, ``*_key``, ``*_no``, ``*_number``, ``*_code``), then dates and times, then
   low-cardinality codes. Floating-point, fractional decimal and long free-text columns are never
   key parts.
2. **Pruning.** A combination whose distinct counts multiply to fewer than the row count cannot
   be unique, so it is never queried.
3. **Testing.** Single columns, then pairs, then triples, smallest first, at most
   ``MAX_TESTS`` queries. A combination is a key when its distinct combinations equal the rows.

The result says what was tested and how, so "no key found" is a statement about the
combinations tried, never a claim that the table has none.
"""

from __future__ import annotations

import re
from itertools import combinations
from typing import Any, Dict, List, Optional, Sequence

from lakelogic.core import profile_findings as pf

#: Widest key tried, most candidate columns considered, most uniqueness queries per table.
MAX_WIDTH = 3
MAX_CANDIDATES = 8
MAX_TESTS = 30

_ID_NAME = re.compile(r"(^id$|_id$|_key$|_no$|_num$|_number$|_code$|^code$|^key$)", re.I)
_FLOAT = ("FLOAT", "DOUBLE", "REAL")
_FRACTIONAL_DECIMAL = re.compile(r"(DECIMAL|NUMERIC)\s*\(\s*\d+\s*,\s*[1-9]", re.I)


def _rank(col: Dict[str, Any], distinct: Optional[int], rows: int) -> Optional[int]:
    """Lower ranks are tried first; ``None`` means never a key part."""
    name = str(col.get("name") or "")
    t = str(col.get("type") or "").upper()
    if (
        pf.is_system_column(name)
        or col.get("sensitive")
        or any(k in t for k in _FLOAT)
        or _FRACTIONAL_DECIMAL.search(t)
    ):  # amounts, rates: measurements, not identifiers
        return None
    if (col.get("null_count") or 0) > 0 or distinct is None or distinct <= 1:
        return None
    if _ID_NAME.search(name):
        return 0
    if pf.is_temporal_type(t):
        return 1
    if pf.is_text_type(t) and distinct > rows * 0.9 and (col.get("len_max") or 0) > 64:
        return None  # long free text (descriptions, payloads)
    if distinct <= 1000:
        return 2  # a code: status, region, line type
    return 3


_TIME_NAME = re.compile(r"(_at|_date|_time|_ts|_timestamp|_on)$|^(date|time|timestamp)$", re.I)


def _is_time(col: Optional[Dict[str, Any]]) -> bool:
    """A date or time — by type, or by name when the table stores everything as text (bronze)."""
    if not col:
        return False
    return pf.is_temporal_type(str(col.get("type") or "").upper()) or bool(
        _TIME_NAME.search(str(col.get("name") or ""))
    )


def candidates(columns: Sequence[Dict[str, Any]], distincts: Dict[str, Optional[int]], rows: int) -> List[str]:
    ranked = []
    for c in columns:
        r = _rank(c, distincts.get(c["name"]), rows)
        if r is not None:
            ranked.append((r, -(distincts.get(c["name"]) or 0), c["name"]))
    return [n for _r, _d, n in sorted(ranked)][:MAX_CANDIDATES]


def _distinct_rows(rel: pf.Relation, cols: Sequence[str]) -> int:
    q = rel.quote
    sel = ", ".join(q(c) for c in cols)
    row = rel.one(f"SELECT COUNT(*) FROM (SELECT DISTINCT {sel} FROM {rel.from_sql}) k")
    return int(row[0] or 0)


def discover_key(
    rel: pf.Relation,
    columns: Sequence[Dict[str, Any]],
    distincts: Dict[str, Optional[int]],
    rows: Optional[int],
    *,
    max_width: int = MAX_WIDTH,
    max_tests: int = MAX_TESTS,
) -> Dict[str, Any]:
    """The smallest unique combination of columns, or ``None`` with what was tried.

    ``distincts`` are exact per-column distinct counts (``profile_findings.exact_distincts``)."""
    out: Dict[str, Any] = {
        "columns": None,
        "rows": rows,
        "tested": [],
        "candidates": [],
        "max_width": max_width,
        "limit_reached": False,
    }
    if not rows:
        return out
    cands = candidates(columns, distincts, rows)
    out["candidates"] = cands
    by_name = {c["name"]: c for c in columns}
    for width in range(1, max_width + 1):
        for combo in combinations(cands, width):
            # A key made only of dates and times identifies WHEN, not WHAT: a signup_date unique
            # in 386 rows is chance, not the rider's identity (2026-10-20). Dates stay allowed
            # as a PART beside an identifier, e.g. (order_id, order_date).
            if all(_is_time(by_name.get(c)) for c in combo):
                continue
            product = 1
            for c in combo:
                product *= distincts.get(c) or 0
            if product < rows:
                continue  # cannot be unique: fewer possible combinations than rows
            if width == 1:
                unique = distincts.get(combo[0]) == rows
            else:
                if len(out["tested"]) >= max_tests:
                    out["limit_reached"] = True
                    return out
                unique = _distinct_rows(rel, combo) == rows
            out["tested"].append(list(combo))
            if unique:
                out["columns"] = list(combo)
                return out
    return out


def describe(found: Dict[str, Any], *, sampled: bool) -> str:
    """One sentence for a finding's evidence."""
    scope = "in the sample" if sampled else "on all rows"
    if found.get("columns"):
        cols = ", ".join(found["columns"])
        kind = "composite key" if len(found["columns"]) > 1 else "key"
        return f"Likely {kind}: ({cols}), unique {scope} ({found['rows']:,} rows)."
    tried = len(found.get("tested") or [])
    if found.get("limit_reached"):
        return f"No unique key found within the first {tried} combinations tested."
    if not found.get("candidates"):
        return "No column qualifies as a key part (every candidate has empty values or repeats)."
    return f"No unique combination of up to {found['max_width']} columns among {', '.join(found['candidates'])}."
