"""Profile findings — what is wrong with a profiled table, and what to do about it.

``lakelogic.core.profile`` returns statistics; this module turns them, plus a few targeted
check queries and the table's contract, into **findings** a person can act on. It is the one
definition: the ``lakelogic profile`` CLI, a job in your own platform, and the LakeLogic
service all import it, so a finding means the same thing wherever it was produced.

**Kinds.**

``confirmed``  A check failed a requirement the matched contract DECLARES (``primary_key``,
               ``accepted_values``, ``required``). The contract is the agreement, so a
               failure against it is a fact, not an opinion.
``potential``  An observation a person must confirm: repeated IDs where no key is declared,
               events out of order, a column more than 20% empty. It may be legitimate
               (event history, optional fields), so it is never reported as an issue.
``gap``        A control is missing: no contract, no declared key, no freshness target,
               categorical columns with no accepted values.

**Values shown.** A value appears in a finding or a frequency list only if its column is not
sensitive AND the value occurs at least ``MIN_VALUE_COUNT`` times — a value that rare could
identify someone. Text columns never keep a smallest/largest value (``hide_text_ranges``):
a name in a column called ``screen_name`` showed two full names (2026-10-13).

**System columns.** Names starting with ``_`` (``_lakelogic_processed_at``, the generator's
``_is_invalid``) are pipeline stamps, not data: tagged, never the subject of a finding.

**Not checked is not passing.** A check that could not run is recorded with its reason and
produces no finding; the table's ``checks`` entry says it was not run.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

KIND_CONFIRMED = "confirmed"
KIND_POTENTIAL = "potential"
KIND_GAP = "gap"
KIND_ORDER = (KIND_CONFIRMED, KIND_POTENTIAL, KIND_GAP)

#: A value is shown only when it occurs at least this often (and its column is not sensitive).
MIN_VALUE_COUNT = 5
#: Columns with at most this many distinct values get value frequencies.
FREQUENCY_MAX_DISTINCT = 12
#: A column more than this % empty is a potential issue (the baseline's own threshold).
EMPTY_THRESHOLD_PCT = 20.0
#: A candidate key this close to unique is the key; its repeats are duplicates, not a sign
#: of a composite key.
NEAR_UNIQUE_RATIO = 0.95
#: A candidate key must be distinct on at least this share of rows to be checked as a key.
KEY_MIN_DISTINCT_RATIO = 0.5

#: Lifecycle stages, earliest first. A timestamp column's stage is read from its name
#: (``pickup_at`` → pickup). Rows where a later stage is earlier than an earlier one are counted.
LIFECYCLE: Tuple[Tuple[str, ...], ...] = (
    ("requested", "ordered", "placed", "booked", "submitted"),
    ("accepted", "assigned", "confirmed", "approved"),
    ("pickup", "started", "start", "departed", "dispatched", "shipped"),
    ("dropoff", "completed", "ended", "end", "arrived", "delivered", "finished", "closed"),
)
#: Pairs checked independently of the lifecycle: (earlier, later).
ORDERED_PAIRS: Tuple[Tuple[str, str], ...] = (
    ("created", "updated"),
    ("valid_from", "valid_to"),
    ("start", "end"),
    ("signup", "updated"),
)

_TS_SUFFIX = re.compile(r"_(at|time|ts|timestamp|date|datetime|on)$", re.I)
_TEXT_TYPES = ("CHAR", "STRING", "TEXT", "VARCHAR", "UUID", "JSON")


#: Identifier-like names: an id needs a key or relationship check, never a list of allowed values.
_IDENTIFIER = re.compile(r"(^id$|_id$|_key$|_uuid$|_guid$|_no$|_num$|_number$|_ref$)", re.I)


def is_identifier_name(name: str) -> bool:
    return bool(_IDENTIFIER.search(str(name or "")))


#: Free-text names: few distinct values in a sample does not make prose a category.
_FREE_TEXT = re.compile(
    r"(^|_)(notes?|comments?|descriptions?|desc|reasons?|messages?|remarks?|feedback|text|body|summary)$", re.I
)


def is_free_text_name(name: str) -> bool:
    return bool(_FREE_TEXT.search(str(name or "")))


#: Names whose values change by design (a new release, a new payload shape): a fixed list breaks.
_NOT_A_CATEGORY = re.compile(r"(^|_)(version|json|payload|properties|attributes|metadata|params|tags)$", re.I)


def is_category_candidate(col: Dict[str, Any]) -> bool:
    """Whether a column's observed values make a useful ``accepted_values`` suggestion.

    Not when only ONE value was seen ("status: active" is what the sample held, not every
    status the column allows), when the type or name says JSON/version, or when the values read
    as prose or code (spaces, semicolons, braces). Ported from the platform's copy (2026-10-15),
    which the move to Core left behind: the findings suggested ``status: active`` again
    (2026-10-20).
    """
    name = str(col.get("name") or "")
    if is_identifier_name(name) or is_free_text_name(name) or _NOT_A_CATEGORY.search(name):
        return False
    if "JSON" in str(col.get("type") or "").upper():
        return False
    values = [str(v.get("value")) for v in (col.get("frequencies") or []) if v.get("value") is not None]
    if len(values) < 2:
        return False
    return not any(re.search(r"[;{}\[\]]|\s\S+\s", v) for v in values)


# ─────────────────────────────────────────────────────────────────────────────
# Column helpers
# ─────────────────────────────────────────────────────────────────────────────


def is_system_column(name: str) -> bool:
    return str(name or "").startswith("_")


def is_text_type(type_name: Optional[str]) -> bool:
    t = str(type_name or "").upper()
    return any(k in t for k in _TEXT_TYPES)


def is_temporal_type(type_name: Optional[str]) -> bool:
    t = str(type_name or "").upper()
    return "TIMESTAMP" in t or t.startswith("DATE")


def hide_text_ranges(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Drop the smallest/largest VALUE of every text column (lengths stay). In place."""
    for c in doc.get("columns") or []:
        if is_text_type(c.get("type")) and (c.get("min") is not None or c.get("max") is not None):
            c["min"] = None
            c["max"] = None
            c["range_hidden"] = "text"
    return doc


def visible_values(pairs: Iterable[Tuple[Any, int]], *, sensitive: bool) -> Tuple[List[Dict[str, Any]], int]:
    """``(shown, hidden_count)``: values seen at least ``MIN_VALUE_COUNT`` times, unless the
    column is sensitive (then nothing is shown)."""
    shown: List[Dict[str, Any]] = []
    hidden = 0
    for value, count in pairs:
        if not sensitive and int(count) >= MIN_VALUE_COUNT:
            shown.append({"value": None if value is None else str(value), "count": int(count)})
        else:
            hidden += 1
    return shown, hidden


def _stage_of(name: str) -> Optional[int]:
    stem = _TS_SUFFIX.sub("", name.lower())
    tokens = stem.split("_")
    for i, words in enumerate(LIFECYCLE):
        if any(t in words for t in tokens):
            return i
    return None


def _looks_temporal(col: Dict[str, Any]) -> bool:
    return is_temporal_type(col.get("type")) or bool(_TS_SUFFIX.search(str(col.get("name") or "")))


def lifecycle_pairs(columns: Sequence[Dict[str, Any]]) -> List[Tuple[str, str]]:
    """(earlier, later) column pairs to compare, read from column names. Each column is compared
    with the nearest LATER stage present, so a missing middle stage does not break the chain."""
    temporal = [c for c in columns if _looks_temporal(c) and not is_system_column(c["name"])]
    staged = sorted(((s, c["name"]) for c in temporal if (s := _stage_of(c["name"])) is not None))
    pairs: List[Tuple[str, str]] = []
    for i, (stage, name) in enumerate(staged):
        later = [n for s, n in staged[i + 1 :] if s > stage]
        if later:
            nxt_stage = min(s for s, n in staged[i + 1 :] if s > stage)
            pairs += [(name, n) for s, n in staged[i + 1 :] if s == nxt_stage]
    names = {c["name"].lower(): c["name"] for c in temporal}
    for a, b in ORDERED_PAIRS:
        ca = next((names[n] for n in names if _TS_SUFFIX.sub("", n) == a), None)
        cb = next((names[n] for n in names if _TS_SUFFIX.sub("", n) == b), None)
        if ca and cb and (ca, cb) not in pairs:
            pairs.append((ca, cb))
    return pairs


def _singular(word: str) -> str:
    if word.endswith("ies"):
        return word[:-3] + "y"
    if word.endswith("ses") or word.endswith("xes"):
        return word[:-2]
    if word.endswith("s") and not word.endswith("ss"):
        return word[:-1]
    return word


def candidate_key(
    entity: str, columns: Sequence[Dict[str, Any]], distincts: Dict[str, Optional[int]], rows: Optional[int]
) -> Optional[str]:
    """The column most likely to identify a row when the contract declares no key.

    Among ``id`` / ``*_id`` columns that are mostly distinct (``KEY_MIN_DISTINCT_RATIO``),
    prefer one named after the entity (``trip_id`` for ``trip_completed``), then the most
    distinct. ``None`` when nothing qualifies: a key is never guessed from a low-distinct
    column (``driver_id`` in telemetry repeats by design)."""
    if not rows:
        return None
    base = entity.replace("\\", "/").rsplit("/", 1)[-1].rsplit(".", 1)[-1].lower()
    named = {f"{base}_id", f"{_singular(base)}_id"} | {f"{_singular(t)}_id" for t in base.split("_") if t}
    best = None
    for c in columns:
        n = str(c.get("name") or "")
        low = n.lower()
        if is_system_column(n) or not (low == "id" or low.endswith("_id")):
            continue
        d = distincts.get(n)
        if d is None or d < KEY_MIN_DISTINCT_RATIO * rows:
            continue
        score = (1 if low in named else 0, d)
        if best is None or score > best[0]:
            best = (score, n)
    return best[1] if best else None


# ─────────────────────────────────────────────────────────────────────────────
# Check queries — run on the SAME relation the profile read (full table or the sample)
# ─────────────────────────────────────────────────────────────────────────────


class Relation:
    """What the checks query: a FROM clause, a quoting rule, and a way to fetch a few rows."""

    def __init__(
        self,
        from_sql: str,
        quote: Callable[[str], str],
        fetch: Callable[[str, int], List[tuple]],
        text_type: str = "VARCHAR",
    ):
        self.from_sql = from_sql
        self.text_type = text_type
        self.quote = quote
        self.fetch = fetch

    def one(self, sql: str) -> tuple:
        rows = self.fetch(sql, 2)
        return rows[0] if rows else ()


def exact_distincts(rel: Relation, names: Sequence[str]) -> Dict[str, Optional[int]]:
    if not names:
        return {}
    q = rel.quote
    sel = ", ".join(f"COUNT(DISTINCT {q(n)})" for n in names)
    row = rel.one(f"SELECT {sel} FROM {rel.from_sql}")
    return {n: (int(v) if v is not None else None) for n, v in zip(names, row)}


def frequencies(rel: Relation, name: str, limit: int = FREQUENCY_MAX_DISTINCT + 1) -> List[Tuple[Any, int]]:
    c = rel.quote(name)
    return [
        (r[0], int(r[1]))
        for r in rel.fetch(
            f"SELECT {c}, COUNT(*) AS n FROM {rel.from_sql} WHERE {c} IS NOT NULL "
            f"GROUP BY {c} ORDER BY n DESC LIMIT {int(limit)}",
            limit,
        )
    ]


def duplicate_keys(rel: Relation, key: Sequence[str]) -> Dict[str, int]:
    """Keys that appear more than once among rows whose key is complete."""
    q = rel.quote
    cols = ", ".join(q(k) for k in key)
    not_null = " AND ".join(f"{q(k)} IS NOT NULL" for k in key)
    row = rel.one(
        f"SELECT COUNT(*), COALESCE(SUM(n), 0) FROM (SELECT {cols}, COUNT(*) AS n FROM {rel.from_sql} "
        f"WHERE {not_null} GROUP BY {cols} HAVING COUNT(*) > 1) d"
    )
    return {"repeated_keys": int(row[0] or 0), "rows_involved": int(row[1] or 0)}


def order_violations(rel: Relation, earlier: str, later: str) -> Dict[str, int]:
    """Rows where ``later`` is before ``earlier`` (both present and readable as timestamps)."""
    a = f"TRY_CAST({rel.quote(earlier)} AS TIMESTAMP)"
    b = f"TRY_CAST({rel.quote(later)} AS TIMESTAMP)"
    row = rel.one(
        f"SELECT COUNT(*) FILTER (WHERE {a} IS NOT NULL AND {b} IS NOT NULL), "
        f"COUNT(*) FILTER (WHERE {b} < {a}) FROM {rel.from_sql}"
    )
    return {"compared": int(row[0] or 0), "violations": int(row[1] or 0)}


def _as_number(v: Any) -> Optional[float]:
    if isinstance(v, bool):
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def outside_accepted(
    rel: Relation, name: str, accepted: Sequence[Any], type_name: Optional[str] = None
) -> List[Tuple[Any, int]]:
    """Values (with counts) of ``name`` that are not in ``accepted``.

    A numeric column with all-numeric accepted values is compared AS NUMBERS: a CSV reader types
    a tip of ``2`` as ``2.0``, and a text comparison called 710 valid tips invalid (2026-10-14).
    Everything else is compared as text."""
    c = rel.quote(name)
    nums = [_as_number(v) for v in accepted]
    if not is_text_type(type_name) and type_name and all(n is not None for n in nums):
        allowed = ", ".join(repr(float(n)) for n in nums)
        return [
            (r[0], int(r[1]))
            for r in rel.fetch(
                f"SELECT {c} AS v, COUNT(*) AS n FROM {rel.from_sql} WHERE {c} IS NOT NULL "
                f"AND TRY_CAST({c} AS DOUBLE) NOT IN ({allowed}) GROUP BY v ORDER BY n DESC LIMIT 20",
                20,
            )
        ]
    allowed = ", ".join("'" + str(v).replace("'", "''") + "'" for v in accepted)
    return [
        (r[0], int(r[1]))
        for r in rel.fetch(
            f"SELECT CAST({c} AS {rel.text_type}) AS v, COUNT(*) AS n FROM {rel.from_sql} WHERE {c} IS NOT NULL "
            f"AND CAST({c} AS {rel.text_type}) NOT IN ({allowed}) GROUP BY v ORDER BY n DESC LIMIT 20",
            20,
        )
    ]


# ─────────────────────────────────────────────────────────────────────────────
# The contract, read once
# ─────────────────────────────────────────────────────────────────────────────


def contract_rules(doc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """What a contract declares that the checks can test. Missing keys are absent, not empty."""
    doc = doc or {}
    model = doc.get("model") or {}
    fields = [f for f in (model.get("fields") or []) if isinstance(f, dict) and f.get("name")]
    pk = doc.get("primary_key") or model.get("primary_key") or []
    if isinstance(pk, str):
        pk = [pk]
    sl = doc.get("service_levels") or {}
    return {
        "primary_key": [str(k) for k in pk],
        "required": [f["name"] for f in fields if f.get("required") is True],
        "accepted_values": {
            f["name"]: list(dict.fromkeys(f["accepted_values"]))
            for f in fields
            if isinstance(f.get("accepted_values"), list)
        },
        "pii": [f["name"] for f in fields if f.get("pii") is True],
        "freshness": (sl.get("freshness") if isinstance(sl, dict) else None),
        "quality_rules": len(((doc.get("quality") or {}).get("row_rules") or []))
        + len(((doc.get("quality") or {}).get("dataset_rules") or []))
        if isinstance(doc.get("quality"), dict)
        else 0,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Running the checks
# ─────────────────────────────────────────────────────────────────────────────


def run_checks(rel: Relation, entity: str, doc: Dict[str, Any], rules: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Exact distincts, frequencies, key, order and contract-rule checks for one table.

    Mutates ``doc``'s columns with ``distinct_exact``, ``frequencies``, ``values_hidden`` and
    ``system``; returns the check results (``checks``) for :func:`findings_for`."""
    cols = list(doc.get("columns") or [])
    rows = doc.get("row_count")
    names = [c["name"] for c in cols]
    out: Dict[str, Any] = {"errors": []}

    def guard(label: str, fn: Callable[[], Any]) -> Any:
        try:
            return fn()
        except Exception as exc:  # noqa: BLE001 — one failed check never fails the table
            out["errors"].append({"check": label, "error": f"{type(exc).__name__}: {str(exc)[:200]}"})
            return None

    distincts = guard("exact distinct counts", lambda: exact_distincts(rel, names)) or {}
    for c in cols:
        c["system"] = is_system_column(c["name"])
        if c["name"] in distincts:
            c["distinct_exact"] = distincts[c["name"]]
        d = distincts.get(c["name"])
        if d is not None and 0 < d <= FREQUENCY_MAX_DISTINCT and not c["system"]:
            pairs = guard(f"frequencies of {c['name']}", lambda n=c["name"]: frequencies(rel, n))
            if pairs is not None:
                shown, hidden = visible_values(pairs, sensitive=bool(c.get("sensitive")))
                c["frequencies"] = shown
                c["values_hidden"] = hidden

    rules = rules or {}
    declared = [k for k in rules.get("primary_key") or [] if k in names]
    key = declared
    if not declared:
        # No declared key: find the smallest unique combination from the data (composite keys
        # included). Found means the rows ARE unique on it, so nothing repeats; only when no
        # combination is unique is the single-column guess checked for repeated values.
        from lakelogic.core.profile_keys import discover_key

        found = guard("key discovery", lambda: discover_key(rel, cols, distincts, rows))
        if found is not None:
            out["key_discovery"] = found
        single = candidate_key(entity, cols, distincts, rows)
        near_unique = bool(single and rows and (distincts.get(single) or 0) >= NEAR_UNIQUE_RATIO * rows)
        if not (found and found.get("columns")) or (near_unique and found["columns"] != [single]):
            # An id column already unique on almost every row (trip_id, 1,000 of 1,001) is the
            # key, and its repeats are duplicates: a timestamp that happens to be unique, or one
            # added to "make it unique", would hide them. Only an id far from unique (order_id on
            # order lines) needs a partner.
            key = [single] if single else []
    if key:
        res = guard("duplicate keys", lambda: duplicate_keys(rel, key))
        if res is not None:
            out["key"] = {"columns": key, "declared": bool(declared), **res}
    out["order"] = []
    for a, b in lifecycle_pairs(cols):
        res = guard(f"order of {a} and {b}", lambda a=a, b=b: order_violations(rel, a, b))
        if res is not None:
            out["order"].append({"earlier": a, "later": b, **res})
    out["accepted"] = []
    for name, values in (rules.get("accepted_values") or {}).items():
        if name not in names:
            continue
        ctype = next((c.get("type") for c in cols if c["name"] == name), None)
        res = guard(f"accepted values of {name}", lambda n=name, v=values, t=ctype: outside_accepted(rel, n, v, t))
        if res is not None:
            col = next(c for c in cols if c["name"] == name)
            shown, hidden = visible_values(res, sensitive=bool(col.get("sensitive")))
            out["accepted"].append(
                {
                    "column": name,
                    "accepted": [str(v) for v in values],
                    "rows_outside": sum(n for _v, n in res),
                    "values": shown,
                    "values_hidden": hidden,
                }
            )
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Findings
# ─────────────────────────────────────────────────────────────────────────────


def _pct(n: int, d: Optional[int]) -> str:
    return f"{(100.0 * n / d):.1f}%" if d else "n/a"


def _finding(
    kind: str,
    table: str,
    title: str,
    *,
    evidence: str,
    why: str,
    action: str,
    verify: str,
    columns: Sequence[str] = (),
    count: Optional[int] = None,
) -> Dict[str, Any]:
    slug = re.sub(r"[^a-z0-9]+", "-", f"{kind}-{table}-{title}".lower()).strip("-")[:120]
    return {
        "id": slug,
        "kind": kind,
        "table": table,
        "columns": list(columns),
        "title": title,
        "count": count,
        "evidence": evidence,
        "why": why,
        "action": action,
        "verify": verify,
    }


def findings_for(
    table: str,
    doc: Dict[str, Any],
    checks: Dict[str, Any],
    contract: Optional[Dict[str, Any]],
    rules: Optional[Dict[str, Any]],
    *,
    sampled: bool,
) -> List[Dict[str, Any]]:
    """The findings for one profiled table, confirmed first. See the module docstring."""
    rows = doc.get("row_count")
    cols = {c["name"]: c for c in doc.get("columns") or []}
    scope = " in the sample" if sampled else ""
    out: List[Dict[str, Any]] = []
    rules = rules or {}
    cname = (contract or {}).get("name")

    key = checks.get("key")
    if key and key["repeated_keys"] > 0:
        k = ", ".join(key["columns"])
        ev = (
            f"{key['repeated_keys']} {k} values appear more than once, across {key['rows_involved']} "
            f"of {rows} rows{scope}."
        )
        if key["declared"]:
            out.append(
                _finding(
                    KIND_CONFIRMED,
                    table,
                    f"Duplicate primary key ({k})",
                    columns=key["columns"],
                    count=key["rows_involved"],
                    evidence=ev,
                    why=f'The contract "{cname}" declares {k} as the primary key: one row per key. '
                    "Joins and counts downstream will multiply these rows.",
                    action="Find where the duplicates enter (re-sent files, retries, missing dedup) "
                    "and deduplicate on the key before the table is published.",
                    verify=f"Profile again: repeated {k} values should be 0.",
                )
            )
        else:
            out.append(
                _finding(
                    KIND_POTENTIAL,
                    table,
                    f"Repeated {k} values",
                    columns=key["columns"],
                    count=key["rows_involved"],
                    evidence=ev,
                    why=f"If one row per {k} is expected, these rows are duplicates and will "
                    "inflate every count that uses this table. Event or history tables "
                    "repeat keys by design.",
                    action=f"Confirm the table's grain. If it is one row per {k}, declare "
                    f"primary_key: [{k}] in the contract so it is enforced.",
                    verify="Once declared, the contract's run reports duplicates, and this "
                    "becomes a confirmed issue until they are gone.",
                )
            )

    for o in checks.get("order") or []:
        if o["violations"] <= 0:
            continue
        out.append(
            _finding(
                KIND_POTENTIAL,
                table,
                f"{o['later']} before {o['earlier']}",
                columns=[o["earlier"], o["later"]],
                count=o["violations"],
                evidence=f"{o['violations']} of {o['compared']} rows ({_pct(o['violations'], o['compared'])}) "
                f"have {o['later']} earlier than {o['earlier']}{scope}.",
                why="Events recorded out of order make durations negative and break any metric built on "
                "them (trip time, time to deliver).",
                action=f"Check the source's clock and time zone handling for {o['later']}. If the order "
                f"is a rule, add a row rule: {o['later']} >= {o['earlier']}.",
                verify=f"Profile again: rows with {o['later']} before {o['earlier']} should be 0.",
            )
        )

    for a in checks.get("accepted") or []:
        if a["rows_outside"] <= 0:
            continue
        shown = ", ".join(f"{v['value']} ({v['count']})" for v in a["values"])
        h = a["values_hidden"]
        more = (
            (
                f"{'; ' if shown else ''}{h} {'value' if h == 1 else 'values'} not shown "
                f"(seen fewer than {MIN_VALUE_COUNT} times, or sensitive)"
            )
            if h
            else ""
        )
        out.append(
            _finding(
                KIND_CONFIRMED,
                table,
                f"{a['column']} outside its accepted values",
                columns=[a["column"]],
                count=a["rows_outside"],
                evidence=f"{a['rows_outside']} of {rows} rows{scope} are not one of "
                f"{', '.join(a['accepted'])}" + (f": {shown}{more}." if (shown or more) else "."),
                why=f'The contract "{cname}" lists the values {a["column"]} may take. Reports that group '
                "by it will show unexpected categories or drop these rows.",
                action="Decide whether the new values are valid. If they are, add them to accepted_values; "
                "if not, fix them at the source or map them in the pipeline.",
                verify=f"Profile again: rows outside accepted values for {a['column']} should be 0.",
            )
        )

    for name in rules.get("required") or []:
        c = cols.get(name)
        if c and (c.get("null_count") or 0) > 0:
            out.append(
                _finding(
                    KIND_CONFIRMED,
                    table,
                    f"Required {name} is empty",
                    columns=[name],
                    count=c["null_count"],
                    evidence=f"{c['null_count']} of {rows} rows ({c.get('null_pct')}%) have no {name}{scope}.",
                    why=f'The contract "{cname}" marks {name} as required.',
                    action=f"Find why {name} is missing at the source, or stop marking it required.",
                    verify=f"Profile again: empty {name} should be 0.",
                )
            )

    required = set(rules.get("required") or [])
    for c in cols.values():
        if c.get("system") or c["name"] in required or c.get("null_pct") is None:
            continue
        if float(c["null_pct"]) > EMPTY_THRESHOLD_PCT:
            out.append(
                _finding(
                    KIND_POTENTIAL,
                    table,
                    f"{c['name']} is mostly empty",
                    columns=[c["name"]],
                    count=c.get("null_count"),
                    evidence=f"{c['null_pct']}% of rows{scope} have no {c['name']}.",
                    why="An empty field cannot be used for filtering, joining or reporting. It may be "
                    "optional by design.",
                    action=f"Confirm whether {c['name']} is optional. If it should always be present, "
                    "mark it required in the contract.",
                    verify=f"Profile again and compare: {c['name']} should drop below "
                    f"{EMPTY_THRESHOLD_PCT:.0f}% empty.",
                )
            )

    found = checks.get("key_discovery")
    likely = ""
    if found:
        from lakelogic.core.profile_keys import describe

        likely = " " + describe(found, sampled=sampled)
    if contract is None:
        out.append(
            _finding(
                KIND_GAP,
                table,
                "No contract governs this table",
                evidence="No published contract reads or names this table." + likely,
                why="Nothing declares what this table should contain, so none of its problems can be "
                "confirmed or enforced.",
                action="Draft a contract from this profile (types, key, accepted values), then publish it.",
                verify="Profile again: findings for this table are then checked against the contract.",
            )
        )
        out[-1]["controls"] = [{"control": "contract"}]
    else:
        missing = []
        if not rules.get("primary_key"):
            key_cols = (found or {}).get("columns")
            scope = "in the sample" if sampled else "on all rows"
            missing.append(
                f"no primary key (likely ({', '.join(key_cols)}), unique {scope})" if key_cols else "no primary key"
            )
        if not rules.get("freshness"):
            missing.append("no freshness target")
        categorical = [
            c["name"]
            for c in cols.values()
            if not c.get("system")
            and not c.get("sensitive")
            and c.get("frequencies") is not None
            and c["name"] not in (rules.get("accepted_values") or {})
            and is_text_type(c.get("type"))
            and is_category_candidate(c)
        ]
        if categorical:
            noun = "column" if len(categorical) == 1 else "columns"
            missing.append(
                f"{len(categorical)} categorical {noun} without accepted values "
                f"({', '.join(categorical[:5])}{'…' if len(categorical) > 5 else ''})"
            )
        if missing:
            out.append(
                _finding(
                    KIND_GAP,
                    table,
                    "Contract is missing controls",
                    columns=categorical,
                    evidence=f'The contract "{cname}" has ' + "; ".join(missing) + ".",
                    why="Without these, duplicates, stale data and unexpected categories pass unnoticed.",
                    action="Add what is missing to the contract. The profile shows the observed key and "
                    "values to start from.",
                    verify="Once added, the next profile checks them and reports any failures as confirmed.",
                )
            )
            out[-1]["controls"] = _controls(rules, checks, cols, categorical)
    rank = {k: i for i, k in enumerate(KIND_ORDER)}
    return sorted(out, key=lambda f: (rank[f["kind"]], -(f.get("count") or 0)))


def _controls(
    rules: Dict[str, Any], checks: Dict[str, Any], cols: Dict[str, Dict[str, Any]], categorical: Sequence[str]
) -> List[Dict[str, Any]]:
    """The missing controls of one table, structured, each with what the profile suggests, so a
    surface can group gaps by fix ("No freshness target: 7 tables") instead of by table."""
    out: List[Dict[str, Any]] = []
    if not rules.get("primary_key"):
        found = (checks.get("key_discovery") or {}).get("columns")
        key = checks.get("key") or {}
        suggested = found or (
            key.get("columns") if key and not key.get("declared") and key.get("repeated_keys") == 0 else None
        )
        out.append({"control": "primary_key", "suggested": suggested})
    if not rules.get("freshness"):
        out.append({"control": "freshness"})
    if categorical:
        out.append(
            {
                "control": "accepted_values",
                "columns": [
                    {"name": n, "values": [v["value"] for v in (cols[n].get("frequencies") or [])]} for n in categorical
                ],
            }
        )
    return out


def count_by_kind(findings: Iterable[Dict[str, Any]]) -> Dict[str, int]:
    out = {k: 0 for k in KIND_ORDER}
    for f in findings:
        out[f["kind"]] = out.get(f["kind"], 0) + 1
    return out
