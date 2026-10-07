"""When a quality check runs: the ONE definition every engine and the linter import.

OLC runs a contract in this order:

    pre transforms → type checks → **pre** checks → good/bad split →
    post transforms (good rows only) → **post** checks

* A check whose ``phase`` is WRITTEN runs in that phase.
* A check whose phase is NOT written (the default) runs where its columns EXIST: in the pre phase,
  unless it reads a column that only a POST transformation creates — then in the post phase.
  This covers the automatic ``required`` check of a model field, field-level rules, and row rules
  left at the default. A gold model describes its OUTPUT, so ``required: true`` on a column the
  post SQL builds means "required in the output".
* A check written ``phase: pre`` that reads a post-created column is a contract error
  (lint ``PHS-001``): the column does not exist yet, so every row would fail.

Until 2026-10-07 the engines disagreed: Spark ran pre checks before post transforms (as the spec
says); Polars and DuckDB ran every check after them. One contract, two results — a gold contract
whose ``required`` column came from its post SQL produced rows on Polars and quarantined every row
on Spark. All three engines now import this module.

"Created" means the column does not exist before the transformation. A transformation that
REWRITES an existing column in place (``trim``, ``cast``, ``map_values`` without ``output``) does
not create it: a pre check of that column checks the source value, as written.
"""

from __future__ import annotations

import re
from typing import Any, Dict, Iterable, List, Optional, Set

#: Names in SQL text that are keywords or functions, never columns (for ``referenced_columns``).
_SQL_WORDS = {
    "and",
    "or",
    "not",
    "is",
    "null",
    "in",
    "like",
    "ilike",
    "between",
    "case",
    "when",
    "then",
    "else",
    "end",
    "true",
    "false",
    "as",
    "cast",
    "coalesce",
    "length",
    "len",
    "lower",
    "upper",
    "trim",
    "regexp",
    "rlike",
    "abs",
    "round",
    "date",
    "timestamp",
    "string",
    "int",
    "integer",
    "bigint",
    "double",
    "float",
    "decimal",
    "varchar",
    "boolean",
    "current_date",
    "current_timestamp",
    "now",
    "exists",
    "distinct",
}


def _as_dict(obj: Any) -> Dict[str, Any]:
    if obj is None:
        return {}
    if isinstance(obj, dict):
        return obj
    if hasattr(obj, "model_dump"):
        try:
            return obj.model_dump()
        except Exception:  # pragma: no cover - defensive
            return {}
    return {}


def _get(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _sql_aliases(sql: str) -> Set[str]:
    """``AS name`` aliases a SQL step produces, minus identity aliases (``currency AS currency``)."""
    out: Set[str] = set()
    for m in re.finditer(r"([`\"\w.]+)?\s+AS\s+[`\"]?(\w+)[`\"]?", str(sql), re.IGNORECASE):
        expr, alias = (m.group(1) or "").strip('`"'), m.group(2)
        if expr.split(".")[-1].lower() == alias.lower():
            continue  # passes a column through unchanged
        if alias.lower() in _SQL_WORDS:
            continue  # `CAST(x AS DOUBLE)` — a type, not a column
        out.add(alias)
    return out


def post_created_columns(contract: Any) -> Dict[str, str]:
    """Columns that only a POST-phase transformation creates: ``{column: transformation kind}``."""
    created: Dict[str, str] = {}
    fields = [_get(f, "name") for f in (_get(_get(contract, "model"), "fields") or [])]
    for t in _get(contract, "transformations") or []:
        td = _as_dict(t)
        if str(td.get("phase") or "post").lower() != "post":
            continue

        def add(name: Optional[str], kind: str) -> None:
            if name:
                created.setdefault(str(name), kind)

        for kind in ("derive", "json_extract", "lookup", "bucket", "date_diff"):
            add(_as_dict(td.get(kind)).get("field"), kind)
        add(_as_dict(td.get("date_range_explode")).get("output"), "date_range_explode")
        co = _as_dict(td.get("coalesce"))
        if co:
            add(co.get("output") or co.get("field"), "coalesce")
        for kind in ("split", "explode", "map_values"):
            cfg = _as_dict(td.get(kind))
            if cfg.get("output") and cfg.get("output") != cfg.get("field"):
                add(cfg["output"], kind)
        for _src, tgt in (_as_dict(td.get("rename")).get("mappings") or {}).items():
            add(tgt, "rename")
        jn = _as_dict(td.get("join"))
        for f in jn.get("fields") or []:
            add(f"{jn.get('prefix') or ''}{f}", "join")
        ru = _as_dict(td.get("rollup"))
        for name in ru.get("aggregations") or {}:
            add(name, "rollup")
        up = _as_dict(td.get("unpivot"))
        if up:
            add(up.get("key_field") or "key", "unpivot")
            add(up.get("value_field") or "value", "unpivot")
        pv = _as_dict(td.get("pivot"))
        if pv:
            # Pivot column names come from the DATA; the model names them. Everything the model
            # declares beyond the id columns is pivot output.
            ids = set(pv.get("id_vars") or [])
            for f in fields:
                if f and f not in ids:
                    add(f, "pivot")
        if td.get("sql"):
            for alias in _sql_aliases(td["sql"]):
                add(alias, "sql")
    return created


def referenced_columns(sql: str, candidates: Iterable[str]) -> Set[str]:
    """Which of ``candidates`` a rule's SQL mentions (identifier match, case-insensitive)."""
    tokens = {t.lower() for t in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", re.sub(r"'[^']*'", "", str(sql or "")))}
    return {c for c in candidates if c.lower() in tokens and c.lower() not in _SQL_WORDS}


def phase_was_written(rule: Any) -> bool:
    """Whether the contract wrote this rule's ``phase`` (vs. took the default)."""
    if isinstance(rule, dict):
        return "phase" in rule
    fs = getattr(rule, "model_fields_set", None)
    return bool(fs and "phase" in fs)


def effective_phase(rule: Any, post_created: Dict[str, str], field: Optional[str] = None) -> str:
    """The phase this check runs in — see the module docstring."""
    written = str(_get(rule, "phase") or "pre").lower()
    if phase_was_written(rule):
        return written
    if field and field in post_created:
        return "post"
    if referenced_columns(_get(rule, "sql") or "", post_created):
        return "post"
    return "pre"


def phase_conflicts(contract: Any) -> List[Dict[str, str]]:
    """Checks written ``phase: pre`` that read a post-created column (lint PHS-001)."""
    created = post_created_columns(contract)
    out: List[Dict[str, str]] = []
    if not created:
        return out
    rules = list(_get(_get(contract, "quality"), "row_rules") or [])
    for f in _get(_get(contract, "model"), "fields") or []:
        rules += list(_get(f, "rules") or [])
    for r in rules:
        if not phase_was_written(r) or str(_get(r, "phase") or "pre").lower() != "pre":
            continue
        for col in sorted(referenced_columns(_get(r, "sql") or "", created)):
            out.append({"rule": str(_get(r, "name") or ""), "column": col, "created_by": created[col]})
    return out
