"""Detect cross-layer double-hashing of a masked column.

WHAT THIS IS FOR
----------------
Masking in LakeLogic is applied **write-side, per contract run**, and the
silver/gold contract templates propagate the ``masking:`` strategy downward. So
a field hashed in bronze was read back by silver, matched ``masking: hash`` on
silver's *own* contract, and was hashed AGAIN —
``sha256(salt + sha256(salt + value))`` — with gold making it three deep.
Nothing raised and nothing was logged. The only symptom was that the same
person's key stopped matching across layers, so every cross-layer join or
reconciliation on that column silently returned nothing.

``masking_engine`` now carries an idempotence guard (``_is_already_hashed``),
so NEW writes cannot double-hash. This module is about the data that was
already written before that guard existed.

WHAT THIS IS **NOT**
--------------------
**This does not repair anything.** SHA-256 is one-way. A column holding
``H(H(x))`` cannot be turned back into ``H(x)``, by this module or by anything
else — the information is gone from that column. There is no in-place fix, no
migration, no "unhash" step, and this module deliberately offers none.

What it does is *detect* the condition, which is enough, because the damage is
recoverable by **reprocessing**: bronze still holds ``B = H(x)``, so re-running
the downstream contract from its upstream — with the guard now in place —
rewrites the downstream column correctly. That is an ordinary pipeline run, not
a data migration.

THE TEST
--------
If upstream stores ``B`` and downstream stores ``S`` for the same entity, then
downstream is double-hashed **iff** ``H(salt + B) == S``. Three outcomes are
possible per row, and they are NOT the same thing:

``double_hashed``
    ``H(salt + B) == S`` (or ``H(B) == S`` unsalted). Downstream re-hashed a
    value that was already hashed. Broken.

``consistent``
    ``B == S``. Downstream carried the upstream value through unchanged. This
    is the correct behaviour, and it is salt-independent — it is true whatever
    the salt was.

``indeterminate``
    Neither matched. A different salt, a different transformation, or the
    column was re-sourced from somewhere else entirely. **This is not "clean".**
    It means the check could not tell, and it is reported as such — never
    folded into ``consistent``.

Damage is frequently PARTIAL — a backfill that straddled the fix leaves some
rows double-hashed and some fine — so every result carries per-row counts. A
single boolean for the column would be a lie.

SALT
----
The salt is ``LAKELOGIC_PII_SALT`` (the same variable ``processor.py`` feeds to
``MaskingEngine``). If it is empty or unset, the salted form cannot be
computed. In that case this module still tries the UNSALTED form and says
explicitly which one matched (``salt_match``), and if any rows come back
indeterminate it marks the whole result ``conclusive=False`` with the verdict
``inconclusive`` — because "no match under an unknown salt" is not evidence of
absence. It never reports a salt-less non-match as clean.

Usage::

    from lakelogic.core.masking_diagnostics import diagnose_double_hashing

    result = diagnose_double_hashing(
        upstream=bronze_df,
        downstream=silver_df,
        column="rider_key",
        join_key="rider_id",
        salt=os.environ.get("LAKELOGIC_PII_SALT", ""),
    )
    print(result.render())
"""

from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

# ── Verdicts ─────────────────────────────────────────────────────────────────
#
# Five distinct values. `mixed` is a composite of the per-row outcomes and never
# replaces them — the counts are always the authority. `inconclusive` means the
# check could not be run properly (no salt), which is different from
# `indeterminate`, which means it WAS run and neither form matched.

VERDICT_DOUBLE_HASHED = "double_hashed"
VERDICT_CONSISTENT = "consistent"
VERDICT_INDETERMINATE = "indeterminate"
VERDICT_MIXED = "mixed"
VERDICT_INCONCLUSIVE = "inconclusive"
VERDICT_NO_OVERLAP = "no_overlap"

#: Verdicts that mean "this column is damaged, reprocess the downstream contract".
DAMAGED_VERDICTS = frozenset({VERDICT_DOUBLE_HASHED, VERDICT_MIXED})

#: Verdicts that must NOT be treated as a clean bill of health.
NOT_CLEAN_VERDICTS = frozenset(
    {VERDICT_DOUBLE_HASHED, VERDICT_MIXED, VERDICT_INDETERMINATE, VERDICT_INCONCLUSIVE, VERDICT_NO_OVERLAP}
)

#: Which salt form produced the match.
SALT_MATCH_SALTED = "salted"
SALT_MATCH_UNSALTED = "unsalted"

_REMEDIATION = (
    "SHA-256 is one-way: the double-hashed column CANNOT be repaired in place, and no "
    "tool here will pretend otherwise. The fix is to REPROCESS the downstream contract "
    "from its upstream with the masking idempotence guard in place, which rewrites the "
    "column correctly from the upstream value that is still intact."
)


def _sha256(value: str, salt: str = "") -> str:
    return hashlib.sha256(f"{salt}{value}".encode("utf-8")).hexdigest()


# ── Result ───────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class DoubleHashDiagnosis:
    """Structured verdict for one (column, join_key) pair across two layers.

    Every count is per ROW. ``verdict`` is a convenience summary derived from
    the counts; it never carries information the counts do not.
    """

    column: str
    join_key: str

    # Row accounting. joined_rows == double_hashed + consistent + indeterminate.
    upstream_rows: int
    downstream_rows: int
    joined_rows: int
    double_hashed_rows: int
    consistent_rows: int
    indeterminate_rows: int

    # Excluded from the verdict, reported separately so nothing vanishes.
    unjoined_downstream_rows: int
    unjoined_upstream_rows: int
    null_rows: int
    ambiguous_key_rows: int

    verdict: str
    salt_provided: bool
    salt_match: Optional[str]  # "salted" | "unsalted" | None
    conclusive: bool
    inconclusive_reason: Optional[str]
    remediation: str = _REMEDIATION
    sample_keys: List[Any] = field(default_factory=list)

    # ── Derived helpers ──────────────────────────────────────────────────────

    @property
    def is_damaged(self) -> bool:
        """True only when double-hashing was positively PROVEN for >=1 row."""
        return self.double_hashed_rows > 0

    @property
    def is_clean(self) -> bool:
        """True only when every joined row was proven to be a correct pass-through.

        Deliberately strict: indeterminate rows, an unusable salt, or an empty
        join all make this False. "Could not tell" is not "clean".
        """
        return (
            self.verdict == VERDICT_CONSISTENT
            and self.conclusive
            and self.joined_rows > 0
            and self.indeterminate_rows == 0
            and self.double_hashed_rows == 0
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "column": self.column,
            "join_key": self.join_key,
            "verdict": self.verdict,
            "conclusive": self.conclusive,
            "inconclusive_reason": self.inconclusive_reason,
            "salt_provided": self.salt_provided,
            "salt_match": self.salt_match,
            "upstream_rows": self.upstream_rows,
            "downstream_rows": self.downstream_rows,
            "joined_rows": self.joined_rows,
            "double_hashed_rows": self.double_hashed_rows,
            "consistent_rows": self.consistent_rows,
            "indeterminate_rows": self.indeterminate_rows,
            "unjoined_downstream_rows": self.unjoined_downstream_rows,
            "unjoined_upstream_rows": self.unjoined_upstream_rows,
            "null_rows": self.null_rows,
            "ambiguous_key_rows": self.ambiguous_key_rows,
            "sample_keys": list(self.sample_keys),
            "remediation": self.remediation,
        }

    def render(self) -> str:
        """Plain-text report. Says what was measured and what to do about it."""
        lines = [
            f"column     : {self.column}   (joined on '{self.join_key}')",
            f"verdict    : {self.verdict.upper()}" + ("" if self.conclusive else "   [NOT CONCLUSIVE]"),
            f"joined rows: {self.joined_rows}",
            f"  double-hashed : {self.double_hashed_rows}"
            + (f"   downstream == H(upstream), {self.salt_match} form" if self.salt_match else ""),
            f"  consistent    : {self.consistent_rows}   (B == S — carried through correctly)",
            f"  indeterminate : {self.indeterminate_rows}   (neither form matched — NOT clean, just unknown)",
            "excluded from the verdict:",
            f"  downstream rows with no upstream match : {self.unjoined_downstream_rows}",
            f"  upstream rows with no downstream match : {self.unjoined_upstream_rows}",
            f"  rows with a null on either side        : {self.null_rows}",
            f"  rows on an ambiguous upstream key      : {self.ambiguous_key_rows}",
            f"salt       : {'provided' if self.salt_provided else 'EMPTY/UNSET'}",
        ]
        if self.inconclusive_reason:
            lines.append(f"caveat     : {self.inconclusive_reason}")
        if self.sample_keys:
            lines.append(f"example double-hashed keys: {', '.join(str(k) for k in self.sample_keys)}")
        lines.append("")
        lines.append(_REMEDIATION)
        return "\n".join(lines)


# ── Frame extraction (engine-agnostic, same dispatch order as masking_engine) ─


def _extract_pairs(df: Any, join_key: str, column: str) -> List[Tuple[Any, Any]]:
    """Return ``[(key, value), ...]`` from any supported frame.

    Dispatch mirrors ``MaskingEngine.apply``: Polars, pandas, Spark (classic and
    Connect), then DuckDB relations. A plain list-of-dicts is also accepted so
    callers can drive this without a dataframe library at all.
    """
    if isinstance(df, (list, tuple)):
        rows = [r for r in df]
        if rows:
            _require_columns(rows[0].keys(), join_key, column, rows)
        return [(r[join_key], r[column]) for r in rows]

    try:
        import polars as pl

        if isinstance(df, pl.LazyFrame):
            df = df.collect()
        if isinstance(df, pl.DataFrame):
            _require_columns(df.columns, join_key, column, df)
            return list(zip(df[join_key].to_list(), df[column].to_list()))
    except ImportError:  # pragma: no cover - polars is a hard dep in practice
        pass

    try:
        import pandas as pd

        if isinstance(df, pd.DataFrame):
            _require_columns(list(df.columns), join_key, column, df)
            keys = df[join_key].tolist()
            vals = [None if v is None or (isinstance(v, float) and v != v) else v for v in df[column].tolist()]
            return list(zip(keys, vals))
    except ImportError:  # pragma: no cover
        pass

    # Spark — the diagnostic collects only the two columns it needs. That is a
    # real (bounded) action on the cluster and is stated plainly rather than
    # hidden.
    for mod, name in (("pyspark.sql", "DataFrame"), ("pyspark.sql.connect.dataframe", "DataFrame")):
        try:
            spark_df_cls = getattr(__import__(mod, fromlist=[name]), name)
        except Exception:  # pragma: no cover - pyspark not installed
            continue
        if isinstance(df, spark_df_cls):
            _require_columns(list(df.columns), join_key, column, df)
            return [(r[0], r[1]) for r in df.select(join_key, column).collect()]

    if hasattr(df, "fetchdf"):  # DuckDB relation
        return _extract_pairs(df.fetchdf(), join_key, column)

    raise TypeError(f"Unsupported dataframe type: {type(df)}")


def _require_columns(columns: Any, join_key: str, column: str, frame: Any) -> None:
    present = set(columns)
    missing = [c for c in (join_key, column) if c not in present]
    if missing:
        raise KeyError(f"Column(s) {missing} not present in frame of type {type(frame).__name__}")


# ── The check ────────────────────────────────────────────────────────────────


def resolve_salt(salt: Optional[str] = None) -> str:
    """Resolve the masking salt, defaulting to ``LAKELOGIC_PII_SALT``.

    Returns ``""`` when unset — callers must treat that as "cannot compute the
    salted form", not as "the salt is the empty string and all is well".
    """
    if salt is not None:
        return salt
    return os.environ.get("LAKELOGIC_PII_SALT", "") or ""


def diagnose_double_hashing(
    upstream: Any,
    downstream: Any,
    *,
    column: str,
    join_key: str,
    salt: Optional[str] = None,
    downstream_column: Optional[str] = None,
    sample_limit: int = 5,
) -> DoubleHashDiagnosis:
    """Is ``downstream[column]`` a re-hash of ``upstream[column]``?

    Parameters
    ----------
    upstream, downstream
        Frames from the two layers (Polars / pandas / Spark / DuckDB / list of
        dicts). Only ``join_key`` and ``column`` are read.
    column
        The masked column to check, as named upstream.
    join_key
        A column present and comparable in BOTH frames — the entity identity.
        It must NOT itself be the masked column, or the join is comparing the
        damage to itself.
    salt
        Masking salt. ``None`` reads ``LAKELOGIC_PII_SALT``. An empty salt makes
        the salted form uncomputable and is reported, never silently swapped for
        the unsalted form.
    downstream_column
        Downstream name for the column, if it was renamed. Defaults to ``column``.
    sample_limit
        How many example double-hashed join keys to carry back for triage.

    Returns
    -------
    DoubleHashDiagnosis
        Per-row counts plus a derived verdict. This function reads data and
        computes hashes; it writes nothing and repairs nothing (see the module
        docstring — SHA-256 double-hashing is not reversible).
    """
    if join_key == column:
        raise ValueError(
            "join_key must differ from the masked column — joining a masked column to "
            "itself cannot detect whether it was re-hashed."
        )

    down_col = downstream_column or column
    resolved_salt = resolve_salt(salt)
    salt_provided = bool(resolved_salt)

    up_pairs = _extract_pairs(upstream, join_key, column)
    down_pairs = _extract_pairs(downstream, join_key, down_col)

    # Build the upstream lookup. A key mapping to more than one DISTINCT
    # upstream value is ambiguous — we would be guessing which one downstream
    # derived from — so those keys are excluded and counted, not guessed at.
    up_index: Dict[Any, Any] = {}
    ambiguous_keys: set = set()
    for key, value in up_pairs:
        if key in up_index:
            if up_index[key] != value:
                ambiguous_keys.add(key)
        else:
            up_index[key] = value

    joined = 0
    double_hashed = 0
    consistent = 0
    indeterminate = 0
    unjoined_down = 0
    nulls = 0
    ambiguous_rows = 0
    salted_hits = 0
    unsalted_hits = 0
    samples: List[Any] = []
    matched_up_keys: set = set()

    for key, down_value in down_pairs:
        if key in ambiguous_keys:
            ambiguous_rows += 1
            continue
        if key not in up_index:
            unjoined_down += 1
            continue
        up_value = up_index[key]
        matched_up_keys.add(key)
        if up_value is None or down_value is None:
            nulls += 1
            continue

        up_str = str(up_value)
        down_str = str(down_value)
        joined += 1

        if up_str == down_str:
            # Salt-independent: the value was carried through untouched.
            consistent += 1
        elif salt_provided and _sha256(up_str, resolved_salt) == down_str:
            double_hashed += 1
            salted_hits += 1
            if len(samples) < sample_limit:
                samples.append(key)
        elif _sha256(up_str, "") == down_str:
            double_hashed += 1
            unsalted_hits += 1
            if len(samples) < sample_limit:
                samples.append(key)
        else:
            indeterminate += 1

    unjoined_up = len({k for k, _ in up_pairs} - matched_up_keys - ambiguous_keys)

    # Which salt form actually explained the double-hashing. Reported so nobody
    # has to guess whether the pipeline salt was in play.
    salt_match: Optional[str] = None
    if salted_hits and unsalted_hits:
        salt_match = f"{SALT_MATCH_SALTED}+{SALT_MATCH_UNSALTED}"
    elif salted_hits:
        salt_match = SALT_MATCH_SALTED
    elif unsalted_hits:
        salt_match = SALT_MATCH_UNSALTED

    # Verdict from the counts.
    if joined == 0:
        verdict = VERDICT_NO_OVERLAP
    elif double_hashed == joined:
        verdict = VERDICT_DOUBLE_HASHED
    elif consistent == joined:
        verdict = VERDICT_CONSISTENT
    elif indeterminate == joined:
        verdict = VERDICT_INDETERMINATE
    else:
        verdict = VERDICT_MIXED

    # Conclusiveness. An indeterminate row with no salt is not a finding — it is
    # a check we could not run, because the salted form was uncomputable. Say so
    # instead of letting it read as "no double-hashing found".
    conclusive = True
    reason: Optional[str] = None
    if joined == 0:
        conclusive = False
        reason = (
            "No rows joined between the two frames, so nothing was compared. "
            "Check that the join key identifies the same entity in both layers."
        )
    elif not salt_provided and indeterminate > 0:
        conclusive = False
        verdict = VERDICT_INCONCLUSIVE
        reason = (
            f"LAKELOGIC_PII_SALT is empty/unset, so the salted form H(salt + upstream) "
            f"could not be computed; only the unsalted form was tried. {indeterminate} row(s) "
            f"matched neither and may well BE double-hashed under the real salt. "
            f"Re-run with the pipeline's salt before concluding anything."
        )
    elif indeterminate > 0:
        reason = (
            f"{indeterminate} row(s) matched neither H(salt + upstream) nor upstream itself. "
            f"That is UNKNOWN, not clean — a different salt, a different transformation, or a "
            f"column re-sourced from elsewhere all look like this."
        )

    return DoubleHashDiagnosis(
        column=column,
        join_key=join_key,
        upstream_rows=len(up_pairs),
        downstream_rows=len(down_pairs),
        joined_rows=joined,
        double_hashed_rows=double_hashed,
        consistent_rows=consistent,
        indeterminate_rows=indeterminate,
        unjoined_downstream_rows=unjoined_down,
        unjoined_upstream_rows=unjoined_up,
        null_rows=nulls,
        ambiguous_key_rows=ambiguous_rows,
        verdict=verdict,
        salt_provided=salt_provided,
        salt_match=salt_match,
        conclusive=conclusive,
        inconclusive_reason=reason,
        sample_keys=samples,
    )
