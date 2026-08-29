"""Diagnose — and conservatively repair — SCD2 history corrupted by late arrivals.

WHAT THIS IS FOR
----------------
``_scd2_frames`` (and its Spark twin ``_spark_scd2_late_arrivals``) assumed
chronological arrival. A row whose change date fell INSIDE an already-closed
interval was appended to the end instead of slotted into history, producing three
corruptions at once::

    d1 equire   2024-01-01 -> 2024-01-04  is_current=False
    d1 notified 2024-01-03 -> 9999-12-31  is_current=True    <- late row became current
    d1 serius   2024-01-04 -> 2024-01-03  is_current=False   <- effective_to < effective_from

plus overlapping windows (``equire`` and ``notified`` both valid on 3 Jan).

Materialization now slots late rows in correctly, so NEW writes cannot corrupt a
dimension this way. This module is about tables that were already written.

WHY THIS ONE **IS** REPAIRABLE
------------------------------
Unlike the double-hash case (see :mod:`lakelogic.core.masking_diagnostics`, where
SHA-256 destroyed the information), nothing here was destroyed. Every
``effective_from`` is intact — the bug never mutated it — and ``effective_from`` is
the input to the surrogate key ``sha256(pk | effective_from)``. So the correct
intervals are DERIVABLE: order a key's versions by ``effective_from``; a version's
``effective_to`` follows from the next version's start; ``is_current`` belongs to
the latest.

**The repair therefore never changes ``effective_from``.** Doing so would renumber
every surrogate key and orphan the facts holding them. Only ``effective_to`` and
the current-flag column are ever rewritten. ``_version`` is likewise safe to leave
alone: materialization derives it by ranking on ``effective_from`` within the key,
and repair does not reorder anything, so the stored version numbers are already
correct even in a corrupted table.

CONSERVATISM — WHAT IS **NOT** REPAIRED
---------------------------------------
A GAP is legitimate SCD2. A record deleted and later re-added leaves
``effective_to`` strictly BEFORE the next version's ``effective_from``, on purpose.
Closing gaps would destroy real history, which is worse than the bug. So only
demonstrably broken shapes are touched:

``inverted``
    ``effective_to < effective_from`` on a row. Never valid.

``overlapping``
    A row's ``effective_to`` is strictly GREATER than the next version's
    ``effective_from`` — two versions of one key valid at the same instant.

``is_current_wrong``
    The flag is set on a row that is not the latest by ``effective_from``, or on
    more than one row, or on none while the latest row is open-ended.

``contiguous`` (``to == next from``) and ``gapped`` (``to < next from``) are LEFT
ALONE. They are counted and reported, so the report shows how much history was
deliberately preserved.

AMBIGUITY IS REPORTED, NEVER GUESSED
------------------------------------
Two shapes make a key impossible to reason about without inventing facts:

* two versions of one key sharing the same ``effective_from`` — there is no way to
  say which came first;
* a boundary that cannot be parsed as a date/time — the key cannot be ordered.

Such keys are reported as ``unrepairable`` with the reason and are left completely
untouched by repair. Row-local ``inverted`` findings on them are still counted
(that check needs no ordering), but nothing about them is rewritten.

DIAGNOSE AND REPAIR ARE SEPARATE
--------------------------------
The default is diagnose-only, and it returns ``repaired_frame is None``. A caller
must pass ``repair=True`` to get a frame back. Nothing is ever written to disk from
here; the CLI (``lakelogic diagnose scd2``) is read-only unless given an explicit
output path.

Usage::

    from lakelogic.core.scd2_diagnostics import diagnose_scd2

    result = diagnose_scd2(dim_df, primary_key="driver_id")
    print(result.render())

    fixed = diagnose_scd2(dim_df, primary_key="driver_id", repair=True).repaired_frame
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple

# ── Defect names ─────────────────────────────────────────────────────────────

DEFECT_INVERTED = "inverted"
DEFECT_OVERLAPPING = "overlapping"
DEFECT_IS_CURRENT_WRONG = "is_current_wrong"
DEFECT_UNREPAIRABLE = "unrepairable"

#: Every defect this module knows how to name, in report order.
DEFECT_NAMES = (DEFECT_INVERTED, DEFECT_OVERLAPPING, DEFECT_IS_CURRENT_WRONG, DEFECT_UNREPAIRABLE)

#: Reasons a key is reported unrepairable rather than guessed at.
REASON_DUPLICATE_EFFECTIVE_FROM = "duplicate_effective_from"
REASON_UNPARSEABLE_BOUNDARY = "unparseable_boundary"

SAFETY_NOTE = (
    "Repair rewrites ONLY effective_to and the current-flag column. effective_from is never "
    "modified, so the surrogate key sha256(pk | effective_from) is byte-identical afterwards and "
    "every fact row already holding an SK still resolves. _version is derived by ranking on "
    "effective_from, which repair does not reorder, so it needs no rewrite either."
)

CONSERVATISM_NOTE = (
    "Gaps are LEFT ALONE. A record deleted and re-added legitimately leaves effective_to before "
    "the next version's effective_from; closing that would destroy real history. Only inverted "
    "intervals, true overlaps, and a misplaced current flag are touched."
)


# ── Result ───────────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Scd2Diagnosis:
    """Structured verdict for one SCD2 frame.

    Counts are per ROW except ``unrepairable_keys``, which is per KEY (a key is
    unorderable as a whole, not row by row).
    """

    primary_key: List[str]
    effective_from_field: str
    effective_to_field: str
    current_flag_field: str

    rows_inspected: int
    keys_inspected: int

    inverted_rows: int
    overlapping_rows: int
    is_current_wrong_rows: int
    unrepairable_keys: int

    # Deliberately preserved shapes — proof the repair did not eat real history.
    gap_boundaries_preserved: int
    contiguous_boundaries: int

    keys_with_defects: int
    healthy_keys: int
    rows_changed: int  # rows repair would rewrite (computed whether or not repair was asked for)

    example_keys: Dict[str, List[Any]]
    unrepairable: List[Dict[str, Any]]

    repair_requested: bool
    repaired_frame: Optional[Any] = None
    notes: List[str] = field(default_factory=list)

    # ── Derived helpers ──────────────────────────────────────────────────────

    @property
    def defect_counts(self) -> Dict[str, int]:
        return {
            DEFECT_INVERTED: self.inverted_rows,
            DEFECT_OVERLAPPING: self.overlapping_rows,
            DEFECT_IS_CURRENT_WRONG: self.is_current_wrong_rows,
            DEFECT_UNREPAIRABLE: self.unrepairable_keys,
        }

    @property
    def total_defects(self) -> int:
        return sum(self.defect_counts.values())

    @property
    def is_corrupted(self) -> bool:
        """True when at least one repairable defect was PROVEN."""
        return (self.inverted_rows + self.overlapping_rows + self.is_current_wrong_rows) > 0

    @property
    def is_clean(self) -> bool:
        """True only when nothing was found AND nothing was ambiguous."""
        return self.total_defects == 0 and self.rows_inspected > 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "primary_key": list(self.primary_key),
            "effective_from_field": self.effective_from_field,
            "effective_to_field": self.effective_to_field,
            "current_flag_field": self.current_flag_field,
            "rows_inspected": self.rows_inspected,
            "keys_inspected": self.keys_inspected,
            "defects": self.defect_counts,
            "gap_boundaries_preserved": self.gap_boundaries_preserved,
            "contiguous_boundaries": self.contiguous_boundaries,
            "keys_with_defects": self.keys_with_defects,
            "healthy_keys": self.healthy_keys,
            "rows_changed": self.rows_changed,
            "example_keys": {k: [_key_str(v) for v in vs] for k, vs in self.example_keys.items()},
            "unrepairable": [{**u, "key": _key_str(u["key"])} for u in self.unrepairable],
            "repair_requested": self.repair_requested,
            "repair_returned": self.repaired_frame is not None,
            "notes": list(self.notes),
            "safety": SAFETY_NOTE,
            "conservatism": CONSERVATISM_NOTE,
        }

    def render(self) -> str:
        """Plain-text report."""
        lines = [
            f"primary key : {', '.join(self.primary_key)}",
            f"columns     : {self.effective_from_field} / {self.effective_to_field} / {self.current_flag_field}",
            f"inspected   : {self.rows_inspected} rows across {self.keys_inspected} keys",
            "",
            "defects:",
            f"  inverted         : {self.inverted_rows:>8}   (effective_to < effective_from)",
            f"  overlapping      : {self.overlapping_rows:>8}   (effective_to > next version's effective_from)",
            f"  is_current_wrong : {self.is_current_wrong_rows:>8}   (flag not on the single latest open version)",
            f"  unrepairable     : {self.unrepairable_keys:>8}   KEYS — reported, never guessed at",
            "",
            "left alone on purpose:",
            f"  gap boundaries   : {self.gap_boundaries_preserved:>8}   (deleted-then-re-added: REAL history)",
            f"  contiguous       : {self.contiguous_boundaries:>8}   (to == next from: already correct)",
            "",
            f"keys with defects : {self.keys_with_defects}   (healthy: {self.healthy_keys})",
            f"rows repair would rewrite : {self.rows_changed}",
        ]
        for defect in DEFECT_NAMES:
            examples = self.example_keys.get(defect) or []
            if examples:
                lines.append(f"  example {defect} keys: {', '.join(_key_str(k) for k in examples)}")
        if self.unrepairable:
            lines.append("")
            lines.append("unrepairable keys (left untouched):")
            for item in self.unrepairable:
                lines.append(f"  {_key_str(item['key'])}: {item['reason']} — {item['detail']}")
        for note in self.notes:
            lines.append(f"note        : {note}")
        lines.append("")
        lines.append(
            "repair      : "
            + (
                "frame returned"
                if self.repaired_frame is not None
                else "NOT requested — this was a read-only diagnosis"
            )
        )
        lines.append("")
        lines.append(SAFETY_NOTE)
        lines.append(CONSERVATISM_NOTE)
        return "\n".join(lines)


def _key_str(key: Any) -> str:
    if isinstance(key, tuple):
        return "|".join(str(k) for k in key)
    return str(key)


# ── Boundary parsing ─────────────────────────────────────────────────────────

_NULL_SENTINELS = {"", "none", "null", "nat", "nan", "n/a"}

_FALLBACK_FORMATS = ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%d %H:%M:%S", "%Y%m%d")


class _Unparseable:
    """Distinct from ``None``: ``None`` means NULL (an open interval)."""

    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return "<unparseable>"


UNPARSEABLE = _Unparseable()


def _parse_boundary(value: Any) -> Any:
    """Return a naive ``datetime``, ``None`` for NULL, or ``UNPARSEABLE``.

    Deliberately conservative: only unambiguous formats are accepted. Anything
    else comes back ``UNPARSEABLE`` and makes its key unrepairable, rather than
    being coerced into an ordering that was never in the data.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return UNPARSEABLE
    # pandas NaT / NaN and numpy nan all fail self-equality.
    if value != value:  # noqa: PLR0124
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo is not None else value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)

    text = str(value).strip()
    if text.lower() in _NULL_SENTINELS:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    # Trim sub-microsecond precision (numpy datetime64 stringifies to nanoseconds).
    if "." in text:
        head, _, tail = text.partition(".")
        digits = ""
        for char in tail:
            if char.isdigit():
                digits += char
            else:
                break
        if digits:
            text = f"{head}.{digits[:6]}{tail[len(digits) :]}"
    try:
        parsed = datetime.fromisoformat(text)
        return parsed.replace(tzinfo=None) if parsed.tzinfo is not None else parsed
    except ValueError:
        pass
    for fmt in _FALLBACK_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return UNPARSEABLE


_TRUE_STRINGS = {"true", "t", "yes", "y", "1"}


def _as_bool(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if value != value:  # NaN/NaT  # noqa: PLR0124
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in _TRUE_STRINGS


# ── Frame access (engine-agnostic; dispatch mirrors masking_diagnostics) ──────


def _extract_columns(df: Any, columns: Sequence[str], *, allow_collect: bool) -> Tuple[str, int, Dict[str, List[Any]]]:
    """Return ``(kind, n_rows, {column: values})`` for any supported frame."""
    if isinstance(df, (list, tuple)):
        rows = list(df)
        if rows:
            _require_columns(rows[0].keys(), columns, df)
        return "records", len(rows), {c: [r.get(c) for r in rows] for c in columns}

    try:
        import polars as pl

        if isinstance(df, pl.LazyFrame):
            df = df.collect()
        if isinstance(df, pl.DataFrame):
            _require_columns(df.columns, columns, df)
            return "polars", df.height, {c: df[c].to_list() for c in columns}
    except ImportError:  # pragma: no cover - polars is a hard dep in practice
        pass

    try:
        import pandas as pd

        if isinstance(df, pd.DataFrame):
            _require_columns(list(df.columns), columns, df)
            return "pandas", len(df), {c: df[c].tolist() for c in columns}
    except ImportError:  # pragma: no cover
        pass

    for mod, name in (("pyspark.sql", "DataFrame"), ("pyspark.sql.connect.dataframe", "DataFrame")):
        try:
            spark_df_cls = getattr(__import__(mod, fromlist=[name]), name)
        except Exception:  # pragma: no cover - pyspark not installed
            continue
        if isinstance(df, spark_df_cls):
            if not allow_collect:
                raise Scd2SparkCollectRequired(
                    "SCD2 diagnosis needs every version of every key in one place to order them, "
                    "which on Spark means a FULL COLLECT of the dimension to the driver. That is "
                    "not done implicitly: it can OOM the driver on a large table. Either filter to "
                    "the keys you are investigating first, or pass allow_collect=True (CLI: "
                    "--allow-spark-collect) to consent. Note that with allow_collect=True a "
                    "repaired frame comes back as a pandas DataFrame, not a Spark DataFrame."
                )
            _require_columns(list(df.columns), columns, df)
            return _extract_columns(df.toPandas(), columns, allow_collect=allow_collect)

    if hasattr(df, "fetchdf"):  # DuckDB relation
        return _extract_columns(df.fetchdf(), columns, allow_collect=allow_collect)

    raise TypeError(f"Unsupported dataframe type: {type(df)}")


def _require_columns(present: Any, columns: Sequence[str], frame: Any) -> None:
    have = set(present)
    missing = [c for c in columns if c not in have]
    if missing:
        raise KeyError(f"Column(s) {missing} not present in frame of type {type(frame).__name__}")


class Scd2SparkCollectRequired(RuntimeError):
    """Raised instead of silently collecting a Spark dimension to the driver."""


def _spark_source(df: Any) -> bool:
    for mod, name in (("pyspark.sql", "DataFrame"), ("pyspark.sql.connect.dataframe", "DataFrame")):
        try:
            spark_df_cls = getattr(__import__(mod, fromlist=[name]), name)
        except Exception:  # pragma: no cover
            continue
        if isinstance(df, spark_df_cls):
            return True
    return False


def _write_back(df: Any, kind: str, updates: Dict[str, List[Any]]) -> Any:
    """Return a copy of ``df`` with ``updates`` (column -> full value list) applied."""
    if kind == "records":
        rows = [dict(r) for r in df]
        for column, values in updates.items():
            for row, value in zip(rows, values):
                row[column] = value
        return rows

    if kind == "polars":
        import polars as pl

        if isinstance(df, pl.LazyFrame):
            df = df.collect()
        series = []
        for column, values in updates.items():
            dtype = df.schema.get(column)
            try:
                series.append(pl.Series(column, values, dtype=dtype))
            except Exception:
                series.append(pl.Series(column, values, strict=False))
        return df.with_columns(series)

    if kind == "pandas":
        out = df.copy()
        for column, values in updates.items():
            try:
                out[column] = values
            except (TypeError, ValueError):  # pragma: no cover - dtype refusal
                out[column] = out[column].astype(object)
                out[column] = values
        return out

    raise TypeError(f"Cannot write back to frame kind {kind!r}")  # pragma: no cover


def _pick_flag_literals(values: Sequence[Any]) -> Tuple[Any, Any]:
    """Reuse the column's own truthy/falsy literals so repair does not change dtype."""
    true_val: Any = True
    false_val: Any = False
    for value in values:
        if isinstance(value, bool) or value is None:
            continue
        if value != value:  # NaN  # noqa: PLR0124
            continue
        if _as_bool(value):
            true_val = value
        else:
            false_val = value
    return true_val, false_val


# ── The check ────────────────────────────────────────────────────────────────


def diagnose_scd2(
    frame: Any,
    *,
    primary_key: Any,
    effective_from_field: str = "effective_from",
    effective_to_field: str = "effective_to",
    current_flag_field: str = "is_current",
    effective_to_default: Optional[str] = "9999-12-31",
    repair: bool = False,
    sample_limit: int = 5,
    allow_collect: bool = False,
) -> Scd2Diagnosis:
    """Find (and optionally repair) intervals corrupted by late-arriving SCD2 rows.

    Parameters
    ----------
    frame
        The dimension table (Polars / pandas / DuckDB relation / list of dicts).
        A Spark DataFrame requires ``allow_collect=True`` — see
        :class:`Scd2SparkCollectRequired`.
    primary_key
        Business key column, or list of columns. NOT the surrogate key: history
        is ordered *within* a business key.
    effective_from_field, effective_to_field, current_flag_field
        The SCD2 control column names, matching ``scd2_cfg``.
    effective_to_default
        The open-interval sentinel (``scd2_cfg["effective_to_default"]``). ``None``
        means the convention is a NULL end date.
    repair
        ``False`` (default) diagnoses only and returns ``repaired_frame=None``.
        ``True`` also returns a repaired copy. The input frame is never mutated.
    sample_limit
        How many example keys to carry back per defect.

    Returns
    -------
    Scd2Diagnosis
        Per-defect counts, example keys, ambiguous keys with reasons, and — only
        when asked — the repaired frame. ``effective_from`` is never written.
    """
    pk_cols: List[str] = [primary_key] if isinstance(primary_key, str) else list(primary_key)
    if not pk_cols:
        raise ValueError("primary_key is required — SCD2 history is only orderable within a key.")
    for control in (effective_from_field, effective_to_field, current_flag_field):
        if control in pk_cols:
            raise ValueError(
                f"primary_key must not include the SCD2 control column {control!r} — the key "
                "identifies the entity, the control columns describe its versions."
            )

    wanted = pk_cols + [effective_from_field, effective_to_field, current_flag_field]
    spark_input = _spark_source(frame)
    kind, n_rows, cols = _extract_columns(frame, wanted, allow_collect=allow_collect)

    ef_raw = cols[effective_from_field]
    et_raw = cols[effective_to_field]
    cf_raw = cols[current_flag_field]

    open_sentinel = _parse_boundary(effective_to_default) if effective_to_default is not None else None
    if open_sentinel is UNPARSEABLE:
        raise ValueError(f"effective_to_default {effective_to_default!r} is not a parseable date/time.")

    def _is_open(parsed: Any) -> bool:
        if parsed is None:
            return effective_to_default is None or open_sentinel is None
        if parsed is UNPARSEABLE:
            return False
        return open_sentinel is not None and parsed >= open_sentinel

    # A repaired open interval reuses a raw sentinel already present in the column,
    # so the written value keeps the column's own type and formatting.
    open_raw: Any = effective_to_default
    for raw in et_raw:
        parsed = _parse_boundary(raw)
        if parsed is not UNPARSEABLE and _is_open(parsed):
            open_raw = raw
            break

    true_literal, false_literal = _pick_flag_literals(cf_raw)

    # Group row positions by business key, preserving frame order.
    groups: Dict[Any, List[int]] = {}
    for i in range(n_rows):
        key = tuple(cols[c][i] for c in pk_cols)
        key_value = key[0] if len(key) == 1 else key
        groups.setdefault(key_value, []).append(i)

    new_et = list(et_raw)
    new_cf = list(cf_raw)

    inverted_rows = 0
    overlapping_rows = 0
    is_current_wrong_rows = 0
    gap_boundaries = 0
    contiguous_boundaries = 0
    keys_with_defects = 0
    changed_positions: set = set()
    examples: Dict[str, List[Any]] = {name: [] for name in DEFECT_NAMES}
    unrepairable: List[Dict[str, Any]] = []

    def _example(defect: str, key: Any) -> None:
        bucket = examples[defect]
        if key not in bucket and len(bucket) < sample_limit:
            bucket.append(key)

    for key_value, positions in groups.items():
        parsed_from = {i: _parse_boundary(ef_raw[i]) for i in positions}
        parsed_to = {i: _parse_boundary(et_raw[i]) for i in positions}

        # ── Ambiguity gates: report, never guess ────────────────────────────
        bad_boundary = [
            i
            for i in positions
            if parsed_from[i] is UNPARSEABLE or parsed_from[i] is None or parsed_to[i] is UNPARSEABLE
        ]
        starts = [parsed_from[i] for i in positions if isinstance(parsed_from[i], datetime)]
        duplicate_starts = len(starts) != len(set(starts))

        if bad_boundary or duplicate_starts:
            reason = REASON_UNPARSEABLE_BOUNDARY if bad_boundary else REASON_DUPLICATE_EFFECTIVE_FROM
            detail = (
                f"{len(bad_boundary)} row(s) have an effective_from/effective_to that is null or "
                f"cannot be parsed as a date, so this key cannot be ordered"
                if bad_boundary
                else "two or more versions share the same effective_from, so their order cannot be "
                "determined without inventing one"
            )
            unrepairable.append({"key": key_value, "reason": reason, "detail": detail})
            _example(DEFECT_UNREPAIRABLE, key_value)
            # An inverted interval is row-local and needs no ordering, so it is still
            # reported — but nothing on this key is rewritten.
            for i in positions:
                pf, pt = parsed_from[i], parsed_to[i]
                if isinstance(pf, datetime) and isinstance(pt, datetime) and pt < pf:
                    inverted_rows += 1
                    _example(DEFECT_INVERTED, key_value)
            continue

        ordered = sorted(positions, key=lambda i: parsed_from[i])
        key_defective = False

        for rank, i in enumerate(ordered):
            start = parsed_from[i]
            end = parsed_to[i]
            is_last = rank == len(ordered) - 1
            next_start = None if is_last else parsed_from[ordered[rank + 1]]

            inverted = end is not None and end < start
            overlapping = (not inverted) and next_start is not None and end is not None and end > next_start

            if inverted:
                inverted_rows += 1
                key_defective = True
                _example(DEFECT_INVERTED, key_value)
            elif overlapping:
                overlapping_rows += 1
                key_defective = True
                _example(DEFECT_OVERLAPPING, key_value)
            elif next_start is not None and end is not None:
                # end <= next_start: either contiguous (correct) or a gap (REAL
                # history — a delete followed by a re-add). Both left alone.
                if end == next_start:
                    contiguous_boundaries += 1
                else:
                    gap_boundaries += 1

            if inverted or overlapping:
                # The only two shapes that earn a rewrite. A non-last version ends
                # where the next begins; a broken last version reopens.
                new_et[i] = ef_raw[ordered[rank + 1]] if not is_last else open_raw
                changed_positions.add(i)
                parsed_to[i] = _parse_boundary(new_et[i])

        # ── is_current, judged against the REPAIRED end dates ───────────────
        latest = ordered[-1]
        latest_open = _is_open(parsed_to[latest])
        for i in ordered:
            actual = _as_bool(cf_raw[i])
            if i == latest:
                # Only force the flag ON when the latest version is genuinely open.
                # A closed latest version (a deleted record) is left as it is —
                # that shape is not in the defect taxonomy and may be intentional.
                expected = True if latest_open else actual
            else:
                expected = False
            if expected != actual:
                is_current_wrong_rows += 1
                key_defective = True
                _example(DEFECT_IS_CURRENT_WRONG, key_value)
                new_cf[i] = true_literal if expected else false_literal
                changed_positions.add(i)

        if key_defective:
            keys_with_defects += 1

    notes: List[str] = []
    if spark_input:
        notes.append(
            "Input was a Spark DataFrame and was fully collected to the driver with your consent; "
            "any repaired frame is a pandas DataFrame."
        )
        kind = "pandas"
        frame = frame.toPandas()

    repaired_frame = None
    if repair:
        repaired_frame = _write_back(frame, kind, {effective_to_field: new_et, current_flag_field: new_cf})

    return Scd2Diagnosis(
        primary_key=pk_cols,
        effective_from_field=effective_from_field,
        effective_to_field=effective_to_field,
        current_flag_field=current_flag_field,
        rows_inspected=n_rows,
        keys_inspected=len(groups),
        inverted_rows=inverted_rows,
        overlapping_rows=overlapping_rows,
        is_current_wrong_rows=is_current_wrong_rows,
        unrepairable_keys=len(unrepairable),
        gap_boundaries_preserved=gap_boundaries,
        contiguous_boundaries=contiguous_boundaries,
        keys_with_defects=keys_with_defects,
        healthy_keys=len(groups) - keys_with_defects - len(unrepairable),
        rows_changed=len(changed_positions),
        example_keys=examples,
        unrepairable=unrepairable,
        repair_requested=repair,
        repaired_frame=repaired_frame,
        notes=notes,
    )


def repair_scd2(frame: Any, **kwargs: Any) -> Any:
    """Convenience wrapper: return the repaired frame, discarding the diagnosis.

    Prefer :func:`diagnose_scd2` — the counts it returns are the evidence that the
    repair did what you expected, and this wrapper throws them away.
    """
    kwargs["repair"] = True
    return diagnose_scd2(frame, **kwargs).repaired_frame
