"""``lakelogic diagnose`` — read-only checks on data that was already written.

Unlike ``validate``/``lint`` (which inspect contracts) these commands inspect
DATA, to answer questions a contract cannot: "did this pipeline already corrupt
a column before the fix landed?"

Every command here defaults to READ-ONLY. ``double-hash`` cannot repair at all
(SHA-256 is one-way — see its ``--help``). ``scd2`` can, because nothing was
destroyed there, but it still writes only when given an explicit output path.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, List, Optional

import typer

diagnose_app = typer.Typer(
    name="diagnose",
    help="Read-only diagnostics on already-written data (e.g. cross-layer double-hashed masking).",
    no_args_is_help=True,
)


def _read_frame(path: Path) -> Any:
    """Load a CSV / Parquet / JSON / NDJSON / Delta file into a Polars frame."""
    import polars as pl

    if path.is_dir():
        # A Delta table directory, or a partitioned parquet dataset.
        try:
            from lakelogic.core.delta_compat import read_delta as _read_delta

            return _read_delta(str(path))
        except Exception:
            return pl.read_parquet(str(path / "**" / "*.parquet"))

    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pl.read_parquet(path)
    if suffix == ".csv":
        return pl.read_csv(path, infer_schema_length=0)
    if suffix in (".json", ".ndjson", ".jsonl"):
        try:
            return pl.read_ndjson(path)
        except Exception:
            return pl.read_json(path)
    raise typer.BadParameter(
        f"Unsupported file type '{suffix}' for {path} (use csv, parquet, json/ndjson, or a delta dir)."
    )


@diagnose_app.command("double-hash")
def double_hash(
    upstream: Path = typer.Option(
        ...,
        "--upstream",
        "-u",
        help="Upstream (e.g. bronze) data file/dir — the layer that hashed the column FIRST.",
    ),
    downstream: Path = typer.Option(
        ...,
        "--downstream",
        "-d",
        help="Downstream (e.g. silver/gold) data file/dir — the layer suspected of re-hashing it.",
    ),
    column: str = typer.Option(..., "--column", "-c", help="The masked column to check."),
    join_key: str = typer.Option(
        ...,
        "--key",
        "-k",
        help="An unmasked column identifying the same entity in both layers. Must not be --column.",
    ),
    downstream_column: Optional[str] = typer.Option(
        None, "--downstream-column", help="Downstream name of the column, if it was renamed."
    ),
    salt: Optional[str] = typer.Option(
        None,
        "--salt",
        help="Masking salt. Defaults to $LAKELOGIC_PII_SALT. Without it the check cannot be conclusive.",
    ),
    fmt: str = typer.Option("text", "--format", "-f", help="Output format: text | json."),
    fail_on_damage: bool = typer.Option(
        False,
        "--fail-on-damage",
        help="Exit non-zero if any row is proven double-hashed (for CI).",
    ),
) -> None:
    """Detect whether a masked column was hashed AGAIN downstream.

    Masking is applied write-side per contract, and the silver/gold templates
    propagate ``masking:``. A field hashed in bronze could therefore be hashed
    again in silver — ``sha256(salt + sha256(salt + value))`` — silently, so the
    same person's key stopped matching across layers and cross-layer joins
    returned nothing.

    The check: if upstream holds ``B`` and downstream holds ``S``, downstream is
    double-hashed iff ``H(salt + B) == S``.

    THIS DOES NOT REPAIR DATA. SHA-256 is one-way; a double-hashed column cannot
    be recovered from itself, here or anywhere. The fix it points at is to
    REPROCESS the downstream contract from its (still intact) upstream, with the
    masking idempotence guard in place. That is a normal pipeline run.

    Results are per ROW, never a single boolean — a backfill that straddled the
    fix leaves a column part-damaged, and a column-level verdict would hide it.

    Examples:

        lakelogic diagnose double-hash -u bronze/riders.parquet -d silver/riders.parquet \\
            --column rider_key --key rider_id

        lakelogic diagnose double-hash -u bronze/ -d gold/ -c email_hash -k rider_id -f json
    """
    from lakelogic.core.masking_diagnostics import (
        DAMAGED_VERDICTS,
        diagnose_double_hashing,
    )

    for label, p in (("--upstream", upstream), ("--downstream", downstream)):
        if not p.exists():
            typer.secho(f"✗  {label} not found: {p}", fg=typer.colors.RED, err=True)
            raise typer.Exit(code=1)

    try:
        up_df = _read_frame(upstream)
        down_df = _read_frame(downstream)
        result = diagnose_double_hashing(
            up_df,
            down_df,
            column=column,
            join_key=join_key,
            salt=salt,
            downstream_column=downstream_column,
        )
    except (ValueError, KeyError, TypeError) as exc:
        typer.secho(f"✗  {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    if fmt == "json":
        typer.echo(json.dumps(result.to_dict(), indent=2, default=str))
    else:
        _render(result)

    if fail_on_damage and result.verdict in DAMAGED_VERDICTS:
        raise typer.Exit(code=1)
    raise typer.Exit(code=0)


def _write_frame(df: Any, path: Path) -> None:
    """Write a Polars frame back out, matching the suffix of ``path``."""
    suffix = path.suffix.lower()
    path.parent.mkdir(parents=True, exist_ok=True)
    if suffix == ".parquet":
        df.write_parquet(path)
    elif suffix == ".csv":
        df.write_csv(path)
    elif suffix in (".json", ".ndjson", ".jsonl"):
        df.write_ndjson(path)
    else:
        raise typer.BadParameter(f"Unsupported output type '{suffix}' for {path} (use csv, parquet, or ndjson).")


@diagnose_app.command("scd2")
def scd2(
    table: Path = typer.Option(..., "--table", "-t", help="The SCD2 dimension data file/dir to inspect."),
    primary_key: List[str] = typer.Option(
        ...,
        "--key",
        "-k",
        help="Business key column (repeat for a composite key). NOT the surrogate key.",
    ),
    effective_from: str = typer.Option(
        "effective_from", "--effective-from", help="Version-start column. NEVER modified."
    ),
    effective_to: str = typer.Option("effective_to", "--effective-to", help="Version-end column."),
    current_flag: str = typer.Option("is_current", "--current-flag", help="Boolean live-row column."),
    open_value: Optional[str] = typer.Option(
        "9999-12-31",
        "--open-value",
        help="Open-interval sentinel for effective_to (scd2_cfg.effective_to_default).",
    ),
    repair_out: Optional[Path] = typer.Option(
        None,
        "--repair-out",
        help="EXPLICIT opt-in to writing: path for the repaired copy. Omit for a read-only diagnosis.",
    ),
    allow_in_place: bool = typer.Option(
        False, "--allow-in-place", help="Permit --repair-out to overwrite the source table."
    ),
    allow_spark_collect: bool = typer.Option(
        False, "--allow-spark-collect", help="Consent to collecting a Spark dimension to the driver."
    ),
    fmt: str = typer.Option("text", "--format", "-f", help="Output format: text | json."),
    fail_on_defect: bool = typer.Option(
        False, "--fail-on-defect", help="Exit non-zero if any defect is found (for CI)."
    ),
) -> None:
    """Find intervals corrupted by late-arriving SCD2 rows, and optionally repair them.

    Before the late-arrival fix, a change date landing inside an already-closed
    interval was appended instead of slotted in, producing an inverted interval
    (``effective_to`` before ``effective_from``), overlapping versions, and
    ``is_current`` sitting on an older fact.

    THIS ONE IS REPAIRABLE. Every ``effective_from`` survived the bug, and the
    correct intervals follow from ordering a key's versions by it: a version ends
    where the next begins, and the latest holds ``is_current``.

    ``effective_from`` IS NEVER MODIFIED, so the surrogate key
    ``sha256(pk | effective_from)`` is unchanged and facts already holding an SK
    still resolve. ``_version`` is derived from that same ordering, so it is left
    alone too. Only ``effective_to`` and the current-flag column are rewritten.

    GAPS ARE NOT CLOSED. A record deleted and re-added legitimately leaves
    ``effective_to`` before the next version's ``effective_from``; closing that
    would destroy real history. Only inverted intervals, true overlaps, and a
    misplaced current flag are touched. Keys that cannot be ordered — two versions
    sharing an ``effective_from``, or an unparseable boundary — are reported as
    ``unrepairable`` and left completely untouched rather than guessed at.

    Read-only unless you pass ``--repair-out``.

    Examples:

        lakelogic diagnose scd2 -t gold/dim_driver.parquet -k driver_id

        lakelogic diagnose scd2 -t gold/dim_driver.parquet -k driver_id \\
            --repair-out gold/dim_driver.repaired.parquet
    """
    from lakelogic.core.scd2_diagnostics import (
        SAFETY_NOTE,
        Scd2SparkCollectRequired,
        diagnose_scd2,
    )

    if not table.exists():
        typer.secho(f"✗  --table not found: {table}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    if repair_out is not None and repair_out.resolve() == table.resolve() and not allow_in_place:
        typer.secho(
            "✗  --repair-out points at the source table. Pass --allow-in-place if you really "
            "mean to overwrite it (there is no undo).",
            fg=typer.colors.RED,
            err=True,
        )
        raise typer.Exit(code=1)

    try:
        df = _read_frame(table)
        result = diagnose_scd2(
            df,
            primary_key=list(primary_key),
            effective_from_field=effective_from,
            effective_to_field=effective_to,
            current_flag_field=current_flag,
            effective_to_default=open_value,
            repair=repair_out is not None,
            allow_collect=allow_spark_collect,
        )
    except (ValueError, KeyError, TypeError, Scd2SparkCollectRequired) as exc:
        typer.secho(f"✗  {exc}", fg=typer.colors.RED, err=True)
        raise typer.Exit(code=1)

    if fmt == "json":
        typer.echo(json.dumps(result.to_dict(), indent=2, default=str))
    else:
        _render_scd2(result)

    if repair_out is not None:
        _write_frame(result.repaired_frame, repair_out)
        if fmt != "json":
            typer.echo(
                typer.style(
                    f"  ✎  repaired copy written to {repair_out} "
                    f"({result.rows_changed} row(s) rewritten). {SAFETY_NOTE}",
                    fg=typer.colors.CYAN,
                )
            )
            typer.echo("")

    if fail_on_defect and result.total_defects > 0:
        raise typer.Exit(code=1)
    raise typer.Exit(code=0)


def _render_scd2(result: Any) -> None:
    """Text report for the SCD2 interval diagnosis."""
    from lakelogic.core.scd2_diagnostics import (
        CONSERVATISM_NOTE,
        DEFECT_INVERTED,
        DEFECT_IS_CURRENT_WRONG,
        DEFECT_OVERLAPPING,
        DEFECT_UNREPAIRABLE,
        SAFETY_NOTE,
    )

    typer.echo("")
    typer.echo(typer.style("  SCD2 interval diagnosis", bold=True))
    typer.echo("  " + "═" * 62)
    typer.echo(f"  primary key   : {typer.style(', '.join(result.primary_key), bold=True)}")
    typer.echo(
        f"  columns       : {result.effective_from_field} / {result.effective_to_field} / {result.current_flag_field}"
    )
    typer.echo(f"  inspected     : {result.rows_inspected} rows across {result.keys_inspected} keys")
    typer.echo("")
    typer.echo(typer.style("  Defects:", bold=True))
    rows = (
        (DEFECT_INVERTED, result.inverted_rows, "rows", "effective_to < effective_from"),
        (DEFECT_OVERLAPPING, result.overlapping_rows, "rows", "effective_to > next version's effective_from"),
        (
            DEFECT_IS_CURRENT_WRONG,
            result.is_current_wrong_rows,
            "rows",
            "flag not on the single latest open version",
        ),
        (DEFECT_UNREPAIRABLE, result.unrepairable_keys, "KEYS", "cannot be ordered — reported, never guessed"),
    )
    for name, count, unit, blurb in rows:
        color = typer.colors.RED if count and name != DEFECT_UNREPAIRABLE else typer.colors.YELLOW
        line = f"    {name:<17}: {count:>8} {unit}"
        typer.echo((typer.style(line, fg=color) if count else line) + f"   {blurb}")

    typer.echo("")
    typer.echo(typer.style("  Left alone on purpose:", dim=True))
    typer.echo(
        f"    gap boundaries   : {result.gap_boundaries_preserved:>8}"
        "   deleted-then-re-added — REAL history, never closed"
    )
    typer.echo(f"    contiguous       : {result.contiguous_boundaries:>8}   to == next from, already correct")
    typer.echo("")
    typer.echo(f"  keys with defects : {result.keys_with_defects}   (healthy: {result.healthy_keys})")
    typer.echo(f"  rows repair would rewrite : {result.rows_changed}")

    for name in (DEFECT_INVERTED, DEFECT_OVERLAPPING, DEFECT_IS_CURRENT_WRONG, DEFECT_UNREPAIRABLE):
        examples = result.example_keys.get(name) or []
        if examples:
            rendered = ", ".join("|".join(str(p) for p in k) if isinstance(k, tuple) else str(k) for k in examples)
            typer.echo(f"    example {name} keys: {rendered}")

    if result.unrepairable:
        typer.echo("")
        typer.echo(typer.style("  Unrepairable keys (left completely untouched):", fg=typer.colors.YELLOW, bold=True))
        for item in result.unrepairable:
            key = item["key"]
            rendered = "|".join(str(p) for p in key) if isinstance(key, tuple) else str(key)
            typer.echo(f"    {rendered}: {item['reason']} — {item['detail']}")

    for note in result.notes:
        typer.echo("")
        typer.echo(typer.style(f"  ⚠  {note}", fg=typer.colors.YELLOW))

    typer.echo("")
    if result.is_corrupted:
        typer.echo(
            typer.style("  ✖  This dimension contains corrupted SCD2 intervals.", fg=typer.colors.RED, bold=True)
        )
    elif result.unrepairable:
        typer.echo(
            typer.style(
                "  ?  No repairable defect found, but some keys could not be ordered — not a clean bill of health.",
                fg=typer.colors.YELLOW,
                bold=True,
            )
        )
    else:
        typer.echo(
            typer.style(
                "  ✔  Every key's history is ordered, non-overlapping and correctly flagged.",
                fg=typer.colors.GREEN,
                bold=True,
            )
        )

    typer.echo("")
    if not result.repair_requested:
        typer.echo(
            typer.style("  Read-only diagnosis — nothing was written. Pass --repair-out PATH to repair.", dim=True)
        )
    typer.echo(typer.style(f"  {SAFETY_NOTE}", dim=True))
    typer.echo(typer.style(f"  {CONSERVATISM_NOTE}", dim=True))
    typer.echo("")


def _render(result: Any) -> None:
    """Text report. Leads with the column and the counts, ends with the fix."""
    from lakelogic.core.masking_diagnostics import (
        VERDICT_CONSISTENT,
        VERDICT_DOUBLE_HASHED,
        VERDICT_INCONCLUSIVE,
        VERDICT_INDETERMINATE,
        VERDICT_MIXED,
        VERDICT_NO_OVERLAP,
    )

    colors = {
        VERDICT_DOUBLE_HASHED: typer.colors.RED,
        VERDICT_MIXED: typer.colors.RED,
        VERDICT_CONSISTENT: typer.colors.GREEN,
        VERDICT_INDETERMINATE: typer.colors.YELLOW,
        VERDICT_INCONCLUSIVE: typer.colors.YELLOW,
        VERDICT_NO_OVERLAP: typer.colors.YELLOW,
    }
    color = colors.get(result.verdict, typer.colors.YELLOW)

    typer.echo("")
    typer.echo(typer.style("  Masking double-hash diagnosis", bold=True))
    typer.echo("  " + "═" * 62)
    typer.echo(f"  column        : {typer.style(result.column, bold=True)}   (joined on '{result.join_key}')")
    typer.echo(
        "  verdict       : "
        + typer.style(result.verdict.upper(), fg=color, bold=True)
        + ("" if result.conclusive else typer.style("   [NOT CONCLUSIVE]", fg=typer.colors.YELLOW, bold=True))
    )
    typer.echo(f"  salt          : {'provided' if result.salt_provided else 'EMPTY/UNSET'}")
    typer.echo("")
    typer.echo(typer.style(f"  Joined rows: {result.joined_rows}", bold=True))
    typer.echo(
        typer.style(f"    double-hashed : {result.double_hashed_rows:>8}", fg=typer.colors.RED)
        + (f"   downstream == H(upstream), {result.salt_match} form" if result.salt_match else "")
    )
    typer.echo(
        typer.style(f"    consistent    : {result.consistent_rows:>8}", fg=typer.colors.GREEN)
        + "   upstream == downstream (carried through correctly)"
    )
    typer.echo(
        typer.style(f"    indeterminate : {result.indeterminate_rows:>8}", fg=typer.colors.YELLOW)
        + "   neither matched — UNKNOWN, not clean"
    )
    typer.echo("")
    typer.echo(typer.style("  Excluded from the verdict:", dim=True))
    typer.echo(f"    downstream rows with no upstream match : {result.unjoined_downstream_rows}")
    typer.echo(f"    upstream rows with no downstream match : {result.unjoined_upstream_rows}")
    typer.echo(f"    rows with a null on either side        : {result.null_rows}")
    typer.echo(f"    rows on an ambiguous upstream key      : {result.ambiguous_key_rows}")

    if result.sample_keys:
        typer.echo("")
        typer.echo(f"  Example double-hashed keys: {', '.join(str(k) for k in result.sample_keys)}")

    if result.inconclusive_reason:
        typer.echo("")
        typer.echo(typer.style(f"  ⚠  {result.inconclusive_reason}", fg=typer.colors.YELLOW))

    typer.echo("")
    if result.is_damaged:
        typer.echo(typer.style(f"  ✖  '{result.column}' IS double-hashed downstream.", fg=typer.colors.RED, bold=True))
    elif result.is_clean:
        typer.echo(
            typer.style(
                f"  ✔  '{result.column}' was carried through correctly on every joined row.",
                fg=typer.colors.GREEN,
                bold=True,
            )
        )
    else:
        typer.echo(
            typer.style(
                f"  ?  No double-hashing PROVEN for '{result.column}' — and this is not a clean bill of health.",
                fg=typer.colors.YELLOW,
                bold=True,
            )
        )
    typer.echo("")
    typer.echo(typer.style("  How to fix it:", bold=True))
    typer.echo(typer.style(f"    {result.remediation}", dim=True))
    typer.echo("")
