"""Detecting a column that was hashed AGAIN downstream.

Masking is write-side and per contract, and the silver/gold templates propagate
`masking:`, so a field hashed in bronze was hashed again in silver —
sha256(salt + sha256(salt + value)) — silently, breaking every cross-layer join
on that key. `masking_engine`'s idempotence guard stops NEW double-hashing, but
data written before it stays broken and nothing detected it.

SHA-256 is one-way, so there is no in-place repair and these tests do not test
for one. What IS true is that the condition is DETECTABLE: if bronze holds
B = H(x) and silver holds S, silver is double-hashed iff H(salt + B) == S. These
tests pin that detection, and pin the three outcomes staying distinguishable —
in particular that "neither matched" is never reported as clean.
"""

from __future__ import annotations

import hashlib

import pytest

from lakelogic.core.masking_diagnostics import (
    VERDICT_CONSISTENT,
    VERDICT_DOUBLE_HASHED,
    VERDICT_INCONCLUSIVE,
    VERDICT_INDETERMINATE,
    VERDICT_MIXED,
    VERDICT_NO_OVERLAP,
    diagnose_double_hashing,
    resolve_salt,
)

pl = pytest.importorskip("polars")
pd = pytest.importorskip("pandas")

SALT = "pipeline-salt"


def H(value: str, salt: str = SALT) -> str:
    return hashlib.sha256(f"{salt}{value}".encode("utf-8")).hexdigest()


# ── Frame builders — the same fixture through every supported engine ─────────


def _polars(keys, values):
    return pl.DataFrame({"rider_id": list(keys), "rider_key": list(values)}, schema_overrides={"rider_key": pl.Utf8})


def _pandas(keys, values):
    return pd.DataFrame({"rider_id": list(keys), "rider_key": list(values)})


def _rows(keys, values):
    return [{"rider_id": k, "rider_key": v} for k, v in zip(keys, values)]


FRAMES = pytest.mark.parametrize("frame", [_polars, _pandas, _rows], ids=["polars", "pandas", "rows"])


def _diagnose(frame, up, down, **kw):
    up_keys, up_vals = zip(*up) if up else ((), ())
    down_keys, down_vals = zip(*down) if down else ((), ())
    kw.setdefault("salt", SALT)
    return diagnose_double_hashing(
        frame(up_keys, up_vals),
        frame(down_keys, down_vals),
        column="rider_key",
        join_key="rider_id",
        **kw,
    )


# ── 1. A genuinely double-hashed column is detected, with the right count ────


@FRAMES
def test_double_hashed_column_is_detected_with_the_right_count(frame):
    """Bronze holds H(x); silver holds H(H(x)). Every joined row is damaged."""
    raw = ["alice", "bob", "carol"]
    bronze = [(i, H(v)) for i, v in enumerate(raw)]
    silver = [(i, H(H(v))) for i, v in enumerate(raw)]

    d = _diagnose(frame, bronze, silver)

    assert d.verdict == VERDICT_DOUBLE_HASHED
    assert d.double_hashed_rows == 3
    assert d.consistent_rows == 0
    assert d.indeterminate_rows == 0
    assert d.joined_rows == 3
    assert d.is_damaged is True
    assert d.is_clean is False
    assert d.salt_match == "salted"
    assert d.conclusive is True
    assert d.sample_keys == [0, 1, 2]


# ── 2. A correctly-passed-through column is CONSISTENT, not double-hashed ────


@FRAMES
def test_passed_through_column_is_consistent_not_double_hashed(frame):
    """B == S is the CORRECT outcome — silver carried the bronze hash through.
    Reporting this as damage would send people reprocessing healthy pipelines."""
    raw = ["alice", "bob", "carol"]
    bronze = [(i, H(v)) for i, v in enumerate(raw)]
    silver = list(bronze)

    d = _diagnose(frame, bronze, silver)

    assert d.verdict == VERDICT_CONSISTENT
    assert d.double_hashed_rows == 0
    assert d.consistent_rows == 3
    assert d.indeterminate_rows == 0
    assert d.is_damaged is False
    assert d.is_clean is True


# ── 3. Partial damage — a backfill that straddled the fix ────────────────────


@FRAMES
def test_mixed_column_reports_both_counts(frame):
    """A column can be PART double-hashed. A single boolean would hide it, so
    the counts have to be per row and both have to be right."""
    raw = ["a", "b", "c", "d", "e"]
    bronze = [(i, H(v)) for i, v in enumerate(raw)]
    # rows 0,1 re-hashed by the pre-fix run; rows 2,3,4 written after the guard
    silver = [(0, H(H("a"))), (1, H(H("b"))), (2, H("c")), (3, H("d")), (4, H("e"))]

    d = _diagnose(frame, bronze, silver)

    assert d.verdict == VERDICT_MIXED
    assert d.double_hashed_rows == 2
    assert d.consistent_rows == 3
    assert d.indeterminate_rows == 0
    assert d.joined_rows == 5
    assert d.is_damaged is True
    assert d.is_clean is False


# ── 4. Re-sourced / unrelated column is INDETERMINATE and NOT clean ─────────


@FRAMES
def test_unrelated_column_is_indeterminate_and_never_reported_clean(frame):
    """Neither H(salt+B) nor B matches. That is UNKNOWN — a different salt, a
    different transform, or a column re-sourced elsewhere all look like this.
    Collapsing it into 'consistent' would be a false all-clear."""
    bronze = [(i, H(v)) for i, v in enumerate(["a", "b", "c"])]
    silver = [(i, H(v, salt="a-completely-different-salt")) for i, v in enumerate(["a", "b", "c"])]

    d = _diagnose(frame, bronze, silver)

    assert d.verdict == VERDICT_INDETERMINATE
    assert d.verdict != VERDICT_CONSISTENT
    assert d.indeterminate_rows == 3
    assert d.consistent_rows == 0
    assert d.double_hashed_rows == 0
    assert d.is_clean is False, "indeterminate must never read as a clean bill of health"
    assert d.is_damaged is False, "and it must not be reported as proven damage either"
    assert d.inconclusive_reason and "not clean" in d.inconclusive_reason.lower()


# ── 5. Empty/unset salt → explicit inconclusive, never a false negative ─────


@FRAMES
def test_empty_salt_is_inconclusive_not_a_false_negative(frame):
    """The data IS double-hashed under the pipeline salt. Run the check with no
    salt and the salted form is uncomputable, so nothing matches — that must be
    reported as INCONCLUSIVE, never as 'no double-hashing found'."""
    raw = ["alice", "bob"]
    bronze = [(i, H(v)) for i, v in enumerate(raw)]
    silver = [(i, H(H(v))) for i, v in enumerate(raw)]

    d = _diagnose(frame, bronze, silver, salt="")

    assert d.salt_provided is False
    assert d.conclusive is False
    assert d.verdict == VERDICT_INCONCLUSIVE
    assert d.verdict != VERDICT_CONSISTENT
    assert d.is_clean is False
    assert "LAKELOGIC_PII_SALT" in (d.inconclusive_reason or "")


def test_unset_salt_env_var_is_treated_as_empty(monkeypatch):
    monkeypatch.delenv("LAKELOGIC_PII_SALT", raising=False)
    assert resolve_salt(None) == ""

    monkeypatch.setenv("LAKELOGIC_PII_SALT", SALT)
    assert resolve_salt(None) == SALT
    # An explicit argument still wins over the environment.
    assert resolve_salt("") == ""


@FRAMES
def test_unsalted_double_hashing_is_still_proven_without_a_salt(frame):
    """A positive is a positive: if the pipeline used no salt, H(B) == S proves
    the double-hash outright, and the report says WHICH form matched."""
    bronze = [(i, H(v, salt="")) for i, v in enumerate(["a", "b"])]
    silver = [(i, H(H(v, salt=""), salt="")) for i, v in enumerate(["a", "b"])]

    d = _diagnose(frame, bronze, silver, salt="")

    assert d.verdict == VERDICT_DOUBLE_HASHED
    assert d.double_hashed_rows == 2
    assert d.salt_match == "unsalted"
    assert d.conclusive is True


@FRAMES
def test_report_names_which_salt_form_matched(frame):
    bronze = [(i, H(v)) for i, v in enumerate(["a", "b"])]
    silver = [(i, H(H(v))) for i, v in enumerate(["a", "b"])]
    assert _diagnose(frame, bronze, silver).salt_match == "salted"


# ── 6. Non-joining rows are excluded from the verdict, reported separately ──


@FRAMES
def test_unjoined_rows_are_excluded_from_the_verdict_and_counted_separately(frame):
    """Rows with no counterpart cannot be compared. Silently counting them as
    anything — clean or damaged — would corrupt the percentages people act on."""
    bronze = [(1, H("a")), (2, H("b")), (3, H("c"))]  # key 3 never reaches silver
    silver = [(1, H(H("a"))), (2, H(H("b"))), (99, H(H("z")))]  # key 99 has no bronze row

    d = _diagnose(frame, bronze, silver)

    assert d.joined_rows == 2
    assert d.double_hashed_rows == 2
    assert d.unjoined_downstream_rows == 1  # key 99
    assert d.unjoined_upstream_rows == 1  # key 3
    assert d.double_hashed_rows + d.consistent_rows + d.indeterminate_rows == d.joined_rows
    assert d.upstream_rows == 3 and d.downstream_rows == 3


@FRAMES
def test_no_overlap_at_all_is_not_clean(frame):
    bronze = [(1, H("a"))]
    silver = [(2, H("b"))]

    d = _diagnose(frame, bronze, silver)

    assert d.verdict == VERDICT_NO_OVERLAP
    assert d.joined_rows == 0
    assert d.conclusive is False
    assert d.is_clean is False


@FRAMES
def test_null_rows_are_excluded_and_counted(frame):
    bronze = [(1, H("a")), (2, None)]
    silver = [(1, H(H("a"))), (2, H(H("b")))]

    d = _diagnose(frame, bronze, silver)

    assert d.null_rows == 1
    assert d.joined_rows == 1
    assert d.double_hashed_rows == 1


@FRAMES
def test_ambiguous_upstream_keys_are_excluded_and_counted(frame):
    """One upstream key with two DIFFERENT values means we cannot know which one
    downstream derived from. Guessing would invent a verdict."""
    bronze = [(1, H("a")), (1, H("z")), (2, H("b"))]
    silver = [(1, H(H("a"))), (2, H(H("b")))]

    d = _diagnose(frame, bronze, silver)

    assert d.ambiguous_key_rows == 1
    assert d.joined_rows == 1
    assert d.double_hashed_rows == 1


# ── Guard rails ──────────────────────────────────────────────────────────────


def test_join_key_must_not_be_the_masked_column():
    """Joining the masked column to itself can only ever find rows that already
    match, so it could never detect the damage it is looking for."""
    with pytest.raises(ValueError, match="join_key must differ"):
        diagnose_double_hashing(
            _polars([1], [H("a")]),
            _polars([1], [H(H("a"))]),
            column="rider_key",
            join_key="rider_key",
            salt=SALT,
        )


def test_missing_column_raises_rather_than_reporting_zero_damage():
    with pytest.raises(KeyError):
        diagnose_double_hashing(
            _polars([1], [H("a")]),
            _polars([1], [H(H("a"))]),
            column="not_a_column",
            join_key="rider_id",
            salt=SALT,
        )


def test_renamed_downstream_column_is_supported():
    bronze = pl.DataFrame({"rider_id": [1, 2], "rider_key": [H("a"), H("b")]})
    silver = pl.DataFrame({"rider_id": [1, 2], "rider_key_hashed": [H(H("a")), H(H("b"))]})

    d = diagnose_double_hashing(
        bronze,
        silver,
        column="rider_key",
        join_key="rider_id",
        downstream_column="rider_key_hashed",
        salt=SALT,
    )
    assert d.verdict == VERDICT_DOUBLE_HASHED
    assert d.double_hashed_rows == 2


def test_duckdb_relations_are_supported():
    duckdb = pytest.importorskip("duckdb")
    bronze = duckdb.from_df(pd.DataFrame({"rider_id": [1, 2], "rider_key": [H("a"), H("b")]}))
    silver = duckdb.from_df(pd.DataFrame({"rider_id": [1, 2], "rider_key": [H(H("a")), H(H("b"))]}))

    d = diagnose_double_hashing(bronze, silver, column="rider_key", join_key="rider_id", salt=SALT)
    assert d.verdict == VERDICT_DOUBLE_HASHED
    assert d.double_hashed_rows == 2


# ── Honesty: the output must never promise a repair ─────────────────────────


def test_output_points_at_reprocessing_and_never_claims_to_repair():
    """SHA-256 is one-way. If any surface implied this fixes the column, someone
    would run it expecting the data back."""
    bronze = [(i, H(v)) for i, v in enumerate(["a", "b"])]
    silver = [(i, H(H(v))) for i, v in enumerate(["a", "b"])]
    d = _diagnose(_polars, bronze, silver)

    text = (d.render() + " " + d.remediation).lower()
    assert "reprocess" in text
    assert "cannot be repaired in place" in text
    for lie in ("repairs the column", "restores the", "un-hash", "unhash", "reverses the hash"):
        assert lie not in text

    payload = d.to_dict()
    assert payload["verdict"] == VERDICT_DOUBLE_HASHED
    assert payload["double_hashed_rows"] == 2
    assert "reprocess" in payload["remediation"].lower()


def test_render_shows_all_three_outcome_counts_separately():
    raw = ["a", "b", "c"]
    bronze = [(i, H(v)) for i, v in enumerate(raw)]
    silver = [(0, H(H("a"))), (1, H("b")), (2, H("c", salt="other"))]
    out = _diagnose(_polars, bronze, silver).render()

    assert "double-hashed : 1" in out
    assert "consistent    : 1" in out
    assert "indeterminate : 1" in out


# ── CLI ──────────────────────────────────────────────────────────────────────


def _write_pair(tmp_path, bronze, silver):
    up = tmp_path / "bronze.parquet"
    down = tmp_path / "silver.parquet"
    _polars(*zip(*bronze)).write_parquet(up)
    _polars(*zip(*silver)).write_parquet(down)
    return up, down


def test_cli_double_hash_reports_the_column_the_counts_and_the_fix(tmp_path):
    from typer.testing import CliRunner

    from lakelogic.cli.main import app

    raw = ["alice", "bob", "carol"]
    bronze = [(i, H(v)) for i, v in enumerate(raw)]
    silver = [(0, H(H("alice"))), (1, H(H("bob"))), (2, H("carol"))]
    up, down = _write_pair(tmp_path, bronze, silver)

    res = CliRunner().invoke(
        app,
        [
            "diagnose",
            "double-hash",
            "--upstream",
            str(up),
            "--downstream",
            str(down),
            "--column",
            "rider_key",
            "--key",
            "rider_id",
            "--salt",
            SALT,
        ],
    )

    assert res.exit_code == 0, res.output
    assert "rider_key" in res.output  # names the column
    assert "MIXED" in res.output
    assert "double-hashed :        2" in res.output
    assert "consistent    :        1" in res.output
    assert "reprocess" in res.output.lower()
    assert "cannot be repaired in place" in res.output.lower()


def test_cli_json_output_and_fail_on_damage_exit_code(tmp_path):
    import json

    from typer.testing import CliRunner

    from lakelogic.cli.main import app

    bronze = [(i, H(v)) for i, v in enumerate(["a", "b"])]
    silver = [(i, H(H(v))) for i, v in enumerate(["a", "b"])]
    up, down = _write_pair(tmp_path, bronze, silver)

    args = [
        "diagnose",
        "double-hash",
        "-u",
        str(up),
        "-d",
        str(down),
        "-c",
        "rider_key",
        "-k",
        "rider_id",
        "--salt",
        SALT,
        "-f",
        "json",
    ]
    runner = CliRunner()

    res = runner.invoke(app, args)
    assert res.exit_code == 0, res.output
    payload = json.loads(res.output)
    assert payload["verdict"] == VERDICT_DOUBLE_HASHED
    assert payload["double_hashed_rows"] == 2
    assert payload["salt_match"] == "salted"

    res_ci = runner.invoke(app, args + ["--fail-on-damage"])
    assert res_ci.exit_code == 1


def test_cli_clean_column_does_not_fail_ci(tmp_path):
    from typer.testing import CliRunner

    from lakelogic.cli.main import app

    bronze = [(i, H(v)) for i, v in enumerate(["a", "b"])]
    up, down = _write_pair(tmp_path, bronze, bronze)

    res = CliRunner().invoke(
        app,
        [
            "diagnose",
            "double-hash",
            "-u",
            str(up),
            "-d",
            str(down),
            "-c",
            "rider_key",
            "-k",
            "rider_id",
            "--salt",
            SALT,
            "--fail-on-damage",
        ],
    )
    assert res.exit_code == 0, res.output
    assert "CONSISTENT" in res.output
