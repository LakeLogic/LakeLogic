"""Diagnose and repair SCD2 history that the late-arrival bug already corrupted.

The materialization fix stops NEW corruption. Tables written before it stay broken,
and unlike the double-hash case they ARE repairable: `effective_from` survived the
bug intact, and it is both the ordering key and the surrogate-key input, so the
correct intervals are derivable without inventing anything.

The corruption under test is the exact reported shape:

    d1 equire   2024-01-01 -> 2024-01-04  is_current=False
    d1 notified 2024-01-03 -> 9999-12-31  is_current=True   <- late row became current
    d1 serius   2024-01-04 -> 2024-01-03  is_current=False  <- to BEFORE from

The single most important test in this file is
`test_a_legitimate_gap_is_not_closed`: a delete-then-re-add leaves a deliberate hole
in history, and a repair that eats it is worse than the bug it fixes.
"""

from __future__ import annotations

import hashlib

import pandas as pd
import polars as pl
import pytest

from lakelogic.core.scd2_diagnostics import (
    DEFECT_INVERTED,
    DEFECT_IS_CURRENT_WRONG,
    DEFECT_OVERLAPPING,
    DEFECT_UNREPAIRABLE,
    REASON_DUPLICATE_EFFECTIVE_FROM,
    REASON_UNPARSEABLE_BOUNDARY,
    diagnose_scd2,
    repair_scd2,
)

OPEN = "9999-12-31"


def _row(key, status, ef, et, current):
    return {
        "driver_id": key,
        "status": status,
        "effective_from": ef,
        "effective_to": et,
        "is_current": current,
    }


def corrupted_rows(key: str = "d1") -> list[dict]:
    """The exact frame the late-arrival bug produced."""
    return [
        _row(key, "equire", "2024-01-01", "2024-01-04", False),
        _row(key, "notified", "2024-01-03", OPEN, True),
        _row(key, "serius", "2024-01-04", "2024-01-03", False),
    ]


def healthy_rows(key: str = "d1") -> list[dict]:
    return [
        _row(key, "equire", "2024-01-01", "2024-01-03", False),
        _row(key, "notified", "2024-01-03", "2024-01-04", False),
        _row(key, "serius", "2024-01-04", OPEN, True),
    ]


def gapped_rows(key: str = "d2") -> list[dict]:
    """Legitimate SCD2: the driver was deleted on 3 Jan and re-added on 10 Jan.

    `effective_to` (2024-01-03) is deliberately BEFORE the next version's
    `effective_from` (2024-01-10). Nothing was valid in between, and that IS the
    fact being recorded.
    """
    return [
        _row(key, "active", "2024-01-01", "2024-01-03", False),
        _row(key, "rehired", "2024-01-10", OPEN, True),
    ]


def _view(rows: list[dict]) -> list[dict]:
    ordered = sorted(rows, key=lambda r: (r["effective_from"], r["status"]))
    return [
        {
            "status": r["status"],
            "effective_from": str(r["effective_from"]),
            "effective_to": str(r["effective_to"]),
            "is_current": bool(r["is_current"]),
        }
        for r in ordered
    ]


def _sk(pk: str, effective_from: str) -> str:
    """The surrogate key exactly as materialization computes it."""
    return hashlib.sha256(f"{pk}|{effective_from}".encode("utf-8")).hexdigest()[:16]


# ── The reported corruption ──────────────────────────────────────────────────


def test_corrupted_frame_is_diagnosed_with_the_right_per_defect_counts():
    result = diagnose_scd2(corrupted_rows(), primary_key="driver_id")

    assert result.rows_inspected == 3
    assert result.keys_inspected == 1
    assert result.defect_counts == {
        # `serius` ends 2024-01-03 but starts 2024-01-04.
        DEFECT_INVERTED: 1,
        # `equire` runs to 01-04 over `notified` starting 01-03; `notified` runs to
        # 9999-12-31 over `serius` starting 01-04.
        DEFECT_OVERLAPPING: 2,
        # The flag must come off `notified` and go onto `serius`.
        DEFECT_IS_CURRENT_WRONG: 2,
        DEFECT_UNREPAIRABLE: 0,
    }
    assert result.is_corrupted is True
    assert result.is_clean is False
    assert result.keys_with_defects == 1
    assert result.example_keys[DEFECT_INVERTED] == ["d1"]
    assert result.example_keys[DEFECT_OVERLAPPING] == ["d1"]


def test_corrupted_frame_repairs_to_the_correct_three_rows():
    repaired = repair_scd2(corrupted_rows(), primary_key="driver_id")

    assert _view(repaired) == [
        {"status": "equire", "effective_from": "2024-01-01", "effective_to": "2024-01-03", "is_current": False},
        {"status": "notified", "effective_from": "2024-01-03", "effective_to": "2024-01-04", "is_current": False},
        {"status": "serius", "effective_from": "2024-01-04", "effective_to": OPEN, "is_current": True},
    ], f"repair did not reconstruct the intervals:\n{repaired}"

    current = [r["status"] for r in repaired if r["is_current"]]
    assert current == ["serius"], "is_current did not move onto the genuinely-latest version"


def test_repairing_twice_is_a_no_op():
    once = repair_scd2(corrupted_rows(), primary_key="driver_id")
    second = diagnose_scd2(once, primary_key="driver_id", repair=True)

    assert second.total_defects == 0
    assert second.rows_changed == 0
    assert _view(second.repaired_frame) == _view(once)


# ── THE conservatism test ────────────────────────────────────────────────────


def test_a_legitimate_gap_is_not_closed():
    """A delete-then-re-add hole is REAL history and must survive untouched.

    `effective_to` 2024-01-03 < next `effective_from` 2024-01-10 is exactly what a
    deletion looks like. It is not an overlap and it is not inverted, so nothing
    about it may be rewritten — closing it would assert the driver was active for a
    week when the business said it was not.
    """
    before = gapped_rows()
    result = diagnose_scd2(before, primary_key="driver_id", repair=True)

    assert result.total_defects == 0, f"a legitimate gap was reported as a defect: {result.defect_counts}"
    assert result.gap_boundaries_preserved == 1, "the gap was not recognised and counted as preserved"
    assert result.contiguous_boundaries == 0
    assert result.rows_changed == 0
    assert _view(result.repaired_frame) == _view(before), "the repair CLOSED a legitimate gap"
    assert result.repaired_frame[0]["effective_to"] == "2024-01-03"


def test_a_gap_inside_an_otherwise_corrupted_key_still_survives_repair():
    """Repair is per-boundary, not per-key: fixing an overlap must not close a gap."""
    rows = [
        _row("d3", "a", "2024-01-01", "2024-01-02", False),  # gap: nothing valid 02→05
        _row("d3", "b", "2024-01-05", "2024-01-20", False),  # overlaps c (starts 01-10)
        _row("d3", "c", "2024-01-10", OPEN, False),  # latest, open, but not flagged
    ]
    result = diagnose_scd2(rows, primary_key="driver_id", repair=True)

    assert result.defect_counts[DEFECT_OVERLAPPING] == 1
    assert result.defect_counts[DEFECT_IS_CURRENT_WRONG] == 1
    assert result.gap_boundaries_preserved == 1
    assert _view(result.repaired_frame) == [
        {"status": "a", "effective_from": "2024-01-01", "effective_to": "2024-01-02", "is_current": False},
        {"status": "b", "effective_from": "2024-01-05", "effective_to": "2024-01-10", "is_current": False},
        {"status": "c", "effective_from": "2024-01-10", "effective_to": OPEN, "is_current": True},
    ]


# ── Healthy input ────────────────────────────────────────────────────────────


def test_healthy_contiguous_frame_reports_zero_defects_and_is_unchanged():
    before = healthy_rows()
    result = diagnose_scd2(before, primary_key="driver_id", repair=True)

    assert result.defect_counts == {
        DEFECT_INVERTED: 0,
        DEFECT_OVERLAPPING: 0,
        DEFECT_IS_CURRENT_WRONG: 0,
        DEFECT_UNREPAIRABLE: 0,
    }
    assert result.is_clean is True
    assert result.contiguous_boundaries == 2
    assert result.gap_boundaries_preserved == 0
    assert result.rows_changed == 0
    assert _view(result.repaired_frame) == _view(before)


def test_a_closed_final_version_is_left_alone():
    """A deleted record has no current row. That is legal, not a missing flag."""
    rows = [
        _row("d4", "a", "2024-01-01", "2024-01-05", False),
        _row("d4", "b", "2024-01-05", "2024-01-09", False),  # deleted on 9 Jan, never returned
    ]
    result = diagnose_scd2(rows, primary_key="driver_id", repair=True)

    assert result.total_defects == 0
    assert result.rows_changed == 0
    assert _view(result.repaired_frame) == _view(rows)


# ── Ambiguity is reported, not guessed ───────────────────────────────────────


def test_two_versions_sharing_an_effective_from_are_unrepairable_not_guessed():
    rows = [
        _row("d5", "a", "2024-01-01", "2024-01-04", False),
        _row("d5", "b", "2024-01-01", OPEN, True),
    ]
    result = diagnose_scd2(rows, primary_key="driver_id", repair=True)

    assert result.defect_counts[DEFECT_UNREPAIRABLE] == 1
    assert result.unrepairable[0]["key"] == "d5"
    assert result.unrepairable[0]["reason"] == REASON_DUPLICATE_EFFECTIVE_FROM
    assert result.example_keys[DEFECT_UNREPAIRABLE] == ["d5"]
    assert result.is_clean is False
    # Nothing invented: the frame comes back exactly as it went in.
    assert result.rows_changed == 0
    assert _view(result.repaired_frame) == _view(rows)


def test_an_unparseable_boundary_makes_the_key_unrepairable():
    rows = [
        _row("d6", "a", "not-a-date", "2024-01-04", False),
        _row("d6", "b", "2024-01-04", OPEN, True),
    ]
    result = diagnose_scd2(rows, primary_key="driver_id", repair=True)

    assert result.defect_counts[DEFECT_UNREPAIRABLE] == 1
    assert result.unrepairable[0]["reason"] == REASON_UNPARSEABLE_BOUNDARY
    assert result.rows_changed == 0
    assert _view(result.repaired_frame) == _view(rows)


def test_an_inverted_row_on_an_unrepairable_key_is_still_reported_but_not_rewritten():
    """The inverted check is row-local, so it survives an unorderable key.

    Reporting it is honest; rewriting it is not, because the correct end date comes
    from an ordering that does not exist here.
    """
    rows = [
        _row("d7", "a", "2024-01-05", "2024-01-01", False),  # inverted
        _row("d7", "b", "2024-01-05", OPEN, True),  # duplicate effective_from
    ]
    result = diagnose_scd2(rows, primary_key="driver_id", repair=True)

    assert result.defect_counts[DEFECT_INVERTED] == 1
    assert result.defect_counts[DEFECT_UNREPAIRABLE] == 1
    assert result.rows_changed == 0
    assert _view(result.repaired_frame) == _view(rows)


# ── effective_from and surrogate keys are untouched ──────────────────────────


def test_effective_from_and_derived_surrogate_keys_are_byte_identical_after_repair():
    """The whole safety argument for repairing at all.

    SK = sha256(pk | effective_from). Repair writes only effective_to and the
    current flag, so every SK a fact table already holds still resolves.
    """
    before = corrupted_rows()
    before_ef = [r["effective_from"] for r in before]
    before_sk = [_sk(r["driver_id"], r["effective_from"]) for r in before]

    after = repair_scd2(before, primary_key="driver_id")
    after_ef = [r["effective_from"] for r in after]
    after_sk = [_sk(r["driver_id"], r["effective_from"]) for r in after]

    assert after_ef == before_ef, "repair mutated effective_from — every surrogate key would renumber"
    assert after_sk == before_sk, "surrogate keys churned; facts holding the old SK are now orphaned"
    # Byte-identical, not merely equal-after-parsing.
    for original, repaired in zip(before_ef, after_ef):
        assert type(original) is type(repaired)
        assert str(original).encode() == str(repaired).encode()


def test_repair_does_not_touch_any_non_scd2_column():
    rows = [
        {**_row("d8", "equire", "2024-01-01", "2024-01-04", False), "city": "berlin", "_sk": "abc123", "_version": 1},
        {**_row("d8", "notified", "2024-01-03", OPEN, True), "city": "paris", "_sk": "def456", "_version": 2},
        {**_row("d8", "serius", "2024-01-04", "2024-01-03", False), "city": "rome", "_sk": "ghi789", "_version": 3},
    ]
    repaired = repair_scd2(rows, primary_key="driver_id")

    for original, fixed in zip(rows, repaired):
        for column in ("driver_id", "status", "effective_from", "city", "_sk", "_version"):
            assert fixed[column] == original[column], f"repair modified {column}"


def test_the_input_frame_is_never_mutated_in_place():
    rows = corrupted_rows()
    snapshot = _view(rows)
    repair_scd2(rows, primary_key="driver_id")
    assert _view(rows) == snapshot, "diagnose_scd2 mutated the caller's frame"


# ── Diagnose / repair separation ─────────────────────────────────────────────


def test_diagnose_only_mode_never_returns_a_mutated_frame():
    result = diagnose_scd2(corrupted_rows(), primary_key="driver_id")

    assert result.repair_requested is False
    assert result.repaired_frame is None, "diagnose-only handed back a frame — repair must be opt-in"
    # The counts are still complete: diagnosis does not need the repair to run.
    assert result.rows_changed == 3
    assert result.is_corrupted is True


def test_repair_is_opt_in_per_call():
    diagnosed = diagnose_scd2(corrupted_rows(), primary_key="driver_id", repair=False)
    repaired = diagnose_scd2(corrupted_rows(), primary_key="driver_id", repair=True)

    assert diagnosed.repaired_frame is None
    assert repaired.repaired_frame is not None
    assert diagnosed.defect_counts == repaired.defect_counts, "the verdict must not depend on repair mode"


# ── Multiple keys ────────────────────────────────────────────────────────────


def test_keys_are_repaired_independently():
    rows = corrupted_rows("d1") + healthy_rows("dOK") + gapped_rows("dGAP")
    result = diagnose_scd2(rows, primary_key="driver_id", repair=True)

    assert result.keys_inspected == 3
    assert result.keys_with_defects == 1
    assert result.healthy_keys == 2
    assert result.defect_counts == {
        DEFECT_INVERTED: 1,
        DEFECT_OVERLAPPING: 2,
        DEFECT_IS_CURRENT_WRONG: 2,
        DEFECT_UNREPAIRABLE: 0,
    }

    fixed = result.repaired_frame
    by_key = {}
    for row in fixed:
        by_key.setdefault(row["driver_id"], []).append(row)

    assert _view(by_key["d1"]) == _view(healthy_rows("d1")), "the corrupted key was not repaired"
    assert _view(by_key["dOK"]) == _view(healthy_rows("dOK")), "a healthy key was altered"
    assert _view(by_key["dGAP"]) == _view(gapped_rows("dGAP")), "a gapped key lost its gap"


def test_one_unrepairable_key_does_not_block_repairing_the_others():
    rows = corrupted_rows("d1") + [
        _row("dBAD", "a", "2024-01-01", "2024-01-04", False),
        _row("dBAD", "b", "2024-01-01", OPEN, True),
    ]
    result = diagnose_scd2(rows, primary_key="driver_id", repair=True)

    assert result.defect_counts[DEFECT_UNREPAIRABLE] == 1
    by_key = {}
    for row in result.repaired_frame:
        by_key.setdefault(row["driver_id"], []).append(row)
    assert _view(by_key["d1"]) == _view(healthy_rows("d1"))
    assert _view(by_key["dBAD"]) == _view(rows[3:])


def test_composite_primary_key_is_supported():
    rows = [{**r, "region": "eu"} for r in corrupted_rows("d1")] + [{**r, "region": "us"} for r in healthy_rows("d1")]
    result = diagnose_scd2(rows, primary_key=["driver_id", "region"], repair=True)

    assert result.keys_inspected == 2
    assert result.keys_with_defects == 1
    assert result.example_keys[DEFECT_INVERTED] == [("d1", "eu")]


# ── Engine coverage ──────────────────────────────────────────────────────────


def test_pandas_frame_is_diagnosed_and_repaired_in_place_of_type():
    df = pd.DataFrame(corrupted_rows())
    result = diagnose_scd2(df, primary_key="driver_id", repair=True)

    assert result.defect_counts[DEFECT_OVERLAPPING] == 2
    assert isinstance(result.repaired_frame, pd.DataFrame)
    assert _view(result.repaired_frame.to_dict("records")) == _view(healthy_rows())
    # The caller's frame is untouched.
    assert list(df["effective_to"]) == ["2024-01-04", OPEN, "2024-01-03"]


def test_polars_frame_is_diagnosed_and_repaired_in_place_of_type():
    df = pl.DataFrame(corrupted_rows())
    result = diagnose_scd2(df, primary_key="driver_id", repair=True)

    assert result.defect_counts[DEFECT_INVERTED] == 1
    assert isinstance(result.repaired_frame, pl.DataFrame)
    assert _view(result.repaired_frame.to_dicts()) == _view(healthy_rows())
    assert df["effective_to"].to_list() == ["2024-01-04", OPEN, "2024-01-03"]


def test_polars_lazyframe_is_accepted():
    result = diagnose_scd2(pl.DataFrame(corrupted_rows()).lazy(), primary_key="driver_id", repair=True)
    assert _view(result.repaired_frame.to_dicts()) == _view(healthy_rows())


def test_real_datetime_columns_are_handled_not_only_strings():
    """Polars/pandas dimensions usually store datetimes, not ISO strings."""
    sentinel = "2099-12-31"
    rows = [
        _row("d1", "equire", "2024-01-01", "2024-01-04", False),
        _row("d1", "notified", "2024-01-03", sentinel, True),
        _row("d1", "serius", "2024-01-04", "2024-01-03", False),
    ]
    df = pd.DataFrame(rows)
    for column in ("effective_from", "effective_to"):
        df[column] = pd.to_datetime(df[column])

    result = diagnose_scd2(df, primary_key="driver_id", effective_to_default=sentinel, repair=True)

    assert result.defect_counts[DEFECT_INVERTED] == 1
    assert result.defect_counts[DEFECT_OVERLAPPING] == 2
    fixed = result.repaired_frame.sort_values("effective_from")
    assert [str(pd.Timestamp(v).date()) for v in fixed["effective_to"]] == [
        "2024-01-03",
        "2024-01-04",
        sentinel,
    ]
    assert list(fixed[fixed["is_current"].astype(bool)]["status"]) == ["serius"]
    # effective_from survived as datetimes, unchanged.
    assert [str(pd.Timestamp(v).date()) for v in fixed["effective_from"]] == [
        "2024-01-01",
        "2024-01-03",
        "2024-01-04",
    ]


def test_spark_frame_is_refused_rather_than_silently_collected():
    """A full collect of a dimension can OOM the driver, so it needs consent."""
    from lakelogic.core.scd2_diagnostics import Scd2SparkCollectRequired

    pyspark = pytest.importorskip("pyspark")

    class _FakeSparkDataFrame(pyspark.sql.DataFrame):  # type: ignore[misc]
        """Passes the isinstance dispatch without needing a live SparkSession."""

    fake = object.__new__(_FakeSparkDataFrame)

    with pytest.raises(Scd2SparkCollectRequired) as excinfo:
        diagnose_scd2(fake, primary_key="driver_id")

    assert "FULL COLLECT" in str(excinfo.value)
    assert "allow_collect=True" in str(excinfo.value)


# ── Guardrails ───────────────────────────────────────────────────────────────


def test_a_missing_control_column_is_an_explicit_error():
    with pytest.raises(KeyError):
        diagnose_scd2([{"driver_id": "d1"}], primary_key="driver_id")


def test_the_surrogate_key_may_not_be_used_as_the_primary_key_target():
    with pytest.raises(ValueError):
        diagnose_scd2(corrupted_rows(), primary_key="effective_from")


def test_render_states_the_safety_property_and_the_gap_policy():
    text = diagnose_scd2(corrupted_rows(), primary_key="driver_id").render()
    assert "effective_from is never modified" in text
    assert "Gaps are LEFT ALONE" in text
    assert "read-only diagnosis" in text
    assert "inverted" in text and "overlapping" in text


def test_to_dict_is_json_serialisable():
    import json

    payload = diagnose_scd2(corrupted_rows(), primary_key="driver_id").to_dict()
    assert json.loads(json.dumps(payload, default=str))["defects"][DEFECT_OVERLAPPING] == 2


# ── CLI ──────────────────────────────────────────────────────────────────────


def _cli(args):
    """Invoke the real CLI.

    NOTE: `tests/test_public_modules.py::test_cli_module_exports_app_and_main_executes_once`
    replaces `sys.modules["lakelogic.cli.main"]` with a stub module whose `app` is a
    plain function, and does that with a bare assignment rather than through
    `monkeypatch`, so it is never undone. Any CLI test in a file sorting after
    `test_public_modules` therefore inherits the stub. Re-import the real module if
    the cached one is not a Typer app. This is a pre-existing leak in that test, not
    something this module introduced.
    """
    import importlib
    import sys

    from typer.testing import CliRunner

    module = sys.modules.get("lakelogic.cli.main")
    if module is None or not hasattr(getattr(module, "app", None), "registered_commands"):
        sys.modules.pop("lakelogic.cli.main", None)
        module = importlib.import_module("lakelogic.cli.main")

    return CliRunner().invoke(module.app, args)


def test_cli_reports_the_corrupted_example_and_writes_nothing_by_default(tmp_path):
    table = tmp_path / "dim_driver.parquet"
    pl.DataFrame(corrupted_rows()).write_parquet(table)
    before = table.read_bytes()

    result = _cli(["diagnose", "scd2", "--table", str(table), "--key", "driver_id"])

    assert result.exit_code == 0, result.output
    assert "inverted" in result.output and "overlapping" in result.output
    assert "example inverted keys: d1" in result.output
    assert "effective_from is never modified" in result.output
    assert "Read-only diagnosis" in result.output
    assert table.read_bytes() == before, "a read-only diagnosis rewrote the source table"


def test_cli_repairs_only_when_given_an_explicit_output_path(tmp_path):
    table = tmp_path / "dim_driver.parquet"
    out = tmp_path / "dim_driver.repaired.parquet"
    pl.DataFrame(corrupted_rows()).write_parquet(table)

    result = _cli(["diagnose", "scd2", "--table", str(table), "--key", "driver_id", "--repair-out", str(out)])

    assert result.exit_code == 0, result.output
    assert out.exists()
    assert _view(pl.read_parquet(out).to_dicts()) == _view(healthy_rows())
    assert _view(pl.read_parquet(table).to_dicts()) == _view(corrupted_rows()), "the source was modified"


def test_cli_refuses_to_overwrite_the_source_without_allow_in_place(tmp_path):
    table = tmp_path / "dim_driver.parquet"
    pl.DataFrame(corrupted_rows()).write_parquet(table)

    result = _cli(["diagnose", "scd2", "--table", str(table), "--key", "driver_id", "--repair-out", str(table)])

    assert result.exit_code == 1
    assert "--allow-in-place" in result.output
    assert _view(pl.read_parquet(table).to_dicts()) == _view(corrupted_rows())


def test_cli_fail_on_defect_exits_non_zero_only_when_broken(tmp_path):
    broken = tmp_path / "broken.parquet"
    fine = tmp_path / "fine.parquet"
    pl.DataFrame(corrupted_rows()).write_parquet(broken)
    pl.DataFrame(healthy_rows()).write_parquet(fine)

    assert _cli(["diagnose", "scd2", "-t", str(broken), "-k", "driver_id", "--fail-on-defect"]).exit_code == 1
    assert _cli(["diagnose", "scd2", "-t", str(fine), "-k", "driver_id", "--fail-on-defect"]).exit_code == 0


def test_cli_json_output_carries_the_per_defect_counts(tmp_path):
    import json

    table = tmp_path / "dim.parquet"
    pl.DataFrame(corrupted_rows()).write_parquet(table)

    result = _cli(["diagnose", "scd2", "-t", str(table), "-k", "driver_id", "-f", "json"])

    payload = json.loads(result.output)
    assert payload["defects"] == {
        DEFECT_INVERTED: 1,
        DEFECT_OVERLAPPING: 2,
        DEFECT_IS_CURRENT_WRONG: 2,
        DEFECT_UNREPAIRABLE: 0,
    }
    assert payload["repair_returned"] is False
    assert "effective_from is never modified" in payload["safety"]


def test_cli_does_not_close_a_gap(tmp_path):
    """The conservatism guarantee has to hold at the CLI boundary too."""
    table = tmp_path / "dim.parquet"
    out = tmp_path / "out.parquet"
    pl.DataFrame(gapped_rows()).write_parquet(table)

    result = _cli(["diagnose", "scd2", "-t", str(table), "-k", "driver_id", "--repair-out", str(out)])

    assert result.exit_code == 0, result.output
    assert _view(pl.read_parquet(out).to_dicts()) == _view(gapped_rows())
