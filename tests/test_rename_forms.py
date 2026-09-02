"""Every declared form of ``rename`` must be honoured — or say why not.

``rename`` had no conformance coverage at all, and three separate defects were
living in it:

1. The bare ``rename: {old: new}`` shorthand was parsed into pydantic *extras* and
   read by nothing. It validated clean and did nothing.
2. DuckDB read ``trans.rename.mappings`` DIRECTLY instead of ``iter_pairs()``, so it
   honoured exactly one of the three forms and dropped the other two silently.
3. DuckDB gated rename on ``phase == "pre"`` — but ``phase`` defaults to ``"post"``
   and that engine has no rename branch in the post pass, so a rename without an
   explicit phase was dropped entirely.

(2) and (3) compound: either alone hides the other. In every case the column kept
its old name and any rule referencing the new name failed against a column that had
never existed.
"""

from __future__ import annotations

import pytest

pl = pytest.importorskip("polars")

from lakelogic.core.models import DataContract
from lakelogic.core.processor import DataProcessor

ROWS = [{"id": 1, "old_status": "ok"}, {"id": 2, "old_status": None}]

CANONICAL_FORMS = {
    "mappings": {"mappings": {"old_status": "status"}},
    "from_to": {"from_name": "old_status", "to_name": "status"},
}


def _contract(rename_spec: dict, phase: str | None = None) -> dict:
    trans: dict = {"rename": rename_spec}
    if phase:
        trans["phase"] = phase
    return {
        "version": "1.0.0",
        "info": {"title": "T", "table_name": "t"},
        "model": {
            "fields": [
                {"name": "id", "type": "integer"},
                {"name": "status", "type": "string"},
            ]
        },
        "quality": {"row_rules": [{"name": "status_not_null", "sql": "status IS NOT NULL"}]},
        "transformations": [trans],
    }


# ── the rename actually happens, on every engine and every canonical form ────


@pytest.mark.parametrize("engine", ["polars", "duckdb"])
@pytest.mark.parametrize("form", sorted(CANONICAL_FORMS))
def test_canonical_forms_are_honoured(engine, form):
    """The rule names the POST-rename column, so if the rename is skipped nothing
    passes — which is exactly how the silent drop used to present."""
    good, bad = DataProcessor(_contract(CANONICAL_FORMS[form]), engine=engine).run(pl.DataFrame(ROWS))
    assert len(good) == 1, f"{engine}/{form}: rename not applied"
    assert len(bad) == 1


@pytest.mark.parametrize("engine", ["polars", "duckdb"])
def test_rename_without_an_explicit_phase_is_applied(engine):
    """`phase` defaults to "post" and DuckDB had no post rename branch, so an
    un-phased rename silently vanished on that engine while working on Polars."""
    good, _ = DataProcessor(_contract(CANONICAL_FORMS["mappings"]), engine=engine).run(pl.DataFrame(ROWS))
    assert len(good) == 1


@pytest.mark.parametrize("engine", ["polars", "duckdb"])
def test_explicit_pre_phase_still_works(engine):
    """Removing the phase gate must not break contracts that DO declare it."""
    good, _ = DataProcessor(_contract(CANONICAL_FORMS["mappings"], phase="pre"), engine=engine).run(pl.DataFrame(ROWS))
    assert len(good) == 1


# ── the shorthand: applied, but never silently ───────────────────────────────


def test_shorthand_is_parsed_into_pairs():
    contract = DataContract(**_contract({"old_status": "status"}))
    assert contract.transformations[0].rename.iter_pairs() == [("old_status", "status")]


def test_shorthand_warns_that_it_is_not_canonical():
    """It is applied (it is obvious what was meant) but strict/canonical OLC REJECTS
    it, so a contract using it works locally and fails the spec gate. The user has to
    hear that at the point of use, not from CI."""
    from loguru import logger

    records: list[str] = []
    sink = logger.add(lambda m: records.append(str(m)), level="WARNING", format="{message}")
    try:
        DataContract(**_contract({"old_status": "status"})).transformations[0].rename.iter_pairs()
    finally:
        logger.remove(sink)

    assert len(records) == 1, records
    msg = records[0]
    assert "non-canonical" in msg
    assert "REJECTS" in msg
    assert "mappings" in msg  # names the portable form


def test_the_shorthand_is_genuinely_rejected_by_strict_validation():
    """Pins the reason the warning exists. If the canonical model ever accepts this
    form, the warning becomes false and must be removed."""
    with pytest.raises(Exception) as exc:
        DataProcessor(_contract({"old_status": "status"}), engine="polars", strict=True)
    assert "transformations.rename.old_status" in str(exc.value)


@pytest.mark.parametrize("form", sorted(CANONICAL_FORMS))
def test_canonical_forms_do_not_warn(form):
    """The warning must fire ONLY for the non-canonical form."""
    from loguru import logger

    records: list[str] = []
    sink = logger.add(lambda m: records.append(str(m)), level="WARNING", format="{message}")
    try:
        DataContract(**_contract(CANONICAL_FORMS[form])).transformations[0].rename.iter_pairs()
    finally:
        logger.remove(sink)

    assert not [r for r in records if "non-canonical" in r]


def test_config_keys_are_not_mistaken_for_rename_pairs():
    """`from_name`/`to_name`/`mappings` configure the rename; they are not columns.
    Treating them as pairs would invent a rename nobody asked for."""
    contract = DataContract(**_contract(CANONICAL_FORMS["from_to"]))
    assert contract.transformations[0].rename.iter_pairs() == [("old_status", "status")]
