"""Conventional code columns get a small, realistic set of values, not random codes.

Live (2026-09-29): `platform` came out as `PLA-3906`, `event_name` as `EVE-8304`, so the gold code
dimensions built from them had ~5,500 rows. Domain-specific columns (`cancelled_by`) are NOT
guessed here: their values come from the contract's accepted_values.
"""

from __future__ import annotations

import pytest

from lakelogic.core.generator import DataGenerator

pl = pytest.importorskip("polars")


def _values(col: str, n: int = 400) -> set:
    gen = DataGenerator([("id", "integer"), (col, "string")], seed=7)
    return set(gen.generate(rows=n)[col].drop_nulls().to_list())


@pytest.mark.parametrize(
    "col, expected",
    [
        ("platform", {"ios", "android", "web"}),
        ("level", {"low", "medium", "high"}),
    ],
)
def test_conventional_codes_come_from_a_small_realistic_set(col, expected):
    assert _values(col) <= expected


def test_event_name_is_an_app_event_not_a_code_or_a_person():
    vals = _values("event_name")
    assert vals and len(vals) <= 9
    assert not any(v.startswith("EVE-") for v in vals)
    assert "purchase" in vals or "app_open" in vals
