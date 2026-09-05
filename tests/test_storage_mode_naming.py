"""`storage_mode` names the addressing scheme, not a vendor — and old names still work.

"uc" named ONE vendor's product ("Unity Catalog") for a behaviour every warehouse
shares: a Snowflake, BigQuery or Fabric user had to select an option referring to a
Databricks catalog they do not have. Its partner, "direct", named no scheme at all.
They are now "catalog" and "path", which answer the same question — how is the target
addressed? — and read correctly on every supported platform.

Nothing breaks: the old spellings still resolve, with a warning.
"""

from __future__ import annotations

import pytest
from loguru import logger

from lakelogic.core.registry import STORAGE_MODES, normalize_storage_mode


def _warnings_from(fn):
    lines = []
    sink = logger.add(lambda m: lines.append(str(m)), level="WARNING")
    try:
        result = fn()
    finally:
        logger.remove(sink)
    return result, lines


def test_the_canonical_names_are_catalog_and_path():
    assert STORAGE_MODES == ("catalog", "path")


@pytest.mark.parametrize("value", ["catalog", "path"])
def test_canonical_values_pass_through_silently(value):
    result, warnings = _warnings_from(lambda: normalize_storage_mode(value))
    assert result == value
    assert warnings == [], f"a current value must not warn: {warnings}"


@pytest.mark.parametrize(
    "legacy,canonical",
    [("uc", "catalog"), ("direct", "path"), ("unity_catalog", "catalog")],
)
def test_legacy_names_still_work(legacy, canonical):
    """Backward compatibility: existing pipelines keep running."""
    result, warnings = _warnings_from(lambda: normalize_storage_mode(legacy))
    assert result == canonical
    assert warnings, "a deprecated value must say so"
    assert canonical in warnings[0], f"the warning must name the replacement: {warnings[0]}"


@pytest.mark.parametrize("value,expected", [("UC", "catalog"), ("Direct", "path"), ("  uc  ", "catalog")])
def test_case_and_whitespace_are_normalised(value, expected):
    """`storage_mode="UC"` used to match NEITHER branch and silently take a third path."""
    assert normalize_storage_mode(value)[0] == expected[0]
    assert normalize_storage_mode(value) == expected


def test_an_unknown_value_raises_instead_of_silently_misresolving():
    with pytest.raises(ValueError) as exc:
        normalize_storage_mode("typo")
    message = str(exc.value)
    assert "catalog" in message and "path" in message, message


def test_none_defaults_to_catalog():
    assert normalize_storage_mode(None) == "catalog"
