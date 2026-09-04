"""Failing source rows are never captured or transmitted.

This is a product guarantee, not an implementation detail, and it was stated only in
comments and docs — several of which said the opposite ("attach first 50 quarantined
rows", "send up to 50 failing rows"). Those were wrong: `include_quarantine_sample` has
only ever controlled *rule attribution*, built from LakeLogic's own annotation columns.

The misleading names survive on the wire (`include_quarantine_sample`, the payload's
`quarantined_rows` field) for compatibility, so prose alone cannot keep this honest —
a future change could quietly start reading data columns and every name in the codebase
would seem to endorse it. These tests fail if that ever happens.
"""

from __future__ import annotations

import pytest

pl = pytest.importorskip("polars")

from lakelogic.core.processor import DataProcessor


class _Adapter:
    ERROR_COLUMN = "_lakelogic_errors"
    CATEGORY_COLUMN = "_lakelogic_categories"


def _extract(bad_df):
    """Call the extractor without constructing a full DataProcessor."""
    proc = DataProcessor.__new__(DataProcessor)
    proc.adapter = _Adapter()
    proc.engine_name = "polars"
    return DataProcessor._extract_row_rule_failures(proc, bad_df)


# A quarantined frame carrying obviously identifiable values alongside the annotation
# columns. If any of these strings reach the output, source data is leaving.
SECRETS = ["ada@example.com", "4111111111111111", "SORT-99-88-77"]

BAD_DF = pl.DataFrame(
    {
        "email": [SECRETS[0], SECRETS[0]],
        "card_number": [SECRETS[1], SECRETS[1]],
        "sort_code": [SECRETS[2], SECRETS[2]],
        "_lakelogic_errors": [
            ["Rule failed: email_is_valid (email LIKE '%@%')"],
            ["Rule failed: email_is_valid (email LIKE '%@%')"],
        ],
        "_lakelogic_categories": [["validity"], ["validity"]],
    }
)


class TestExtractorCarriesNoSourceData:
    def test_no_source_value_appears_anywhere_in_the_output(self):
        failures = _extract(BAD_DF)
        blob = repr(failures)
        for secret in SECRETS:
            assert secret not in blob, (
                f"source value {secret!r} reached the telemetry payload — failing rows "
                "must never be captured or transmitted"
            )

    def test_no_source_column_name_becomes_a_payload_key(self):
        # Column names are less sensitive than values but still describe the customer's
        # data; the descriptor shape is fixed and must not widen to carry them.
        allowed = {"name", "sql", "message", "count", "category"}
        for entry in _extract(BAD_DF):
            assert set(entry) <= allowed, f"unexpected key(s): {set(entry) - allowed}"

    def test_it_reports_the_rule_and_the_count(self):
        # The guarantee is worth nothing if it is met by sending nothing useful.
        failures = _extract(BAD_DF)
        assert len(failures) == 1
        entry = failures[0]
        assert entry["name"] == "email_is_valid"
        assert entry["count"] == 2
        assert entry["category"] == "validity"

    def test_a_frame_with_no_annotation_columns_yields_nothing(self):
        # No annotations means no attribution — NOT a fallback to reading data columns.
        plain = pl.DataFrame({"email": SECRETS[0:1], "card_number": SECRETS[1:2]})
        assert _extract(plain) == []
