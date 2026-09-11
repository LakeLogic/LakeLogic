"""A row-rule failure must carry its rule NAME, whichever engine produced it.

`_extract_row_rule_failures` parses the error string each engine writes into the
quarantine frame. Four of the five engines write

    Rule failed: positive_spend (fare_amount >= 0)

but Spark writes a PHASE-PREFIXED variant:

    [pre] Rule failed: positive_spend (fare_amount >= 0)
    [post] Rule failed: …

The parser tested `startswith("Rule failed: ")`, so every Spark failure missed the branch
that pulls out `name` and `sql` and fell through to the bare `{message, count}` else. It kept
`count` and `category` — both set in either branch — which is what made the bug so quiet: the
telemetry looked populated, just anonymous.

Downstream that surfaced as "238 rule failures · no field recorded — the pipeline recorded a
failure but not which rule produced it", on a contract whose YAML plainly names the rule
`positive_spend`. The name was never lost; it was inside `message`, one prefix away from
being read.

The phase is kept rather than discarded: a rule that fails BEFORE the transformation and one
that fails AFTER it are different findings.
"""

from __future__ import annotations

import pytest

from lakelogic.core.processor import DataProcessor


class _Extractor(DataProcessor):
    """`_extract_row_rule_failures` reads only `self.adapter` + `self.engine_name`, so the
    parsing can be exercised without building a contract or a processing chain."""

    def __init__(self):  # noqa: D107 - deliberately skips DataProcessor.__init__
        self.adapter = None
        self.engine_name = "polars"


def _extract(messages):
    """Run the real parser over a bad-frame containing `messages`."""
    pl = pytest.importorskip("polars")
    bad = pl.DataFrame(
        {
            "_lakelogic_errors": [[m] for m in messages],
            "_lakelogic_categories": [["correctness"] for _ in messages],
        }
    )
    return _Extractor()._extract_row_rule_failures(bad)


BARE = "Rule failed: positive_spend (fare_amount >= 0)"
PRE = "[pre] Rule failed: positive_spend (fare_amount >= 0)"
POST = "[post] Rule failed: valid_rating (rating <= 5)"


def test_the_unprefixed_form_still_parses():
    """Polars / DuckDB / BigQuery / Snowflake. This already worked — pin it so the fix for
    Spark cannot regress the other four engines."""
    (f,) = _extract([BARE])
    assert f["name"] == "positive_spend"
    assert f["sql"] == "fare_amount >= 0"
    assert "phase" not in f


def test_a_spark_pre_failure_keeps_its_name():
    """The exact string from the RideFlow trips run that reported 238 anonymous failures."""
    (f,) = _extract([PRE])
    assert f["name"] == "positive_spend", "the rule name was dropped because of the [pre] prefix"
    assert f["sql"] == "fare_amount >= 0"
    assert f["phase"] == "pre"


def test_a_spark_post_failure_keeps_its_name_and_phase():
    (f,) = _extract([POST])
    assert f["name"] == "valid_rating"
    assert f["phase"] == "post"


def test_count_and_category_are_unaffected():
    """These survived the bug; they must survive the fix. `count` is what made the broken
    records look complete enough that nobody questioned the missing name."""
    (f,) = _extract([PRE, PRE, PRE])
    assert f["count"] == 3
    assert f["category"] == "correctness"


def test_a_message_that_is_not_a_rule_failure_is_left_alone():
    """Not every quarantine reason is a named rule — a type coercion error has no rule, and
    must not be given a fabricated one."""
    (f,) = _extract(["Type error: expected int, got 'abc'"])
    assert "name" not in f
    assert f["message"] == "Type error: expected int, got 'abc'"
