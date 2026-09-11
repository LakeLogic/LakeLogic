"""A type-cast failure is not a failed rule.

`row_failures` is the quarantine breakdown, and it mixes two different things. A rule
failure names the rule that rejected the row; a TYPE MISMATCH has no rule behind it at
all — the value simply could not be cast to its declared type:

    {"message": "Type Mismatch: fare_amount cannot be cast to float",
     "count": 21, "category": "schema"}                       <- no `name`
    {"name": "positive_spend", "sql": "fare_amount >= 0",
     "count": 173, "category": "correctness"}                 <- a rule

`compute_rule_counts` counted the length of the whole list, so every mis-typed column
became a failed rule. On a real `silver_rideflow_trips` run that read:

    rules_evaluated = 2      rules_passed = 0      rules_failed = 11

2 configured row rules, 1 of which failed, plus 10 columns that failed to cast. Three
things are wrong with it at once: `failed` exceeds `evaluated`, which is not a number
anyone can act on; `passed` is 0 although `positive_spend`'s sibling rule passed, because
passed is `evaluated - failed` floored at zero; and `rules_failed > 0` is the predicate
that pins a data product to Degraded — so the verdict cited rules that do not exist.

The cast failures are NOT being hidden. They already travel as quarantine signals under
`category: "schema"`, which is where a type mismatch belongs, and the row counts are
unchanged. They are simply not rules, so they are not counted as rules.

Sibling of test_rule_counts_exclude_deferred.py: the same function, the same class of
error — a number beside the payload disagreeing with the payload.
"""
from __future__ import annotations

import pytest

from lakelogic.core.run_log import compute_rule_counts

# The real payload from the run that exposed this, trimmed to the shape that matters.
RULE_FAILURE = {"name": "positive_spend", "sql": "fare_amount >= 0",
                "message": "Rule failed: positive_spend (fare_amount >= 0)",
                "count": 173, "category": "correctness"}
CAST_FAILURE = {"message": "Type Mismatch: fare_amount cannot be cast to float",
                "count": 21, "category": "schema"}
ROW_RULES = [{"name": "positive_spend"}, {"name": "valid_rating"}]


def test_the_real_run_now_adds_up():
    """silver_rideflow_trips: 2 row rules, 1 failing, 10 columns that would not cast."""
    casts = [dict(CAST_FAILURE, message=f"Type Mismatch: c{i} cannot be cast to float") for i in range(10)]
    evaluated, passed, failed = compute_rule_counts(ROW_RULES, [], [], [RULE_FAILURE, *casts])
    assert (evaluated, passed, failed) == (2, 1, 1)


def test_failed_never_exceeds_evaluated():
    """The property the old count violated. A rule that was never evaluated cannot fail."""
    casts = [dict(CAST_FAILURE, message=f"Type Mismatch: c{i}") for i in range(25)]
    evaluated, passed, failed = compute_rule_counts(ROW_RULES, [], [], casts)
    assert failed <= evaluated, f"{failed} failed out of {evaluated} evaluated"


def test_a_run_whose_only_quarantine_is_cast_failures_reports_no_failed_rules():
    """Every rule passed; the rows were rejected on type. Reporting a failed rule here is
    what pinned the product to Degraded with no rule to point at."""
    evaluated, passed, failed = compute_rule_counts(ROW_RULES, [], [], [CAST_FAILURE])
    assert (evaluated, passed, failed) == (2, 2, 0)


def test_a_named_rule_failure_is_still_counted():
    """The guard on the guard — a fix that stopped counting rule failures would silence
    quality reporting across the estate."""
    evaluated, passed, failed = compute_rule_counts(ROW_RULES, [], [], [RULE_FAILURE])
    assert (evaluated, passed, failed) == (2, 1, 1)


@pytest.mark.parametrize("blank", [None, "", "   "])
def test_a_blank_name_is_not_a_rule(blank):
    """Absent and empty must behave the same. A failure carrying `name: ""` names nothing,
    and would otherwise be counted as a rule whose identity is a blank string."""
    _, _, failed = compute_rule_counts(ROW_RULES, [], [], [dict(CAST_FAILURE, name=blank)])
    assert failed == 0


def test_a_spark_phase_prefixed_failure_still_counts():
    """Spark writes `[pre] Rule failed: …`; the parser keeps the name and adds `phase`.
    That is a real rule failure and must survive this filter — the two fixes have to
    compose, not cancel."""
    pre = dict(RULE_FAILURE, phase="pre")
    _, _, failed = compute_rule_counts(ROW_RULES, [], [], [pre])
    assert failed == 1
