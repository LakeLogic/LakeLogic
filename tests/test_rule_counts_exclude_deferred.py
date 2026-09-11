"""A rule that could not be evaluated is neither passed nor failed.

`compute_rule_counts` produces the `rules_evaluated / rules_passed / rules_failed` triple that
every run log carries, and that the SaaS turns into a data product's Quality verdict —
`rules_failed > 0` is what pins a product to Degraded.

A DEFERRED dataset rule reports `passed: None`. It was not evaluated at all: an SCD2 surrogate
key does not exist until the materializer has run, so `unique: driver_sk` has nothing to check
during validation.

The count used `not r.get("passed")`, and `not None` is True — so the rule that explicitly
could NOT be evaluated was counted as a failure. The run payload and the counters beside it
then disagreed with each other:

    dataset_rules : [{"name": "driver_sk_unique",
                      "value": "not evaluated — 'driver_sk' is materialized after validation",
                      "passed": null, "deferred": true}]
    rules_failed  : 1

and the product stayed Degraded on the evidence of a rule that never ran. Observed on a live
`gold_rideflow_dim_driver` run after the deferral itself was already fixed and released — the
payload was right and the number beside it was wrong, so nothing a user could see had changed.

The SaaS consumers of this field already test `is False`. This is the PRODUCER catching up;
the identity check has to be on both sides or the number arrives wrong however carefully it
is read.
"""

from __future__ import annotations

import pytest

from lakelogic.core.run_log import compute_rule_counts

PASSED = {"name": "row_count_positive", "passed": True}
FAILED = {"name": "positive_spend", "passed": False}
DEFERRED = {"name": "driver_sk_unique", "passed": None, "deferred": True}


def test_a_deferred_rule_is_not_a_failure():
    """The exact shape that kept dim_driver Degraded."""
    evaluated, passed, failed = compute_rule_counts([], [DEFERRED], [DEFERRED], [])
    assert failed == 0, "a rule that could not be evaluated was counted as failed"


def test_a_deferred_rule_is_not_counted_as_evaluated():
    """It must leave the denominator too — otherwise `passed` is understated by exactly the
    deferred count, and a healthy product reads as partially unverified."""
    evaluated, passed, failed = compute_rule_counts([], [DEFERRED], [DEFERRED], [])
    assert (evaluated, passed, failed) == (0, 0, 0)


def test_a_real_failure_is_still_counted():
    """The guard on the guard: a fix that stopped counting failures would silence quality
    reporting across the estate."""
    evaluated, passed, failed = compute_rule_counts([], [FAILED], [FAILED], [])
    assert failed == 1
    assert evaluated == 1


def test_deferred_and_failed_together():
    """One of each — the deferred one drops out, the failing one survives."""
    evaluated, passed, failed = compute_rule_counts([], [FAILED, DEFERRED], [FAILED, DEFERRED], [])
    assert (evaluated, passed, failed) == (1, 0, 1)


def test_row_rule_failures_are_added():
    """Row-level failures count alongside dataset ones, one per rule however many rows it hit."""
    evaluated, passed, failed = compute_rule_counts(
        [{"name": "a"}, {"name": "b"}], [], [], [{"name": "a", "count": 238}]
    )
    assert (evaluated, passed, failed) == (2, 1, 1)


def test_nothing_countable_stays_absent():
    """An absent count must not become a confident 0 — the Observatory used to render
    '0 evaluated, 0 passed, 0 failed' for runs that did evaluate rules."""
    assert compute_rule_counts([], [], [], []) == (None, None, None)


@pytest.mark.parametrize("passed_value", [True, False, None])
def test_passed_is_never_negative(passed_value):
    """`passed` is derived by subtraction; it must not go below zero on any input."""
    rule = {"name": "r", "passed": passed_value}
    _, passed, _ = compute_rule_counts([], [rule], [rule], [{"name": "x"}, {"name": "y"}])
    assert passed >= 0
