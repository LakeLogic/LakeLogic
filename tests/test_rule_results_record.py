"""Per-rule results in the run payload (#570): every declared rule's own verdict.

The payload listed only failures, so a rule that passed was invisible to any history. These
pin the record the platform builds its quality timeline from.
"""

from __future__ import annotations

from types import SimpleNamespace

from lakelogic.core.run_log import MEASUREMENT_VERSION, build_rule_results, rules_fingerprint

ROW = [SimpleNamespace(name="valid_email", sql="email like '%@%'", category="validity"),
       SimpleNamespace(name="licence_required", sql="licence_number IS NOT NULL", category="completeness")]
DATASET = [SimpleNamespace(name="unique_driver", sql="SELECT ...")]


def _by_name(out):
    return {r["name"]: r for r in out}


def test_a_passing_rule_is_listed_with_its_denominator():
    out = _by_name(build_rule_results(ROW, [], [], [{"name": "valid_email", "count": 4}], rows_checked=55))
    assert out["valid_email"] == {"name": "valid_email", "kind": "row", "category": "validity",
                                  "status": "failed", "failed_rows": 4, "rows_evaluated": 55}
    assert out["licence_required"]["status"] == "passed"
    assert out["licence_required"]["failed_rows"] == 0 and out["licence_required"]["rows_evaluated"] == 55


def test_schema_failures_are_not_rules():
    out = _by_name(build_rule_results(ROW, [], [], [{"message": "Type mismatch", "count": 9}], rows_checked=10))
    assert all(r["status"] == "passed" for r in out.values())


def test_a_run_that_failed_before_its_rules_evaluated_nothing():
    out = build_rule_results(ROW, DATASET, [], [], rows_checked=None, run_failed=True)
    rows = [r for r in out if r["kind"] == "row"]
    assert rows and all(r["status"] == "not_evaluated" and r["failed_rows"] is None for r in rows)


def test_dataset_rules_take_their_own_verdict_and_deferred_is_not_evaluated():
    out = _by_name(build_rule_results([], DATASET, [{"name": "unique_driver", "passed": False},
                                                    {"name": "late_check", "passed": None, "deferred": True}],
                                      [], rows_checked=10))
    assert out["unique_driver"]["status"] == "failed"
    assert out["late_check"]["status"] == "not_evaluated"


def test_a_declared_dataset_rule_with_no_result_is_not_evaluated_never_passed():
    out = _by_name(build_rule_results([], DATASET, [], [], rows_checked=10))
    assert out["unique_driver"]["status"] == "not_evaluated"


def test_the_fingerprint_changes_exactly_when_the_rules_do():
    a = rules_fingerprint(ROW, DATASET)
    assert a == rules_fingerprint(list(reversed(ROW)), DATASET)  # order-free
    changed = [ROW[0], SimpleNamespace(name="licence_required", sql="licence_number <> ''")]
    assert a != rules_fingerprint(changed, DATASET)
    assert rules_fingerprint([], []) is None


def test_the_record_has_a_version():
    assert MEASUREMENT_VERSION == 1


def test_dict_rules_work_too():
    out = build_rule_results([{"name": "x", "sql": "a > 0"}], [], [], [], rows_checked=3)
    assert out == [{"name": "x", "kind": "row", "category": None, "status": "passed",
                    "failed_rows": 0, "rows_evaluated": 3}]
