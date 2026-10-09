"""``primary_key`` implies a uniqueness check; the same check written by hand is not run twice;
misplaced keys are refused instead of ignored; every ``unique`` form reads the same.

Before: a contract with ``primary_key`` alone let duplicate keys through, ``model.primary_key``
ran with no key at all, and ``unique: {column: id}`` checked nothing.
"""

from __future__ import annotations

import polars as pl
import pytest

from lakelogic import DataProcessor
from lakelogic.core.models import DataContract
from lakelogic.engines.base import _checked_unique_columns

DUPES = pl.DataFrame({"order_id": [1, 1, 2], "line_no": [1, 1, 1], "email": ["a", "a", "b"]})
FIELDS = [
    {"name": "order_id", "type": "integer"},
    {"name": "line_no", "type": "integer"},
    {"name": "email", "type": "string"},
]


def _contract(**extra):
    return DataContract.model_validate({"version": "1.0.0", "dataset": "orders", "model": {"fields": FIELDS}, **extra})


def _dataset_results(contract):
    proc = DataProcessor(contract=contract, engine="polars")
    proc.run(DUPES)
    return proc.last_report.get("dataset_rules") or []


def test_primary_key_alone_checks_the_key_is_unique():
    results = _dataset_results(_contract(primary_key=["order_id", "line_no"]))
    assert [(r["name"], r["passed"]) for r in results] == [("order_id_line_no_unique", False)]
    assert "Implied by primary_key" in results[0]["description"]


def test_a_unique_key_passes():
    proc = DataProcessor(contract=_contract(primary_key=["email"]), engine="polars")
    proc.run(DUPES.unique())
    assert [r["passed"] for r in proc.last_report["dataset_rules"]] == [True]


@pytest.mark.parametrize(
    "rule",
    [
        {"unique": ["line_no", "order_id"]},  # same columns, any order
        {"unique": {"columns": ["order_id", "line_no"], "name": "pk_check"}},
        {
            "name": "pk_sql",
            "sql": "SELECT COUNT(*) - COUNT(DISTINCT CONCAT_WS('|', order_id, line_no)) FROM source",
            "must_be_less_than": 1,
        },
        {
            "name": "pk_dupes",
            "sql": "SELECT COUNT(*) FROM (SELECT order_id, line_no FROM source "
            "GROUP BY order_id, line_no HAVING COUNT(*) > 1)",
            "must_be_less_than": 1,
        },
    ],
)
def test_the_same_check_written_by_hand_is_not_run_twice(rule):
    results = _dataset_results(_contract(primary_key=["order_id", "line_no"], quality={"dataset_rules": [rule]}))
    assert len(results) == 1 and results[0]["passed"] is False


def test_a_check_on_other_columns_does_not_count():
    results = _dataset_results(
        _contract(primary_key=["order_id", "line_no"], quality={"dataset_rules": [{"unique": "email"}]})
    )
    assert sorted(r["name"] for r in results) == ["email_unique", "order_id_line_no_unique"]


def test_scd2_keeps_versions_so_no_key_check_is_added():
    results = _dataset_results(_contract(primary_key=["order_id"], materialization={"strategy": "scd2"}))
    assert results == []


def test_labels_beside_unique_are_used():
    results = _dataset_results(
        _contract(
            quality={
                "dataset_rules": [
                    {"name": "email_once", "unique": "email", "severity": "warning", "description": "one row per email"}
                ]
            }
        )
    )
    assert [(r["name"], r["description"]) for r in results] == [("email_once", "one row per email")]


@pytest.mark.parametrize(
    "model,where",
    [
        ({"fields": FIELDS, "primary_key": ["order_id"]}, "move it to the top level"),
        ({"fields": [{"name": "order_id", "type": "integer", "primary_key": True}]}, "top-level `primary_key"),
        ({"fields": [{"name": "email", "type": "string", "unique": True}]}, "quality.dataset_rules"),
    ],
)
def test_misplaced_keys_are_refused_with_where_they_belong(model, where):
    with pytest.raises(ValueError, match="Misplaced keys") as err:
        DataContract.model_validate({"version": "1.0.0", "dataset": "orders", "model": model})
    assert where in str(err.value)


@pytest.mark.parametrize("rule", [{"unique": {"column": "email"}}, {"unique": {"field": ["email", "order_id"]}}])
def test_unique_mappings_that_used_to_check_nothing_or_crash_are_refused(rule):
    with pytest.raises(ValueError):
        _contract(quality={"dataset_rules": [rule]})


@pytest.mark.parametrize(
    "sql,cols",
    [
        ("SELECT COUNT(*) - COUNT(DISTINCT id) FROM source", {"id"}),
        (
            "SELECT COUNT(*) = COUNT(DISTINCT CONCAT_WS('|', CAST(\"a\" AS STRING), CAST(b AS STRING))) FROM t",
            {"a", "b"},
        ),
        ("SELECT COUNT(*) FROM (SELECT a FROM t GROUP BY a HAVING COUNT(*) > 1)", {"a"}),
        ("SELECT SUM(amount) FROM t", None),
        ("not sql at all (", None),
    ],
)
def test_recognising_a_uniqueness_check_in_sql(sql, cols):
    assert _checked_unique_columns(sql) == cols
