"""Quality-rule categories: one closed list, and no default.

A rule with no category used to become ``correctness``, so an unclassified rule could not be told
apart from a classified one. The model now keeps ``None``; a run records ``unclassified``.
"""

from __future__ import annotations

import pytest

from lakelogic.core.models import (
    QUALITY_CATEGORIES,
    UNCLASSIFIED,
    DataContract,
    QualityRule,
    runtime_category,
)

pl = pytest.importorskip("polars")


def test_the_list_is_the_five():
    assert QUALITY_CATEGORIES == ("completeness", "uniqueness", "validity", "consistency", "accuracy")


def test_a_missing_category_stays_missing():
    assert QualityRule(name="r", sql="1 = 1").category is None
    assert QualityRule(name="r", sql="1 = 1", category="  ").category is None


def test_a_legacy_value_is_kept_not_remapped():
    """Remapping `correctness` would make an unclassified rule look classified."""
    assert QualityRule(name="r", sql="1 = 1", category="correctness").category == "correctness"


def test_synonyms_resolve_to_the_list():
    assert QualityRule(name="r", sql="1 = 1", category="Unique").category == "uniqueness"
    assert QualityRule(name="r", sql="1 = 1", category="referential_integrity").category == "consistency"


def test_a_run_records_unclassified_for_a_missing_category():
    assert runtime_category(QualityRule(name="r", sql="1 = 1")) == UNCLASSIFIED
    assert runtime_category(QualityRule(name="r", sql="1 = 1", category="validity")) == "validity"


def _contract(row_rules):
    return DataContract(
        version="1.0.0",
        dataset="orders",
        model={"fields": [{"name": "id", "type": "int"}, {"name": "amount", "type": "double"}]},
        quality={"row_rules": row_rules},
    )


@pytest.mark.parametrize("engine", ["polars", "duckdb"])
def test_errors_and_categories_stay_aligned_when_a_rule_has_no_category(engine):
    """Errors and categories are parallel lists with nulls dropped from both. A None category
    would drop from one list only, and every later failure would carry the wrong category."""
    if engine == "duckdb":
        pytest.importorskip("duckdb")
        from lakelogic.engines.duckdb import DuckDBAdapter as Adapter
    else:
        from lakelogic.engines.polars import PolarsAdapter as Adapter

    adapter = Adapter(
        _contract(
            [
                {"name": "unclassified_rule", "sql": "amount > 100"},
                {"name": "positive", "sql": "amount > 0", "category": "validity"},
            ]
        )
    )
    _, bad = adapter.execute(pl.DataFrame({"id": [1], "amount": [-1.0]}))

    errors = bad["_lakelogic_errors"][0].to_list()
    categories = bad["_lakelogic_categories"][0].to_list()
    assert len(errors) == len(categories) == 2
    by_rule = {("positive" if "positive" in e else "unclassified_rule"): c for e, c in zip(errors, categories)}
    assert by_rule == {"unclassified_rule": UNCLASSIFIED, "positive": "validity"}


@pytest.mark.parametrize(
    "rule, expected",
    [
        ({"not_null": "amount"}, "completeness"),
        ({"accepted_values": {"field": "id", "values": [1, 2]}}, "validity"),
        ({"range": {"field": "amount", "min": 0}}, "validity"),
        ({"regex_match": {"field": "id", "pattern": "^[0-9]+$"}}, "validity"),
    ],
)
def test_built_in_rules_classify_themselves(rule, expected):
    from lakelogic.engines.polars import PolarsAdapter

    adapter = PolarsAdapter(_contract([rule]))
    rules = adapter.get_row_rules()
    assert {r.category for r in rules} == {expected}
