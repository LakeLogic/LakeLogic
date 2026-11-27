"""Structured metrics (OLC, 2026-11-27): Core reads parts through OLC's generator and checks them
with OLC's rule, so a contract with parts is not silently dropped from the business map."""

import pytest

pytest.importorskip("olc.models.metrics")
from olc.models import metrics as olc_metrics  # noqa: E402

if not hasattr(olc_metrics, "metric_expression"):
    pytest.skip("an OLC release before structured metrics", allow_module_level=True)

from lakelogic.core.business_map import _metric_expression  # noqa: E402
from lakelogic.core.schema_api import validate_contract  # noqa: E402

CLOSED = {"name": "Closed cases", "aggregation": "count_distinct", "expr": "Id", "filter": "ClosedDate IS NOT NULL"}
TOTAL = {"name": "Total cases", "aggregation": "count_distinct", "expr": "Id"}
RATE = {"name": "Case closure rate", "type": "ratio", "numerator": "Closed cases", "denominator": "Total cases"}


def test_parts_become_an_expression_and_expressions_stay():
    assert _metric_expression(CLOSED, [CLOSED]) == "COUNT(DISTINCT CASE WHEN ClosedDate IS NOT NULL THEN Id END)"
    assert "NULLIF" in _metric_expression(RATE, [CLOSED, TOTAL, RATE])
    assert _metric_expression({"name": "X", "expression": "COUNT(*)"}, []) == "COUNT(*)"


def _contract(metrics):
    return {
        "version": "1.0.0",
        "info": {"title": "Fact Coupons"},
        "primary_key": ["coupon_id"],
        "model": {
            "fields": [
                {"name": "coupon_id", "type": "string"},
                {"name": "fare_amount", "type": "float"},
                {"name": "currency", "type": "string"},
            ]
        },
        "metrics": metrics,
    }


def test_core_validation_uses_the_olc_rule_with_types_and_keys():
    bad = validate_contract(_contract([{"name": "Fare", "aggregation": "sum", "expr": "fare_amount"}]))
    assert not bad.valid and any("across currencies" in e.message for e in bad.errors)
    key = validate_contract(_contract([{"name": "Coupons", "aggregation": "sum", "expr": "coupon_id"}]))
    assert any("a key" in e.message for e in key.errors)
    ok = validate_contract(
        _contract(
            [
                {"name": "Fare", "aggregation": "sum", "expr": "fare_amount", "dimensions": ["currency"]},
                {"name": "Coupons", "aggregation": "count_distinct", "expr": "coupon_id"},
            ]
        )
    )
    assert not [e for e in ok.errors if e.field == "metrics"]
