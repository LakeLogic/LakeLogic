"""`required: true` and a written `<field> IS NOT NULL` rule are one check, not two.

The engine turns every required field into `<field>_required` (IS NOT NULL). A contract that
also writes `rider_id IS NOT NULL` (or `not_null: rider_id`) used to scan each row twice and
report a null against both rules. The written rule wins; the automatic one is skipped.
"""
import pytest

from lakelogic.core.models import DataContract
from lakelogic.engines.polars import PolarsAdapter


def _contract(row_rules, required=True, field_rules=None):
    field = {"name": "rider_id", "type": "string", "required": required}
    if field_rules:
        field["rules"] = field_rules
    return DataContract(
        version="1.0.0",
        model={"fields": [field, {"name": "email", "type": "string", "required": True}]},
        quality={"row_rules": row_rules},
    )


def _names(contract):
    return [r.name for r in PolarsAdapter(contract).get_row_rules()]


@pytest.mark.parametrize("rule", [
    {"name": "rider_id_not_null", "sql": "rider_id IS NOT NULL"},
    {"name": "rider_id_not_null", "sql": '"rider_id" is not null'},
    {"name": "rider_id_not_null", "sql": "(RIDER_ID IS NOT NULL)"},
    {"not_null": "rider_id"},
])
def test_a_written_not_null_replaces_the_automatic_required_check(rule):
    names = _names(_contract([rule]))
    assert "rider_id_required" not in names
    assert sum("rider_id" in n for n in names) == 1
    assert "email_required" in names          # other required fields still get theirs


def test_a_field_level_not_null_rule_also_counts():
    names = _names(_contract([], field_rules=[{"name": "rider_id_nn", "sql": "rider_id IS NOT NULL"}]))
    assert "rider_id_required" not in names and "rider_id_nn" in names


def test_a_broader_rule_does_not_replace_it():
    # Not exactly `rider_id IS NOT NULL`: the required check must still run.
    names = _names(_contract([{"name": "both", "sql": "rider_id IS NOT NULL AND email IS NOT NULL"}]))
    assert "rider_id_required" in names


def test_without_a_written_rule_required_is_checked():
    assert "rider_id_required" in _names(_contract([]))


def test_enforce_required_false_keeps_field_level_rules():
    """`enforce_required: false` switches off the automatic required checks — not the
    field-level rules the author wrote, which it used to drop as well."""
    contract = _contract([], field_rules=[{"name": "rider_id_format", "sql": "rider_id LIKE 'R%'"}])
    contract.quality.enforce_required = False
    names = _names(contract)
    assert "rider_id_format" in names
    assert "rider_id_required" not in names and "email_required" not in names
