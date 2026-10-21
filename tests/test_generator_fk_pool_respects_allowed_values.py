"""A foreign-key pool never overrides a column's own allowed values (2026-10-20).

A trip's `city_code` links to the cities table AND must be one of seven codes. The pool from the
generated cities replaced the allowed list, so valid trips carried codes the rule rejected and
~146 good rows per table were quarantined ("lost") in every RideFlow test run.
"""
import yaml

from lakelogic.core.generator import DataGenerator

CONTRACT = {
    "version": "1.0.0",
    "dataset": "trips",
    "model": {"fields": [
        {"name": "trip_id", "type": "string", "required": True},
        {"name": "city_code", "type": "string",
         "foreign_key": {"contract": "cities", "column": "city_code"}},
    ]},
    "quality": {"row_rules": [
        {"name": "city_code_accepted_values", "sql": "city_code IS NULL OR city_code IN ('BER', 'LON', 'NYC')"},
    ]},
}


def _gen(tmp_path):
    path = tmp_path / "trips.yaml"
    path.write_text(yaml.safe_dump(CONTRACT), encoding="utf-8")
    return DataGenerator(str(path))


def test_the_pool_is_narrowed_to_the_allowed_values(tmp_path):
    df = _gen(tmp_path).generate(
        rows=200, reference_data={"city_code": ["BER", "LON", "MAD", "ROM", "OSL"]})
    codes = {c for c in df["city_code"].to_list() if c is not None}
    assert codes and codes <= {"BER", "LON"}


def test_with_no_overlap_the_allowed_values_win(tmp_path):
    df = _gen(tmp_path).generate(rows=200, reference_data={"city_code": ["MAD", "ROM"]})
    codes = {c for c in df["city_code"].to_list() if c is not None}
    assert codes and codes <= {"BER", "LON", "NYC"}


def test_a_quoted_column_name_is_read_like_a_bare_one():
    # `"reason_code" IS NOT NULL` was not understood, so a valid row could leave the key empty.
    from lakelogic.core.generator import _sql_rule_constraints

    assert _sql_rule_constraints('"reason_code" IS NOT NULL')[0] == (("reason_code", "not_null", True),)
    assert _sql_rule_constraints("`reason_code` IS NOT NULL")[0] == (("reason_code", "not_null", True),)
    quoted_in = _sql_rule_constraints('"city_code" IS NULL OR "city_code" IN (\'A\', \'B\')')[0]
    assert ("city_code", "accepted_values", ("A", "B")) in quoted_in or any(
        c[0] == "city_code" and c[1] == "accepted_values" and list(c[2]) == ["A", "B"] for c in quoted_in)
