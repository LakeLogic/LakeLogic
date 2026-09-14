"""Generated rows must pass the contract's own SQL row rules.

Contracts write most rules as SQL (`surge IS NULL OR (surge >= 1 AND surge <= 10)`). The generator
only read a bare `col IN (...)`, so a RideFlow run quarantined 370 of 600 trip requests on
`surge_in_range`, 366 trips on `valid_rating` and 357 cancellations on `valid_cancelled_by` —
data failing the very contract it was generated from.
"""

import pytest
import yaml

from lakelogic.core import generator as gen
from lakelogic.core.generator import DataGenerator, _sql_rule_constraints


def test_nullable_in_list_is_an_accepted_values_domain_that_allows_nulls():
    constraints, comparisons = _sql_rule_constraints("cancelled_by IS NULL OR cancelled_by IN ('rider','driver')")
    assert constraints == (("cancelled_by", "accepted_values", ("rider", "driver")),)
    assert comparisons == ()


def test_guarded_range_and_between_become_min_and_max():
    assert _sql_rule_constraints("surge IS NULL OR (surge >= 1.0 AND surge <= 10.0)") == (
        (("surge", "min", 1.0), ("surge", "max", 10.0)),
        (),
    )
    assert _sql_rule_constraints("rating BETWEEN 1 AND 5")[0] == (
        ("rating", "min", 1),
        ("rating", "max", 5),
        ("rating", "not_null", True),
        ("rating", "not_null", True),
    )


def test_an_unguarded_rule_rejects_nulls():
    assert ("fare_amount", "not_null", True) in _sql_rule_constraints("fare_amount >= 0")[0]
    assert _sql_rule_constraints("licence_number IS NOT NULL")[0] == (("licence_number", "not_null", True),)
    assert _sql_rule_constraints("email like '%@%'")[0] == (("email", "not_null", True),)
    assert _sql_rule_constraints("0 < `amount`")[0] == (("amount", "min_exclusive", 0), ("amount", "not_null", True))


def test_two_column_rules_are_comparisons_with_the_smaller_column_first():
    constraints, comparisons = _sql_rule_constraints(
        "requested_at IS NULL OR cancelled_at IS NULL OR cancelled_at >= requested_at"
    )
    assert comparisons == (("requested_at", "<=", "cancelled_at"),)
    assert constraints == (), "both columns are guarded, so neither is forced non-null"
    assert _sql_rule_constraints("impressions IS NULL OR clicks IS NULL OR clicks <= impressions")[1] == (
        ("clicks", "<=", "impressions"),
    )


@pytest.mark.parametrize(
    "sql",
    [
        "status = 'active' OR amount > 10",  # neither side alone is required
        "agent_id IN (SELECT id FROM agents)",
        "LENGTH(code) = 3",
    ],
)
def test_rules_it_cannot_read_yield_nothing(sql):
    constraints, comparisons = _sql_rule_constraints(sql)
    assert [c for c in constraints if c[1] != "not_null"] == []
    assert comparisons == ()


RULES = [
    "surge_multiplier IS NULL OR (surge_multiplier >= 1.0 AND surge_multiplier <= 10.0)",
    "rider_rating IS NULL OR (rider_rating >= 1.0 AND rider_rating <= 5.0)",
    "cancelled_by IS NULL OR cancelled_by IN ('rider','driver')",
    "fare_amount >= 0",
    "trip_count > 0",
    "licence_number IS NOT NULL",
    "email like '%@%'",
    "requested_at IS NULL OR cancelled_at IS NULL OR cancelled_at >= requested_at",
    "impressions IS NULL OR clicks IS NULL OR clicks <= impressions",
]


def _contract(tmp_path):
    doc = {
        "version": "1.0.0",
        "info": {"title": "sql rules", "table_name": "trips"},
        "model": {
            "fields": [
                {"name": "trip_id", "type": "string", "required": True},
                {"name": "surge_multiplier", "type": "double"},
                {"name": "rider_rating", "type": "double"},
                {"name": "cancelled_by", "type": "string"},
                {"name": "fare_amount", "type": "double"},
                {"name": "trip_count", "type": "integer"},
                {"name": "licence_number", "type": "string"},
                {"name": "email", "type": "string"},
                {"name": "requested_at", "type": "timestamp"},
                {"name": "cancelled_at", "type": "timestamp"},
                {"name": "impressions", "type": "integer"},
                {"name": "clicks", "type": "integer"},
            ]
        },
        "quality": {"row_rules": [{"name": f"rule_{i}", "sql": sql} for i, sql in enumerate(RULES)]},
    }
    path = tmp_path / "trips.yaml"
    path.write_text(yaml.safe_dump(doc), encoding="utf-8")
    return path


@pytest.mark.parametrize("seed", [1, 7, 42])
def test_every_valid_generated_row_passes_every_sql_rule(tmp_path, seed):
    duckdb = pytest.importorskip("duckdb")
    frame = DataGenerator(str(_contract(tmp_path)), seed=seed).generate(rows=400)
    trips = frame.drop([c for c in frame.columns if c.startswith("_")]).to_arrow()  # noqa: F841 - read by duckdb
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE t AS SELECT * REPLACE ("
        "TRY_CAST(requested_at AS TIMESTAMP) AS requested_at, TRY_CAST(cancelled_at AS TIMESTAMP) AS cancelled_at"
        ") FROM trips"
    )
    failures = {sql: con.execute(f"SELECT count(*) FROM t WHERE ({sql}) IS NOT TRUE").fetchone()[0] for sql in RULES}
    assert failures == {sql: 0 for sql in RULES}


def test_a_name_profile_does_not_override_a_declared_range(monkeypatch):
    # `rating` has a Faker/distribution profile; the contract's range must still win.
    monkeypatch.setattr(gen, "_match_distribution", lambda name: {"distribution": "fixed"})
    monkeypatch.setattr(DataGenerator, "_sample_distribution", lambda self, profile: 999.0)
    instance = DataGenerator({"rider_rating": "double"}, seed=3)
    value = instance._make_valid_value("rider_rating", "double", {"min": 1.0, "max": 5.0}, nullable=False)
    assert 1.0 <= value <= 5.0
