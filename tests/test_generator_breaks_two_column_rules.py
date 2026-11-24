"""A test of a two-column rule must contain rows that break it.

Only single-column rules (required, range, accepted values, length, pattern) were ever chosen as
the rule a broken row breaks, and every coherence pass put comparisons back in order. So a
sandbox test of ``dropoff_at >= pickup_at`` generated 200 rows and reported "caught 0" on every
run (2026-11-24). Comparisons are now targets like any declared rule.
"""
import pytest
import yaml

from lakelogic.core.generator import DataGenerator

RULES = [
    "dropoff_at IS NULL OR pickup_at IS NULL OR dropoff_at >= pickup_at",
    "impressions IS NULL OR clicks IS NULL OR clicks <= impressions",
    "end_date IS NULL OR start_date IS NULL OR end_date > start_date",
]


def _contract(tmp_path, rules=RULES):
    doc = {
        "version": "1.0.0",
        "info": {"title": "comparisons", "table_name": "trips"},
        "model": {
            "fields": [
                {"name": "trip_id", "type": "string", "required": True},
                {"name": "pickup_at", "type": "timestamp"},
                {"name": "dropoff_at", "type": "timestamp"},
                {"name": "impressions", "type": "integer"},
                {"name": "clicks", "type": "integer"},
                {"name": "start_date", "type": "date"},
                {"name": "end_date", "type": "date"},
            ]
        },
        "quality": {"row_rules": [{"name": f"rule_{i}", "sql": sql} for i, sql in enumerate(rules)]},
    }
    path = tmp_path / "trips.yaml"
    path.write_text(yaml.safe_dump(doc), encoding="utf-8")
    return path


def _failures(frame):
    duckdb = pytest.importorskip("duckdb")
    trips = frame.to_arrow()  # noqa: F841 - read by duckdb
    con = duckdb.connect()
    con.execute(
        "CREATE TABLE t AS SELECT * REPLACE ("
        "TRY_CAST(pickup_at AS TIMESTAMP) AS pickup_at, TRY_CAST(dropoff_at AS TIMESTAMP) AS dropoff_at, "
        "TRY_CAST(start_date AS DATE) AS start_date, TRY_CAST(end_date AS DATE) AS end_date"
        ") FROM trips"
    )
    return {
        sql: con.execute(f"SELECT count(*), count(*) FILTER (WHERE _is_invalid) FROM t WHERE ({sql}) IS NOT TRUE").fetchone()
        for sql in RULES
    }


@pytest.mark.parametrize("seed", [1, 7, 42])
def test_broken_rows_break_every_two_column_rule(tmp_path, seed):
    frame = DataGenerator(str(_contract(tmp_path)), seed=seed).generate(rows=600, invalid_ratio=0.2)
    for sql, (failing, failing_invalid) in _failures(frame).items():
        assert failing > 0, f"no row breaks {sql!r}: a test of it would catch 0"
        # Every row that breaks a rule was built to: valid rows still pass it.
        assert failing == failing_invalid, f"a valid row breaks {sql!r}"


def test_the_break_is_reported_as_a_comparison_test_case(tmp_path):
    frame = DataGenerator(str(_contract(tmp_path)), seed=3).generate(rows=400, invalid_ratio=0.2)
    types = ",".join(v for v in frame["_test_case_types"].to_list() if v)
    assert "COMPARISON_VIOLATION" in types


def test_with_no_rows_asked_to_break_nothing_breaks(tmp_path):
    frame = DataGenerator(str(_contract(tmp_path)), seed=5).generate(rows=300, invalid_ratio=0.0)
    assert all(failing == 0 for failing, _ in _failures(frame).values())


def test_a_timestamp_break_moves_the_later_column_before_the_earlier():
    gen = DataGenerator({"pickup_at": "timestamp", "dropoff_at": "timestamp"}, seed=11)
    row = {"pickup_at": "2026-11-20T10:00:00", "dropoff_at": "2026-11-20T10:30:00"}
    info = gen._break_comparison(row, ("pickup_at", "<=", "dropoff_at"), {})
    assert row["dropoff_at"] < row["pickup_at"]
    assert info.type == "COMPARISON_VIOLATION" and info.description == "dropoff_at before pickup_at"


def test_a_number_break_puts_the_smaller_column_above_the_larger():
    gen = DataGenerator({"clicks": "integer", "impressions": "integer"}, seed=11)
    row = {"clicks": 3, "impressions": 10}
    gen._break_comparison(row, ("clicks", "<=", "impressions"), {})
    assert row["clicks"] > row["impressions"]


def test_a_null_column_is_filled_first_so_the_guard_cannot_pass_it():
    gen = DataGenerator({"pickup_at": "timestamp", "dropoff_at": "timestamp"}, seed=2)
    row = {"pickup_at": None, "dropoff_at": "2026-11-20T10:30:00"}
    gen._break_comparison(row, ("pickup_at", "<=", "dropoff_at"), {})
    assert row["pickup_at"] is not None and row["dropoff_at"] < row["pickup_at"]
