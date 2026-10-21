"""Key discovery from the data -- for estates with no contracts to declare one."""

import duckdb
import pytest

from lakelogic.core import profile_findings as pf
from lakelogic.core.profile import DuckDBExecutor, profile_table
from lakelogic.core.profile_keys import candidates, describe, discover_key


@pytest.fixture()
def con():
    c = duckdb.connect()
    c.execute("""CREATE TABLE order_lines AS
        SELECT o AS order_id, l AS line_no, (o * 7 + l) % 13 AS qty, 'sku-' || ((o + l) % 40) AS sku,
               1.5 * l AS price
        FROM range(200) t(o), range(1, 4) u(l)""")
    c.execute("""CREATE TABLE trips AS SELECT range AS trip_id, range % 7 AS driver_id FROM range(500)""")
    c.execute("""CREATE TABLE telemetry AS
        SELECT d AS driver_id, TIMESTAMP '2024-01-01' + to_minutes(m::INT) AS event_at, m % 3 AS speed_band
        FROM range(20) a(d), range(30) b(m)""")
    c.execute("""CREATE TABLE no_key AS SELECT range % 5 AS region_code, range % 3 AS status_code FROM range(100)
                 UNION ALL SELECT 1, 1""")
    return c


def _find(con, table):
    ex = DuckDBExecutor(conn=con)
    doc = profile_table(table, ex)
    rel = pf.Relation(table, lambda n: '"' + n + '"', lambda s, k: con.execute(s).fetchmany(k))
    names = [c["name"] for c in doc["columns"]]
    distincts = pf.exact_distincts(rel, names)
    return discover_key(rel, doc["columns"], distincts, doc["row_count"]), distincts, doc


def test_a_composite_key_is_found(con):
    found, _d, _doc = _find(con, "order_lines")
    assert found["columns"] == ["order_id", "line_no"]
    assert describe(found, sampled=False) == "Likely composite key: (order_id, line_no), unique on all rows (600 rows)."


def test_a_single_key_is_found_without_a_query(con):
    found, _d, _doc = _find(con, "trips")
    assert found["columns"] == ["trip_id"]
    assert found["tested"] == [["trip_id"]]


def test_an_event_table_is_keyed_by_who_and_when(con):
    found, _d, _doc = _find(con, "telemetry")
    assert found["columns"] == ["driver_id", "event_at"]


def test_no_key_is_stated_as_what_was_tried(con):
    found, _d, _doc = _find(con, "no_key")
    assert found["columns"] is None
    assert describe(found, sampled=False).startswith("No unique combination of up to 3 columns among")


def test_floats_and_columns_with_empty_values_are_never_key_parts(con):
    _found, d, doc = _find(con, "order_lines")
    assert "price" not in candidates(doc["columns"], d, doc["row_count"])
    cols = [{"name": "a_id", "type": "BIGINT", "null_count": 3}]
    assert candidates(cols, {"a_id": 100}, 100) == []


def test_combinations_that_cannot_be_unique_are_never_queried(con):
    found, _d, _doc = _find(con, "no_key")
    # 5 regions x 3 statuses = 15 < 101 rows: no query is spent on it.
    assert ["region_code", "status_code"] not in found["tested"]


def test_the_number_of_queries_is_bounded(con):
    ex = DuckDBExecutor(conn=con)
    doc = profile_table("order_lines", ex)
    rel = pf.Relation("order_lines", lambda n: '"' + n + '"', lambda s, k: con.execute(s).fetchmany(k))
    d = pf.exact_distincts(rel, [c["name"] for c in doc["columns"]])
    found = discover_key(rel, doc["columns"], d, doc["row_count"], max_tests=0)
    assert found["limit_reached"] is True and found["columns"] is None


def test_a_composite_key_stops_a_false_repeated_values_finding(con):
    from lakelogic.core.profile_assess import assess_table

    e = assess_table("order_lines", DuckDBExecutor(conn=con))
    titles = [f["title"] for f in e["findings"]]
    assert not any(t.startswith("Repeated") for t in titles)  # order_id repeats by design
    gap = next(f for f in e["findings"] if f["title"] == "No contract governs this table")
    assert "Likely composite key: (order_id, line_no), unique on all rows (600 rows)." in gap["evidence"]


def test_the_contract_gap_names_the_likely_key(con):
    from lakelogic.core.profile_assess import assess_table

    e = assess_table("order_lines", DuckDBExecutor(conn=con), contract={"info": {"title": "Order lines"}})
    gap = next(f for f in e["findings"] if f["title"] == "Contract is missing controls")
    assert "no primary key (likely (order_id, line_no), unique on all rows)" in gap["evidence"]


def test_a_declared_key_is_never_replaced_by_a_discovered_one(con):
    from lakelogic.core.profile_assess import assess_table

    e = assess_table("order_lines", DuckDBExecutor(conn=con), contract={"primary_key": ["order_id"]})
    assert "key_discovery" not in e["checks"]
    assert any(f["title"] == "Duplicate primary key (order_id)" for f in e["findings"])


def test_a_near_unique_key_with_repeats_is_duplicates_not_a_composite(con):
    from lakelogic.core.profile_assess import assess_table

    con.execute("INSERT INTO trips VALUES (5, 3)")  # trip_id 5 twice: 500 distinct of 501
    e = assess_table("trips", DuckDBExecutor(conn=con))
    assert any(f["title"] == "Repeated trip_id values" for f in e["findings"])


def test_a_date_alone_is_never_the_key_even_when_unique():
    import duckdb
    from lakelogic.core import profile_findings as pf
    from lakelogic.core.profile_keys import discover_key
    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT * FROM (VALUES ('r1','2026-10-01T01:00:00Z'),('r1','2026-10-02T01:00:00Z'),"
                "('r2','2026-10-03T01:00:00Z')) v(rider_id, signup_date)")
    cols = [{"name": "rider_id", "type": "VARCHAR", "null_count": 0}, {"name": "signup_date", "type": "VARCHAR", "null_count": 0}]
    rel = pf.Relation("t", lambda n: '"' + n + '"', lambda s, k: con.execute(s).fetchmany(k))
    found = discover_key(rel, cols, {"rider_id": 2, "signup_date": 3}, 3)
    assert found["columns"] != ["signup_date"]
    assert found["columns"] in (None, ["rider_id", "signup_date"])  # a date may be a PART beside an id
