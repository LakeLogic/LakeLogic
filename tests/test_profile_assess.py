"""Profiling with findings, sampling and path selection -- in Core, so the CLI, a job in your own
platform and the LakeLogic service produce the same findings."""

import duckdb
import pytest

from lakelogic.core import profile_findings as pf
from lakelogic.core import profile_scope as ps
from lakelogic.core.profile import DuckDBExecutor
from lakelogic.core.profile_assess import assess_table, assess_tables


@pytest.fixture()
def ex():
    con = duckdb.connect()
    con.execute("""
        CREATE TABLE trips AS
        SELECT range AS trip_id,
               CASE WHEN range % 2 = 0 THEN 'card' ELSE 'cash' END AS payment,
               TIMESTAMP '2024-01-01' + to_minutes(range::INT) AS pickup_at,
               TIMESTAMP '2024-01-01' + to_minutes(range::INT + 10) AS dropoff_at,
               CASE WHEN range < 400 THEN NULL ELSE 'x' END AS note
        FROM range(1000)
    """)
    con.execute("INSERT INTO trips VALUES (5, 'card', TIMESTAMP '2024-02-01', TIMESTAMP '2024-01-01', 'x')")
    con.execute("UPDATE trips SET payment = 'crypto' WHERE trip_id BETWEEN 10 AND 16")
    return DuckDBExecutor(conn=con)


CONTRACT = {
    "info": {"title": "Trips", "domain": "mobility"},
    "primary_key": ["trip_id"],
    "model": {
        "fields": [
            {"name": "trip_id", "type": "long", "required": True},
            {"name": "payment", "type": "string", "accepted_values": ["card", "cash"]},
        ]
    },
}


def _titles(entry):
    return {(f["kind"], f["title"]) for f in entry["findings"]}


def test_a_full_assessment_confirms_what_the_contract_declares(ex):
    e = assess_table("trips", ex, contract=CONTRACT)
    t = _titles(e)
    assert ("confirmed", "Duplicate primary key (trip_id)") in t
    assert ("confirmed", "payment outside its accepted values") in t
    assert ("potential", "dropoff_at before pickup_at") in t
    assert ("potential", "note is mostly empty") in t
    assert e["contract"] == {"name": "Trips"}
    assert all(f["domain"] == "mobility" for f in e["findings"])
    assert e["finding_counts"]["confirmed"] == 2


def test_rare_values_are_never_shown(ex):
    e = assess_table("trips", ex, contract=CONTRACT)
    acc = e["checks"]["accepted"][0]
    assert acc["values"] == [{"value": "crypto", "count": 7}]  # seen 7 times: shown
    assert acc["values_hidden"] == 0


def test_without_a_contract_the_table_is_a_control_gap(ex):
    t = _titles(assess_table("trips", ex))
    assert ("gap", "No contract governs this table") in t
    assert ("potential", "Repeated trip_id values") in t  # a key found, not declared


def test_a_percentage_sample_reads_that_share_and_says_so(ex):
    e = assess_table("trips", ex, settings={"mode": "sample", "sample_unit": "percent", "sample_percent": 10})
    assert e["profile"]["row_count"] == 101  # ceil(1001 * 10%)
    assert e["profile"]["sampling"]["selection"] == "newest 10% by pickup_at"
    assert all("in the sample" in f["evidence"] for f in e["findings"] if f["kind"] != "gap")


def test_a_row_sample_at_random(ex):
    e = assess_table("trips", ex, settings={"mode": "sample", "sample_rows": 1000, "sample_order": "random"})
    assert e["profile"]["row_count"] == 1000
    assert e["profile"]["sampling"]["method"] == "sample"


def test_a_run_reads_the_tables_under_a_path_and_summarises(ex):
    ex.conn.execute("CREATE SCHEMA sales")
    ex.conn.execute("CREATE TABLE sales.orders AS SELECT range AS order_id FROM range(10)")
    ex.conn.execute("CREATE TABLE sales.refunds AS SELECT range AS refund_id FROM range(3)")
    run = assess_tables(["trips", "sales.orders", "sales.refunds"], ex, settings={"paths": ["sales"]})
    assert sorted(t["name"] for t in run["tables"]) == ["sales.orders", "sales.refunds"]
    assert run["summary"]["tables_profiled"] == 2
    assert run["summary"]["tables_over_cap"] == 1  # one table not in this run
    assert run["summary"]["settings"]["label"] == "full scan"


def test_one_unreadable_table_does_not_end_the_run(ex):
    run = assess_tables(["trips", "missing_table"], ex)
    status = {t["name"]: t["status"] for t in run["tables"]}
    assert status == {"trips": "profiled", "missing_table": "failed"}


def test_text_ranges_are_hidden(ex):
    e = assess_table("trips", ex)
    pay = next(c for c in e["profile"]["columns"] if c["name"] == "payment")
    assert pay["min"] is None and pay["max"] is None and pay["range_hidden"] == "text"


@pytest.mark.parametrize(
    "path,table,expected",
    [
        ("raw/finance", "raw/finance/invoices", True),
        ("raw/fin", "raw/finance/invoices", False),
        ("main.sales", "main.sales.orders", True),
    ],
)
def test_one_path_rule_for_buckets_and_warehouses(path, table, expected):
    assert ps.under_path(table, path) is expected


def test_choices_are_refused_with_the_reason():
    every = ["a/x", "a/y", "b/z"]
    with pytest.raises(ValueError, match="No tables under: c"):
        ps.choose_tables(every, ps.normalize_settings({"paths": ["c"]}))
    with pytest.raises(ValueError, match="2 tables chosen; the limit per run is 1"):
        ps.choose_tables(every, ps.normalize_settings({"paths": ["a"]}), 1)
    with pytest.raises(ValueError, match="0.1% to 100%"):
        ps.normalize_settings({"mode": "sample", "sample_unit": "percent", "sample_percent": 0})


def test_change_since_the_previous_run():
    def run(null_pct, rc):
        return {
            "tables": [
                {
                    "name": "t",
                    "status": "profiled",
                    "profile": {"row_count": rc, "columns": [{"name": "c", "null_pct": null_pct}]},
                }
            ]
        }

    d = ps.diff(run(30.0, 120), run(10.0, 100))
    assert d["tables"][0]["row_count_change"] == 20
    assert d["tables"][0]["null_shifts"][0]["delta_points"] == 20.0
    assert ps.diff(run(1, 1), None) is None


def test_the_findings_module_is_self_contained():
    assert pf.KIND_ORDER == ("confirmed", "potential", "gap")


def test_a_folder_of_files_gets_the_same_findings(tmp_path):
    from lakelogic.core.profile_assess import assess_files

    d = tmp_path / "landing" / "payments"
    d.mkdir(parents=True)
    rows = (
        ["payment_id,method,paid_at"]
        + [f"{i},{'card' if i % 2 else 'cash'},2024-01-0{1 + i % 9} 10:00:00" for i in range(200)]
        + ["7,card,2024-01-05 10:00:00"]
    )
    (d / "part-1.csv").write_text("\n".join(rows), encoding="utf-8")
    e = assess_files(
        str(d), name="landing/payments", contract={"primary_key": ["payment_id"], "info": {"title": "Payments"}}
    )
    assert ("confirmed", "Duplicate primary key (payment_id)") in _titles(e)
    s = assess_files(str(d), settings={"mode": "sample", "sample_unit": "percent", "sample_percent": 10})
    assert s["profile"]["sampling"]["rows_available"] == 201
    assert s["profile"]["row_count"] == 21


def test_an_identifier_is_never_asked_for_accepted_values(ex):
    ex.conn.execute(
        "CREATE TABLE pings AS SELECT 'drv-' || (range % 4) AS driver_id, "
        "CASE WHEN range % 2 = 0 THEN 'on' ELSE 'off' END AS status FROM range(200)"
    )
    e = assess_table("pings", ex, contract={"info": {"title": "Pings"}, "primary_key": ["driver_id"]})
    gap = next(f for f in e["findings"] if f["title"] == "Contract is missing controls")
    assert "status" in gap["evidence"] and "driver_id" not in gap["columns"]


def test_free_text_is_never_asked_for_accepted_values(ex):
    ex.conn.execute(
        "CREATE TABLE tickets AS SELECT CASE WHEN range % 2 = 0 THEN 'open' ELSE 'closed' END AS status, "
        "CASE WHEN range % 2 = 0 THEN 'call back' ELSE 'resolved' END AS notes FROM range(100)"
    )
    e = assess_table("tickets", ex, contract={"info": {"title": "T"}, "primary_key": ["status"]})
    gap = next(f for f in e["findings"] if f["title"] == "Contract is missing controls")
    assert "status" in gap["columns"] and "notes" not in gap["columns"]


def test_one_value_seen_is_not_suggested_as_accepted_values():
    """2026-10-20, seen in the UI: "status · seen: active" was offered as an accepted-values gap.
    One value in a sample is what the sample held, not every value the column allows."""
    from lakelogic.core.profile_findings import is_category_candidate

    one = {"name": "status", "type": "VARCHAR", "frequencies": [{"value": "active", "count": 101}]}
    two = {
        "name": "vehicle_type",
        "type": "VARCHAR",
        "frequencies": [{"value": "suv", "count": 5}, {"value": "van", "count": 3}],
    }
    assert is_category_candidate(one) is False
    assert is_category_candidate(two) is True
    assert is_category_candidate({**two, "name": "app_version"}) is False
    assert is_category_candidate({**two, "frequencies": [{"value": "DROP TABLE x;"}, {"value": "a b c"}]}) is False
