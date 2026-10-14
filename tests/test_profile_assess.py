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
    "model": {"fields": [
        {"name": "trip_id", "type": "long", "required": True},
        {"name": "payment", "type": "string", "accepted_values": ["card", "cash"]},
    ]},
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


@pytest.mark.parametrize("path,table,expected", [
    ("raw/finance", "raw/finance/invoices", True),
    ("raw/fin", "raw/finance/invoices", False),
    ("main.sales", "main.sales.orders", True),
])
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
        return {"tables": [{"name": "t", "status": "profiled",
                            "profile": {"row_count": rc, "columns": [{"name": "c", "null_pct": null_pct}]}}]}
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
    rows = ["payment_id,method,paid_at"] + [f"{i},{'card' if i % 2 else 'cash'},2024-01-0{1 + i % 9} 10:00:00"
                                            for i in range(200)] + ["7,card,2024-01-05 10:00:00"]
    (d / "part-1.csv").write_text("\n".join(rows), encoding="utf-8")
    e = assess_files(str(d), name="landing/payments", contract={"primary_key": ["payment_id"],
                                                                 "info": {"title": "Payments"}})
    assert ("confirmed", "Duplicate primary key (payment_id)") in _titles(e)
    s = assess_files(str(d), settings={"mode": "sample", "sample_unit": "percent", "sample_percent": 10})
    assert s["profile"]["sampling"]["rows_available"] == 201
    assert s["profile"]["row_count"] == 21
