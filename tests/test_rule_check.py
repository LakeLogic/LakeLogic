"""Checking a table against its contract's rules without a pipeline (#570).

Real SQL through DuckDB: a row fails a rule when the rule is false OR null, as the pipeline
keeps only rows where it is true; one bad rule cannot hide the others.
"""

from __future__ import annotations

import duckdb

from lakelogic.core.rule_check import RECORD_TYPE, check_rules, run_log_payload


class Duck:
    def __init__(self):
        self.con = duckdb.connect()
        self.con.execute("""CREATE TABLE drivers AS SELECT * FROM (VALUES
            ('d1', 'a@x.com', 'L1', 4.5), ('d2', 'nope', 'L2', 3.0), ('d3', NULL, NULL, 6.0),
            ('d4', 'b@y.com', 'L4', NULL)) t(driver_id, email, licence, rating)""")

    def aggregate_row(self, sql):
        cur = self.con.execute(sql)
        names = [d[0] for d in cur.description]
        return dict(zip(names, cur.fetchone()))


CONTRACT = {"quality": {
    "row_rules": [
        {"name": "valid_email", "sql": "email like '%@%'", "category": "validity"},
        {"name": "licence_required", "sql": "licence IS NOT NULL", "category": "completeness"},
        {"name": "valid_rating", "sql": "rating IS NULL OR (rating >= 1 AND rating <= 5)"},
    ],
    "dataset_rules": [{"name": "unique_driver", "sql": "SELECT ..."}],
}}


def _by(result):
    return {r["name"]: r for r in result["rule_results"]}


def test_each_rule_counts_its_failing_rows_and_null_fails():
    r = check_rules(Duck(), "drivers", CONTRACT)
    out = _by(r)
    assert r["rows_checked"] == 4
    assert out["valid_email"]["status"] == "failed" and out["valid_email"]["failed_rows"] == 2  # 'nope' + NULL
    assert out["licence_required"]["failed_rows"] == 1
    assert out["valid_rating"]["failed_rows"] == 1 and out["valid_rating"]["rows_evaluated"] == 4
    assert out["unique_driver"]["status"] == "not_evaluated"  # dataset rules: not guessed at
    assert r["rules_fingerprint"] and r["measurement_version"] == 1


def test_a_bad_rule_is_not_evaluated_and_does_not_hide_the_others():
    bad = {"quality": {"row_rules": CONTRACT["quality"]["row_rules"] + [{"name": "broken", "sql": "no_such_col > 1"}]}}
    r = check_rules(Duck(), "drivers", bad)
    out = _by(r)
    assert out["broken"]["status"] == "not_evaluated" and out["broken"]["failed_rows"] is None
    assert out["valid_email"]["failed_rows"] == 2
    assert r["errors"] and r["errors"][0]["rule"] == "broken"


def test_a_passing_rule_passes():
    ok = {"quality": {"row_rules": [{"name": "has_id", "sql": "driver_id IS NOT NULL"}]}}
    assert _by(check_rules(Duck(), "drivers", ok))["has_id"] == {
        "name": "has_id", "kind": "row", "category": None, "status": "passed", "failed_rows": 0, "rows_evaluated": 4}


def test_the_payload_is_a_check_not_a_pipeline_run():
    r = check_rules(Duck(), "drivers", CONTRACT)
    p = run_log_payload("silver_drivers", r, started_at="2026-10-20T01:00:00Z", finished_at="2026-10-20T01:00:02Z",
                        environment="dev", source="platform")
    assert p["metadata"]["record_type"] == RECORD_TYPE and p["engine"] == "quality_check"
    assert p["status"] == "success"  # rules failing is a RESULT of a check that ran
    assert p["rules_evaluated"] == 3 and p["rules_failed"] == 3
    assert p["metadata"]["source"] == "platform" and p["metadata"]["rule_results"]


def test_a_check_that_could_not_run_any_rule_is_a_failed_check():
    broken = {"quality": {"row_rules": [{"name": "x", "sql": "missing_col = 1"}]}}
    r = check_rules(Duck(), "drivers", broken)
    p = run_log_payload("t", r, started_at="a", finished_at="b")
    assert p["status"] == "failed" and p["rules_evaluated"] == 0 and p["error_message"]


# ── the client path (#570) ────────────────────────────────────────────────────


def test_check_contract_reports_under_the_runs_dataset_name_and_sends(monkeypatch):
    import lakelogic.core.rule_check as rc

    sent = []
    monkeypatch.setattr(rc, "send", lambda payload, endpoint, api_key, **kw: sent.append((payload, endpoint, api_key)) or True)
    doc = {"dataset": "silver_drivers", "version": "1.0.0", "info": {"table_name": "drivers"}, **CONTRACT}
    out = rc.check_contract(doc, duckdb_con=Duck().con, endpoint="https://x/ingest", api_key="k", environment="dev")
    assert out["sent"] is True
    payload, endpoint, key = sent[0]
    assert payload["dataset"] == "silver_drivers"  # the name a pipeline run sends, so one history
    assert payload["metadata"]["source"] == "client" and payload["metadata"]["environment"] == "dev"
    assert endpoint == "https://x/ingest" and key == "k"


def test_without_an_endpoint_nothing_is_sent(monkeypatch):
    import lakelogic.core.rule_check as rc

    monkeypatch.delenv("LAKELOGIC_OBSERVATORY_ENDPOINT", raising=False)
    out = rc.check_contract({"info": {"table_name": "drivers"}, **CONTRACT}, duckdb_con=Duck().con)
    assert out["sent"] is False and out["result"]["rows_checked"] == 4


def test_send_buffers_on_an_outage_and_never_raises(monkeypatch, tmp_path):
    import requests

    import lakelogic.core.rule_check as rc

    def boom(*a, **k):
        raise requests.ConnectionError("down")

    monkeypatch.setattr(requests, "post", boom)
    assert rc.send({"dataset": "t"}, "https://x/ingest", "k", spool={"dir": str(tmp_path)}) is False
    assert any(tmp_path.iterdir())  # buffered for a later run


def test_spark_executor_reads_one_row():
    import lakelogic.core.rule_check as rc

    class Row:
        def asDict(self):
            return {"rows_checked": 3}

    class Spark:
        def sql(self, s):
            return type("DF", (), {"first": lambda self: Row()})()

    assert rc.SparkExecutor(Spark()).aggregate_row("SELECT 1") == {"rows_checked": 3}


def test_a_templated_name_reports_under_the_contract_file_stem(tmp_path, monkeypatch):
    import yaml

    import lakelogic.core.rule_check as rc

    monkeypatch.delenv("LAKELOGIC_OBSERVATORY_ENDPOINT", raising=False)
    f = tmp_path / "silver_rideflow_driver_profiles_v1.0.yaml"
    f.write_text(yaml.safe_dump({"info": {"table_name": "{silver_layer}_rideflow_driver_profiles"}, **CONTRACT}))
    out = rc.check_contract(str(f), duckdb_con=Duck().con, table="drivers")
    assert out["payload"]["dataset"] == "silver_rideflow_driver_profiles"



def test_results_land_in_a_local_table_one_row_per_rule(monkeypatch):
    import lakelogic.core.rule_check as rc

    monkeypatch.delenv("LAKELOGIC_OBSERVATORY_ENDPOINT", raising=False)
    d = Duck()
    out = rc.check_contract({"dataset": "silver_drivers", **CONTRACT}, duckdb_con=d.con, table="drivers",
                            results_table="lakelogic_quality_checks", environment="dev")
    assert out["results_table"] == "lakelogic_quality_checks" and out["results_table_error"] is None
    rows = d.con.execute("SELECT rule_name, status, failed_rows, rows_checked, dataset, source "
                         "FROM lakelogic_quality_checks ORDER BY rule_name").fetchall()
    assert ("valid_email", "failed", 2, 4, "silver_drivers", "client") in rows
    assert len(rows) == 4  # 3 row rules + 1 dataset rule, one row each
    rc.check_contract({"dataset": "silver_drivers", **CONTRACT}, duckdb_con=d.con, table="drivers",
                      results_table="lakelogic_quality_checks")
    assert d.con.execute("SELECT COUNT(DISTINCT check_id) FROM lakelogic_quality_checks").fetchone()[0] == 2  # appended


def test_a_failed_results_write_never_fails_the_check():
    import lakelogic.core.rule_check as rc

    err = rc.write_results([{"rule_name": "x"}], "no_such_schema.t", duckdb_con=Duck().con)
    assert isinstance(err, str) and err


class TestIncrementalScope:
    """Only changed rows when that is still correct; a full check when it is not."""

    NOW = "2026-10-20T10:00:00+00:00"

    def plan(self, last, version=12, fp="a"):
        from lakelogic.core.rule_check import plan_scope

        return plan_scope(last=last, current_version=version, fingerprint=fp, now_iso=self.NOW)

    def last(self, **kw):
        return {"to_version": 10, "rules_fingerprint": "a", "last_full_at": "2026-10-18T10:00:00+00:00", **kw}

    def test_changed_rows_since_the_last_checked_version(self):
        assert self.plan(self.last()) == {"scope": "changed", "from_version": 11, "reason": "rows changed since version 10"}

    def test_nothing_written_since_is_skipped(self):
        assert self.plan(self.last(), version=10)["scope"] == "skip"

    def test_first_check_and_non_delta_are_full(self):
        assert self.plan(None)["scope"] == "full"
        assert self.plan(self.last(), version=None)["reason"].startswith("not a Delta table")

    def test_changed_rules_force_a_full_check(self):
        assert self.plan(self.last(), fp="b") == {"scope": "full", "reason": "the rules changed"}

    def test_a_full_check_at_least_weekly(self):
        assert self.plan(self.last(last_full_at="2026-10-12T10:00:00+00:00"))["reason"] == "last full check 8 days ago"


def test_a_failing_rule_forces_a_full_check_once_anything_is_written():
    """New good rows passing must not read as the table recovering: the bad rows are older."""
    from lakelogic.core.rule_check import plan_scope

    last = {"to_version": 10, "rules_fingerprint": "a", "last_full_at": "2026-10-19T10:00:00+00:00",
            "verdicts": [{"rule_name": "rating_range_max", "status": "failed"}]}
    out = plan_scope(last=last, current_version=11, fingerprint="a", now_iso="2026-10-20T10:00:00+00:00")
    assert out["scope"] == "full" and "was failing" in out["reason"]
    # Nothing written: the failing verdict is carried forward, not re-read.
    assert plan_scope(last=last, current_version=10, fingerprint="a", now_iso="2026-10-20T10:00:00+00:00")["scope"] == "skip"


def test_the_results_table_is_named_by_the_metadata_standard():
    from lakelogic.core.metadata_names import legacy_table_names
    from lakelogic.core.rule_check import results_table_name

    assert results_table_name("cat.lakelogic") == "cat.lakelogic.lakelogic_quality_checks"
    assert results_table_name("lakelogic", backend="fabric") == "lakelogic.lakelogic_quality_checks"
    assert "_quality_checks" in legacy_table_names("quality_checks")


def test_an_identical_repeated_rule_is_checked_and_reported_once():
    from lakelogic.core.rule_check import _row_rules
    rule = {"name": "driver_id_not_null", "sql": "driver_id IS NOT NULL", "category": "completeness"}
    doc = {"quality": {"row_rules": [rule, dict(rule), {"name": "status_not_null", "sql": "status IS NOT NULL"}]}}
    assert [r["name"] for r in _row_rules(doc)] == ["driver_id_not_null", "status_not_null"]


def test_dataset_rules_are_judged_through_the_engine_expansion_on_the_whole_table():
    import duckdb
    from lakelogic.core.rule_check import DuckDBExecutor, check_rules
    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1,'a'),(1,'b'),(2,NULL),(3,'c')) v(id, phone)")
    doc = {"quality": {"row_rules": [{"name": "id_set", "sql": "id IS NOT NULL"}],
                       "dataset_rules": [{"unique": "id"}, {"null_ratio": {"field": "phone", "max": 0.5}},
                                         {"null_ratio": {"field": "phone", "max": 0.1, "name": "phone_mostly_set"}}]}}
    res = {r["name"]: r for r in check_rules(DuckDBExecutor(con), "t", doc)["rule_results"]}
    assert res["id_unique"]["status"] == "failed" and res["id_unique"]["failed_rows"] == 1
    assert res["phone_null_ratio"]["status"] == "passed"
    assert res["phone_mostly_set"]["status"] == "failed" and res["phone_mostly_set"]["failed_rows"] is None
    # an incremental check: rows from a subset, but uniqueness judged over the whole table
    sub = {r["name"]: r for r in check_rules(DuckDBExecutor(con), "(SELECT * FROM t WHERE id = 2) s", doc,
                                             dataset_table="t")["rule_results"]}
    assert sub["id_unique"]["status"] == "failed"


def test_a_dataset_rule_form_that_cannot_be_expanded_is_not_evaluated_never_passed():
    import duckdb
    from lakelogic.core.rule_check import DuckDBExecutor, check_rules
    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT 1 AS id")
    res = check_rules(DuckDBExecutor(con), "t", {"quality": {"dataset_rules": [{"name": "odd", "weird": 1}]}})
    assert [(r["name"], r["status"]) for r in res["rule_results"]] == [("odd", "not_evaluated")]


def test_a_null_ratio_of_zero_is_a_bound_not_a_missing_one():
    import duckdb
    from lakelogic.core.rule_check import DuckDBExecutor, check_rules
    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1,'a'),(2,NULL)) v(id, status)")
    res = check_rules(DuckDBExecutor(con), "t", {"quality": {"dataset_rules": [{"null_ratio": {"field": "status", "max": 0}}]}})
    assert [(r["name"], r["status"]) for r in res["rule_results"]] == [("status_null_ratio", "failed")]


def test_a_version_means_nothing_across_a_recreated_or_rewritten_table():
    from lakelogic.core.rule_check import plan_scope
    last = {"to_version": 40, "table_id": "a", "rules_fingerprint": "f", "last_full_at": "2026-10-20T00:00:00",
            "verdicts": [{"status": "passed"}]}
    now = "2026-10-21T00:00:00"
    plan = lambda **k: plan_scope(last=last, fingerprint="f", now_iso=now, **k)
    assert plan(current_version=41, table_id="a")["scope"] == "changed"
    assert plan(current_version=41, table_id="b")["reason"] == "the table was recreated"
    assert plan(current_version=3, table_id=None)["reason"] == "the table's history was rewritten"
    assert plan(current_version=41, table_id="a", operation="RESTORE")["scope"] == "full"
    assert plan(current_version=41, table_id="a", operation="CREATE OR REPLACE TABLE AS SELECT")["scope"] == "full"
    assert plan(current_version=41, table_id="a", operation="WRITE")["scope"] == "changed"
    assert plan(current_version=40, table_id="a", operation="RESTORE")["scope"] == "skip"  # already judged


def test_the_platform_memory_drives_the_same_decision_as_the_results_table():
    from lakelogic.core.rule_check import memory_from_payload, plan_scope
    payload = {"started_at": "2026-10-20T06:00:00", "metadata": {"scope": "full", "to_version": 7, "table_id": "a",
               "rules_fingerprint": "f", "rule_results": [{"name": "r", "kind": "row", "status": "passed"}]}}
    mem = memory_from_payload(payload)
    assert mem["last_full_at"] == "2026-10-20T06:00:00" and mem["verdicts"][0]["rule_name"] == "r"
    nxt = memory_from_payload({**payload, "started_at": "2026-10-20T07:00:00",
                               "metadata": {**payload["metadata"], "scope": "changed", "to_version": 8}}, mem)
    assert nxt["last_full_at"] == "2026-10-20T06:00:00"  # a changed-rows check never resets the full clock
    assert plan_scope(last=nxt, current_version=9, fingerprint="f", now_iso="2026-10-21T00:00:00",
                      table_id="a")["scope"] == "changed"
