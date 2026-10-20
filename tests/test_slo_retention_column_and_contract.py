"""Retention measures a record's AGE, and a contract can declare its own period (2026-10-19).

Retention used to borrow freshness's columns (``updated_at`` first) and only the domain's
per-layer period. Freshness wants the newest CHANGE; retention the date the clock STARTED.
And OLC's ``compliance.retention`` lets one contract be held to its own period.
"""

from __future__ import annotations

import datetime
from types import SimpleNamespace

from lakelogic.core.slo import (
    STATUS_BREACHED,
    STATUS_WARN,
    SLOValidator,
    _contract_retention,
    retention_columns,
)

NOW = datetime.datetime.now(datetime.timezone.utc)


class _Spark:
    """Answers MIN() per column from ``ages`` (days old); a missing column raises."""

    def __init__(self, ages):
        self.ages = ages
        self.queries = []

    def sql(self, q):
        self.queries.append(q)
        col = q.split("TRY_CAST(")[1].split(" AS")[0]
        if col not in self.ages:
            raise RuntimeError(f"no column {col}")
        ts = NOW - datetime.timedelta(days=self.ages[col])
        return SimpleNamespace(first=lambda: {"min_ts": ts})


def _registry(contract_dict=None, retention=None, freshness=None):
    contract = SimpleNamespace(layer="silver", entity="silver_rideflow_driver_profiles",
                               contract_dict=contract_dict or {"info": {"table_name": "silver_rideflow_driver_profiles"}})
    return SimpleNamespace(
        domain="marketplace", system="rideflow",
        storage=SimpleNamespace(bronze_root=None, silver_root=None, gold_root=None,
                                domain_catalog="`rideflow_dev`.marketplace"),
        slo=SimpleNamespace(freshness=freshness or {}),
        retention=retention if retention is not None else {"silver": "P90D"},
        get_active_contracts=lambda: [contract], metadata={},
    )


def _with_retention(**ret):
    return {"info": {"table_name": "silver_rideflow_driver_profiles"}, "compliance": {"retention": ret}}


class TestColumns:
    def test_never_updated_or_modified_and_load_stamps_last(self):
        cols = retention_columns(None, ["updated_at", "last_modified", "event_timestamp", "created_at",
                                        "_lakelogic_processed_at", "signup_date"])
        assert "updated_at" not in cols and "last_modified" not in cols
        assert cols[0] == "created_at"
        assert "signup_date" in cols
        assert cols[-2:] == ["_lakelogic_loaded_at", "_lakelogic_processed_at"]

    def test_the_contracts_column_alone(self):
        assert retention_columns("signup_date", ["updated_at"]) == ["signup_date"]

    def test_a_ten_year_old_record_touched_last_month_is_ten_years_old(self):
        spark = _Spark({"updated_at": 30, "created_at": 3650})
        r = SLOValidator(_registry(freshness={"silver": SimpleNamespace(check_columns=["updated_at"])}),
                         spark=spark).check_retention()[0]
        assert r.source_column_used == "created_at"
        assert r.status == STATUS_BREACHED and r.passed is False


class TestLoadStamp:
    def test_a_load_stamp_within_the_period_is_not_a_pass(self):
        r = SLOValidator(_registry(), spark=_Spark({"_lakelogic_loaded_at": 1})).check_retention()[0]
        assert r.passed is None and r.status == STATUS_WARN
        assert r.retention_evidence == "load_time"
        assert "compliance.retention.column" in r.message

    def test_a_load_stamp_past_the_period_is_a_breach(self):
        r = SLOValidator(_registry(), spark=_Spark({"_lakelogic_loaded_at": 400})).check_retention()[0]
        assert r.passed is False and r.status == STATUS_BREACHED


class TestContractPeriod:
    def test_the_contracts_period_and_column_win_over_the_layer(self):
        spark = _Spark({"signup_date": 365 * 5, "created_at": 1})
        reg = _registry(_with_retention(period="P6Y", column="signup_date"))
        r = SLOValidator(reg, spark=spark).check_retention()[0]
        assert r.retention_period == "P6Y" and r.retention_source == "contract"
        assert r.source_column_used == "signup_date" and r.passed is True
        assert all("signup_date" in q for q in spark.queries)

    def test_a_contract_period_is_checked_when_no_layer_declares_one(self):
        reg = _registry(_with_retention(period="P6Y"), retention={})
        r = SLOValidator(reg, spark=_Spark({"created_at": 10})).check_retention()[0]
        assert r.retention_source == "contract" and r.passed is True

    def test_the_documented_short_form_is_read_as_iso(self):
        assert _contract_retention(_with_retention(period="400d", basis="legal"))["period"] == "P400D"
        assert _contract_retention(_with_retention(period="6y"))["period"] == "P6Y"
        assert _contract_retention({"compliance": {"erasure": {}}}) is None

    def test_an_unparseable_contract_period_is_not_replaced_by_the_layers(self):
        reg = _registry(_with_retention(period="soon"))
        assert SLOValidator(reg, spark=_Spark({"created_at": 1})).check_retention() == []


def test_the_platform_receives_the_column_source_and_evidence():
    from lakelogic.core.slo import SLOCheckResult

    r = SLOCheckResult(layer="silver", entity="t", check_type="retention", status="OK", passed=True,
                       retention_period="P6Y", retention_age_minutes=10.0, retention_limit_minutes=99,
                       source_column_used="signup_date", retention_source="contract",
                       retention_evidence="record_age")
    assert r.retention_source == "contract" and r.retention_evidence == "record_age"
