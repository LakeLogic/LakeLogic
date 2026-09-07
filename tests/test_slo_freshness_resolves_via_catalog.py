"""Freshness AND retention must find their table in a catalog-addressed mesh.

FOUND LIVE (2026-09-07)
    `_slo_checks` held 158 rows and NOT ONE freshness verdict. Every run logged

        SLO Freshness Check: scanning 18 contracts in marketplace/rideflow
        SLO Freshness Summary: 0 checks | 0 passed | 0 failed | 0 errors

    one millisecond apart — every contract hit a `continue` before touching a table.

THE CAUSE
    `check_freshness` could name a table two ways: a layer `*_root`, or a
    materialization path. This mesh uses neither — it addresses tables as
    `` `catalog`.schema.table `` via `storage.domain_catalog`, and the roots are all
    None. So the guard skipped all 18, silently, and the four freshness columns
    (delay_minutes, slo_max_minutes, source_delay_minutes, source_column_used) were
    empty table-wide.

    Freshness was declared in every domain and measured in none of them. The
    row-count and quality checks worked throughout because they read the run-log
    table, which resolves from `domain_catalog` — so the failure was invisible
    behind checks that did work.
"""
from __future__ import annotations

import datetime
from types import SimpleNamespace

from lakelogic.core.slo import SLOValidator


def _registry(**storage_kw):
    storage = SimpleNamespace(
        bronze_root=None, silver_root=None, gold_root=None,
        domain_catalog="`rideflow_dev_demo`.marketplace",
    )
    for k, v in storage_kw.items():
        setattr(storage, k, v)
    contract = SimpleNamespace(
        layer="bronze",
        entity="bronze_rideflow_rider_profiles",
        contract_dict={"info": {"table_name": "bronze_rideflow_rider_profiles"}},
    )
    return SimpleNamespace(
        domain="marketplace", system="rideflow", storage=storage,
        slo=SimpleNamespace(freshness={
            "bronze": SimpleNamespace(max_delay_minutes=60,
                                      check_columns=["updated_at"],
                                      exclude_tables=[], include_tables=[]),
        }),
        get_active_contracts=lambda: [contract],
        metadata={},
    )


class _Spark:
    """Records the tables asked for, and answers with a fresh timestamp."""

    def __init__(self):
        self.queries = []

    def sql(self, q):
        self.queries.append(" ".join(q.split()))
        ts = datetime.datetime.now(datetime.timezone.utc)
        return SimpleNamespace(first=lambda: {"latest_ts": ts})


def test_a_catalog_addressed_mesh_produces_freshness_checks():
    """THE REGRESSION: 18 contracts scanned, 0 checks produced."""
    spark = _Spark()
    results = SLOValidator(_registry(), spark=spark).check_freshness()
    assert len(results) == 1, "a catalog-addressed contract must be measured"
    assert results[0].passed is True


def test_the_table_is_addressed_through_the_catalog():
    spark = _Spark()
    SLOValidator(_registry(), spark=spark).check_freshness()
    assert spark.queries, "no query was issued at all"
    assert "rideflow_dev_demo.marketplace.bronze_rideflow_rider_profiles" in spark.queries[0]


def test_the_declared_table_name_wins_over_composition():
    """`make_table_name` composes {layer}_{system}_{entity}. Entity keys now carry
    their layer, so composing would ask for
    `bronze_rideflow_bronze_rideflow_rider_profiles`.
    """
    spark = _Spark()
    SLOValidator(_registry(), spark=spark).check_freshness()
    assert "bronze_rideflow_bronze_rideflow" not in spark.queries[0], (
        "the entity's layer prefix was composed on twice"
    )


def test_a_root_configured_mesh_is_unaffected():
    """The catalog branch is a FALLBACK; path-addressed meshes keep their behaviour."""
    spark = _Spark()
    SLOValidator(_registry(bronze_root="s3://lake/bronze"), spark=spark).check_freshness()
    assert spark.queries[0].startswith("SELECT MAX(updated_at)")
    assert "s3://lake/bronze.bronze_rideflow_rider_profiles" in spark.queries[0]


def test_a_contract_that_cannot_be_located_warns_rather_than_vanishing(caplog):
    """A silently skipped contract is an objective promised and not measured, which
    reads downstream as 'no problem'."""
    from loguru import logger as _logger

    lines = []
    sink = _logger.add(lines.append, level="WARNING")
    try:
        registry = _registry(domain_catalog=None)
        results = SLOValidator(registry, spark=_Spark()).check_freshness()
    finally:
        _logger.remove(sink)

    assert results == []
    assert any("bronze_rideflow_rider_profiles" in x for x in lines), (
        "the skipped contract must be named, not dropped in silence"
    )


# ── check_retention had the identical defect ─────────────────────────────────


def _retention_registry():
    contract = SimpleNamespace(
        layer="bronze",
        entity="bronze_rideflow_rider_profiles",
        contract_dict={"info": {"table_name": "bronze_rideflow_rider_profiles"}},
    )
    return SimpleNamespace(
        domain="marketplace", system="rideflow",
        storage=SimpleNamespace(bronze_root=None, silver_root=None, gold_root=None,
                                domain_catalog="`rideflow_dev_demo`.marketplace"),
        slo=SimpleNamespace(freshness={
            "bronze": SimpleNamespace(max_delay_minutes=60,
                                      check_columns=["updated_at"],
                                      exclude_tables=[], include_tables=[]),
        }),
        retention={"bronze": "P7D"},
        get_active_contracts=lambda: [contract],
        metadata={},
    )


class _RetentionSpark:
    def __init__(self):
        self.queries = []

    def sql(self, q):
        self.queries.append(" ".join(q.split()))
        # A record from well inside the 7-day window.
        ts = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=1)
        return SimpleNamespace(first=lambda: {"min_ts": ts})


def test_retention_also_resolves_through_the_catalog():
    """FIXING ONE OF TWO IDENTICAL SITES IS NOT FIXING IT.

    `check_retention` carried the same root-or-path-only guard as `check_freshness`,
    so on this mesh it skipped every contract and produced ZERO rows — while
    bronze P7D / silver P90D / gold P7Y sat declared and unmeasured. The freshness
    fix landed first and this twin was left behind.
    """
    spark = _RetentionSpark()
    results = SLOValidator(_retention_registry(), spark=spark).check_retention()
    assert results, "a catalog-addressed contract must be retention-checked"
    assert "rideflow_dev_demo.marketplace.bronze_rideflow_rider_profiles" in spark.queries[0]


def test_retention_does_not_double_prefix_the_entity():
    spark = _RetentionSpark()
    SLOValidator(_retention_registry(), spark=spark).check_retention()
    assert "bronze_rideflow_bronze_rideflow" not in spark.queries[0]


def test_retention_does_not_depend_on_a_freshness_objective():
    """Retention is declared in its OWN top-level block, next to compliance —
    `slo.freshness` is a service level. One must not be able to switch off the other.

    The code read `check_columns` off `slo.freshness` and, finding none, skipped at
    DEBUG level. A domain declaring `retention: {bronze: P7D}` but no freshness
    objective therefore measured nothing, and "not measured" looked exactly like
    "passed" — on a promise about deleting data on time.
    """
    contract = SimpleNamespace(
        layer="bronze",
        entity="bronze_rideflow_rider_profiles",
        contract_dict={"info": {"table_name": "bronze_rideflow_rider_profiles"}},
    )
    registry = SimpleNamespace(
        domain="marketplace", system="rideflow",
        storage=SimpleNamespace(bronze_root=None, silver_root=None, gold_root=None,
                                domain_catalog="`rideflow_dev_demo`.marketplace"),
        slo=SimpleNamespace(freshness={}),      # NO freshness objective at all
        retention={"bronze": "P7D"},            # but retention IS declared
        get_active_contracts=lambda: [contract],
        metadata={},
    )
    spark = _RetentionSpark()
    results = SLOValidator(registry, spark=spark).check_retention()
    assert results, "retention must still be measured without a freshness objective"
    assert "_lakelogic_processed_at" in spark.queries[0], (
        "it should fall back to the audit columns the framework always writes"
    )


# ── Retention persists its OWN measurements ─────────────────────────────────


def test_retention_writes_its_own_fields_not_freshness_fields():
    """`source_delay_minutes` / `source_slo_max_minutes` are documented as UPSTREAM
    DATA STALENESS. Retention wrote its age and limit there, so one column meant two
    things depending on `check_type`, and "which tables are near their retention
    limit" could not be queried without parsing the status sentence.
    """
    spark = _RetentionSpark()
    r = SLOValidator(_retention_registry(), spark=spark).check_retention()[0]

    assert r.retention_period == "P7D", "the DECLARED promise was persisted nowhere"
    assert r.retention_limit_minutes == 7 * 24 * 60
    assert r.retention_age_minutes is not None and r.retention_age_minutes > 0

    assert r.source_delay_minutes is None, "freshness's field must stay freshness's"
    assert r.source_slo_max_minutes is None
    # Shared on purpose: "which timestamp column was resolved" means the same thing
    # for freshness and retention.
    assert r.source_column_used == "updated_at"


def test_the_slo_checks_row_carries_the_retention_numbers():
    from lakelogic.core.run_log import _flatten_slo_check

    spark = _RetentionSpark()
    result = SLOValidator(_retention_registry(), spark=spark).check_retention()[0]
    row = _flatten_slo_check(result, check_run_id="c1", pipeline_run_id=None,
                             checked_at="2026-09-07T00:00:00Z",
                             domain="marketplace", system="rideflow")
    assert row["retention_period"] == "P7D"
    assert row["retention_limit_minutes"] == 10080
    assert row["retention_age_minutes"] is not None


def test_the_platform_receives_more_than_a_boolean():
    """`"retention": {"pass": true}` cannot answer "how close to the limit are we" —
    the question anyone asks BEFORE a breach."""
    import types
    from unittest.mock import MagicMock, patch

    from lakelogic.core import run_log

    result = SLOValidator(_retention_registry(), spark=_RetentionSpark()).check_retention()[0]
    reg = types.SimpleNamespace(
        observatory={"enabled": True, "endpoint": "https://x/i", "api_key": "k"},
        domain="marketplace", system="rideflow")
    with patch("requests.post") as post, \
         patch("lakelogic.core.observatory_spool.flush_spool", return_value=0):
        post.return_value = MagicMock(status_code=200, text="ok")
        run_log.emit_slo_report(reg, [result], environment="dev")
        section = post.call_args_list[0].kwargs["json"]["metadata"]["slo_json"]["retention"]

    assert section["pass"] is True
    assert section["period"] == "P7D"
    assert section["limit_minutes"] == 10080
    assert section["age_minutes"] is not None
