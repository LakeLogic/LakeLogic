"""A verdict says WHICH RUN it is about.

THE GAP
    `_slo_checks.pipeline_run_id` records the pipeline execution that TRIGGERED the
    check. For the hourly service-level job that is legitimately NULL: the job is
    downstream of no single run, and one check run covers 20 entities last written
    by 20 different runs. Filling it with the check's own id would attach a verdict
    to a run that did not produce the data.

    So the column was empty and there was no way to join a verdict back to the run
    it judged — even though the row-count and quality checks read exactly that run's
    row (`ORDER BY timestamp DESC LIMIT 1`) and selected only the counts from it,
    discarding `pipeline_run_id` and `run_id` that came back in the same fetch.

THE RULE - plain names, because `source_*` is already taken
    `SLOCheckResult.source_delay_minutes` / `source_column_used` mean UPSTREAM DATA
    STALENESS, so a `source_run_id` would read as "the upstream source's run id" -
    the same word for a different subject. These say what they mean instead:

    produced_by_run_id           the run that wrote THIS entity's rows
    produced_by_pipeline_run_id  the pipeline execution that run belonged to

    NULL IS MEANINGFUL. Freshness reads `MAX(updated_at)` off the table and the
    anomaly check aggregates a lookback window, so neither has one run behind it.
    A null here means "no single run produced this verdict", not "we lost it".
"""
from __future__ import annotations

from lakelogic.core.run_log import _flatten_slo_check
from lakelogic.core.slo import SLOCheckResult

PIPELINE_RUN = "c098354a-6ea8-4df8-b72a-948600f6183b"
ENTITY_RUN = "9ee54926-40f4-46c8-8b90-2cc5a61ea7e0"


def _result(**kw):
    base = dict(layer="bronze", entity="bronze_rideflow_rider_profiles",
                status="OK", passed=True)
    base.update(kw)
    return SLOCheckResult(**base)


def test_the_result_model_can_carry_the_run_it_measured():
    r = _result(check_type="row_count", row_count=915,
                produced_by_run_id=ENTITY_RUN, produced_by_pipeline_run_id=PIPELINE_RUN)
    assert r.produced_by_run_id == ENTITY_RUN
    assert r.produced_by_pipeline_run_id == PIPELINE_RUN


def test_provenance_defaults_to_none_not_to_a_guess():
    """An unset source must never inherit the check's own id."""
    r = _result()
    assert r.produced_by_run_id is None
    assert r.produced_by_pipeline_run_id is None


def test_the_slo_checks_row_carries_both_ids():
    row = _flatten_slo_check(
        _result(check_type="row_count", produced_by_run_id=ENTITY_RUN,
                produced_by_pipeline_run_id=PIPELINE_RUN),
        check_run_id="check-1", pipeline_run_id=None,
        checked_at="2026-09-07T08:39:03Z", domain="marketplace", system="rideflow",
    )
    assert row["produced_by_run_id"] == ENTITY_RUN
    assert row["produced_by_pipeline_run_id"] == PIPELINE_RUN


def test_the_trigger_and_the_measured_run_are_different_columns():
    """The distinction the whole change rests on: a scheduled check has no trigger
    but still measures a specific run."""
    row = _flatten_slo_check(
        _result(check_type="row_count", produced_by_run_id=ENTITY_RUN,
                produced_by_pipeline_run_id=PIPELINE_RUN),
        check_run_id="check-1",
        pipeline_run_id=None,  # scheduled: downstream of no single run
        checked_at="2026-09-07T08:39:03Z", domain="marketplace", system="rideflow",
    )
    assert row["pipeline_run_id"] is None, "a scheduled check must not invent a trigger"
    assert row["produced_by_run_id"] == ENTITY_RUN, "but it does know what it measured"


def test_a_freshness_verdict_has_no_source_run():
    """Freshness reads the TABLE, so null here is the honest answer."""
    row = _flatten_slo_check(
        _result(check_type="freshness", delay_minutes=12.0),
        check_run_id="check-1", pipeline_run_id=None,
        checked_at="2026-09-07T08:39:03Z", domain="marketplace", system="rideflow",
    )
    assert row["produced_by_run_id"] is None
    assert row["produced_by_pipeline_run_id"] is None


# ── The correlation must survive the trip to the platform ────────────────────


def _emit_and_capture(monkeypatch, results):
    import types

    import lakelogic.core.observatory_spool as spool
    from lakelogic.core import run_log

    cfg = {"enabled": True, "endpoint": "https://obs.invalid/ingest", "api_key": "k"}
    monkeypatch.setattr(spool, "resolve_observatory_config", lambda _x: cfg)
    monkeypatch.setattr(spool, "spool_payload", lambda *a, **k: None)
    monkeypatch.setattr(spool, "flush_spool", lambda *a, **k: None)
    monkeypatch.setattr(run_log, "_lakelogic_version", lambda: "test")

    sent = []

    def _post(url, json=None, **kw):
        sent.append(json)
        return types.SimpleNamespace(status_code=200, text="ok")

    monkeypatch.setattr("requests.post", _post)
    registry = types.SimpleNamespace(observatory={}, domain="marketplace", system="rideflow")
    run_log.emit_slo_report(registry, results, environment="dev")
    return sent


def test_the_platform_payload_carries_the_measured_run(monkeypatch):
    sent = _emit_and_capture(monkeypatch, [
        _result(check_type="row_count", row_count=915, slo_min_rows=1,
                produced_by_run_id=ENTITY_RUN, produced_by_pipeline_run_id=PIPELINE_RUN),
    ])
    section = sent[0]["metadata"]["slo_json"]["row_count"]
    assert section["produced_by_run_id"] == ENTITY_RUN
    assert section["produced_by_pipeline_run_id"] == PIPELINE_RUN


def test_a_section_without_provenance_omits_the_keys(monkeypatch):
    """Omitted, not null: the platform reads absence as 'not applicable' and a
    null would look like a lost value."""
    sent = _emit_and_capture(monkeypatch, [
        _result(check_type="freshness", delay_minutes=12.0, slo_max_minutes=60),
    ])
    section = sent[0]["metadata"]["slo_json"]["freshness"]
    assert "produced_by_run_id" not in section
    assert "produced_by_pipeline_run_id" not in section


def test_provenance_does_not_change_the_configured_count(monkeypatch):
    """`_slo_signal_counts` counts a section CONFIGURED from threshold/min/max/pass.
    Adding produced-by keys must not make an unconfigured section look configured.
    """
    sent = _emit_and_capture(monkeypatch, [
        _result(check_type="row_count", row_count=915,
                produced_by_run_id=ENTITY_RUN, produced_by_pipeline_run_id=PIPELINE_RUN),
    ])
    section = sent[0]["metadata"]["slo_json"]["row_count"]
    signal_fields = {"threshold", "threshold_seconds", "min", "max"}
    assert not (signal_fields & set(section)), (
        "no threshold was configured, so no threshold key may appear"
    )
    assert section["pass"] is True


# ── Naming the run must never cost a correct verdict ─────────────────────────────


def test_a_run_log_without_the_columns_still_produces_a_verdict():
    """THE REGRESSION THIS GUARDS.

    Widening the SELECT to grab `pipeline_run_id, run_id` makes the query fail on a
    run log written before those columns existed. If that surfaced, a table with
    150 rows would be reported "NO DATA" — the same false negative that made all 7
    bronze entities look empty while holding 17,845 rows.

    Naming the run is worth having. It is not worth a wrong answer, so the check
    falls back to the narrow query and simply reports no producing run.
    """
    import datetime
    from types import SimpleNamespace

    from lakelogic.core.slo import SLOValidator

    now = datetime.datetime(2026, 3, 26, 12, 0, tzinfo=datetime.timezone.utc)

    class NarrowCon:
        """A run log that has no pipeline_run_id/run_id columns."""

        def execute(self, query):
            if "pipeline_run_id" in query:
                raise Exception("Binder Error: column pipeline_run_id does not exist")

            class R:
                def fetchone(self_inner):
                    return (150, now)

            return R()

    registry = SimpleNamespace(
        slo=SimpleNamespace(row_count={
            "bronze": SimpleNamespace(min_rows=10, max_rows=1000,
                                      check_field="counts_good",
                                      exclude_tables=[], anomaly=None),
        }),
        storage=SimpleNamespace(run_log_table="run_logs"),
        get_active_contracts=lambda: [SimpleNamespace(layer="bronze", entity="orders")],
    )

    results = SLOValidator(registry, duckdb_con=NarrowCon()).check_row_counts()
    assert len(results) == 1
    assert results[0].passed is True, "the verdict must survive the missing columns"
    assert results[0].row_count == 150
    assert results[0].produced_by_run_id is None, "no producing run available, and that is fine"


def test_a_row_count_verdict_is_labelled_row_count_not_freshness():
    """`check_type` DEFAULTS to "freshness" and check_row_counts never set it.

    Found live: 13 of 33 rows in `_slo_checks` were labelled `freshness` while
    carrying a `row_count` and no `delay_minutes`. It is not just a label —
    `emit_slo_report` keys the platform payload BY check_type, so row counts were
    written into `slo_json.freshness`, the exact field `_freshness_status()` reads
    to decide whether data is fresh or stale.
    """
    import datetime
    from types import SimpleNamespace

    from lakelogic.core.slo import SLOValidator

    now = datetime.datetime(2026, 3, 26, 12, 0, tzinfo=datetime.timezone.utc)

    class Con:
        def execute(self, query):
            class R:
                def fetchone(self_inner):
                    return (150, now, "pipe-1", "run-1")

            return R()

    registry = SimpleNamespace(
        slo=SimpleNamespace(row_count={
            "bronze": SimpleNamespace(min_rows=10, max_rows=1000,
                                      check_field="counts_good",
                                      exclude_tables=[], anomaly=None),
        }),
        storage=SimpleNamespace(run_log_table="run_logs"),
        get_active_contracts=lambda: [SimpleNamespace(layer="bronze", entity="orders")],
    )
    results = SLOValidator(registry, duckdb_con=Con()).check_row_counts()
    assert [r.check_type for r in results] == ["row_count"]
    assert results[0].produced_by_run_id == "run-1"
    assert results[0].produced_by_pipeline_run_id == "pipe-1"


def test_a_row_count_lands_in_its_own_payload_section(monkeypatch):
    """The consequence of the mislabel, pinned: a row count must not appear under
    `freshness`, which the platform reads as a statement about data age."""
    sent = _emit_and_capture(monkeypatch, [
        _result(check_type="row_count", row_count=915, slo_min_rows=1),
    ])
    slo_json = sent[0]["metadata"]["slo_json"]
    assert "row_count" in slo_json
    assert "freshness" not in slo_json, "a row count is not a freshness verdict"


# ── Drift detection: same run, and it must not be erased by its sibling ──────


def test_the_anomaly_verdict_names_the_run_that_drifted():
    """The lookback is the BASELINE, not the subject.

    An anomaly judges one run's count against history, so it is about that run.
    "Volume dropped 70%" is close to useless if you cannot name the run that
    dropped it — this is the verdict provenance matters most for.
    """
    from types import SimpleNamespace

    from lakelogic.core.slo import SLOValidator

    cfg = SimpleNamespace(enabled=True, lookback_runs=14, min_ratio=0.5,
                          max_ratio=2.0, method="median",
                          min_runs_before_enforcement=5, check_field=None)

    class Con:
        def execute(self, query):
            class R:
                def fetchall(self_inner):
                    # Six rows: the newest is the value under test (excluded from
                    # the baseline), leaving five historical runs — exactly
                    # min_runs_before_enforcement.
                    return [(300,), (1000,), (1000,), (1000,), (1000,), (1000,)]

            return R()

    registry = SimpleNamespace(
        slo=SimpleNamespace(row_count={}),
        storage=SimpleNamespace(run_log_table="run_logs"),
        get_active_contracts=lambda: [],
    )
    v = SLOValidator(registry, duckdb_con=Con())
    result = v.check_row_count_anomaly(
        "orders", "bronze", 300, cfg,
        produced_by_run_id="run-1", produced_by_pipeline_run_id="pipe-1",
    )
    assert result is not None and result.passed is False, "0.3x should breach min_ratio"
    assert result.anomaly_ratio == 0.3
    assert result.produced_by_run_id == "run-1"
    assert result.produced_by_pipeline_run_id == "pipe-1"


def test_bounds_and_drift_share_a_section_without_erasing_each_other(monkeypatch):
    """Both are check_type="row_count". Replacement kept only the later verdict, so
    a drift breach arrived with no thresholds and a bounds pass erased the ratio.
    """
    bounds = _result(check_type="row_count", row_count=915,
                     slo_min_rows=1, slo_max_rows=500000,
                     produced_by_run_id=ENTITY_RUN, produced_by_pipeline_run_id=PIPELINE_RUN)
    drift = _result(check_type="row_count", row_count=915, passed=False,
                    anomaly_ratio=0.3, anomaly_baseline=3000.0,
                    produced_by_run_id=ENTITY_RUN, produced_by_pipeline_run_id=PIPELINE_RUN)

    sent = _emit_and_capture(monkeypatch, [bounds, drift])
    section = sent[0]["metadata"]["slo_json"]["row_count"]

    assert section["min"] == 1 and section["max"] == 500000, "bounds evidence kept"
    assert section["anomaly_ratio"] == 0.3, "drift evidence kept"
    assert section["pass"] is False, "a failure on either side fails the section"


def test_a_passing_sibling_cannot_mask_a_breach(monkeypatch):
    """Order must not decide the verdict."""
    drift = _result(check_type="row_count", passed=False, anomaly_ratio=0.3,
                    anomaly_baseline=3000.0)
    bounds = _result(check_type="row_count", row_count=915, slo_min_rows=1)

    for order in ([drift, bounds], [bounds, drift]):
        sent = _emit_and_capture(monkeypatch, order)
        assert sent[0]["metadata"]["slo_json"]["row_count"]["pass"] is False


# ── A baseline must not contain the value it is judging ─────────────────────


def _anomaly_validator(counts):
    """A run log whose newest row is first, as the real query returns it."""
    from types import SimpleNamespace

    from lakelogic.core.slo import SLOValidator

    class Rows:
        def fetchall(self_inner):
            return [(c,) for c in counts]

    registry = SimpleNamespace(storage=SimpleNamespace(run_log_table="run_logs"))
    return SLOValidator(registry, duckdb_con=SimpleNamespace(execute=lambda q: Rows()))


CFG = None


def _cfg(**kw):
    from types import SimpleNamespace

    base = dict(enabled=True, lookback_runs=14, min_ratio=0.5, max_ratio=2.0,
                method="median", min_runs_before_enforcement=2, check_field="counts_good")
    base.update(kw)
    return SimpleNamespace(**base)


def test_the_current_value_is_not_part_of_its_own_baseline():
    """FOUND LIVE: every verdict read `ratio=1.00x` with `baseline == rows`.

    `actual_count` is the newest run-log row and the lookback is ordered
    newest-first, so the value under test was element 0 of the series it was
    compared against. On a steady series the median simply became the current
    value, and the check could not detect anything at all.
    """
    v = _anomaly_validator([300, 1000, 1000, 1000, 1000])
    r = v.check_row_count_anomaly("orders", "bronze", 300, _cfg())
    assert r is not None
    assert r.anomaly_baseline == 1000.0, "the 300 must not drag its own baseline down"
    assert r.anomaly_ratio == 0.3
    assert r.passed is False and "VOLUME DROP" in r.status


def test_a_steady_series_still_reads_as_normal():
    """Excluding the newest must not invent drift where there is none."""
    v = _anomaly_validator([1000, 1000, 1000, 1000])
    r = v.check_row_count_anomaly("orders", "bronze", 1000, _cfg())
    assert r is not None and r.passed is True
    assert r.anomaly_ratio == 1.0


def test_a_full_window_of_history_survives_the_exclusion():
    """One extra row is fetched, so dropping the newest still leaves lookback_runs
    of genuine history rather than one fewer."""
    counts = [500] + [100] * 14          # newest + 14 historical
    v = _anomaly_validator(counts)
    r = v.check_row_count_anomaly("orders", "bronze", 500, _cfg(lookback_runs=14))
    assert r is not None
    assert r.anomaly_baseline == 100.0
    assert r.anomaly_ratio == 5.0, "a 5x spike must read as 5x, not be damped toward 1"


def test_enforcement_counts_history_not_the_current_row():
    """With min_runs_before_enforcement=5, four historical runs plus the current one
    is still four — not enough."""
    v = _anomaly_validator([100, 100, 100, 100, 100])
    assert v.check_row_count_anomaly(
        "orders", "bronze", 100, _cfg(min_runs_before_enforcement=5)
    ) is None
