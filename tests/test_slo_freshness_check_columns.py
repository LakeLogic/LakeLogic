"""Freshness resolves ONE ordered candidate list, not two overlapping fields.

`check_column` (the table's own audit column) and `check_columns` (the
source's business columns) did the same job — resolve the first available
timestamp — against two different lists, so every estate had to declare the same
information twice and keep the two in sync.
"""

import pytest

from lakelogic.core.registry import SLOFreshnessConfig


def test_default_covers_both_audit_spellings():
    """The old default was `_lakelogic_loaded_at` alone, but the framework writes
    `_lakelogic_processed_at` unless a system overrides it — so the guaranteed
    fallback was routinely a column that did not exist."""
    assert SLOFreshnessConfig(max_delay_minutes=60).check_columns == [
        "_lakelogic_processed_at",
        "_lakelogic_loaded_at",
    ]


def test_explicit_list_is_kept_verbatim():
    """Config stays as authored — the audit fallback is appended at RESOLUTION time
    (see the evaluator), so a declared list is never silently rewritten here."""
    cfg = SLOFreshnessConfig(max_delay_minutes=60, check_columns=["updated_at", "created_at"])
    assert cfg.check_columns == ["updated_at", "created_at"]


def test_legacy_check_column_merges_last():
    """Order is the whole point of the merge.

    An audit column is written by us on every run, so it is always fresh. If it
    sorted ahead of the business timestamps the SOURCE-delay check would resolve to
    our own write time and could never fire — the check would look configured and
    silently never fail.
    """
    cfg = SLOFreshnessConfig(
        max_delay_minutes=60,
        check_column="_lakelogic_processed_at",
        max_source_delay_minutes=60,
        check_columns=["updated_at", "last_modified", "event_timestamp"],
    )
    assert cfg.check_columns == [
        "updated_at",
        "last_modified",
        "event_timestamp",
        "_lakelogic_processed_at",
    ]


def test_legacy_merge_deduplicates_without_reordering():
    cfg = SLOFreshnessConfig(
        max_delay_minutes=60,
        check_column="updated_at",
        check_columns=["updated_at", "created_at"],
    )
    assert cfg.check_columns == ["updated_at", "created_at"]


def test_legacy_check_column_accepts_a_list_too():
    cfg = SLOFreshnessConfig(max_delay_minutes=60, check_column=["a", "b"])
    assert cfg.check_columns == ["a", "b"]


def test_legacy_check_column_warns_with_the_replacement():
    from loguru import logger

    warnings: list[str] = []
    sink = logger.add(warnings.append, level="WARNING")
    try:
        SLOFreshnessConfig(max_delay_minutes=60, check_column="updated_at")
    finally:
        logger.remove(sink)

    assert any("deprecated" in w.lower() for w in warnings)
    # The warning must carry the exact replacement, not merely scold.
    assert any("check_columns: ['updated_at']" in w for w in warnings)


def test_no_legacy_field_means_no_warning():
    from loguru import logger

    warnings: list[str] = []
    sink = logger.add(warnings.append, level="WARNING")
    try:
        SLOFreshnessConfig(max_delay_minutes=60, check_columns=["updated_at"])
    finally:
        logger.remove(sink)
    assert warnings == []


def test_legacy_merge_does_not_promote_the_default_audit_columns():
    """The default value must not leak into a legacy merge.

    `check_columns` defaults to the audit columns. Seeding the merge from the
    field unconditionally put an audit column FIRST — and since we write that column
    on every run it is always fresh, so the freshness check could never fail while
    still looking correctly configured.
    """
    cfg = SLOFreshnessConfig(max_delay_minutes=60, check_column="updated_at")
    assert cfg.check_columns == ["updated_at"]
    assert "_lakelogic_processed_at" not in cfg.check_columns[:1]


def test_evaluator_appends_audit_fallback_for_datasets_with_no_source_timestamp():
    """A dataset carrying no business timestamp must still be measurable."""
    import inspect

    from lakelogic.core import slo

    src = inspect.getsource(slo.SLOEvaluator.evaluate_freshness if hasattr(slo, "SLOEvaluator") else slo)
    assert "_lakelogic_processed_at" in src and "_lakelogic_loaded_at" in src


# ── one measurement → one threshold ───────────────────────────────────────────


def test_legacy_source_threshold_replaces_the_pipeline_one():
    """The SOURCE limit survives, not the pipeline limit.

    The remaining measurement is "how stale is the data", which is what
    `max_source_delay_minutes` bounded. `max_delay_minutes` bounded how long our own
    write took — applying that budget to data age would fire constantly.
    """
    cfg = SLOFreshnessConfig(max_delay_minutes=30, max_source_delay_minutes=60)
    assert cfg.max_delay_minutes == 60


def test_single_threshold_is_left_alone():
    assert SLOFreshnessConfig(max_delay_minutes=30).max_delay_minutes == 30


def test_identical_thresholds_do_not_warn():
    from loguru import logger

    warnings: list[str] = []
    sink = logger.add(warnings.append, level="WARNING")
    try:
        SLOFreshnessConfig(max_delay_minutes=60, max_source_delay_minutes=60)
    finally:
        logger.remove(sink)
    assert warnings == []


# ── scoping: freshness is meaningless for non-volatile reference data ─────────


def _cfg(**kw):
    return SLOFreshnessConfig(max_delay_minutes=60, **kw)


def test_empty_scope_covers_everything():
    assert _cfg().covers("silver_trips") is True


def test_exclude_wins_over_nothing_declared():
    assert _cfg(exclude_tables=["dim_currency"]).covers("dim_currency") is False


def test_include_list_restricts_to_named_tables():
    cfg = _cfg(include_tables=["silver_trips", "silver_payments"])
    assert cfg.covers("silver_trips") is True
    assert cfg.covers("dim_currency") is False


def test_patterns_scope_a_whole_reference_family():
    """A reference family is one entry, not a list somebody has to maintain."""
    cfg = _cfg(exclude_tables=["dim_*", "ref_*"])
    assert cfg.covers("dim_currency") is False
    assert cfg.covers("ref_country") is False
    assert cfg.covers("silver_trips") is True


def test_include_and_exclude_read_as_these_except_those():
    cfg = _cfg(include_tables=["silver_*"], exclude_tables=["silver_*_snapshot"])
    assert cfg.covers("silver_trips") is True
    assert cfg.covers("silver_trips_snapshot") is False
    assert cfg.covers("bronze_trips") is False


def test_a_typod_freshness_key_is_rejected_not_ignored():
    """`sorce_check_columns` used to be accepted and dropped, leaving the check on
    its defaults while reading as configured — the silent-config failure mode."""
    import pydantic

    with pytest.raises(pydantic.ValidationError):
        SLOFreshnessConfig(max_delay_minutes=60, sorce_check_columns=["updated_at"])


def test_source_check_columns_is_accepted_as_a_deprecated_alias():
    """Renamed to `check_columns`: the listed columns need not be source columns."""
    from loguru import logger

    warnings: list[str] = []
    sink = logger.add(warnings.append, level="WARNING")
    try:
        cfg = SLOFreshnessConfig(max_delay_minutes=60, source_check_columns=["updated_at"])
    finally:
        logger.remove(sink)

    assert cfg.check_columns == ["updated_at"]
    assert any("check_columns: ['updated_at']" in w for w in warnings)
