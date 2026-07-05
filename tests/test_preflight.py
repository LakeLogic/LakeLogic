"""Tests for pre-flight materialization validation (lakelogic.core.preflight)
and its non-breaking hook in DataProcessor."""

import pytest

from lakelogic.core.preflight import preflight_check, assert_preflight, PreflightError
from lakelogic.core.processor import DataProcessor

# merge with no key → PK-002 (would materialize non-deterministically)
BAD_MERGE = {
    "info": {"target_layer": "silver"},
    "materialization": {"strategy": "merge"},
    "model": {"fields": [{"name": "id", "type": "string"}]},
}
# scd2 with no track_columns → SCD-001 (version churn)
BAD_SCD2 = {
    "info": {"target_layer": "gold"},
    "primary_key": ["id"],
    "materialization": {"strategy": "scd2", "scd2": {"surrogate_key": "sk"}},
    "model": {"fields": [{"name": "id", "type": "string"}]},
}
CLEAN = {
    "info": {"target_layer": "silver"},
    "primary_key": ["id"],
    "materialization": {"strategy": "merge"},
    "model": {"fields": [{"name": "id", "type": "string"}]},
}


def test_preflight_flags_materialization_blockers():
    ids = {f.check_id for f in preflight_check(BAD_MERGE, "c")}
    assert "PK-002" in ids
    assert "SCD-001" in {f.check_id for f in preflight_check(BAD_SCD2, "c")}


def test_preflight_forces_critical_materialization():
    fs = preflight_check(BAD_MERGE, "c")
    assert fs and all(f.severity == "critical" and f.category == "materialization" for f in fs)


def test_preflight_clean_contract_passes():
    assert preflight_check(CLEAN, "c") == []


def test_preflight_ignores_non_materialization_gaps():
    # A missing SLO / masking is NOT a pre-flight blocker — only correctness ones.
    ids = {f.check_id for f in preflight_check(CLEAN, "c")}
    assert ids == set()  # CLEAN merges fine; VOL/PII/etc. are not pre-flight


def test_preflight_accepts_a_path(tmp_path):
    import yaml

    p = tmp_path / "silver_x.yaml"
    p.write_text(yaml.safe_dump(BAD_MERGE), encoding="utf-8")
    assert "PK-002" in {f.check_id for f in preflight_check(p, "silver_x")}


def test_assert_preflight_raises_on_blocker():
    with pytest.raises(PreflightError):
        assert_preflight(BAD_MERGE, "c")
    assert_preflight(CLEAN, "c")  # no raise


# ── The non-breaking DataProcessor hook ─────────────────────────────────────
# `_run_preflight` only uses self.contract for the display name, so we can drive
# it on a lightweight stub without constructing a full processor/adapter.


class _Stub:
    contract = None


def test_processor_hook_is_non_breaking_by_default(monkeypatch):
    monkeypatch.delenv("LAKELOGIC_STRICT_PREFLIGHT", raising=False)
    # Must NOT raise even on a blocking contract (logs only).
    DataProcessor._run_preflight(_Stub(), BAD_MERGE)


def test_processor_hook_raises_under_strict(monkeypatch):
    monkeypatch.setenv("LAKELOGIC_STRICT_PREFLIGHT", "1")
    with pytest.raises(PreflightError):
        DataProcessor._run_preflight(_Stub(), BAD_MERGE)
    # a clean contract still doesn't raise under strict
    DataProcessor._run_preflight(_Stub(), CLEAN)
