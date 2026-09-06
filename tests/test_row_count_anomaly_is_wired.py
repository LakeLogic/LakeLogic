"""Volume-drift detection must actually run.

``check_row_count_anomaly`` was fully implemented — config model, median/rolling
baseline, min/max ratio bounds, dedicated ``anomaly_ratio``/``anomaly_baseline``
run-log columns — and **nothing in the codebase called it**. Configuring drift
detection on a contract did nothing at all, silently.

Three defects, all of which look like a working feature from the outside:

1. no call site anywhere;
2. ``check_field`` was read off the anomaly config, where the attribute did not
   exist (it lives on the parent), so ``hasattr`` was always False and every
   configuration fell back to ``counts_good``;
3. ``check_row_counts`` returned early when no ``min_rows``/``max_rows`` were set,
   so a contract configuring ONLY drift was skipped before reaching the check.

These tests pin the wiring, not the maths: the arithmetic already worked, which is
precisely why nobody noticed it was never executed.
"""
from __future__ import annotations

import ast
import inspect
from pathlib import Path

import pytest

from lakelogic.core import slo as slo_mod
from lakelogic.core.registry import SLORowCountAnomalyConfig, SLORowCountConfig


def test_the_anomaly_check_has_a_caller():
    """The defect in one assertion: a method nothing invokes is not a feature."""
    source = Path(slo_mod.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    calls = [
        node for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "check_row_count_anomaly"
    ]
    assert calls, (
        "check_row_count_anomaly is never called. It was dead code for its whole "
        "life: configurable, tested-looking, and never executed."
    )


def test_check_field_is_settable_on_the_anomaly_config():
    """It was only on the parent, so the detector's hasattr() always failed."""
    cfg = SLORowCountAnomalyConfig(enabled=True, check_field="counts_deduplicated")
    assert cfg.check_field == "counts_deduplicated"


def test_check_field_defaults_to_none_so_the_parent_can_supply_it():
    assert SLORowCountAnomalyConfig().check_field is None


def test_the_anomaly_check_accepts_an_explicit_check_field():
    """The parent's check_field must be able to reach the detector — previously
    there was no parameter for it, so it could not."""
    sig = inspect.signature(slo_mod.SLOValidator.check_row_count_anomaly)
    assert "check_field" in sig.parameters


def test_a_drift_only_contract_is_not_skipped_before_the_check():
    """`min_rows`/`max_rows` are optional. A contract that configures ONLY drift
    hit `if min_rows is None and max_rows is None: continue` and never reached
    the anomaly call."""
    source = inspect.getsource(slo_mod.SLOValidator.check_row_counts)
    assert "and not _anomaly_on" in source, (
        "the early-continue still skips contracts that configure drift alone"
    )


def test_drift_detection_is_off_unless_asked_for():
    """The property that makes wiring this safe: no existing contract changes
    behaviour, so switching the detector on cannot start failing live pipelines."""
    assert SLORowCountAnomalyConfig().enabled is False
    assert SLORowCountConfig().anomaly is None


def test_a_baseline_is_required_before_enforcement():
    """One run is not a baseline. Without this, the first ever run would compare
    against itself and any second run would look like drift."""
    assert SLORowCountAnomalyConfig().min_runs_before_enforcement >= 2


@pytest.mark.parametrize("expression", [
    "counts_deduplicated",
    "counts_deduplicated / NULLIF(counts_source, 0)",
])
def test_check_field_carries_a_ratio_expression_unchanged(expression):
    """Drift on a RAW dedup count is mostly a volume signal — double the input and
    the count doubles with nothing wrong. The ratio is what says "this contract
    normally dedups 31%, today 68%". The field is interpolated into the SELECT on
    the Spark and DuckDB paths, so an expression is valid; this pins that the
    config does not mangle or reject one."""
    assert SLORowCountAnomalyConfig(enabled=True, check_field=expression).check_field == expression
