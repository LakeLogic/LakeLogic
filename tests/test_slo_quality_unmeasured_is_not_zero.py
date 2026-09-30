"""A run with no good/quarantined counts gets no quality verdict - not "total data loss"."""
from types import SimpleNamespace

from lakelogic.core.slo import SLOValidator

Q = SimpleNamespace(min_good_ratio=0.95, max_quarantine_ratio=0.05)


def test_unmeasured_counts_give_no_verdict():
    # gold_dim_date: source counted, nothing else reported (2026-09-30).
    assert SLOValidator._evaluate_quality_counts("gold_dim_date", {"source": 4018}, Q) == []


def test_measured_total_loss_still_fails():
    (r,) = SLOValidator._evaluate_quality_counts("t", {"total": 10, "good": 0, "quarantined": 10}, Q)
    assert r.passed is False
