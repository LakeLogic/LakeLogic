"""quality.quarantine_scope: set aside only the groups that failed a dataset rule (2026-11-18).

A trading app looped per symbol (34 ms of set-up each) to keep one bad symbol from rejecting
the whole batch. With quarantine_scope: [symbol] only that symbol's rows go to quarantine.
"""
from __future__ import annotations

import pytest

pl = pytest.importorskip("polars")


def _contract(**quality):
    return {
        "version": "1.0.0", "info": {"title": "bars"}, "dataset": "bars",
        "model": {"fields": [{"name": "symbol", "type": "string"}, {"name": "ts", "type": "integer"}]},
        "quality": {"on_dataset_failure": "quarantine",
                    "dataset_rules": [{"name": "one_bar_per_time",
                                       "sql": "SELECT COUNT(*) - COUNT(DISTINCT ts) FROM source",
                                       "must_be_less_than": 1}], **quality},
    }


def test_only_the_failing_symbol_is_set_aside():
    from lakelogic import DataProcessor

    proc = DataProcessor(_contract(quarantine_scope=["symbol"]), engine="polars")
    df = pl.DataFrame({"symbol": ["A", "A", "B", "B", "C"], "ts": [1, 1, 1, 2, 1]})   # A has a duplicate bar
    good, bad = proc.run(df)
    assert sorted(good["symbol"].to_list()) == ["B", "B", "C"]
    assert bad["symbol"].to_list() == ["A", "A"]
    errors = [e for row in bad["_lakelogic_errors"].to_list() for e in row]
    assert any("Group rejected: 1 symbol group(s) failed: A (one_bar_per_time)" in e for e in errors)
    assert proc.batch_quarantined is None                                           # not the whole batch


def test_without_a_scope_the_whole_batch_goes():
    from lakelogic import DataProcessor

    proc = DataProcessor(_contract(), engine="polars")
    good, bad = proc.run(pl.DataFrame({"symbol": ["A", "A", "B"], "ts": [1, 1, 1]}))
    assert len(good) == 0 and len(bad) == 3


def test_a_rule_no_single_group_fails_falls_back_to_the_whole_batch():
    from lakelogic import DataProcessor

    c = _contract(quarantine_scope=["symbol"])
    c["quality"]["dataset_rules"] = [
        {"name": "enough_rows", "sql": "SELECT COUNT(*) FROM source", "must_be_greater_than": 3}
    ]
    proc = DataProcessor(c, engine="polars")
    good, bad = proc.run(pl.DataFrame({"symbol": ["A", "B"], "ts": [1, 1]}))
    assert len(good) == 0 and len(bad) == 2 and proc.batch_quarantined
