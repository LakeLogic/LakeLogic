"""Quarantined rows are masked like good rows (2026-11-18).

A fresh-agent test found `masking: hash` protected the output while the quarantine kept the raw
email: a compliance leak no one could see.
"""
from __future__ import annotations


def _contract(masking: str) -> dict:
    return {
        "version": "1.0.0",
        "info": {"title": "customers"},
        "model": {"fields": [
            {"name": "customer_id", "type": "integer"},
            {"name": "email", "type": "string", "pii": True, "masking": masking},
            {"name": "age", "type": "integer"},
        ]},
        "quality": {"row_rules": [{"name": "adult", "sql": "age >= 18"}]},
    }


def test_quarantined_rows_do_not_keep_the_raw_value(tmp_path):
    from lakelogic import DataProcessor

    (tmp_path / "c.csv").write_text(
        "customer_id,email,age\n1,a@example.com,30\n2,kid@example.com,12\n", encoding="utf-8")
    good, bad = DataProcessor(_contract("hash"), engine="polars").run_source(str(tmp_path / "c.csv"))
    assert len(bad) == 1
    assert "kid@example.com" not in [str(v) for v in bad["email"].to_list()]
    assert "a@example.com" not in [str(v) for v in good["email"].to_list()]


def test_redacted_quarantine_matches_the_good_rows_strategy(tmp_path):
    from lakelogic import DataProcessor

    (tmp_path / "c.csv").write_text("customer_id,email,age\n2,kid@example.com,12\n", encoding="utf-8")
    _, bad = DataProcessor(_contract("nullify"), engine="polars").run_source(str(tmp_path / "c.csv"))
    assert bad["email"].to_list() == [None]
