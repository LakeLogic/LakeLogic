"""Field-level constraints run (2026-10-24).

OLC: rules "travel with a field … still SQL under the hood" — `accepted_values`, `min`/`max`,
`max_length`. Core read them for profiling and data generation but never ran them, so a contract
declaring `status: accepted_values: [...]` let any status through. `accepted_values` and
`min`/`max` mean exactly what their shorthand twins mean (an empty value fails both forms);
`max_length` lets an empty value pass. A field a written shorthand rule covers gets no second rule.
"""
from __future__ import annotations

from collections import Counter

import polars as pl
import pytest

import lakelogic as ll

ROWS = pl.DataFrame({
    "id": ["a1", "a2", "a3", "a4", "a5", "a6"],
    "status": ["open", "closed", "lost", None, "open", "open"],
    "amount": [10.0, 0.0, 50.0, 20.0, -1.0, 2_000.0],
    "code": ["AB", "ABC", "AB", "AB", None, "ABCDEFG"],
})


def _write(tmp_path, quality: str = "") -> str:
    p = tmp_path / "c.yaml"
    p.write_text(
        "version: 1.0.0\ndataset: t\ninfo: {title: T, target_layer: silver}\nmodel:\n  fields:\n"
        "    - {name: id, type: string}\n"
        "    - {name: status, type: string, accepted_values: [open, closed]}\n"
        "    - {name: amount, type: double, min: 0, max: 1000}\n"
        "    - {name: code, type: string, max_length: 3}\n" + quality,
        encoding="utf-8",
    )
    return str(p)


def _failures(bad) -> Counter:
    return Counter(e.split(" (")[0].replace("Rule failed: ", "")
                   for errs in bad["_lakelogic_errors"].to_list() for e in errs)


@pytest.mark.parametrize("engine", ["polars", "duckdb"])
def test_field_constraints_quarantine_the_rows_that_break_them(tmp_path, engine):
    good, bad = ll.DataProcessor(_write(tmp_path), engine=engine).run(ROWS)
    failed = _failures(bad)
    # a3 "lost" and a4 empty fail the status list, as `status IN (...)` does; a5 -1 and a6 2,000
    # break the range; a6's 7-character code breaks max_length, a5's empty code does not.
    assert failed == {"status_accepted_values": 2, "amount_range": 2, "code_max_length": 1}
    assert sorted(bad["id"].to_list()) == ["a3", "a4", "a5", "a6"]
    assert len(good) + len(bad) == len(ROWS)


@pytest.mark.parametrize("engine", ["polars", "duckdb"])
def test_a_written_shorthand_rule_is_not_doubled(tmp_path, engine):
    quality = (
        "quality:\n  row_rules:\n"
        "    - accepted_values: {name: status_known, field: status, values: [open, closed]}\n"
        "    - range: {name: amount_ok, field: amount, min: 0, max: 1000}\n"
    )
    _good, bad = ll.DataProcessor(_write(tmp_path, quality), engine=engine).run(ROWS)
    failed = _failures(bad)
    assert "status_accepted_values" not in failed and "amount_range" not in failed
    assert failed["status_known"] == 2 and failed["amount_ok"] == 2
