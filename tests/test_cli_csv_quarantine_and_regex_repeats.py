"""Two generation and output bugs found while rewriting the lakelogic.org posts (2026-11-02).

``--output-bad bad.csv`` failed: quarantined rows carry ``_lakelogic_errors`` as a list, and CSV
cannot hold a list. And a repeated regex token was drawn once: ``^ORD-[0-9]{4}$`` gave
``ORD-3333``-style values, ten possible ids where the pattern allows ten thousand.
"""

from __future__ import annotations

import csv
import json
import re

import polars as pl

from lakelogic.cli.main import _csv_ready, run
from lakelogic.core.generator import DataGenerator

CONTRACT = """version: 1.0.0
info:
  title: Orders
model:
  fields:
    - {name: order_id, type: string, required: true}
    - {name: amount, type: float}
quality:
  row_rules:
    - name: amount_positive
      sql: "amount > 0"
"""


def test_quarantined_rows_can_be_written_as_csv(tmp_path):
    contract = tmp_path / "orders.yaml"
    contract.write_text(CONTRACT, encoding="utf-8")
    source = tmp_path / "orders.csv"
    source.write_text("order_id,amount\nA,10\nB,-5\n", encoding="utf-8")
    bad = tmp_path / "bad.csv"

    run(contract=contract, source=source, engine="polars", stage=None, output_good=None,
        output_bad=bad, output_format=None, materialize=False, materialize_target=None,
        trace=False, verbose=False)

    rows = list(csv.DictReader(bad.open(encoding="utf-8")))
    assert [r["order_id"] for r in rows] == ["B"]
    errors = json.loads(rows[0]["_lakelogic_errors"])  # the list survives as JSON text
    assert any("amount_positive" in e for e in errors)


def test_only_nested_columns_change_for_csv():
    df = pl.DataFrame({"id": [1], "errs": [["a", "b"]], "meta": [{"k": 1}]})
    out = _csv_ready(df)
    assert out["id"].to_list() == [1]
    assert json.loads(out["errs"][0]) == ["a", "b"]
    assert json.loads(out["meta"][0]) == {"k": 1}
    flat = pl.DataFrame({"id": [1]})
    assert _csv_ready(flat) is flat


def _gen():
    return DataGenerator({"order_id": "string"}, seed=7)


def test_a_repeated_class_draws_each_character():
    g = _gen()
    values = [g._generate_from_regex("^ORD-[0-9]{4}$") for _ in range(500)]
    assert all(re.fullmatch(r"ORD-[0-9]{4}", v) for v in values)
    assert len(set(values)) > 400, len(set(values))  # was 10: one digit repeated four times
    assert any(len(set(v[4:])) > 1 for v in values)


def test_repeats_of_shorthand_dot_and_groups_vary_too():
    g = _gen()
    for pattern, check in [
        (r"^\d{6}$", r"\d{6}"),
        (r"^[A-Z]{3}-\w{2}$", r"[A-Z]{3}-\w{2}"),
        (r"^(ab|cd){3}$", r"(ab|cd){3}"),
    ]:
        values = [g._generate_from_regex(pattern) for _ in range(200)]
        assert all(re.fullmatch(check, v) for v in values), pattern
        assert len(set(values)) > 5, pattern


def test_a_literal_repeat_stays_literal():
    assert _gen()._generate_from_regex("^X{3}-[0-9]$")[:4] == "XXX-"
