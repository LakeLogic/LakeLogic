"""Engine and generator defects a SaaS dry run surfaced (run d3b11930, 7 of 65 gold facts failed).

* ``decimal(p,s)`` was never cast by the Polars engine, so a string ``"24.56"`` stayed a string
  in silver and every gold ``SUM(cost)`` failed with ``sum(VARCHAR)``;
* the generator did not recognise ``decimal(p,s)`` and generated ``"CAN-3538"`` for a fee;
* an accumulating snapshot's milestone rule failed when the EARLIER milestone was null;
* message milestones (sent -> delivered -> opened) were generated in any order.
"""
from __future__ import annotations

import datetime as dt
from decimal import Decimal

import polars as pl
import pytest
import yaml

from lakelogic import DataGenerator, DataProcessor


def _contract(tmp_path, fields, **extra):
    doc = {"version": "1.0.0", "info": {"title": "t"}, "dataset": "t",
           "model": {"fields": fields}, **extra}
    path = tmp_path / "c.yaml"
    path.write_text(yaml.safe_dump(doc), encoding="utf-8")
    return str(path)


def test_the_polars_engine_casts_a_parameterised_decimal(tmp_path):
    path = _contract(tmp_path, [{"name": "id", "type": "string"},
                                {"name": "cost", "type": "decimal(18,2)"}])
    result = DataProcessor(path, engine="polars").run(
        pl.DataFrame({"id": ["a", "b"], "cost": ["24.56", "1.5"]}))
    assert result.bad_count == 0
    assert result.good.schema["cost"] == pl.Decimal(18, 2)
    assert result.good["cost"].to_list() == [Decimal("24.56"), Decimal("1.50")]


def test_the_generator_makes_a_parameterised_decimal_a_number(tmp_path):
    path = _contract(tmp_path, [{"name": "cancellation_fee", "type": "decimal(10,2)",
                                 "required": True}])
    df = DataGenerator(path, seed=1).generate(rows=20, output_format="polars")
    values = [v for v in df["cancellation_fee"].to_list() if v is not None]
    assert values and all(isinstance(float(v), float) for v in values)


def test_a_milestone_rule_does_not_fail_a_row_whose_earlier_milestone_is_missing(tmp_path):
    path = _contract(
        tmp_path,
        [{"name": "id", "type": "string"},
         {"name": "delivered_at", "type": "timestamp"},
         {"name": "opened_at", "type": "timestamp"}],
        materialization={"strategy": "append", "fact": {
            "type": "accumulating_snapshot", "milestone_dates": ["delivered_at", "opened_at"]}},
    )
    t = dt.datetime(2026, 7, 1)
    result = DataProcessor(path, engine="polars").run(pl.DataFrame({
        "id": ["no-delivery", "in-order", "out-of-order"],
        "delivered_at": [None, t, t],
        "opened_at": [t, t + dt.timedelta(hours=1), t - dt.timedelta(hours=1)],
    }))
    assert result.bad["id"].to_list() == ["out-of-order"]


def test_message_milestones_are_generated_in_order():
    gen = DataGenerator({"sent_at": "timestamp", "delivered_at": "timestamp",
                         "opened_at": "timestamp"}, seed=3)
    row = {"sent_at": "2026-07-10T10:00:00", "delivered_at": "2026-07-01T10:00:00",
           "opened_at": "2026-06-01T10:00:00"}
    gen._apply_temporal_ordering(row)
    assert row["sent_at"] <= row["delivered_at"] <= row["opened_at"]
    # No send recorded: delivered -> opened must still hold on its own.
    row = {"sent_at": None, "delivered_at": "2026-07-10T10:00:00",
           "opened_at": "2026-07-01T10:00:00"}
    gen._apply_temporal_ordering(row)
    assert row["delivered_at"] <= row["opened_at"]


# ── A value that does not fit its decimal ───────────────────────────────────────
# Approve crashed with `decimal precision 10 can't fit values with 11 digits`: the generator
# ignored precision, and Polars' number -> Decimal cast RAISES on overflow even non-strict.


def test_the_generator_keeps_valid_decimals_inside_their_precision_and_scale(tmp_path):
    path = _contract(tmp_path, [{"name": n, "type": "decimal(4,2)", "required": True}
                                for n in ("estimated_fare", "total_value", "rate")])
    df = DataGenerator(path, seed=1).generate(rows=500, output_format="polars")
    for col in ("estimated_fare", "total_value", "rate"):
        values = [float(v) for v in df[col].to_list() if v is not None]
        assert values and max(abs(v) for v in values) <= 99.99, col
        assert all(round(v, 2) == v for v in values), col


def test_the_generator_respects_a_tighter_max():
    from lakelogic.core.generator import _fit_declared_width

    assert _fit_declared_width(123456789012.0, "decimal(10,2)", {}) <= 99_999_999.99
    assert _fit_declared_width(750.0, "decimal(10,2)", {"max": 500}) <= 500
    assert _fit_declared_width(40000, "smallint", {}) <= 32767
    assert _fit_declared_width("x", "decimal(10,2)", {}) == "x"


@pytest.mark.parametrize("engine", ["polars", "duckdb"])
@pytest.mark.parametrize("values", [
    [12345678901.5, 12.5],
    [123456789.12, 12.5],   # 11 digits: THIS one made Polars raise rather than null
    ["12345678901.5", "12.5"],
])
def test_a_decimal_overflow_quarantines_the_row_with_a_plain_reason(tmp_path, engine, values):
    # No quality rules on purpose: DuckDB used to skip type errors entirely without one.
    path = _contract(tmp_path, [{"name": "id", "type": "string"},
                                {"name": "estimated_fare", "type": "decimal(10,2)"}])
    result = DataProcessor(path, engine=engine).run(
        pl.DataFrame({"id": ["big", "ok"], "estimated_fare": values}))
    assert result.good["id"].to_list() == ["ok"]
    assert result.bad["id"].to_list() == ["big"]
    assert result.bad["_lakelogic_errors"].to_list() == [
        ["Type Mismatch: estimated_fare exceeds decimal(10,2) or is not a number"]]
