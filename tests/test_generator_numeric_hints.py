"""Numeric columns use the name hints (rating, price), and pyfloat hints are rounded."""

import pytest
import yaml

from lakelogic.core.generator import DataGenerator

pytest.importorskip("faker")


def _values(frame, column):
    return [v for v in frame[column] if v is not None]


@pytest.mark.parametrize("ftype", ["float", "double", "string"])
def test_rating_is_a_rating_for_every_type(ftype):
    """A float `rating` came out as 629.56: numeric types skipped the name hints."""
    df = DataGenerator({"driver_id": "string", "rating": ftype}, seed=7).generate(rows=200)
    values = [float(v) for v in _values(df, "rating")]
    # float columns are 32-bit, so allow float32 rounding at the bounds.
    assert values and all(3.6 - 1e-6 <= v <= 5.0 + 1e-6 for v in values), values


@pytest.mark.parametrize("ftype", ["double", "string"])
def test_pyfloat_hints_keep_their_decimals(ftype):
    """Faker's pyfloat returned 4.415903208264697 for right_digits=1."""
    df = DataGenerator({"driver_id": "string", "rating": ftype}, seed=7).generate(rows=200)
    values = [float(v) for v in _values(df, "rating")]
    assert values and all(v == round(v, 1) for v in values), values


def test_a_declared_range_still_wins(tmp_path):
    doc = {
        "version": "1.0.0",
        "info": {"title": "drivers", "table_name": "drivers"},
        "model": {
            "fields": [
                {"name": "driver_id", "type": "string", "required": True},
                {"name": "rating", "type": "double"},
            ]
        },
        "quality": {"row_rules": [{"name": "rating_range", "sql": "rating BETWEEN 1 AND 2"}]},
    }
    path = tmp_path / "drivers.yaml"
    path.write_text(yaml.safe_dump(doc), encoding="utf-8")
    df = DataGenerator(str(path), seed=7).generate(rows=100)
    values = [float(v) for v in _values(df, "rating")]
    assert values and all(1.0 <= v <= 2.0 for v in values), values


def test_unhinted_numeric_columns_are_unchanged():
    df = DataGenerator({"id": "string", "widget_factor": "double"}, seed=7).generate(rows=100)
    assert max(float(v) for v in _values(df, "widget_factor")) > 5.0
