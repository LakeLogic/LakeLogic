"""A monetary column must generate a number, whatever its declared type.

Bronze contracts declare every column as STRING, so the generator picks values by column
NAME. `cost`, `price` and `amount` are in its semantic map; `conversion_value` was not,
so it fell through to the "code-like reference" fallback and produced `CON-9592`. Silver
then cast that to DOUBLE and every gold aggregate over it failed:

    [CAST_INVALID_INPUT] The value 'CON-9592' of the type "STRING" cannot be cast to
    "DOUBLE" because it is malformed.

The generic fallback is right for genuine identifiers, so the fix is to name the money
columns rather than to guess from a suffix -- `attribute_value` and `enum_value` are
legitimately text.
"""
import tempfile
from pathlib import Path

import pytest
import yaml

from lakelogic.core.generator import DataGenerator

# Columns that must produce a parseable number even when declared STRING.
MONEY_COLUMNS = ["conversion_value", "spend", "ad_spend", "cost", "price", "amount"]


def _contract(field_names):
    return {
        "dataset": "bronze_probe",
        "version": "1.0.0",
        "info": {"title": "Probe", "table_name": "bronze_probe", "target_layer": "bronze"},
        "model": {"fields": [{"name": n, "type": "string"} for n in field_names]},
    }


def _generate(contract, rows=6):
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "c.yaml"
        path.write_text(yaml.safe_dump(contract, sort_keys=False), encoding="utf-8")
        frame = DataGenerator(str(path), seed=7).generate(rows)
    return frame.to_dicts() if hasattr(frame, "to_dicts") else frame


@pytest.mark.parametrize("column", MONEY_COLUMNS)
def test_money_column_is_numeric_even_when_declared_string(column):
    rows = _generate(_contract([column]))
    assert rows, "generator produced no rows"
    for row in rows:
        value = row[column]
        # The regression produced 'CON-9592'; float() is exactly the check that failed
        # downstream, so it is the check worth making here.
        float(value)


def test_identifier_columns_still_get_readable_codes():
    """The fallback is right for real identifiers -- do not let the fix widen into one."""
    # widget_code exercises the GENERIC fallback. campaign_reference does not -- it has
    # its own reference rule -- so it cannot detect the fallback changing.
    rows = _generate(_contract(["widget_code"]))
    values = [r["widget_code"] for r in rows]
    assert any(not _is_number(v) for v in values), (
        "an identifier column now generates numbers; the money fix has over-reached"
    )


def _is_number(value):
    try:
        float(value)
    except (TypeError, ValueError):
        return False
    return True
