import pytest
from lakeguard.core.models import DataContract

def test_contract_parsing_minimal():
    """Test that a minimal contract can be parsed."""
    data = {
        "version": "1.0.0",
        "dataset": "test_ds"
    }
    contract = DataContract(**data)
    assert contract.version == "1.0.0"
    assert contract.dataset == "test_ds"

def test_contract_quality_rules():
    """Test that quality rules are correctly structured."""
    data = {
        "version": "1.0.0",
        "quality": {
            "row_rules": [
                {"name": "test_rule", "sql": "id > 0", "category": "correctness"}
            ]
        }
    }
    contract = DataContract(**data)
    assert len(contract.quality.row_rules) == 1
    assert contract.quality.row_rules[0].name == "test_rule"
    assert contract.quality.row_rules[0].sql == "id > 0"

def test_transformation_lookup():
    """Test the lookup transformation model."""
    data = {
        "version": "1.0.0",
        "transformations": [
            {
                "lookup": {
                    "field": "name",
                    "reference": "ref_table",
                    "on": "id",
                    "key": "ref_id",
                    "value": "ref_name",
                    "default_value": "Unknown"
                }
            }
        ]
    }
    contract = DataContract(**data)
    lookup = contract.transformations[0].lookup
    assert lookup.field == "name"
    assert lookup.default_value == "Unknown"
