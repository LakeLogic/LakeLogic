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

def test_notification_aliases():
    """Test notification target aliases."""
    data = {
        "version": "1.0.0",
        "quarantine": {
            "notifications": [
                {"type": "slack", "channel": "#alerts", "on_events": ["quarantine"]},
                {"type": "email", "to": "owner@example.com", "on_events": ["failure"]},
                {"type": "teams", "url": "https://example.com/webhook", "on_events": ["quarantine"]}
            ]
        }
    }
    contract = DataContract(**data)
    assert contract.quarantine.notifications[0].target == "#alerts"
    assert contract.quarantine.notifications[1].target == "owner@example.com"
    assert contract.quarantine.notifications[2].target == "https://example.com/webhook"

def test_service_level_objective():
    """Test service level parsing with nested objects."""
    data = {
        "version": "1.0.0",
        "service_levels": {
            "freshness": {"description": "Daily", "threshold": "24h", "field": "updated_at"},
            "availability": {"description": "Gold layer", "threshold": 99.9}
        }
    }
    contract = DataContract(**data)
    assert contract.service_levels.freshness.description == "Daily"
    assert contract.service_levels.freshness.threshold == "24h"
    assert contract.service_levels.availability.threshold == 99.9
