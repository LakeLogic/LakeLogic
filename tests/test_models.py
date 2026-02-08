import pytest
from lakelogic.core.models import DataContract

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

def test_row_rule_not_null_list_expands():
    """Ensure not_null accepts a list of fields and expands into rules."""
    from lakelogic.engines.base import EngineAdapter

    class DummyAdapter(EngineAdapter):
        def execute(self, df):
            raise NotImplementedError

    data = {
        "version": "1.0.0",
        "quality": {
            "row_rules": [
                {"not_null": ["a", "b", "c"]}
            ]
        }
    }
    contract = DataContract(**data)
    adapter = DummyAdapter(contract)
    rules = adapter.get_row_rules()
    names = {rule.name for rule in rules}
    assert names == {"a_not_null", "b_not_null", "c_not_null"}

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

def test_notification_extra_fields():
    """Extra notification fields should be preserved for adapters."""
    data = {
        "version": "1.0.0",
        "quarantine": {
            "notifications": [
                {
                    "type": "smtp",
                    "target": "alerts@example.com",
                    "smtp_host": "smtp.example.com",
                    "smtp_port": 587,
                    "from_email": "lakelogic@example.com",
                }
            ]
        }
    }
    contract = DataContract(**data)
    notif = contract.quarantine.notifications[0]
    dumped = notif.model_dump()
    assert dumped["smtp_host"] == "smtp.example.com"
    assert dumped["from_email"] == "lakelogic@example.com"

def test_external_logic_parsing():
    """Test external logic configuration parsing."""
    data = {
        "version": "1.0.0",
        "external_logic": {
            "type": "python",
            "path": "gold/build_sales.py",
            "entrypoint": "build_sales",
            "args": {"target_table": "fact_sales"},
            "output_path": "output/fact_sales.parquet",
            "output_format": "parquet",
            "handles_output": True,
        }
    }
    contract = DataContract(**data)
    logic = contract.external_logic
    assert logic.type == "python"
    assert logic.entrypoint == "build_sales"
    assert logic.output_format == "parquet"

def test_transformation_rename_mappings():
    """Rename transformation should accept mappings dict."""
    data = {
        "version": "1.0.0",
        "transformations": [
            {"rename": {"mappings": {"old_a": "new_a", "old_b": "new_b"}}}
        ],
    }
    contract = DataContract(**data)
    rename = contract.transformations[0].rename
    assert rename is not None
    assert rename.iter_pairs() == [("old_a", "new_a"), ("old_b", "new_b")]

def test_quality_rule_category_normalization_warns():
    """Unknown categories should warn and be normalized to lowercase."""
    data = {
        "version": "1.0.0",
        "quality": {
            "row_rules": [
                {"name": "weird_cat", "sql": "id > 0", "category": "WeirdCategory"}
            ]
        }
    }
    with pytest.warns(UserWarning):
        contract = DataContract(**data)
    rule = contract.quality.row_rules[0]
    assert rule.category == "weirdcategory"
