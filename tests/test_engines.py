import pytest
import polars as pl
from lakelogic.core.models import DataContract
from lakelogic.engines.polars import PolarsAdapter

@pytest.fixture
def mock_contract_simple():
    return DataContract(
        version="1.0.0",
        dataset="test_data",
        quality={
            "row_rules": [
                {"name": "check_id", "sql": "id >= 10"}
            ]
        }
    )

def test_polars_adapter_basic(mock_contract_simple):
    """Test that PolarsAdapter correctly splits good and bad data."""
    df = pl.DataFrame({
        "id": [5, 10, 15]
    })
    
    adapter = PolarsAdapter(mock_contract_simple)
    good_df, bad_df = adapter.execute(df)
    
    # 5 should fail (id < 10), 10 and 15 should pass
    assert len(good_df) == 2
    assert len(bad_df) == 1
    assert bad_df["id"][0] == 5
    assert "_lakelogic_errors" in bad_df.columns

def test_polars_adapter_transformations():
    """Test that transformations are applied to good data."""
    contract = DataContract(
        version="1.0.0",
        transformations=[
            {"rename": {"from": "old_name", "to": "new_name"}},
            {"derive": {"field": "doubled_id", "sql": "id * 2"}}
        ]
    )
    df = pl.DataFrame({
        "old_name": ["a", "b"],
        "id": [1, 2]
    })
    
    adapter = PolarsAdapter(contract)
    good_df, _ = adapter.execute(df)
    
    assert "new_name" in good_df.columns
    assert "old_name" not in good_df.columns
    assert good_df["doubled_id"].to_list() == [2, 4]

def test_polars_adapter_helper_transformations():
    """Test business-friendly helper transformations."""
    contract = DataContract(
        version="1.0.0",
        transformations=[
            {"trim": {"fields": ["name"]}},
            {"lower": {"fields": ["status"]}},
            {"map_values": {"field": "status", "mapping": {"active": "A"}, "default": "U", "output": "status_code"}},
            {"coalesce": {"field": "age", "sources": ["age", "age_backup"], "default": 0, "output": "age_clean"}},
            {"select": {"columns": ["name", "status_code", "age_clean", "tags"]}},
        ],
    )
    df = pl.DataFrame({
        "name": [" Alice "],
        "status": ["ACTIVE"],
        "age": [None],
        "age_backup": [30],
        "tags": ["a|b"],
    })
    adapter = PolarsAdapter(contract)
    good_df, _ = adapter.execute(df)
    assert good_df["name"].to_list() == ["Alice"]
    assert good_df["status_code"].to_list() == ["A"]
    assert good_df["age_clean"].to_list() == [30]

def test_polars_adapter_split_explode():
    """Test split and explode helpers."""
    contract = DataContract(
        version="1.0.0",
        transformations=[
            {"split": {"field": "tags", "delimiter": "|", "output": "tags_array"}},
            {"explode": {"field": "tags_array", "output": "tag"}},
        ],
    )
    df = pl.DataFrame({"id": [1], "tags": ["a|b"]})
    adapter = PolarsAdapter(contract)
    good_df, _ = adapter.execute(df)
    assert set(good_df["tag"].to_list()) == {"a", "b"}

def test_schema_policy_drop_unknown_fields():
    """Unknown fields are dropped when policy is 'drop'."""
    contract = DataContract(
        version="1.0.0",
        schema_policy={"unknown_fields": "drop"},
        model={"fields": [{"name": "id", "type": "integer"}]}
    )
    df = pl.DataFrame({"id": [1, 2], "extra": ["x", "y"]})
    adapter = PolarsAdapter(contract)
    good_df, bad_df = adapter.execute(df)
    assert "extra" not in good_df.columns
    assert len(bad_df) == 0

def test_schema_policy_quarantine_unknown_fields():
    """Unknown fields are quarantined when policy is 'quarantine'."""
    contract = DataContract(
        version="1.0.0",
        schema_policy={"unknown_fields": "quarantine"},
        model={"fields": [{"name": "id", "type": "integer"}]}
    )
    df = pl.DataFrame({"id": [1], "extra": ["x"]})
    adapter = PolarsAdapter(contract)
    good_df, bad_df = adapter.execute(df)
    assert len(good_df) == 0
    assert len(bad_df) == 1

def test_quality_helper_expansion():
    """Structured quality helpers expand into SQL rules."""
    contract = DataContract(
        version="1.0.0",
        dataset="demo",
        quality={
            "row_rules": [
                {"not_null": "email"},
                {"accepted_values": {"field": "status", "values": ["A", "B"]}},
                {"range": {"field": "age", "min": 18, "max": 65}},
            ],
            "dataset_rules": [
                {"unique": "id"},
                {"null_ratio": {"field": "email", "max": 0.1}},
                {"row_count_between": {"min": 1, "max": 100}},
            ],
        },
    )
    adapter = PolarsAdapter(contract)
    row_rules = adapter.get_row_rules()
    dataset_rules = adapter.get_dataset_rules()

    assert any(rule.sql == "\"email\" IS NOT NULL" for rule in row_rules)
    assert any("\"status\" IN ('A', 'B')" in rule.sql for rule in row_rules)
    assert any("\"age\" >=" in rule.sql and "\"age\" <=" in rule.sql for rule in row_rules)

    unique_rule = next(rule for rule in dataset_rules if rule.name == "id_unique")
    assert "COUNT(DISTINCT \"id\")" in unique_rule.sql
    null_ratio_rule = next(rule for rule in dataset_rules if rule.name == "email_null_ratio")
    assert null_ratio_rule.must_be_less_than == 0.1
    row_count_rule = next(rule for rule in dataset_rules if rule.name == "row_count_between")
    assert row_count_rule.must_be_between == [1, 100]

def test_quality_helper_quotes_spaced_fields():
    """Generated rules should quote spaced identifiers."""
    contract = DataContract(
        version="1.0.0",
        quality={"row_rules": [{"not_null": "full name"}]},
    )
    adapter = PolarsAdapter(contract)
    rule = adapter.get_row_rules()[0]
    assert rule.sql == "\"full name\" IS NOT NULL"
