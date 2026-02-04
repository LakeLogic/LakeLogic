import pytest
import polars as pl
from lakeguard.core.models import DataContract
from lakeguard.engines.polars import PolarsAdapter

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
    assert "_lakeguard_errors" in bad_df.columns

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
