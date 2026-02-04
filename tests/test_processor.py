import pytest
import polars as pl
from lakeguard import DataProcessor
from lakeguard.core.models import DataContract

def test_processor_init():
    """Test processor initialization with different contract formats."""
    # From dict
    data = {"version": "1.0.0", "dataset": "test"}
    proc = DataProcessor(engine="polars", contract=data)
    assert proc.engine_name == "polars"
    assert proc.contract.version == "1.0.0"

def test_processor_run_mock():
    """Test the full run cycle with mock data."""
    contract_data = {
        "version": "1.0.0",
        "dataset": "test_ds",
        "quality": {
            "row_rules": [{"name": "not_null", "sql": "val IS NOT NULL"}]
        }
    }
    df = pl.DataFrame({"val": [1, None, 3]})
    
    processor = DataProcessor(engine="polars", contract=contract_data)
    good_df, bad_df = processor.run(df)
    
    assert len(good_df) == 2
    assert len(bad_df) == 1
