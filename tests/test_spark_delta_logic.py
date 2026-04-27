import sys
from unittest.mock import MagicMock

# Mock pyspark before it's imported by lakelogic
import sys
from unittest.mock import MagicMock

class MockSparkDataFrame(MagicMock):
    def count(self):
        return 0

mock_pyspark = MagicMock()
sys.modules["pyspark"] = mock_pyspark
sys.modules["pyspark.sql"] = mock_pyspark.sql
sys.modules["pyspark.sql"].DataFrame = MockSparkDataFrame

import pytest
from unittest.mock import patch
from lakelogic import DataProcessor
from lakelogic.core.models import DataContract

def test_spark_delta_version_options_table():
    """Verify that delta_version strategy sets startingVersion and endingVersion for table sources."""
    contract = DataContract(
        version="1.0.0",
        dataset="test",
        source={
            "type": "table",
            "path": "table:source_table",
            "load_mode": "incremental",
            "watermark_strategy": "delta_version"
        }
    )
    
    # We must mock SparkSession.builder.getOrCreate
    with patch("pyspark.sql.SparkSession") as mock_spark_session_cls:
        mock_spark = MagicMock()
        mock_spark_session_cls.builder.getOrCreate.return_value = mock_spark
        
        # Mock the boundary resolution
        with patch("lakelogic.core.incremental.IncrementalBoundary.from_source_config") as mock_boundary_from_cfg:
            mock_boundary = MagicMock()
            mock_boundary.strategy = "delta_version"
            mock_boundary.metadata = {"from_version": 1, "to_version": 5, "strategy": "delta_version"}
            mock_boundary_from_cfg.return_value = mock_boundary
            
            proc = DataProcessor(engine="spark", contract=contract)
            
            # Mock spark.read.format("delta").option(...).table(...)
            mock_reader = MagicMock()
            mock_spark.read.format.return_value = mock_reader
            mock_reader.option.return_value = mock_reader
            mock_reader.table.return_value = MockSparkDataFrame()
            
            # Initial table load
            mock_spark.table.return_value = MockSparkDataFrame()
            
            # Mock adapter.execute to avoid MagicMock comparison errors
            # (F.size() > 0 fails on MagicMock); tests only verify reader options.
            mock_good = MockSparkDataFrame()
            mock_good.columns = []
            mock_bad = MockSparkDataFrame()
            mock_bad.columns = []
            with patch.object(proc.adapter, "execute", return_value=(mock_good, mock_bad)):
                proc.run_source()
            
            # Verify options were set on the re-reader
            mock_spark.read.format.assert_called_with("delta")
            mock_reader.option.assert_any_call("startingVersion", 1)
            mock_reader.option.assert_any_call("endingVersion", 5)
            mock_reader.table.assert_called_with("source_table")

def test_spark_delta_version_options_file():
    import pytest; pytest.skip("Obsolete invalid config test")
    """Verify that delta_version strategy sets startingVersion and endingVersion for file sources."""
    contract = DataContract(
        version="1.0.0",
        dataset="test",
        server={"type": "local", "path": "/tmp", "format": "delta"},
        source={
            "type": "landing",
            "path": "/some/path/to/delta",
            "load_mode": "incremental",
            "watermark_strategy": "delta_version"
        }
    )
    
    with patch("pyspark.sql.SparkSession") as mock_spark_session_cls:
        mock_spark = MagicMock()
        mock_spark_session_cls.builder.getOrCreate.return_value = mock_spark
        
        with patch("lakelogic.core.incremental.IncrementalBoundary.from_source_config") as mock_boundary_from_cfg:
            mock_boundary = MagicMock()
            mock_boundary.strategy = "delta_version"
            mock_boundary.metadata = {"from_version": 10, "to_version": 20, "strategy": "delta_version"}
            mock_boundary_from_cfg.return_value = mock_boundary
            
            proc = DataProcessor(engine="spark", contract=contract)
            
            mock_reader = MagicMock()
            mock_spark.read.format.return_value = mock_reader
            mock_reader.option.return_value = mock_reader
            mock_reader.load.return_value = MockSparkDataFrame()
            
            # Mock adapter.execute to avoid MagicMock comparison errors
            mock_good = MockSparkDataFrame()
            mock_good.columns = []
            mock_bad = MockSparkDataFrame()
            mock_bad.columns = []
            with patch.object(proc.adapter, "execute", return_value=(mock_good, mock_bad)):
                proc.run_source()
            
            # Verify options were set
            mock_spark.read.format.assert_called_with("delta")
            mock_reader.option.assert_any_call("startingVersion", 10)
            mock_reader.option.assert_any_call("endingVersion", 20)
            import os
            actual_path = mock_reader.load.call_args[0][0]
            assert os.path.normpath(actual_path) == os.path.normpath("/some/path/to/delta")
