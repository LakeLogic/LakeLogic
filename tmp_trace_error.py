import sys
import os
from unittest.mock import MagicMock, patch

# Mock pyspark
class MockSparkDataFrame(MagicMock):
    def count(self): return 0
mock_pyspark = MagicMock()
sys.modules["pyspark"] = mock_pyspark
sys.modules["pyspark.sql"] = mock_pyspark.sql
sys.modules["pyspark.sql"].DataFrame = MockSparkDataFrame

# Add project root
sys.path.insert(0, os.getcwd())

from lakelogic import DataProcessor
from lakelogic.core.models import DataContract

def debug_test():
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
    
    with patch("pyspark.sql.SparkSession") as mock_spark_session_cls:
        mock_spark = MagicMock()
        mock_spark_session_cls.builder.getOrCreate.return_value = mock_spark
        
        # Patching the EXACT target that processor.py uses
        with patch("lakelogic.core.processor.IncrementalBoundary.from_source_config") as mock_boundary_from_cfg:
            mock_boundary = MagicMock()
            mock_boundary.strategy = "delta_version"
            mock_boundary.metadata = {"from_version": 1, "to_version": 5, "strategy": "delta_version"}
            mock_boundary_from_cfg.return_value = mock_boundary
            
            proc = DataProcessor(engine="spark", contract=contract)
            
            mock_reader = MagicMock()
            mock_spark.read.format.return_value = mock_reader
            mock_reader.option.return_value = mock_reader
            mock_reader.table.return_value = MockSparkDataFrame()
            mock_spark.table.return_value = MockSparkDataFrame()
            
            print("Running run_source()...")
            try:
                proc.run_source()
                print("Finished run_source() successfully!")
            except Exception as e:
                import traceback
                traceback.print_exc()

if __name__ == "__main__":
    debug_test()
