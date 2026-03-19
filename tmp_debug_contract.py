import sys
import os
from unittest.mock import MagicMock

# Mock pyspark
mock_pyspark = MagicMock()
sys.modules["pyspark"] = mock_pyspark
sys.modules["pyspark.sql"] = mock_pyspark.sql

# Add project root to path
sys.path.insert(0, os.getcwd())

from lakelogic.core.models import DataContract

try:
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
    print("Success!")
except Exception as e:
    print(f"FAILED: {e}")
    import traceback
    traceback.print_exc()
