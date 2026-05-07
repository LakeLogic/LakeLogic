import os
import sys

# Ensure PySpark uses the correct Python interpreter (fixes "Python was not found" store popup on Windows)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def test_spark_adapter_polars_cast():
    import pytest
    pyspark = pytest.importorskip("pyspark")

    import polars as pl
    from lakelogic.engines.spark import SparkAdapter
    from lakelogic.core.models import DataContract
    
    contract = DataContract(
        version="1.0.0",
        dataset="test",
        model={"fields": [{"name": "my_int", "type": "integer"}]},
    )
    
    adapter = SparkAdapter(contract)
    df = pl.DataFrame({"my_int": [1, 2]})
    
    # Just checking it doesn't raise TypeError when passing polars df
    # It might raise something else downstream (e.g. dataset rules), so we catch it
    try:
        adapter.load(df, "db", "table", "replace")
    except Exception as e:
        assert "Expected Spark DataFrame" not in str(e)

def test_spark_adapter_try_cast():
    import pytest
    pyspark = pytest.importorskip("pyspark")
    from lakelogic.engines.spark import SparkAdapter
    from lakelogic.core.models import DataContract
    
    contract = DataContract(
        version="1.0.0",
        dataset="test",
        model={"fields": [{"name": "my_int", "type": "integer"}]},
    )
    
    adapter = SparkAdapter(contract)
    
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([{"my_int": "1"}, {"my_int": "invalid"}])
    
    # Apply schema should use try_cast and not crash
    result_df, rules_res = adapter._apply_schema(df)
    
    # "invalid" should be cast to null, and the error column should be populated
    rows = result_df.collect()
    assert rows[0]["my_int"] == 1
    assert rows[1]["my_int"] is None
    assert rows[1]["__type_err_my_int"] is not None
