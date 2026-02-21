import os
import polars as pl
from lakelogic import DataProcessor, DataContract

# Create a dummy contract
contract_data = {
    "dataset": "test_verification",
    "quality": {
        "row_rules": [
            {"name": "age_check", "rule": "age > 18", "type": "sql"}
        ]
    }
}

# Create test data
df = pl.DataFrame({
    "id": [1, 2, 3],
    "name": ["Alice", "Bob", "Charlie"],
    "age": [25, 15, 30]
})
df.write_csv("verify_trace.csv")

print("--- Running with trace='enabled' ---")
processor = DataProcessor(contract=DataContract(**contract_data), trace="enabled")
# This should automatically show the trace AND log row samples (if log level is DEBUG)
# We can set loguru level to DEBUG manually if needed
from loguru import logger
import sys

# Remove default logger and add one with DEBUG level
logger.remove()
logger.add(sys.stderr, level="DEBUG")

result = processor.run(df)

print("\n--- Running run_source ---")
# This should also show the trace including 'Load Source' step
result_source = processor.run_source("verify_trace.csv")

print("\n--- Manual show_trace call ---")
processor.show_trace()
