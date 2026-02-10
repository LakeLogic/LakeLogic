"""
Databricks Unity Catalog Example

Demonstrates how to interact with Databricks Unity Catalog using LakeLogic.

Prerequisites:
    pip install "lakelogic[databricks]"

What it does:
- Connects to a Databricks SQL Warehouse or Cluster.
- Queries a table in Unity Catalog.
- Applies LakeLogic quality gates.
- Demonstrates catalog/schema/table hierarchy.
"""

from lakelogic.core.data_processor import DataProcessor
import os

def main():
    print("=" * 80)
    print("Databricks Unity Catalog Integration")
    print("=" * 80)
    print()

    # In a real scenario, these would come from env vars or secrets
    DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "your-host.azuredatabricks.net")
    HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH", "/sql/1.0/endpoints/your-endpoint")
    
    contract = {
        "version": "1.0.0",
        "dataset": "silver_sales",
        "description": "Sales data from Unity Catalog silver layer",
        
        "source": {
            "type": "database",
            "connector": "databricks",
            "connection": {
                "host": DATABRICKS_HOST,
                "http_path": HTTP_PATH,
                "access_token": "${SECRET:databricks_token}"
            },
            "query": "SELECT * FROM main.silver.sales_transactions LIMIT 1000"
        },
        
        "quality": {
            "row_rules": [
                {
                    "name": "valid_transaction_id",
                    "rule": "not_null",
                    "column": "transaction_id"
                },
                {
                    "name": "positive_amount",
                    "rule": "sql",
                    "expression": "amount > 0"
                }
            ]
        },
        
        "materialization": {
            "gold": {
                "enabled": True,
                "path": "main.gold.aggregated_sales",
                "format": "delta",
                "mode": "overwrite",
                "target_type": "unity_catalog"
            }
        }
    }

    print(f"🚀 Initializing processing from Databricks: {DATABRICKS_HOST}")
    print("📊 Catalog: main, Schema: silver")
    
    # processor = DataProcessor(contract)
    # result = processor.run()
    
    print("\n✅ Example setup complete.")
    print("To run this, ensure DATABRICKS_HOST and DATABRICKS_TOKEN are configured.")

if __name__ == "__main__":
    main()
