"""
SQL Server Example

Simple example of extracting data from SQL Server.

Prerequisites:
    pip install "lakelogic[sqlserver]"

Setup:
    1. SQL Server running (local or remote)
    2. Database with sample data
    3. User with read permissions

Run:
    python sqlserver_example.py

What it does:
- Connects to SQL Server
- Extracts data from a table
- Validates schema
- Writes to Delta Lake
"""

from lakelogic.core.data_processor import DataProcessor

def main():
    """
    Extract data from SQL Server.
    """
    
    print("=" * 80)
    print("SQL Server Data Extraction")
    print("=" * 80)
    print()
    
    # Create contract (inline for demo)
    contract = {
        "version": "1.0.0",
        "dataset": "customers",
        "description": "Customer data from SQL Server",
        
        # Source: SQL Server
        "source": {
            "type": "database",
            "connector": "sqlserver",
            "connection": {
                "server": "localhost",
                "database": "AdventureWorks",
                "username": "sa",
                "password": "${SECRET:sqlserver_password}",  # From Key Vault
                "driver": "ODBC Driver 17 for SQL Server"
            },
            "query": "SELECT * FROM Sales.Customer"
        },
        
        # Schema
        "model": {
            "fields": [
                {"name": "CustomerID", "type": "integer"},
                {"name": "PersonID", "type": "integer"},
                {"name": "StoreID", "type": "integer"},
                {"name": "TerritoryID", "type": "integer"},
                {"name": "AccountNumber", "type": "string"},
                {"name": "ModifiedDate", "type": "timestamp"}
            ]
        },
        
        # Quality rules
        "quality": {
            "row_rules": [
                {
                    "name": "customer_id_not_null",
                    "rule": "not_null",
                    "column": "CustomerID",
                    "severity": "error"
                }
            ]
        },
        
        # Output
        "materialization": {
            "bronze": {
                "enabled": True,
                "path": "./data/bronze/customers/",
                "format": "delta",
                "mode": "overwrite"
            }
        }
    }
    
    # Process data
    print("📊 Extracting data from SQL Server...")
    
    processor = DataProcessor(contract)
    result = processor.run()
    
    print()
    print("✅ Extraction complete!")
    print(f"   Records processed: {result.get('records_processed', 0)}")
    print(f"   Output: {contract['materialization']['bronze']['path']}")


if __name__ == "__main__":
    main()
