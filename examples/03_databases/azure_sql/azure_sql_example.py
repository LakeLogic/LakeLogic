"""
Azure SQL Database Example

Simple example of extracting data from Azure SQL Database.

Prerequisites:
    pip install "lakelogic[azure]"

Setup:
    1. Azure SQL Database created
    2. Azure AD authentication configured (recommended)
    3. Or SQL authentication with username/password

Run:
    python azure_sql_example.py

What it does:
- Connects to Azure SQL Database
- Uses Azure AD authentication (automatic)
- Extracts data from a table
- Writes to Delta Lake

Authentication:
- Uses DefaultAzureCredential (Azure AD)
- No password needed if using Managed Identity or az login
"""

from lakelogic.core.data_processor import DataProcessor

def main():
    """
    Extract data from Azure SQL Database.
    """
    
    print("=" * 80)
    print("Azure SQL Database Data Extraction")
    print("=" * 80)
    print()
    
    # Create contract (inline for demo)
    contract = {
        "version": "1.0.0",
        "dataset": "customers",
        "description": "Customer data from Azure SQL Database",
        
        # Source: Azure SQL Database
        "source": {
            "type": "database",
            "connector": "azure_sql",
            "connection": {
                "server": "myserver.database.windows.net",
                "database": "AdventureWorks",
                # Azure AD authentication (automatic)
                # No username/password needed!
                "authentication": "azure_ad"
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
    print("📊 Extracting data from Azure SQL Database...")
    print("   Using Azure AD authentication (automatic)")
    print()
    
    processor = DataProcessor(contract)
    result = processor.run()
    
    print()
    print("✅ Extraction complete!")
    print(f"   Records processed: {result.get('records_processed', 0)}")
    print(f"   Output: {contract['materialization']['bronze']['path']}")


if __name__ == "__main__":
    main()
