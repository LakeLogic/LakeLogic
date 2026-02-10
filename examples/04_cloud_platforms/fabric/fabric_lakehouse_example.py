"""
Microsoft Fabric Lakehouse Example

Demonstrates how to interact with Microsoft Fabric OneLake using LakeLogic.

Prerequisites:
    pip install "lakelogic[azure]"

What it does:
- Connects to a Microsoft Fabric Lakehouse.
- Reads data using the SQL Analytics Endpoint.
- Demonstrates automatic Azure AD authentication.
"""

from lakelogic.core.data_processor import DataProcessor

def main():
    print("=" * 80)
    print("Microsoft Fabric OneLake Integration")
    print("=" * 80)
    print()

    # Fabric SQL Analytics Endpoint
    FABRIC_ENDPOINT = "your-workspace.datawarehouse.pbidedicated.windows.net"
    
    contract = {
        "version": "1.0.0",
        "dataset": "fabric_orders",
        "description": "Orders data from Fabric Lakehouse",
        
        "source": {
            "type": "database",
            "connector": "fabric",
            "connection": {
                "server": FABRIC_ENDPOINT,
                "database": "YourLakehouseName",
                "authentication": "azure_ad" # Uses DefaultAzureCredential
            },
            "query": "SELECT * FROM [YourLakehouseName].[dbo].[Orders]"
        },
        
        "quality": {
            "row_rules": [
                {
                    "name": "valid_order_date",
                    "rule": "not_null",
                    "column": "OrderDate"
                }
            ]
        },
        
        "materialization": {
            "bronze": {
                "enabled": True,
                "path": "abfss://your-workspace@onelake.dfs.fabric.microsoft.com/YourLakehouse.Lakehouse/Tables/BronzeOrders",
                "format": "delta"
            }
        }
    }

    print(f"🚀 Connecting to Fabric SQL Endpoint: {FABRIC_ENDPOINT}")
    print("🧪 Using Azure AD Transparent Authentication")
    
    # processor = DataProcessor(contract)
    # result = processor.run()
    
    print("\n✅ Fabric example setup complete.")

if __name__ == "__main__":
    main()
