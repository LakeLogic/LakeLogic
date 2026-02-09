"""
Azure SQL CDC Example - Incremental Data Extraction

This example demonstrates:
1. Automatic Azure AD authentication for Azure SQL
2. Incremental CDC extraction using watermark column
3. Data validation with LakeLogic contracts
4. Writing to Delta Lake (Bronze layer)
5. MERGE to Silver layer (deduplication)

Prerequisites:
- pip install "lakelogic[azuresql,delta]"
- az login (for Azure AD authentication)
- Set environment variables (if not using Azure AD):
  - AZURE_SQL_SERVER
  - AZURE_SQL_DATABASE
"""

import os
from datetime import datetime
from lakelogic.engines.database_connectors import AzureSQLConnector
from lakelogic.engines.delta_adapter import DeltaAdapter
from lakelogic import DataProcessor

# ============================================================================
# Example 1: Basic Azure SQL Connection (Azure AD)
# ============================================================================

print("=" * 80)
print("Example 1: Azure SQL Connection with Azure AD")
print("=" * 80)

# Create connector (Azure AD authentication is automatic!)
connector = AzureSQLConnector(
    server="myserver.database.windows.net",
    database="production_db"
)

# Test connection with simple query
df = connector.extract_full(
    table="dbo.orders",
    columns=["order_id", "customer_id", "total_amount"],
    where="order_date >= '2026-02-01'"
)

print(f"✅ Connected to Azure SQL Database")
print(f"✅ Extracted {len(df)} orders")
print(df.head())

connector.close()


# ============================================================================
# Example 2: Incremental CDC Extraction
# ============================================================================

print("\n" + "=" * 80)
print("Example 2: Incremental CDC Extraction")
print("=" * 80)

# Create connector
connector = AzureSQLConnector(
    server="myserver.database.windows.net",
    database="production_db"
)

# First run - extract all data
print("\n📥 First run - full extraction")
df_full = connector.extract_full("dbo.orders")
print(f"✅ Extracted {len(df_full)} total orders")

# Get max watermark
max_watermark = df_full["updated_at"].max()
print(f"📊 Max watermark: {max_watermark}")

# Subsequent runs - incremental extraction
print("\n📥 Subsequent run - incremental extraction")
df_incremental = connector.extract_incremental(
    table="dbo.orders",
    watermark_column="updated_at",
    last_watermark=max_watermark
)

print(f"✅ Extracted {len(df_incremental)} new/updated orders")
print(df_incremental.head())

connector.close()


# ============================================================================
# Example 3: CDC with LakeLogic Contract Validation
# ============================================================================

print("\n" + "=" * 80)
print("Example 3: CDC with Contract Validation")
print("=" * 80)

# Extract incremental data
connector = AzureSQLConnector(
    server="myserver.database.windows.net",
    database="production_db"
)

df_incremental = connector.extract_incremental(
    table="dbo.orders",
    watermark_column="updated_at",
    last_watermark="2026-02-08T00:00:00Z"
)

print(f"📥 Extracted {len(df_incremental)} incremental records")

# Validate with LakeLogic contract
processor = DataProcessor(
    engine="polars",
    contract="examples/04_features/azuresql_cdc_contract.yaml"
)

good_df, bad_df = processor.run(df_incremental)

print(f"✅ Validation complete:")
print(f"  - Good records: {len(good_df)}")
print(f"  - Quarantined records: {len(bad_df)}")

if len(bad_df) > 0:
    print("\n⚠️ Quarantined records:")
    print(bad_df.select(["order_id", "_lakelogic_quarantine_reason"]).head())

connector.close()


# ============================================================================
# Example 4: CDC → Bronze → Silver Pipeline
# ============================================================================

print("\n" + "=" * 80)
print("Example 4: Complete CDC Pipeline (Bronze → Silver)")
print("=" * 80)

# Step 1: Extract incremental data from Azure SQL
print("\n📥 Step 1: Extract from Azure SQL (CDC)")
connector = AzureSQLConnector(
    server="myserver.database.windows.net",
    database="production_db"
)

df_incremental = connector.extract_incremental(
    table="dbo.orders",
    watermark_column="updated_at",
    last_watermark="2026-02-08T00:00:00Z"
)

print(f"✅ Extracted {len(df_incremental)} incremental records")
connector.close()

# Step 2: Validate with contract
print("\n✅ Step 2: Validate with LakeLogic contract")
processor = DataProcessor(
    engine="polars",
    contract="examples/04_features/azuresql_cdc_contract.yaml"
)

good_df, bad_df = processor.run(df_incremental)
print(f"✅ Good: {len(good_df)}, Quarantined: {len(bad_df)}")

# Step 3: Write to Bronze layer (append)
print("\n💾 Step 3: Write to Bronze layer (Delta Lake)")
delta_adapter = DeltaAdapter()

delta_adapter.write(
    df=good_df,
    path="s3://datalake/bronze/orders/",
    mode="append",
    partition_by=["order_date"]
)

print(f"✅ Written {len(good_df)} records to Bronze layer")

# Step 4: MERGE to Silver layer (deduplication)
print("\n🔄 Step 4: MERGE to Silver layer (deduplication)")
merge_stats = delta_adapter.merge(
    target_path="s3://datalake/silver/orders/",
    source_df=good_df,
    merge_key="order_id"
)

print(f"✅ MERGE complete:")
print(f"  - Updated: {merge_stats['num_updated']} records")
print(f"  - Inserted: {merge_stats['num_inserted']} records")

# Step 5: Write quarantined records
if len(bad_df) > 0:
    print("\n⚠️ Step 5: Write quarantined records")
    delta_adapter.write(
        df=bad_df,
        path="s3://datalake/quarantine/orders/",
        mode="append"
    )
    print(f"✅ Written {len(bad_df)} quarantined records")


# ============================================================================
# Example 5: Scheduled CDC Pipeline
# ============================================================================

print("\n" + "=" * 80)
print("Example 5: Scheduled CDC Pipeline (Production Pattern)")
print("=" * 80)

def run_cdc_pipeline(
    server: str,
    database: str,
    table: str,
    watermark_column: str,
    contract_path: str,
    bronze_path: str,
    silver_path: str,
    quarantine_path: str
):
    """
    Production-ready CDC pipeline function.
    
    This function can be scheduled to run every hour/day to:
    1. Extract incremental data from Azure SQL
    2. Validate with LakeLogic contract
    3. Write to Bronze layer
    4. MERGE to Silver layer
    5. Track quarantined records
    """
    
    # Get last watermark from metadata (simplified - use metadata store in production)
    # For demo, we'll use a fixed watermark
    last_watermark = "2026-02-08T00:00:00Z"
    
    print(f"\n🚀 Starting CDC pipeline for {table}")
    print(f"📊 Last watermark: {last_watermark}")
    
    # Step 1: Extract incremental data
    print("\n📥 Extracting incremental data...")
    connector = AzureSQLConnector(server=server, database=database)
    
    df_incremental = connector.extract_incremental(
        table=table,
        watermark_column=watermark_column,
        last_watermark=last_watermark
    )
    
    print(f"✅ Extracted {len(df_incremental)} records")
    connector.close()
    
    if len(df_incremental) == 0:
        print("ℹ️ No new records to process")
        return
    
    # Step 2: Validate
    print("\n✅ Validating data...")
    processor = DataProcessor(engine="polars", contract=contract_path)
    good_df, bad_df = processor.run(df_incremental)
    
    print(f"✅ Good: {len(good_df)}, Quarantined: {len(bad_df)}")
    
    # Step 3: Write to Bronze
    print("\n💾 Writing to Bronze layer...")
    delta_adapter = DeltaAdapter()
    delta_adapter.write(df=good_df, path=bronze_path, mode="append")
    print(f"✅ Written {len(good_df)} records to Bronze")
    
    # Step 4: MERGE to Silver
    print("\n🔄 Merging to Silver layer...")
    merge_stats = delta_adapter.merge(
        target_path=silver_path,
        source_df=good_df,
        merge_key="order_id"
    )
    print(f"✅ Updated: {merge_stats['num_updated']}, Inserted: {merge_stats['num_inserted']}")
    
    # Step 5: Handle quarantined records
    if len(bad_df) > 0:
        print(f"\n⚠️ Writing {len(bad_df)} quarantined records...")
        delta_adapter.write(df=bad_df, path=quarantine_path, mode="append")
    
    # Step 6: Update watermark (simplified - use metadata store in production)
    new_watermark = df_incremental[watermark_column].max()
    print(f"\n📊 New watermark: {new_watermark}")
    
    print(f"\n✅ CDC pipeline complete!")
    
    return {
        "records_extracted": len(df_incremental),
        "records_validated": len(good_df),
        "records_quarantined": len(bad_df),
        "records_updated": merge_stats['num_updated'],
        "records_inserted": merge_stats['num_inserted'],
        "new_watermark": new_watermark
    }


# Run the pipeline
stats = run_cdc_pipeline(
    server="myserver.database.windows.net",
    database="production_db",
    table="dbo.orders",
    watermark_column="updated_at",
    contract_path="examples/04_features/azuresql_cdc_contract.yaml",
    bronze_path="s3://datalake/bronze/orders/",
    silver_path="s3://datalake/silver/orders/",
    quarantine_path="s3://datalake/quarantine/orders/"
)

print("\n📊 Pipeline Statistics:")
for key, value in stats.items():
    print(f"  - {key}: {value}")


# ============================================================================
# Example 6: Multi-Table CDC Pipeline
# ============================================================================

print("\n" + "=" * 80)
print("Example 6: Multi-Table CDC Pipeline")
print("=" * 80)

# Define tables to sync
tables_config = [
    {
        "table": "dbo.orders",
        "watermark_column": "updated_at",
        "contract": "examples/04_features/azuresql_cdc_contract.yaml",
        "bronze_path": "s3://datalake/bronze/orders/",
        "silver_path": "s3://datalake/silver/orders/",
        "merge_key": "order_id"
    },
    {
        "table": "dbo.customers",
        "watermark_column": "updated_at",
        "contract": "examples/04_features/customers_contract.yaml",
        "bronze_path": "s3://datalake/bronze/customers/",
        "silver_path": "s3://datalake/silver/customers/",
        "merge_key": "customer_id"
    },
    {
        "table": "dbo.products",
        "watermark_column": "updated_at",
        "contract": "examples/04_features/products_contract.yaml",
        "bronze_path": "s3://datalake/bronze/products/",
        "silver_path": "s3://datalake/silver/products/",
        "merge_key": "product_id"
    }
]

# Process each table
connector = AzureSQLConnector(
    server="myserver.database.windows.net",
    database="production_db"
)

delta_adapter = DeltaAdapter()

for config in tables_config:
    print(f"\n🔄 Processing {config['table']}...")
    
    # Extract incremental
    df = connector.extract_incremental(
        table=config["table"],
        watermark_column=config["watermark_column"],
        last_watermark="2026-02-08T00:00:00Z"
    )
    
    if len(df) == 0:
        print(f"  ℹ️ No new records")
        continue
    
    # Validate
    processor = DataProcessor(engine="polars", contract=config["contract"])
    good_df, bad_df = processor.run(df)
    
    # Write to Bronze
    delta_adapter.write(df=good_df, path=config["bronze_path"], mode="append")
    
    # MERGE to Silver
    merge_stats = delta_adapter.merge(
        target_path=config["silver_path"],
        source_df=good_df,
        merge_key=config["merge_key"]
    )
    
    print(f"  ✅ Processed {len(df)} records")
    print(f"     - Updated: {merge_stats['num_updated']}, Inserted: {merge_stats['num_inserted']}")

connector.close()

print("\n✅ Multi-table CDC pipeline complete!")
