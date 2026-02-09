"""
Synapse Analytics Example - Spark-Free Delta Lake Operations

This example demonstrates:
1. Reading from Synapse Analytics tables using Delta-RS (no Spark)
2. Validating data with LakeLogic contracts
3. Writing validated data back to Synapse
4. MERGE operations (upsert) without Spark

Prerequisites:
- pip install "lakelogic[delta]"
- Set SYNAPSE_STORAGE_ACCOUNT environment variable
- Set Azure credentials (AZURE_STORAGE_ACCOUNT_NAME, AZURE_STORAGE_ACCOUNT_KEY)
  Or use Azure AD authentication (az login)
"""

import os
from lakelogic import DataProcessor
from lakelogic.engines.delta_adapter import DeltaAdapter
from lakelogic.engines.unity_catalog import resolve_catalog_path
import polars as pl

# ============================================================================
# Setup: Configure Credentials
# ============================================================================

# Required: Synapse storage account
os.environ["SYNAPSE_STORAGE_ACCOUNT"] = "mysynapsestorage"

# Option 1: Account Key Authentication
os.environ["AZURE_STORAGE_ACCOUNT_NAME"] = "mysynapsestorage"
os.environ["AZURE_STORAGE_ACCOUNT_KEY"] = "..."

# Option 2: Azure AD Authentication (Recommended)
# Run: az login
# Credentials are automatically picked up

# ============================================================================
# Example 1: Read Synapse Analytics Table (Simple)
# ============================================================================

print("=" * 80)
print("Example 1: Read Synapse Analytics Table")
print("=" * 80)

# Use Synapse table name directly (database.schema.table)
processor = DataProcessor(
    engine="polars",
    contract="contracts/synapse_analytics_contract.yaml"
)

# LakeLogic automatically:
# 1. Resolves "inventorydb.dbo.stock_levels" to ADLS Gen2 path
# 2. Uses Delta-RS to read (no Spark!)
# 3. Validates data
good_df, bad_df = processor.run_source("inventorydb.dbo.stock_levels")

print(f"✅ Good records: {len(good_df)}")
print(f"❌ Quarantined records: {len(bad_df)}")
print(f"\nGood data preview:")
print(good_df.head())

if len(bad_df) > 0:
    print(f"\nQuarantined data preview:")
    print(bad_df.head())

# ============================================================================
# Example 2: Manual Path Resolution
# ============================================================================

print("\n" + "=" * 80)
print("Example 2: Manual Path Resolution")
print("=" * 80)

# Resolve Synapse table name to ADLS Gen2 path
table_name = "inventorydb.dbo.stock_levels"
storage_path = resolve_catalog_path(table_name, platform="synapse")

print(f"Table name: {table_name}")
print(f"ADLS Gen2 path: {storage_path}")
# Output: abfss://inventorydb@mysynapsestorage.dfs.core.windows.net/dbo/stock_levels/

# Read directly with Delta adapter
adapter = DeltaAdapter()
df = adapter.read(table_name)  # Can use table name or ADLS path

print(f"Records read: {len(df)}")
print(df.head())

# ============================================================================
# Example 3: MERGE Operation (Upsert) - No Spark Required!
# ============================================================================

print("\n" + "=" * 80)
print("Example 3: MERGE Operation (Upsert)")
print("=" * 80)

# Create new/updated inventory data
new_inventory = pl.DataFrame({
    "product_id": ["PROD-001", "PROD-002", "PROD-999"],
    "warehouse_id": ["WH-01", "WH-01", "WH-02"],
    "quantity_on_hand": [100, 50, 200],
    "quantity_reserved": [10, 5, 20],
    "quantity_available": [90, 45, 180],
    "reorder_point": [50, 25, 100],
    "last_updated": ["2026-02-09T10:00:00Z", "2026-02-09T10:00:00Z", "2026-02-09T10:00:00Z"],
    "location": ["Aisle A1", "Aisle A2", "Aisle B1"]
})

print(f"New/updated inventory: {len(new_inventory)}")
print(new_inventory)

# MERGE into Synapse table (atomic, no Spark!)
adapter = DeltaAdapter()
stats = adapter.merge(
    target_path="inventorydb.dbo.stock_levels",  # Synapse table name
    source_df=new_inventory,
    merge_key=["product_id", "warehouse_id"]  # Composite key
)

print(f"\n✅ MERGE complete:")
print(f"  - Updated: {stats['num_updated']} records")
print(f"  - Inserted: {stats['num_inserted']} records")

# ============================================================================
# Example 4: Inventory Reconciliation
# ============================================================================

print("\n" + "=" * 80)
print("Example 4: Inventory Reconciliation")
print("=" * 80)

# Create inventory data with calculated fields
inventory_data = pl.DataFrame({
    "product_id": ["PROD-100", "PROD-101"],
    "warehouse_id": ["WH-03", "WH-03"],
    "quantity_on_hand": [150, 75],
    "quantity_reserved": [30, 15],
    "quantity_available": [None, None],  # Will be calculated
    "reorder_point": [50, 25],
    "last_updated": ["2026-02-09T11:00:00Z", "2026-02-09T11:00:00Z"],
    "location": ["Aisle C1", "Aisle C2"]
})

print("Inventory data (before calculation):")
print(inventory_data)

# Process with contract (calculates quantity_available)
processor = DataProcessor(
    engine="polars",
    contract="contracts/synapse_analytics_contract.yaml"
)
good_df, bad_df = processor.run(inventory_data)

print(f"\nAfter processing:")
print(good_df[["product_id", "quantity_on_hand", "quantity_reserved", "quantity_available", "needs_reorder"]])

# ============================================================================
# Example 5: Reorder Report
# ============================================================================

print("\n" + "=" * 80)
print("Example 5: Reorder Report")
print("=" * 80)

# Read inventory data
df = adapter.read("inventorydb.dbo.stock_levels")

# Filter products that need reordering
needs_reorder = df.filter(pl.col("quantity_available") <= pl.col("reorder_point"))

print(f"Products needing reorder: {len(needs_reorder)}")
print(needs_reorder[["product_id", "warehouse_id", "quantity_available", "reorder_point"]])

# ============================================================================
# Example 6: Write to Synapse
# ============================================================================

print("\n" + "=" * 80)
print("Example 6: Write to Synapse")
print("=" * 80)

# Create sample data
sample_data = pl.DataFrame({
    "product_id": ["PROD-200", "PROD-201", "PROD-202"],
    "warehouse_id": ["WH-04", "WH-04", "WH-04"],
    "quantity_on_hand": [80, 60, 40],
    "quantity_reserved": [8, 6, 4],
    "quantity_available": [72, 54, 36],
    "reorder_point": [30, 30, 30],
    "last_updated": ["2026-02-09T12:00:00Z", "2026-02-09T12:00:00Z", "2026-02-09T12:00:00Z"],
    "location": ["Aisle D1", "Aisle D2", "Aisle D3"]
})

# Write to Synapse table
adapter.write(
    df=sample_data,
    path="inventorydb.dbo.stock_levels_test",  # Synapse table name
    mode="append"
)

print(f"✅ Wrote {len(sample_data)} records to Synapse")

# ============================================================================
# Example 7: Complete Pipeline (Read → Validate → Overwrite)
# ============================================================================

print("\n" + "=" * 80)
print("Example 7: Complete Pipeline (Full Refresh)")
print("=" * 80)

# Step 1: Read from Synapse
print("Step 1: Reading from Synapse...")
processor = DataProcessor(
    engine="polars",
    contract="contracts/synapse_analytics_contract.yaml"
)
good_df, bad_df = processor.run_source("inventorydb.dbo.stock_levels")

print(f"  ✅ Good: {len(good_df)}, ❌ Bad: {len(bad_df)}")

# Step 2: Overwrite validated table (full refresh for inventory)
print("\nStep 2: Overwriting validated table...")
adapter = DeltaAdapter()
adapter.write(
    df=good_df,
    path="inventorydb.dbo.stock_levels_validated",
    mode="overwrite"  # Full refresh
)

print(f"  ✅ Wrote {len(good_df)} validated records")

# Step 3: Write quarantined data
if len(bad_df) > 0:
    print("\nStep 3: Writing quarantined data...")
    adapter.write(
        df=bad_df,
        path="inventorydb.dbo.stock_levels_quarantine",
        mode="append"
    )
    print(f"  ✅ Wrote {len(bad_df)} quarantined records")

print("\n✅ Pipeline complete!")

# ============================================================================
# Example 8: Time Travel
# ============================================================================

print("\n" + "=" * 80)
print("Example 8: Time Travel")
print("=" * 80)

# Read specific version
df_v1 = adapter.read("inventorydb.dbo.stock_levels", version=1)
print(f"Version 1: {len(df_v1)} records")

# Read at specific timestamp
df_yesterday = adapter.read(
    "inventorydb.dbo.stock_levels",
    timestamp="2026-02-08T00:00:00Z"
)
print(f"Yesterday: {len(df_yesterday)} records")

# Compare inventory changes
if len(df_yesterday) > 0 and len(df) > 0:
    print("\nInventory changes:")
    # Join on product_id and warehouse_id
    changes = df.join(
        df_yesterday,
        on=["product_id", "warehouse_id"],
        suffix="_yesterday"
    ).with_columns([
        (pl.col("quantity_on_hand") - pl.col("quantity_on_hand_yesterday")).alias("quantity_change")
    ])
    
    print(changes[["product_id", "warehouse_id", "quantity_on_hand", "quantity_on_hand_yesterday", "quantity_change"]])

# Get table history
history = adapter.get_history("inventorydb.dbo.stock_levels", limit=5)
print(f"\nTable history (last 5 commits):")
print(history)

# ============================================================================
# Example 9: Optimize & Vacuum
# ============================================================================

print("\n" + "=" * 80)
print("Example 9: Optimize & Vacuum")
print("=" * 80)

# Optimize (compact small files)
print("Optimizing table...")
stats = adapter.optimize("inventorydb.dbo.stock_levels")
print(f"✅ Optimization complete:")
print(f"  - Files added: {stats['num_files_added']}")
print(f"  - Files removed: {stats['num_files_removed']}")

# Vacuum (delete old files) - dry run first
print("\nVacuum dry run (7 days retention)...")
files = adapter.vacuum(
    "inventorydb.dbo.stock_levels",
    retention_hours=168,
    dry_run=True
)
print(f"Would delete {len(files)} files")

# Uncomment to actually vacuum:
# adapter.vacuum("inventorydb.dbo.stock_levels", retention_hours=168, dry_run=False)

# ============================================================================
# Example 10: Azure AD Authentication
# ============================================================================

print("\n" + "=" * 80)
print("Example 10: Azure AD Authentication (Recommended)")
print("=" * 80)

try:
    from azure.identity import DefaultAzureCredential
    
    # Get Azure AD token
    credential = DefaultAzureCredential()
    token = credential.get_token("https://storage.azure.com/.default")
    
    # Create adapter with Azure AD auth
    adapter_ad = DeltaAdapter(storage_options={
        "AZURE_STORAGE_ACCOUNT_NAME": "mysynapsestorage",
        "BEARER_TOKEN": token.token
    })
    
    # Read with Azure AD auth
    df = adapter_ad.read("inventorydb.dbo.stock_levels")
    print(f"✅ Read {len(df)} records using Azure AD authentication")

except ImportError:
    print("Azure Identity not installed. Install with: pip install azure-identity")

# ============================================================================
# Summary
# ============================================================================

print("\n" + "=" * 80)
print("Summary")
print("=" * 80)
print("""
✅ Synapse Analytics + Delta-RS Features:
  - Read Synapse tables (no Spark!)
  - Write to Synapse tables
  - Atomic MERGE operations (upsert)
  - Inventory reconciliation & calculated fields
  - Time travel & change tracking
  - Optimize & vacuum
  - Azure AD authentication support
  - Full refresh (overwrite) mode for inventory

📚 Learn More:
  - docs/delta_lake_support.md
  - docs/catalog_table_names.md
  - examples/04_features/synapse_analytics_contract.yaml
""")
