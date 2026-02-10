"""
Fabric LakeDB Example - Spark-Free Delta Lake Operations

This example demonstrates:
1. Reading from Fabric LakeDB tables using Delta-RS (no Spark)
2. Validating data with LakeLogic contracts
3. Writing validated data back to Fabric LakeDB
4. MERGE operations (upsert) without Spark

Prerequisites:
- pip install "lakelogic[delta]"
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

# Option 1: Account Key Authentication
os.environ["AZURE_STORAGE_ACCOUNT_NAME"] = "onelake"
os.environ["AZURE_STORAGE_ACCOUNT_KEY"] = "..."

# Option 2: Azure AD Authentication (Recommended)
# Run: az login
# Credentials are automatically picked up

# ============================================================================
# Example 1: Read Fabric LakeDB Table (Simple)
# ============================================================================

print("=" * 80)
print("Example 1: Read Fabric LakeDB Table")
print("=" * 80)

# Use Fabric table name directly (workspace.lakehouse.table)
processor = DataProcessor(
    engine="polars",
    contract="contracts/fabric_lakedb_contract.yaml"
)

# LakeLogic automatically:
# 1. Resolves "myworkspace.sales_lakehouse.transactions" to OneLake path
# 2. Uses Delta-RS to read (no Spark!)
# 3. Validates data
good_df, bad_df = processor.run_source("myworkspace.sales_lakehouse.transactions")

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

# Resolve Fabric table name to OneLake path
table_name = "myworkspace.sales_lakehouse.transactions"
storage_path = resolve_catalog_path(table_name, platform="fabric")

print(f"Table name: {table_name}")
print(f"OneLake path: {storage_path}")
# Output: abfss://myworkspace@onelake.dfs.fabric.microsoft.com/sales_lakehouse.Lakehouse/Tables/transactions/

# Read directly with Delta adapter
adapter = DeltaAdapter()
df = adapter.read(table_name)  # Can use table name or OneLake path

print(f"Records read: {len(df)}")
print(df.head())

# ============================================================================
# Example 3: MERGE Operation (Upsert) - No Spark Required!
# ============================================================================

print("\n" + "=" * 80)
print("Example 3: MERGE Operation (Upsert)")
print("=" * 80)

# Create new/updated transaction data
new_transactions = pl.DataFrame({
    "transaction_id": ["TXN001", "TXN002", "TXN999"],
    "customer_id": [101, 102, 999],
    "product_id": ["PROD-A", "PROD-B", "PROD-C"],
    "quantity": [2, 5, 1],
    "unit_price": [29.99, 49.99, 19.99],
    "total_amount": [59.98, 249.95, 19.99],
    "transaction_date": ["2026-02-09", "2026-02-09", "2026-02-09"],
    "payment_method": ["credit_card", "paypal", "debit_card"]
})

print(f"New/updated transactions: {len(new_transactions)}")
print(new_transactions)

# MERGE into Fabric LakeDB table (atomic, no Spark!)
adapter = DeltaAdapter()
stats = adapter.merge(
    target_path="myworkspace.sales_lakehouse.transactions",  # Fabric table name
    source_df=new_transactions,
    merge_key="transaction_id"
)

print(f"\n✅ MERGE complete:")
print(f"  - Updated: {stats['num_updated']} records")
print(f"  - Inserted: {stats['num_inserted']} records")

# ============================================================================
# Example 4: Calculated Fields & Validation
# ============================================================================

print("\n" + "=" * 80)
print("Example 4: Calculated Fields & Validation")
print("=" * 80)

# Create transaction data with missing total_amount
transactions_with_missing = pl.DataFrame({
    "transaction_id": ["TXN100", "TXN101"],
    "customer_id": [201, 202],
    "product_id": ["PROD-X", "PROD-Y"],
    "quantity": [3, 2],
    "unit_price": [15.00, 25.00],
    "total_amount": [None, None],  # Missing - will be calculated
    "transaction_date": ["2026-02-09", "2026-02-09"],
    "payment_method": ["cash", "credit_card"]
})

print("Transactions with missing total_amount:")
print(transactions_with_missing)

# Process with contract (calculates total_amount)
processor = DataProcessor(
    engine="polars",
    contract="contracts/fabric_lakedb_contract.yaml"
)
good_df, bad_df = processor.run(transactions_with_missing)

print(f"\nAfter processing:")
print(good_df[["transaction_id", "quantity", "unit_price", "total_amount"]])

# ============================================================================
# Example 5: Write to Fabric LakeDB
# ============================================================================

print("\n" + "=" * 80)
print("Example 5: Write to Fabric LakeDB")
print("=" * 80)

# Create sample data
sample_data = pl.DataFrame({
    "transaction_id": ["TXN200", "TXN201", "TXN202"],
    "customer_id": [301, 302, 303],
    "product_id": ["PROD-1", "PROD-2", "PROD-3"],
    "quantity": [1, 2, 3],
    "unit_price": [10.00, 20.00, 30.00],
    "total_amount": [10.00, 40.00, 90.00],
    "transaction_date": ["2026-02-09", "2026-02-09", "2026-02-09"],
    "payment_method": ["credit_card", "debit_card", "paypal"]
})

# Write to Fabric LakeDB table
adapter.write(
    df=sample_data,
    path="myworkspace.sales_lakehouse.transactions_test",  # Fabric table name
    mode="append"
)

print(f"✅ Wrote {len(sample_data)} records to Fabric LakeDB")

# ============================================================================
# Example 6: Complete Pipeline (Read → Validate → MERGE)
# ============================================================================

print("\n" + "=" * 80)
print("Example 6: Complete Pipeline")
print("=" * 80)

# Step 1: Read from Fabric LakeDB
print("Step 1: Reading from Fabric LakeDB...")
processor = DataProcessor(
    engine="polars",
    contract="contracts/fabric_lakedb_contract.yaml"
)
good_df, bad_df = processor.run_source("myworkspace.sales_lakehouse.transactions")

print(f"  ✅ Good: {len(good_df)}, ❌ Bad: {len(bad_df)}")

# Step 2: MERGE validated data back to Fabric
print("\nStep 2: MERGE validated data to validated table...")
adapter = DeltaAdapter()
stats = adapter.merge(
    target_path="myworkspace.sales_lakehouse.transactions_validated",
    source_df=good_df,
    merge_key="transaction_id"
)

print(f"  ✅ Updated: {stats['num_updated']}, Inserted: {stats['num_inserted']}")

# Step 3: Write quarantined data
if len(bad_df) > 0:
    print("\nStep 3: Writing quarantined data...")
    adapter.write(
        df=bad_df,
        path="myworkspace.sales_lakehouse.transactions_quarantine",
        mode="append"
    )
    print(f"  ✅ Wrote {len(bad_df)} quarantined records")

print("\n✅ Pipeline complete!")

# ============================================================================
# Example 7: Time Travel
# ============================================================================

print("\n" + "=" * 80)
print("Example 7: Time Travel")
print("=" * 80)

# Read specific version
df_v1 = adapter.read("myworkspace.sales_lakehouse.transactions", version=1)
print(f"Version 1: {len(df_v1)} records")

# Read at specific timestamp
df_yesterday = adapter.read(
    "myworkspace.sales_lakehouse.transactions",
    timestamp="2026-02-08T00:00:00Z"
)
print(f"Yesterday: {len(df_yesterday)} records")

# Get table history
history = adapter.get_history("myworkspace.sales_lakehouse.transactions", limit=5)
print(f"\nTable history (last 5 commits):")
print(history)

# ============================================================================
# Example 8: Optimize & Vacuum
# ============================================================================

print("\n" + "=" * 80)
print("Example 8: Optimize & Vacuum")
print("=" * 80)

# Optimize (compact small files)
print("Optimizing table...")
stats = adapter.optimize("myworkspace.sales_lakehouse.transactions")
print(f"✅ Optimization complete:")
print(f"  - Files added: {stats['num_files_added']}")
print(f"  - Files removed: {stats['num_files_removed']}")

# Vacuum (delete old files) - dry run first
print("\nVacuum dry run (7 days retention)...")
files = adapter.vacuum(
    "myworkspace.sales_lakehouse.transactions",
    retention_hours=168,
    dry_run=True
)
print(f"Would delete {len(files)} files")

# Uncomment to actually vacuum:
# adapter.vacuum("myworkspace.sales_lakehouse.transactions", retention_hours=168, dry_run=False)

# ============================================================================
# Example 9: Azure AD Authentication
# ============================================================================

print("\n" + "=" * 80)
print("Example 9: Azure AD Authentication (Recommended)")
print("=" * 80)

try:
    from azure.identity import DefaultAzureCredential
    
    # Get Azure AD token
    credential = DefaultAzureCredential()
    token = credential.get_token("https://storage.azure.com/.default")
    
    # Create adapter with Azure AD auth
    adapter_ad = DeltaAdapter(storage_options={
        "AZURE_STORAGE_ACCOUNT_NAME": "onelake",
        "BEARER_TOKEN": token.token
    })
    
    # Read with Azure AD auth
    df = adapter_ad.read("myworkspace.sales_lakehouse.transactions")
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
✅ Fabric LakeDB + Delta-RS Features:
  - Read Fabric LakeDB tables (no Spark!)
  - Write to Fabric LakeDB tables
  - Atomic MERGE operations (upsert)
  - Calculated fields & transformations
  - Time travel (version/timestamp)
  - Optimize & vacuum
  - Azure AD authentication support

📚 Learn More:
  - docs/delta_lake_support.md
  - docs/catalog_table_names.md
  - examples/04_features/fabric_lakedb_contract.yaml
""")
