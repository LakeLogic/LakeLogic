"""
Unity Catalog Example - Spark-Free Delta Lake Operations

This example demonstrates:
1. Reading from Unity Catalog tables using Delta-RS (no Spark)
2. Validating data with LakeLogic contracts
3. Writing validated data back to Unity Catalog
4. MERGE operations (upsert) without Spark

Prerequisites:
- pip install "lakelogic[delta]"
- pip install databricks-sdk
- Set DATABRICKS_HOST and DATABRICKS_TOKEN environment variables
- Set AWS credentials (if using S3) or Azure credentials (if using ADLS)
"""

import os
from lakelogic import DataProcessor
from lakelogic.engines.delta_adapter import DeltaAdapter
from lakelogic.engines.unity_catalog import resolve_catalog_path
import polars as pl

# ============================================================================
# Setup: Configure Credentials
# ============================================================================

# Option 1: Set environment variables
os.environ["DATABRICKS_HOST"] = "https://your-workspace.cloud.databricks.com"
os.environ["DATABRICKS_TOKEN"] = "dapi..."

# AWS S3 credentials (if Unity Catalog uses S3)
os.environ["AWS_REGION"] = "us-west-2"
os.environ["AWS_ACCESS_KEY_ID"] = "AKIA..."
os.environ["AWS_SECRET_ACCESS_KEY"] = "..."

# Or Azure credentials (if Unity Catalog uses ADLS)
# os.environ["AZURE_STORAGE_ACCOUNT_NAME"] = "your_account"
# os.environ["AZURE_STORAGE_ACCOUNT_KEY"] = "..."

# ============================================================================
# Example 1: Read Unity Catalog Table (Simple)
# ============================================================================

print("=" * 80)
print("Example 1: Read Unity Catalog Table")
print("=" * 80)

# Use Unity Catalog table name directly
processor = DataProcessor(
    engine="polars",
    contract="contracts/unity_catalog_contract.yaml"
)

# LakeLogic automatically:
# 1. Resolves "main.default.customers" to storage path
# 2. Uses Delta-RS to read (no Spark!)
# 3. Validates data
good_df, bad_df = processor.run_source("main.default.customers")

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

# Resolve Unity Catalog table name to storage path
table_name = "main.default.customers"
storage_path = resolve_catalog_path(table_name)

print(f"Table name: {table_name}")
print(f"Storage path: {storage_path}")

# Read directly with Delta adapter
adapter = DeltaAdapter()
df = adapter.read(table_name)  # Can use table name or storage path

print(f"Records read: {len(df)}")
print(df.head())

# ============================================================================
# Example 3: MERGE Operation (Upsert) - No Spark Required!
# ============================================================================

print("\n" + "=" * 80)
print("Example 3: MERGE Operation (Upsert)")
print("=" * 80)

# Create new/updated customer data
new_customers = pl.DataFrame({
    "customer_id": [1, 2, 999],
    "email": ["alice@example.com", "bob@example.com", "charlie@example.com"],
    "first_name": ["Alice", "Bob", "Charlie"],
    "last_name": ["Smith", "Jones", "Brown"],
    "created_at": ["2026-02-09T10:00:00Z", "2026-02-09T11:00:00Z", "2026-02-09T12:00:00Z"],
    "country": ["US", "UK", "CA"],
    "status": ["active", "active", "active"]
})

print(f"New/updated customers: {len(new_customers)}")
print(new_customers)

# MERGE into Unity Catalog table (atomic, no Spark!)
adapter = DeltaAdapter()
stats = adapter.merge(
    target_path="main.default.customers",  # Unity Catalog table name
    source_df=new_customers,
    merge_key="customer_id"
)

print(f"\n✅ MERGE complete:")
print(f"  - Updated: {stats['num_updated']} records")
print(f"  - Inserted: {stats['num_inserted']} records")

# ============================================================================
# Example 4: Time Travel
# ============================================================================

print("\n" + "=" * 80)
print("Example 4: Time Travel")
print("=" * 80)

# Read specific version
df_v1 = adapter.read("main.default.customers", version=1)
print(f"Version 1: {len(df_v1)} records")

# Read at specific timestamp
df_yesterday = adapter.read(
    "main.default.customers",
    timestamp="2026-02-08T00:00:00Z"
)
print(f"Yesterday: {len(df_yesterday)} records")

# Get table history
history = adapter.get_history("main.default.customers", limit=5)
print(f"\nTable history (last 5 commits):")
print(history)

# ============================================================================
# Example 5: Optimize & Vacuum
# ============================================================================

print("\n" + "=" * 80)
print("Example 5: Optimize & Vacuum")
print("=" * 80)

# Optimize (compact small files)
print("Optimizing table...")
stats = adapter.optimize("main.default.customers")
print(f"✅ Optimization complete:")
print(f"  - Files added: {stats['num_files_added']}")
print(f"  - Files removed: {stats['num_files_removed']}")

# Vacuum (delete old files) - dry run first
print("\nVacuum dry run (7 days retention)...")
files = adapter.vacuum("main.default.customers", retention_hours=168, dry_run=True)
print(f"Would delete {len(files)} files")

# Uncomment to actually vacuum:
# adapter.vacuum("main.default.customers", retention_hours=168, dry_run=False)

# ============================================================================
# Example 6: Write to Unity Catalog
# ============================================================================

print("\n" + "=" * 80)
print("Example 6: Write to Unity Catalog")
print("=" * 80)

# Create sample data
sample_data = pl.DataFrame({
    "customer_id": [1000, 1001, 1002],
    "email": ["test1@example.com", "test2@example.com", "test3@example.com"],
    "first_name": ["Test", "Test", "Test"],
    "last_name": ["User1", "User2", "User3"],
    "created_at": ["2026-02-09T13:00:00Z", "2026-02-09T14:00:00Z", "2026-02-09T15:00:00Z"],
    "country": ["US", "US", "US"],
    "status": ["active", "active", "active"]
})

# Write to Unity Catalog table
adapter.write(
    df=sample_data,
    path="main.default.customers_test",  # Unity Catalog table name
    mode="append"
)

print(f"✅ Wrote {len(sample_data)} records to main.default.customers_test")

# ============================================================================
# Example 7: Complete Pipeline (Read → Validate → MERGE)
# ============================================================================

print("\n" + "=" * 80)
print("Example 7: Complete Pipeline")
print("=" * 80)

# Step 1: Read from Unity Catalog
print("Step 1: Reading from Unity Catalog...")
processor = DataProcessor(
    engine="polars",
    contract="contracts/unity_catalog_contract.yaml"
)
good_df, bad_df = processor.run_source("main.default.customers")

print(f"  ✅ Good: {len(good_df)}, ❌ Bad: {len(bad_df)}")

# Step 2: MERGE validated data back to Unity Catalog
print("\nStep 2: MERGE validated data to silver layer...")
adapter = DeltaAdapter()
stats = adapter.merge(
    target_path="main.silver.customers",  # Silver layer
    source_df=good_df,
    merge_key="customer_id"
)

print(f"  ✅ Updated: {stats['num_updated']}, Inserted: {stats['num_inserted']}")

# Step 3: Write quarantined data
if len(bad_df) > 0:
    print("\nStep 3: Writing quarantined data...")
    adapter.write(
        df=bad_df,
        path="main.quarantine.customers",  # Quarantine table
        mode="append"
    )
    print(f"  ✅ Wrote {len(bad_df)} quarantined records")

print("\n✅ Pipeline complete!")

# ============================================================================
# Example 8: Using Databricks SDK for Metadata
# ============================================================================

print("\n" + "=" * 80)
print("Example 8: Using Databricks SDK for Metadata")
print("=" * 80)

try:
    from databricks.sdk import WorkspaceClient
    
    # Connect to Databricks
    w = WorkspaceClient()
    
    # List catalogs
    print("Catalogs:")
    for catalog in w.catalogs.list():
        print(f"  - {catalog.name}")
    
    # List schemas in 'main' catalog
    print("\nSchemas in 'main':")
    for schema in w.schemas.list(catalog_name="main"):
        print(f"  - {schema.name}")
    
    # List tables in 'main.default'
    print("\nTables in 'main.default':")
    for table in w.tables.list(catalog_name="main", schema_name="default"):
        print(f"  - {table.name}")
        print(f"    Location: {table.storage_location}")
        print(f"    Format: {table.data_source_format}")

except ImportError:
    print("Databricks SDK not installed. Install with: pip install databricks-sdk")

# ============================================================================
# Summary
# ============================================================================

print("\n" + "=" * 80)
print("Summary")
print("=" * 80)
print("""
✅ Unity Catalog + Delta-RS Features:
  - Read Unity Catalog tables (no Spark!)
  - Write to Unity Catalog tables
  - Atomic MERGE operations (upsert)
  - Time travel (version/timestamp)
  - Optimize & vacuum
  - 10-100x faster than Spark for small/medium data

📚 Learn More:
  - docs/delta_lake_support.md
  - docs/catalog_table_names.md
  - examples/04_features/unity_catalog_contract.yaml
""")
