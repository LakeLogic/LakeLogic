"""
REST API Integration Example - OAuth2 with Pagination

This example demonstrates:
1. REST API connection with OAuth2 authentication
2. Automatic pagination handling
3. Rate limiting and retry logic
4. Data validation with LakeLogic contracts
5. Writing to Delta Lake

Prerequisites:
- pip install "lakelogic[api,delta]"
- Set environment variables:
  - OAUTH_CLIENT_ID
  - OAUTH_CLIENT_SECRET
"""

import os
from lakelogic.engines.integration_connectors import RESTAPIConnector
from lakelogic.engines.delta_adapter import DeltaAdapter
from lakelogic import DataProcessor

# ============================================================================
# Example 1: Basic REST API Connection (OAuth2)
# ============================================================================

print("=" * 80)
print("Example 1: REST API with OAuth2 Authentication")
print("=" * 80)

# Create connector with OAuth2
connector = RESTAPIConnector(
    base_url="https://api.example.com",
    auth_type="oauth2",
    client_id=os.getenv("OAUTH_CLIENT_ID"),
    client_secret=os.getenv("OAUTH_CLIENT_SECRET"),
    token_url="https://api.example.com/oauth/token"
)

# Extract data (no pagination)
df = connector.extract(
    endpoint="/v1/customers",
    params={"status": "active"}
)

print(f"✅ Extracted {len(df)} customers")
print(df.head())

connector.close()


# ============================================================================
# Example 2: API with Automatic Pagination
# ============================================================================

print("\n" + "=" * 80)
print("Example 2: Automatic Pagination")
print("=" * 80)

connector = RESTAPIConnector(
    base_url="https://api.example.com",
    auth_type="oauth2",
    client_id=os.getenv("OAUTH_CLIENT_ID"),
    client_secret=os.getenv("OAUTH_CLIENT_SECRET"),
    token_url="https://api.example.com/oauth/token"
)

# Extract with offset-based pagination
df = connector.extract(
    endpoint="/v1/customers",
    params={"status": "active"},
    pagination_type="offset",  # offset, page, cursor
    page_size=100
)

print(f"✅ Extracted {len(df)} customers across multiple pages")
print(f"📊 Columns: {df.columns}")

connector.close()


# ============================================================================
# Example 3: API with Rate Limiting
# ============================================================================

print("\n" + "=" * 80)
print("Example 3: Rate Limiting & Retry Logic")
print("=" * 80)

connector = RESTAPIConnector(
    base_url="https://api.example.com",
    auth_type="api_key",
    api_key=os.getenv("API_KEY"),
    api_key_header="X-API-Key",
    rate_limit=10,  # Max 10 requests per second
    retry_attempts=3,
    retry_delay=1
)

# Extract with rate limiting
df = connector.extract(
    endpoint="/v1/orders",
    pagination_type="page",
    page_size=50,
    max_pages=10  # Limit to 10 pages
)

print(f"✅ Extracted {len(df)} orders (rate limited to 10 req/s)")

connector.close()


# ============================================================================
# Example 4: API → Contract Validation → Delta Lake
# ============================================================================

print("\n" + "=" * 80)
print("Example 4: API → Validation → Delta Lake Pipeline")
print("=" * 80)

# Step 1: Extract from API
print("\n📥 Step 1: Extract from REST API")
connector = RESTAPIConnector(
    base_url="https://api.example.com",
    auth_type="oauth2",
    client_id=os.getenv("OAUTH_CLIENT_ID"),
    client_secret=os.getenv("OAUTH_CLIENT_SECRET"),
    token_url="https://api.example.com/oauth/token",
    rate_limit=10
)

df_api = connector.extract(
    endpoint="/v1/customers",
    params={"status": "active", "include_metadata": "true"},
    pagination_type="offset",
    page_size=100
)

print(f"✅ Extracted {len(df_api)} customers from API")
connector.close()

# Step 2: Validate with LakeLogic contract
print("\n✅ Step 2: Validate with LakeLogic contract")
processor = DataProcessor(
    engine="polars",
    contract="examples/04_features/rest_api_contract.yaml"
)

good_df, bad_df = processor.run(df_api)

print(f"✅ Validation complete:")
print(f"  - Good records: {len(good_df)}")
print(f"  - Quarantined records: {len(bad_df)}")

if len(bad_df) > 0:
    print("\n⚠️ Quarantined records:")
    print(bad_df.select(["id", "email", "_lakelogic_quarantine_reason"]).head())

# Step 3: Write to Delta Lake (Bronze layer)
print("\n💾 Step 3: Write to Bronze layer (Delta Lake)")
delta_adapter = DeltaAdapter()

delta_adapter.write(
    df=good_df,
    path="s3://datalake/bronze/api_customers/",
    mode="append"
)

print(f"✅ Written {len(good_df)} records to Bronze layer")

# Step 4: MERGE to Silver layer
print("\n🔄 Step 4: MERGE to Silver layer")
merge_stats = delta_adapter.merge(
    target_path="s3://datalake/silver/api_customers/",
    source_df=good_df,
    merge_key="id"
)

print(f"✅ MERGE complete:")
print(f"  - Updated: {merge_stats['num_updated']} records")
print(f"  - Inserted: {merge_stats['num_inserted']} records")


# ============================================================================
# Example 5: Multiple API Endpoints
# ============================================================================

print("\n" + "=" * 80)
print("Example 5: Multiple API Endpoints")
print("=" * 80)

connector = RESTAPIConnector(
    base_url="https://api.example.com",
    auth_type="oauth2",
    client_id=os.getenv("OAUTH_CLIENT_ID"),
    client_secret=os.getenv("OAUTH_CLIENT_SECRET"),
    token_url="https://api.example.com/oauth/token"
)

# Define endpoints to extract
endpoints = [
    {
        "endpoint": "/v1/customers",
        "params": {"status": "active"},
        "output_path": "s3://datalake/bronze/api_customers/"
    },
    {
        "endpoint": "/v1/orders",
        "params": {"status": "completed"},
        "output_path": "s3://datalake/bronze/api_orders/"
    },
    {
        "endpoint": "/v1/products",
        "params": {"category": "electronics"},
        "output_path": "s3://datalake/bronze/api_products/"
    }
]

delta_adapter = DeltaAdapter()

for config in endpoints:
    print(f"\n🔄 Processing {config['endpoint']}...")
    
    # Extract
    df = connector.extract(
        endpoint=config["endpoint"],
        params=config["params"],
        pagination_type="offset",
        page_size=100
    )
    
    print(f"  ✅ Extracted {len(df)} records")
    
    # Write to Delta Lake
    delta_adapter.write(
        df=df,
        path=config["output_path"],
        mode="append"
    )
    
    print(f"  ✅ Written to {config['output_path']}")

connector.close()

print("\n✅ Multi-endpoint extraction complete!")


# ============================================================================
# Example 6: POST Request with JSON Body
# ============================================================================

print("\n" + "=" * 80)
print("Example 6: POST Request with JSON Body")
print("=" * 80)

connector = RESTAPIConnector(
    base_url="https://api.example.com",
    auth_type="bearer",
    bearer_token=os.getenv("BEARER_TOKEN")
)

# POST request with search query
df = connector.extract(
    endpoint="/v1/search",
    method="POST",
    json_body={
        "query": "active customers",
        "filters": {
            "country": "US",
            "subscription_tier": ["premium", "enterprise"]
        },
        "sort": "created_at",
        "order": "desc"
    },
    pagination_type="cursor",
    page_size=50
)

print(f"✅ Extracted {len(df)} search results")
print(df.head())

connector.close()


# ============================================================================
# Example 7: Scheduled API Sync Pipeline
# ============================================================================

print("\n" + "=" * 80)
print("Example 7: Scheduled API Sync (Production Pattern)")
print("=" * 80)

def run_api_sync_pipeline(
    base_url: str,
    endpoint: str,
    auth_config: dict,
    params: dict,
    contract_path: str,
    bronze_path: str,
    silver_path: str,
    merge_key: str
):
    """
    Production-ready API sync pipeline.
    
    This function can be scheduled to run every hour/day to:
    1. Extract data from REST API
    2. Validate with LakeLogic contract
    3. Write to Bronze layer
    4. MERGE to Silver layer
    """
    
    print(f"\n🚀 Starting API sync for {endpoint}")
    
    # Step 1: Extract from API
    print("\n📥 Extracting from API...")
    connector = RESTAPIConnector(
        base_url=base_url,
        **auth_config,
        rate_limit=10,
        retry_attempts=3
    )
    
    df_api = connector.extract(
        endpoint=endpoint,
        params=params,
        pagination_type="offset",
        page_size=100
    )
    
    print(f"✅ Extracted {len(df_api)} records")
    connector.close()
    
    if len(df_api) == 0:
        print("ℹ️ No records to process")
        return
    
    # Step 2: Validate
    print("\n✅ Validating data...")
    processor = DataProcessor(engine="polars", contract=contract_path)
    good_df, bad_df = processor.run(df_api)
    
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
        merge_key=merge_key
    )
    print(f"✅ Updated: {merge_stats['num_updated']}, Inserted: {merge_stats['num_inserted']}")
    
    print(f"\n✅ API sync complete!")
    
    return {
        "records_extracted": len(df_api),
        "records_validated": len(good_df),
        "records_quarantined": len(bad_df),
        "records_updated": merge_stats['num_updated'],
        "records_inserted": merge_stats['num_inserted']
    }


# Run the pipeline
stats = run_api_sync_pipeline(
    base_url="https://api.example.com",
    endpoint="/v1/customers",
    auth_config={
        "auth_type": "oauth2",
        "client_id": os.getenv("OAUTH_CLIENT_ID"),
        "client_secret": os.getenv("OAUTH_CLIENT_SECRET"),
        "token_url": "https://api.example.com/oauth/token"
    },
    params={"status": "active"},
    contract_path="examples/04_features/rest_api_contract.yaml",
    bronze_path="s3://datalake/bronze/api_customers/",
    silver_path="s3://datalake/silver/api_customers/",
    merge_key="id"
)

print("\n📊 Pipeline Statistics:")
for key, value in stats.items():
    print(f"  - {key}: {value}")
