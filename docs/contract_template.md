# Complete Contract Template Reference

This is a **fully annotated contract template** showing every available configuration option with detailed comments explaining business value and use case scenarios.

Use this as a reference to understand what's possible, then create your own contracts by copying only the sections you need.

---

## Template Structure

```yaml
# ============================================================
# LAKELOGIC DATA CONTRACT - COMPLETE REFERENCE TEMPLATE
# ============================================================
# This template shows ALL available options with detailed comments.
# Copy and customize only the sections you need for your use case.

# ============================================================
# 1. VERSION & METADATA
# ============================================================
# REQUIRED: Contract version for compatibility tracking
version: 1.0.0

# OPTIONAL: Human-readable metadata about this contract
info:
  title: "Customer Master Data - Silver Layer"
  # Business value: Clear identification in logs and monitoring
  
  version: "2.1.0"
  # Business value: Track contract evolution over time
  
  description: "Validated, deduplicated customer records with full quality enforcement"
  # Business value: Documentation for team members and stakeholders
  
  owner: "data-platform-team@company.com"
  # Business value: Clear ownership for questions and incidents
  
  contact:
    email: "data-platform@company.com"
    slack: "#data-quality"
  # Business value: Quick escalation path for issues
  
  target_layer: "silver"
  # Business value: Clarifies position in medallion architecture
  
  status: "production"
  # Options: development, staging, production, deprecated
  # Business value: Lifecycle management
  
  classification: "confidential"
  # Options: public, internal, confidential, restricted
  # Business value: Data governance and compliance
  
  domain: "sales"
  # Data mesh domain (e.g. "sales", "finance", "real-estate")
  # Business value: Domain ownership in data mesh architectures
  
  system: "crm"
  # Source system identifier (e.g. "salesforce", "sap", "zoopla")
  # Business value: Trace data back to source system

# OPTIONAL: Custom metadata for tagging and organization
metadata:
  domain: "sales"
  # Business value: Data mesh domain ownership
  
  system: "crm"
  # Business value: Source system identification
  
  data_layer: "silver"
  # Business value: Medallion layer classification
  
  pii_present: true
  # Business value: Privacy compliance tracking
  
  retention_days: 2555
  # Business value: Data retention policy (7 years)
  
  cost_center: "CC-1234"
  # Business value: Chargeback and cost allocation
  
  sla_tier: "tier1"
  # Business value: SLA classification (tier1 = critical)

# ============================================================
# 2. DATA SOURCE CONFIGURATION
# ============================================================
# OPTIONAL: Where to load data from (for run_source() method)
source:
  type: "landing"
  # Options: landing (files), stream (Kafka), table (database/catalog)
  # Business value: Defines acquisition pattern
  
  path: "s3://bronze-bucket/customers/*.parquet"
  # Supports: Local paths, S3, GCS, ADLS, glob patterns
  # Business value: Flexible source location
  
  load_mode: "incremental"
  # Options: full, incremental, cdc
  # Business value: Optimize processing (only new/changed data)
  
  pattern: "*.parquet"
  # OPTIONAL: File pattern filter
  # Business value: Select specific files from directory
  
  watermark_field: "updated_at"
  # REQUIRED for incremental: Field to track progress
  # Business value: Efficient incremental loading
  
  cdc_op_field: "operation"
  # REQUIRED for cdc: Field indicating operation type
  # Business value: Change data capture support
  
  cdc_delete_values: ["D", "DELETE"]
  # Values indicating delete operations
  # Business value: Handle deletes in CDC streams
  
  watermark_strategy: "max_target"
  # Options: max_target, pipeline_log, manifest, lookback, date_range
  # max_target: MAX(watermark_field) on target Delta table (default)
  # pipeline_log: last successful run from audit log table
  # manifest: JSON manifest file listing processed partitions
  # lookback: sliding window back from NOW (e.g. "7 days")
  # date_range: explicit from_date / to_date (backfills)
  # Business value: Flexible incremental boundary resolution
  
  target_path: "s3://silver-bucket/customers"
  # REQUIRED for max_target strategy: target table path
  
  lookback: "7 days"
  # For lookback strategy: "3 hours", "30 mins", "1 month" etc.
  
  from_date: "2024-01-01"
  to_date: "2024-12-31"
  # For date_range strategy: explicit ISO date boundaries
  
  pipeline_log_table: "meta.pipeline_runs"
  pipeline_name: "bronze_to_silver_customers"
  # For pipeline_log strategy: audit table and pipeline ID
  
  manifest_path: "/dbfs/mnt/meta/manifests/customers.json"
  # For manifest strategy: JSON manifest file path
  
  watermark_date_parts: ["year", "month", "day"]
  # Multi-column partition support when temporal boundary is
  # spread across multiple columns instead of a single date field
  # Also accepts named dict: {year: "partition_year", month: "partition_month"}
  # Business value: Works with Hive-style year/month/day partitions
  
  partition_filters:
    country: "GB"
    region: "south"
  # Static partition values ANDed into every incremental filter
  # Business value: Scope incremental reads to specific partitions
  
  flatten_nested: ["derived", "pricing", "location"]
  # Options: false (default), true (all), [col, col, ...] (named)
  # Flatten JSON-string columns from bronze tables into flat columns
  # Business value: Bronze → Silver workflows with nested JSON data

# ============================================================
# 3. SERVER/STORAGE CONFIGURATION
# ============================================================
# OPTIONAL: Output storage and ingestion controls
server:
  type: "s3"
  # Options: s3, gcs, adls, azure, local, glue
  # Business value: Cloud platform flexibility
  
  format: "delta"
  # Options: parquet, delta, iceberg, csv, json
  # Business value: Choose optimal storage format
  
  path: "s3://silver-bucket/customers"
  # Output location for materialized data
  # Business value: Centralized data lake organization
  
  mode: "validate"
  # Options: validate (quality gate), ingest (raw capture)
  # Business value: Bronze uses "ingest", Silver/Gold use "validate"
  
  schema_evolution: "strict"
  # Options: strict, append, merge, overwrite
  # strict: Fail on schema changes (production safety)
  # append: Allow new fields (flexible ingestion)
  # merge: Merge new fields into schema (smart evolution)
  # overwrite: Replace schema completely (reprocessing)
  # Business value: Control schema change behavior
  
  allow_schema_drift: false
  # If true, log drift but don't fail
  # Business value: Monitoring vs enforcement trade-off
  
  cast_to_string: false
  # If true, cast all columns to string (Bronze "all strings" pattern)
  # Business value: Zero ingestion failures, defer type validation

# ============================================================
# 4. ENVIRONMENT-SPECIFIC OVERRIDES
# ============================================================
# OPTIONAL: Override paths/formats per environment
environments:
  dev:
    path: "s3://dev-bucket/customers"
    format: "parquet"
    # Business value: Cheaper storage for dev/test
  
  staging:
    path: "s3://staging-bucket/customers"
    format: "delta"
    # Business value: Production-like testing
  
  prod:
    path: "s3://prod-bucket/customers"
    format: "delta"
    # Business value: Production configuration

# Usage: export LAKELOGIC_ENV=dev

# ============================================================
# 5. REFERENCE DATA LINKS
# ============================================================
# OPTIONAL: Link to dimension tables or reference data
links:
  - name: "dim_countries"
    path: "./reference/countries.parquet"
    type: "parquet"
    # Options: parquet, csv, table
    broadcast: true
    # If true, Spark will broadcast join (for small tables)
    # Business value: Lookup/join enrichment, referential integrity
  
  - name: "dim_products"
    table: "catalog.reference.products"
    type: "table"
    broadcast: false
    # Business value: Unity Catalog / Hive table reference
  
  - name: "valid_emails"
    path: "s3://reference/email_domains.csv"
    type: "csv"
    # Business value: External validation lists

# ============================================================
# 6. DATASET IDENTIFICATION
# ============================================================
# OPTIONAL: Logical dataset name (used in SQL transformations)
dataset: "customers"
# Business value: SQL table alias in transformations

# OPTIONAL: Business key(s) for the entity
primary_key:
  - "customer_id"
# Business value: Uniqueness validation, merge operations

# ============================================================
# 7. SCHEMA MODEL
# ============================================================
# OPTIONAL: Define expected schema with types and constraints
model:
  fields:
    - name: "customer_id"
      type: "long"
      # Types: string, int, long, double, boolean, date, timestamp
      required: true
      # If true, generates automatic not_null rule
      pii: false
      classification: "public"
      description: "Unique customer identifier"
      # Business value: Schema documentation and enforcement
      
      # OPTIONAL: Generator hints (used by DataGenerator)
      accepted_values: ["premium", "standard", "basic"]
      # Generator picks from this list; validator checks IN rule
      min: 1
      max: 999999
      # Generator stays within range; validator checks >= / <= rules
      
      # OPTIONAL: Foreign key reference to another contract
      foreign_key:
        contract: "silver_agents"
        column: "agent_id"
        severity: "error"
      # Generator samples from PK pool of referenced contract
      # Business value: Referential integrity + synthetic data generation
      
      # OPTIONAL: Field-level quality rules
      rules:
        - name: "customer_id_positive"
          sql: "customer_id > 0"
          category: "correctness"
          severity: "error"
          # Business value: Field-specific validation
    
    - name: "email"
      type: "string"
      required: true
      pii: true
      classification: "confidential"
      description: "Customer email address"
      rules:
        - name: "email_format"
          sql: "email RLIKE '^[^@]+@[^@]+\\.[^@]+$'"
          category: "correctness"
          description: "Valid email format"
    
    - name: "age"
      type: "int"
      required: false
      pii: false
      classification: "internal"
      description: "Customer age in years"
      rules:
        - name: "age_range"
          sql: "age BETWEEN 18 AND 120"
          category: "correctness"
    
    - name: "status"
      type: "string"
      required: true
      pii: false
      description: "Customer account status"
    
    - name: "created_at"
      type: "timestamp"
      required: true
      pii: false
      description: "Account creation timestamp"
    
    - name: "updated_at"
      type: "timestamp"
      required: true
      pii: false
      description: "Last update timestamp"

# ============================================================
# 8. SCHEMA POLICY
# ============================================================
# OPTIONAL: How to handle schema evolution and unknown fields
schema_policy:
  evolution: "strict"
  # Options: strict, compatible, allow
  # strict: Fail on any schema change
  # compatible: Allow backward-compatible changes
  # allow: Accept all changes
  # Business value: Production safety vs flexibility
  
  unknown_fields: "quarantine"
  # Options: quarantine, drop, allow
  # quarantine: Send rows with unknown fields to quarantine
  # drop: Remove unknown fields
  # allow: Keep unknown fields
  # Business value: Handle unexpected columns

# ============================================================
# 9. TRANSFORMATIONS
# ============================================================
# OPTIONAL: Data transformations (pre and post quality checks)
transformations:
  # ──────────────────────────────────────────────────────
  # PRE-PROCESSING (before quality checks)
  # ──────────────────────────────────────────────────────
  
  # Rename columns to standardize naming
  - rename:
      from: "cust_id"
      to: "customer_id"
    phase: "pre"
    # Business value: Align source schema to target schema
  
  # Or rename multiple columns at once
  - rename:
      mappings:
        "cust_id": "customer_id"
        "email_addr": "email"
        "cust_status": "status"
    phase: "pre"
  
  # Drop junk rows early
  - filter:
      sql: "customer_id IS NOT NULL AND email IS NOT NULL"
    phase: "pre"
    # Business value: Remove obvious garbage before validation
  
  # Deduplicate before validation
  - deduplicate:
      on: ["customer_id"]
      sort_by: ["updated_at"]
      order: "desc"
    phase: "pre"
    # Business value: Keep most recent record per customer
  
  # Select specific columns
  - select:
      columns: ["customer_id", "email", "age", "status"]
    phase: "pre"
    # Business value: Drop unnecessary columns
  
  # Drop specific columns
  - drop:
      columns: ["internal_notes", "temp_field"]
    phase: "pre"
    # Business value: Remove sensitive or temporary fields
  
  # Cast data types
  - cast:
      columns:
        customer_id: "long"
        age: "int"
        created_at: "timestamp"
    phase: "pre"
    # Business value: Type coercion before validation
  
  # Trim whitespace
  - trim:
      fields: ["email", "status"]
      side: "both"
    # Options: both, left, right
    phase: "pre"
    # Business value: Clean string data
  
  # Convert to lowercase
  - lower:
      fields: ["email", "status"]
    phase: "pre"
    # Business value: Normalize string values
  
  # Convert to uppercase
  - upper:
      fields: ["country_code"]
    phase: "pre"
    # Business value: Standardize codes
  
  # Coalesce multiple fields
  - coalesce:
      field: "email"
      sources: ["primary_email", "secondary_email", "backup_email"]
      default: "unknown@example.com"
      output: "email"
    phase: "pre"
    # Business value: Fill nulls from multiple sources
  
  # Split string into array
  - split:
      field: "tags"
      delimiter: ","
      output: "tag_array"
    phase: "pre"
    # Business value: Parse delimited strings
  
  # Explode array into rows
  - explode:
      field: "tag_array"
      output: "tag"
    phase: "pre"
    # Business value: Normalize nested data
  
  # Map values
  - map_values:
      field: "status"
      mapping:
        "A": "ACTIVE"
        "I": "INACTIVE"
        "P": "PENDING"
      default: "UNKNOWN"
      output: "status"
    phase: "pre"
    # Business value: Standardize code values
  
  # ──────────────────────────────────────────────────────
  # POST-PROCESSING (after quality checks, on good data)
  # ──────────────────────────────────────────────────────
  
  # Derive new fields
  - derive:
      field: "age_group"
      sql: "CASE WHEN age < 25 THEN 'young' WHEN age < 65 THEN 'adult' ELSE 'senior' END"
    phase: "post"
    # Business value: Calculated fields for analytics
  
  # Lookup/join dimension data
  - lookup:
      field: "country_name"
      reference: "dim_countries"
      on: "country_code"
      key: "code"
      value: "name"
      default_value: "Unknown"
    phase: "post"
    # Business value: Enrich with reference data
  
  # Full join with multiple fields
  - join:
      reference: "dim_products"
      on: "product_id"
      key: "id"
      fields: ["product_name", "category", "price"]
      type: "left"
      # Options: left, inner, right, full
      prefix: "product_"
      defaults:
        product_name: "Unknown Product"
        category: "Uncategorized"
    phase: "post"
    # Business value: Multi-field enrichment
  
  # SQL transformation
  - sql: |
      SELECT
        *,
        DATEDIFF(CURRENT_DATE, created_at) AS days_since_signup,
        CASE
          WHEN status = 'ACTIVE' AND age > 65 THEN 'senior_active'
          WHEN status = 'ACTIVE' THEN 'active'
          ELSE 'inactive'
        END AS segment
      FROM source
    phase: "post"
    # Business value: Complex transformations
  
  # Rollup/aggregation with lineage tracking
  - rollup:
      group_by: ["customer_segment", "country"]
      aggregations:
        total_customers: "COUNT(*)"
        avg_age: "AVG(age)"
        total_revenue: "SUM(lifetime_value)"
      keys: "customer_id"
      # Track which customer IDs rolled into each group
      rollup_keys_column: "_lakelogic_rollup_keys"
      rollup_keys_count_column: "_lakelogic_rollup_keys_count"
      upstream_run_id_column: "_upstream_run_id"
      upstream_run_ids_column: "_upstream_lakelogic_run_ids"
      distinct: true
    phase: "post"
    # Business value: Aggregation with full lineage

  # Pivot long metrics into wide columns
  - pivot:
      id_vars: ["customer_id"]
      pivot_col: "metric"
      value_cols: ["value"]
      values: ["clicks", "impressions"]
      # values list required for portable SQL pivot
      agg: "sum"
      name_template: "{pivot_alias}"
    phase: "post"
    # Business value: Wide analytics-ready metrics

  # Unpivot wide columns back to long form
  - unpivot:
      id_vars: ["customer_id"]
      value_vars: ["clicks", "impressions"]
      key_field: "metric"
      value_field: "value"
      include_nulls: false
    phase: "post"
    # Business value: Normalize wide metrics to long rows

  # Bucket numeric values into labelled bands
  - bucket:
      field: "price_band"
      source: "listing_price"
      bins:
        - lt: 250000
          label: "sub_250k"
        - lt: 500000
          label: "250k_500k"
        - lt: 1000000
          label: "500k_1m"
      default: "1m_plus"
    phase: "post"
    # Compiles to SQL CASE expression — identical across all engines
    # Business value: Categorize values into named segments

  # Extract scalar values from JSON string columns
  - json_extract:
      field: "latitude"
      source: "location_coordinates"
      path: "$.latitude"
      cast: "float"
    phase: "post"
    # Engine-agnostic: Polars/DuckDB/Spark each use native JSON ops
    # Business value: Parse nested JSON without full flattening

  # Compute date differences
  - date_diff:
      field: "listing_age_days"
      from_col: "creation_date"
      to_col: "event_date"
      unit: "days"
    # Options: days, hours, months
    phase: "post"
    # Business value: Calculate durations between events

  # Explode date ranges into one row per day
  - date_range_explode:
      output: "snapshot_date"
      start_col: "creation_date"
      end_col: "deleted_at"
      # end_col is nullable — defaults to today when null
      interval: "1d"
    phase: "post"
    # Business value: Create daily snapshots from validity ranges

# ============================================================
# 10. QUALITY RULES
# ============================================================
# OPTIONAL: Data quality validation rules
quality:
  enforce_required: true
  # If true, generate not_null rules for required fields
  # Business value: Automatic completeness checks
  
  # ──────────────────────────────────────────────────────
  # ROW-LEVEL RULES (quarantine individual bad rows)
  # ──────────────────────────────────────────────────────
  row_rules:
    # Simple not-null check
    - not_null: "email"
      # Business value: Ensure critical fields are populated
    
    # Not-null with custom config
    - not_null:
        field: "customer_id"
        name: "customer_id_required"
        category: "completeness"
        description: "Customer ID must be present"
        severity: "error"
      # Severity: error (quarantine), warning (log only), info
    
    # Multiple not-null checks
    - not_null:
        fields: ["email", "status", "created_at"]
        category: "completeness"
    
    # Accepted values (enum validation)
    - accepted_values:
        field: "status"
        values: ["ACTIVE", "INACTIVE", "PENDING", "SUSPENDED"]
        category: "consistency"
        description: "Status must be valid enum value"
      # Business value: Enforce controlled vocabularies
    
    # Regex pattern matching
    - regex_match:
        field: "email"
        pattern: "^[^@]+@[^@]+\\.[^@]+$"
        category: "correctness"
        description: "Email must be valid format"
      # Business value: Format validation
    
    # Numeric range validation
    - range:
        field: "age"
        min: 18
        max: 120
        inclusive: true
        category: "correctness"
        description: "Age must be between 18 and 120"
      # Business value: Plausibility checks
    
    # Referential integrity (foreign key)
    - referential_integrity:
        field: "country_code"
        reference: "dim_countries"
        key: "code"
        category: "consistency"
        description: "Country code must exist in reference table"
      # Business value: Prevent orphaned records
    
    # Lifecycle window validation (SCD Type 2)
    - lifecycle_window:
        event_ts: "order_date"
        event_key: "customer_id"
        reference: "dim_customers"
        reference_key: "customer_id"
        start_field: "valid_from"
        end_field: "valid_to"
        end_default: "9999-12-31"
        category: "consistency"
        description: "Order must fall within customer validity window"
      # Business value: Temporal referential integrity
    
    # Custom SQL rule
    - name: "email_domain_valid"
      sql: "email NOT LIKE '%@temp-mail.%' AND email NOT LIKE '%@disposable.%'"
      category: "validity"
      description: "Block disposable email domains"
      severity: "error"
      # Business value: Business-specific validation
  
  # ──────────────────────────────────────────────────────
  # DATASET-LEVEL RULES (aggregate checks on good data)
  # ──────────────────────────────────────────────────────
  dataset_rules:
    # Uniqueness check
    - unique: "customer_id"
      # Business value: Prevent duplicates
    
    # Uniqueness with custom config
    - unique:
        field: "email"
        name: "email_unique"
        category: "uniqueness"
        description: "Email addresses must be unique"
        severity: "error"
    
    # Null ratio threshold
    - null_ratio:
        field: "phone"
        max: 0.20
        # Max 20% null values allowed
        category: "completeness"
        description: "Phone number should be present for most customers"
      # Business value: Data completeness monitoring
    
    # Row count validation
    - row_count_between:
        min: 1000
        max: 10000000
        category: "completeness"
        description: "Expected customer count range"
      # Business value: Detect missing or duplicate data
    
    # Custom SQL dataset rule
    - name: "active_customer_ratio"
      sql: "SELECT SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) / COUNT(*) FROM source"
      category: "validity"
      description: "At least 60% of customers should be active"
      must_be_greater_than: 0.60
      # Business value: Business metric validation

# ============================================================
# 11. QUARANTINE CONFIGURATION
# ============================================================
# OPTIONAL: Quarantine settings and notifications
quarantine:
  enabled: true
  # If false, pipeline fails on any quality rule failure
  # Business value: Fail-fast vs graceful degradation
  
  target: "s3://quarantine-bucket/customers"
  # Where to write quarantined records
  # Business value: Centralized bad data repository
  
  include_error_reason: true
  # If true, include _lakelogic_errors column
  # Business value: Root cause analysis
  
  strict_notifications: true
  # If true, fail pipeline if notification fails
  # Business value: Ensure alerts are delivered
  
  format: "parquet"
  # Options: parquet (default), csv, delta, json
  # Output format for file-based quarantine targets
  # Business value: Match quarantine format to your tooling
  
  write_mode: "append"
  # Options: append (default), overwrite
  # append: add bad records to existing quarantine data
  # overwrite: replace quarantine target on every run
  # Business value: Control quarantine growth vs freshness
  
  # ──────────────────────────────────────────────────────
  # NOTIFICATION CHANNELS
  # ──────────────────────────────────────────────────────
  notifications:
    # Default notification via Apprise (auto-detects channel from URL)
    - target: "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
      on_events: ["quarantine", "failure", "schema_drift"]
      subject_template: "[{{ event | upper }}] {{ contract.title }}"
      message_template: "Run={{ run_id }}\nMessage={{ message }}"
      # type defaults to "apprise" — auto-detects Slack, Teams, email etc.
      # Events: quarantine, failure, schema_drift, dataset_rule_failed
      # Business value: Real-time alerting
    
    # Microsoft Teams notification
    - type: "teams"
      channel: "https://outlook.office.com/webhook/YOUR/WEBHOOK/URL"
      on_events: ["quarantine", "failure"]
    
    # Email notification
    - type: "email"
      to: "data-platform@company.com"
      subject_template_file: "templates/alerts/failure_subject.j2"
      message_template_file: "templates/alerts/failure_body.j2"
      on_events: ["failure", "dataset_rule_failed"]
      # Requires SMTP configuration
    
    # Generic webhook
    - type: "webhook"
      url: "https://api.company.com/data-quality/alerts"
      on_events: ["quarantine", "failure", "schema_drift"]
      # Business value: Integration with custom systems
    
    # Multi-channel fan-out (send to multiple URLs at once)
    - targets:
        - "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
        - "https://outlook.office.com/webhook/YOUR/WEBHOOK/URL"
      on_events: ["failure"]
      # Business value: Broadcast critical alerts to multiple channels

# ============================================================
# 12. MATERIALIZATION
# ============================================================
# OPTIONAL: How to write output data
materialization:
  strategy: "merge"
  # Options: append, merge, scd2, overwrite
  # append: Add new rows (fact tables)
  # merge: Upsert based on primary key (SCD Type 1)
  # scd2: Slowly Changing Dimension Type 2 (history tracking)
  # overwrite: Replace all data (daily snapshots)
  # Business value: Choose appropriate write pattern
  
  partition_by:
    - "country"
    - "created_date"
  # Partition columns for performance
  # Business value: Query optimization
  
  cluster_by:
    - "customer_id"
  # Clustering columns (Delta/Iceberg)
  # Business value: Further query optimization
  
  reprocess_policy: "overwrite_partition"
  # Options: overwrite_partition, append, fail
  # How to handle re-running same partition
  # Business value: Idempotent pipeline execution
  
  target_path: "s3://silver-bucket/customers"
  # Override default output path
  # Business value: Explicit output control
  
  format: "delta"
  # Override default format
  # Options: parquet, delta, iceberg, csv
  # Business value: Format selection
  
  # SCD Type 2 specific configuration
  scd2:
    primary_key: "customer_id"
    # Business key for the entity
    
    timestamp_field: "updated_at"
    # Field to determine record version
    
    start_date_field: "valid_from"
    # Column to store validity start date
    
    end_date_field: "valid_to"
    # Column to store validity end date
    
    current_flag_field: "is_current"
    # Boolean flag for current record
    
    end_date_default: "9999-12-31"
    # Default value for open-ended records
    
    hash_fields: ["email", "status", "age"]
    # Fields to hash for change detection
    # Business value: Full history tracking
  
  # OPTIONAL: Soft-delete support (mark deleted instead of removing)
  soft_delete_column: "_lakelogic_is_deleted"
  # Column name for soft-delete boolean flag
  soft_delete_value: true
  # Value to set when record is deleted
  soft_delete_time_column: "_lakelogic_deleted_at"
  # Timestamp for when soft-delete occurred
  soft_delete_reason_column: "_lakelogic_delete_reason"
  # Reason for deletion (e.g. "GDPR request", "duplicate")
  # Business value: GDPR compliance — keep audit trail without hard deletes
  
  # OPTIONAL: External storage location for Unity Catalog tables
  location: "abfss://container@account.dfs.core.windows.net/silver/customers/"
  # Business value: Control physical storage for UC managed tables
  
  # OPTIONAL: Delta/Iceberg table properties
  table_properties:
    "delta.autoOptimize.optimizeWrite": "true"
    "delta.autoOptimize.autoCompact": "true"
  # Business value: Optimize table performance without manual maintenance
  
  # OPTIONAL: Auto-compaction and vacuum settings
  compaction:
    auto: true
    vacuum_retention_hours: 168
  # Business value: Automated storage optimization

# ============================================================
# 13. LINEAGE & OBSERVABILITY
# ============================================================
# OPTIONAL: Lineage capture configuration
lineage:
  enabled: true
  # If true, inject lineage columns
  # Business value: Data provenance tracking
  
  capture_source_path: true
  source_column_name: "_lakelogic_source"
  # Capture source file/table path
  
  capture_timestamp: true
  timestamp_column_name: "_lakelogic_processed_at"
  # Capture processing timestamp
  
  capture_run_id: true
  run_id_column_name: "_lakelogic_run_id"
  # Capture unique run identifier
  
  capture_domain: true
  domain_column_name: "_lakelogic_domain"
  # Capture domain from metadata
  
  capture_system: true
  system_column_name: "_lakelogic_system"
  # Capture source system from metadata
  
  preserve_upstream: ["_upstream_run_id", "_upstream_source"]
  # Preserve lineage columns from upstream datasets
  # Business value: Multi-hop lineage tracking
  
  upstream_prefix: "_upstream"
  # Prefix for preserved upstream columns
  
  run_id_source: "run_id"
  # Options: run_id, pipeline_run_id
  # Use pipeline_run_id for cross-contract correlation
  
  capture_contract_name: true
  contract_name_column_name: "_lakelogic_contract_name"
  # Inject contract title into every output row
  # Business value: Identify which contract produced each record

# ============================================================
# 14. SERVICE LEVEL OBJECTIVES
# ============================================================
# OPTIONAL: SLO definitions for monitoring
service_levels:
  freshness:
    threshold: "24h"
    # Data must be refreshed within 24 hours
    field: "updated_at"
    # Field to check for freshness
    description: "Customer data must be updated daily"
    # Business value: Timeliness monitoring
  
  availability: 99.9
    # Percentage uptime target
    # Business value: Reliability tracking

# ============================================================
# 15. EXTERNAL LOGIC
# ============================================================
# OPTIONAL: Custom Python/Notebook processing
external_logic:
  type: "python"
  # Options: python, notebook
  
  path: "./gold/build_customer_gold.py"
  # Path to Python file or Jupyter notebook
  
  entrypoint: "build_gold"
  # Function name to call (Python only)
  
  args:
    apply_ml_scoring: true
    model_path: "s3://models/churn_predictor.pkl"
    target_table: "gold_customers"
  # Custom arguments passed to function
  # Business value: ML scoring, complex business logic
  
  output_path: "s3://gold-bucket/customers"
  # Override output path
  
  output_format: "delta"
  # Override output format
  
  handles_output: false
  # If true, external logic writes output itself
  # If false, LakeLogic materializes the returned DataFrame
  
  kernel_name: "python3"
  # Jupyter kernel for notebook execution

# ============================================================
# 16. ORCHESTRATION & DEPENDENCIES
# ============================================================
# OPTIONAL: Pipeline orchestration metadata
upstream:
  - "bronze_crm_contacts"
  - "bronze_web_signups"
# List of upstream datasets this depends on
# Business value: DAG construction in orchestrators

schedule: "0 2 * * *"
# Cron expression for scheduling
# Business value: Automated execution timing

# ============================================================
# 17. TIER / LAYER
# ============================================================
# OPTIONAL: Explicit medallion tier for single-contract mode
tier: "silver"
# Options: bronze, silver, gold, reference
# Also accepts synonyms: raw, landing, ingest → bronze
#                        stage, staging, cleansed, transform → silver
#                        curated, presentation, consumption → gold
#                        ref, seed, lookup, masterdata → reference
# Business value: Automatic tier-aware defaults and classification

# ============================================================
# 18. DOWNSTREAM CONSUMERS
# ============================================================
# OPTIONAL: Declare what uses this contract's output
downstream:
  - type: "dashboard"
    name: "Monthly Revenue Dashboard"
    platform: "power_bi"
    # Options: power_bi, tableau, looker, databricks_sql, metabase, grafana
    url: "https://app.powerbi.com/groups/.../dashboards/..."
    owner: "analytics-team"
    description: "Executive revenue dashboard"
    refresh: "daily 06:00 UTC"
    columns_used: ["customer_segment", "total_revenue", "country"]
    sla: "< 4 hours"
    # Business value: Know who consumes your data
  
  - type: "ml_model"
    name: "Churn Prediction"
    platform: "mlflow"
    owner: "data-science"
    # Business value: End-to-end lineage from source → gold → ML
  
  - type: "api"
    name: "Customer Lookup API"
    platform: "internal"
    url: "https://api.internal.com/v1/customers"
    # Types: dashboard, report, api, ml_model, application, notebook, export

# ============================================================
# 19. LLM EXTRACTION (Unstructured → Structured)
# ============================================================
# OPTIONAL: Extract structured data from unstructured text via LLM
extraction:
  provider: "openai"
  # ─── Cloud Providers (require API key via env var) ───
  #   openai       → OPENAI_API_KEY
  #   azure_openai → AZURE_OPENAI_API_KEY + AZURE_OPENAI_ENDPOINT
  #   anthropic    → ANTHROPIC_API_KEY
  #   google       → GOOGLE_API_KEY
  #   bedrock      → AWS credentials (boto3)
  #
  # ─── Local Providers (no API key required) ──────────
  #   ollama       → Local Ollama server (default: http://localhost:11434)
  #                  Override with OLLAMA_BASE_URL env var
  #                  Install Ollama: https://ollama.com
  #   local        → Direct HuggingFace Transformers (Phi-3-mini default)
  #                  No server needed, downloads to ~/.cache/huggingface/
  #                  Install with: pip install lakelogic[local]
  #
  # Global env var overrides:
  #   LAKELOGIC_AI_PROVIDER → default provider for all contracts
  #   LAKELOGIC_AI_MODEL    → default model for all contracts
  
  model: "gpt-4o-mini"
  # Cloud models: gpt-4o, gpt-4o-mini, claude-sonnet-4-20250514, gemini-2.0-flash
  # Ollama models: llama3.1, mistral, phi3, codellama (any pulled model)
  # Local models: auto (uses per-field extraction_task routing), or any HuggingFace model ID
  temperature: 0.1
  # Low temperature for deterministic extraction
  max_tokens: 1000
  response_format: "json"
  
  prompt_template: |
    Extract the following from this support ticket:
    {{ ticket_body }}
  system_prompt: "You are a data extraction assistant."
  
  text_column: "ticket_body"
  # Column containing text to extract from
  context_columns: ["customer_id", "ticket_date"]
  # Extra columns available in prompt template
  
  output_schema:
    - name: "sentiment"
      type: "string"
      accepted_values: ["positive", "neutral", "negative"]
      extraction_task: "classification"
    - name: "issue_category"
      type: "string"
      extraction_task: "classification"
      extraction_examples: ["billing", "technical", "account"]
  
  # Processing controls
  batch_size: 50
  concurrency: 5
  retry:
    max_attempts: 3
    backoff: "exponential"
    initial_delay: 1.0
  
  # Confidence scoring
  confidence:
    enabled: true
    method: "field_completeness"
    # Options: log_probs, self_assessment, consistency, field_completeness
    column: "_lakelogic_extraction_confidence"
  
  # Cost controls
  max_cost_per_run: 50.00
  max_rows_per_run: 10000
  # Business value: Budget safety for LLM API costs
  
  # Fallback model (cheaper/faster if primary fails)
  fallback_model: "gpt-4o-mini"
  fallback_provider: "openai"
  
  # PII safety
  redact_pii_before_llm: true
  pii_fields: ["email", "phone"]
  # Business value: Never send PII to external LLM providers
  
  # OPTIONAL: Preprocessing pipeline for raw files (PDF, image, audio, video)
  preprocessing:
    content_type: "pdf"
    # Options: pdf, image, video, audio, html, email, text
    ocr:
      enabled: true
      engine: "tesseract"
      # Options: tesseract, azure_di, textract, google_vision
      language: "eng"
    chunking:
      strategy: "page"
      # Options: page, paragraph, sentence, fixed_size
      max_chunk_tokens: 4000
      overlap_tokens: 200
    # Business value: Process PDFs, images, audio, video into structured data

```

---

## Common Use Case Templates

### Use Case 1: Bronze Ingestion (Capture Everything)

```yaml
version: 1.0.0
info:
  title: "Bronze CRM Contacts"
  target_layer: "bronze"

source:
  type: "landing"
  path: "s3://landing/crm/*.csv"
  load_mode: "incremental"
  watermark_field: "file_modified_time"

server:
  type: "s3"
  path: "s3://bronze/crm_contacts"
  format: "parquet"
  mode: "ingest"
  cast_to_string: true
  schema_evolution: "append"
  allow_schema_drift: true

quality:
  row_rules:
    - name: "has_id"
      sql: "id IS NOT NULL"

materialization:
  strategy: "append"
  partition_by: ["ingestion_date"]

lineage:
  enabled: true
```

### Use Case 2: Silver Validation (Quality Gate)

```yaml
version: 1.0.0
info:
  title: "Silver Customers"
  target_layer: "silver"

dataset: "customers"
primary_key: ["customer_id"]

model:
  fields:
    - name: "customer_id"
      type: "long"
      required: true
    - name: "email"
      type: "string"
      required: true
      pii: true
    - name: "status"
      type: "string"
      required: true

transformations:
  - deduplicate:
      on: ["customer_id"]
      sort_by: ["updated_at"]
      order: "desc"
    phase: "pre"

quality:
  enforce_required: true
  row_rules:
    - regex_match:
        field: "email"
        pattern: "^[^@]+@[^@]+\\.[^@]+$"
    - accepted_values:
        field: "status"
        values: ["ACTIVE", "INACTIVE"]
  dataset_rules:
    - unique: "customer_id"

quarantine:
  enabled: true
  target: "s3://quarantine/customers"
  notifications:
    - type: "slack"
      target: "https://hooks.slack.com/..."
      on_events: ["quarantine"]

materialization:
  strategy: "merge"
  partition_by: ["country"]
```

### Use Case 3: Gold Aggregation (Analytics)

```yaml
version: 1.0.0
info:
  title: "Gold Customer Metrics"
  target_layer: "gold"

dataset: "silver_customers"

transformations:
  - sql: |
      SELECT
        customer_segment,
        country,
        DATE_TRUNC('month', created_at) AS month,
        COUNT(*) AS customer_count,
        AVG(lifetime_value) AS avg_ltv,
        SUM(total_orders) AS total_orders
      FROM source
      WHERE status = 'ACTIVE'
      GROUP BY customer_segment, country, month
    phase: "post"

materialization:
  strategy: "overwrite"
  partition_by: ["month"]

lineage:
  enabled: true
```

### Use Case 4: SCD Type 2 (History Tracking)

```yaml
version: 1.0.0
info:
  title: "Silver Customer History (SCD2)"
  target_layer: "silver"

dataset: "customers"
primary_key: ["customer_id"]

materialization:
  strategy: "scd2"
  scd2:
    primary_key: "customer_id"
    timestamp_field: "updated_at"
    start_date_field: "valid_from"
    end_date_field: "valid_to"
    current_flag_field: "is_current"
    end_date_default: "9999-12-31"
    hash_fields: ["email", "status", "address"]
```

---

## Quick Reference: When to Use What

| Feature | Bronze | Silver | Gold |
| ------- | ------ | ------ | ---- |
| `tier` | `bronze` | `silver` | `gold` |
| `server.mode` | `ingest` | `validate` | `validate` |
| `server.cast_to_string` | `true` | `false` | `false` |
| `server.schema_evolution` | `append` | `strict` | `strict` |
| `source.flatten_nested` | N/A | `true` / `[cols]` | `true` / `[cols]` |
| `quality.enforce_required` | `false` | `true` | `true` |
| `quality.row_rules` | Minimal | Full | Minimal |
| `quality.dataset_rules` | None | Yes | Yes |
| `materialization.strategy` | `append` | `merge`/`scd2` | `overwrite` |
| `lineage.enabled` | `true` | `true` | `true` |
| `downstream` | N/A | Optional | Recommended |
| `extraction` | Optional | N/A | N/A |

---

*For more examples, see the [LakeLogic Examples](https://github.com/lakelogic/LakeLogic/tree/main/examples) directory.*
