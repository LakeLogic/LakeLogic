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
  
  # ──────────────────────────────────────────────────────
  # NOTIFICATION CHANNELS
  # ──────────────────────────────────────────────────────
  notifications:
    # Slack notification
    - type: "slack"
      target: "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
      on_events: ["quarantine", "failure", "schema_drift"]
      subject_template: "[{{ event | upper }}] {{ contract.title }}"
      message_template: "Run={{ run_id }}\nMessage={{ message }}"
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
# 17. STAGES (ENVIRONMENT OVERRIDES)
# ============================================================
# OPTIONAL: Override entire sections per stage
stages:
  bronze:
    server:
      mode: "ingest"
      cast_to_string: true
      schema_evolution: "append"
    quality:
      row_rules:
        - name: "email_basic"
          sql: "email LIKE '%@%'"
    # Business value: Minimal validation for Bronze
  
  silver:
    server:
      mode: "validate"
      schema_evolution: "strict"
    quality:
      enforce_required: true
      row_rules:
        - regex_match:
            field: "email"
            pattern: "^[^@]+@[^@]+\\.[^@]+$"
      dataset_rules:
        - unique: "customer_id"
    # Business value: Full validation for Silver
  
  gold:
    materialization:
      strategy: "overwrite"
    transformations:
      - sql: |
          SELECT
            customer_segment,
            COUNT(*) AS customer_count,
            AVG(lifetime_value) AS avg_ltv
          FROM source
          GROUP BY customer_segment
    # Business value: Aggregation for Gold

# Usage: DataProcessor(contract="contract.yaml", stage="silver")
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
|---------|--------|--------|------|
| `server.mode` | `ingest` | `validate` | `validate` |
| `server.cast_to_string` | `true` | `false` | `false` |
| `server.schema_evolution` | `append` | `strict` | `strict` |
| `quality.enforce_required` | `false` | `true` | `true` |
| `quality.row_rules` | Minimal | Full | Minimal |
| `quality.dataset_rules` | None | Yes | Yes |
| `materialization.strategy` | `append` | `merge`/`scd2` | `overwrite` |
| `lineage.enabled` | `true` | `true` | `true` |

---

*For more examples, see the [LakeLogic Examples](../examples/) directory.*
