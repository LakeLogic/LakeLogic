# Complete Contract Template Reference

This is a **fully annotated contract template** showing every available configuration option with detailed comments explaining business value and use case scenarios.

Use this as a reference to understand what's possible, then create your own contracts by copying only the sections you need.

---

## The Versioning Architecture

The industry gold standard for Data Contracts is a hybrid approach known as **"Major Pinning, Minor Floating"**.

In LakeLogic, you mandate that all contracts use strict Semantic Versioning (e.g., `v1.2.0` -> `Major.Minor.Patch`):

* **Major Changes (`v2.0`):** Breaking changes (deleting columns, altering core data types, rotating primary keys).
* **Minor Changes (`v1.3`):** Additive changes (appending new optional columns, adding new metadata tags).

Because Fact tables act as permanent systemic ledgers, your downstream contracts should pin their `foreign_key` validation pointers **only** to the Major version string:

```yaml
      foreign_key:
        contract: "gold_sales_dim_accounts_v1"
```

This strategy strictly immunizes your data pipeline against destructive upstream changes (like an upstream domain deleting columns in `v2.0`), while dynamically floating those additive features (like a new `v1.3` attribute) straight into your data model safely!

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
  
  # ── Run Log Persistence ────────────────────────────────────
  # Controls where pipeline run metadata is stored.
  # All three targets can be used simultaneously.
  
  run_log_dir: "{log_root}/runs/{domain}_{system}_{bronze_layer}_events"
  # Per-run JSON files — one file per run. Works on local + ADLS + S3 + GCS.
  # Best for: lightweight logging, cloud storage
  
  # run_log_path: "{log_root}/run_log.json"
  # Single JSON file — overwritten each run (no history).
  # Best for: simplest possible logging
  
  run_log_table: "{domain_catalog}._run_logs"
  # Queryable table — auto-detects backend from engine:
  #   spark engine  → Spark Delta table (Unity Catalog)
  #   polars engine → Delta table via delta-rs (ADLS/S3/local)
  #   duckdb engine → local DuckDB file
  # Best for: production — queryable, partitionable, Delta format
  
  # run_log_backend: "delta"
  # Explicit backend override. Options: spark, delta, duckdb, sqlite
  # If omitted, auto-detected from engine (spark → spark, everything else → delta)
  
  # run_log_table_partition_by: ["domain", "data_layer"]
  # Partition the run log table for faster queries (Spark + delta backends)
  
  # run_log_database: "logs/lakelogic_run_logs.duckdb"
  # Only for duckdb/sqlite backends: path to embedded database file

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
  #
  # ── Load Mode Validation ─────────────────────────────────────
  # The pipeline validates required properties per load_mode:
  #   incremental → watermark_field recommended (defaults to _lakelogic_processed_at)
  #   cdc         → cdc_op_field REQUIRED (raises error if missing)
  #   full        → no additional requirements
  
  pattern: "*.parquet"
  # OPTIONAL: File pattern filter
  # Business value: Select specific files from directory

  format: "csv"
  # OPTIONAL: Source file format (default: auto-detected from path/extension)
  # Options: parquet | csv | json | delta | avro | orc | xml
  # Business value: Explicit format when auto-detection isn't sufficient
  
  options:
    header: "true"
    inferSchema: "true"
    recursiveFileLookup: "true"
    multiLine: "true"
    delimiter: ","
  # OPTIONAL: Reader options passed directly to the engine (Spark, Pandas, etc.)
  # Common CSV options: header, inferSchema, delimiter, quote, escape, encoding
  # Common JSON options: multiLine, allowComments
  # Common Parquet options: mergeSchema
  # Business value: Fine-grained control over file parsing behavior

  cdc_op_field: "operation"
  # REQUIRED for cdc: Field indicating operation type
  # Business value: Change data capture support
  
  cdc_delete_values: ["D", "DELETE"]
  # Values indicating delete operations
  # Business value: Handle deletes in CDC streams (see materialization.soft_delete_column for detail on Tombstone vs Hard Delete behavior)
  
  cdc_timestamp_field: "source_record_updated_at"
  # OPTIONAL for cdc: Source column providing the exact event time for soft deletes.
  # If specified, the engine will use this timestamp (coalescing to current time if null).
  # Business value: High accuracy tracking of exactly when a record was deleted upstream.
  
  # ── CDC Example Usage ────────────────────────────────────────
  # To enable CDC, set load_mode: cdc and map your source operation flags:
  #
  # Example:
  #   source:
  #     type: table
  #     path: "{domain_catalog}.{bronze_layer}_{system}_events"
  #     load_mode: cdc
  #     cdc_op_field: "__START_AT"  # Or whatever column holds the CDC op
  #     cdc_delete_values: ["D", "Delete", "Remove"]
  #     cdc_timestamp_field: "_lakelogic_processed_at"
  
  watermark_strategy: "max_target"
  # Options: max_target (default), pipeline_log, manifest, lookback, date_range, delta_version
  # Business value: Flexible incremental boundary resolution
  #
  # ── Strategy Comparison ──────────────────────────────────────
  #
  #  Strategy        Best For                 State Stored In          Requires Spark?
  #  ──────────────  ───────────────────────  ───────────────────────  ───────────────
  #  max_target      Most batch pipelines     Target table (self-heal) Yes
  #  pipeline_log    Partial overwrites       _run_logs table          Yes
  #  lookback        Simple rolling windows   None (stateless)         No
  #  date_range      Backfills & widgets      None (explicit dates)    No
  #  manifest        Non-Spark pipelines      JSON manifest file       No
  #  delta_version   Large streaming tables   Target TBLPROPERTIES     Yes
  #
  # ── Strategy 1: max_target (DEFAULT) ──────────────────────────────
  # Queries MAX(watermark_field) on the target Delta table.
  # Self-healing: if target is manually edited, next run re-reads from
  # the actual high-water mark.
  #
  # Example:
  #   source:
  #     type: table
  #     path: "{domain_catalog}.{bronze_layer}_{system}_events"
  #     load_mode: incremental
  #     watermark_strategy: max_target
  #     watermark_field: "_lakelogic_processed_at"
  #     target_path: "abfss://silver@acct.dfs.core.windows.net/events"
  #   # Requires: target_path pointing to the target Delta table
  #   # Uses watermark_field: YES — to query MAX and to filter source
  #
  # ── Strategy 2: pipeline_log ──────────────────────────────────────
  # Queries the _run_logs table (written by LakeLogic after each run)
  # for the last successful run of this dataset.
  # Uses MAX(timestamp) from _run_logs filtered by dataset column.
  #
  # Example:
  #   source:
  #     type: table
  #     path: "{domain_catalog}.{bronze_layer}_{system}_events"
  #     load_mode: incremental
  #     watermark_strategy: pipeline_log
  #   # State: _run_logs table (configured via metadata.run_log_table)
  #   # Filters by: dataset (target table name), data_layer, domain, system
  #   # Uses watermark_field: YES — to filter source after boundary resolved
  #   # Fallback: if no prior runs, uses NOW - 90 days
  #
  # ── Strategy 3: lookback ──────────────────────────────────────────
  # Sliding window: processes everything from NOW - duration to NOW.
  # No state needed. Simple and predictable.
  #
  # Example (daily):
  #   source:
  #     type: table
  #     path: "{domain_catalog}.{bronze_layer}_{system}_events"
  #     load_mode: incremental
  #     watermark_strategy: lookback
  #     lookback: "7 days"
  #     watermark_field: "_lakelogic_processed_at"
  #
  # Example (near-real-time):
  #   source:
  #     watermark_strategy: lookback
  #     lookback: "3 hours"
  #     watermark_field: "event_timestamp"
  #
  # Example (monthly batch):
  #   source:
  #     watermark_strategy: lookback
  #     lookback: "1 month"
  #
  # Supported durations: "N days", "N hours", "N mins", "N month(s)"
  # Uses watermark_field: YES — to filter source
  # Overlap: allows reprocessing of data within the window (idempotent writes recommended)
  #
  # ── Strategy 4: date_range ────────────────────────────────────────
  # Explicit from/to dates. Ideal for backfills and Databricks Widgets.
  #
  # Example:
  #   source:
  #     type: table
  #     path: "{domain_catalog}.{bronze_layer}_{system}_events"
  #     load_mode: incremental
  #     watermark_strategy: date_range
  #     from_date: "2024-01-01"
  #     to_date: "2024-12-31"
  #     watermark_field: "event_date"
  #   # Uses watermark_field: YES — to filter source
  #   # Tip: from_date/to_date can be overridden at runtime via pipeline params
  #
  # ── Strategy 5: manifest ──────────────────────────────────────────
  # Reads a JSON manifest file listing already-processed partitions.
  # Lightweight alternative for non-Spark environments (Polars, Pandas, Azure Functions).
  #
  # Example:
  #   source:
  #     type: landing
  #     path: "abfss://landing@acct.dfs.core.windows.net/events/"
  #     load_mode: incremental
  #     watermark_strategy: manifest
  #     manifest_path: "abfss://meta@acct.dfs.core.windows.net/manifests/events.json"
  #     watermark_field: "_snapshot_date"
  #   # Manifest JSON format:
  #   #   { "processed_partitions": ["2024-03-20", "2024-03-21"], "last_updated": "..." }
  #   # State: the manifest file itself
  #   # Uses watermark_field: YES — as the partition field tracked in the manifest
  #   # Note: external process must update the manifest after each successful run
  #
  # ── Strategy 6: delta_version ─────────────────────────────────────
  # Uses Delta transaction log versions instead of timestamp columns.
  # Reads only the specific Parquet files added/changed between versions.
  # Fastest strategy for large tables — no column scanning required.
  #
  # Example:
  #   source:
  #     type: table
  #     path: "catalog.bronze.sessions"
  #     load_mode: incremental
  #     watermark_strategy: delta_version
  #     target_path: "table:catalog.silver.sessions"
  #   # State: target table TBLPROPERTIES('lakelogic.last_source_version')
  #   # Also captured in _run_logs for full audit trail
  #   # Uses watermark_field: NO — versions replace timestamps entirely
  #   # Safeguards:
  #   #   - Detects source rollback (from_v > to_v) and auto-resets
  #   #   - Exempt from mandatory run_log requirement (uses TBLPROPERTIES)
  #   # Requirement: Spark-only, both source and target must be Delta tables

  watermark_field: "_lakelogic_processed_at"
  # For incremental loads: Field to track processing progress.
  # If omitted, defaults to "_lakelogic_processed_at" (stamped by lineage as TIMESTAMP).
  # For partition-pruned reads, use the partition column (e.g. session_date).
  # NOT used by delta_version strategy (operates on version numbers).
  # Business value: Efficient incremental loading
  
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
  
  # ── Date-Partitioned Landing ───────────────────────────────
  # Limits file globbing to only relevant date partitions instead
  # of scanning the entire landing directory. Critical for scale.
  partition:
    format: "y_%Y/m_%m/d_%d"
    # Strftime-style format defining the directory structure.
    # Examples:
    #   "y_%Y/m_%m/d_%d"     → events/y_2026/m_03/d_22/*.json
    #   "date=%Y%m%d"         → events/date=20260322/*.json
    #   "%Y/%m/%d"            → events/2026/03/22/*.json
    #   "year=%Y/month=%m"    → events/year=2026/month=03/*.json
    #   "dt=%Y-%m-%d/%H"      → events/dt=2026-03-22/17/*.json (hourly)
    # Business value: Scan scope drops from millions of files to days
    
    lookback_days: 3
    # How many days back to scan from today. Default: 1.
    # On FIRST RUN (no watermark), lookback is respected to prevent massive backfills.
    # Can be overridden at runtime: pipeline.run(lookback_days=30)
    # Business value: Controls daily scan scope and initial load bounds
    
    # start_date: "2026-01-01"
    # end_date: "2026-03-22"
    # Optional: explicit date range for backfills.
    # Also auto-populated from pipeline.run(reprocess_from=..., reprocess_to=...)
    # Business value: Targeted historical reprocessing
    
    # file_pattern: "*.json"
    # Optional: glob pattern for files within each partition.
    # Auto-derived from source.format if omitted (format: json → *.json)
    # Business value: Only needed when file_pattern differs from format

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
  # If true, allow undocumented columns to pass through the pipeline.
  # If false (and schema_evolution is strict), the pipeline automatically prunes any
  # unmapped columns from the final table, strictly enforcing the contract exactly.
  # Business value: Monitoring vs strict contract enforcement
  
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
    columns: ["code", "name", "region"]
    # Column projection — only load these columns from the linked table.
    # Reduces DataFrame footprint by avoiding unused columns.
    # If omitted or empty, all columns are loaded (default).
    # Business value: Memory optimization for wide reference tables
  
  - name: "dim_products"
    table: "catalog.reference.products"
    type: "table"
    broadcast: false
    columns: ["id", "product_name", "category", "price"]
    # Business value: Unity Catalog / Hive table reference with projection
  
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

# OPTIONAL: Business/natural key for SCD2 dimensions
# In SCD2, primary_key is the surrogate (unique per row), while natural_key
# is the business key that repeats across versions.
natural_key:
  - "customer_id"
# Business value: Separates surrogate identity from business identity

# ============================================================
# 7. SCHEMA MODEL
# ============================================================
# OPTIONAL: Define expected schema with types and constraints
model:
  # OPTIONAL: Human-readable grain description
  grain: "one row per customer"
  # Business value: Self-documenting data contract — makes the conceptual unit explicit

  # OPTIONAL: Columns that define the grain (subset of primary_key)
  grain_key: ["customer_id"]
  # Business value: LakeLogic can validate no duplicate grain rows are produced

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

      # OPTIONAL: Kimball Dimensional Modelling flags
      nullable: true
      # Explicit nullability declaration — critical for accumulating snapshot milestones
      # where dates start as null until that lifecycle stage is reached.
      # Business value: Generates COALESCE merge logic for milestone columns

      milestone: true
      # Flag for accumulating snapshot milestone date columns.
      # Business value: Signals that this column tracks a lifecycle stage

      generated: true
      # Flag for auto-generated fields (surrogate keys, SCD2 validity columns).
      # Fields marked generated: true are produced by LakeLogic, not the source.
      # Business value: Complete, honest description of the output table schema
      
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
      # ── PII Security Group Mapping ──
      security_groups: ["pii-readers", "compliance-team"]
      # Groups allowed to see unmasked values.
      # Maps to IAM/AD groups or Databricks UC is_member() groups.
      # Business value: Role-based data masking per field
      masking: "partial"
      # Default mask when user NOT in security_groups.
      # Options: nullify | hash | redact | partial | tokenize | encrypt
      # partial → j***@company.com  (preserves structure)
      # Business value: Configurable masking strategy per field
      pii_vault: "vault://pii-encryption-key"
      # Optional: encrypted vault table for reversible unmasking.
      # When set, pipeline dual-writes: masked output + encrypted PII vault.
      # Authorized users can rejoin vault to reconstruct unmasked data.
      # Business value: Reversible masking for authorized re-identification
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
      "on": ["customer_id"]
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
      "on": "country_code"
      key: "code"
      value: "name"
      default_value: "Unknown"
    phase: "post"
    # Business value: Enrich with reference data
  
  # Full join with multiple fields
  - join:
      reference: "dim_products"
      "on": "product_id"
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
    # Business value: Compute metrics (Pair with `fact: type: aggregate` below for auto-lineage!)

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
#
# ── Pipeline Execution Order ──────────────────────────────────
# 1. Pre-transforms  → renames, filters, dedup (phase: pre)
# 2. Schema enforcement → cast columns to contract types
# 3. Pre quality rules (default) → validate source columns
# 4. Good/bad split → quarantine rows failing pre-rules
# 5. Post-transforms → derives, lookups, joins (phase: post)
# 6. Post quality rules (phase: post) → validate derived columns,
#    quarantine failures
#
# Pre-phase rules can only reference SOURCE columns.
# Post-phase rules can reference both source AND derived columns.
# ──────────────────────────────────────────────────────────────
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
    
    # Post-phase rule (runs AFTER transforms — can reference derived columns)
    - name: "derived_date_not_null"
      sql: "event_date_parsed IS NOT NULL"
      phase: "post"
      category: "correctness"
      description: "Derived date must parse successfully"
      # phase: pre (default) — runs before transforms, source columns only
      # phase: post — runs after transforms, can reference derived columns
      # Errors tagged [pre] or [post] in _lakelogic_errors for traceability
  
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
  # append: Add new rows (e.g. Bronze raw logs, Gold fact tables)
  #         (Add fact: block below to enable Kimball auto-governance and structural validations)
  # merge: Upsert based on primary key (e.g. Silver CDC, Gold dimensions)
  #        (Add scd1: block below to enable SCD Type 1 auto-SK and Unknown Member)
  # scd2: Slowly Changing Dimension Type 2 (history tracking - Configured via scd2: block below)
  # overwrite: Replace all data (e.g. daily snapshots)
  # Business value: Choose appropriate write pattern across all Medallion layers
  
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
  
  # ── Fact Table Configuration (Auto-Governance) ───────────────
  # OPTIONAL: Define the business purpose of this fact table.
  # LakeLogic uses this to automatically enforce data quality rules 
  # based on Kimball best practices, so you don't have to write them manually.
  fact:
    type: "accumulating_snapshot"  
    # Options & Business Value (Pick One):
    # - transaction: Immutable log of events (e.g. Sales, Clicks). Cannot be updated.
    # - periodic_snapshot: State at a point in time (e.g. Daily Account Balances).
    # - accumulating_snapshot: Tracks lifecycle of a process (e.g. Order Pipelines, SLAs).
    # - factless: Tracks events that have no numeric metrics (e.g. Student Attendance).
    # - aggregate: Pre-summarized metrics for dashboards (e.g. Monthly Revenue).
    #   (Pair this with a `rollup:` transformation block below to compute the actual math!)
    
    milestone_dates:
      # Required ONLY for 'accumulating_snapshot'. 
      # Tracks the progression of an entity through defined stages.
      # LakeLogic automatically ensures these dates always flow sequentially
      # (e.g. preventing 'shipped_date' from arriving before 'placed_date').
      - "placed_date"
      - "shipped_date"
      - "delivered_date"

  # ── SCD Type 2 specific configuration ────────────────────────
  scd2:
    timestamp_field: "updated_at"
    # Field to determine record version
    
    start_date_field: "valid_from"
    # Column to store validity start date
    
    end_date_field: "valid_to"
    # Column to store validity end date
    
    current_flag_field: "is_current"
    # Boolean flag for current record
    
    start_date_default: "1900-01-01"
    # Default value for initial load records

    end_date_default: "9999-12-31"
    # Default value for open-ended records

    track_columns: ["email", "status", "age"]
    # OPTIONAL: Only open a new version when one of these columns changes.
    # If omitted, any incoming row for a known key triggers a new version.
    # Business value: Reduce version churn from no-op updates
    
    # ── Surrogate Key (auto-generated) ─────────────────────────
    surrogate_key: "_sk"
    # Column name for auto-generated surrogate key. Default: "_sk"
    # Set to null/empty to disable.
    # Business value: Unique row-level ID for downstream joins
    
    surrogate_key_strategy: "hash"
    # Options: hash (default), uuid
    # hash: SHA256(primary_key|effective_from)[:16] — deterministic, reprocess-safe
    # uuid: random 16-char hex — unique but non-deterministic
    # Business value: Idempotent surrogate keys for reprocessing
    
    # ── Version Number (auto-generated) ────────────────────────
    version_column: "_version"
    # Column name for auto-incrementing version per business key. Default: "_version"
    # Computed as ROW_NUMBER() OVER (partition_by primary_key ORDER BY effective_from)
    # Set to null/empty to disable.
    # Business value: "Give me version N of this customer"
    
    # ── Change Reason (auto-generated) ─────────────────────────
    change_reason_column: "_change_reason"
    # Column name for tracking which fields triggered a new version.
    # Default: "_change_reason". Set to null/empty to disable.
    # Values:
    #   "initial_load"   — first appearance of this business key
    #   "email,status"   — comma-separated list of changed tracked fields
    #   "all"            — no track_columns specified (all changes trigger version)
    # Business value: Audit trail — why was a new version created?
    
    # ── Unknown Member Injection ───────────────────────────────
    # Enabled by default. Provides an idempotently generated late-arriving fact fallback row.
    unknown_member:
      enabled: true
      surrogate_key_value: "-1"
      # OPTIONAL: Explicit default values to override lakelogic's auto-inference.
      # If omitted or a column is left out, lakelogic automatically infers defaults 
      # from schema data types (Numeric->-1, Boolean->False). 
      # Strings default to "Unknown" but safely truncate to fit VARCHAR(N) constraints.
      # Examples: VARCHAR(2) -> "Un", VARCHAR(4) -> "Unkn", VARCHAR(X) > 7 -> "Unknown".
      # default_values:
      #   customer_id: -1
      #   email: "Unknown"
      #   status: "Unknown"

  # SCD Type 1 specific configuration (only applied if strategy: "merge")
  scd1:
    surrogate_key: "_sk"
    # Auto-generates a Surrogate Key for the dimension (Hash of primary key)
    # Business value: Fast integer/hash joins for downstream facts where history is not needed
    
    surrogate_key_strategy: "hash"
    # Options: hash (default), uuid
    # hash: SHA256(primary_key)[:16] — deterministic
    
    # ── Unknown Member Injection ───────────────────────────────
    # Enabled by default. Provides an idempotently generated late-arriving fact fallback row even for SCD1.
    unknown_member:
      enabled: true
      surrogate_key_value: "-1"
      # OPTIONAL: Explicit default values to override lakelogic's auto-inference.
      # If omitted or a column is left out, lakelogic automatically infers defaults 
      # from schema data types (Numeric->-1, Boolean->False). 
      # Strings default to "Unknown" but safely truncate to fit VARCHAR(N) constraints.
      # Examples: VARCHAR(2) -> "Un", VARCHAR(4) -> "Unkn", VARCHAR(X) > 7 -> "Unknown".
      # default_values:
      #   customer_id: -1
      #   email: "Unknown"
      #   status: "Unknown"

  # OPTIONAL: Soft-delete support (mark deleted instead of removing). 
  # - If set (Tombstone): Deletes from CDC flip this flag to true and preserve the row. 
  #   Late-arriving inserts for already deleted records will safely insert as tombstones.
  # - If omitted (Hard Delete): Deletes from CDC physically remove the row from the target.
  #
  # ✨ NOTE: When `source.load_mode` is set to `cdc`, LakeLogic automatically 
  # enables soft-deletes (Tombstone) by injecting the default columns below. 
  # To force Hard Deletes during a CDC load, explicitly set `soft_delete_column: ""` (empty string).
  soft_delete_column: "_lakelogic_is_deleted"
  # Column name for soft-delete boolean flag
  soft_delete_value: true
  # Value to set when record is deleted
  soft_delete_time_column: "_lakelogic_deleted_at"
  # Timestamp for when soft-delete occurred (auto-filled on CDC deletes if empty)
  soft_delete_reason_column: "_lakelogic_delete_reason"
  # Reason for deletion (auto-filled to 'cdc_delete_signal' on CDC deletes if empty)
  # Business value: GDPR compliance & Auditability — maintain history without hard deletes
  
  # OPTIONAL: External storage location for Unity Catalog tables
  location: "abfss://container@account.dfs.core.windows.net/silver/customers/"
  # Business value: Control physical storage for UC managed tables
  # Supports placeholders: "{silver_path}/{silver_layer}_{system}_customers"
  # See _system.yaml Placeholders section below
  
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
# OPTIONAL: Lineage capture configuration.
# When enabled, up to 8 lineage columns are injected into every output row.
#
# All 8 lineage columns:
#   _lakelogic_source         — source file/table path
#   _lakelogic_processed_at   — updated every pipeline run
#   _lakelogic_run_id         — unique run identifier
#   _lakelogic_contract_name  — YAML contract filename
#   _lakelogic_domain         — domain from metadata
#   _lakelogic_system         — system from metadata
#   _lakelogic_created_at     — first-insert timestamp (immutable)
#   _lakelogic_created_by     — username/service principal
#
lineage:
  enabled: true
  # If true, inject lineage columns
  # Business value: Data provenance tracking
  
  capture_source_path: true
  source_column_name: "_lakelogic_source"
  # Capture source file/table path
  
  capture_timestamp: true
  timestamp_column_name: "_lakelogic_processed_at"
  # Updated every pipeline run (processing time)
  # Also serves as default watermark for incremental loading
  
  capture_run_id: true
  run_id_column_name: "_lakelogic_run_id"
  # Capture unique run identifier
  
  capture_contract_name: true
  contract_name_column_name: "_lakelogic_contract_name"
  # Inject contract title into every output row
  # Business value: Identify which contract produced each record
  
  capture_domain: true
  domain_column_name: "_lakelogic_domain"
  # Capture domain from metadata
  
  capture_system: true
  system_column_name: "_lakelogic_system"
  # Capture source system from metadata
  
  # ── Record Creation Tracking ─────────────────────────────
  capture_created_at: true
  created_at_column_name: "_lakelogic_created_at"
  # Stamped when record is FIRST created (immutable on re-runs).
  # Unlike _lakelogic_processed_at (updated every run),
  # this preserves the original insertion timestamp.
  # Business value: Audit trail — when was this record first ingested?
  
  capture_created_by: true
  created_by_column_name: "_lakelogic_created_by"
  # Captures the user/service principal that created the record.
  # Default: getpass.getuser() — resolves to service principal in Databricks.
  # Business value: Track who/what system inserted each record
  
  created_by_override: "etl_pipeline_svc"
  # OPTIONAL: Override the default user detection with a static value.
  # Useful for CI/CD pipelines or shared service accounts.
  # If omitted, falls back to getpass.getuser().
  # Can also be passed from pipeline_driver.py or Databricks Widgets.
  # Business value: Consistent identity in multi-user environments
  
  # ── Upstream Lineage ─────────────────────────────────────
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
    # Data must be refreshed within 24 hours (supports: "30m", "6h", "1d")
    field: "updated_at"
    # Field to check for freshness (MAX of this column vs current time)
    description: "Customer data must be updated daily"
    # Business value: Timeliness monitoring

  availability:
    threshold: 99.9
    # ── NOTE: This is FIELD COMPLETENESS, not uptime! ──
    # Percentage of rows that must have a non-null value in the specified field.
    # Example: 99.9 means <= 0.1% of rows can have a null value.
    field: "customer_id"
    # Field to measure non-null ratio against
    description: "customer_id must be populated in 99.9% of rows"
    # Business value: Data completeness monitoring

  row_count:
    min_rows: 100
    # Minimum expected rows per pipeline run (fail if below)
    max_rows: 10000000
    # Maximum expected rows per pipeline run (fail if above — catches runaway cartesian joins)
    check_field: "counts_source"
    # Which run log count to validate against:
    #   counts_source: rows received from upstream (default for Bronze)
    #   counts_good:   rows after quality filtering (default)
    #   counts_total:  good + quarantined before filtering
    skip_reprocess_days: 3
    # When the reprocess date range (reprocess_from → reprocess_to) exceeds
    # this many days, SLO row count checks AND counts_source computation
    # are skipped entirely — avoids expensive Spark wide-transformation
    # actions on large backfills where thresholds don't apply.
    # Default: 3 (skip for backfills > 3 days). Set to 0 to never skip.
    description: "Revenue transactions must produce between 100 and 10M rows"
    # Business value: Volumetric anomaly detection — catches empty loads AND data explosions
    # NOTE: min_rows, max_rows, and check_field are captured in the run log
    # at pipeline time for point-in-time auditability.

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
  # Function name to call (Python only, default: "run")
  #
  # ─── Required function signature ───────────────────────
  # def build_gold(df, *, contract, engine, **kwargs):
  #     """
  #     Args:
  #         df:       Validated good DataFrame (Polars/Pandas/Spark)
  #         contract: DataContract instance (read-only)
  #         engine:   Engine name ("polars", "pandas", "spark", etc.)
  #         **kwargs: Values from args: below (e.g. apply_ml_scoring)
  #
  #     Returns:
  #         DataFrame  → replaces good_df for materialization
  #         str/Path   → LakeLogic reads this file as output
  #         None       → original good_df is kept
  #     """
  #     return df
  # ────────────────────────────────────────────────────────
  # Optional extra kwargs (accepted but not required):
  #   add_trace:  callback to append TraceStep objects
  #   trace_step: context manager for traced blocks
  #
  # Runs in a sandboxed thread with timeout (default 300s).
  # Blocked imports: subprocess, shutil, socket.
  # Blocked builtins: exec, eval, compile.

  args:
    apply_ml_scoring: true
    model_path: "s3://models/churn_predictor.pkl"
    target_table: "gold_customers"
  # Custom arguments passed as **kwargs to the function
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

# ============================================================
# 20. CLOUD REPORTING (Registry-Level)
# ============================================================
# OPTIONAL: Send run reports to LakeLogic Cloud for centralized
# observability, trend detection, and ops intelligence.
#
# This is typically set at the REGISTRY level (_registry.yaml)
# so all contracts in a domain share the same config.
# It can also be set per-contract if needed.
#
# The cloud block maps to RemoteObserver env vars:
#   cloud.enabled    → LAKELOGIC_REMOTE_OBSERVER
#   cloud.report_url → LINEAGELOGIC_REPORT_URL
#   cloud.api_key    → LINEAGELOGIC_API_KEY
#
# Supports ${ENV_VAR} syntax for secrets.
cloud:
  enabled: true
  # Toggle remote reporting (default: false)

  report_url: "${LINEAGELOGIC_REPORT_URL}"
  # API endpoint for run report ingestion
  # Resolved from environment variable at runtime

  api_key: "${LAKELOGIC_API_KEY}"
  # Auth key — identifies your tenant
  # Resolved from environment variable at runtime
  # Business value: Centralized quality dashboards, SLO trend
  # detection, quarantine spike alerts, cross-contract intelligence

# ── What gets reported ─────────────────────────────────────
# Each run sends (no raw data — metadata only):
#   - Contract name, dataset, stage, engine, timestamp
#   - Row counts: source, total, good, quarantined
#   - Per-rule failure breakdown
#   - Schema drift events
#   - SLO scores (freshness, availability)
#   - Duration (ms)
#   - Domain, system, data_layer metadata

# ============================================================
# 21. COMPLIANCE — Regulatory Metadata
# ============================================================
# OPTIONAL: Multi-framework compliance metadata for automated
# compliance reporting in LakeLogic Cloud. Supports GDPR, EU AI Act,
# CCPA/CPRA, HIPAA, SOX, PIPEDA, LGPD, and Basel/BCBS 239.
#
# This section enables LakeLogic Cloud to generate audit-ready
# compliance reports from your data contract registry.

compliance:

  # ── GDPR (EU) 2016/679 ──────────────────────────────────
  gdpr:
    applicable: true
    # Whether GDPR applies to this dataset
    # Business value: Scope determination

    legal_basis: "legitimate_interest"
    # Options: consent, contract, legitimate_interest,
    #          legal_obligation, public_interest, vital_interest
    # GDPR Art. 6(1) — lawful basis for processing
    # Business value: Art. 30 RoPA mandatory field

    legal_basis_detail: >
      Processing based on legitimate interest for direct
      marketing under Art. 6(1)(f). Data subjects can object
      via unsubscribe mechanism at any time.
    # Business value: Detailed justification for DPO review

    purpose: "Customer engagement tracking and marketing analytics"
    # Purpose of processing (Art. 5(1)(b))
    # Business value: Purpose limitation compliance

    retention_period: "24 months"
    # How long data is retained (Art. 5(1)(e))
    # Options: any human-readable duration string
    # Business value: Storage limitation compliance

    retention_basis: "Marketing consent validity"
    # Why this retention period was chosen
    # Business value: Audit justification

    consent_type: "opt_in"
    # Options: opt_in, opt_out, implied, none
    # Business value: Consent management tracking

    withdrawal_mechanism: true
    # Whether data subjects can withdraw consent
    # Business value: Art. 7(3) compliance

    dpia_required: false
    # Whether a Data Protection Impact Assessment is needed
    # Business value: Art. 35 compliance

    dpia_status: "not_required"
    # Options: not_required, not_started, in_progress, completed
    # Business value: Track DPIA progress

    data_subject_categories:
      - "retail_customers"
      - "newsletter_subscribers"
    # Categories of people whose data is processed
    # Business value: Art. 30 RoPA field

    cross_border_transfer: true
    # Whether data crosses EU/EEA borders
    # Business value: Chapter V transfer obligations

    transfer_mechanism: "SCCs"
    # Options: SCCs, adequacy, BCRs, derogation
    # Business value: Transfer safeguards (Art. 46)

    shared_with:
      - name: "Klaviyo Inc."
        role: "processor"         # processor or joint_controller
        country: "US"
        agreement: "DPA"          # DPA, BAA, SCC, joint_controller
        mechanism: "SCCs"
      - name: "Shopify Inc."
        role: "joint_controller"
        country: "CA"
        agreement: "DPA"
        mechanism: "SCCs"
    # Data sharing agreements
    # Business value: Art. 28 processor contracts, Art. 30 disclosures

  # ── EU AI Act (Reg. 2024/1689) ─────────────────────────
  eu_ai_act:
    applicable: true
    # Whether the EU AI Act applies (data feeds AI systems)
    # Business value: Scope determination

    risk_tier: "limited"
    # Options: prohibited, high, gpai, limited, minimal
    # Art. 6 + Annex III risk classification
    # Business value: Determines obligation level

    risk_tier_rationale: >
      Marketing automation data classified as LIMITED RISK
      under Art. 50. Transparency obligations apply. Would
      escalate to HIGH RISK if used for credit scoring or
      HR decisions under Annex III.
    # Business value: Documented risk assessment

    ai_system_purpose: "Marketing automation and personalisation"
    # What the AI system does with this data
    # Business value: Art. 11 technical documentation

    ai_systems_using_data:
      - name: "send_time_optimisation"
        risk_tier: "limited"
        description: "ML model predicting optimal email send time"
      - name: "churn_prediction"
        risk_tier: "limited"
        description: "Model predicting likelihood of unsubscribe"
    # AI systems that consume this dataset
    # Business value: AI system inventory (Art. 6)

    training_data_provenance: true
    # Whether training data origin is documented
    # Business value: Art. 10 — data governance for training

    bias_examination: false
    # Whether bias examination has been conducted
    # Business value: Art. 10(2)(f) — bias detection

    transparency_disclosure: true
    # Whether users are informed about AI processing
    # Business value: Art. 13 + Art. 50 transparency

    human_oversight: true
    # Whether human oversight mechanisms exist
    # Business value: Art. 14 — human-in-the-loop

    logging_enabled: true
    # Whether AI operations are logged
    # Business value: Art. 12 — automatic logging

  # ── Other Frameworks (coming soon) ─────────────────────
  # Supported but detailed section not yet available:
  ccpa:
    applicable: true
    status: "coming_soon"
  hipaa:
    applicable: false
  sox:
    applicable: false
  pipeda:
    applicable: true
    status: "coming_soon"
  lgpd:
    applicable: true
    status: "coming_soon"
  bcbs_239:
    applicable: false

```

---

## Common Use Case Templates

### Use Case 1: Bronze Ingestion (Capture Everything)

```yaml
version: 1.0.0
info:
  title: "Bronze CRM Contacts"
  table_name: "{bronze_layer}_{system}_contacts"
  target_layer: "bronze"

# ── The Reader: Defines WHAT and WHEN to read (State Tracking) ────
source:
  type: "landing"
  path: "s3://landing/crm/*.csv"
  load_mode: "incremental"
  watermark_field: "file_modified_time"

# ── The Adapter: Defines HOW to physically parse the raw payload ──
# OPTIONAL: Only strictly needed if connecting to a DB, or if requiring 
# raw format rules (like safely casting messy JSON/CSV to strings).
server:
  type: "s3"
  path: "s3://bronze/crm_contacts"
  format: "parquet"
  mode: "ingest"
  cast_to_string: true    # Safety-net against schema crashes!
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
  table_name: "{silver_layer}_{system}_customers"
  target_layer: "silver"

source:
  type: "table"
  path: "{domain_catalog}.{bronze_layer}_{system}_customers"
  load_mode: "incremental"
  watermark_strategy: "pipeline_log"

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
  - sql: |
      WITH ranked_data AS (
        SELECT 
          *,
          ROW_NUMBER() OVER(PARTITION BY customer_id ORDER BY updated_at DESC) as _rn
        FROM source
      )
      SELECT 
        s.* EXCEPT(_rn),
        COALESCE(p.marketing_opt_in, FALSE) AS is_opted_in 
      FROM ranked_data s
      LEFT JOIN {domain_catalog}.{silver_layer}_{system}_preferences p
        ON s.customer_id = p.customer_id
      WHERE s._rn = 1
    phase: "pre"
  # ── Alternative YAML syntax:
  # - deduplicate:
  #     "on": ["customer_id"]
  #     sort_by: ["updated_at"]
  #     order: "desc"
  #   phase: "pre"

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
  table_name: "{gold_layer}_{system}_customer_metrics"
  target_layer: "gold"

source:
  type: "table"
  path: "{domain_catalog}.{silver_layer}_{system}_customers"
  load_mode: "incremental"
  watermark_strategy: "pipeline_log"

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
  # ── Alternative YAML syntax:
  # - filter:
  #     sql: "status = 'ACTIVE'"
  #   phase: "post"
  # - derive:
  #     field: "month"
  #     sql: "DATE_TRUNC('month', created_at)"
  #   phase: "post"
  # - rollup:
  #     group_by: ["customer_segment", "country", "month"]
  #     aggregations:
  #       customer_count: "COUNT(*)"
  #       avg_ltv: "AVG(lifetime_value)"
  #       total_orders: "SUM(total_orders)"
  #   phase: "post"

materialization:
  strategy: "overwrite"
  partition_by: ["month"]
  fact:
    type: "aggregate"

lineage:
  enabled: true
```

### Use Case 4: Gold Fact Table (Transaction Ledger)

```yaml
version: 1.0.0
info:
  title: "Gold Revenue Transactions"
  table_name: "{gold_layer}_{system}_fact_revenue"
  target_layer: "gold"

source:
  type: "table"
  path: "{domain_catalog}.{silver_layer}_{system}_transactions"
  load_mode: "incremental"
  watermark_strategy: "pipeline_log"

dataset: "fact_revenue"
primary_key: ["transaction_id"]

model:
  grain: "one row per revenue transaction"
  grain_key: ["transaction_id"]
  fields:
    - name: "transaction_id"
      type: "string"
      primary_key: true
    - name: "user_pseudo_id"
      type: "string"
      foreign_key:
        contract: "gold_marketing_dim_users"
        column: "user_pseudo_id"
    - name: "revenue_amount"
      type: "double"
    - name: "user_surrogate_key"
      type: "string"
      generated: true
      description: "Fetched dynamically from dim_users via SQL join"

transformations:
  - sql: |
      -- Drive SQL-first point-in-time (SCD2) dimension lookups natively!
      SELECT 
        s.*,
        COALESCE(d.user_key, '-1') AS user_surrogate_key
      FROM source s
      LEFT JOIN {domain_catalog}.{gold_layer}_marketing_dim_users d
        ON s.user_pseudo_id = d.user_pseudo_id
        AND s.transaction_date >= d.effective_from 
        AND s.transaction_date < COALESCE(d.effective_to, '9999-12-31')
    phase: "post"

materialization:
  strategy: "append"
  partition_by: ["transaction_date"]
  fact:
    type: "transaction"
```

### Use Case 5: Gold Fact Table (Accumulating Snapshot)

```yaml
version: 1.0.0
info:
  title: "Gold Order Funnel"
  table_name: "{gold_layer}_{system}_fact_order_timeline"
  target_layer: "gold"

source:
  type: "table"
  path: "{domain_catalog}.{silver_layer}_{system}_orders"
  load_mode: "incremental"
  watermark_strategy: "pipeline_log"

dataset: "fact_order_timeline"
primary_key: ["order_id"]

model:
  grain: "one row per order lifecycle"
  grain_key: ["order_id"]
  fields:
    - name: "order_id"
      type: "string"
      primary_key: true
    - name: "customer_id"
      type: "string"
      foreign_key:
        contract: "gold_sales_dim_customers"
        column: "customer_id"
    - name: "placed_date"
      type: "timestamp"
    - name: "shipped_date"
      type: "timestamp"
      nullable: true
      milestone: true
    - name: "delivered_date"
      type: "timestamp"
      nullable: true
      milestone: true
    - name: "customer_surrogate_key"
      type: "string"
      generated: true

transformations:
  - sql: |
      SELECT 
        o.*,
        COALESCE(c.customer_key, '-1') AS customer_surrogate_key
      FROM source o
      LEFT JOIN {domain_catalog}.{gold_layer}_sales_dim_customers c
        ON o.customer_id = c.customer_id
        AND o.placed_date >= c.effective_from 
        AND o.placed_date < COALESCE(c.effective_to, '9999-12-31')
    phase: "post"

materialization:
  strategy: "merge"
  fact:
    type: "accumulating_snapshot"
    milestone_dates:
      - "placed_date"
      - "shipped_date"
      - "delivered_date"
```

### Use Case 6: Gold Fact Table (Periodic Snapshot)

```yaml
version: 1.0.0
info:
  title: "Gold Daily Account Balances"
  table_name: "{gold_layer}_{system}_fact_account_balance"
  target_layer: "gold"

source:
  type: "table"
  path: "{domain_catalog}.{silver_layer}_{system}_accounts"
  load_mode: "incremental"
  watermark_strategy: "pipeline_log"

dataset: "fact_account_balance"
primary_key: ["account_id", "snapshot_date"]

model:
  grain: "one row per account per snapshot date"
  grain_key: ["account_id", "snapshot_date"]
  fields:
    - name: "account_id"
      type: "string"
      primary_key: true
      foreign_key:
        contract: "gold_sales_dim_accounts"
        column: "account_id"
    - name: "snapshot_date"
      type: "date"
      primary_key: true
    - name: "ending_balance"
      type: "double"
    - name: "account_surrogate_key"
      type: "string"
      generated: true

transformations:
  - sql: |
      SELECT 
        b.*,
        COALESCE(a.account_key, '-1') AS account_surrogate_key
      FROM source b
      LEFT JOIN {domain_catalog}.{gold_layer}_sales_dim_accounts a
        ON b.account_id = a.account_id
        AND b.snapshot_date >= a.effective_from 
        AND b.snapshot_date < COALESCE(a.effective_to, '9999-12-31')
    phase: "post"

materialization:
  strategy: "append"
  partition_by: ["snapshot_date"]
  fact:
    type: "periodic_snapshot"
```

### Use Case 7: Gold Fact Table (Factless)

```yaml
version: 1.0.0
info:
  title: "Gold Student Attendance"
  table_name: "{gold_layer}_{system}_fact_attendance"
  target_layer: "gold"

source:
  type: "table"
  path: "{domain_catalog}.{silver_layer}_{system}_events"
  load_mode: "incremental"
  watermark_strategy: "pipeline_log"

dataset: "fact_attendance"
primary_key: ["student_id", "class_id", "date"]

model:
  grain: "one row per student-class-date attendance event"
  grain_key: ["student_id", "class_id", "date"]
  fields:
    - name: "student_id"
      type: "string"
      foreign_key:
        contract: "dim_students"
        column: "student_id"
    - name: "class_id"
      type: "string"
    - name: "date"
      type: "date"
    - name: "student_surrogate_key"
      type: "string"
      generated: true

transformations:
  - sql: |
      SELECT 
        e.*,
        COALESCE(s.student_key, '-1') AS student_surrogate_key
      FROM source e
      LEFT JOIN {domain_catalog}.{gold_layer}_school_dim_students s
        ON e.student_id = s.student_id
        AND e.date >= s.effective_from 
        AND e.date < COALESCE(s.effective_to, '9999-12-31')
    phase: "post"

materialization:
  strategy: "append"
  partition_by: ["date"]
  fact:
    type: "factless"
    # LakeLogic automatically verifies no metric/numeric columns exist here!
```

### Use Case 8: SCD Type 1 (Upsert & Unknown Member)

```yaml
version: 1.0.0
info:
  title: "Silver Products (SCD1)"
  table_name: "{silver_layer}_{system}_dim_products"
  target_layer: "silver"

source:
  type: "table"
  path: "{domain_catalog}.{bronze_layer}_{system}_products"
  load_mode: "incremental"
  watermark_strategy: "pipeline_log"

dataset: "dim_products"
primary_key: ["product_id"]

model:
  grain: "one row per product (current state)"
  grain_key: ["product_id"]
  fields:
    - name: "product_id"
      type: "string"
      primary_key: true
    - name: "product_name"
      type: "string"
    - name: "category"
      type: "string"
    - name: "product_key"
      type: "string"
      generated: true
      description: "Surrogate key - SHA2 hash of product_id"

materialization:
  strategy: "merge"
  scd1:
    surrogate_key: "product_key"
    surrogate_key_strategy: "hash"
    unknown_member: true
    # Automatically generates a -1 hash record to catch late-arriving facts
```

### Use Case 9: SCD Type 2 (History Tracking)

```yaml
version: 1.0.0
info:
  title: "Silver Customer History (SCD2)"
  table_name: "{silver_layer}_{system}_dim_customers"
  target_layer: "silver"

source:
  type: "table"
  path: "{domain_catalog}.{bronze_layer}_{system}_customers"
  load_mode: "incremental"
  watermark_strategy: "pipeline_log"

dataset: "customers"
primary_key: ["customer_key"]
natural_key: ["customer_id"]

model:
  grain: "one row per customer per version (history tracked)"
  grain_key: ["customer_id", "valid_from"]
  fields:
    - name: "customer_key"
      type: "string"
      primary_key: true
      generated: true
      description: "Surrogate key - SHA2 hash of customer_id + valid_from"
    - name: "customer_id"
      type: "string"
    - name: "email"
      type: "string"
    - name: "status"
      type: "string"
    - name: "address"
      type: "string"
    - name: "updated_at"
      type: "timestamp"
    - name: "valid_from"
      type: "timestamp"
      generated: true
    - name: "valid_to"
      type: "timestamp"
      generated: true
      nullable: true
    - name: "is_current"
      type: "boolean"
      generated: true

materialization:
  strategy: "scd2"
  scd2:
    timestamp_field: "updated_at"
    start_date_field: "valid_from"
    end_date_field: "valid_to"
    current_flag_field: "is_current"
    end_date_default: "9999-12-31"
    track_columns: ["email", "status", "address"]
```

### Use Case 10: Marketing Data with Compliance (GDPR + EU AI Act)

```yaml
version: 1.0.0
info:
  title: "Bronze Marketing Events"
  table_name: "{bronze_layer}_{system}_events"
  target_layer: "bronze"
  domain: "marketing"
  system: "klaviyo"

source:
  type: "landing"
  path: "s3://landing/klaviyo/events/"
  load_mode: "incremental"
  watermark_strategy: "manifest"

model:
  fields:
    - name: "event_id"
      type: "string"
      required: true
    - name: "email"
      type: "string"
      pii: true
      pii_classification: "email_address"
    - name: "phone_number"
      type: "string"
      pii: true
      pii_classification: "phone_number"
    - name: "event_name"
      type: "string"
      required: true
    - name: "timestamp"
      type: "timestamp"
      required: true

downstream:
  - name: "Marketing Engagement Dashboard"
    type: "dashboard"
    platform: "Tableau"
    owner: "Marketing Team"

lineage:
  enabled: true
  upstream:
    - name: "klaviyo_api"
      type: "external_api"

compliance:
  gdpr:
    applicable: true
    legal_basis: "legitimate_interest"
    purpose: "Marketing campaign analytics"
    retention_period: "24 months"
    consent_type: "opt_in"
    dpia_required: false
    cross_border_transfer: true
    transfer_mechanism: "SCCs"
    shared_with:
      - name: "Klaviyo Inc."
        role: "processor"
        country: "US"
        agreement: "DPA"
  eu_ai_act:
    applicable: true
    risk_tier: "limited"
    ai_system_purpose: "Marketing automation"
    training_data_provenance: true
    human_oversight: true
  ccpa:
    applicable: true
    status: "coming_soon"
```

---

## `_system.yaml` Placeholders & Layer Aliases

Contracts support placeholder variables resolved from the domain registry's `_system.yaml`.
This keeps paths DRY and portable across environments.

### Available Placeholders

```yaml
# _system.yaml — domain-level configuration
domain: marketing
system: google_analytics

# Configurable layer aliases (default values shown)
bronze_layer: bronze           # → {bronze_layer} in contracts
silver_layer: silver           # → {silver_layer}
gold_layer: gold               # → {gold_layer}

# Storage paths
bronze_path: "abfss://bronze@acct.dfs.core.windows.net"
silver_path: "abfss://silver@acct.dfs.core.windows.net"
gold_path: "abfss://gold@acct.dfs.core.windows.net"
landing_root: "abfss://landing@acct.dfs.core.windows.net"
log_root: "abfss://logs@acct.dfs.core.windows.net"

# Catalog
domain_catalog: "retail_marketing"
```

### Placeholder Usage in Contracts

```yaml
source:
  path: "{landing_root}/events"

materialization:
  target_path: "{bronze_path}/{bronze_layer}_{system}_events"
  location: "{bronze_path}/{bronze_layer}/{domain}/{system}/events"

quarantine:
  target: "{bronze_path}/{bronze_layer}_{system}_events_quarantine"

metadata:
  run_log_dir: "{log_root}/runs/{domain}/{system}/{bronze_layer}_events"
  run_log_table: "{domain_catalog}._run_logs"
```

### Layer Alias Flexibility

Override layer names per domain — useful for migrating naming conventions:

```yaml
# _system.yaml
bronze_layer: raw        # {bronze_layer} → "raw" everywhere
silver_layer: cleansed    # {silver_layer} → "cleansed"
gold_layer: curated       # {gold_layer} → "curated"
```

### Run Log Backend Selection

```yaml
# Metadata in contract or _system.yaml defaults
metadata:
  # Auto-detected from engine:
  #   spark  → Spark Delta table (Unity Catalog)
  #   polars → Delta table via delta-rs (cloud or local)
  #   duckdb → DuckDB file
  run_log_table: "{domain_catalog}._run_logs"

  # Explicit override:
  # run_log_backend: delta    # spark | delta | duckdb | sqlite
```

### Pipeline Runtime Parameters

```python
from lakelogic.pipeline import LakehousePipeline

pipeline = LakehousePipeline(registry)

# Daily run — uses contract defaults (lookback_days: 3)
pipeline.run()

# Override lookback at runtime
pipeline.run(lookback_days=30)

# Backfill — auto-scopes partition scan to date range
pipeline.run(reprocess_from="2026-01-01", reprocess_to="2026-03-22")
```

---

## _system.yaml: External Sources (Cross-Domain Lineage)

When your domain consumes tables managed by **another** domain's pipeline,
declare them as `external_sources` so the DAG shows the full lineage:

```yaml
# In _system.yaml
external_sources:
  - name: "silver_ga4_sessions"
    catalog_path: "catalog.silver.ga4_sessions"
    source_domain: "marketing/google_analytics"
    consumed_by: ["gold_marketing_funnel", "gold_attribution"]
    # consumed_by: entity names of contracts in THIS registry that read this table

  - name: "silver_crm_customers"
    catalog_path: "catalog.silver.crm_customers"
    source_domain: "sales/crm"
    consumed_by: ["gold_customer_360"]
```

**What this does:**

- 🌐 External nodes appear in the DAG with dashed borders (teal) in an "EXTERNAL" column
- Edges connect external sources → consuming contracts. Generic entity matches (e.g. `['events']`) automatically restrict to the Bronze layer to prevent duplicates, but you can explicitly map to other layers via ID (e.g. `['silver_events']`).
- Metadata-only — LakeLogic does **not** orchestrate the external pipeline
- Late-arriving data in the external source is picked up automatically if
  consuming contracts use `watermark_field: _lakelogic_processed_at`

---

## Contract YAML: Downstream Consumers (DAG Lineage)

When a contract is consumed by reports, dashboards, APIs, or external teams, declare them at the top level of the contract file (`downstream:`) to visualize them in the DAG:

```yaml
# In cross-domain or Gold contracts
downstream:
  - name: "Executive KPI Dashboard"
    type: "dashboard"    # Icons: dashboard (📊), api (🔌), report (📈), table (📋)
    platform: "PowerBI"
    owner: "Executive Team"
    
  - name: "Churn Prediction Model"
    type: "api"
    platform: "Databricks Model Serving"
    owner: "Data Science"
```

**What this does:**
- 🌐 Creates purple `DOWNSTREAM` nodes at the very end of your DAG hierarchy.
- Edges connect the current contract → the defined business consumers.
- Helps teams immediately identify which critical business assets will be impacted by upstream schema drift or freshness delays.

---


## SCD1 (Merge) Notes

SCD1 uses `strategy: merge` with the contract-level `primary_key` for the merge ON condition:

```yaml
primary_key: [customer_id]

materialization:
  strategy: merge
  # Merge uses primary_key to match rows (NOT incremental_key)
  # incremental_key is only used for source filtering (what's new?)
```

**Key behaviors:**
- **Matched rows** → UPDATE all non-key columns
- **Unmatched incoming rows** → INSERT
- **Unmatched existing rows** → no change (kept as-is)
- **`_lakelogic_processed_at`** → updated on every merge (serves as "last modified")
- **`_lakelogic_created_at`** → immutable (preserves first-insert time)

---

## Quick Reference: When to Use What

| Feature | Bronze | Silver | Gold |
| ------- | ------ | ------ | ---- |
| `tier` | `bronze` | `silver` | `gold` |
| `server.mode` | `ingest` | `validate` | `validate` |
| `server.cast_to_string` | `true` | `false` | `false` |
| `server.schema_evolution` | `append` | `strict` | `strict` |
| `source.partition` | Recommended | N/A | N/A |
| `source.partition.lookback_days` | 1-7 | N/A | N/A |
| `source.flatten_nested` | N/A | `true` / `[cols]` | `true` / `[cols]` |
| `quality.enforce_required` | `false` | `true` | `true` |
| `quality.row_rules` | Minimal | Full | Minimal |
| `quality.dataset_rules` | None | Yes | Yes |
| `materialization.strategy` | `append` | `merge`/`scd2` | `overwrite` |
| `lineage.enabled` | `true` | `true` | `true` |
| `metadata.run_log_table` | Optional | Optional | Optional |
| `downstream` | N/A | Optional | Recommended |
| `external_sources` | N/A | N/A | Via `_system.yaml` |
| `extraction` | Optional | N/A | N/A |
| `cloud.enabled` | Optional | Optional | Optional |
| `external_logic` | N/A | N/A | Optional |
| `compliance` | Recommended | Recommended | Optional |

---

*For more examples, see the [LakeLogic Examples](https://github.com/lakelogic/LakeLogic/tree/main/examples) directory.*
