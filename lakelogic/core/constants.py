"""
LakeLogic standard constants

Centralised constants for delete/erasure reasons, metadata column names,
and compliance values used across the platform.
"""

# ── Delete / Erasure Reason Constants ────────────────────────────────────────
# Used in the ``_lakelogic_delete_reason`` metadata column to distinguish
# *why* a record was soft-deleted or had PII erased.

# Source-system driven
DELETE_REASON_CDC_SOURCE = "cdc_source_delete"  # Source system sent a delete signal

# GDPR (EU General Data Protection Regulation)
DELETE_REASON_GDPR_ART17 = "gdpr_article_17"  # Right to erasure (right to be forgotten)
DELETE_REASON_GDPR_ART16 = "gdpr_article_16"  # Right to rectification
DELETE_REASON_GDPR_ART21 = "gdpr_article_21"  # Right to object (processing ceased)

# HIPAA (US Health Insurance Portability and Accountability Act)
DELETE_REASON_HIPAA_PHI = "hipaa_phi_erasure"  # PHI removal request

# CCPA (California Consumer Privacy Act)
DELETE_REASON_CCPA_DELETE = "ccpa_right_to_delete"  # Right to delete personal information
DELETE_REASON_CCPA_OPTOUT = "ccpa_right_to_opt_out"  # Right to opt out of sale of PI

# Operational
DELETE_REASON_DATA_STEWARD = "manual_data_correction"  # Manual fix by data steward
DELETE_REASON_RETENTION = "retention_policy_expired"  # Data retention period ended
DELETE_REASON_DUPLICATE = "duplicate_record_removal"  # Deduplication
DELETE_REASON_DATA_QUALITY = "data_quality_rejection"  # Failed quality threshold


# ── Metadata Column Names ────────────────────────────────────────────────────
# Standard column names stamped by LakeLogic during pipeline processing.
# These are auto-added to contracts with PII/PHI fields.

META_IS_DELETED = "_lakelogic_is_deleted"
META_DELETED_AT = "_lakelogic_deleted_at"
META_DELETE_REASON = "_lakelogic_delete_reason"
META_UPDATED_AT = "_lakelogic_updated_at"

# Lineage columns (already used in lineage.py / processor.py)
META_SOURCE = "_lakelogic_source"
META_PROCESSED_AT = "_lakelogic_processed_at"
META_LOADED_AT = "_lakelogic_loaded_at"
META_RUN_ID = "_lakelogic_run_id"
META_CONTRACT_NAME = "_lakelogic_contract_name"
META_DOMAIN = "_lakelogic_domain"
META_SYSTEM = "_lakelogic_system"
META_ERRORS = "_lakelogic_errors"
META_EXTRACTION_CONFIDENCE = "_lakelogic_extraction_confidence"


# ── Erasure Strategy Values ──────────────────────────────────────────────────
ERASURE_NULLIFY = "nullify"  # Set PII to NULL
ERASURE_HASH = "hash"  # One-way SHA-256 hash
ERASURE_REDACT = "redact"  # Replace with ***REDACTED***

VALID_ERASURE_STRATEGIES = (ERASURE_NULLIFY, ERASURE_HASH, ERASURE_REDACT)


# ── Compliance Regulation Tags ───────────────────────────────────────────────
# Used to tag contracts/entities with applicable regulations.
REGULATION_GDPR = "gdpr"
REGULATION_HIPAA = "hipaa"
REGULATION_CCPA = "ccpa"
REGULATION_SOX = "sox"
REGULATION_PCI_DSS = "pci_dss"


# ── System Table Schemas (Spark DDL) ─────────────────────────────────────────
# Used by the pipeline runner to bootstrap system tables in Unity Catalog.
# The runner generates CREATE TABLE IF NOT EXISTS ... (schema) USING DELTA.
# These must match the schemas written by run_log.py and the engine adapters.

SYSTEM_TABLE_SCHEMA_LOGS = (
    "  pipeline_run_id STRING,\n"
    "  run_id STRING,\n"
    "  timestamp STRING,\n"
    "  start_time STRING,\n"
    "  end_time STRING,\n"
    "  run_duration_seconds DOUBLE,\n"
    "  engine STRING,\n"
    "  contract STRING,\n"
    "  contract_version STRING,\n"
    "  stage STRING,\n"
    "  dataset STRING,\n"
    "  domain STRING,\n"
    "  system STRING,\n"
    "  environment STRING,\n"
    "  data_layer STRING,\n"
    "  status STRING,\n"
    "  error_message STRING,\n"
    "  error_traceback STRING,\n"
    "  lakelogic_version STRING,\n"
    "  source_path STRING,\n"
    "  counts_source BIGINT,\n"
    "  counts_total BIGINT,\n"
    "  counts_good BIGINT,\n"
    "  counts_quarantined BIGINT,\n"
    "  quarantine_ratio DOUBLE,\n"
    "  estimated_cost DOUBLE,\n"
    "  cost_currency STRING,\n"
    "  cost_confidence STRING,\n"
    "  max_source_mtime DOUBLE,\n"
    "  max_watermark_value STRING,\n"
    "  dlt_state_json STRING,\n"
    "  slo_json STRING,\n"
    "  report_json STRING"
)

SYSTEM_TABLE_SCHEMA_QUARANTINE = (
    "  _lakelogic_errors STRING,\n"
    "  _lakelogic_categories STRING,\n"
    "  quarantine_state STRING,\n"
    "  quarantine_reprocessed BOOLEAN,\n"
    "  _lakelogic_source STRING,\n"
    "  _lakelogic_run_id STRING,\n"
    "  _lakelogic_processed_at TIMESTAMP,\n"
    "  _lakelogic_entity STRING,\n"
    "  _lakelogic_layer STRING"
)
