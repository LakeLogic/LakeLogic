"""
GDPR Right-to-be-Forgotten — Erase specific data subjects.

This example shows how to:
  1. Load customer data with PII-annotated fields
  2. Process a GDPR erasure request for specific customers
  3. Generate an audit report for compliance records
  4. Compare results across three erasure strategies
"""
from pathlib import Path
import polars as pl

from lakelogic.core.processor import DataProcessor
from lakelogic.core.gdpr import forget_subjects, generate_erasure_report

# ── Setup ────────────────────────────────────────────────────────────────────

CONTRACT = Path(__file__).parent / "customer_contract.yaml"
DATA     = Path(__file__).parent / "data" / "customers.csv"

proc = DataProcessor(str(CONTRACT), engine="polars")
df   = pl.read_csv(str(DATA))

print("=" * 70)
print("ORIGINAL DATA")
print("=" * 70)
print(df)
print()

# ── Scenario: Customer cust_003 requests full data erasure ───────────────────

subject_ids = ["cust_003"]

# Strategy 1: Nullify (default) — set PII to NULL
print("=" * 70)
print("STRATEGY 1: NULLIFY")
print("=" * 70)
nullified = proc.forget(df, "customer_id", subject_ids, erasure_strategy="nullify")
print(nullified)
print()

# Strategy 2: Hash — one-way SHA-256 (preserves JOIN-ability across tables)
print("=" * 70)
print("STRATEGY 2: HASH (with salt)")
print("=" * 70)
hashed = proc.forget(df, "customer_id", subject_ids,
                     erasure_strategy="hash", hash_salt="lakelogic-gdpr-2024")
print(hashed)
print()

# Strategy 3: Redact — visible marker for auditing
print("=" * 70)
print("STRATEGY 3: REDACT")
print("=" * 70)
redacted = proc.forget(df, "customer_id", subject_ids, erasure_strategy="redact")
print(redacted)
print()

# ── Generate Audit Report ────────────────────────────────────────────────────

report = generate_erasure_report(
    proc.contract,
    subject_column="customer_id",
    subject_ids=subject_ids,
    erasure_strategy="nullify",
    affected_rows=1,
)

print("=" * 70)
print("GDPR ERASURE AUDIT REPORT")
print("=" * 70)
for key, value in report.items():
    print(f"  {key}: {value}")
print()

# ── Multi-Subject Erasure ────────────────────────────────────────────────────
# Real-world: process a batch of GDPR requests at once

batch_ids = ["cust_001", "cust_004", "cust_005"]
print(f"Batch erasure for {len(batch_ids)} subjects...")
batch_result = proc.forget(df, "customer_id", batch_ids)
print(batch_result)
print()
print("✅ PII columns for erased subjects are now NULL.")
print("✅ Non-PII columns (loyalty_tier, total_spend) are preserved.")
print("✅ Non-targeted subjects (cust_002, cust_003) are untouched.")
