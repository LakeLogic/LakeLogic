"""
PII Masking — Create anonymised datasets for dev/test environments.

This example shows how to:
  1. Mask all PII columns across all rows (not just specific subjects)
  2. Use hash strategy to preserve referential integrity
  3. Generate a safe dataset for development without real PII
"""
from pathlib import Path
import polars as pl

from lakelogic.core.processor import DataProcessor

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

# ── Strategy 1: Nullify All PII ──────────────────────────────────────────────
# Use when PII is not needed at all in the target environment.

print("=" * 70)
print("NULLIFIED (all PII removed)")
print("=" * 70)
nullified = proc.mask_pii(df, strategy="nullify")
print(nullified)
print()

# ── Strategy 2: Hash All PII ─────────────────────────────────────────────────
# Use when you need to preserve referential integrity across tables.
# e.g., hashed customer_email can still be JOINed between orders and customers
# as long as both tables use the same salt.

print("=" * 70)
print("HASHED (preserves referential integrity)")
print("=" * 70)
hashed = proc.mask_pii(df, strategy="hash", hash_salt="dev-env-2024")
print(hashed)
print()

# Verify: same email hashes to same value (important for JOINs)
print("Referential integrity check:")
dup_df = pl.DataFrame({
    "customer_id": ["c1", "c2", "c1"],
    "email": ["alice@example.com", "bob@example.com", "alice@example.com"],
    "full_name": ["Alice", "Bob", "Alice"],
    "phone": ["111", "222", "111"],
    "date_of_birth": ["1985-01-01", "1990-01-01", "1985-01-01"],
    "shipping_address": ["Addr1", "Addr2", "Addr1"],
})
hashed_dup = proc.mask_pii(dup_df, strategy="hash", hash_salt="dev-env-2024")
print(f"  email[0] == email[2]: {hashed_dup['email'][0] == hashed_dup['email'][2]}")
print(f"  email[0] == email[1]: {hashed_dup['email'][0] == hashed_dup['email'][1]}")
print()

# ── Strategy 3: Redact All PII ───────────────────────────────────────────────
# Use when you want a visible marker that PII was removed.

print("=" * 70)
print("REDACTED (visible marker)")
print("=" * 70)
redacted = proc.mask_pii(df, strategy="redact")
print(redacted)
print()

# ── Selective Masking ────────────────────────────────────────────────────────
# Mask only specific columns (override contract annotations)

print("=" * 70)
print("SELECTIVE MASKING (email + phone only)")
print("=" * 70)
selective = proc.mask_pii(df, columns=["email", "phone"])
print(selective)
print()
print("✅ Only email and phone are masked.")
print("✅ full_name, date_of_birth, shipping_address are preserved.")
