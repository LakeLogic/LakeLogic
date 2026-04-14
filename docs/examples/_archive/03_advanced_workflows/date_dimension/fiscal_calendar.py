"""
Fiscal Calendar Examples — Demonstrate fiscal year alignment.

Shows how to generate date dimensions with different fiscal year
start months used across industries:
  - UK Government / NHS: April (month 4)
  - US Federal Government: October (month 10)
  - Retail (4-4-5 calendar): February (month 2)
  - Standard Calendar Year: January (month 1)
"""
from lakelogic.core.dim_date import generate_date_dimension

# ── April Fiscal Year (UK Government, NHS, many UK companies) ────────────────

print("=" * 70)
print("APRIL FISCAL YEAR (UK Government)")
print("=" * 70)

df_apr = generate_date_dimension(
    start_date="2024-01-01",
    end_date="2025-03-31",
    fiscal_year_start_month=4,
    holiday_calendar="uk",
    engine="polars",
)

# Key dates to check
import polars as pl

check_dates = [20240115, 20240415, 20240715, 20241015, 20250115]
for dk in check_dates:
    row = df_apr.filter(df_apr["date_key"] == dk)
    if row.height > 0:
        print(f"  {row['full_date'][0]}  →  FY{row['fiscal_year'][0]}  "
              f"Q{row['fiscal_quarter'][0]}  ({row['fiscal_year_quarter'][0]})")

print()
print("  Key: Jan 2024 = FY2023-Q4, Apr 2024 = FY2024-Q1")
print()

# ── October Fiscal Year (US Federal Government) ─────────────────────────────

print("=" * 70)
print("OCTOBER FISCAL YEAR (US Federal Government)")
print("=" * 70)

df_oct = generate_date_dimension(
    start_date="2024-01-01",
    end_date="2025-09-30",
    fiscal_year_start_month=10,
    holiday_calendar="us",
    engine="polars",
)

check_dates_oct = [20240115, 20240415, 20241015, 20250115]
for dk in check_dates_oct:
    row = df_oct.filter(df_oct["date_key"] == dk)
    if row.height > 0:
        print(f"  {row['full_date'][0]}  →  FY{row['fiscal_year'][0]}  "
              f"Q{row['fiscal_quarter'][0]}  ({row['fiscal_year_quarter'][0]})")

print()
print("  Key: Jan 2024 = FY2023-Q2, Oct 2024 = FY2024-Q1")
print()

# ── Calendar Year (Default) ─────────────────────────────────────────────────

print("=" * 70)
print("CALENDAR YEAR (Default, fiscal_year_start_month=1)")
print("=" * 70)

df_cal = generate_date_dimension(
    start_date="2024-01-01",
    end_date="2024-12-31",
    fiscal_year_start_month=1,  # Default
    engine="polars",
)

check_dates_cal = [20240115, 20240415, 20240715, 20241015]
for dk in check_dates_cal:
    row = df_cal.filter(df_cal["date_key"] == dk)
    if row.height > 0:
        print(f"  {row['full_date'][0]}  →  FY{row['fiscal_year'][0]}  "
              f"Q{row['fiscal_quarter'][0]}  ({row['fiscal_year_quarter'][0]})")

print()
print("  Key: Fiscal year = Calendar year, Q1 = Jan–Mar")
print()
print("✅ Fiscal calendar examples complete.")
