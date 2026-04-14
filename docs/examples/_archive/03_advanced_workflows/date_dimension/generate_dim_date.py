"""
Date Dimension Generator — Build production-ready dim_date tables.

This example shows how to:
  1. Generate a date dimension with US Federal holidays
  2. Customise fiscal year start month
  3. Add custom holidays
  4. Output to Polars, Pandas, and DuckDB
  5. Inspect key columns
"""
from lakelogic.core.processor import DataProcessor
from lakelogic.core.dim_date import generate_date_dimension

# ── 1. Basic Date Dimension (US Calendar, Calendar Year) ─────────────────────

print("=" * 70)
print("1. BASIC DATE DIMENSION (2024, US Holidays)")
print("=" * 70)

df = generate_date_dimension(
    start_date="2024-01-01",
    end_date="2024-12-31",
    holiday_calendar="us",
    engine="polars",
)

print(f"  Total rows: {df.height}")
print(f"  Columns: {len(df.columns)}")
print(f"  Column names: {df.columns}")
print()

# Show first 5 rows
print("First 5 days:")
print(df.head(5))
print()

# ── 2. Holiday Summary ──────────────────────────────────────────────────────

print("=" * 70)
print("2. US FEDERAL HOLIDAYS IN 2024")
print("=" * 70)

holidays = df.filter(df["is_holiday"] == True).select(
    "full_date", "day_name", "holiday_name"
)
print(holidays)
print()

# ── 3. Weekend / Business Day Distribution ───────────────────────────────────

print("=" * 70)
print("3. BUSINESS DAY DISTRIBUTION")
print("=" * 70)

total = df.height
weekends = df.filter(df["is_weekend"] == True).height
holidays_count = df.filter(df["is_holiday"] == True).height
business_days = df.filter(df["is_business_day"] == True).height

print(f"  Total days:    {total}")
print(f"  Weekends:      {weekends}")
print(f"  Holidays:      {holidays_count}")
print(f"  Business days: {business_days}")
print()

# ── 4. Quarter Boundaries ───────────────────────────────────────────────────

print("=" * 70)
print("4. QUARTER START/END DATES")
print("=" * 70)

quarter_starts = df.filter(df["is_quarter_start"] == True).select(
    "full_date", "quarter", "year_quarter"
)
print("Quarter starts:")
print(quarter_starts)

quarter_ends = df.filter(df["is_quarter_end"] == True).select(
    "full_date", "quarter", "year_quarter"
)
print("Quarter ends:")
print(quarter_ends)
print()

# ── 5. Custom Holidays ──────────────────────────────────────────────────────

print("=" * 70)
print("5. CUSTOM HOLIDAYS")
print("=" * 70)

df_custom = generate_date_dimension(
    start_date="2024-01-01",
    end_date="2024-12-31",
    holiday_calendar="us",
    custom_holidays={
        "2024-03-17": "St Patrick's Day",
        "2024-10-31": "Halloween",
        "2024-02-14": "Valentine's Day",
        "2024-04-01": "April Fool's Day",
    },
    engine="polars",
)

all_holidays = df_custom.filter(df_custom["is_holiday"] == True).select(
    "full_date", "holiday_name"
).sort("full_date")
print(all_holidays)
print()

# ── 6. UK Calendar ──────────────────────────────────────────────────────────

print("=" * 70)
print("6. UK BANK HOLIDAYS IN 2024")
print("=" * 70)

df_uk = generate_date_dimension(
    start_date="2024-01-01",
    end_date="2024-12-31",
    holiday_calendar="uk",
    engine="polars",
)

uk_holidays = df_uk.filter(df_uk["is_holiday"] == True).select(
    "full_date", "day_name", "holiday_name"
)
print(uk_holidays)
print()

# ── 7. DuckDB Output ────────────────────────────────────────────────────────

print("=" * 70)
print("7. DUCKDB TABLE OUTPUT")
print("=" * 70)

import duckdb
con = duckdb.connect()

table = generate_date_dimension(
    start_date="2020-01-01",
    end_date="2030-12-31",
    engine="duckdb",
    table_name="dim_date",
    connection=con,
)

count = con.execute("SELECT count(*) FROM dim_date").fetchone()[0]
print(f"  Created table: {table}")
print(f"  Total rows: {count}")

# Query example: business days per quarter in 2024
result = con.execute("""
    SELECT year_quarter, count(*) as business_days
    FROM dim_date
    WHERE year = 2024 AND is_business_day = true
    GROUP BY year_quarter
    ORDER BY year_quarter
""").fetchdf()
print("\n  Business days per quarter (2024):")
print(result.to_string(index=False))
con.close()
print()

# ── 8. Via DataProcessor (static method) ─────────────────────────────────────

print("=" * 70)
print("8. VIA DATAPROCESSOR")
print("=" * 70)

df_via_proc = DataProcessor.generate_date_dimension(
    start_date="2025-01-01",
    end_date="2025-03-31",
    fiscal_year_start_month=4,
    engine="polars",
)
print(f"  Generated {df_via_proc.height} rows via DataProcessor.generate_date_dimension()")
print(f"  Fiscal year for Jan 2025: FY{df_via_proc['fiscal_year'][0]}")
print()
print("✅ Date dimension generated successfully.")
