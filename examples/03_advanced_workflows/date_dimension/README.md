# Date Dimension Generator

Generate production-ready date dimension tables for your lakehouse,
with fiscal calendars, holiday awareness, and ISO week numbering.

## Components

- **generate_dim_date.py**: Generate date dimensions with multiple configurations.
- **fiscal_calendar.py**: April (UK) and October (US Federal) fiscal year examples.

## When to Use This Pattern

- **Star Schema Setup**: Every analytical warehouse needs a `dim_date`.
- **Fiscal Year Reporting**: Align dates to your organisation's fiscal calendar.
- **Holiday-Aware Analytics**: Flag business days vs. holidays for accurate KPIs.
- **Time Intelligence**: Pre-compute quarter, ISO week, and month metadata.

## Supported Configurations

| Feature | Options |
|---|---|
| Holiday Calendars | US Federal, UK Bank, custom, none |
| Fiscal Year Start | Any month (1–12) |
| Output Engines | Polars, Pandas, DuckDB |
| Relative Flags | is_today, is_current_month, days_from_today |
| Date Range | Any valid ISO date range |

## Generated Columns (35+)

```
date_key, full_date, year, month, day, day_of_week, day_of_year,
day_name, day_abbrev, month_name, month_abbrev, quarter, year_quarter,
year_month, iso_year, iso_week, iso_weekday, is_weekend, is_business_day,
is_holiday, holiday_name, is_month_start, is_month_end, is_quarter_start,
is_quarter_end, is_year_start, is_year_end, fiscal_year, fiscal_quarter,
fiscal_month, fiscal_year_quarter
```

## Prerequisites

- LakeLogic core installed.

## Next Steps

- Use the generated dim_date in JOIN-based transformations within your contracts.
- **07_production/** for scheduling nightly dim_date refreshes.
