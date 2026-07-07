"""
Date dimension generator for LakeLogic.

Generates fully-featured date dimension tables for lakehouses,
driven by contract configuration or standalone parameters.

Features:
  - Date range generation with customizable start/end
  - Fiscal calendar support (custom fiscal year start month)
  - Holiday awareness (built-in + custom holidays)
  - ISO week numbering
  - Quarter, month, day-of-week metadata
  - Weekend/business day flags
  - Relative date flags (is_today, is_current_month, etc.)

Usage (Python API):
    from lakelogic.core.dim_date import generate_date_dimension

    # Polars DataFrame
    df = generate_date_dimension(
        start_date="2020-01-01",
        end_date="2030-12-31",
        fiscal_year_start_month=4,  # April fiscal year
        engine="polars",
    )

    # DuckDB (directly creates the table)
    generate_date_dimension(
        start_date="2020-01-01",
        end_date="2030-12-31",
        engine="duckdb",
        db_path="warehouse.duckdb",
        table_name="dim_date",
    )

Usage via DataProcessor:
    proc = DataProcessor("contract.yaml")
    df = proc.generate_date_dimension(start_date="2020-01-01", end_date="2030-12-31")
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Dict, List, Optional

from loguru import logger


# ── Built-in US Federal Holidays (recurring rules) ──────────────────────────
# These are approximations for common holidays. Users can override with custom lists.

_MONTH_NAMES = [
    "",
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
]

_DAY_NAMES = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]

_DAY_ABBREVS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _easter_date(year: int) -> date:
    """Compute Easter Sunday using the Anonymous Gregorian algorithm."""
    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    l_val = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l_val) // 451
    month = (h + l_val - 7 * m + 114) // 31
    day = ((h + l_val - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _nth_weekday(year: int, month: int, weekday: int, n: int) -> date:
    """
    Get the nth occurrence of a weekday in a given month.
    weekday: 0=Monday, 6=Sunday
    n: 1-based (1=first, 2=second, etc.)
    """
    first_day = date(year, month, 1)
    # Days until the first occurrence of the target weekday
    days_ahead = weekday - first_day.weekday()
    if days_ahead < 0:
        days_ahead += 7
    first_occurrence = first_day + timedelta(days=days_ahead)
    return first_occurrence + timedelta(weeks=n - 1)


def _last_weekday(year: int, month: int, weekday: int) -> date:
    """Get the last occurrence of a weekday in a given month."""
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    last_day = next_month - timedelta(days=1)
    days_back = (last_day.weekday() - weekday) % 7
    return last_day - timedelta(days=days_back)


def get_us_federal_holidays(year: int) -> Dict[date, str]:
    """
    Get US Federal holidays for a given year.

    Returns:
        Dict mapping date to holiday name.
    """
    holidays = {}

    # New Year's Day - January 1
    holidays[date(year, 1, 1)] = "New Year's Day"

    # MLK Day - 3rd Monday of January
    holidays[_nth_weekday(year, 1, 0, 3)] = "Martin Luther King Jr. Day"

    # Presidents' Day - 3rd Monday of February
    holidays[_nth_weekday(year, 2, 0, 3)] = "Presidents' Day"

    # Memorial Day - Last Monday of May
    holidays[_last_weekday(year, 5, 0)] = "Memorial Day"

    # Juneteenth - June 19
    holidays[date(year, 6, 19)] = "Juneteenth"

    # Independence Day - July 4
    holidays[date(year, 7, 4)] = "Independence Day"

    # Labor Day - 1st Monday of September
    holidays[_nth_weekday(year, 9, 0, 1)] = "Labor Day"

    # Columbus Day - 2nd Monday of October
    holidays[_nth_weekday(year, 10, 0, 2)] = "Columbus Day"

    # Veterans Day - November 11
    holidays[date(year, 11, 11)] = "Veterans Day"

    # Thanksgiving - 4th Thursday of November
    holidays[_nth_weekday(year, 11, 3, 4)] = "Thanksgiving"

    # Christmas Day - December 25
    holidays[date(year, 12, 25)] = "Christmas Day"

    return holidays


def get_uk_bank_holidays(year: int) -> Dict[date, str]:
    """Get UK bank holidays for a given year."""
    holidays = {}

    holidays[date(year, 1, 1)] = "New Year's Day"

    # Good Friday (2 days before Easter)
    easter = _easter_date(year)
    holidays[easter - timedelta(days=2)] = "Good Friday"

    # Easter Monday
    holidays[easter + timedelta(days=1)] = "Easter Monday"

    # Early May bank holiday - 1st Monday of May
    holidays[_nth_weekday(year, 5, 0, 1)] = "Early May Bank Holiday"

    # Spring bank holiday - Last Monday of May
    holidays[_last_weekday(year, 5, 0)] = "Spring Bank Holiday"

    # Summer bank holiday - Last Monday of August
    holidays[_last_weekday(year, 8, 0)] = "Summer Bank Holiday"

    # Christmas Day
    holidays[date(year, 12, 25)] = "Christmas Day"

    # Boxing Day
    holidays[date(year, 12, 26)] = "Boxing Day"

    return holidays


# ── Core Generator ───────────────────────────────────────────────────────────


def _generate_date_records(
    start_date: date,
    end_date: date,
    fiscal_year_start_month: int = 1,
    holidays: Optional[Dict[date, str]] = None,
    include_relative_flags: bool = False,
) -> List[Dict[str, Any]]:
    """
    Generate raw date dimension records as a list of dicts.

    Args:
        start_date: First date in the dimension.
        end_date: Last date in the dimension.
        fiscal_year_start_month: Month when fiscal year starts (1-12).
        holidays: Dict mapping dates to holiday names.
        include_relative_flags: Include is_today, is_current_month, etc.

    Returns:
        List of dictionaries, one per date.
    """
    if holidays is None:
        holidays = {}

    today = date.today()
    records = []
    current = start_date

    while current <= end_date:
        year = current.year
        month = current.month
        day = current.day
        weekday = current.weekday()  # 0=Monday

        # Date key (YYYYMMDD integer)
        date_key = year * 10000 + month * 100 + day

        # ISO calendar
        iso_year, iso_week, iso_weekday = current.isocalendar()

        # Quarter
        quarter = (month - 1) // 3 + 1

        # Fiscal year / quarter
        if fiscal_year_start_month == 1:
            fiscal_year = year
            fiscal_month = month
        else:
            if month >= fiscal_year_start_month:
                fiscal_year = year
                fiscal_month = month - fiscal_year_start_month + 1
            else:
                fiscal_year = year - 1
                fiscal_month = month + (12 - fiscal_year_start_month + 1)
        fiscal_quarter = (fiscal_month - 1) // 3 + 1

        # Day of year
        day_of_year = (current - date(year, 1, 1)).days + 1

        # Weekend / business day
        is_weekend = weekday >= 5
        holiday_name = holidays.get(current)
        is_holiday = holiday_name is not None
        is_business_day = not is_weekend and not is_holiday

        # Month start/end
        if month == 12:
            next_month_first = date(year + 1, 1, 1)
        else:
            next_month_first = date(year, month + 1, 1)
        month_end = next_month_first - timedelta(days=1)
        is_month_start = day == 1
        is_month_end = current == month_end

        # Quarter start/end
        quarter_start_month = (quarter - 1) * 3 + 1
        is_quarter_start = month == quarter_start_month and day == 1
        quarter_end_month = quarter * 3
        if quarter_end_month == 12:
            quarter_end_date = date(year, 12, 31)
        else:
            quarter_end_date = date(year, quarter_end_month + 1, 1) - timedelta(days=1)
        is_quarter_end = current == quarter_end_date

        # Year start/end
        is_year_start = month == 1 and day == 1
        is_year_end = month == 12 and day == 31

        record = {
            "date_key": date_key,
            "full_date": current.isoformat(),
            "year": year,
            "month": month,
            "day": day,
            "day_of_week": weekday,
            "day_of_year": day_of_year,
            "day_name": _DAY_NAMES[weekday],
            "day_abbrev": _DAY_ABBREVS[weekday],
            "month_name": _MONTH_NAMES[month],
            "month_abbrev": _MONTH_NAMES[month][:3],
            "quarter": quarter,
            "year_quarter": f"{year}-Q{quarter}",
            "year_month": f"{year}-{month:02d}",
            "iso_year": iso_year,
            "iso_week": iso_week,
            "iso_weekday": iso_weekday,
            "is_weekend": is_weekend,
            "is_business_day": is_business_day,
            "is_holiday": is_holiday,
            "holiday_name": holiday_name,
            "is_month_start": is_month_start,
            "is_month_end": is_month_end,
            "is_quarter_start": is_quarter_start,
            "is_quarter_end": is_quarter_end,
            "is_year_start": is_year_start,
            "is_year_end": is_year_end,
            "fiscal_year": fiscal_year,
            "fiscal_quarter": fiscal_quarter,
            "fiscal_month": fiscal_month,
            "fiscal_year_quarter": f"FY{fiscal_year}-Q{fiscal_quarter}",
        }

        if include_relative_flags:
            record["is_today"] = current == today
            record["is_current_month"] = year == today.year and month == today.month
            record["is_current_quarter"] = year == today.year and quarter == ((today.month - 1) // 3 + 1)
            record["is_current_year"] = year == today.year
            record["days_from_today"] = (current - today).days

        records.append(record)
        current += timedelta(days=1)

    return records


def generate_date_dimension(
    start_date: str = "2020-01-01",
    end_date: str = "2030-12-31",
    *,
    fiscal_year_start_month: int = 1,
    holiday_calendar: str = "us",
    custom_holidays: Optional[Dict[str, str]] = None,
    include_relative_flags: bool = False,
    engine: str = "polars",
    table_name: Optional[str] = None,
    db_path: Optional[str] = None,
    connection: Any = None,
) -> Any:
    """
    Generate a date dimension table.

    Args:
        start_date: Start date (ISO format string).
        end_date: End date (ISO format string).
        fiscal_year_start_month: Month when fiscal year starts (1=Jan, 4=Apr, 7=Jul, 10=Oct).
        holiday_calendar: Built-in holiday calendar ("us", "uk", "none").
        custom_holidays: Dict of "YYYY-MM-DD" → "Holiday Name" to add/override.
        include_relative_flags: Include dynamic flags like is_today.
        engine: Output engine ("polars", "pandas", "duckdb").
        table_name: For DuckDB, create the table with this name.
        db_path: For DuckDB, database path.
        connection: Existing database connection (DuckDB/SQLite).

    Returns:
        DataFrame (Polars or Pandas) or DuckDB table name.
    """
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    if start > end:
        raise ValueError(f"start_date ({start_date}) must be before end_date ({end_date}).")

    if fiscal_year_start_month < 1 or fiscal_year_start_month > 12:
        raise ValueError(f"fiscal_year_start_month must be 1-12, got {fiscal_year_start_month}.")

    # Build holiday lookup
    holidays: Dict[date, str] = {}
    years = range(start.year, end.year + 1)

    if holiday_calendar.lower() == "us":
        for y in years:
            holidays.update(get_us_federal_holidays(y))
    elif holiday_calendar.lower() == "uk":
        for y in years:
            holidays.update(get_uk_bank_holidays(y))
    elif holiday_calendar.lower() != "none":
        logger.warning(f"Unknown holiday calendar '{holiday_calendar}', using 'none'.")

    # Merge custom holidays
    if custom_holidays:
        for date_str, name in custom_holidays.items():
            holidays[date.fromisoformat(date_str)] = name

    records = _generate_date_records(
        start,
        end,
        fiscal_year_start_month=fiscal_year_start_month,
        holidays=holidays,
        include_relative_flags=include_relative_flags,
    )

    total_days = len(records)
    logger.info(
        f"Generated date dimension: {total_days} days "
        f"({start_date} to {end_date}), "
        f"fiscal_start=month {fiscal_year_start_month}, "
        f"holidays={holiday_calendar}"
    )

    engine = engine.lower()

    if engine == "polars":
        import polars as pl

        df = pl.DataFrame(records)
        # Cast types for better storage
        df = df.with_columns(
            [
                pl.col("full_date").str.to_date().alias("full_date"),
                pl.col("is_weekend").cast(pl.Boolean),
                pl.col("is_business_day").cast(pl.Boolean),
                pl.col("is_holiday").cast(pl.Boolean),
                pl.col("is_month_start").cast(pl.Boolean),
                pl.col("is_month_end").cast(pl.Boolean),
                pl.col("is_quarter_start").cast(pl.Boolean),
                pl.col("is_quarter_end").cast(pl.Boolean),
                pl.col("is_year_start").cast(pl.Boolean),
                pl.col("is_year_end").cast(pl.Boolean),
            ]
        )
        if include_relative_flags:
            df = df.with_columns(
                [
                    pl.col("is_today").cast(pl.Boolean),
                    pl.col("is_current_month").cast(pl.Boolean),
                    pl.col("is_current_quarter").cast(pl.Boolean),
                    pl.col("is_current_year").cast(pl.Boolean),
                ]
            )
        return df

    elif engine == "pandas":
        import pandas as pd

        df = pd.DataFrame(records)
        df["full_date"] = pd.to_datetime(df["full_date"]).dt.date
        return df

    elif engine == "duckdb":
        import duckdb
        import pandas as pd

        pdf = pd.DataFrame(records)
        pdf["full_date"] = pd.to_datetime(pdf["full_date"]).dt.date

        con = connection or duckdb.connect(database=str(db_path or ":memory:"))
        resolved_table = table_name or "dim_date"

        try:
            con.execute(f"DROP TABLE IF EXISTS {resolved_table}")
            con.execute(f"CREATE TABLE {resolved_table} AS SELECT * FROM pdf")
            logger.info(f"Created DuckDB table '{resolved_table}' with {total_days} rows.")
        finally:
            if not connection:
                con.close()

        return resolved_table

    else:
        raise ValueError(f"Unsupported engine: {engine}. Use 'polars', 'pandas', or 'duckdb'.")
