import sqlglot

tests = [
    ("try_to_date(CAST(event_date AS STRING), 'yyyyMMdd')", "spark", "duckdb"),
    ("timestamp_micros(event_timestamp)", "spark", "duckdb"),
]

for sql, read, write in tests:
    try:
        expr = sqlglot.parse_one(sql, read=read)
        result = expr.sql(dialect=write)
        print(f"Result: {result}")
    except Exception as e:
        print(f"ERROR: {e}")
