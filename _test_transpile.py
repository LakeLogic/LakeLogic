import sqlglot

tests = [
    ("try_to_date(event_date, 'yyyyMMdd')", "spark", "duckdb"),
    ("timestamp_micros(event_timestamp)", "spark", "duckdb"),
    ("to_date(CAST(event_date AS STRING), 'yyyyMMdd')", "spark", "duckdb"),
]

for sql, read, write in tests:
    try:
        result = sqlglot.transpile(sql, read=read, write=write)
        print(f"  {read} -> {write}: {sql}")
        print(f"  Result: {result[0]}")
    except Exception as e:
        print(f"  {read} -> {write}: {sql}")
        print(f"  ERROR: {e}")
    print()
