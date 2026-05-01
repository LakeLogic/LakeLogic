import sys

sys.stdout.reconfigure(encoding="utf-8")
import nbformat

path = "07_dlt_prefect_pipeline.ipynb"
with open(path, "r", encoding="utf-8") as f:
    nb = nbformat.read(f, as_version=4)

new_cell_6 = '''os.makedirs('07_dlt_prefect_pipeline/contracts/weather', exist_ok=True)

latitude  = lat_widget.value
longitude = lon_widget.value

# ── Bronze: Raw ingestion from Open-Meteo ──────────────────────────────
with open('07_dlt_prefect_pipeline/contracts/weather/bronze.yaml', 'w') as f:
    f.write(f"""version: "1.0"

info:
  title: weather_forecast
  table_name: bronze_weather
  target_layer: bronze

dataset: bronze_weather

source:
  type: dlt
  dlt:
    base_url: https://api.open-meteo.com/v1
    endpoints:
      - name: forecast
        path: forecast
        params:
          latitude: {latitude}
          longitude: {longitude}
          current_weather: "true"

materialization:
  target_path: 07_dlt_prefect_pipeline/data/bronze_weather
  format: delta

  # Dual-write: also materialize to a database via dlt
  secondary_targets:
    - format: dlt
      dlt_destination: duckdb
      dlt_dataset_name: analytics
      table_name: weather_summary
""")

# ── Silver: Deduplicated merge ─────────────────────────────────────────
with open('07_dlt_prefect_pipeline/contracts/weather/silver.yaml', 'w') as f:
    f.write("""version: "1.0"

info:
  title: weather_forecast_cleaned
  table_name: silver_weather_cleaned
  target_layer: silver

dataset: silver_weather_cleaned
primary_key: [_dlt_id]

source:
  type: table
  path: 07_dlt_prefect_pipeline/data/bronze_weather

transformations:
  - derive:
      field: temperature_f
      sql: "current_weather__temperature * 9.0 / 5.0 + 32.0"

quality:
  row_rules:
    - name: valid_temperature
      sql: "current_weather__temperature BETWEEN -90 AND 60"
      severity: error
      description: "Temperature must be within physically plausible range"

materialization:
  strategy: merge
  target_path: 07_dlt_prefect_pipeline/data/silver_weather_cleaned
  format: delta

  # Dual-write: also materialize to a database via dlt
  secondary_targets:
    - format: dlt
      dlt_destination: duckdb
      dlt_dataset_name: analytics
      table_name: weather_summary
""")

# ── Gold: Analytics-ready ──────────────────────────────────────────────
with open('07_dlt_prefect_pipeline/contracts/weather/gold.yaml', 'w') as f:
    f.write("""version: "1.0"

info:
  title: weather_forecast
  table_name: gold_weather_summary
  target_layer: gold

dataset: gold_weather_summary
primary_key: [_dlt_id]

source:
  type: table
  path: 07_dlt_prefect_pipeline/data/silver_weather_cleaned

downstream:
  - type: dashboard
    name: Weather Monitoring Dashboard
    platform: grafana
    owner: platform-team
    refresh: "every 15 minutes"

  - type: api
    name: Weather Alerts Service
    platform: internal
    owner: ops-team

materialization:
  strategy: merge

  target_path: 07_dlt_prefect_pipeline/data/gold_weather_summary
  format: delta

  # Dual-write: also materialize to a database via dlt
  secondary_targets:
    - format: dlt
      dlt_destination: duckdb
      dlt_credentials: "duckdb_test_db.duckdb"
      dlt_dataset_name: analytics
      table_name: weather_summary
""")

print("✅ Weather contracts written: bronze.yaml, silver.yaml, gold.yaml")'''

nb.cells[6].source = new_cell_6

new_cell_14 = """# The Gold contract already has secondary_targets configured.
# The pipeline run above wrote to BOTH Delta (primary) and DuckDB (secondary) via dlt.
# Verify by querying the dlt-managed DuckDB database:

import duckdb
import glob
import os

# Find the dlt-created DuckDB file
db_path = "duckdb_test_db.duckdb"
if os.path.exists(db_path):
    conn = duckdb.connect(db_path, read_only=True)
    tables = conn.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema='analytics'").fetchall()
    print(f"✅ Dual-write verified! dlt created: {db_path}")
    print(f"   Tables in analytics schema: {[t[1] for t in tables]}")
    row_count = conn.execute("SELECT COUNT(*) FROM analytics.weather_summary").fetchone()[0]
    print(f"   Rows in weather_summary: {row_count}")
    conn.close()
else:
    print("No secondary dlt database found yet.")
    print("Re-run the pipeline cell above to trigger dual-write.")
    print("To target PostgreSQL, change dlt_destination and add dlt_credentials.")
"""

nb.cells[14].source = new_cell_14

with open(path, "w", encoding="utf-8") as f:
    nbformat.write(nb, f)

print("Updated cells 6 and 14 in notebook.")
