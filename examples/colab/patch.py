import json

with open(
    "C:\\_Personal\\_SaaS\\lakelogic\\examples\\colab\\07_dlt_prefect_pipeline.ipynb", "r", encoding="utf-8"
) as f:
    nb = json.load(f)

for cell in nb.get("cells", []):
    if cell.get("cell_type") == "code":
        source = cell.get("source", [])
        for i, line in enumerate(source):
            if "with open('07_dlt_prefect_pipeline/_registry.yaml', 'w')" in line:
                new_source = []
                new_source.append("with open('07_dlt_prefect_pipeline/_registry.yaml', 'w') as f:\n")
                new_source.append("    f.write('''\n")
                new_source.append("domain: marketplace\n")
                new_source.append("system: colab_demo\n")
                new_source.append("\n")
                new_source.append("# ── 1. Telemetry & Observability ──────────────────────────────────────────\n")
                new_source.append("# Both Run Logs and Quarantine records are dual-written to an analytics DB\n")
                new_source.append("# via dlt! Here we use DuckDB to simulate Postgres/Snowflake/BigQuery.\n")
                new_source.append("metadata:\n")
                new_source.append("  run_log_table: pipeline_runs\n")
                new_source.append("  run_log_backend: dlt\n")
                new_source.append("  dlt_destination: duckdb\n")
                new_source.append('  dlt_credentials: "07_dlt_prefect_pipeline/data/observability.duckdb"\n')
                new_source.append("  dlt_dataset_name: system_logs\n")
                new_source.append("\n")
                new_source.append("quarantine:\n")
                new_source.append("  enabled: true\n")
                new_source.append("  table: quarantine_records\n")
                new_source.append("  format: dlt\n")
                new_source.append("  dlt_destination: duckdb\n")
                new_source.append('  dlt_credentials: "07_dlt_prefect_pipeline/data/observability.duckdb"\n')
                new_source.append("  dlt_dataset_name: system_logs\n")
                new_source.append("\n")
                new_source.append("lineage:\n")
                new_source.append("  enabled: true\n")
                new_source.append("  timestamp_column_name: _lakelogic_loaded_at\n")
                new_source.append("\n")
                new_source.append("storage:\n")
                new_source.append('  external_location_root: "."\n')
                new_source.append("\n")
                new_source.append("external_sources:\n")
                new_source.append('  - name: "Weather (open-meteo) API"\n')
                new_source.append("    type: api\n")
                new_source.append("    consumed_by:\n")
                new_source.append('      - "bronze_weather"\n')
                new_source.append("\n")
                new_source.append("contracts:\n")
                new_source.append("  # -- Bronze (no dependencies - these are the entry points) --\n")
                new_source.append("  - entity: bronze_weather\n")
                new_source.append("    layer: bronze\n")
                new_source.append("    path: contracts/weather/bronze.yaml\n")
                new_source.append("\n")
                new_source.append("  # -- Silver (depends on Bronze) --\n")
                new_source.append("  - entity: silver_weather_cleaned\n")
                new_source.append("    layer: silver\n")
                new_source.append("    depends_on: [bronze_weather]\n")
                new_source.append("    path: contracts/weather/silver.yaml\n")
                new_source.append("\n")
                new_source.append("  # -- Gold (depends on Silver) --\n")
                new_source.append("  - entity: gold_weather_summary\n")
                new_source.append("    layer: gold\n")
                new_source.append("    depends_on: [silver_weather_cleaned]\n")
                new_source.append("    path: contracts/weather/gold.yaml\n")
                new_source.append("\n")
                new_source.append("''')\n")
                new_source.append("\n")
                new_source.append("# Load and resolve all contracts + paths\n")
                new_source.append(
                    "registry = DomainRegistry.from_yaml("
                    "'07_dlt_prefect_pipeline/_registry.yaml', storage_mode='direct')\n"
                )
                new_source.append("\n")
                new_source.append(
                    'print(f"✅ Registry loaded: {len(registry.contracts)} '
                    'contracts across {registry.domain}/{registry.system}")\n'
                )
                new_source.append("print(f\"   Lineage: {registry.lineage.get('enabled', False)}\")")
                cell["source"] = new_source
                break

    # Let's also fix the %pip lint error
    if cell.get("cell_type") == "markdown":
        for i, line in enumerate(cell.get("source", [])):
            if "`%pip install dlt" in line:
                cell["source"][i] = line.replace(
                    "`%pip install dlt", "`%pip install dlt"
                )  # wait, the lint says use `%pip install`.
            if "`!pip install dlt" in line:
                cell["source"][i] = line.replace("`!pip install dlt", "`%pip install dlt")

with open(
    "C:\\_Personal\\_SaaS\\lakelogic\\examples\\colab\\07_dlt_prefect_pipeline.ipynb", "w", encoding="utf-8"
) as f:
    json.dump(nb, f, indent=1)
