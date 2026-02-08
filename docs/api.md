# API Reference

## DataProcessor

```python
from lakelogic import DataProcessor

processor = DataProcessor(
    engine="polars",
    contract="contract.yaml",
    stage="silver",
)

good_df, bad_df = processor.run(df)
```

### Constructor

- `engine` (str): `polars`, `pandas`, `duckdb`, `spark`, `snowflake`, or `bigquery`
- `contract` (str | Path | dict | DataContract): YAML path or dict
- `stage` (str | None): Apply contract stage overrides (e.g., `bronze`, `silver`)

### run(df, source_path=None, materialize=False, materialize_target=None)

Executes the contract against a DataFrame.

Returns:
- `ValidationResult` object that unpacks to `(source_df, good_df, bad_df)`
- `source_df`: original input dataframe
- `good_df`: validated, transformed records
- `bad_df`: quarantined records with error reasons

### run_source(path)

Loads data using the engine's native reader and runs the contract in one step.

Returns:
- `ValidationResult` object that unpacks to `(source_df, good_df, bad_df)`
- `source_df`: original input dataframe
- `good_df`: validated, transformed records
- `bad_df`: quarantined records with error reasons

### materialize(good_df, bad_df=None, target_path=None)

Writes validated data to the configured materialization target. If `quarantine.target` is set, bad records are also written.

### last_report

After a run, `processor.last_report` contains a structured summary (counts, dataset rule outcomes, run id).

## DataContract

Use a YAML file or a dict. Key sections:

- `model.fields`: schema and types
- `schema_policy`: behavior for unknown fields
- `quality.row_rules`: row-level checks
- `quality.dataset_rules`: aggregate checks
- `transformations`: SQL transformation steps (preferred) with optional `phase` (`pre` or `post`)
- `transformations` structured helpers: rename, derive, lookup, filter, deduplicate, select, drop, cast, trim, lower, upper, coalesce, split, explode, map_values, join
- `links`: reference datasets (file path or table name)
- `links[].broadcast`: Spark-only hint to broadcast small lookup tables
- `source`: ingestion metadata for pipelines (`type`, `path`, `load_mode`, `pattern`, `watermark_field`, `cdc_op_field`, `cdc_delete_values`)
- `quarantine`: quarantine settings + notifications (supports file paths or `table:` targets)
- `lineage`: lineage injection settings (`enabled`, source path, timestamp, run id)
- `materialization`: append/merge/scd2/overwrite + partitioning (CSV/Parquet locally, Delta/Iceberg on Spark)
- `external_logic`: optional python/notebook hook for advanced processing (gold patterns)
- `metadata.run_log_path` or `metadata.run_log_dir`: write run logs to JSON
- `metadata.run_log_table`: append run logs to a table (Spark, DuckDB, or SQLite backend)
- `metadata.run_log_backend`: `spark` | `duckdb` | `sqlite`
- `metadata.run_log_database`: database path for DuckDB/SQLite backends
- `metadata.run_log_merge_on_run_id`: Spark-only idempotent merge by `run_id`
- `metadata.run_log_table_format`: Spark-only table format (default `delta`)
- `metadata.quarantine_table_backend`: `spark` | `duckdb` | `sqlite` | `snowflake` | `bigquery` (defaults to engine)
- `metadata.quarantine_table_database`: database path for DuckDB/SQLite backends
- `metadata.quarantine_format`: Quarantine file format (default `parquet`; Spark also supports `delta`, `iceberg`, `json`)
- `metadata.quarantine_table_format`: Spark-only table format (default `iceberg`)
- `metadata.quarantine_table_mode`: Spark-only write mode (default `append`)
- `metadata.snowflake_*`: Connection settings for Snowflake (`snowflake_account`, `snowflake_user`, `snowflake_password`, `snowflake_warehouse`, `snowflake_database`, `snowflake_schema`, `snowflake_role`)
- `metadata.bigquery_project`: Optional project override for BigQuery connections
- `metadata.source_table`: Optional default source table for warehouse adapters (or `snowflake_source_table`/`bigquery_source_table`)
- `server.schema_evolution`: strict/append/merge ingestion behavior
- `server.allow_schema_drift`: send drift alerts when false
- `server.cast_to_string`: ingest all columns as strings (Bronze pattern)
- `service_levels`: freshness/availability SLO scoring
- `quality` helpers: not_null, accepted_values, regex_match, range, unique, null_ratio, row_count_between, referential_integrity
- `stages`: optional stage override map (e.g., `stages.bronze`, `stages.silver`) to reuse one contract across layers

### external_logic
Run dedicated Python modules or notebooks for advanced processing.

- `external_logic.type`: `python` or `notebook`
- `external_logic.path`: Path to the script/notebook (relative to contract)
- `external_logic.entrypoint`: Python function name (default `run`)
- `external_logic.args`: Dict of parameters injected into the logic
- `external_logic.output_path`: Optional output file path to read back in
- `external_logic.output_format`: `csv` or `parquet` (optional)
- `external_logic.handles_output`: If true, skip built-in materialize
- `external_logic.kernel_name`: Notebook kernel override
- Notebook params include `lakelogic_input_path` and `lakelogic_input_format` for validated data access.
