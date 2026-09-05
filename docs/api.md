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

- `engine` (str): `polars`, `spark`, `snowflake`, or `bigquery`
- `contract` (str | Path | dict | DataContract): YAML path or dict
- `stage` (str | None): Apply contract stage overrides (e.g., `bronze`, `silver`)

### run(df, source_path=None, materialize=False, materialize_target=None)

Executes the contract against a DataFrame.

Returns:
- `ValidationResult` object that unpacks to `(good_df, bad_df)`
- `good_df`: validated, transformed records
- `bad_df`: quarantined records with error reasons

### run_source(path)

Loads data using the engine's native reader and runs the contract in one step.

Returns:
- `ValidationResult` object that unpacks to `(good_df, bad_df)`
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
- `transformations` structured helpers: rename, derive, lookup, filter, deduplicate, select, drop, cast, trim, lower, upper, coalesce, split, explode, map_values, pivot, unpivot, join
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
- `server.schema_policy.evolution`: strict/append/merge/overwrite/compatible/allow behavior
- `server.schema_policy.unknown_fields`: quarantine/drop/allow behavior for undocumented fields
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

---

## DataGenerator

Generate schema-aware synthetic data from any Data Contract YAML.

```python
from lakelogic import DataGenerator

gen = DataGenerator("contracts/bronze_orders.yaml", seed=42)
df = gen.generate(rows=1_000, invalid_ratio=0.1)
```

### Constructor

- `contract_path` (str | Path): Path to the contract YAML file
- `seed` (int, optional): Random seed for reproducibility
- `use_faker` (bool): Use Faker for semantic field generation (default `True`)

### Alternative Constructors

```python
# From an existing data file (schema inferred, no contract needed)
gen = DataGenerator.from_file("data/sample.csv", seed=42)

# From a dbt schema.yml
gen = DataGenerator.from_dbt("models/schema.yml", model="customers")
```

### generate(rows, invalid_ratio, output_format, reference_data, window_start, window_end, ai)

Generate synthetic rows conforming to the contract schema.

- `rows` (int): Total rows to generate (default 100)
- `invalid_ratio` (float): Fraction of intentionally bad rows (default 0.0)
- `output_format` (str): `"polars"` or `"pandas"` (default `"polars"`)
- `reference_data` (dict): FK pools mapping column → list of valid parent PKs
- `window_start` (datetime): Constrain all timestamps to this time window
- `window_end` (datetime): End of window (defaults to `now()` if `window_start` is set)
- `ai` (bool): Enable LLM-powered realistic value generation (default False)

Returns: `polars.DataFrame` or `pandas.DataFrame`

### generate_stream(rows_per_batch, interval_minutes, batches, output_dir, ...)

Generate successive time-windowed batches, simulating a streaming source.

- `rows_per_batch` (int): Rows per batch (default 100)
- `interval_minutes` (int): Window duration per batch (default 5)
- `batches` (int): Total batches to generate (default 12)
- `output_dir` (str | Path): Auto-save to partitioned directories
- `format` (str): File format: `"parquet"`, `"csv"`, `"json"` (default `"parquet"`)
- `partition_template` (str): Partition directory pattern (default `yyyy={Y}/mm={m}/dd={d}/hh={H}/mi={M}`)
- `start_from` (datetime): Override starting timestamp

Yields: `tuple[datetime, datetime, DataFrame]` — `(window_start, window_end, batch_df)`

### generate_related(contracts, rows, seed, ai) — classmethod

Generate referentially consistent data across multiple related contracts.

- `contracts` (list): Paths to contract YAML files
- `rows` (dict | int): Rows per entity (or uniform count)
- `seed` (int): Random seed

Returns: `dict[str, DataFrame]` — entity name → generated DataFrame

### save(df, output, format)

Save a DataFrame to disk.

- `df`: Polars or Pandas DataFrame
- `output` (str | Path): Destination file path
- `format` (str): `"parquet"`, `"csv"`, or `"json"`

### save_partitioned(df, output_dir, filename_field, format)

Save one file per unique key value (e.g., one JSON per entity ID).

- `df`: Generated DataFrame
- `output_dir` (str | Path): Directory to write files into
- `filename_field` (str): Column whose value becomes the filename
- `format` (str): `"json"`, `"parquet"`, or `"csv"`
