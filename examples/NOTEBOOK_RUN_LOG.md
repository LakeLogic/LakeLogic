Notebook run log
Date: 2026-02-07

Notebook: D:\Github\_SaaS\lakeguard\examples\01_getting_started\basic_validation\tutorial.ipynb
Command: python -m jupyter nbconvert --execute --to notebook "...\tutorial.ipynb" --output "...\tutorial.executed.ipynb"
Result: FAILED
Error: DataContract validation error: missing required field info.version in contract.yaml
Traceback head: DataProcessor(contract="contract.yaml") -> DataContract(...) -> ValidationError info.version field required
Action needed: add info.version to contract.yaml or relax schema requirement.

Update: 2026-02-07
- Fixed contract: added info.version to examples/01_getting_started/basic_validation/contract.yaml
- Fixed DuckDB identifier quoting (removed invalid f-string backslash) in lakeguard/engines/duckdb.py
- Updated tutorial.ipynb to set LAKEGUARD_ENGINE="polars" (duckdb not installed in env)
- Re-ran notebook via nbconvert: SUCCESS

Update: 2026-02-07
- Observed schema drift warning (unknown column rn) due to row_number() in pre-transform SQL.
- Fixed by adding a pre-phase drop of rn in examples/01_getting_started/basic_validation/contract.yaml.
- Re-ran tutorial.ipynb: SUCCESS (schema drift warning removed; Polars performance warnings may still appear).

Update: 2026-02-07
- Notebook was forcing LAKEGUARD_ENGINE="duckdb" which is not installed in env_lakeguard.
- Updated tutorial.ipynb to set LAKEGUARD_ENGINE="polars" again.
- Re-ran tutorial.ipynb: SUCCESS.

Update: 2026-02-07
- Notebook was still setting LAKEGUARD_ENGINE="duckdb"; switched to "polars" again.
- Fixed NameError in DuckDB engine by importing uuid4 (lakeguard/engines/duckdb.py).
- Re-ran tutorial.ipynb: SUCCESS.

Update: 2026-02-07
- Notebook install cell now uses `%pip install lakeguard[pandas]` to target the kernel env.
- Added comment about editable install for local repo changes.

Update: 2026-02-07
- Fixed dataset rule comparison to handle NULL values (duckdb/polars/spark).
- Pandas adapter now materializes DuckDB results to DataFrames.
- DuckDB adapter now materializes bad rows to avoid view recursion/type-change binder errors.
- Re-ran tutorial.ipynb with kernel lakeguard-env: SUCCESS.

Update: 2026-02-07
- Spark run returned 0 rows due to relative path resolution.
- DataProcessor.run_source now resolves relative paths against the contract base path and uses absolute paths for Spark reads.
- Re-run tutorial.ipynb with Spark after reinstalling local lakeguard (if using PyPI install).

Update: 2026-02-07
- Spark adapter now filters nulls from error/category arrays to avoid NULL has_errors dropping all rows.
- Updated tutorial.ipynb install cell to prefer editable local install when running from repo.

Update: 2026-02-07
- Spark counts now include pre_transform_dropped (source_count - (good + bad)) for excluded row metrics.

Update: 2026-02-07
- Log summary now labels total as post-transform and includes Source count when available.

Update: 2026-02-07
- Added domain/system/data_layer tags to the run summary log line when present in contract metadata.
- Lineage now supports optional domain/system columns when lineage is enabled; data_layer is no longer captured in lineage.

Update: 2026-02-07
- Materialization now prefers native Polars writes for csv/parquet to avoid pyarrow dependency (affects both good and quarantine outputs).

Update: 2026-02-08
- Added quality.enforce_required flag so bronze stages can skip required-field quarantines while still using the model in silver.

Update: 2026-02-08
- Lineage columns are now excluded from schema drift/unknown field detection across engines.

Update: 2026-02-08
- Added source_files and max_source_mtime to run reports and run log tables.
- Added incremental load support for glob sources using run log watermark lookup.
