# Capability Matrix

This matrix summarizes what each engine supports in the OSS runtime. When a feature is engine-specific, use an explicit engine selection.

## Engine Support

| Engine | File Sources | Table Sources | File Outputs | Table Outputs | Quarantine Table Targets | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| Polars | CSV, Parquet | No | CSV, Parquet | No | Yes (DuckDB/SQLite backend) | Fast local engine. Table targets use the configured quarantine backend. |
| Pandas | CSV, Parquet | No | CSV, Parquet | No | Yes (DuckDB/SQLite backend) | Uses DuckDB for SQL execution. |
| DuckDB | CSV, Parquet | No | CSV, Parquet | DuckDB tables | Yes (DuckDB/SQLite backend) | Table outputs are DuckDB database tables. |
| Spark | CSV, Parquet, Delta, Iceberg, JSON | Spark tables | CSV, Parquet, Delta, Iceberg, JSON | Spark tables | Yes (Spark tables) | Recommended for lakehouse and Unity Catalog. |
| Snowflake | Table-only | Table-only | Table writes (Snowflake) | Snowflake tables | Yes (Snowflake tables) | Requires connector and credentials. |
| BigQuery | Table-only | Table-only | Table writes (BigQuery) | BigQuery tables | Yes (BigQuery tables) | Requires client and credentials. |

## Format Defaults

- Quarantine **file** targets default to **Parquet** (override with file suffix or `metadata.quarantine_format`).
- Quarantine **table** targets on Spark default to **Iceberg** (override with `metadata.quarantine_table_format`).
- Non-Spark engines support **CSV/Parquet** for file targets.

If you need an unsupported combination, use Spark or route through an external staging step.
