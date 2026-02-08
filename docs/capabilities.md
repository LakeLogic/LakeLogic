# Capability Matrix

This matrix summarizes what each engine supports in the OSS runtime. When a feature is engine-specific, use an explicit engine selection.

## Engine Support

| Engine | File Sources | Table Sources | File Outputs | Table Outputs | Quarantine Table Targets | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| Polars | CSV, Parquet | No | CSV, Parquet | No | Yes (DuckDB/SQLite backend) | Fast local engine. For managed lakehouse tables (Unity Catalog/Fabric), use Spark. |
| Pandas | CSV, Parquet | No | CSV, Parquet | No | Yes (DuckDB/SQLite backend) | Uses DuckDB for SQL execution. |
| DuckDB | CSV, Parquet | No | CSV, Parquet | DuckDB tables | Yes (DuckDB/SQLite backend) | Table outputs are DuckDB database tables. |
| Spark | CSV, Parquet, Delta, Iceberg, JSON | Spark tables | CSV, Parquet, Delta, Iceberg, JSON | Spark tables | Yes (Spark tables) | Recommended for lakehouse catalogs (Unity Catalog/Fabric). |
| Snowflake | Table-only | Table-only | Table writes (Snowflake) | Snowflake tables | Yes (Snowflake tables) | Requires connector and credentials. |
| BigQuery | Table-only | Table-only | Table writes (BigQuery) | BigQuery tables | Yes (BigQuery tables) | Requires client and credentials. |

## Materialization Strategies

| Strategy | Polars | Pandas | DuckDB | Spark |
| --- | --- | --- | --- | --- |
| `overwrite` | Native | Native | Native | Native |
| `append` | Native | Native | Native | Native |
| `merge` | Requires pandas | Native | Requires pandas | Native (distributed) |
| `scd2` | Requires pandas | Native | Requires pandas | Native (distributed) |

**Spark advantage:** Merge and SCD2 operations run natively using distributed DataFrame operations, avoiding driver memory bottlenecks. For Delta Lake tables, LakeLogic uses `MERGE INTO` when available.

## Format Defaults

- Quarantine **file** targets default to **Parquet** (override with file suffix or `metadata.quarantine_format`).
- Quarantine **table** targets on Spark default to **Iceberg** (override with `metadata.quarantine_table_format`).
- Non-Spark engines support **CSV/Parquet** for file targets.

If you need an unsupported combination, use Spark or route through an external staging step.

## Lakehouse Catalog Notes (Unity Catalog / Fabric)

- **Managed tables require Spark.** Polars/Pandas can write files, but cannot register or manage tables in a catalog.
- **Iceberg tables in a catalog require Spark.** File-based Iceberg output is not the same as a catalog-managed table.
