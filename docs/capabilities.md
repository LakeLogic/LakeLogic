# Capability Matrix

This matrix summarizes what each engine supports in the OSS runtime. When a feature is engine-specific, use an explicit engine selection.

## Engine Support

| Engine | File Sources | Table Sources | File Outputs | Table Outputs | Quarantine Targets | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| Polars | CSV, Parquet, Delta¹ | Delta tables¹ | CSV, Parquet, Delta¹, Iceberg² | Delta tables¹ | DuckDB/SQLite table, Delta¹ file | Delta-RS provides Spark-free lakehouse table support. |
| Pandas | CSV, Parquet, Delta¹ | Delta tables¹ | CSV, Parquet, Delta¹, Iceberg² | Delta tables¹ | DuckDB/SQLite table, Delta¹ file | Uses DuckDB for SQL execution. |
| DuckDB | CSV, Parquet, Delta¹ | Delta tables¹ | CSV, Parquet, Delta¹, Iceberg² | DuckDB + Delta tables¹ | DuckDB/SQLite table, Delta¹ file | Table outputs are DuckDB or Delta tables. |
| Spark | CSV, Parquet, Delta, Iceberg, JSON | Spark tables | CSV, Parquet, Delta, Iceberg, JSON | Spark tables | Spark tables (Delta/Iceberg) | Recommended for lakehouse catalogs (Unity Catalog/Fabric). |
| Snowflake | Table-only | Table-only | Table writes (Snowflake) | Snowflake tables | Snowflake tables | Requires `snowflake-connector-python` and credentials. |
| BigQuery | Table-only | Table-only | Table writes (BigQuery) | BigQuery tables | BigQuery tables | Requires `google-cloud-bigquery` and credentials. |
| LLM | Text, PDF, Image, Audio, Video | N/A | Structured rows | N/A | Via parent engine | Unstructured → structured extraction via LLM providers. |

¹ Delta support via [delta-rs](https://delta-io.github.io/delta-rs/) (`pip install deltalake`). Includes read, write, atomic MERGE, vacuum, optimize, and time travel — no Spark/JVM required. Cloud storage auto-credentials via `DeltaAdapter`.
² Iceberg file output uses DuckDB's iceberg extension (`COPY ... FORMAT ICEBERG`).

## Materialization Strategies

| Strategy | Polars | Pandas | DuckDB | Spark |
| --- | --- | --- | --- | --- |
| `overwrite` | Native | Native | Native | Native |
| `append` | Native | Native | Native | Native |
| `merge` | Native (Delta via delta-rs) / pandas (CSV/Parquet) | Native | Native (Delta via delta-rs) / pandas (CSV/Parquet) | Native (distributed) |
| `scd2` | Via pandas | Native | Via pandas | Native (distributed) |

**Spark advantage:** Merge and SCD2 operations run natively using distributed DataFrame operations, avoiding driver memory bottlenecks. For Delta Lake tables, LakeLogic uses `MERGE INTO` when available.

**Delta merge via delta-rs:** When the output format is Delta, Polars and DuckDB use `DeltaTable.merge()` from delta-rs for atomic merge operations — no pandas conversion needed. This uses the same MERGE INTO semantics as Spark Delta but runs without JVM.

**Non-Delta merge/SCD2:** For CSV/Parquet targets, Polars and DuckDB fall back to pandas for merge and SCD2 operations. This works well for moderate data volumes but may hit driver memory limits at scale — use Spark or Delta format for large datasets.

## Format Defaults

- Quarantine **file** targets default to **Parquet** (override with `quarantine.format`, or `metadata.quarantine_format`, or file suffix).
- Quarantine **table** targets on Spark default to **Delta** (override with `metadata.quarantine_table_format`).
- Non-Spark engines support **CSV, Parquet, and Delta** for quarantine file targets.

If you need an unsupported combination, use Spark or route through an external staging step.

## LLM Extraction Engine

The LLM engine (`engines/llm.py`) extracts structured data from unstructured inputs:

| Provider | API Key Env Var | Notes |
| --- | --- | --- |
| `openai` | `OPENAI_API_KEY` | GPT-4o, GPT-4o-mini |
| `azure_openai` | `AZURE_OPENAI_API_KEY` + `AZURE_OPENAI_ENDPOINT` | Azure-hosted OpenAI |
| `anthropic` | `ANTHROPIC_API_KEY` | Claude 3.5 Sonnet, Haiku |
| `google` | `GOOGLE_API_KEY` | Gemini 2.0 Flash |
| `bedrock` | AWS credentials (boto3) | Claude, Llama, Titan via AWS |
| `ollama` | None (local) | Any Ollama model, default `http://localhost:11434` |
| `local` | None (local) | HuggingFace Transformers (Phi-3-mini default) |

Preprocessing supports PDF (OCR), image, audio (Whisper), and video inputs.

## Lakehouse Catalog Notes (Unity Catalog / Fabric)

- **Unity Catalog external tables work with Polars/Pandas.** LakeLogic resolves 3-part table names (`catalog.schema.table`) to storage paths via the Databricks API, then reads/writes via delta-rs. No Spark needed.
- **UC managed tables require Spark.** Managed tables don't expose a direct storage path — if resolution returns no `storage_location`, Spark is required.
- **Iceberg tables in a catalog require Spark.** File-based Iceberg output is not the same as a catalog-managed table.
- **Delta via delta-rs** is used automatically by LakeLogic for read, write, merge, vacuum, and time travel on any Delta table accessible via path. A standalone `DeltaAdapter` class is also available for ad-hoc use outside the pipeline.
