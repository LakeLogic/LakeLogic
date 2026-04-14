# Ingestion (Source Configuration)

The `source:` block defines **what to read, from where, and how**.

> **Think of it like a mail room.** You tell LakeLogic which mailbox to open (file path or table), what kind of mail to expect (CSV, Parquet, JSON), and how often to check for new deliveries (full refresh vs incremental).

---

## Source Types

Three ways to bring data into your lakehouse:

```yaml
source:
  type: "landing"     # File-based (local, S3, ADLS, GCS)
  # type: "table"     # Catalog table (Unity Catalog, Hive)
  # type: "stream"    # Kafka / streaming
```

---

## File-Based Ingestion (Landing)

For ingesting raw files (CSV, JSON, Parquet, etc.) from cloud storage or local filesystem.

!!! example "Example: CSV file ingestion from S3"

    ```yaml
    source:
      type: "landing"
      path: "s3://bronze-bucket/customers/*.parquet"
      format: "csv"                  # parquet | csv | json | delta | avro | orc | xml
      pattern: "*.parquet"           # File glob filter
      load_mode: "incremental"       # full | incremental | cdc

      # Reader options (passed to engine: Spark, Polars, DuckDB)
      options:
        header: "true"
        inferSchema: "true"
        delimiter: ","
        multiLine: "true"
        recursiveFileLookup: "true"
    ```

### Date-Partitioned Landing

Limits file scanning to relevant date partitions — **critical at scale** to avoid scanning millions of files.

!!! example "Example: Scan only the last 3 days of partitions"

    ```yaml
    source:
      type: "landing"
      path: "s3://landing/events/"
      partition:
        format: "y_%Y/m_%m/d_%d"      # → events/y_2026/m_03/d_22/*.json
        lookback_days: 3               # Scan last 3 days (default: 1)
        # start_date: "2026-01-01"     # For explicit backfills
        # end_date: "2026-03-22"
        # file_pattern: "*.json"       # Auto-derived from source.format
    ```

**Supported format patterns:**

| Pattern | Directory Structure |
| --- | --- |
| `y_%Y/m_%m/d_%d` | `events/y_2026/m_03/d_22/*.json` |
| `date=%Y%m%d` | `events/date=20260322/*.json` |
| `%Y/%m/%d` | `events/2026/03/22/*.json` |
| `year=%Y/month=%m` | `events/year=2026/month=03/*.json` |
| `dt=%Y-%m-%d/%H` | `events/dt=2026-03-22/17/*.json` (hourly) |

---

## Table-Based Ingestion

For reading from an existing catalog table (e.g., a Bronze table feeding Silver).

!!! example "Example: Read from a Bronze Unity Catalog table"

    ```yaml
    source:
      type: "table"
      path: "{domain_catalog}.{bronze_layer}_{system}_events"
      load_mode: "incremental"
      watermark_strategy: "pipeline_log"
    ```

---

## CDC (Change Data Capture)

For source systems that provide insert/update/delete operation flags (e.g., Debezium, Oracle GoldenGate).

!!! example "Example: CDC with operation flags"

    ```yaml
    source:
      type: "table"
      path: "{domain_catalog}.{bronze_layer}_{system}_events"
      load_mode: "cdc"
      cdc_op_field: "operation"                    # Column with I/U/D flags
      cdc_delete_values: ["D", "DELETE"]           # Values indicating deletes
      cdc_timestamp_field: "source_record_updated_at"  # For soft-delete timestamps
    ```

---

## Flatten Nested JSON

When Bronze tables contain JSON-string columns, flatten them in Silver to make the data queryable.

!!! example "Example: Flatten specific nested columns"

    ```yaml
    source:
      flatten_nested: ["derived", "pricing", "location"]
      # true → flatten all JSON columns
      # [col, ...] → flatten specific columns
    ```

---

## Reference Data Links

Register additional datasets for use in SQL transformations. This is how you **join multiple datasets within a single contract** — for example, enriching orders with customer details.

> **Why this matters:** Without links, you'd need a separate pipeline step to join data. With links, you declare the reference table once and use it directly in SQL — keeping everything in one contract.

!!! example "Example: Register reference tables for joins"

    ```yaml
    links:
      - name: "dim_countries"
        path: "./reference/countries.parquet"
        type: "parquet"              # parquet | csv | delta | table
        broadcast: true              # Spark broadcast join for small tables
        columns: ["code", "name", "region"]  # Column projection

      - name: "dim_products"
        table: "catalog.reference.products"
        type: "table"
        broadcast: false
        columns: ["id", "product_name", "category", "price"]
    ```

!!! example "Example: Using linked datasets in SQL transforms"

    ```yaml
    transformations:
      - sql: >
          SELECT s.*, c.name AS country_name
          FROM source s
          LEFT JOIN dim_countries c ON s.country_code = c.code
        phase: "post"
    ```

---

## Load Mode Validation

The pipeline validates required properties per load_mode:

| Mode | Requirements |
| --- | --- |
| `full` | None |
| `incremental` | `watermark_field` recommended (defaults to `_lakelogic_processed_at`) |
| `cdc` | At least ONE of: `cdc_op_field`, `cdc_timestamp_field` |

See [Watermark Strategies](watermark_strategies.md) for incremental state tracking.
