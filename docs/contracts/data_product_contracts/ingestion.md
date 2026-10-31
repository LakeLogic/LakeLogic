# Ingestion (Source Configuration)

!!! abstract "Open Lakehouse Contract - the standard"
    LakeLogic implements the **[Open Lakehouse Contract (OLC)](https://olc.lakelogic.io/)**, the open
    standard for lakehouse data contracts. The authoritative field-level spec for
    **ingestion & sources** is the OLC reference: **[ingestion & sources reference →](https://olc.lakelogic.io/reference/ingestion/)**.
    This page shows how the LakeLogic runtime *applies* that spec (engine behavior,
    examples, and runtime-specific notes).


The `source:` block defines **what to read, from where, and how**.

> **Think of it like a mail room.** You tell LakeLogic which mailbox to open (file path or table), what kind of mail to expect (CSV, Parquet, JSON), and how often to check for new deliveries (full refresh vs incremental).

---

## Source Types

`source.type` is one of eight values (the set is defined by the Open Lakehouse Contract; any other value fails on load):

```yaml
source:
  type: "landing"     # File-based (local, S3, ADLS, GCS)
  # type: "stream"    # A file/table location read in micro-batches by watermark
  # type: "table"     # Catalog table (Unity Catalog, Hive) or table: reference
  # type: "delta"     # Delta table directory, by path
  # type: "iceberg"   # Iceberg table directory, by path
  # type: "database"  # Operational DB via JDBC/SQL (Postgres, MySQL, SQL Server, Oracle, SQLite)
  # type: "dlt"       # HTTP API or dlt verified source (the `dlt:` block)
  # type: "sftp"      # Files on an SFTP server (pip install "lakelogic[sftp]"; host keys are verified)
```

!!! note "SFTP credentials"
    Give the password as an environment variable reference, `password: "env:SFTP_PASSWORD"`.
    LakeLogic reads the variable at run time (1.83+); a literal password written into a contract is
    refused, so secrets never land in version control.

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

## Database Ingestion (JDBC / native SQL)

Read directly from an operational database (PostgreSQL, MySQL, SQL Server, Oracle, SQLite) with `type: "database"` and a SQLAlchemy-style connection URI. LakeLogic pushes the contract's `model.fields` down as a `SELECT` (column projection), applies any incremental `WHERE`, then validates every row through your contract.

!!! example "Example: Extract from Postgres with projection + memory-bounded batching"

    ```yaml
    source:
      type: "database"
      path: "postgresql://user:pass@host:5432/analytics"   # SQLAlchemy URI
      load_mode: "incremental"           # optional CDC watermarking
      watermark_field: "updated_at"
      options:
        fetch_size: 100000               # memory-bounded ingestion of large tables
        # Parallel partitioned read (Spark only):
        # partition_column: "id"
        # partition_num: 8
        # partition_lower_bound: 1
        # partition_upper_bound: 10000000
    ```

### Engine support

The contract is identical across engines, but each reaches the database through a different connector — so **driver requirements and `fetch_size` semantics differ**:

| Engine | Connector | Driver needed | `fetch_size` | Large-table strategy |
| --- | --- | --- | --- | --- |
| **polars** | ConnectorX / ADBC (`pl.read_database`) | `pip install connectorx` | rows per chunk | driver-side chunk loop (SQLAlchemy iterator) |
| **duckdb** | native `postgres_scan` / `mysql_scan` / `sqlite_scan` | bundled extension | no-op | native vectorised streaming scan |
| **spark** | JDBC (`spark.read.format("jdbc")`) | **dialect driver jar** on the classpath | → JDBC `fetchsize` | partitions streamed across executors; `partition_column` + bounds → parallel read |

!!! warning "Spark needs a JDBC driver jar"
    Spark reads via JDBC, so the dialect's driver must be on the classpath — e.g. `spark.jars.packages="org.postgresql:postgresql"` (Postgres), `com.mysql:mysql-connector-j` (MySQL), `com.microsoft.sqlserver:mssql-jdbc` (SQL Server), `org.xerial:sqlite-jdbc` (SQLite). If it is missing, LakeLogic raises an actionable error naming the exact package to add.

> **Portability note:** schema, quality rules, transforms, lineage and quarantine run identically on any engine. Only the *connector layer* for databases is engine-specific — file / landing / cloud sources are fully uniform.

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

## Zero-Retention Architecture (Post-Ingestion Lifecycle)

After Bronze ingestion commits data to Delta, you may want to **delete or archive** the original landing zone files. This is critical for GDPR compliance — raw PII should not persist in unmanaged file storage.

The `post_ingestion` config controls what happens to source files **after a successful Bronze commit**.

### Contract-Level (Recommended)

The simplest setup — declare `post_ingestion` directly on the `source:` block:

!!! example "Example: Zero-retention on a single contract"

    ```yaml
    source:
      type: landing
      path: "/landing/crm/customers"
      format: parquet
      post_ingestion:
        action: delete                # delete | archive | retain
        cleanup_is_blocking: false    # cleanup failure ≠ pipeline failure
    ```

This is ideal for simple pipelines and single-contract setups — no `server:` block needed.

### System-Level Default

For data mesh and multi-system pipelines, set a default for **all** Bronze contracts in `_system.yaml`:

!!! example "Example: Zero-retention for the entire system"

    ```yaml
    server:
      bronze:
        post_ingestion:
          action: delete
          cleanup_is_blocking: false
    ```

Individual contracts can then override the system default:

!!! example "Example: Override system default to archive for audit-sensitive data"

    ```yaml
    source:
      type: landing
      path: "s3://landing/finance/invoices"
      post_ingestion:
        action: archive               # Override system-level 'delete'
        archive_path: "s3://archive/finance"   # a ROOT: see "Where archived files go"
    ```

### Where archived files go

`archive_path` is a **root**, not a folder. LakeLogic adds the **table name** (the `info.table_name` of the contract, with its `{system}` and layer tokens filled in: what the end user sees) and the outcome beneath it, so one system-level root serves every table and a failed file never lands among the good ones:

```
<archive_path>/<table>/success/<path relative to the landing folder>
<archive_path>/<table>/error/<path relative to the landing folder>
```

For example, with `archive_path: s3://archive/finance`, a file read into the `bronze_invoices` table ends up at `s3://archive/finance/bronze_invoices/success/dt=2026-10-01/invoices.csv`. The path under the landing folder is kept, so date folders survive. A file name that already exists in the destination is never overwritten: the new file gets a timestamp in its name.

Only the files **that load read** are moved or deleted. A file that arrived while the load was running stays in the landing folder for the next run.

### Files whose cleanup fails (`retry_orphaned_files`)

A file can be loaded and still fail to move or delete (a permission or network error). With `cleanup_is_blocking: false` the run carries on, and LakeLogic **remembers the file**: it writes its path to `<archive_path>/<table>/_orphaned_files.json` (with `action: delete` and no archive root, to the run-log folder instead). At the start of the next bronze run it retries the cleanup of every remembered file, and it **does not read a remembered file again**, so its rows are never loaded twice. Once the cleanup succeeds the file leaves the manifest. A failure while recording is a warning in the log, and the next run may then read the file again. `cleanup_is_blocking: true` still fails the run, and still remembers the file. This is applied by `LakehousePipeline`. Set `retry_orphaned_files: false` (system level, under `server.bronze.post_ingestion`) to turn this off: the files then stay in landing and a later run may read them again.

### Files that fail to read (`on_error`)

```yaml
server:
  bronze:
    post_ingestion:
      action: archive
      archive_path: "s3://archive/finance"
      on_error: move            # retain (default) | move
```

With `on_error: move`, a Bronze load that **fails while reading** (a corrupt or unparseable file) moves the files it read to `<archive_path>/<table>/error/` and then fails as before. It works whatever `action` is, including `delete`, because failed files should be kept for a person to look at.

How a failed file is found depends on the engine:

- **Polars and DuckDB** read the landing folder eagerly. The read is one batch and does not say which file broke it, so **every file in the failed load** moves to `error/`. The log says so.
- **Spark** reads lazily, so a corrupt file fails after the read began, and Spark's error **names the file**. Only the files the error names move to `error/`. (Proven on Databricks serverless: a corrupt Parquet file went to `error/` and the good ones were archived.)
- A failure that names no file (a rule crashing, the Delta write) is not the file's fault; the files stay in the landing folder.
- Spark's CSV and JSON readers are permissive: a malformed line becomes a bad row, not a failed read. Those rows go to quarantine and the file is cleaned up as a success.

### Auto Loader (`source.type: stream`, `options.kind: files`)

On Databricks the file stream is read with Auto Loader, which keeps its own record of files in the checkpoint and does not say which a run read. LakeLogic lists the landing folder **before** the run (hidden `_x` / `.x` files are skipped, as Auto Loader skips them). After a successful run, `post_ingestion` archives or deletes those files; a file that lands during the run is left for the next one. Proven on Databricks serverless across two runs: the first archived the two listed files, the second only the file that arrived later.

A continuous stream (`trigger: continuous` / `processing_time`) never finishes, so it has no cleanup.

Auto Loader's own `cloudFiles.cleanSource` option is not used: in testing on Databricks serverless (environments 1 and 3, `MOVE`, with zero and default retention) it moved no files.

`on_error` is applied by `LakehousePipeline`, and so is the Auto Loader cleanup above. A contract run on its own with `DataProcessor.run_source` honours `action` but not `on_error`.

### Precedence

Contract-level config takes priority, **field by field**: a contract that sets only `on_error` still inherits the system's `action` and `archive_path`.

| Level | Location | Priority |
| --- | --- | --- |
| **Contract** | `source.post_ingestion` | Highest — overrides the system value of each field it sets |
| **System** | `server.bronze.post_ingestion` in `_system.yaml` | Default for all Bronze contracts |
| **Storage** | `storage.archive_path` in `_system.yaml` | Fallback archive root |
| **None** | Neither configured | `retain` (files stay in place) |

### Actions

| Action | Behaviour | Use Case |
| --- | --- | --- |
| `delete` | Remove landing files after Bronze commit | GDPR zero-retention, cost reduction |
| `archive` | Move files to `archive_path` | Regulatory audit trails, 7-year retention |
| `retain` | Leave files in place (default) | Development, debugging, replay scenarios |

### Safety Guarantees

The cleanup engine follows strict safety rules:

1. **Cleanup only runs after a successful Bronze Delta commit** — if ingestion fails, files are untouched
2. **Cleanup failures are non-blocking by default** (`cleanup_is_blocking: false`) — the pipeline succeeds with a warning
3. **Cleanup failures are logged** — if a delete/archive fails, the warning is logged for manual intervention
4. **No double-counting risk** — the watermark has advanced past cleaned files, so they won't be re-ingested

### Failure Matrix

| Stage | Failure | Result |
| --- | --- | --- |
| Read fails | File is unreadable or unparseable | Load fails; with `on_error: move` the files go to `<archive_path>/<table>/error/`, otherwise they stay in landing |
| Rows fail rules | Bad values in a readable file | Rows go to `quarantine:`; the file is cleaned up as a success |
| Write fails | Bronze commit crashes | File untouched in landing, retried on next run |
| Delete/Archive fails | Permission / network error | Pipeline succeeds with warning, file stays in landing |

### Configuration Reference

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `action` | `string` | `"retain"` | `delete`, `archive`, or `retain` |
| `cleanup_is_blocking` | `bool` | `false` | If `true`, cleanup failure fails the pipeline |
| `archive_path` | `string` | `null` | Archive **root**; files go under `<root>/<table>/success/` or `.../error/`. Required for `action: archive` and for `on_error: move` |
| `on_error` | `string` | `"retain"` | `retain` or `move`: what happens to the files of a load that failed while reading |

> **Archive path resolution:** `archive_path` from `post_ingestion` (contract, then system). If neither sets it, the PipelineRunner falls back to `storage.archive_path` in `_system.yaml`. How long archived files are retained is controlled by your cloud provider's lifecycle policy (e.g., Azure Blob Lifecycle Management), not by LakeLogic.

---

## Load Mode Validation

The pipeline validates required properties per load_mode:

| Mode | Requirements |
| --- | --- |
| `full` | None |
| `incremental` | `watermark_field` recommended (defaults to `_lakelogic_processed_at`) |
| `cdc` | At least ONE of: `cdc_op_field`, `cdc_timestamp_field` |

See [Watermark Strategies](watermark_strategies.md) for incremental state tracking.

