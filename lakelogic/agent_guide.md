# LakeLogic Core — agent guide

LakeLogic Core runs **Open Lakehouse Contracts** (`*.olc.yaml`): it reads a source, applies the
contract's transformations and quality rules, splits rows into **good** and **quarantined**, and
writes the output, the rejects and a run log. Same contract on Polars, DuckDB, Spark, Snowflake
or BigQuery.

- **Writing a contract** (which keys, where they go): `olc guide`, then `olc reference <Section>`.
  The `olc` command is installed with LakeLogic. Validate with `olc validate <file>`.
- **Running it** (this guide): Python `DataProcessor`, or the `lakelogic` command.
- Print this guide any time: `lakelogic agent-guide`. Save it for an agent: `lakelogic agent-guide --write AGENTS.md`.

## Install

| You need | Install |
|---|---|
| Local files (CSV/JSON/Parquet/Excel), Polars | `pip install "lakelogic[polars]"` |
| DuckDB engine | `pip install "lakelogic[duckdb]"` |
| Spark / Databricks | `pip install "lakelogic[spark]"` (on Databricks, Spark is already there) |
| Delta Lake tables | `pip install "lakelogic[delta]"` |
| Write to Postgres | `pip install "lakelogic[dlt-postgres]"` |
| Write to SQL Server / Azure SQL | `pip install "lakelogic[dlt-mssql]"` |
| Other dlt destinations / REST API sources | `pip install "lakelogic[dlt]"` |
| Read from Postgres / MySQL / Azure SQL | `lakelogic[postgresql]` / `[mysql]` / `[azuresql]` |
| SFTP sources | `pip install "lakelogic[sftp]"` |
| Snowflake | `pip install "lakelogic[snowflake]"` |
| Kafka / streaming | `pip install "lakelogic[kafka]"` / `[streaming]` |

Extras combine: `pip install "lakelogic[polars,dlt-postgres]"`. A missing extra fails with an
error naming the extra to install.

## Run one contract

```python
from lakelogic import DataProcessor

proc = DataProcessor("orders.olc.yaml", engine="polars")   # engine optional; default polars
result = proc.run_source("data/orders.csv")                # or proc.run(df) for a DataFrame
good, bad = result                                         # unpacks to 2 frames
print(proc.last_report)                                    # counts, rule outcomes, run id
proc.materialize(result.good, result.bad)                  # write output + quarantine as the contract says
proc.write_run_log()                                       # write the run log the contract names
```

```bash
lakelogic run --contract orders.olc.yaml --source data/orders.csv            # validate + report
lakelogic run -c orders.olc.yaml --source data/orders.csv --materialize      # also write output
lakelogic run -c orders.olc.yaml --source data/orders.csv --output-bad bad.csv
```

`run_source()` with no argument, or `lakelogic run` with no `--source`, reads `source.path` from
the contract. A file is read in the format its extension names (`.csv`, `.parquet`, `.jsonl`, …);
set `source.format` for anything else.

## How do I …

| Task | Do this |
|---|---|
| Validate rows only, write nothing | `proc.run_source(path)`; inspect `result.good` / `result.bad` |
| Write the output | `proc.materialize(good, bad)` or `lakelogic run … --materialize` |
| See why rows were rejected | `result.bad` has `_lakelogic_errors` (rule names) per row |
| See each step | `DataProcessor(…, trace=True)`, then `result.trace`; CLI `--trace` |
| Choose an engine | `engine="duckdb"` / `"spark"` / `"snowflake"` / `"bigquery"` / `"polars"`; CLI `--engine` |
| Apply a layer's overrides | `stage="silver"`; CLI `--stage silver` |
| Fail on any problem | `DataProcessor(…, strict=True)` |
| Set a batch aside when a dataset rule fails, and carry on | contract `quality.on_dataset_failure: quarantine` (+ `source.post_ingestion: {on_error: move, archive_path: …}` so it is not read again) |
| Reprocess a date range | `proc.run_source(reprocess_from="2026-01-01", reprocess_to="2026-01-31")` |
| Write to a database dlt supports (any other) | contract `materialization.format: dlt` + `dlt_destination` (see below) |
| Write to Postgres / MySQL / SQL Server / DuckDB / SQLite fast | `materialization.format: database` + `connection` + `table` (see "Write to a database fast") |
| Set aside only the failing groups of a batch | `quality.on_dataset_failure: quarantine` + `quarantine_scope: [symbol]` |
| Merge into Snowflake / BigQuery without moving data | `engine="snowflake"` / `"bigquery"` + `strategy: merge` + `target_path: table:…` |
| Run a whole system (bronze → silver → gold) | `_system.yaml` + `LakehousePipeline` or `lakelogic-driver` (see below) |
| Draft a contract from sample files | `lakelogic bootstrap --landing <dir> --output-dir contracts/` or `lakelogic.infer_contract(path)` |
| Profile data | `lakelogic profile …` or `lakelogic.profile(df)` |
| Generate test data from a contract | `lakelogic generate …` or `lakelogic.DataGenerator` |
| Check the environment | `lakelogic doctor` |

## Engines

| Engine | Good for | Notes |
|---|---|---|
| `polars` (default) | Local files, fast single machine | |
| `duckdb` | Local SQL-heavy work, DuckDB/DuckLake | |
| `spark` | Databricks, Fabric, large data, Delta/Iceberg tables | pass an active session where needed |
| `snowflake` | Data already in Snowflake | connection via `metadata.snowflake_*`; writes in place (below) |
| `bigquery` | Data already in BigQuery | writes in place (below) |

The contract never names the engine; the caller does.

### Writing inside Snowflake / BigQuery (native, no data leaves the warehouse)

With `engine="snowflake"` or `engine="bigquery"` and a `table:` target in the same warehouse,
the output is written by one SQL statement inside the warehouse - not fetched to the machine
and pushed back.

```yaml
primary_key: [order_id]              # required for merge
materialization:
  strategy: merge                    # append | overwrite | merge (upsert) - all native
  target_path: table:analytics.orders
```

| Strategy | Native in Snowflake | Native in BigQuery |
|---|---|---|
| `append` | yes (`INSERT`) | yes (`INSERT`) |
| `overwrite` / `replace` | yes | yes (`CREATE OR REPLACE`) |
| `merge` / `upsert` | yes (`MERGE INTO`) | yes (`MERGE`) |
| `scd2` | no - goes through dlt | no - goes through dlt |

A merge keeps one row per key from the batch, inserts new keys, and updates a matched row only
when a business column changed - re-sending unchanged rows rewrites nothing. The target table is
created when missing. Any other engine, or a file target, writes through the normal path.

## Where things are written

Wherever output can be written, its **quarantine** and **run log** can be written there too —
files, tables, or a database. Never skip rejects or logs because the target is not cloud storage.

Snowflake and BigQuery with their own engine: see "Writing inside Snowflake / BigQuery" above.

### Write to a database fast (`format: database`)

Postgres, MySQL / MariaDB, SQL Server / Azure SQL, DuckDB and SQLite are written natively: a
bulk load (Postgres `COPY`, SQL Server fast bulk insert, …) then one upsert statement (Postgres
`ON CONFLICT`, MySQL `ON DUPLICATE KEY`, SQL Server `MERGE … HOLDLOCK`), in one transaction per
batch. Measured on Postgres: a 1,200-row merge in about 0.02 s (dlt: about
0.4 s), 100,000-row append in about 0.15 s. Choose **`database`** for loads into these databases; **`dlt`** for
any other destination and for API sources.

```yaml
primary_key: [symbol, ts]            # needed for merge and for on_conflict: ignore
materialization:
  format: database
  connection: env:PG_URL             # postgresql:// | mysql:// | mssql://…?TrustServerCertificate=yes | duckdb:/// | sqlite:///
  table: market_data.market_bars     # schema.table; created from the data when missing
  strategy: merge                    # append | merge | overwrite   (scd2: use dlt)
  write:                             # all optional
    on_conflict: error               # append only: error (default) | ignore
    only_changed: true               # merge: unchanged rows are not rewritten (default true)
    batch_size: 100000
    create_table: true
    allow_empty_overwrite: false     # overwrite with 0 rows is refused unless true
quarantine:
  format: database
  connection: env:PG_URL
  table: quarantine.quarantine_market_bars
metadata:
  run_log_backend: database
  run_log_table: lakelogic.run_logs
  run_log_connection: env:PG_URL     # defaults to materialization.connection
```

- A merge refuses a batch with the same key twice ("… appear more than once …"); add a
  `deduplicate` step. A failed write is rolled back - nothing is half-written.
- New columns are added (`ALTER TABLE ADD COLUMN`); `schema_policy.evolution: strict` refuses them.
  Columns are never dropped or retyped.
- **Retries:** an append without a key carries `_lakelogic_batch_key` (a fingerprint of the source
  files - path, size and modified time - or `proc.materialize(good, bad, batch_key="…")`); a
  retried batch replaces its own rows. A file that is rewritten is a new batch: its rows are added.
  `run(df)` has no files, so pass `batch_key=` to make a DataFrame append retry-safe.
  Quarantined rows are keyed row by row by their content when there is no batch key: a re-run
  does not add the same rejects again (database, Delta, parquet, csv, duckdb, sqlite quarantine).
- A merge leaves a declared column the batch does not carry as it is in the table (Delta too).
- A primary key column cannot be null: such rows are quarantined (`<column>_required`).
- MySQL and SQL Server cannot store NaN or infinity; a target write is refused before it starts.
  Quarantine them first: `<column> BETWEEN -1.7e308 AND 1.7e308` (the quarantine stores them as NULL).
- A dataset rule whose WHERE matches no rows (`SELECT SUM(x) FROM source WHERE x < 0`) passes:
  nothing broke it. Rows that match but give NULL still fail (unmeasured).
- A quarantine write that fails sets the run status `quarantine_not_saved`.
- A table dropped or recreated outside LakeLogic is found again on the next write.
- Still on dlt? `dlt_loader_file_format: csv` and `dlt_merge_strategy: upsert` make it faster.

| What | Cloud / files | Database (Postgres, SQL Server, …) |
|---|---|---|
| Output | `materialization: {format: delta \| parquet \| iceberg \| csv, target_path: …}` | `materialization: {format: dlt, dlt_destination: postgres, dlt_credentials: env:PG_URL}` |
| Quarantine | `quarantine: {format: parquet \| delta \| iceberg \| csv, target: …}` | `quarantine: {format: dlt, dlt_destination: postgres, dlt_credentials: env:PG_URL}` (no `target`; set `table:` to name the table) |
| Run log | `metadata: {run_log_dir: logs/}` (JSON) or `run_log_table` + `run_log_backend: delta \| duckdb \| sqlite \| spark` | `metadata: {run_log_table: run_logs, run_log_backend: dlt, dlt_destination: postgres, dlt_credentials: env:PG_URL}` |

Credentials are references, never values: `env:NAME`, `${ENV:NAME}`, `keyvault://…`.

In a database the run log lands in schema `lakelogic_run_log` (unless `dlt_dataset_name` is
set), quarantine in `quarantine`; dlt also keeps a `<schema>_staging` copy - expected.

`lakelogic run` writes the run log the contract's `metadata` names. From Python, call
`proc.write_run_log()` after the run. A pipeline run (`LakehousePipeline`) writes it from
`_system.yaml`.
**Personal data** (`pii: true` + `masking: nullify | hash | redact | partial | encrypt`) is masked
in the output AND in quarantined rows.

## Run a whole system

Fastest route: let LakeLogic build the project from your contracts, then run it.

```bash
lakelogic scaffold contracts/*.olc.yaml --out myproject --engine polars
cd myproject && python run_pipeline.py
```

The registry file (`_registry.yaml`, or `_system.yaml`) **lists its contracts** - nothing is
discovered by scanning folders:

```yaml
domain: sales                  # required
system: shop                   # required
storage_mode: path
storage:
  external_location_root: ./lake
contracts:                     # every contract the system runs
  - {layer: bronze, entity: orders, path: contracts/bronze/orders.yaml}
  - {layer: silver, entity: orders, path: contracts/silver/orders.yaml, depends_on: [bronze_orders]}
```

```python
from lakelogic.core.registry import DomainRegistry
from lakelogic.pipeline.runner import LakehousePipeline

registry = DomainRegistry.from_yaml("myproject/_registry.yaml", environment="dev", storage_mode="path")
LakehousePipeline(registry, engine="polars").run()          # target_layers="bronze,silver" to limit
```

Or `lakelogic-driver --registry myproject/_registry.yaml --layers bronze,silver --engine polars`.
"No contracts processed" means the `contracts:` list is empty or its paths are wrong (relative
to the registry file). For business names, add a `_domain.yaml` beside it.

Check a registry: `lakelogic registry validate <file>`; see where a value comes from:
`lakelogic registry explain <file> --key <dotted.key>`.

## Reading results

| Attribute | What it holds |
|---|---|
| `result.good` | Rows that passed every rule, transformed |
| `result.bad` | Quarantined rows, with error reasons |
| `result.raw` | Input as read (`len(raw) == len(good) + len(bad)` unless rows were deduplicated or filtered) |
| `result.trace` | Step-by-step trace (when `trace=True`) |
| `proc.last_report` | Counts, dataset-rule outcomes, run id, timings |

A failing **row** rule quarantines the row. A failing **dataset** rule does what
`quality.on_dataset_failure` says:

| Value | The batch (everything one run reads) | The run |
|---|---|---|
| `continue` (default) | written; the failure is reported | carries on |
| `stop` (= `fail_pipeline_on_dataset_error: true`) | nothing written | fails (`DatasetRulesFailed`) |
| `quarantine` | every row goes to quarantine, reason `Batch rejected: …`; nothing reaches the target | carries on; `proc.batch_quarantined` / `last_report["batch_quarantined"]` say so |
| `quarantine` + `quarantine_scope` | only the failing groups go to quarantine, reason `Group rejected: …`; the rest is written | carries on; run log `status: success_with_rejects`, `rejects_reason: 1 symbol group(s) failed: …` |

## Good to know

- `olc validate` checks a contract's shape, not that it runs. Run it on sample data.
- **SCD2**: the first version of each key starts at `1900-01-01` ("valid since the beginning");
  set `scd2.effective_from_default` to change it. `_version` 1 is the oldest; `is_current`
  marks the live row; `effective_to` is `9999-12-31` while current.

## When it fails

| Message contains | Cause and fix |
|---|---|
| `requires 'dlt'` / `requires 'deltalake'` / `No module named` | Install the extra named in the message |
| `Unsupported engine` | Use one of: polars, duckdb, spark, snowflake, bigquery |
| `Unsupported quarantine format` | Use parquet, csv, delta, iceberg, duckdb, sqlite or dlt |
| A validation error naming a key | Run `olc validate <file>`; check the key in `olc guide` |
| Credentials missing | Set the environment variable the `env:` reference names |
| Every row quarantined | Read `result.bad` `_lakelogic_errors`: usually a type or `not_null` rule |
| A bad type also fails another rule | A value that fails its type cast becomes null, so a rule on it fails too. Fix the type first |
| `Invalid table location …` | The path is read as a Delta table: point at the table folder, or set `source.format` |
