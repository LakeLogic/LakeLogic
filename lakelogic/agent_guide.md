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
| Reprocess a date range | `proc.run_source(reprocess_from="2026-01-01", reprocess_to="2026-01-31")` |
| Write to Postgres / SQL Server | contract `materialization.format: dlt` + `dlt_destination` (see below) |
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
| `snowflake` | Data already in Snowflake | connection via `metadata.snowflake_*` |
| `bigquery` | Data already in BigQuery | |

The contract never names the engine; the caller does.

## Where things are written

Wherever output can be written, its **quarantine** and **run log** can be written there too —
files, tables, or a database. Never skip rejects or logs because the target is not cloud storage.

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

A system is a folder of contracts plus `_system.yaml` (needs at least `domain:` and `system:`;
also storage roots, run log, defaults) and,
for a domain, `_domain.yaml` (business names). Contracts declare `upstream`, and the runner
orders them bronze → silver → gold.

```python
from lakelogic.core.registry import DomainRegistry
from lakelogic.pipeline import LakehousePipeline

registry = DomainRegistry.from_yaml("contracts/sales/_system.yaml", environment="dev")
LakehousePipeline(registry, engine="polars").run(target_layers="bronze,silver,gold")
```

```bash
lakelogic-driver --registry contracts/_registry.yaml --layers bronze,silver --engine polars
```

Check a system file: `lakelogic registry validate <file>`; see where a value comes from:
`lakelogic registry explain <file> --key <dotted.key>`.

## Reading results

| Attribute | What it holds |
|---|---|
| `result.good` | Rows that passed every rule, transformed |
| `result.bad` | Quarantined rows, with error reasons |
| `result.raw` | Input as read (`len(raw) == len(good) + len(bad)` unless rows were deduplicated or filtered) |
| `result.trace` | Step-by-step trace (when `trace=True`) |
| `proc.last_report` | Counts, dataset-rule outcomes, run id, timings |

A failing **row** rule quarantines the row; a failing **dataset** rule fails the run only when
`quality.fail_pipeline_on_dataset_error: true`.

## Known gaps (being fixed)

State these to the user rather than working around them silently.

- **SCD2 output**: dates may come out as strings, the first version starts at `1900-01-01`,
  `_version` counts down (1 = current), and a second load warns that SCD2's own columns are
  "not in the contract" - those warnings are harmless.
- `olc validate` passing does not mean the contract runs. Run it on sample data.

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
