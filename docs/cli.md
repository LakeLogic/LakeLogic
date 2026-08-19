# CLI Reference

The LakeLogic CLI is the high-efficiency entry point for enforcing your data contracts.
It is designed for **Speed-to-Production** and **Engine Portability**.

## Strategic Value

- **Developer Productivity:** Bootstrap production-ready contracts from raw data in seconds.
- **Infrastructure Optionality:** Use the `--engine` flag to swap between Polars (local speed) and Spark (cluster scale) with zero code changes.
- **Audit Readiness:** Every execution generates a run summary for instant reconciliation.

---

## Invoking the CLI

Run `lakelogic` with no arguments to display the full help page:

```bash
lakelogic
```

```
 Usage: lakelogic [OPTIONS] COMMAND [ARGS]...

 LakeLogic — Consistent Data Contracts across engines.
 ...

┌─ Contract Execution ──────────────────────────────────────────────────────┐
│ run        Run a data contract against a source file.                     │
│ bootstrap  Bootstrap contracts and registry from a landing zone.          │
└───────────────────────────────────────────────────────────────────────────┘
┌─ Data Tooling ────────────────────────────────────────────────────────────┐
│ generate   Generate synthetic data from a contract definition.            │
│ import-dbt Import dbt schema.yml / sources.yml -> LakeLogic contract YAML.│
└───────────────────────────────────────────────────────────────────────────┘
┌─ Governance ──────────────────────────────────────────────────────────────┐
│ registry   Validate & inspect the mesh registry (_domain/_system.yaml).   │
│ lint       Lint contracts for governance issues.                          │
└───────────────────────────────────────────────────────────────────────────┘
┌─ Environment Setup ───────────────────────────────────────────────────────┐
│ setup-oss  Pre-install DuckDB extensions & check OSS dependencies.        │
└───────────────────────────────────────────────────────────────────────────┘
┌─ Help ────────────────────────────────────────────────────────────────────┐
│ help       Show contextual help for LakeLogic commands.                   │
└───────────────────────────────────────────────────────────────────────────┘
```

Commands are grouped into logical sections matching the Databricks CLI convention.
Use `lakelogic [COMMAND] --help` for full option detail on any command.

---

## Command Groups

### Contract Execution

#### `lakelogic run`

Validates a source dataset against a contract and (optionally) materializes clean output.

```bash
lakelogic run \
  --contract contract.yaml \
  --source data.csv
```

**Key options:**

| Flag | Short | Description |
|:-----|:------|:------------|
| `--contract` | `-c` | Path to the YAML contract |
| `--source` | `-s` | Input file (CSV / Parquet) or table name for warehouse engines |
| `--engine` | `-e` | Engine: `polars`, `spark`, `snowflake`, `bigquery` |
| `--stage` | | Apply stage overrides from the contract's `stages` block (e.g., `bronze`, `silver`) |
| `--output-good` | | Save good records to CSV / Parquet |
| `--output-bad` | | Save quarantined records to CSV / Parquet |
| `--output-format` | | `csv` or `parquet` (defaults to CSV or inferred from extension) |
| `--materialize` / `--no-materialize` | | Write good data to the contract materialization target |
| `--materialize-target` | | Override the materialization target path |
| `--verbose` | `-v` | Enable debug logging |
| `--trace` | | Display a step-by-step execution trace in the terminal |

> **Spark note:** `--output-good/--output-bad` are written with the Spark writer and produce a directory of part files — standard Spark behaviour.

**Examples:**

```bash
# Polars (default — fastest local)
lakelogic run --contract orders.yaml --source data/orders.csv

# DuckDB with quarantine output
lakelogic run --engine duckdb --contract orders.yaml \
  --source data/orders.parquet \
  --output-good good.parquet --output-bad quarantine.parquet \
  --output-format parquet

# Snowflake (table-only)
lakelogic run --engine snowflake --contract contract.yaml \
  --source table:ANALYTICS.SILVER.CUSTOMERS

# Materialise clean records and print a full trace
lakelogic run --contract orders.yaml --source data/orders.csv \
  --materialize --trace

# Apply a stage override
lakelogic run --contract pipeline.yaml --source data.csv --stage silver
```

---

#### `lakelogic bootstrap`

Scans a landing zone directory, infers schema from sample files, and generates:

- A ready-to-use **contract YAML** per entity
- A **`_registry.yaml`** that maps entities to their contracts

This is the **Governance Accelerator** for Day 1 compliance.

```bash
lakelogic bootstrap \
  --landing data/landing \
  --output-dir contracts/ \
  --registry contracts/_registry.yaml \
  --format csv \
  --pattern "*.csv"
```

**Key options:**

| Flag | Description |
|:-----|:------------|
| `--landing` | Landing zone root path |
| `--output-dir` | Directory to write generated contracts |
| `--registry` | Output path for the registry YAML |
| `--format` | Input file format: `csv`, `parquet`, `json` |
| `--pattern` | File glob pattern (default `*.csv`) |
| `--layer` | Layer prefix for dataset names (default `bronze`) |
| `--sample-rows` | Rows to sample for schema inference (default 1000) |
| `--sync` | Sync an existing registry with new landing data |
| `--sync-update-schema` | Add new columns to existing contracts |
| `--sync-overwrite` | Overwrite existing contracts entirely |
| `--profile` | Generate a DataProfiler report per entity |
| `--detect-pii` | Detect PII using Presidio and tag fields |
| `--suggest-rules` | Auto-suggest quality rules from data profile |
| `--profile-output-dir` | Directory for profile JSON reports |
| `--pii-sample-size` | Sample values per column for PII detection (default 50) |

**Sync mode** — align an existing registry as new data arrives:

```bash
lakelogic bootstrap \
  --landing data/landing \
  --output-dir contracts/ \
  --registry contracts/_registry.yaml \
  --sync --sync-update-schema
```

**PII + Profile + rule suggestion in one pass:**

```bash
lakelogic bootstrap \
  --landing data/landing \
  --output-dir contracts/ \
  --registry contracts/_registry.yaml \
  --profile --detect-pii --suggest-rules \
  --profile-output-dir reports/
```

> Requires `lakelogic[profiling]` for `--profile` and `--detect-pii`.

---

### Data Tooling

#### `lakelogic generate`

Generates **synthetic test data** from a contract definition.
Respects field types, nullability, `accepted_values`, and range constraints.
Use `--invalid-ratio` to inject intentionally bad rows for validating your quarantine pipeline.

```bash
lakelogic generate --contract orders.yaml --rows 1000 --output sample.parquet
```

**Key options:**

| Flag | Short | Description |
|:-----|:------|:------------|
| `--contract` | `-c` | Path to the contract YAML file |
| `--rows` | `-n` | Number of rows to generate (default 100) |
| `--output` | `-o` | Output file path (CSV / Parquet / JSON) |
| `--format` | `-f` | Output format: `parquet`, `csv`, `json` (default `parquet`) |
| `--engine` | `-e` | DataFrame engine: `polars` (default `polars`) |
| `--invalid-ratio` | | Fraction of rows that intentionally break rules (0.0–1.0) |
| `--seed` | | Random seed for reproducibility |
| `--preview` | | Rows to print to console (default 5; `0` = silent) |

**Examples:**

```bash
# Generate 1 000 clean rows as Parquet
lakelogic generate --contract orders.yaml --rows 1000 --output sample.parquet

# Inject 10 % bad rows to test quarantine logic
lakelogic generate --contract orders.yaml --rows 500 \
  --invalid-ratio 0.1 --format csv --output orders_with_errors.csv

# 200 rows with Polars, print 10 preview rows, reproducible
lakelogic generate --contract contracts/events.yaml \
  --rows 200 --engine polars --seed 42 --preview 10

# Dry-run without saving — just print to console
lakelogic generate --contract orders.yaml --rows 50 --preview 50
```

---

#### `lakelogic import-dbt`

Imports a **dbt `schema.yml` or `sources.yml`** file and converts its model definitions
into LakeLogic contract YAMLs. Eliminates duplicate schema maintenance across dbt and LakeLogic.

```bash
lakelogic import-dbt \
  --schema models/schema.yml \
  --output contracts/
```

**Key options:**

| Flag | Short | Description |
|:-----|:------|:------------|
| `--schema` | | Path to the dbt `schema.yml` or `sources.yml` file |
| `--model` | `-m` | Import a single model by name; omit to import all models |
| `--source-name` | | dbt source name (for `sources.yml` files) |
| `--source-table` | | dbt source table name (for `sources.yml` files) |
| `--output` | `-o` | Output path: a `.yaml` file for a single contract, or a directory for batch import |
| `--overwrite` / `--no-overwrite` | | Overwrite existing contracts (default: skip) |
| `--dry-run` | | Print generated YAML to console without writing files |
| `--verbose` | `-v` | Verbose output |

**Examples:**

```bash
# Import a single dbt model
lakelogic import-dbt \
  --schema models/schema.yml \
  --model customers \
  --output contracts/

# Import all models in a schema file
lakelogic import-dbt \
  --schema models/schema.yml \
  --output contracts/

# Import a dbt source table
lakelogic import-dbt \
  --schema models/sources.yml \
  --source-name raw --source-table orders \
  --output contracts/

# Dry-run — preview the generated YAML without writing
lakelogic import-dbt \
  --schema models/schema.yml \
  --model customers \
  --dry-run
```

---

### Governance

Validate and inspect the **mesh registry** — the `_domain.yaml` / `_system.yaml` files
that sit *above* individual contracts and supply their shared defaults. See
[Resolution & Inheritance](contracts/inheritance.md) for how those files combine.

**Why it matters**

- **Catch drift before it ships.** A typo (`server_defaults` instead of `server`), a
  contract path that no longer exists, or a system whose `domain` disagrees with its
  folder — all become a red line in CI instead of a silent production surprise.
- **Answer "where did this setting come from?"** `explain` shows, per key, whether a
  value is declared on the system, inherited from the domain, or locked by the domain —
  so governance decisions are auditable, not archaeology.
- **One command in CI** guards an entire estate of hundreds of contracts.

#### `lakelogic registry validate`

Structurally and referentially validate a file or a whole tree. Exit code `0` = clean,
non-zero = at least one error (or a warning under `--strict`) — drop it straight into CI.

```bash
# Validate one domain's systems…
lakelogic registry validate domains_rideflow/marketplace

# …or the entire mesh, failing the build on warnings too
lakelogic registry validate domains_rideflow --strict
```

Checks: unknown/misplaced keys, wrong types, missing identity, duplicate contract
entities, every `contracts:` path resolves to a real file, and domain-identity agreement
between a `_system.yaml` and its sibling `_domain.yaml`.

#### `lakelogic registry explain`

Show where each governance/identity key on a system comes from — the resolved-origin
view of the inheritance chain, backed by the real domain → system merge.

```bash
lakelogic registry explain domains_rideflow/marketplace/rideflow/_system.yaml
lakelogic registry explain .../rideflow/_system.yaml --deep          # per-leaf origins
lakelogic registry explain .../rideflow/_system.yaml --key slo.freshness   # trace one path
lakelogic registry explain .../rideflow/_system.yaml --env dev       # what varies per environment
```

```
  marketplace / rideflow
  KEY                  ORIGIN           WHY
  ──────────────────────────────────────────────
  slo                  domain           inherited
  cost                 system+domain    deep-merged
  domain               domain           domain-locked
  materialization      system           declared
```

With `--deep`, provenance drills into merged blocks — answering "why is
`slo.freshness.bronze.max_delay_minutes` **30** here?" with `system / overridden` while its
sibling `check_column` reads `domain / inherited`.

#### `lakelogic registry schema`

Emit the JSON Schema for a manifest — wire it into your editor or a language-agnostic
CI validator.

```bash
lakelogic registry schema domain
lakelogic registry schema system --output schemas/system-manifest.schema.json
```

The generated schemas are also checked into the repo under
[`schemas/registry/`](https://github.com/lakelogic/LakeLogic/tree/main/schemas/registry) —
so non-Python tooling can validate `_domain.yaml` / `_system.yaml` without installing
LakeLogic.

> Validating an individual **contract** (not the registry)? Use `lakelogic validate
> --contract path.yaml` for structural + gate checks, or the portable `olc validate` from
> the [Open Lakehouse Contract CLI](https://lakelogic.github.io/open-lakehouse-contract/reference/cli/).

---

### Environment Setup

#### `lakelogic setup-oss`

Pre-installs DuckDB extensions (Iceberg, Delta, cloud drivers) and checks all OSS
dependencies so they are available **offline and at job runtime** — critical for
air-gapped or ephemeral compute environments.

```bash
lakelogic setup-oss
```

Run this once after installing `lakelogic[duckdb]` or `lakelogic[polars]`.
It verifies `deltalake` is installed and warms DuckDB's extension cache.

---

### Help

#### `lakelogic help`

Prints short usage guidance and examples in the terminal.

```bash
lakelogic help
lakelogic help driver
lakelogic help bootstrap
```

For full option lists, use the `--help` flag on any command:

```bash
lakelogic run --help
lakelogic generate --help
lakelogic import-dbt --help
```

---

## Pipeline Driver

The registry-driven driver is exposed as the separate entry point `lakelogic-driver`.
It orchestrates Bronze → Silver → Gold pipelines from a `_registry.yaml`.

```bash
lakelogic-driver \
  --registry examples/insurance_elt/contracts/insurance/_registry.yaml \
  --reference-registry examples/insurance_elt/contracts/shared/reference/_registry.yaml \
  --gold-registry examples/insurance_elt/contracts/insurance/warehouse/_registry.yaml \
  --layers reference,bronze,silver,gold \
  --window last_success
```

See [Driver Reference](driver.md) for the full option list.

---

## Windows Notes

### Console Encoding

LakeLogic automatically reconfigures `stdout` and `stderr` to **UTF-8** on Windows
at startup. This means:

- No need to set `PYTHONIOENCODING=utf-8` manually
- Rich panel borders, Unicode arrows, and emoji in help text render correctly
- Works in `cmd.exe`, PowerShell, and Windows Terminal alike

This is handled inside the package and requires no external wrapper scripts.

### Making `lakelogic` Available on PATH

After `pip install lakelogic`, the `lakelogic.exe` script lands in Python's user scripts
directory. If your shell cannot find it, add that directory to your `PATH` **once**:

```cmd
:: For Python 3.13 (adjust version as needed)
setx PATH "%USERPROFILE%\AppData\Roaming\Python\Python313\Scripts;%PATH%"
```

Open a **new** terminal and `lakelogic` will be available bare.

> **Developer installs:** When working from a cloned repo with `pip install -e .`,
> the same scripts directory is used. To isolate dependencies in a virtual environment
> see the [Developer Installation](installation.md#developer-installation) guide.
