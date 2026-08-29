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
│ diagnose   Read-only diagnostics on already-written data.                 │
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

### Diagnostics

Where `validate` and `lint` inspect **contracts**, `lakelogic diagnose` inspects **data
that was already written** — to answer questions a contract cannot, such as "did this
pipeline corrupt a column before the fix landed?" Every command in this group **defaults
to read-only**. `double-hash` cannot repair at all — SHA-256 destroyed the information.
`scd2` can, because nothing was destroyed there, but it writes only when you give it an
explicit output path.

#### `lakelogic diagnose double-hash`

Detect a masked column that was hashed **again** downstream.

Masking is applied write-side, per contract run, and the silver/gold templates propagate
the `masking:` strategy. A field hashed in bronze could therefore be hashed a second time
in silver — `sha256(salt + sha256(salt + value))` — and a third time in gold. Nothing
raised and nothing was logged; the only symptom was that the same person's key stopped
matching across layers, so cross-layer joins on that key silently returned nothing.

`MaskingEngine`'s idempotence guard prevents **new** double-hashing. This command is for
data written before that guard existed.

```bash
lakelogic diagnose double-hash \
  --upstream  bronze/riders.parquet \
  --downstream silver/riders.parquet \
  --column rider_key \
  --key    rider_id
```

**The check.** If upstream stores `B` and downstream stores `S` for the same entity, then
downstream is double-hashed **iff** `H(salt + B) == S`. Three per-row outcomes, kept
distinct:

| Outcome | Meaning |
|:--|:--|
| `double_hashed` | `H(salt + B) == S` — downstream re-hashed an already-hashed value. Broken. |
| `consistent` | `B == S` — downstream carried the upstream value through. Correct, and salt-independent. |
| `indeterminate` | Neither matched. A different salt, a different transformation, or a re-sourced column. **Unknown — not clean.** |

The column-level `verdict` is `double_hashed`, `consistent`, `indeterminate`, `mixed`
(more than one outcome present), `inconclusive`, or `no_overlap` — but the **counts** are
the authority. A backfill that straddled the fix leaves a column part-damaged, so a
single boolean per column would hide real damage. Rows that do not join, hold a null, or
sit on an ambiguous upstream key are excluded from the verdict and reported separately.

**Salt.** Read from `$LAKELOGIC_PII_SALT` (the same variable the pipeline masks with), or
`--salt`. Without it the salted form cannot be computed: the command falls back to the
unsalted form, reports **which** form matched (`salt_match`), and — if any row comes back
indeterminate — marks the whole result `inconclusive`. "No match under an unknown salt"
is never reported as clean.

!!! danger "This does not repair data"
    SHA-256 is one-way. A double-hashed column **cannot** be recovered from itself — not
    by this command, not by any other. There is no in-place fix and none is offered.

    What the command gives you is a *reason to reprocess*: the upstream value `B` is
    still intact, so re-running the downstream contract from its upstream — with the
    idempotence guard in place — rewrites the column correctly. That is an ordinary
    pipeline run, not a data migration.

**Key options:**

| Flag | Short | Description |
|:-----|:------|:------------|
| `--upstream` | `-u` | Upstream (e.g. bronze) file or directory — the layer that hashed first |
| `--downstream` | `-d` | Downstream (e.g. silver/gold) file or directory |
| `--column` | `-c` | The masked column to check |
| `--key` | `-k` | An **unmasked** column identifying the same entity in both layers |
| `--downstream-column` | | Downstream name of the column, if it was renamed |
| `--salt` | | Masking salt (defaults to `$LAKELOGIC_PII_SALT`) |
| `--format` | `-f` | `text` or `json` |
| `--fail-on-damage` | | Exit non-zero if any row is *proven* double-hashed (for CI) |

The same check is available in Python, engine-agnostically (Polars, pandas, Spark,
DuckDB, or plain dicts):

```python
from lakelogic.core.masking_diagnostics import diagnose_double_hashing

result = diagnose_double_hashing(
    bronze_df, silver_df, column="rider_key", join_key="rider_id",
)
print(result.double_hashed_rows, result.consistent_rows, result.indeterminate_rows)
print(result.render())
```

#### `lakelogic diagnose scd2`

Find — and optionally repair — SCD2 intervals corrupted by a **late-arriving** row.

Before the late-arrival fix, a change date landing inside an already-closed interval was
appended to the end of history rather than slotted in. One late row produced three
corruptions at once:

```text
d1 equire   2024-01-01 → 2024-01-04  is_current=False
d1 notified 2024-01-03 → 9999-12-31  is_current=True    ← a 3 Jan fact became current
d1 serius   2024-01-04 → 2024-01-03  is_current=False   ← effective_to BEFORE effective_from
```

plus overlapping windows — `equire` and `notified` are both valid on 3 Jan.

```bash
lakelogic diagnose scd2 --table gold/dim_driver.parquet --key driver_id
```

**The defect taxonomy.** Each is detected independently, per row, after ordering a key's
versions by `effective_from`:

| Defect | Detected by | Repair |
|:--|:--|:--|
| `inverted` | `effective_to < effective_from` | ends at the next version's start (or reopens, if last) |
| `overlapping` | `effective_to` **>** the next version's `effective_from` | ends at the next version's start |
| `is_current_wrong` | flag on a non-latest row, on several rows, or on none while the latest is open | flag moves to the single latest open version |
| `unrepairable` | the key cannot be ordered | **nothing** — reported with a reason |

!!! warning "Gaps are never closed"
    A record deleted and later re-added legitimately leaves `effective_to` **before** the
    next version's `effective_from`. That hole is the fact being recorded. Only a
    *strictly greater* end date is an overlap; `to == next from` (contiguous) and
    `to < next from` (a gap) are both left alone, and the report counts how many of each
    it preserved. A conservative repair that eats real history is worse than the bug.

!!! info "Why this one is repairable, unlike `double-hash`"
    Nothing was destroyed. Every `effective_from` survived the bug, and the correct
    intervals follow from it: a version ends where the next begins, and the latest holds
    `is_current`.

    **`effective_from` is therefore never modified**, so the surrogate key
    `sha256(pk | effective_from)` is byte-identical afterwards and every fact row already
    holding an SK still resolves. `_version` is derived by ranking on `effective_from`,
    which the repair does not reorder, so it needs no rewrite either. Only `effective_to`
    and the current-flag column are ever written.

**Ambiguity is reported, never guessed.** Two versions of one key sharing an
`effective_from`, or a boundary that will not parse, make the key unorderable. Those keys
are listed as `unrepairable` with the reason and are left completely untouched rather than
given an invented order.

To repair, name an output path — omitting it keeps the run read-only:

```bash
lakelogic diagnose scd2 -t gold/dim_driver.parquet -k driver_id \
  --repair-out gold/dim_driver.repaired.parquet
```

| Option | Short | Purpose |
|:--|:--|:--|
| `--table` | `-t` | The SCD2 dimension file or directory |
| `--key` | `-k` | Business key column (repeat for a composite key) |
| `--effective-from` | | Version-start column (default `effective_from`) — never modified |
| `--effective-to` | | Version-end column (default `effective_to`) |
| `--current-flag` | | Live-row column (default `is_current`) |
| `--open-value` | | Open-interval sentinel (default `9999-12-31`) |
| `--repair-out` | | Explicit opt-in to writing: path for the repaired copy |
| `--allow-in-place` | | Permit `--repair-out` to overwrite the source table |
| `--allow-spark-collect` | | Consent to collecting a Spark dimension to the driver |
| `--format` | `-f` | `text` or `json` |
| `--fail-on-defect` | | Exit non-zero if any defect is found (for CI) |

Also available in Python, on Polars / pandas / DuckDB / plain dicts. Diagnosis and repair
are separate: the default returns `repaired_frame is None`, and the input frame is never
mutated.

```python
from lakelogic.core.scd2_diagnostics import diagnose_scd2

result = diagnose_scd2(dim_df, primary_key="driver_id")
print(result.defect_counts)   # {'inverted': 1, 'overlapping': 2, ...}
print(result.render())

fixed = diagnose_scd2(dim_df, primary_key="driver_id", repair=True).repaired_frame
```

Spark is deliberately **not** handled implicitly: ordering every version of every key
requires a full collect to the driver, which can OOM on a large dimension. The call raises
`Scd2SparkCollectRequired` explaining this until you pass `allow_collect=True`.

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
