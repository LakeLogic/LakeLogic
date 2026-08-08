# Annotated reference contracts

Complete, **copy-paste** LakeLogic data contracts - one per medallion layer - with
an inline `#` comment on every option explaining what it does and its allowed
values. Instead of hunting across doc pages, open one file and see the whole
surface area in context.

**How to use them:** copy the whole file for the layer you're building, then
**delete the blocks you don't need**. A real contract is usually 20-40 lines; these
are deliberately maximal. They use the lean **Open Lakehouse Contract (OLC)**
vocabulary (`model` / `fields` / `type` / `row_rules`), not the verbose ODCS names.

Every option shown is a **real** field that exists in the code. A handful of
options that can't coexist in a single runnable file (e.g. SCD2 vs merge) are kept
in the file but commented out, with a note explaining why.

## The three references

- **`bronze.annotated.yaml`** - raw landing -> typed, deduplicated, quarantined
  Bronze table. Covers the source block (all `type`s, a cloud `s3://` path you swap
  for `gs://`/`abfss://`, `load_mode` full/incremental/cdc, watermark strategies),
  the headline **schema-evolution** policy (`cast_to_string`, `evolution`,
  `unknown_fields`), fields with `pii`/`sensitive`/`masking`/`security_groups` and a
  nested dotted-path field, `deduplicate`, quarantine (`include_error_reason`),
  append materialization, and the data-mesh `domain`/`system` coordinates.

- **`silver.annotated.yaml`** - clean + conform + enrich into a trustworthy table.
  Covers pre/post `transformations` (rename, trim, lower, cast, deduplicate, filter,
  join, derive - plus a commented gallery of every other transform), full `quality`
  (row + dataset rules with severity/category/phase), PII **masking-as-execution**,
  `service_levels` (freshness/availability/row_count), merge materialization with a
  full SCD2 alternative shown commented, and `lineage`.

- **`gold.annotated.yaml`** - business-ready mart: an aggregate KPI table. Covers the
  aggregate transform, business-metric fields, cross-domain `upstream` dependencies,
  merge materialization with Kimball `fact` governance, `service_levels`, and
  `downstream` consumers (dashboard / api / ml_model) that complete end-to-end
  lineage.

## They are validated - they run

Each file parses as a `DataContract`, lints with **0 critical** findings, and
Bronze/Silver/Gold each execute end-to-end against the tiny CSV samples in
`samples/` (outputs land in `_out/`). Reproduce with the venv Python:

```bash
py=C:/_Personal/_SaaS/LakeLogic_SaaS/.venv/Scripts/python.exe

# Bronze: 5 rows -> dedup drops 1, quarantine catches 1 -> 3 good
"$py" -c "from lakelogic.cli.main import app; app()" run \
  --contract examples/reference/bronze.annotated.yaml \
  --source   examples/reference/samples/bronze_orders.csv \
  --output-good examples/reference/_out/bronze_good.csv \
  --output-bad  examples/reference/_out/bronze_bad.parquet

# Silver: rename+trim+lower+cast+dedup+filter, join currency_rates, derive amount_gbp,
#         PII partial-mask on customer_email -> 4 good
"$py" -c "from lakelogic.cli.main import app; app()" run \
  --contract examples/reference/silver.annotated.yaml \
  --source   examples/reference/samples/silver_orders.csv \
  --output-good examples/reference/_out/silver_good.csv \
  --output-bad  examples/reference/_out/silver_bad.parquet

# Gold: aggregate 5 trips -> 4 city/day KPI rows
"$py" -c "from lakelogic.cli.main import app; app()" run \
  --contract examples/reference/gold.annotated.yaml \
  --source   examples/reference/samples/gold_trips.csv \
  --output-good examples/reference/_out/gold_good.csv \
  --output-bad  examples/reference/_out/gold_bad.parquet
```

> Note: `--output-bad` uses `.parquet` because quarantine rows carry a nested
> error-reason column that CSV can't serialise. Quote the JSON `attributes` column
> if you edit `samples/bronze_orders.csv`. Contract loading reads files as cp1252 on
> Windows, so keep contract files ASCII-only.
