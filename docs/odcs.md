# Open Data Contract Standard (ODCS)

LakeLogic provides **full support for the [Open Data Contract Standard (ODCS) v3.x](https://github.com/bitol-io/open-data-contract-standard)** (Bitol), allowing you to bring existing standard contracts into your data platform and execute them immediately — and to **export** any LakeLogic contract back out as a valid ODCS document.

## How It Works

When LakeLogic reads a dictionary or YAML file, it automatically detects the ODCS fingerprint — specifically `kind: DataContract` **and** an `apiVersion` field. When detected, LakeLogic's internal converter intercepts the payload and translates it transparently into an executable LakeLogic pipeline.

```python
from lakelogic.core.models import DataContract

contract = DataContract(**my_odcs_dict)  # dict → executable contract
contract = DataContract.from_yaml("contract.yaml")
```

```bash
lakelogic run --contract my_odcs_contract.yaml       # run an ODCS contract directly
lakelogic export-odcs --contract native.yaml -o out.odcs.yaml   # LakeLogic → ODCS
```

The converter understands the **full ODCS v3.x document shape**, while staying **backward-compatible** with the legacy simplified flat-`schema` form and with native LakeLogic contracts (which never carry `kind: DataContract` and are passed through untouched).

---

## Import mapping (ODCS → LakeLogic)

### Fundamentals

| ODCS | LakeLogic | Notes |
|---|---|---|
| `apiVersion` (e.g. `v3.0.2`) | `metadata.odcs_api_version` | The **spec** version — preserved, *not* used as the contract version. |
| `version` | `version` | The **contract** version. Falls back to `apiVersion` if absent. |
| `id` | `info.title` (fallback), `metadata.odcs_id` | Round-trip anchor. |
| `name` | `info.title` | Fallback order: `name` → `id` → legacy `dataset`. |
| `status` | `info.status` | |
| `domain` | `info.domain` | |
| `tenant` / `dataProduct` / `tags` | `metadata.*` | Carried so nothing is dropped. |
| `description` (object: `purpose`, `usage`, `limitations`) | `info.description` | Non-empty parts joined. A plain string is also accepted. |
| `customProperties` (non-`lakelogic`) | `metadata.odcs_custom_properties` | |

### Ownership

`team[]` (v3, array of `{username, role}`), `stakeholders[]` (v2), or `roles[]` → `info.owner` (first usable team member's `username`/`name`/`email`; else a role approver).

### `schema[]` — the key mapping

ODCS `schema[]` is an **array of table objects**, each: `name`, `physicalName`, `physicalType`, `description`, `properties[]`. LakeLogic is **one-dataset-per-contract**:

- If a schema entry has a `properties` array → it is a **real ODCS table**; its `properties` become the columns.
- If entries carry `name` + `type` and **no** `properties` → the list is treated as **legacy flat columns** (backward compat).
- With **multiple** table objects, LakeLogic selects the one whose `name` matches the contract `name`/`id` (else the first) and logs a warning naming the skipped tables (no silent drop).
- `physicalName` → `info.table_name`.

**`properties[]` → `model.fields[]`:**

| ODCS property | LakeLogic field | Notes |
|---|---|---|
| `name` | `name` | |
| `logicalType` | `type` | `string→string`, `integer→integer`, `number→double`, `boolean→boolean`, `date→timestamp`, `object`/`array`→`string`. Fallbacks: `physicalType`, legacy `type` (SQL-native types pass through). |
| `required` | `required` | |
| `primaryKey` (+ `primaryKeyPosition`) | top-level `primary_key` (ordered) + `required` | |
| `classification` | `pii` / `sensitive` + `classification` + `masking` | `PII`/personal → `pii=true`; `confidential`/`restricted`/`sensitive`/`secret` → `sensitive=true`; both default `masking: redact`. |
| `criticalDataElement` | `sensitive` | (unless already `pii`) |
| `unique` | dataset `unique` rule | `FieldDefinition` has no `unique` flag, so a dataset uniqueness rule is generated. |
| `partitioned` (+ `partitionKeyPosition`) | `materialization.partition_by` (ordered) | Best-effort. |
| `description` | `description` | |
| `quality[]` | row/dataset rules (see below) | Property-level rules are scoped to that column. |

Direct `pii` / `sensitive` / `masking` keys on a column are also honoured (legacy simplified form).

### `quality[]` (schema-level and property-level)

| ODCS quality | LakeLogic rule |
|---|---|
| `type: sql` / `custom` with a `query`, **no** operator | `quality.row_rules` — per-row predicate SQL |
| `type: sql` / `custom` with a `query` **and** an operator (`mustBe*`) | `quality.dataset_rules` — aggregate SQL + `must_be_*` threshold |
| `type: library`, `rule: nullCount`/`nullCheck` (mustBe 0) | row rule `<col> IS NOT NULL` |
| `type: library`, `rule: duplicateCount`/`uniqueCheck` | dataset `unique` rule |
| `type: library`, `validValues: [...]` | row rule `<col> IN (...)` |
| `type: library`, aggregate (`avg`/`sum`/`min`/`max`/`rowCount`/…) + operator | dataset rule `SELECT <AGG>(<col>) FROM <table>` + `must_be_*` |
| `type: text` | description only — skipped for execution, never errors |

Operator mapping: `mustBeGreaterThan`/`mustBeGreaterOrEqualTo` → `must_be_greater_than`; `mustBeLessThan`/`mustBeLessOrEqualTo` → `must_be_less_than`; `mustBeBetween` → `must_be_between`; `mustBe x` → `must_be_between [x, x]`. `mustNotBe` has no `must_be_*` form and is deferred (logged).

### `slaProperties[]` → `service_levels`

Each `{property, value, unit, element}`. `frequency`/`latency`/`freshness` → `service_levels.freshness` (`"<value><unit>"`, e.g. `1` + `d` → `"1d"`). Other SLA properties are preserved in `metadata.odcs_sla_properties`.

### `servers[]` → `source` (best-effort)

The prod server (else the first) maps to `source`: server `type` → source `type` (object stores/files → `landing`, warehouses/tables → `table`, streams → `stream`); `location`/`path`/`dataset`/`catalog`/`project` → `source.path`; `format` → `source.format`.

### `customProperties.lakelogic` — execution overrides (applied last, they win)

ODCS is a *declarative* standard; it does not say **how** to load data. Put LakeLogic connection/materialization instructions in the official extension block `customProperties.lakelogic` (`source`, `materialization`, `quality`, `tier`, …). These are applied **last** and override the derived mapping. `quality`, `materialization`, and `metadata` are **merged** (rules appended) rather than clobbered.

Finally, any still-unmapped top-level ODCS key is copied through so **nothing is silently lost**.

---

## Full ODCS v3.x example

```yaml
apiVersion: v3.0.2
kind: DataContract
id: urn:lakelogic:silver:customers
name: customers
version: 2.1.0
status: active
domain: customer
tenant: acme
tags: [gold-source, gdpr]

description:
  purpose: Curated customer master.
  usage: Serves the customer-360 product.
  limitations: EU customers only.

team:
  - username: dana.owner@acme.io
    role: owner

slaProperties:
  - property: frequency
    value: 1
    unit: d

servers:
  - server: prod-adls
    type: azure
    environment: prod
    location: abfss://silver@acme.dfs.core.windows.net/customers
    format: delta

schema:
  - name: customers
    physicalName: silver_customers
    physicalType: table
    properties:
      - name: customer_id
        logicalType: integer
        required: true
        primaryKey: true
        primaryKeyPosition: 1
      - name: email
        logicalType: string
        required: true
        classification: PII
        quality:
          - type: library
            rule: nullCount
            mustBe: 0
      - name: status
        logicalType: string
        quality:
          - type: library
            rule: validValues
            validValues: [active, churned, prospect]
      - name: lifetime_value
        logicalType: number
        classification: confidential
      - name: signup_date
        logicalType: date
        partitioned: true
        partitionKeyPosition: 1
    quality:
      - type: sql
        query: "SELECT COUNT(*) FROM silver_customers"
        mustBeGreaterThan: 0
      - type: sql
        query: "lifetime_value >= 0"

# LakeLogic execution instructions (extension block — applied last, wins)
customProperties:
  lakelogic:
    tier: silver
    source:
      type: file
      path: abfss://bronze/customers
      format: parquet
    materialization:
      strategy: merge
```

## Legacy simplified example (still supported)

The original flat-`schema` form remains fully executable — schema entries are treated as columns:

```yaml
kind: DataContract
apiVersion: v3.1.0
dataset: customers
schema:
  - name: id
    type: integer
    required: true
  - name: email
    type: string
    pii: true
customProperties:
  lakelogic:
    tier: silver
    source: { type: file, path: s3://landing/customers/, format: parquet }
    materialization: { strategy: merge, primary_key: [id], target_path: silver.customers }
```

---

## Export (LakeLogic → ODCS)

Any LakeLogic contract can be exported to a valid ODCS v3.x document — the reverse of the mapping above:

```python
odcs_dict = contract.to_odcs()  # method
# or: from lakelogic.core.models import to_odcs; to_odcs(contract)
```

```bash
lakelogic export-odcs --contract contracts/customers.yaml                 # → stdout (YAML)
lakelogic export-odcs --contract contracts/customers.yaml -o out.odcs.json  # → JSON file
```

The export emits `apiVersion: v3.0.2`, `kind`, `id`, `name`, `version`, `status`, `description`, `team[]` (from owner), a single `schema[]` entry with `properties[]` (LakeLogic `type` → `logicalType`, `required`, `primaryKey`, `classification`/`criticalDataElement` from `pii`/`sensitive`, `partitioned`), `quality[]` (row/dataset rules → ODCS `sql`/`library` quality), and `slaProperties[]` (from `service_levels`).

The full LakeLogic execution context (`source`, `materialization`, `tier`, `target_layer`) is written into `customProperties.lakelogic`, so the exported document **round-trips**: re-importing it via `DataContract(**doc)` yields an equivalent, executable LakeLogic contract.
