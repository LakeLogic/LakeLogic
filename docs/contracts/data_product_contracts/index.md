# Data Product Contracts

A data product contract is a single YAML file that fully describes one table/entity in your lakehouse. The contract IS the pipeline — no imperative code required.

---

## Contract Templates by Layer

Below are full, working **example** contracts for each stage of the medallion architecture. Use these as reference templates to jumpstart your own data definitions.

### Bronze — Raw Ingestion

> Capture everything. Validate nothing. Append immutably.

```yaml
version: 1.0.0
info:
  title: "Bronze {System} {Entity}"
  table_name: "{bronze_layer}_{system}_{entity}"
  target_layer: "bronze"
  domain: "{domain}"
  system: "{system}"
  status: "production"

source:
  type: "landing"
  path: "{data_root}/landing/{system}/{entity}/"
  format: "json"                          # json | csv | parquet
  load_mode: "incremental"                # full | incremental | cdc
  partition:
    format: "y_%Y/m_%m/d_%d"
    lookback_days: 3

server:
  cast_to_string: true                    # Ingest everything as strings (schema-on-read)
  schema_evolution: "append"              # Allow new columns from source
  allow_schema_drift: true

materialization:
  strategy: "append"

lineage:
  enabled: true
```

### Silver — Validated & Enriched

> Clean, deduplicate, transform. Type-safe and trusted.

```yaml
version: 1.0.0
info:
  title: "Silver {System} {Entity}"
  table_name: "{silver_layer}_{system}_{entity}"
  target_layer: "silver"
  domain: "{domain}"
  system: "{system}"
  status: "production"
  classification: "internal"

source:
  type: "table"
  path: "{data_root}/{bronze_layer}_{system}_{entity}"
  format: "delta"
  load_mode: "incremental"
  watermark_field: "_lakelogic_loaded_at"

model:
  fields:
    - name: "{entity_id}"
      type: "long"
      required: true
      primary_key: true
      description: "Primary key"
    - name: "email"
      type: "string"
      pii: true
      masking: "hash"
      classification: "confidential"
    - name: "created_at"
      type: "timestamp"
      required: true

primary_key: ["{entity_id}"]

transformations:
  - phase: "pre"
    deduplicate:
      columns: ["{entity_id}"]
      order_by: "_lakelogic_loaded_at"

quality:
  row_rules:
    - not_null: "{entity_id}"
    - sql: "{entity_id} > 0"
  dataset_rules:
    - unique: "{entity_id}"

materialization:
  strategy: "merge"
  format: "delta"

lineage:
  enabled: true
```

### Gold — Analytics-Ready

> Aggregate, model, serve. Dimensional and performant.

```yaml
version: 1.0.0
info:
  title: "Gold {System} {Entity}"
  table_name: "{gold_layer}_{system}_{entity}"
  target_layer: "gold"
  domain: "{domain}"
  system: "{system}"
  status: "production"

links:
  - name: "customers"
    path: "{data_root}/{silver_layer}_{system}_customers"
    type: "delta"

source:
  type: "table"
  path: "{data_root}/{silver_layer}_{system}_{entity}"
  format: "delta"

transformations:
  - phase: "post"
    sql: >
      SELECT
        o.order_id,
        o.customer_id,
        c.name AS customer_name,
        o.order_date,
        ROUND(o.quantity * o.unit_price, 2) AS line_total
      FROM source o
      LEFT JOIN customers c ON o.customer_id = c.customer_id

model:
  fields:
    - name: "order_id"
      type: "long"
      required: true
      primary_key: true
    - name: "customer_id"
      type: "long"
      required: true
    - name: "customer_name"
      type: "string"
    - name: "order_date"
      type: "date"
    - name: "line_total"
      type: "double"

materialization:
  strategy: "merge"
  format: "delta"

lineage:
  enabled: true
```

### Extraction — Unstructured Data (LLM)

> Convert PDFs, images, and text into structured rows.

```yaml
version: 1.0.0
info:
  title: "Bronze {Entity} Extraction"
  table_name: "{bronze_layer}_{system}_{entity}"
  target_layer: "bronze"

source:
  type: "landing"
  path: "{data_root}/landing/{entity}/*.pdf"

extraction:
  provider: "openai"                      # openai | anthropic | azure_openai | ollama
  model: "gpt-4o"
  temperature: 0.0
  preprocessing:
    content_type: "pdf"                   # pdf | image | audio | video | html | text
    ocr:
      enabled: true
      engine: "azure_di"                  # tesseract | azure_di | textract | google_vision
    chunking:
      strategy: "page"
      max_chunk_tokens: 4000
  output_schema:
    - name: "invoice_number"
      type: "string"
      extraction_task: "extraction"
    - name: "vendor_name"
      type: "string"
      extraction_task: "ner"
    - name: "total_amount"
      type: "float"
      extraction_task: "extraction"
  confidence:
    enabled: true
    method: "field_completeness"
  max_cost_per_run: 25.00
  redact_pii_before_llm: true

materialization:
  strategy: "append"

lineage:
  enabled: true
```

Each template uses `{placeholder}` syntax that auto-resolves from your `_system.yaml` and `_domain.yaml` configuration. See [System Config](./../contracts/system_config.md) for all available placeholders.

---

## Contract Anatomy

Every contract can include these sections (all optional except `version`):

| Section | Purpose | Sub-Page |
|---|---|---|
| `version` / `info` / `metadata` | Identity, ownership, classification | This page |
| `source` | Where to read data from | [Ingestion](ingestion.md) |
| `source.watermark_strategy` | How to track incremental progress | [Watermark Strategies](watermark_strategies.md) |
| `model` | Schema definition (fields, types, PII) | [Schema & Model](../schema_model.md) |
| `transformations` | Data transforms (rename, join, SQL) | [Transformations](transformations.md) |
| `quality` | Validation rules (row + dataset) | [Quality](quality.md) |
| `materialization` | Write strategy (append, merge, SCD2) | [Materialization](materialization.md) |
| `materialization.scd2` / `fact` | Kimball dimensional modeling | [Dimensional Modeling](dimensional_modeling.md) |
| `service_levels` | Contract-level SLO overrides | [SLOs](../slo.md) |
| `quarantine` | Bad row handling + notifications | [Notifications](../notifications.md) |
| `lineage` | Provenance tracking columns | [Schema & Model](../schema_model.md) |
| `compliance` | GDPR, EU AI Act, etc. | [Compliance](../compliance.md) |
| `links` | Reference data for joins | [Ingestion](ingestion.md) |
| `extraction` | LLM-based extraction | [LLM Extraction](../../llm_extraction.md) |
| `external_logic` | Custom Python / notebook | This page |

---

## Pipeline Execution Order

This is the **actual sequence** the LakeLogic engine follows for every contract run:

| Step | Stage | What Happens |
| --- | --- | --- |
| 1 | **Source loading** | Read from source (file/table) |
| 2 | **Pre-transforms** | rename, filter, deduplicate, cast (`phase: "pre"`) |
| 3 | **Schema enforcement** | Cast columns to contract types |
| 4 | **Pre quality rules** | Validate source columns → quarantine failures |
| 5 | **Good/bad split** | Route bad rows to quarantine |
| 6 | **Post-transforms** | derive, lookup, join, SQL, rollup (`phase: "post"`) |
| 7 | **Post quality rules** | Validate derived columns → quarantine failures |
| 8 | **PII masking** | Apply field-level masking strategies |
| 9 | **Lineage injection** | Stamp `_lakelogic_*` columns |
| 10 | **Materialization** | Write to Delta (append/merge/scd2/overwrite) |
| 11 | **Run logging** | Write metadata to `_run_logs` |
| 12 | **Notifications** | Alert on failures, SLO breaches, quarantine |

---

## Multi-Dataset Joins (Links)

Contracts can reference **additional datasets** via `links:` and use them in SQL transforms. This is how you join multiple datasets within a single contract:

```yaml
# Register a reference dataset
links:
  - name: "customers"
    path: "{data_root}/{silver_layer}_{system}_customers"
    type: "delta"

source:
  type: "table"
  path: "{data_root}/{bronze_layer}_{system}_orders"
  load_mode: "incremental"

transformations:
  # Pre: deduplicate orders
  - phase: pre
    sql: >
      SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_date DESC) as rn
        FROM source
      ) WHERE rn = 1

  # Post: enrich with customer data via linked dataset
  - phase: post
    sql: >
      SELECT
        o.order_id, o.customer_id, o.order_date,
        o.quantity, o.unit_price,
        ROUND(o.quantity * o.unit_price * (1.0 - COALESCE(o.discount_pct, 0)), 2) AS line_total,
        c.name AS customer_name,
        c.email AS customer_email,
        COALESCE(c.segment, 'unknown') AS customer_segment
      FROM source o
      LEFT JOIN customers c ON o.customer_id = c.customer_id
```

`links:` datasets are registered as named tables available in SQL. The `source` table is always available as `source`.

---

## Identity & Metadata

```yaml
version: 1.0.0

info:
  title: "Customer Master Data - Silver Layer"
  version: "2.1.0"
  description: "Validated, deduplicated customer records"
  owner: "data-platform-team@company.com"
  contact:
    email: "data-platform@company.com"
    slack: "#data-quality"
  target_layer: "silver"
  status: "production"          # development, staging, production, deprecated
  classification: "confidential" # public, internal, confidential, restricted
  domain: "sales"
  system: "crm"

metadata:
  domain: "sales"
  system: "crm"
  data_layer: "silver"
  pii_present: true
  retention_days: 2555
  cost_center: "CC-1234"
  sla_tier: "tier1"
  run_log_table: "{domain_catalog}._run_logs"

dataset: "customers"            # SQL alias in transformations
primary_key: ["customer_id"]    # Used for merge, dedup, uniqueness
natural_key: ["customer_id"]    # Business key for SCD2 (optional)
tier: "silver"                  # Explicit medallion tier
```

---

## External Logic (Custom Python)

For complex transformations that can't be expressed in SQL/YAML:

```yaml
external_logic:
  type: "python"
  path: "./gold/build_customer_gold.py"
  entrypoint: "build_gold"
  args:
    apply_ml_scoring: true
    model_path: "s3://models/churn_predictor.pkl"
  handles_output: false   # LakeLogic materializes the returned DataFrame
```

The function receives the validated DataFrame and must return a DataFrame:

```python
def build_gold(df, *, contract, engine, **kwargs):
    # Custom logic here
    return df
```
