# Transformations

!!! abstract "Open Lakehouse Contract - the standard"
    LakeLogic implements the **[Open Lakehouse Contract (OLC)](https://lakelogic.github.io/open-lakehouse-contract/)**, the open
    standard for lakehouse data contracts. The authoritative field-level spec for
    **transformations** is the OLC reference: **[transformations reference →](https://lakelogic.github.io/open-lakehouse-contract/reference/transformation/)**.
    This page shows how the LakeLogic runtime *applies* that spec (engine behavior,
    examples, and runtime-specific notes).


LakeLogic is **SQL-first**. If you know SQL, you already know how to transform data in LakeLogic. Every transformation — from simple renames to complex multi-table joins — can be expressed as a SQL statement. The YAML shorthand transforms (rename, filter, cast, etc.) are convenience wrappers that compile down to SQL internally.

> **Bottom line:** Write SQL in your contract, LakeLogic runs it. No Python, no Spark code, no notebooks — just SQL in YAML.

Transformations run **before** (pre) or **after** (post) quality checks. Pre-phase transforms prepare raw data; post-phase transforms enrich validated data.

!!! info "All YAML blocks below are examples — copy only what you need into your contract."

---

## SQL-First Design

The most powerful and flexible transform is the `sql:` block. It gives you full SQL with access to `source` (your main dataset) and any `links:` datasets (reference tables):

```yaml
transformations:
  - sql: |
      SELECT
        o.*,
        ROUND(o.quantity * o.unit_price * (1.0 - COALESCE(o.discount_pct, 0)), 2) AS line_total,
        c.name AS customer_name
      FROM source o
      LEFT JOIN customers c ON o.customer_id = c.customer_id
    phase: "post"
```

> **Why SQL-first?** SQL is the universal data language. Your data analysts, data engineers, and even some business analysts already know it. By putting SQL directly in the contract, there's no translation layer — what you write is what runs.

The YAML shorthand transforms below (`rename:`, `filter:`, `derive:`, etc.) are available for simple operations where writing full SQL would be overkill. **You can mix and match** — use shorthand for the simple stuff, SQL for the complex stuff, all in the same contract.

---

## Execution Order

This is the **actual sequence** the engine follows for transformations:

| Step | Stage | What Happens |
| --- | --- | --- |
| 1 | **Source loaded** | Raw data read from source (file/table) |
| 2 | **Pre-transforms** | rename, filter, deduplicate, cast, trim, select, drop |
| 3 | **Schema enforcement** | Cast columns to contract types |
| 4 | **Pre quality rules** | Validate source columns |
| 5 | **Good/bad split** | Quarantine failing rows |
| 6 | **Post-transforms** | derive, lookup, join, SQL, rollup, pivot |
| 7 | **Post quality rules** | Validate derived columns |

> **Pre-phase** transforms can only reference SOURCE columns.
> **Post-phase** transforms can reference both source AND derived columns.

---

## Pre-Processing Transforms

### Rename Columns

```yaml
# Single rename
- rename:
    from: "cust_id"
    to: "customer_id"
  phase: "pre"

# Multiple renames
- rename:
    mappings:
      "cust_id": "customer_id"
      "email_addr": "email"
      "cust_status": "status"
  phase: "pre"
```

### Filter Rows

```yaml
- filter:
    sql: "customer_id IS NOT NULL AND email IS NOT NULL"
  phase: "pre"
```

### Deduplicate

```yaml
- deduplicate:
    "on": ["customer_id"]
    sort_by: ["updated_at"]
    order: "desc"               # Keep most recent
  phase: "pre"
```

### Select / Drop Columns

```yaml
- select:
    columns: ["customer_id", "email", "age", "status"]
  phase: "pre"

- drop:
    columns: ["internal_notes", "temp_field"]
  phase: "pre"
```

### Cast Types

```yaml
- cast:
    columns:
      customer_id: "long"
      age: "int"
      created_at: "timestamp"
  phase: "pre"
```

### String Operations

```yaml
- trim:
    fields: ["email", "status"]
    side: "both"                 # both | left | right
  phase: "pre"

- lower:
    fields: ["email", "status"]
  phase: "pre"

- upper:
    fields: ["country_code"]
  phase: "pre"
```

### Coalesce

```yaml
- coalesce:
    field: "email"
    sources: ["primary_email", "secondary_email", "backup_email"]
    default: "unknown@example.com"
    output: "email"
  phase: "pre"
```

### Map Values

```yaml
- map_values:
    field: "status"
    mapping:
      "A": "ACTIVE"
      "I": "INACTIVE"
      "P": "PENDING"
    default: "UNKNOWN"
    output: "status"
  phase: "pre"
```

### Split / Explode

```yaml
- split:
    field: "tags"
    delimiter: ","
    output: "tag_array"
  phase: "pre"

- explode:
    field: "tag_array"
    output: "tag"
  phase: "pre"
```

---

## Post-Processing Transforms

### Derive New Fields

```yaml
- derive:
    field: "age_group"
    sql: "CASE WHEN age < 25 THEN 'young' WHEN age < 65 THEN 'adult' ELSE 'senior' END"
  phase: "post"
```

### Lookup (Single Field from Reference)

```yaml
- lookup:
    field: "country_name"
    reference: "dim_countries"     # From links: block
    "on": "country_code"
    key: "code"
    value: "name"
    default_value: "Unknown"
  phase: "post"
```

### Join (Multiple Fields from Reference)

```yaml
- join:
    reference: "dim_products"
    "on": "product_id"
    key: "id"
    fields: ["product_name", "category", "price"]
    type: "left"                  # left | inner | right | full
    prefix: "product_"
    defaults:
      product_name: "Unknown Product"
      category: "Uncategorized"
  phase: "post"
```

### SQL Transform

The most powerful option — full SQL with access to `source` and any `links:` datasets:

```yaml
- sql: |
    SELECT
      o.*,
      ROUND(o.quantity * o.unit_price * (1.0 - COALESCE(o.discount_pct, 0)), 2) AS line_total,
      c.name AS customer_name,
      COALESCE(c.segment, 'unknown') AS customer_segment
    FROM source o
    LEFT JOIN customers c ON o.customer_id = c.customer_id
  phase: "post"
```

### Rollup / Aggregation

```yaml
- rollup:
    group_by: ["customer_segment", "country"]
    aggregations:
      total_customers: "COUNT(*)"
      avg_age: "AVG(age)"
      total_revenue: "SUM(lifetime_value)"
    keys: "customer_id"                    # Track rollup lineage
    rollup_keys_column: "_lakelogic_rollup_keys"
    rollup_keys_count_column: "_lakelogic_rollup_keys_count"
    distinct: true
  phase: "post"
```

### Pivot / Unpivot

```yaml
# Pivot long → wide
- pivot:
    id_vars: ["customer_id"]
    pivot_col: "metric"
    value_cols: ["value"]
    values: ["clicks", "impressions"]
    agg: "sum"
    name_template: "{pivot_alias}"
  phase: "post"

# Unpivot wide → long
- unpivot:
    id_vars: ["customer_id"]
    value_vars: ["clicks", "impressions"]
    key_field: "metric"
    value_field: "value"
    include_nulls: false
  phase: "post"
```

### Bucket (Numeric Banding)

```yaml
- bucket:
    field: "price_band"
    source: "listing_price"
    bins:
      - lt: 250000
        label: "sub_250k"
      - lt: 500000
        label: "250k_500k"
      - lt: 1000000
        label: "500k_1m"
    default: "1m_plus"
  phase: "post"
```

### JSON Extract

```yaml
- json_extract:
    field: "latitude"
    source: "location_coordinates"
    path: "$.latitude"
    cast: "float"
  phase: "post"
```

### Date Operations

```yaml
# Date difference
- date_diff:
    field: "listing_age_days"
    from_col: "creation_date"
    to_col: "event_date"
    unit: "days"                  # days | hours | months
  phase: "post"

# Explode date ranges into daily rows
- date_range_explode:
    output: "snapshot_date"
    start_col: "creation_date"
    end_col: "deleted_at"         # Nullable — defaults to today
    interval: "1d"
  phase: "post"
```
