# Dimensional Modeling (Kimball Patterns)

LakeLogic natively supports Kimball dimensional modeling patterns — SCD Type 1 & 2 dimensions, and all five fact table types — via declarative YAML config.

> **Think of it like a hotel guest register.** SCD1 is like a register that only keeps the guest's current address — if they move, you overwrite it. SCD2 is like a register that keeps *every* address the guest has ever had, with dates showing when each was valid. Fact tables are the records of what actually happened — check-ins, room charges, complaints — tied back to who the guest was *at that point in time*.

!!! info "All YAML blocks below are examples — copy only the patterns that match your use case."

---

## Dimension Types

### SCD Type 1 (Current State Only)

Upsert by primary key. No history tracking. Includes auto-generated surrogate key and unknown member.

```yaml
primary_key: ["product_id"]

materialization:
  strategy: "merge"
  scd1:
    surrogate_key: "product_key"
    surrogate_key_strategy: "hash"     # hash (SHA256, deterministic) | uuid
    unknown_member:
      enabled: true
      surrogate_key_value: "-1"
      # default_values:                # Override auto-inferred defaults
      #   product_name: "Unknown"
```

### SCD Type 2 (Full History)

Tracks every change with validity windows. Auto-generates surrogate keys, version numbers, and change reason.

```yaml
primary_key: ["customer_key"]             # Surrogate (unique per row)
natural_key: ["customer_id"]               # Business key (repeats across versions)

materialization:
  strategy: "scd2"
  scd2:
    timestamp_field: "updated_at"          # Determines record version order
    start_date_field: "valid_from"
    end_date_field: "valid_to"
    current_flag_field: "is_current"
    start_date_default: "1900-01-01"
    end_date_default: "9999-12-31"

    # Track only specific columns (reduces version churn)
    track_columns: ["email", "status", "address"]

    # Auto-generated columns
    surrogate_key: "_sk"                   # SHA256(PK|effective_from)[:16]
    surrogate_key_strategy: "hash"         # hash | uuid
    version_column: "_version"             # ROW_NUMBER per business key
    change_reason_column: "_change_reason" # "initial_load", "email,status", etc.

    # Unknown member (late-arriving fact fallback)
    unknown_member:
      enabled: true
      surrogate_key_value: "-1"
```

**Change reason values:**

| Value | Meaning |
|---|---|
| `"initial_load"` | First appearance of this business key |
| `"email,status"` | Comma-separated list of changed tracked fields |
| `"all"` | No `track_columns` specified (all changes trigger version) |

---

## Fact Table Types

Declare the business purpose via `materialization.fact.type` and LakeLogic auto-enforces Kimball governance rules.

### Transaction Fact

Immutable event log. Cannot be updated. One row per event.

```yaml
materialization:
  strategy: "append"
  partition_by: ["transaction_date"]
  fact:
    type: "transaction"
```

### Periodic Snapshot

State at a point in time. Primary key includes the snapshot date.

```yaml
primary_key: ["account_id", "snapshot_date"]

materialization:
  strategy: "append"
  partition_by: ["snapshot_date"]
  fact:
    type: "periodic_snapshot"
```

### Accumulating Snapshot

Tracks lifecycle of a process through milestone stages. Milestones must flow sequentially.

```yaml
primary_key: ["order_id"]

model:
  fields:
    - name: "placed_date"
      type: "timestamp"
    - name: "shipped_date"
      type: "timestamp"
      nullable: true
      milestone: true
    - name: "delivered_date"
      type: "timestamp"
      nullable: true
      milestone: true

materialization:
  strategy: "merge"
  fact:
    type: "accumulating_snapshot"
    milestone_dates:
      - "placed_date"
      - "shipped_date"
      - "delivered_date"
```

LakeLogic auto-validates: `shipped_date` cannot arrive before `placed_date`.

### Factless Fact

Events with no numeric metrics (e.g., student attendance).

```yaml
primary_key: ["student_id", "class_id", "date"]

materialization:
  strategy: "append"
  partition_by: ["date"]
  fact:
    type: "factless"
    # LakeLogic verifies no metric/numeric columns exist
```

### Aggregate Fact

Pre-summarized metrics. Pair with `rollup:` transform.

```yaml
materialization:
  strategy: "overwrite"
  partition_by: ["month"]
  fact:
    type: "aggregate"
```

---

## Dimension Lookups in SQL

Drive point-in-time SCD2 lookups natively in SQL transforms:

```yaml
transformations:
  - sql: |
      SELECT
        s.*,
        COALESCE(d.customer_key, '-1') AS customer_surrogate_key
      FROM source s
      LEFT JOIN {domain_catalog}.{gold_layer}_sales_dim_customers d
        ON s.customer_id = d.customer_id
        AND s.order_date >= d.effective_from
        AND s.order_date < COALESCE(d.effective_to, '9999-12-31')
    phase: "post"
```

The `'-1'` fallback maps to the unknown member row in the dimension.

---

## Quick Reference

| Pattern | Strategy | Primary Key | Extra Config |
|---|---|---|---|
| SCD1 dim | `merge` | Business key | `scd1:` block |
| SCD2 dim | `scd2` | Surrogate key + `natural_key:` | `scd2:` block |
| Transaction fact | `append` | Event key | `fact: { type: transaction }` |
| Periodic snapshot | `append` | Entity + snapshot date | `fact: { type: periodic_snapshot }` |
| Accumulating snapshot | `merge` | Entity key | `fact: { type: accumulating_snapshot, milestone_dates: [...] }` |
| Factless fact | `append` | Composite key | `fact: { type: factless }` |
| Aggregate fact | `overwrite` | Group-by columns | `fact: { type: aggregate }` |
