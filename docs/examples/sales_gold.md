# Example: Gold Layer Sales Fact (Pattern)

This example documents a **Gold Fact** pattern. Local materialization to CSV/Parquet (or Delta/Iceberg on Spark) is available. Full contract-level `logic` blocks are planned; use SQL transformations for now.

## The Goal
Move sales data to the **Gold** layer. If a salesperson is missing from our system, we map them to `-1` (Unknown) so we don't lose the sale record.

## The Contract (`gold_fact_sales.yaml`)

### Structured Flavor (business-friendly)

```yaml
transformations:
  - lookup:
      field: staff_key
      reference: gold_dim_staff
      on: salesperson_id
      key: staff_id
      value: staff_surrogate_key
      default_value: -1
  - derive:
      field: processed_at
      sql: "CURRENT_TIMESTAMP"
```

### SQL Flavor (advanced control)

```yaml
version: 1.0.0
info:
  title: Sales Transactions Gold
  target_layer: gold

dataset: gold_fact_sales

links:
  - name: gold_dim_staff
    type: table
    table: main.gold_dim_staff

transformations:
  # ORPHANED KEY HANDLING + FINAL PROJECTION
  # If the salesperson_id isn't in dim_staff, use -1
  - sql: |
      SELECT 
        src.sale_id,
        src.sale_date,
        src.amount,
        COALESCE(staff.staff_surrogate_key, -1) AS staff_key,
        CURRENT_TIMESTAMP as processed_at
      FROM source src
      LEFT JOIN gold_dim_staff staff ON src.salesperson_id = staff.staff_id
    phase: post

materialization:
  strategy: append
  target_path: output/gold_fact_sales
  format: parquet
```

### When to Use Which
- Use **Structured** when you want readable, intent-first transformations for common patterns.
- Use **SQL** when you need window functions, multi-step joins, or complex filtering logic.

### External Logic (Python/Notebook)
If your Gold logic lives in a dedicated script or notebook, reference it directly:

```yaml
external_logic:
  type: python
  path: ./gold/build_sales_gold.py
  entrypoint: build_sales_gold
  handles_output: true
```

See `docs/examples/external_logic.md` for a runnable example.

> Note: Table-based links are supported with the Spark engine in OSS. For local engines, use file-based links (CSV/Parquet).

## How it handles "Orphans"
When LakeGuard runs this:
1.  It looks at the `silver_pos_sales` table.
2.  It tries to join with `dim_staff`.
3.  If a sale exists but the salesperson doesn't, it uses **`-1`**.
4.  **Result**: Your Total Sales numbers are always 100% accurate, even if your meta-data is lagging!

## The Data Journey

```mermaid
graph TD
    A[Silver POS Sales] --> B{🛡️ LakeGuard}
    B -->|Found Staff| C[Gold: Sale + Staff ID]
    B -->|Staff Missing| D[Gold: Sale + -1]
    
    style B fill:#4f46e5,color:#fff
    style D fill:#fbbf24,color:#000
```
