# How It Works

> **Think of LakeLogic as a spell-checker for your data.**
> You define the rules once (the contract), and LakeLogic applies them automatically every time data flows through your pipeline — flagging problems without losing a single row.

LakeLogic processes data in three clear phases: **Clean → Validate → Enrich**. This page explains each phase and when to use which approach.

---

## Phase 1: Clean (Pre-Processing)

Before checking rules, LakeLogic removes the noise. These steps run **first**, so your quality rules don't waste time on junk data.

| Step | What it does | Real-world example |
|:---|:---|:---|
| `rename` | Align column names | `cust_id` → `customer_id` |
| `filter` | Drop irrelevant rows | `WHERE status != 'deleted'` |
| `deduplicate` | Keep the latest version | Last record per `customer_id` by `updated_at` |
| `trim` / `lower` / `upper` | Standardize text | `"  New York "` → `"new york"` |
| `cast` | Fix data types | `"42"` → `42` |

### Two Ways to Write Transformations

**Structured (business-friendly)** — readable, intent-first. Best for common patterns:

```yaml
transformations:
  - deduplicate:
      on: ["customer_id"]
      sort_by: ["updated_at"]
      order: desc
    phase: pre
```

**SQL (power-user)** — full expressiveness. Best for complex logic:

```yaml
transformations:
  - sql: |
      SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
        FROM source
      ) WHERE rn = 1
    phase: pre
```

Both flavors can be mixed in the same contract. The structured style generates engine-optimized SQL behind the scenes.

---

## Phase 2: Validate (The Quality Gate)

This is where LakeLogic earns its keep. Every row is checked against your **schema** and **quality rules**. Rows that fail are quarantined with clear error reasons — nothing is silently dropped.

### Row-Level Rules

Applied to every individual row:

```yaml
quality:
  row_rules:
    - not_null: email
    - accepted_values:
        field: status
        values: ["ACTIVE", "INACTIVE"]
    - regex_match:
        field: email
        pattern: "^[^@]+@[^@]+\\.[^@]+$"
    - range:
        field: age
        min: 18
        max: 120
```

### Dataset-Level Rules

Applied to the whole table after row validation:

```yaml
  dataset_rules:
    - unique: customer_id
    - null_ratio:
        field: email
        max: 0.05
    - row_count_between:
        min: 1
        max: 1000000
```

**Why this matters:** Dataset rules catch systemic issues — like a source suddenly sending zero rows or an unusual spike in nulls — that individual row checks would miss.

---

## Phase 3: Enrich (Post-Processing)

Only **good** rows reach this phase. Here you derive new fields and join with reference data:

```yaml
transformations:
  - sql: |
      SELECT *, amount * quantity AS revenue
      FROM source
    phase: post
```

Or use structured lookups:

```yaml
transformations:
  - lookup:
      field: country_name
      reference: dim_countries
      on: country_code
      key: code
      value: name
      default_value: "Unknown"
```

**Why this matters:** Enrichment only runs on validated data, so your Gold tables never contain invalid combinations.

---

## Materialization: Where the Data Lands

After processing, LakeLogic writes results to your target format. Choose the right strategy for your use case:

| Strategy | Best for | Analogy |
|:---|:---|:---|
| **`append`** | Transaction tables that keep growing | Adding pages to a journal |
| **`merge`** | Updating existing records (SCD Type 1) | Editing a contact in your phone |
| **`scd2`** | Keeping full history of changes | A filing cabinet with every version |
| **`overwrite`** | Daily snapshots or small summaries | Replacing yesterday's newspaper |

```yaml
materialization:
  strategy: merge
  primary_key: [customer_id]
  target_path: output/silver_customers
  format: parquet
```

---

## External Logic (Gold Patterns)

For advanced Gold layer processing, some teams prefer dedicated Python scripts or notebooks. You can reference them directly in the contract:

```yaml
external_logic:
  type: python
  path: ./gold/build_sales_gold.py
  entrypoint: build_gold
```

LakeLogic will call your function, then optionally validate and materialize the output. This keeps complex business logic in code while still enforcing your quality contract.

---

## Putting It All Together

```text
  Raw Data
     │
     ▼
  ┌──────────────────────────────────────┐
  │  CONTRACT (YAML)                     │
  │                                      │
  │  1. Clean    → rename, dedup, trim   │
  │  2. Validate → schema + rules        │
  │  3. Enrich   → derive, join          │
  │  4. Write    → append/merge/scd2     │
  └──────────────┬───────────────────────┘
                 │
         ┌───────┴───────┐
         ▼               ▼
   Good Data        Quarantine
   (next layer)     (with reasons)
```

**The key insight:** All of this is defined in YAML. No Python validation code to maintain, no scattered business rules, no "it works on my machine" surprises.

---

## What's Next?

- **[Architecture Overview](architecture_diagram.md)** — Visual guide to Bronze → Silver → Gold
- **[Contract Organization](organization.md)** — Structuring contracts for enterprise scale
- **[Tutorials & Examples](https://colab.research.google.com/github/LakeLogic/LakeLogic/blob/main/examples/colab/00_quickstart.ipynb)** — Get hands-on in 5 minutes
