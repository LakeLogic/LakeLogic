# Ingestion & Cloud

> **Think of ingestion as border control.**
> You don't inspect every detail of what's crossing the border — but you do check passports (schema) and flag anything suspicious. The deep security check happens later (Silver layer).

LakeLogic acts as a **schema gate** for ingestion, ensuring that what lands in your Bronze layer has a known shape and a clean lineage — even if the data itself is messy.

---

## Cloud Storage Support

LakeLogic reads from all major cloud storage providers:

| Provider | Path format |
|:---|:---|
| **Amazon S3** | `s3://my-bucket/raw_data/` |
| **Google GCS** | `gs://my-bucket/raw_data/` |
| **Azure ADLS** | `abfss://container@account.dfs.core.windows.net/path/` |
| **Local files** | `./data/raw_customers.parquet` |

---

## Ingestion Mode (Raw → Bronze)

When moving data from external sources into Bronze, you want schema protection without heavy validation:

```yaml
server:
  type: gcs
  path: gs://landing-zone/daily_extract/
  mode: ingest
  schema_policy:
    evolution: append
```

### Schema Evolution Strategies

| Strategy | Behaviour | Best for |
|:---|:---|:---|
| **`strict`** | Fails if incoming file doesn't match the schema exactly | Stable, well-governed sources |
| **`append`** | Automatically allows new columns to pass through | APIs that add fields over time |
| **`merge`** | Upgrades the schema to the greatest common denominator | Multi-file ingestion with mixed schemas |

---

## Schema Drift Protection

> **Think of schema drift like a supplier changing their packaging without telling you.**
> The product inside might be fine, but if your warehouse is set up for boxes and they start sending tubes, things break.

LakeLogic detects unknown or missing fields during ingestion and can trigger alerts:

```yaml
server:
  mode: ingest
  schema_policy:
    evolution: append
    unknown_fields: quarantine  # Alert when drift is detected

quarantine:
  notifications:
    - type: slack
      channel: "#data-alerts"
      on_events: ["schema_drift"]
```

**Why this matters:** You catch schema changes the moment they arrive, not three days later when a dashboard breaks.

---

## Cleanse-on-Arrival

Bronze data often arrives with duplicates or soft-deleted records. Clean them at the gate:

```yaml
transformations:
  - sql: |
      SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
        FROM source
        WHERE is_deleted = false
      ) WHERE rn = 1
    phase: pre
```

**Why this matters:** A lean Bronze layer saves storage costs and compute time in every downstream layer.

---

## Example: Azure to Bronze

```yaml
version: 1.0.0
info:
  title: CRM Ingestion
  target_layer: bronze

server:
  type: adls
  path: abfss://raw@datalake.dfs.core.windows.net/crm/
  mode: ingest
  schema_policy:
    evolution: append

model:
  fields:
    - name: user_id
      type: long
    - name: signup_date
      type: timestamp
```

No quality rules here — we want an exact copy of the source. But we still define the expected schema to catch drift early.

---

## The "All Strings" Bronze Pattern

> **Think of this as "photograph the evidence before you touch it."**

Many high-scale teams read every column as a `string` in Bronze:

```yaml
server:
  mode: ingest
  cast_to_string: true
```

**Why this works:**

| Benefit | How |
|:---|:---|
| **Zero ingestion failures** | You never crash because an API sent `"N/A"` into a numeric field |
| **100% data capture** | Every value is preserved exactly as the source sent it |
| **Fix in Silver** | Casting and cleaning happen in Silver, where quarantine catches rows that won't convert |

---

## What's Next?

- **[Reprocessing & Partitioning](reprocessing.md)** — Handling late data and backfills
- **[Automatic Credentials](automatic_credentials.md)** — Zero-config cloud authentication
