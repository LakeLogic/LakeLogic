# Multi-Cloud Ingestion ☁️

LakeGuard can act as a **schema gate** for ingestion. For local and OSS use, it focuses on validating and quarantining data before it reaches Bronze.

## 1. Cloud Storage Support
LakeGuard adapters can read from cloud-native paths in hosted environments, but the open-source demo currently focuses on **local files**.

-   **Amazon S3 (Simple Storage Service)**: `s3://my-bucket/raw_data/`
-   **Google GCS (Google Cloud Storage)**: `gs://my-bucket/raw_data/`
-   **Azure ADLS (Azure Data Lake Storage)**: `abfss://container@account.dfs.core.windows.net/path/`

## 2. The "Ingestion" Mode (Raw to Bronze)

When moving data from external sources (Raw) into your **Bronze** layer, you might not want complex transformations, but you **always** want to protect your schema.

```yaml
server:
  type: gcs
  path: gs://landing-zone/daily_extract/
  mode: ingest # Tells LakeGuard to focus on Ingestion
  schema_evolution: append # Allow new columns, but don't break old ones
```

> Note: The `server` block is metadata for remote storage in the OSS release, but ingestion controls (`mode`, `schema_evolution`, `cast_to_string`) are now enforced locally.

### Schema Evolution Strategies
| Strategy | Behavior |
| :--- | :--- |
| **`strict`** | Job fails if the incoming file doesn't match the Bronze table exactly. |
| **`append`** | Automatically adds new columns to the Bronze table if they appear in the source. |
| **`merge`** | Upgrades the table schema to the "greatest common denominator" of all files. |

> Note: In the OSS runtime, `append` and `merge` allow unknown columns to pass through locally. Type unification is handled by your downstream table engine.

## 3. Schema Drift Protection
Schema drift is now detected during ingestion. Unknown or missing fields are recorded in the run report and can trigger notifications.

```yaml
server:
  mode: ingest
  schema_evolution: append
  allow_schema_drift: false  # send schema_drift alerts when drift is detected
```

```yaml
quarantine:
  notifications:
    - type: slack
      channel: "#data-alerts"
      on_events: ["schema_drift"]
```

## 4. Cleanse-on-Arrival (Deduplication & Filtering)

Bronze data is often delivered with duplicates or "deleted" flags from source systems. LakeGuard allows you to cleanse this data the moment it arrives.

```yaml
transformations:
  - sql: |
      SELECT * FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) AS rn
        FROM source
        WHERE is_deleted = false
      ) AS t
      WHERE rn = 1
    phase: pre
```

This "Pre-Processing" ensures that your Bronze layer stays lean and accurate, saving storage costs and compute time in downstream layers.

> Note: Window-function SQL (like `ROW_NUMBER`) is supported in DuckDB and Spark engines.

---

## Example: Landing Azure Data to Bronze

```yaml
version: 1.0.0
info:
  title: CRM Ingestion
  target_layer: bronze

server:
  type: adls
  path: abfss://raw@datalake.dfs.core.windows.net/crm/
  mode: ingest
  schema_evolution: append

# Note: This is metadata-only in the OSS release.

# We skip quality rules here because we want an exact copy of the source
# but we still define the "Expected" schema to catch drift.
model:
  fields:
    - name: user_id
      type: long
    - name: signup_date
      type: timestamp
```

## 💡 Pro Tip: The "All Strings" Bronze Pattern

Many high-scale data teams use the **"Bronze as Strings"** pattern. 

In this setup, you read **every** column from the source as a `string` (or `varchar`). 

### Why do this?
1.  **Zero Ingestion Failures**: You never crash your pipeline because an API sent "N/A" into a numeric field.
2.  **100% Data Capture**: You capture the "dirty" data exactly as it was sent.
3.  **Fix in Silver**: You perform the casting and data cleaning in the **Silver** layer, where you can use LakeGuard's `quarantine` to isolate the rows that won't cast to the correct type.

```yaml
# A "Safe" Bronze Ingestion Contract
server:
  mode: ingest
  cast_to_string: true
```

By using LakeGuard at the **Ingestion** point, you ensure that every row in your **Bronze** layer has a known schema and a clean lineage, right from the start. 🛡️☁️

---

## Runnable Example

See `docs/examples/ingestion.md` for a runnable Bronze ingestion example with real data and a real contract.
