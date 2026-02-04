# Multi-Cloud Ingestion ☁️

LakeGuard is more than just a validator; it's a **Universal Ingestion Engine**. Whether your data is in AWS, Azure, or GCP, LakeGuard can move it and protect it.

## 1. Cloud Storage Support
LakeGuard adapters (Spark and Polars) can read directly from cloud-native paths:

-   **AWS S3**: `s3://my-bucket/raw_data/`
-   **Google GCS**: `gs://my-bucket/raw_data/`
-   **Azure ADLS**: `abfss://container@account.dfs.core.windows.net/path/`

## 2. The "Ingestion" Mode (Raw to Bronze)

When moving data from external sources (Raw) into your **Bronze** layer, you might not want complex transformations, but you **always** want to protect your schema.

```yaml
server:
  type: gcs
  path: gs://landing-zone/daily_extract/
  mode: ingest # Tells LakeGuard to focus on Ingestion
  schema_evolution: append # Allow new columns, but don't break old ones
```

### Schema Evolution Strategies:
| Strategy | Behavior |
| :--- | :--- |
| **`strict`** | Job fails if the incoming file doesn't match the Bronze table exactly. |
| **`append`** | Automatically adds new columns to the Bronze table if they appear in the source. |
| **`merge`** | Upgrades the table schema to the "greatest common denominator" of all files. |

## 3. Schema Drift Protection
If your source system (e.g., Salesforce or a Marketing API) suddenly changes a column from `Integer` to `String`, LakeGuard's **Schema Drift** detection will:
1.  **Stop** the ingestion.
2.  **Alert** the data owner.
3.  **Prevent** your Lakehouse from becoming a "Data Swamp."

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
  cast_to_string: true # NEW: Force all incoming fields to strings
```

By using LakeGuard at the **Ingestion** point, you ensure that every row in your **Bronze** layer has a known schema and a clean lineage, right from the start. 🛡️☁️
