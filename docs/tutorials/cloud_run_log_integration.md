# Cloud Run Log Integration

LakeLogic can send run metadata to [LakeLogic Cloud](https://lineagelogic.com) for centralized observability. No raw data leaves your environment — only quality metrics.

---

## What Gets Reported

Each pipeline run sends metadata only:

| Category | Fields |
|----------|--------|
| **Identity** | `run_id`, `pipeline_run_id`, `contract`, `dataset`, `stage`, `engine`, `timestamp` |
| **Row Counts** | `source`, `total`, `good`, `quarantined`, `pre_transform_dropped`, `quarantine_ratio` |
| **Quality** | Per-rule failure breakdown, dataset rule results |
| **Schema** | Schema drift events (what changed and when) |
| **SLOs** | Freshness/availability scores and pass/fail |
| **Performance** | `duration_ms` |
| **Context** | `domain`, `system`, `data_layer`, `source_path` |

> **Note:** No actual row data is ever transmitted. Only aggregate metrics and metadata.

---

## Setup: Registry-Level (Recommended)

Add a `cloud:` block to your `_registry.yaml`. All contracts in the registry inherit it:

```yaml
# _registry.yaml
domain: sales
system: olist

storage:
  # ... your storage roots ...

contracts:
  - layer: bronze
    entity: orders
    path: "contracts/bronze/orders.yaml"
    enabled: true

# ── Cloud Reporting ──────────────────────────────
cloud:
  enabled: true
  report_url: "${LINEAGELOGIC_REPORT_URL}"
  api_key: "${LAKELOGIC_API_KEY}"

environments:
  dev:
    catalog: "my-dev-catalog"
  prod:
    catalog: "my-prod-catalog"
```

Set the environment variables:

```bash
export LINEAGELOGIC_REPORT_URL="https://api.lineagelogic.com/v1/runs"
export LAKELOGIC_API_KEY="llk_your_api_key_here"
```

That's it. Every `DataProcessor.run_source()` call now reports automatically.

---

## Setup: Environment Variables Only

If you're not using a registry, set three env vars:

```bash
export LAKELOGIC_REMOTE_OBSERVER=true
export LINEAGELOGIC_REPORT_URL="https://api.lineagelogic.com/v1/runs"
export LINEAGELOGIC_API_KEY="llk_your_api_key_here"
```

```python
from lakelogic import DataProcessor

processor = DataProcessor(engine="polars", contract="contracts/customers.yaml")
good_df, bad_df = processor.run_source("data/customers.csv")
# → Run report sent automatically after run completes
```

---

## How It Works

```
┌──────────────┐     run_source()     ┌───────────────┐
│  Your Data   │ ──────────────────→  │ DataProcessor  │
│  (stays      │                      │                │
│   local)     │                      │  validate      │
│              │                      │  materialize   │
└──────────────┘                      │  write run log │
                                      └───────┬───────┘
                                              │
                                     metadata only (POST)
                                              │
                                              ▼
                                      ┌───────────────┐
                                      │ RemoteObserver │
                                      │                │
                                      │ 2s timeout     │
                                      │ silent fail    │
                                      │ never blocks   │
                                      │ your pipeline  │
                                      └───────┬───────┘
                                              │
                                              ▼
                                      ┌───────────────┐
                                      │  LakeLogic    │
                                      │  Cloud API    │
                                      └───────────────┘
```

Key design decisions:
- **2-second timeout** — the POST never delays your ETL
- **Silent failure** — if the API is unreachable, the pipeline continues
- **Bearer auth** — API key sent as `Authorization: Bearer <key>`
- **No raw data** — only the flattened run report (same as local run logs)

---

## Run Log Storage

Run logs can be written locally or to cloud storage. Configure in contract `metadata`:

### Local

```yaml
metadata:
  # Option A: Unique JSON file per run (recommended)
  run_log_dir: "logs/"

  # Option B: Single JSON file (overwritten each run)
  run_log_path: "logs/last_run.json"

  # Option C: Database table (DuckDB, SQLite, or Spark)
  run_log_table: "lakelogic.run_logs"
  run_log_backend: "duckdb"  # or sqlite, spark
  run_log_database: "logs/lakelogic_run_logs.duckdb"
```

### Cloud Storage (ADLS, S3, GCS)

Cloud paths are auto-detected via `fsspec`:

```bash
pip install fsspec adlfs   # Azure
pip install fsspec s3fs    # AWS
pip install fsspec gcsfs   # GCP
```

```yaml
metadata:
  # Azure ADLS — unique file per run
  run_log_dir: "abfss://logs@myaccount.dfs.core.windows.net/runs/"

  # AWS S3
  run_log_dir: "s3://my-bucket/lakelogic/run-logs/"

  # Google Cloud Storage
  run_log_dir: "gs://my-bucket/lakelogic/run-logs/"
```

Each run writes `run_<run_id>.json` — no overwrites.

---

## Intelligence Layer (Cloud)

With run logs accumulating over time, the Cloud intelligence layer can:

- **Detect patterns** — a rule fails every Monday morning → upstream batch issue
- **Trend alerts** — quarantine rates trending up for 3 weeks on a contract
- **Threshold recommendations** — suggest tightening a null check based on history
- **Cross-contract correlation** — shared `pipeline_run_id` links Bronze → Silver → Gold

---

## Disabling Cloud Reporting

Set `enabled: false` in the registry:

```yaml
cloud:
  enabled: false
```

Or unset the env var:

```bash
unset LAKELOGIC_REMOTE_OBSERVER
```

Or enable offline mode:

```bash
export LAKELOGIC_OFFLINE=true
```
