# Cloud Run Log Integration
<!-- markdownlint-disable MD013 -->

LakeLogic can send run metadata to LakeLogic Cloud for centralized observability, trend detection, and ops intelligence. **No raw data leaves your environment — only quality metrics.**

---

## Overview

Every `DataProcessor.run_source()` call produces a run report. With cloud reporting enabled, this report is POSTed to a remote API — the same data that's written to local run logs.

### What Gets Reported (Metadata Only)

| Category | Fields |
|----------|--------|
| **Identity** | `run_id`, `pipeline_run_id`, `contract`, `dataset`, `stage`, `engine`, `timestamp` |
| **Row Counts** | `source`, `total`, `good`, `quarantined`, `pre_transform_dropped`, `quarantine_ratio` |
| **Quality** | Per-rule failure breakdown (which rules fired, how many rows), dataset rule results |
| **Schema** | Schema drift events — what changed and when |
| **SLOs** | Freshness/availability scores with pass/fail |
| **Performance** | `duration_ms`, engine type |
| **Context** | `domain`, `system`, `data_layer`, `source_path` |

---

## Configuration

### Registry-Level (Recommended)

Set the `cloud:` block in your `_registry.yaml` so all contracts in the domain inherit it:

```yaml
# _registry.yaml
domain: sales
system: olist

storage:
  landing_root: "/Volumes/{catalog}/.../olist"
  bronze_root: "`{catalog}`.sales_olist_bronze"
  # ...

contracts:
  - layer: bronze
    entity: orders
    path: "contracts/bronze/orders.yaml"
    enabled: true

cloud:
  enabled: true
  report_url: "${LakeLogic_REPORT_URL}"
  api_key: "${LAKELOGIC_API_KEY}"

environments:
  dev:
    catalog: "my-dev-catalog"
  prod:
    catalog: "my-prod-catalog"
```

```bash
# Set these in your environment or CI secrets
export LakeLogic_REPORT_URL="https://api.LakeLogic.com/v1/runs"
export LAKELOGIC_API_KEY="llk_your_api_key_here"
```

### Environment Variables Only

If you're not using a registry:

```bash
export LAKELOGIC_REMOTE_OBSERVER=true
export LakeLogic_REPORT_URL="https://api.LakeLogic.com/v1/runs"
export LakeLogic_API_KEY="llk_your_api_key_here"
```

No code changes needed — `RemoteObserver` reads these automatically.

### Contract-Level

Add `cloud:` directly in a contract YAML (overrides registry):

```yaml
cloud:
  enabled: true
  report_url: "${LakeLogic_REPORT_URL}"
  api_key: "${LAKELOGIC_API_KEY}"
```

---

## Architecture

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
                                      │ Bearer auth    │
                                      └───────┬───────┘
                                              │
                                              ▼
                                      ┌───────────────┐
                                      │  LakeLogic    │
                                      │  Cloud API    │
                                      └───────────────┘
```

Design guarantees:
- **2-second timeout** — never delays your ETL pipeline
- **Silent failure** — if the API is unreachable, the pipeline continues
- **Bearer auth** — API key sent as `Authorization: Bearer <key>`
- **No raw data** — only aggregate metrics and metadata

---

## Integration Points

### OSS CLI Driver (`lakelogic-driver`)

The `PipelineDriver` reads `cloud:` from the first registry and calls `_apply_cloud_config()`:

```bash
lakelogic-driver \
  --registry contracts/_registry.yaml \
  --layers bronze,silver,gold
# → Cloud config applied from registry before any contracts run
```

## Run Log Storage (Local and Cloud)

Run logs are written via contract `metadata`. Both local paths and cloud storage URIs are supported.

### Local Storage

```yaml
metadata:
  # Unique JSON file per run (recommended)
  run_log_dir: "logs/"
  # → writes logs/run_<run_id>.json per run

  # Or: Single JSON file (overwritten each run)
  run_log_path: "logs/last_run.json"

  # Or: Database table
  run_log_table: "lakelogic.run_logs"
  run_log_backend: "duckdb"  # or sqlite, spark
  run_log_database: "logs/lakelogic_run_logs.duckdb"
```

### Cloud Storage (ADLS, S3, GCS)

Cloud paths are auto-detected and written via `fsspec`. Install the relevant driver:

```bash
pip install fsspec adlfs    # Azure ADLS
pip install fsspec s3fs     # AWS S3
pip install fsspec gcsfs    # Google Cloud Storage
```

**Azure ADLS:**

```yaml
metadata:
  # Unique file per run
  run_log_dir: "abfss://logs@mystorageaccount.dfs.core.windows.net/lakelogic/runs/"
  # → abfss://.../runs/run_<run_id>.json

  # Or: single file
  run_log_path: "abfss://logs@mystorageaccount.dfs.core.windows.net/lakelogic/latest.json"
```

**AWS S3:**

```yaml
metadata:
  run_log_dir: "s3://my-bucket/lakelogic/run-logs/"
```

**Google Cloud Storage:**

```yaml
metadata:
  run_log_dir: "gs://my-bucket/lakelogic/run-logs/"
```

Authentication uses the standard credential chain for each provider:
- **ADLS:** `AZURE_STORAGE_ACCOUNT_KEY`, `AZURE_STORAGE_SAS_TOKEN`, or Azure AD (DefaultAzureCredential)
- **S3:** `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`, or IAM role
- **GCS:** `GOOGLE_APPLICATION_CREDENTIALS`, or GCE metadata

---

## Intelligence Layer

With run logs accumulating over time, the Cloud intelligence layer can:

| Capability | Example |
|-----------|---------|
| **Pattern detection** | A rule fails every Monday morning → upstream batch issue |
| **Trend alerts** | Quarantine rates trending up for 3 weeks on a contract |
| **Threshold recommendations** | Suggest tightening a null check based on historical patterns |
| **Cross-contract correlation** | Shared `pipeline_run_id` links Bronze → Silver → Gold |
| **SLO monitoring** | Freshness breaches tracked over rolling 30-day window |

---

## Disabling Cloud Reporting

```yaml
# In _registry.yaml
cloud:
  enabled: false
```

Or via environment:

```bash
unset LAKELOGIC_REMOTE_OBSERVER
# or
export LAKELOGIC_OFFLINE=true
```

---

## Related Documentation

- [Contract Template — Section 20: Cloud Reporting](contract_template.md)
- [Observability](observability.md)
- [Tutorial: Cloud Run Log Integration](tutorials/cloud_run_log_integration.md)

---

*Last Updated: March 2026*
