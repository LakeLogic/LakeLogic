# Connect LakeLogic OSS to LakeLogic Cloud
<!-- markdownlint-disable MD013 -->

LakeLogic OSS can send operational run metadata to LakeLogic Cloud for cross-pipeline observability, trust monitoring, and incident analysis.

The integration does not upload source datasets or accepted records. It can send contract and pipeline identifiers, source paths, counts, quality scores, rule-failure summaries, SLO context, timings, error messages, and cost estimates. Review this payload against your organisation's security policy before enabling it.

---

## OSS and LakeLogic Cloud

The Apache 2.0 framework runs independently. LakeLogic Cloud is an optional hosted control plane built on top of the OSS run evidence.

| Capability | LakeLogic OSS | LakeLogic Cloud |
|---|:---:|:---:|
| Contracts, runtime validation, and quarantine | Yes | Uses OSS evidence |
| CI/CD contract gates | Yes | Uses OSS evidence |
| Polars, DuckDB, and Spark execution | Yes | Not an execution engine |
| Local and customer-managed run logs | Yes | Optional hosted ingestion |
| Cross-pipeline operations view | Local implementation required | Yes |
| Trust history and trend monitoring | Local implementation required | Yes |
| Zeus-assisted incident diagnosis and remediation proposals | No | Yes |
| Visual contract governance across domains | No | Yes |

Cloud is additive: disabling telemetry does not disable contract execution.

## What Is Reported

The current `observatory:` integration maps a run report into this hosted payload:

| Category | Examples |
|---|---|
| **Contract** | Contract name, version, schema fingerprint, domain, system, environment, and layer |
| **Execution** | Status, engine, start and finish times, duration, run ID, and pipeline run ID |
| **Counts** | Input, accepted, quarantined, and output row counts |
| **Quality** | Quality score and optional rule-failure summaries |
| **Context** | Source path, SLO JSON, and an error message when present |
| **Cost** | Estimated cost, currency, and confidence when available |

`include_quarantine_sample` controls whether rule-failure detail is included: for each rule that failed, its name, SQL expression, category, message and a count. **Failing source rows are never captured or transmitted.** There is no option that sends them and none has ever existed — the detail is read from LakeLogic's own rule-annotation columns, never from your data columns.

For the smallest metadata surface, set:

```yaml
include_quarantine_sample: false
```

## Configuration

### Fastest Hosted Connection

Set the API key issued by LakeLogic Cloud:

```bash
export LAKELOGIC_CLOUD_API_KEY="llc_sk_your_key"
```

This enables the current observatory integration and uses the hosted endpoint:

```text
https://api.lakelogic.io/api/v1/operations/run-logs/ingest
```

Override the endpoint only when using an approved alternative deployment:

```bash
export LAKELOGIC_CLOUD_ENDPOINT="https://your-endpoint.example/api/v1/operations/run-logs/ingest"
```

The environment-only connection enables rule-failure details by default. Use an explicit YAML block with `include_quarantine_sample: false` when you require the smaller payload.

### Domain-Level Configuration

Domain configuration is recommended when telemetry settings should be explicit, reviewed, and inherited by every contract in a domain.

The RideFlow Marketplace domain currently uses:

```yaml
# domains_rideflow/marketplace/_domain.yaml
observatory:
  enabled: false
  environments: [dev, prod, staging, local, local_polars]
  endpoint: "${LAKELOGIC_OBSERVATORY_ENDPOINT}"
  api_key: "${LAKELOGIC_API_KEY}"
  emit_on: [success, partial, failed]
```

This is disabled deliberately in the reference repository. To connect it:

1. Set `enabled: true`.
2. Provide the two referenced secrets in the execution environment.
3. Restrict `environments`, `layers`, and `emit_on` if required.

```bash
export LAKELOGIC_OBSERVATORY_ENDPOINT="https://api.lakelogic.io/api/v1/operations/run-logs/ingest"
export LAKELOGIC_API_KEY="llc_sk_your_key"
```

The names inside `${...}` are ordinary environment references. A domain may use its existing secret names. The environment-only convenience connection specifically uses `LAKELOGIC_CLOUD_API_KEY` and `LAKELOGIC_CLOUD_ENDPOINT`.

### Explicit Configuration with Retry Spooling

```yaml
observatory:
  enabled: true
  endpoint: "${LAKELOGIC_OBSERVATORY_ENDPOINT}"
  api_key: "${LAKELOGIC_API_KEY}"
  environments: [prod]
  layers: [silver, gold]
  emit_on: [partial, failed]
  include_quarantine_sample: false

  spool:
    enabled: true
    dir: ~/.lakelogic/observatory_spool
    max_files: 500
    ttl_days: 7
    batch: 20
    max_seconds: 5.0
```

Configuration precedence is:

1. Explicit YAML values
2. Environment-variable convenience values
3. The hosted default endpoint when an environment API key is present

An explicit `enabled: false` is always honoured.

### Contract-Level Override

A contract can override inherited observatory settings:

```yaml
observatory:
  enabled: true
  environments: [prod]
  layers: [gold]
  emit_on: [failed]
  include_quarantine_sample: false
```

Use domain-level defaults for consistency and contract-level settings only for genuine exceptions.

## Delivery Behaviour

The current observatory path:

- sends the API key through the `X-API-Key` header;
- uses a three-second request timeout;
- does not raise telemetry failures into the data pipeline;
- buffers network errors, timeouts, HTTP 408/429 responses, and server errors;
- retries a bounded batch after a later successful connection;
- removes rule-failure detail before writing a failed payload to the local spool.

The spool is bounded by age, file count, batch size, and a wall-clock replay budget. Put `spool.dir` on persistent storage when running on ephemeral compute.

```text
DataProcessor
    |
    | writes run evidence
    v
Local/customer-managed run log
    |
    | selected operational metadata over HTTPS
    v
LakeLogic Cloud ingest
    |
    +--> success: store hosted run evidence and replay a bounded spool batch
    |
    +--> retryable failure: remove failure details and buffer metadata locally
```

## Local and Customer-Managed Run Logs

Cloud telemetry is separate from the normal run-log destination. Local and customer-managed storage remains available whether or not LakeLogic Cloud is enabled.

### Local JSON or Table Storage

```yaml
metadata:
  # Unique JSON file per run
  run_log_dir: "logs/"

  # Or a single JSON file, overwritten on each run
  run_log_path: "logs/last_run.json"

  # Or a database table
  run_log_table: "lakelogic.run_logs"
  run_log_backend: "duckdb"  # duckdb, sqlite, or spark
  run_log_database: "logs/lakelogic_run_logs.duckdb"
```

### ADLS, S3, and GCS

JSON run-log paths support cloud URIs through `fsspec`. Install the filesystem driver you need:

```bash
pip install fsspec adlfs   # Azure ADLS
pip install fsspec s3fs    # AWS S3
pip install fsspec gcsfs   # Google Cloud Storage
```

```yaml
# Azure ADLS
metadata:
  run_log_dir: "abfss://logs@mystorageaccount.dfs.core.windows.net/lakelogic/runs/"
```

```yaml
# AWS S3
metadata:
  run_log_dir: "s3://my-bucket/lakelogic/run-logs/"
```

```yaml
# Google Cloud Storage
metadata:
  run_log_dir: "gs://my-bucket/lakelogic/run-logs/"
```

The run-log helper explicitly maps these credential variables when present:

- **Azure:** `AZURE_STORAGE_ACCOUNT_NAME` or `AZURE_STORAGE_ACCOUNT`, `AZURE_STORAGE_ACCOUNT_KEY`, or `AZURE_TENANT_ID` + `AZURE_CLIENT_ID` + `AZURE_CLIENT_SECRET`
- **AWS:** `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
- **GCS:** credential discovery is delegated to `gcsfs`

Provider filesystem drivers may also support their own default credential discovery. Validate the chosen method in the target runtime.

## Disable Cloud Telemetry

For explicit YAML configuration:

```yaml
observatory:
  enabled: false
```

For an environment-only connection, unset the API key:

```bash
unset LAKELOGIC_CLOUD_API_KEY
```

An explicit `enabled: false` takes precedence over environment convenience settings.

## Legacy `cloud:` Integration

Older deployments may still use the `cloud:` block and `RemoteObserver`. That compatibility path uses `LAKELOGIC_REMOTE_OBSERVER`, `LINEAGELOGIC_REPORT_URL`, `LINEAGELOGIC_API_KEY`, Bearer authentication, and a two-second timeout.

New documentation and deployments should use `observatory:`. Do not combine both integrations unless duplicate delivery has been considered and tested.

## Related Documentation

- [Observability and run evidence](observability.md)
- [Run-log return values](return_values.md)
- [Cloud integration](cloud_integration.md)
- [Automatic cloud credentials](automatic_credentials.md)

---

*Last updated: July 2026*
