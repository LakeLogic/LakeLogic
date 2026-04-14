# Deployment Patterns

> **Think of deployment like a restaurant kitchen.** You can run one chef who does everything (end-to-end), a brigade where each station handles one course (decoupled layers), or a conveyor belt that never stops (streaming). LakeLogic supports all three.

---

## Which Pattern Is Right for You?

| Feature | Decoupled (Layered) | End-to-End (Single Pass) | Streaming (Micro-Batch) |
|:---|:---|:---|:---|
| **Recovery** | Re-run the failed layer only | Re-run the entire pipeline | Automatic (checkpointing) |
| **Complexity** | Medium — needs an orchestrator | Low — single YAML | High — needs Kafka/Lambda |
| **Latency** | Minutes to hours | Minutes | Seconds |
| **Best for** | Enterprise lakehouses | Small projects, POCs | Fraud detection, IoT |

> **Most teams start with End-to-End** for their first project, then graduate to Decoupled as their data mesh grows.

---

## Pattern A: Decoupled Medallion (Recommended for Production)

Each layer is a **separate job** with its own contract. This is the standard for large-scale lakehouses.

```
Job 1: Raw → Bronze   (schema protection, lineage stamping)
Job 2: Bronze → Silver (quality rules, PII masking, dedup)
Job 3: Silver → Gold   (aggregations, business logic)
```

### Why this works

- **Isolation** — if Gold fails, Bronze and Silver data is safe
- **Independent scaling** — ingest every 5 minutes, aggregate hourly
- **Easier debugging** — pinpoint exactly which layer failed
- **Multi-platform** — mix engines per layer to optimise cost and performance

### Deployment Topologies

| Topology | Bronze | Silver | Gold | Best For |
|:---|:---|:---|:---|:---|
| **All-in-one** | Databricks (Spark) | Databricks (Spark) | Databricks (Spark) | Enterprises already on Databricks |
| **Serverless (Azure)** | Azure Functions (Polars) | Azure Functions (Polars) | Databricks / Spark | Cost-effective — serverless for ingestion, Spark only for heavy aggregations |
| **Serverless (AWS)** | Lambda (Polars) | Lambda (Polars) | EMR / Databricks | Same pattern on AWS — pay-per-invocation for Bronze & Silver |
| **Microsoft Fabric** | Fabric Notebooks (Polars) | Fabric Notebooks (Polars) | Fabric / Spark | Native Fabric integration |

> **The key insight:** Bronze and Silver are often lightweight (schema check, dedup, quality rules) — they don't need a Spark cluster. Run them on Polars in serverless functions for pennies, and reserve Spark for Gold aggregations where you actually need distributed compute.

### Example contracts

```yaml
# bronze_customers.yaml — Job 1
dataset: bronze_crm_customers
source:
  path: "abfss://landing@acct.dfs.core.windows.net/crm/customers/"
  load_mode: incremental
  watermark_strategy: pipeline_log

model:
  fields:
    - name: customer_id
      type: string
      required: true
    - name: email
      type: string
    - name: updated_at
      type: timestamp

materialization:
  strategy: append
  target_path: "abfss://lake@acct.dfs.core.windows.net/bronze/crm_customers"
```

```yaml
# silver_customers.yaml — Job 2
dataset: silver_crm_customers
source:
  type: table
  path: "abfss://lake@acct.dfs.core.windows.net/bronze/crm_customers"
  load_mode: incremental
  watermark_strategy: max_target
  watermark_field: "_lakelogic_processed_at"

transformations:
  - deduplicate: [customer_id]
  - sql: "SELECT *, LOWER(TRIM(email)) AS email FROM source"
    phase: pre

model:
  fields:
    - name: customer_id
      type: string
      required: true
    - name: email
      type: string
      required: true
      pii: true
      masking: "hash"

quality:
  row_rules:
    - sql: "email LIKE '%@%.%'"

materialization:
  strategy: merge
  primary_key: [customer_id]
  target_path: "abfss://lake@acct.dfs.core.windows.net/silver/crm_customers"
```

---

## Pattern B: End-to-End (Quick Start)

Single contract, single execution. Data flows from source through validation to materialization in one pass.

```yaml
# crm_pipeline.yaml — everything in one contract
dataset: silver_crm_customers

source:
  path: "data/customers.csv"

transformations:
  - deduplicate: [customer_id]

model:
  fields:
    - name: customer_id
      type: string
      required: true
    - name: email
      type: string
      pii: true
      masking: "hash"

quality:
  row_rules:
    - sql: "email LIKE '%@%.%'"

materialization:
  strategy: merge
  primary_key: [customer_id]
  target_path: output/silver_customers
```

### Why this works

- **Simplicity** — one YAML file, one CLI command
- **Speed** — no intermediate writes, data flows in memory
- **Great for POCs** — prove value before building the full architecture

---

## Pattern C: Streaming & Event-Driven (Real-Time)

For continuous data flows, LakeLogic integrates into streaming pipelines to provide **real-time quality gating**.

### Spark Structured Streaming

```python
from lakelogic import DataProcessor

proc = DataProcessor(contract="events.yaml")

# Use LakeLogic inside foreachBatch
def validate_batch(batch_df, batch_id):
    result = proc.run_dataframe(batch_df)
    result.good.write.format("delta").mode("append").save(silver_path)
    result.bad.write.format("delta").mode("append").save(quarantine_path)

stream.writeStream.foreachBatch(validate_batch).start()
```

### Serverless (Lambda / Azure Functions)

```python
# Triggered by S3/ADLS file arrival
def handler(event):
    from lakelogic import DataProcessor
    file_path = event["Records"][0]["s3"]["object"]["key"]
    result = DataProcessor(contract="events.yaml").run_source(file_path)
    # Bad data triggers Slack alert automatically (via contract notifications)
```

### Why this works

- **Immediate alerts** — bad data flagged within seconds
- **Incremental cost** — only process new data
- **Clean live dashboards** — streaming Gold tables are never poisoned

---

## Orchestrator Integration

LakeLogic works with any orchestrator. Use the CLI or Python API:

=== "CLI (Any Orchestrator)"

    ```bash
    lakelogic run contracts/crm/customers_silver.yaml \
      --window last_success \
      --summary-table run_logs.pipeline_summaries
    ```

=== "Airflow"

    ```python
    from airflow.operators.bash import BashOperator

    validate_customers = BashOperator(
        task_id="validate_customers",
        bash_command="lakelogic run contracts/crm/customers_silver.yaml",
    )
    ```

=== "Databricks Jobs"

    ```python
    # Databricks notebook cell
    from lakelogic import DataProcessor

    result = DataProcessor(
        contract="/Workspace/contracts/customers_silver.yaml",
        engine="spark"
    ).run()
    ```

=== "Prefect / Dagster"

    ```python
    from prefect import flow
    from lakelogic import DataProcessor

    @flow
    def validate_customers():
        return DataProcessor("contracts/customers.yaml").run()
    ```

### CLI Flags for Production

| Flag | Purpose |
|:---|:---|
| `--window last_success` | Resume from last successful watermark |
| `--summary-path ./run.json` | Emit per-run JSON summary |
| `--summary-table db.run_logs` | Write summaries to a table for dashboards |
| `--continue-on-error` | Best-effort run, report all failures in one pass |
| `--metrics-backend prometheus` | Expose `/metrics` endpoint for monitoring |

---

## Gold Layer Best Practices

Gold tables are where **business decisions happen**. A few defaults keep them trustworthy:

- **Use merge with a primary key** when Gold is updated incrementally
- **Keep lineage lean** — often store only `_lakelogic_run_id` in Gold; rely on run logs for the full trail
- **Roll up source keys** when aggregating, to enable drill-down
- **Capture upstream run IDs** via `preserve_upstream` for full cross-layer traceability

```yaml
# Gold rollup example
materialization:
  strategy: merge
  primary_key: [sale_date]

lineage:
  enabled: true
  capture_run_id: true
  preserve_upstream: ["_lakelogic_run_id"]

transformations:
  - rollup:
      group_by: [sale_date]
      aggregations:
        total_sales: "SUM(amount)"       # → output column: total_sales
        order_count: "COUNT(*)"          # → output column: order_count
        avg_order: "AVG(amount)"         # → output column: avg_order
      keys: "sale_id"
```
