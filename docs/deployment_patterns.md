# Deployment Patterns

> Note: These patterns describe how LakeGuard is used in production architectures. Local materialization is available; full orchestration remains on the roadmap.

LakeGuard is flexible. You can choose to process your data in discrete batches (Layer by Layer) or flow it through the entire architecture in a single pass (End-to-End).

## 1. Pattern A: The Decoupled Medallion (Recommended)

In this pattern, each layer is a separate "Job" with its own Data Contract. This is the most common approach for large-scale Lakehouses.

**The Workflow**:
1. **Job 1**: Ingest Raw -> **Bronze**. (Focus: Schema Protection).
2. **Job 2**: Process Bronze -> **Silver**. (Focus: Quality Rules & PII).
3. **Job 3**: Aggregate Silver -> **Gold**. (Focus: Business Logic & Fact Strategies).

### Why use this-
- **Isolation**: If the Gold job fails, your Silver data is still safe and available.
- **Independent Scaling**: You can run Ingestion every 5 minutes, but only run Gold aggregates once an hour.
- **Easier Debugging**: You can see exactly which layer failed.
- **Multi-Platform Orchestration**: You can run Bronze in **Azure Data Factory**, Silver in **Databricks**, and Gold in **Spark-based lakehouses** (Fabric/Synapse/Databricks) or Snowflake/BigQuery (table-only).

---

## 2. Pattern B: The End-to-End Pipe (Low Latency)

For smaller datasets or real-time requirements, you can flow data from Raw all the way to Gold in one single LakeGuard execution.

**The Workflow**:
You define a single "Pipeline Contract" that includes both the Ingestion settings and the final Gold materialization logic.

```yaml
# crm_full_pipeline.yaml
info:
 title: CRM End-to-End
 target_layer: gold # The final destination

server:
 type: s3
 mode: ingest
 
# In-memory transformation from Bronze to Silver logic
transformations:
 - sql: |
 SELECT *, LOWER(email) AS cleaner_email
 FROM source
 phase: post

# Final Materialization into Gold
primary_key: [user_id]
materialization:
 strategy: merge
```

### Why use this-
- **Speed**: No "rest stops" at Bronze or Silver. Data is ready for business faster.
- **Simplicity**: Only one YAML file and one CLI command to manage.

---

## 3. Pattern C: Streaming & Event-Driven (Real-Time)

For modern data stacks, data doesn't wait for a batch window. It flows continuously. LakeGuard can be integrated into streaming pipelines to provide **real-time quality gating**.

**The Workflow**:
1. **Event Trigger**: A file lands in S3 or a message arrives in Kafka.
2. **Serverless execution**: An AWS Lambda or Azure Function triggers a LakeGuard execution on that specific record or micro-batch.
3. **Spark Streaming**: LakeGuard is used inside a `foreachBatch` sink in Spark Structured Streaming to validate every window before it is committed to the Delta/Iceberg table.

### Why use this-
- **Immediate Alerts**: Get a Slack notification for bad data seconds after it is generated.
- **Incremental Cost**: Only process the new data, keeping compute costs low.
- **Clean Live Dashboards**: Ensures that live "Streaming Gold" tables are never poisoned by malformed events.

---

## 4. Gold Best Practices (Aggregates and Traceability)

Gold tables are where business decisions happen. A few defaults help keep them trustworthy and explainable:

- **Use merge with a primary key** when Gold is updated incrementally.
- **Keep lineage lean**: often store only `_lakeguard_run_id` in Gold and rely on run logs for the rest.
- **Preserve upstream run ids when needed**: capture `_upstream_lakeguard_run_ids` for full traceability.
- **Roll up source keys** when aggregating to enable drill-down.
- **Add rollup key counts** so you can validate scale without scanning arrays.

Example (Gold rollup with optional traceability):

```yaml
lineage:
 enabled: true
 capture_run_id: true
 capture_timestamp: false
 capture_source_path: false
 capture_domain: false
 capture_system: false
 run_id_source: pipeline_run_id
 preserve_upstream: ["_lakeguard_run_id"]
 upstream_prefix: "_upstream"

primary_key: ["sale_date"]
materialization:
 strategy: merge

transformations:
 - rollup:
 group_by: ["sale_date"]
 aggregations:
 total_sales: "SUM(amount)"
 keys: "sale_id"
 rollup_keys_column: "_lakeguard_rollup_keys"
 rollup_keys_count_column: "_lakeguard_rollup_keys_count"
 upstream_run_id_column: "_upstream_run_id"
 upstream_run_ids_column: "_upstream_lakeguard_run_ids"
```

---

## Comparison: Which one is right for you-

| Feature | Decoupled (Layered) | End-to-End (Single Pass) | Streaming (Micro-Batch) |
| :--- | :--- | :--- | :--- |
| **Recovery** | Easy: Re-run failed layer. | Harder: Re-run whole pipe. | Automatic (Checkpointing). |
| **Complexity** | Medium (Needs Orchestrator). | Low (Standalone). | High (Needs Kafka/Lambda). |
| **Latency** | High (Minutes/Hours). | Low (Minutes). | Near-Instant (Seconds). |
| **Use Case** | Financial Reporting. | Simple ETL/ELT. | Fraud Detection / IoT. |

## Summary
Most companies start with **Pattern B** for their first project and grow into **Pattern A** as their Lakehouse matures into a "Data Mesh." Real-time environments leverage **Pattern C** to ensure that streaming tables remain business-ready. LakeGuard provides the building blocks for all three, with full orchestration support planned.

---

**Scaling Up-** [LineageLogic](https://lineagelogic.com) provides visual lineage, and AI-powered contract generation on top of LakeGuard.
