# Deployment Patterns 🚀

LakeGuard is flexible. You can choose to process your data in discrete batches (Layer by Layer) or flow it through the entire architecture in a single pass (End-to-End).

## 1. Pattern A: The Decoupled Medallion (Recommended)

In this pattern, each layer is a separate "Job" with its own Data Contract. This is the most common approach for large-scale Lakehouses.

**The Workflow**:
1.  **Job 1**: Ingest Raw → **Bronze**. (Focus: Schema Protection).
2.  **Job 2**: Process Bronze → **Silver**. (Focus: Quality Rules & PII).
3.  **Job 3**: Aggregate Silver → **Gold**. (Focus: Business Logic & Fact Strategies).

### Why use this?
-   **Isolation**: If the Gold job fails, your Silver data is still safe and available.
-   **Independent Scaling**: You can run Ingestion every 5 minutes, but only run Gold aggregates once an hour.
-   **Easier Debugging**: You can see exactly which layer failed.

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
  - derive:
      field: cleaner_email
      sql: "LOWER(email)"

# Final Materialization into Gold
materialization:
  strategy: merge
  primary_key: [user_id]
```

### Why use this?
-   **Speed**: No "rest stops" at Bronze or Silver. Data is ready for business faster.
-   **Simplicity**: Only one YAML file and one CLI command to manage.

---

## Comparison: Which one is right for you?

| Feature | Decoupled (Layered) | End-to-End (Single Pass) |
| :--- | :--- | :--- |
| **Recovery** | Easy: Just re-run the failed layer. | Harder: Must re-run the whole pipe. |
| **Complexity** | Higher (Needs an Orchestrator). | Lower (Standalone). |
| **Latency** | Higher (Wait for each layer). | Lower (Instant flow). |
| **Compliance** | Best: Built-in audit trail at every layer. | Good: Only the end-state is saved. |

## 🛡️ Summary
Most companies start with **Pattern B** for their first project and grow into **Pattern A** as their Lakehouse matures into a "Data Mesh." LakeGuard supports both perfectly.
