# Lineage & Reconciliation

> Note: Automated lineage capture, run logging, and SLO scoring are available in the OSS release. Full orchestration is still on the roadmap.

In a mission-critical data lakehouse, you must be able to prove that **nothing was lost** and **everything came from somewhere**. LakeLogic provides built-in tools for **Data Reconciliation** and **System-Level Lineage**.

---

## 1. Automated metadata capture
LakeLogic can inject lineage columns into every record as it moves from Bronze to Silver (and beyond). This happens for both **Good Data** and **Quarantined Data**.

```yaml
lineage:
  enabled: true
  capture_source_path: true
  capture_timestamp: true
  source_column_name: "_bronze_file_name"
```

If you only want a run id in tables, set explicit capture flags and keep the rest in run logs:

```yaml
lineage:
  enabled: true
  capture_run_id: true
  capture_timestamp: false
  capture_source_path: false
  capture_domain: false
  capture_system: false
```

You can preserve upstream lineage columns before stamping the current run:

```yaml
lineage:
  enabled: true
  preserve_upstream: ["_lakelogic_run_id"]
  upstream_prefix: "_upstream"
```

And for Gold, you can use a pipeline-wide id for `_lakelogic_run_id`:

```yaml
lineage:
  enabled: true
  capture_run_id: true
  run_id_source: pipeline_run_id
```

---

## 2. Reconciliation: the count rule
To ensure that `Bronze = Silver + Quarantine`, use the counts LakeLogic logs on every run.

| Layer | Records | Status |
| :--- | :--- | :--- |
| **Bronze** | 1,000 | Ingested |
| **Silver** | 995 | Cleaned |
| **Quarantine** | 5 | Isolated |
| **Total** | **1,000** | Reconciled |

Run logs include:
- `counts_source` (source rows before transforms)
- `counts_total` (post-transform total)
- `counts_pre_transform_dropped`

Reconciliation is simply checking that `counts_total == counts_good + counts_quarantined`.

---

## 3. Incremental manifests and watermarks
When you ingest from file globs, the run log captures:
- `source_files_json`: list of files processed in that batch
- `max_source_mtime`: max file timestamp in that batch

This supports lightweight watermarks for incremental processing and auditability.

---

## 4. Gold key roll-up (traceability for aggregates)
When you aggregate data in the Gold layer, you lose direct row-to-row traceability. Use a rollup transform to keep source keys.

```yaml
transformations:
  - rollup:
      group_by: ["sale_date"]
      aggregations:
        total_sales: "SUM(amount)"
      keys: "sale_id"
      rollup_keys_column: "_lakelogic_rollup_keys"
      rollup_keys_count_column: "_lakelogic_rollup_keys_count"  # optional
      upstream_run_id_column: "_upstream_run_id"                # optional
      upstream_run_ids_column: "_upstream_lakelogic_run_ids"     # optional
```

### Why do this?
By keeping the rollup keys in Gold:
1. **Drill-down**: a business user can trace a total back to its component rows.
2. **Audit**: you can prove each Gold row is backed by specific Silver records.
3. **Trust**: aggregates become transparent and explainable.

---

## Summary: business value

- **Auditability**: verifiable evidence for regulators and internal governance.
- **Traceability**: fast root-cause analysis when a metric looks wrong.
- **Operational confidence**: clear reconciliation across medallion layers.
- **Transparent aggregates**: Gold tables remain explainable and defensible.
