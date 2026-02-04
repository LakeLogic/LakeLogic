# Lineage & Reconciliation 🧵

> Note: Automated lineage capture is on the roadmap. The OSS release provides contract metadata and run logging.

In a mission-critical Data Lakehouse, you must be able to prove that **nothing was lost** and **everything came from somewhere**. 

LakeGuard provides built-in tools for **Data Reconciliation** and **System-Level Lineage**.

## 1. Automated Metadata Capture

LakeGuard can automatically inject lineage columns into every record as it moves from Bronze to Silver. This happens for both **Good Data** and **Quarantined Data**.

```yaml
lineage:
  capture_source_path: true
  capture_timestamp: true
  source_column_name: "_bronze_file_name"
```

When this is enabled, every row in your Silver table will tell you exactly which file it came from. This is vital for debugging "garbage" data back to the source provider.

## 2. Reconciliation: The "Count" Rule

To ensure that `Bronze = Silver + Quarantine`, you can use **Dataset Rules**.

| Layer | Records | Status |
| :--- | :--- | :--- |
| **Bronze** | 1,000 | Ingested |
| **Silver** | 995 | Cleaned |
| **Quarantine** | 5 | Isolated |
| **Total** | **1,000** | ✅ Reconciled |

LakeGuard logs these counts automatically at the end of every run, providing a clear audit trail for your data platform.

## 3. Gold Column "Key Roll-up"

When you aggregate data in the **Gold** layer (e.g., summarizing 1,000 sales into 1 daily total), you lose the connection to the individual records.

**The LakeGuard Solution**: Use "Key Rolling" in your SQL logic to keep a list of the source IDs.

```yaml
# sales_daily_gold.yaml
logic: |
  SELECT 
    sale_date,
    SUM(amount) as total_sales,
    ARRAY_AGG(sale_id) as silver_source_ids # Roll-up the source keys
  FROM silver_sales
  GROUP BY sale_date
```

### Why do this?
By keeping the `silver_source_ids` in your Gold table:
1.  **Drill-down**: A business user seeing a weird total can immediately find the exact 500 sales that created it.
2.  **Audit**: You can mathematically prove that every row in Gold is backed by a specific set of records in Silver.
3.  **Trust**: It turns your "Black Box" aggregates into "Open Book" data. 🛡️📒
