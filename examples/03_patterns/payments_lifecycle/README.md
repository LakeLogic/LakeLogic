# Payments Lifecycle Pattern (Bronze -> Silver -> Gold -> Platinum)

This pattern demonstrates a complete 4-layer data lifecycle using LakeGuard linked contracts. It shows how data transitions from raw ingestion to a certified, immutable monthly snapshot.

## The Layers

1.  **Bronze (Ingestion):** Validates raw file schema for Payments and Customers.
    * Files: `01_bronze_payments.yaml`, `01_bronze_customers.yaml`
2.  **Silver (Normalization):** Standardizes types, handles deduplication, and cleans data.
    * Files: `02_silver_payments.yaml`, `02_silver_customers.yaml`
3.  **Gold (Truth):** 
    * **Customers:** Implements **SCD Type 2** to track history (e.g., plan upgrades).
      * File: `03_gold_customers.yaml`
    * **Payments:** Joins with the **current** customer dimension to enrich facts with metadata.
      * File: `03_gold_payments.yaml`
4.  **Platinum (Audit):** Freezes a monthly snapshot into **Delta** format with aggregate-level stability checks.
    * File: `04_platinum_ledger.yaml`

## How to Run

You can run the entire pipeline using the LakeGuard Driver:

```bash
lakeguard-driver --registry .
```

Or step-by-step in the included Jupyter Notebook: `payments_lifecycle.ipynb`.

## Key Features Demonstrated
* **Linked Contracts:** Using `upstream` to define dependencies.
* **Cross-Engine Storage:** Moving from CSV -> Parquet -> Iceberg -> Delta.
* **Transformations:** `derive`, `deduplicate`, and `cast`.
* **SCD Type 2:** Dimensional history tracking.
* **Quality Evolution:** Moving from row-level checks in Bronze to dataset-level stability in Platinum.
