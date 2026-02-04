# The Medallion Architecture

LakeGuard is the **quality gate** between the layers of your Data Lakehouse.

```mermaid
graph LR
    subgraph "Landing"
        A[Raw Sources]
    end

    subgraph "Bronze Layer"
        B[Raw Data]
    end

    subgraph "Silver Layer"
        C[Filtered, Cleaned, Transformed, Enriched]
    end

    subgraph "Gold Layer"
        D[Business Ready]
    end

    B -->|🛡️ Quality Gate| C
    C -->|🛡️ Materialize| D
    
    B -.->|Fail| Q[Quarantine 🛑]
    Q -.->|Correction| B
    
    style B fill:#cd7f32,color:#fff
    style C fill:#c0c0c0,color:#333
    style D fill:#ffd700,color:#333
    style Q fill:#ef4444,color:#fff
```

## Cleansing Transformations (Bronze → Silver)

In the Bronze layer, data is often "dirty." Before you apply strict quality rules or perform heavy calculations, you need to clean the noise.

LakeGuard processes transformations in a specific order to ensure maximum performance and safety:

### 1. Pre-Processing (Cleanse)
These run **first**, before schema enforcement and quality rules.
-   **`rename`**: Align column names (e.g., `cust_id` to `customer_id`).
-   **`filter`**: Drop invalid rows immediately (e.g., `WHERE status = 'active'`).
-   **`deduplicate`**: Keep the latest version of a record based on a timestamp.

### 2. Validation Gate
LakeGuard then enforces your **Schema** and runs your **Quality Rules**. Because you've already filtered and deduplicated, this stage is faster and produces fewer "false alerts."

### 3. Post-Processing (Enrich)
These run **last**, only on the "Good" data.
-   **`derive`**: Calculate new fields using SQL (e.g., `price * quantity`).
-   **`lookup`**: Join with dimension tables to add names or categories.

---

## Handling Complex Patterns (Gold)

When moving from **Silver to Gold**, LakeGuard doesn't just check rules; it uses a **Strategy** to build your tables.

### 1. The "Orphaned Key" Problem
Sometimes a transaction (Fact) arrives before its customer info (Dimension). This is a **Late Arriving Dimension**.

**The LakeGuard Solution**:
Instead of losing the transaction, we use the `default_value` feature.
-   If `customer_id` is found: Use it.
-   If `customer_id` is missing: Map it to `-1` (Unknown).
-   This ensures **100% data financial integrity**.

### 2. The Correction Loop
If data fails a rule in **Bronze**, it goes to **Quarantine**. 
1.  **Fix**: The data owner fixes the raw source or provides a correction file.
2.  **Reprocess**: LakeGuard picks up the correction and flows it through to **Silver** and **Gold**.

## Materialization Strategies

> Note: Materialization execution is on the roadmap. The OSS release focuses on validation, transformations, and quarantine.

| Strategy | When to use it |
| :--- | :--- |
| **`append`** | For giant transaction tables where you just keep adding rows. |
| **`merge`** | For "SCD Type 1" (Updating existing records). |
| **`scd2`** | For "History Tracking" (Keeping old and new versions). |
| **`overwrite`** | For small summary tables or daily snapshots. |
