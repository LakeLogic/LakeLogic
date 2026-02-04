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
        C[Cleaned & Validated]
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

| Strategy | When to use it |
| :--- | :--- |
| **`append`** | For giant transaction tables where you just keep adding rows. |
| **`merge`** | For "SCD Type 1" (Updating existing records). |
| **`scd2`** | For "History Tracking" (Keeping old and new versions). |
| **`overwrite`** | For small summary tables or daily snapshots. |
