# Contract Organization & Governance 🏗️

> Note: These are **recommended patterns**. Automatic registry resolution and contract discovery are planned but not yet built into the OSS runtime.

As your Data Lakehouse grows from 10 to 1,000 tables, how you organize your contracts determines whether your team succeeds or drowns in "Contract YAML Hell."

## 1. Directory Hierarchy: The Domain-First Pattern

We recommend organizing your repository by **Business Domain** rather than by Technical Layer. This aligns with **Data Mesh** principles and ensures clear ownership.

### Recommended Structure

```text
contracts/
├── finance/                    <-- Domain (Ownership Boundary)
│   ├── _registry.yaml           <-- Master Registry for the domain
│   ├── sap_erp/                 <-- Source System
│   │   ├── bronze/              <-- Technical Layer
│   │   │   ├── bronze_erp_customers_v1.yml
│   │   │   └── bronze_erp_customers_v2.yml
│   │   └── silver/
│   │       └── silver_erp_customers_active.yml
│   └── payment_gateway/
├── marketing/
└── warehouse/                   <-- Shared/Gold Layer
    └── gold_sales_summary_v1.yml
```

---

## 2. Naming Conventions

Consistency in naming allows your **ETL Driver** to find contracts automatically without hardcoding paths.

### Contract Filenames
We recommend matching the contract filename to the **Target Table Name**, including a version suffix.

*   **Pattern (Bronze/Silver)**: `[layer]_[system]_[entity]_v[version].yml`
*   **Example**: `bronze_erp_customers_v1.yml` (validates table `bronze_erp_customers`)
*   **Example**: `silver_erp_orders_v2.yml` (validates table `silver_erp_orders`)
*   **Pattern (Gold, shared)**: `[layer]_[domain]_[entity]_v[version].yml`
*   **Example**: `gold_dim_customers_v1.yml` (validates table `gold_dim_customers`)

### Why match table names?
1.  **Traceability**: When a dbt test or Spark job fails on `silver_erp_orders`, you immediately know to look for `silver_erp_orders_v[X].yml`.
2.  **Automation**: Your runner script can assume that `lakelogic run --table silver_erp_orders` maps to the contract in that domain folder.

---

## 3. The "Registry" Pattern

Instead of pointing your production jobs to a specific file like `customers_v12_final_v2.yml`, use a **Registry file**.

**File: `finance/_registry.yaml`**
```yaml
entries:
  - entity: customers
    layer: bronze
    active_version: v2
    contract_path: sap_erp/bronze/bronze_erp_customers_v2.yml
    
  - entity: customers
    layer: silver
    active_version: v1
    contract_path: sap_erp/silver/silver_erp_customers_v1.yml
```

### Benefits of the Registry:
-   **Safe Promotion**: To upgrade to `v3`, you test the new YAML in a dev branch and then simply update the `active_version` in the registry.
-   **Multi-Version Support**: You can run `v1` and `v2` in parallel during a migration by having both entries in the registry.

---

## 4. Metadata Standard

Every contract in the LakeLogic ecosystem should include standard metadata. This allows tools to "Capture" and "Browse" your contracts easily.

| Field | Description |
| :--- | :--- |
| **`title`** | Human-readable name (e.g., "Customer Master Data"). |
| **`owner`** | The Slack channel or Team ID responsible for this data. |
| **`status`** | `draft`, `active`, `deprecated`, or `emergency`. |
| **`classification`** | `PII`, `Financial`, or `Public`. |

## 5. Reference Data & Cross-Domain Lookups

Reference data (ISO country codes, currency lists, product categories) is often used across **multiple** domains. To avoid duplicating contracts, we recommend a dedicated `shared/` or `reference/` domain.

### Shared Hierarchy
```text
contracts/
├── shared/                     <-- Global Reference Data
│   ├── geo/
│   │   └── silver_reference_countries_v1.yml    <-- Used by Finance, Marketing, and Logisitics
│   └── currency/
│       └── rates_v1.yml
```

### The "Lookup" Lifecycle
When a **Finance** contract needs to perform a `lookup` against **Shared** reference data, it should reference the "Silver" (cleaned) version of that reference table.

1.  **Shared Owner**: Validates and publishes `silver_reference_countries` using the `silver_reference_countries_v1.yml` contract.
2.  **Finance Owner**: Points their `lookup` rule to `silver_reference_countries`.
3.  **Safety**: Because `silver_reference_countries` has its own contract, the Finance team is guaranteed that the lookup data is valid and schema-compliant.

By centralizing reference data contracts, you ensure that "United States" is represented as `US` (or `USA`) consistently across your entire company. 🛡️🌍

### Table-Based Links (Lakehouse)
If your reference data already lives in a Lakehouse table, you can point `links` directly at a table name (Spark only in OSS).

```yaml
links:
  - name: dim_geography
    type: table
    table: main.reference.dim_geography
    broadcast: true  # Spark-only hint for small lookup tables
```
