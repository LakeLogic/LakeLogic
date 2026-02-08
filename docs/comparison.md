# Why LakeGuard? ⚔️

Choosing the right tool for your data platform depends on your specific needs. LakeGuard is designed to work alongside or as a complement to industry standards like **dbt** and **Great Expectations**.

## Comparison Table

| Feature | LakeGuard 🛡️ | dbt Tests 🧪 | Great Expectations 🦒 |
| :--- | :--- | :--- | :--- |
| **Primary Focus** | Runtime Data Contracts | Transformation & Warehouse Testing | Data Observability & Profiling |
| **Execution Point** | **During** Data Movement (ETL/ELT) | **After** Data Loading (Warehouse) | **Validation Reports** (Post-Process) |
| **Engine Support** | Polars, Spark, DuckDB, Pandas, Snowflake (table-only), BigQuery (table-only) | SQL Warehouse (Snowflake, BQ, etc.) | Python-based |
| **Handling Failures** | **Quarantine**: Detours bad rows in real-time | Fails the build or logs the error | Generates comprehensive quality reports |
| **Best Workflow** | **Prevention**: Shift-left data quality | **Transformation**: Model-driven testing | **Observability**: Long-term data health |

---

## 🤝 Better Together: LakeGuard & dbt

**dbt** is the industry standard for modeling and testing data inside the warehouse. LakeGuard complements dbt by handling the **runtime validation** before the data even reaches the warehouse.

-   **Complementary Strengths**: Use LakeGuard to "clean the front door" at the ingestion and Silver layers, and use dbt for complex business logic validation and cross-table reporting in the Gold layer.
-   **The Workflow**: LakeGuard ensures your **Silver** tables are Filtered, Cleaned, Transformed, and Enriched. dbt then builds your business models on top of these already-validated tables.

## 🤝 Better Together: LakeGuard & Great Expectations (GX)

**Great Expectations** is a powerful tool for deep data profiling and detailed observability documentation.

-   **Complementary Strengths**: GX is excellent for producing detailed "Data Docs" and profiling the overall health of a dataset. LakeGuard is optimized for **runtime enforcement**—making immediate, row-level decisions to quarantine data as it flows through a pipeline.
-   **The Workflow**: Use GX to build trust with stakeholders through rich visualization of data health, and use LakeGuard as the high-performance engine that enforces those health standards during every pipeline run.

## 🌍 Unified Governance: Cross-Platform Portability

In the modern enterprise, data often lives across multiple platforms. LakeGuard provides a **unified governance layer** that works consistently across your entire stack:

-   **Microsoft Fabric & Azure Synapse**: Use LakeGuard as the quality gate for your Spark-based notebooks and pipelines.
-   **Databricks**: Spark-based execution with Unity Catalog-compatible table logging (deeper integrations planned).
-   **Snowflake & BigQuery**: Table‑only warehouse adapters (SQL pushdown; file staging is still on the roadmap).

### Why this matters?
If you migrate from **Synapse to Fabric**, your Data Contracts stay exactly the same. Snowflake/BigQuery adapters now let you run contracts directly in the warehouse (table‑only).

---

## 💰 Modern Scale: The Polars/DuckDB Advantage

One of LakeGuard's core strengths is its engine-agnostic nature, allowing you to choose the most cost-effective compute for your data volume.

### 1. Optimizing Compute Costs
While Spark is the king of petabyte-scale data, many daily ingestion and validation tasks involve 1-100GB of data. For these workloads, a single-node container running **Polars** or **DuckDB** can be:
- **High Performance**: No JVM startup or cluster orchestration overhead.
- **Cost Effective**: Runs on a small Pod in AKS or a Serverless Container in ACA, reducing the need for multi-node clusters for simpler validation tasks.

### 2. Standardized Containerized Pipelines
LakeGuard provides a unified interface for data quality across your entire enterprise:
- **Dev/Test**: Run contracts locally on **DuckDB** for instantaneous debugging.
- **Mid-Scale Production**: Deploy to **AKS (Azure Kubernetes Service)** or **ACA (Azure Container Apps)** using **Polars**.
- **Large-Scale Production**: Seamlessly transition to **Databricks**, **Fabric**, or **Synapse** using **Spark**.

### 3. Spark-Native Scaling

When running on Spark, LakeGuard uses distributed operations throughout:

- **Merge/SCD2**: Uses native DataFrame joins (or Delta Lake `MERGE INTO`) instead of collecting to driver memory.
- **Counts/Metrics**: Single-pass aggregations to avoid multiple DAG executions.
- **No pandas bottleneck**: Data stays distributed for merge and SCD2 operations at any scale.

---

## Summary: The LakeGuard Edge

LakeGuard is about **Runtime Reliability** and **Infrastructure Flexibility**. It turns your Data Contract from passive documentation into an active layer of your Medallion architecture—Ensuring your **Silver** layer is Filtered, Cleaned, Transformed, and Enriched—working in harmony with your existing modeling and observability tools to ensure data integrity at every step across **Azure** and **AWS** Spark platforms and Snowflake/BigQuery (table‑only). 🛡️🏛️

---

## Need More?

LakeGuard is fully open source and works standalone. For enterprise teams looking for additional capabilities, [LineageLogic](https://lineagelogic.com) extends LakeGuard with AI-powered contract generation, visual lineage mapping, and collaborative governance workflows.
