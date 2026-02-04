# Why LakeGuard? ⚔️

Choosing the right tool for your data platform is critical. Here is how LakeGuard compares to other popular tools like **dbt Tests** and **Great Expectations**.

## Comparison Table

| Feature | LakeGuard 🛡️ | dbt Tests 🧪 | Great Expectations 🦒 |
| :--- | :--- | :--- | :--- |
| **Primary Focus** | Data Contracts & Runtime Enforcement | SQL Building & Post-load Testing | Data Profiling & Documentation |
| **When it runs** | **Before** or **During** data movement | **After** data is already in the table | **After** data is loaded |
| **Engine Support** | Polars, Spark, DuckDB, Pandas | SQL Warehouse (Snowflake, BigQuery, etc.) | Python-based |
| **Bad Data Handling** | **Quarantine**: Detours bad rows automatically | Fails the job or logs the error | Generates a JSON report |
| **Goal** | **Prevention**: Stop bad data from entering | **Detection**: Find bad data after the fact | **Observation**: Understand data drift |

---

## 🆚 LakeGuard vs. dbt Tests

**dbt** is great for building models once your data is already in your warehouse. However, dbt tests usually run *after* the model is built. 

-   **The Problem with dbt**: By the time the test fails, your "Silver" table is already polluted with bad data.
-   **The LakeGuard Solution**: LakeGuard acts as a **Quality Gate**. It validates the records *before* they are written. If a row is bad, it goes to Quarantine, while the rest of the job continues.

## 🆚 LakeGuard vs. Great Expectations (GX)

**GX** is an incredible tool for data observability and profiling. However, it can be very "heavy" to set up and is primarily aimed at producing validation reports.

-   **The Difference**: GX tells you *that* your data is broken via a report. LakeGuard **enforces** the contract at the row-level during the ETL process.
-   **Simplicity**: LakeGuard uses standard SQL for everything, making it much easier for SQL-focused data engineers to adopt than the Python-heavy GX API.

## Summary: The LakeGuard Edge

LakeGuard isn't just about "Testing." It's about **Runtime Guarantees**. 

It turns your Data Contract from a passive PDF or YAML file into an **active security guard** that manages your Medallion architecture automatically. 🛡️🏛️
