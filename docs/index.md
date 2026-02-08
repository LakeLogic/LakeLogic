# LakeGuard

**The SQL-First Quality Gate for your Data Lakehouse.**

LakeGuard ensures that your business decisions are based on data you can trust. By separating your **Business Logic (The Intent)** from your **Infrastructure (The Execution)**, LakeGuard lets you enforce strict quality gates across your entire Medallion architecture with **zero code rewrites**.

```mermaid
graph TD
    %% Control Center
    Contract[<br/><b><font size='6'>DATA CONTRACT</font></b><br/>YAML / SQL Rules<br/>]
    
    %% Main Flow
    Source[(<br/><b><font size='6'>RAW SOURCE</font></b><br/>Files / DBs / Tables<br/>)]
    
    Gate{<br/><b><font size='7'>LAKEGUARD</font></b><br/><b><font size='5'>Quality Gate</font></b><br/>}
    
    Q[<br/><b><font size='6'>SAFE QUARANTINE</font></b><br/>Failure Logic / Reason Codes<br/>]
    
    Silver[<br/><b><font size='6'>SILVER LAYER</font></b><br/>Clean / Enriched<br/>]
    Gold[<br/><b><font size='6'>GOLD LAYER</font></b><br/>Aggregates / KPIs<br/>]
    Plat[<br/><b><font size='6'>PLATINUM</font></b><br/>AI / RAG Ready<br/>]

    %% Dependencies
    Contract ===>|<b><font size='5'>ENFORCES</font></b>| Gate
    Source ===>|<b><font size='5'>INGEST</font></b>| Gate
    
    Gate ===>|<b><font size='5'>VALIDATED</font></b>| Silver
    Gate ===>|<b><font size='5'>BROKEN</font></b>| Q
    
    Silver ===> Gold ===> Plat

    %% Telemetry Branch
    Gate -.-|<b><font size='4'>CAPTURES</font></b>| Telem[<br/><b><font size='6'>TELEMETRY</font></b><br/>]
    Telem -.- Lineage[<b><font size='5'>LINEAGE</font></b>]
    Telem -.- Metrics[<b><font size='5'>METRICS</font></b>]

    %% Styles
    style Gate fill:#1e40af,stroke:#1e3a8a,color:#ffffff,stroke-width:8px
    style Q fill:#b91c1c,stroke:#7f1d1d,color:#ffffff,stroke-width:4px
    style Source fill:#c2410c,stroke:#78350f,color:#ffffff,stroke-width:4px
    style Silver fill:#0f172a,stroke:#1e293b,color:#ffffff,stroke-width:3px
    style Gold fill:#a16207,stroke:#713f12,color:#ffffff,stroke-width:3px
    style Plat fill:#020617,stroke:#000000,color:#ffffff,stroke-width:3px
    style Contract fill:#f8fafc,stroke:#cbd5e1,color:#1e293b,stroke-width:3px
    
    style Telem fill:#f1f5f9,stroke:#94a3b8,color:#1e293b,stroke-width:2px,stroke-dasharray: 5 5
    style Lineage fill:#f8fafc,stroke:#cbd5e1,color:#1e293b,stroke-width:2px
    style Metrics fill:#f8fafc,stroke:#cbd5e1,color:#1e293b,stroke-width:2px

    %% Line Thickness
    linkStyle default stroke:#475569,stroke-width:6px
    linkStyle 6,7,8 stroke:#94a3b8,stroke-width:4px,stroke-dasharray: 5 5
```

## The Core Value: Write Once. Run Anywhere

Stop paying the "Re-adaptation Tax." In a traditional stack, moving from a Warehouse (SQL) to a Lakehouse (PySpark) means rewriting your validation rules. With LakeGuard, your **Data Contract is the Source of Truth**.

- **SQL-First:** Define your constraints, rules, and logic in standard SQL—the language your team already speaks.
- **Zero Adaptation:** Move your pipelines from **dbt/Snowflake** to **Databricks/Spark** to **Local/Polars** with **zero changes** to your contract.
- **No Vendor Lock-in:** Your business logic is a portable asset, independent of your cloud provider or execution engine.

## Business ROI: Cost, Risk, & Trust

### 1. Eliminate the "Spark Tax" (Cost Savings)

- **Result:** Cut compute spend by up to 80% for maintenance and small-to-medium datasets.

### 2. 100% Reconciliation (Risk Mitigation)

- **Result:** Mathematically provable data integrity. Bad data is detoured into a **Safe Quarantine** area, ensuring production dashboards are never poisoned.

### 3. Visual Traceability (Stakeholder Trust)

Gold-layer metrics should never be "Black Boxes." LakeGuard supports aggregate roll-ups that preserve source keys.

- **Result:** Provide business users with a visual drill-down from board-level KPIs back to the raw source records.

---

## Technical Capabilities

| Feature | Description |
| :--- | :--- |
| **Declarative Contracts** | Human-readable YAML defines schema, rules, and transforms. |
| **Engine Agnostic** | Auto-discovers and optimizes for Spark, Polars, DuckDB, or Pandas. |
| **SQL-First Rules** | Use standard SQL for Completeness, Correctness, and Consistency checks. |
| **Safe Quarantine** | Isolate bad rows without crashing the pipeline, with built-in reason codes. |
| **Lineage Injection** | Automatically audit every record with Run IDs, Timestamps, and Source paths. |
| **Registry Orchestration** | A generic driver to run Bronze → Silver → Gold layers with parallel execution. |

## Quick Start

The fastest way to get started is with **[uv](https://github.com/astral-sh/uv)**:

```bash
# Install with all engines
uv pip install "lakeguard[all]"

# Run your first contract (auto-discovers the best engine)
lakeguard run --contract my_contract.yaml --source raw_data.parquet
```

## Scale with LineageLogic

LakeGuard is the open-source engine that enforces your rules. For enterprise-scale management, **[LineageLogic](https://lineagelogic.com)** provides:

- **AI-Powered Contract Generation:** Don't write YAML by hand; generate it from your data in seconds.
- **Visual Governance:** See your real-time data health and lineage across your entire mesh.
- **Collaborative Approvals:** Manage contract lifecycle and versioning across decentralized teams.

---

[Quickstart](quickstart.md) | [How It Works](concepts.md) | [Patterns](deployment_patterns.md) | [CLI Usage](cli.md)
