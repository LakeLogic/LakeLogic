# Pipelines & Parallelism

> **Think of your data pipeline like a car assembly line.**
> You can't install seats before the chassis is built. LakeLogic defines the dependency order (chassis → body → seats), while your orchestrator runs each station as fast as possible.

LakeLogic helps you define the network of table dependencies. Your orchestrator (Airflow, Dagster, Prefect, Databricks Workflows) handles the scheduling and execution.

---

## Contract Dependencies

Each contract declares its `upstream` dependencies, creating a clear lineage graph:

```yaml
info:
  title: Silver Customers (CRM)
  target_layer: silver

dataset: silver_crm_customers
upstream:
  - bronze_crm_customers
```

**Why this matters:** When `bronze_crm_customers` fails, your orchestrator automatically holds back `silver_crm_customers` — no stale downstream data, no manual intervention.

---

## Layered Pipeline Strategy

Organize your pipeline by medallion layers:

```text
  BRONZE                   SILVER                   GOLD
  ──────                   ──────                   ────
  bronze_crm_customers ──→ silver_crm_customers ──→ gold_dim_customers ─┐
  bronze_erp_products  ──→ silver_erp_products  ──→ gold_dim_products  ─┼─→ gold_fact_sales
  bronze_pos_orders    ──→ silver_pos_orders    ────────────────────────┘
```

| Layer | What happens | Parallelism |
|:---|:---|:---|
| **Bronze** | Raw ingestion, schema gate | ✅ All Bronze contracts run in parallel |
| **Silver** | Clean, validate, standardize | ✅ All Silver contracts run in parallel |
| **Gold** | Aggregate, join, enrich | Dimensions first, then facts |

**Why this matters:** Bronze and Silver contracts within the same layer have no dependencies on each other — run them all at once and cut your pipeline time dramatically.

---

## Naming Convention

Use a consistent `{layer}_{system}_{entity}` naming pattern:

| Contract | Meaning |
|:---|:---|
| `bronze_crm_customers` | Raw CRM customer data |
| `silver_crm_customers` | Validated, cleansed CRM customers |
| `gold_dim_customers` | Analytics-ready customer dimension |
| `gold_fact_sales` | Aggregated sales facts |

**Why this matters:** Anyone can read a contract name and instantly know its layer, source system, and entity — no documentation lookup needed.

---

## Parallelism

- **Orchestrators** handle task-level parallelism (running multiple contracts at once)
- **LakeLogic engines** are multi-threaded by default — Polars and DuckDB maximize your hardware on each task

You get parallelism at two levels: across contracts (orchestrator) and within each contract (engine).

---

## Gating Rules

Use quality results to decide whether downstream contracts should run:

| Condition | Action |
|:---|:---|
| Dataset rules fail | ❌ Block downstream — something is fundamentally wrong |
| Quarantine ratio > threshold | ❌ Block downstream — too much bad data |
| Only a few row quarantines | ✅ Continue — within acceptable tolerance |

```text
registry = load_all_contracts("contracts/")
DAG = build_dag_from_upstream(registry)

for node in topo_sort(DAG):
    report = run_contract(node)
    if report.dataset_rules_failed:
        stop_downstream(node)
```

**Why this matters:** Your pipeline self-heals by stopping before bad data cascades into Gold tables and dashboards.

---

## Missing Upstreams

When using `--window last_success`, LakeLogic checks the run log for upstream freshness:

| Scenario | Behaviour |
|:---|:---|
| Missing log table | Warning + falls back to full load |
| Missing upstream entry | Skips the downstream contract, records the reason |

This keeps pipelines safe by default — no silently stale Gold data.

---

## What's Next?

- **[Reprocessing & Partitioning](reprocessing.md)** — Handling late data and backfills
- **[Deployment Patterns](deployment_patterns.md)** — CI/CD and production deployment
