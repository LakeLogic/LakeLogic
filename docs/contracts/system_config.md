# System Configuration (`_system.yaml`)

The system registry defines **storage, environments, and contract listings** for a single source system within a domain.

> **Think of it like a building's utility plan.** The domain config says "this is the Marketing building," and the system config says "here's where the electricity, water, and data pipes connect for the Google Analytics floor."

```text
domains_retail/marketing/
└── google_analytics/
    ├── _system.yaml          ← This file
    ├── bronze/
    │   └── events_v1.0.yaml
    └── silver/
        └── sessions_v1.0.yaml
```

---

## Core Structure

Every `_system.yaml` follows this pattern. Customise the values to match your environment:

!!! example "Example: Minimal system registry"

    ```yaml
    domain: marketing
    system: google_analytics

    # ── Metadata & Run Logs ──────────────────────────────────────
    metadata:
      run_log_table: "{log_path}"
      run_log_backend: "delta"

    # ── Server (per layer, contract overrides) ───────────────────
    # Applied to all contracts in this system unless overridden.
    server:
      bronze:
        mode: "ingest"
        format: "delta"
        schema_policy:
          evolution: "append"
          unknown_fields: "allow"
        cast_to_string: true
      silver:
        mode: "validate"
        format: "delta"
        schema_policy:
          evolution: "strict"
          unknown_fields: "quarantine"
      gold:
        mode: "validate"
        format: "delta"
        schema_policy:
          evolution: "strict"
          unknown_fields: "quarantine"

    # ── Materialization Defaults ─────────────────────────────────
    materialization:
      bronze:
        strategy: append
        format: delta
      silver:
        strategy: merge
        format: delta
        merge_dedup_guard: true
      gold:
        strategy: merge
        format: delta

    # ── Lineage ──────────────────────────────────────────────────
    lineage:
      enabled: true
      source_column_name: "_lakelogic_source"
      timestamp_column_name: "_lakelogic_processed_at"

    # ── Quarantine ───────────────────────────────────────────────
    quarantine:
      enabled: true
      fail_on_quarantine: false
      target: "{quarantine_root}/{domain}/{system}"

    # ── Extraction Defaults ─────────────────────────────────────
    # System-level LLM extraction defaults (override domain defaults).
    extraction_defaults:
      provider: "azure_openai"
      model: "gpt-4o"
      temperature: 0.0
      max_cost_per_run: 25.00
      redact_pii_before_llm: true

    # ── Notifications ───────────────────────────────────────────
    # System-specific channels (concatenated with domain channels).
    notifications_enabled: true   # Global switch to disable all system, domain, and contract notifications
    notifications:
      - target: "https://hooks.slack.com/services/YOUR/SYSTEM-SPECIFIC/WEBHOOK"
        on_events: ["failure", "quarantine"]

    # ── Cost Observability ──────────────────────────────────
    # Track estimated compute cost per pipeline run.
    # Budget limits and currency are inherited from _domain.yaml
    cost:
      provider: "manual"
      attribution: "duration_proportional"
      currency: "USD"
      
      rates:
        dbu_per_hour: 0.22
        storage_per_gb_month: 0.023
        
      # Optional: account for autoscaling clusters
      cluster:
        min_nodes: 2
        max_nodes: 8
        scaling_assumption: "avg"

    # ── Storage ──────────────────────────────────────────────────
    storage:
      # ── Table Resolution ──────────────────────────────────────
      # UC mode (Databricks):  tables resolve via domain_catalog
      # Direct mode (DuckDB/Polars/Fabric/Synapse/EMR): tables resolve via external_location_root
      domain_catalog: "`{catalog}`.{domain}"
      external_location_root: "{data_root}"

      # ── Operational Paths (UC mode — Databricks Volumes) ──────
      # Only used when storage_mode="uc". Direct mode ignores them.
      contract_root: "/Workspace/Shared/data_platform/domains/{domain}/{system}"
      landing_root: "/Volumes/{catalog}/nondelta/landing_{domain}/{system}"
      log_root: "/Volumes/{catalog}/nondelta/_logs"

      # ── Storage Paths (direct mode — cloud/local) ─────────────
      # Driven by {storage_root}, {data_root}, {quarantine_root}
      # which are defined per-environment below.
      landing_path: "{storage_root}/_data/{domain}/{system}"
      contract_path: "{storage_root}/_contracts/{domain}/{system}"
      log_path: "{storage_root}/_logs/{domain}"
      quarantine_path: "{quarantine_root}/{domain}/{system}"

    # ── Cloud Storage Anchors (DRY) ─────────────────────────────
    x-azure-storage: &azure_storage
      storage_root: "abfss://nondelta@{storage_account}.dfs.core.windows.net"
      data_root: "abfss://{domain}@{storage_account}.dfs.core.windows.net"
      quarantine_root: "abfss://quarantine@{storage_account}.dfs.core.windows.net"

    # ── Environments ────────────────────────────────────────────
    # Use ${ENV_VAR} to resolve secrets from environment variables.
    # This keeps infrastructure names out of source control.
    environments:
      dev:
        catalog: "${LAKELOGIC_DEV_CATALOG}"
        storage_account: "${LAKELOGIC_DEV_STORAGE_ACCOUNT}"
        <<: *azure_storage
      staging:
        catalog: "${LAKELOGIC_STG_CATALOG}"
        storage_account: "${LAKELOGIC_STG_STORAGE_ACCOUNT}"
        <<: *azure_storage
      prod:
        catalog: "${LAKELOGIC_PROD_CATALOG}"
        storage_account: "${LAKELOGIC_PROD_STORAGE_ACCOUNT}"
        <<: *azure_storage
      local:
        catalog: "local"
        storage_root: "./lakehouse"
        data_root: "./lakehouse/{domain}"
        quarantine_root: "./lakehouse/_quarantine"
      colab:
        catalog: "colab"
        storage_root: "/content/lake"
        data_root: "/content/lake/{domain}"
        quarantine_root: "/content/lake/_quarantine"

    # ── Contracts ───────────────────────────────────────────────
    contracts:
      # ── Bronze Layer ────────────────────────────────────────────
      - layer: bronze
        entity: events
        path: "contracts/{bronze_layer}/{bronze_layer}_{system}_events_v1.0.yaml"
        enabled: true

      - layer: bronze
        entity: sessions
        path: "contracts/{bronze_layer}/{bronze_layer}_{system}_sessions_v1.0.yaml"
        enabled: true

      # ── Silver Layer ────────────────────────────────────────────
      - layer: silver
        entity: sessions_cleaned
        path: "contracts/{silver_layer}/{silver_layer}_{system}_sessions_v1.0.yaml"
        enabled: true

      - layer: silver
        entity: events_cleaned
        path: "contracts/{silver_layer}/{silver_layer}_{system}_events_v1.0.yaml"
        depends_on: [sessions]
        enabled: true

      # ── Gold Layer ──────────────────────────────────────────────
      - layer: gold
        entity: fact_aggregate_channel_performance
        path: "contracts/{gold_layer}/{gold_layer}_{system}_fact_aggregate_channel_performance_v1.0.yaml"
        enabled: true

      - layer: gold
        entity: dim_events_scd2
        path: "contracts/{gold_layer}/{gold_layer}_{system}_dim_events_scd2_v1.0.yaml"
        depends_on: [fact_aggregate_channel_performance]
        enabled: true

      - layer: gold
        entity: fact_accumulating_snapshot_session_funnel
        path: "contracts/{gold_layer}/{gold_layer}_{system}_fact_accumulating_snapshot_session_funnel_v1.0.yaml"
        depends_on: [dim_events_scd2]
        enabled: true

      - layer: gold
        entity: fact_periodic_snapshot_user_daily
        path: "contracts/{gold_layer}/{gold_layer}_{system}_fact_periodic_snapshot_user_daily_v1.0.yaml"
        depends_on: [dim_events_scd2]
        enabled: true

      - layer: gold
        entity: fact_factless_user_conversions
        path: "contracts/{gold_layer}/{gold_layer}_{system}_fact_factless_user_conversions_v1.0.yaml"
        depends_on: [dim_events_scd2]
        enabled: false
    ```

### Explicit vs. Implicit Dependencies (`depends_on`)

LakeLogic pipelines execute sequentially by layer by default (**Landing → Bronze → Silver → Gold**). Because of this inherent layer-by-layer progression, a Silver contract will *always* process after its upstream Bronze contract has finished. 

**Rule of Thumb:**
* **Cross-layer edges are always inferred automatically.** The DAG generator reads each contract's `source.path` and `links:` block to draw the Bronze→Silver and Silver→Gold data-flow lines. You never need to manually declare these.
* **`depends_on` is for intra-layer ordering only.** Use it to ensure dimensional tables (e.g., `silver_rideflow_driver_profiles`) are processed before fact tables (e.g., `silver_rideflow_trips`) within the same Silver layer. The DAG will draw these as blue dependency arrows alongside the automatically inferred cross-layer edges.

Therefore, `depends_on` should primarily be reserved for orchestrating dependencies *within* a specific layer. You do not need to list upstream Bronze tables in a Silver contract's `depends_on` array — LakeLogic handles inter-layer sequencing and DAG visualization automatically.

---

## Global Defaults vs. Local Overrides

LakeLogic uses a powerful inheritance model to keep your data contracts clean. It explicitly separates **Global Defaults** from **Local Overrides**:

### 1. `server` in `_system.yaml` (The Global Template)

This block lives in your `_system.yaml` registry. It acts as the blanket rule for the entire system layer. If you define `evolution: strict` here under `bronze:`, you are telling the engine: *"Unless told otherwise, treat every single Bronze contract in this network as strictly locked down."*

This saves you from copying and pasting the exact same `server:` boilerplate into 50 different contract YAML files!

### 2. `server` in a Contract (The Local Override)

This block lives *inside* a specific individual Data Contract file (e.g., `bronze_google_analytics_events_v1.0.yaml`). Any setting you define here **overrides** whatever was set in the global system defaults.

**Example Use Case:**
Imagine you have 50 Bronze tables. You want 49 of them to be highly regulated and locked down, but one specific API is notoriously messy and you just want to let it drift safely.

You would set your `_system.yaml` to:

```yaml
server:
  bronze:
    schema_policy:
      evolution: strict   # 49 tables inherit this
```

And then, strictly inside that one messy contract file, you would define:

```yaml
server:
  schema_policy:
    evolution: allow      # This specific contract overrides the default
```

LakeLogic automatically merges them at runtime: inheriting the broad infrastructure defaults from the system registry, while respecting the fine-grained custom behaviors of an individual contract.

---

## Property Reference

### `domain` / `system`

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `domain` | `string` | **Yes** | Domain name (inherited from `_domain.yaml` if not set) |
| `system` | `string` | **Yes** | Source system identifier (e.g. `"google_analytics"`, `"rideflow"`) |

---

### `metadata`

Controls run logging and pipeline observability.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `run_log_table` | `string` | `null` | Path or table name for the run log (e.g. `"{log_path}"`) |
| `run_log_backend` | `string` | `"delta"` | Storage backend for run logs (`"delta"`, `"json"`) |

---

### `server`

Per-layer server configuration inherited by all contracts. Each layer (`bronze`, `silver`, `gold`) can have its own settings.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `mode` | `string` | `"validate"` | `"ingest"` (raw-to-Bronze) or `"validate"` (Quality Gate) |
| `format` | `string` | `"delta"` | Output format (`"delta"`, `"parquet"`, `"iceberg"`, `"csv"`, `"json"`) |
| `cast_to_string` | `bool` | `false` | Cast all fields to string (Bronze raw ingestion) |

#### `server.<layer>.schema_policy`

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `evolution` | `string` | `"allow"` | `"strict"`, `"append"`, `"merge"`, `"overwrite"`, `"compatible"`, `"allow"` |
| `unknown_fields` | `string` | `"allow"` | `"quarantine"` (route to quarantine), `"drop"` (silently discard), `"allow"` (keep) |

#### `server.bronze.post_ingestion`

Landing zone lifecycle policy — what to do with source files **after a successful Bronze commit**. See [Zero-Retention Architecture](data_product_contracts/ingestion.md#zero-retention-architecture-post-ingestion-lifecycle) for full details.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `action` | `string` | `"retain"` | `"delete"` (zero-retention), `"archive"` (move to archive), `"retain"` (no-op) |
| `cleanup_is_blocking` | `bool` | `false` | If `true`, cleanup failure fails the pipeline |
| `retry_orphaned_files` | `bool` | `true` | Retry cleanup of previously failed files on next run |

!!! example "Example: GDPR zero-retention for all Bronze contracts"

    ```yaml
    server:
      bronze:
        cast_to_string: true
        schema_policy:
          evolution: append
          unknown_fields: allow
        post_ingestion:
          action: delete
          cleanup_is_blocking: false
          retry_orphaned_files: true
    ```

---

### `materialization`

Per-layer materialization defaults controlling how data is written to the target.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `strategy` | `string` | — | `"append"`, `"merge"`, `"overwrite"`, `"snapshot"` |
| `format` | `string` | `"delta"` | Output format |
| `merge_dedup_guard` | `bool` | `false` | Deduplicate rows before merge (Silver/Gold) |

---

### `lineage`

System-level lineage tracking defaults. Adds metadata columns to every processed table.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | `bool` | `true` | Enable lineage column injection |
| `source_column_name` | `string` | `"_lakelogic_source"` | Column name for source file/table path |
| `timestamp_column_name` | `string` | `"_lakelogic_processed_at"` | Column name for processing timestamp |

---

### `quarantine`

System-level quarantine defaults for rows that fail validation rules.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | `bool` | `true` | Enable quarantine for failed rows |
| `fail_on_quarantine` | `bool` | `false` | If `true`, pipeline fails when any rows are quarantined |
| `include_error_reason` | `bool` | `false` | Include the validation error in quarantine records |
| `target` | `string` | — | Quarantine table/path (e.g. `"{quarantine_path}"`) |
| `format` | `string` | `"delta"` | Output format for quarantine table |
| `mode` | `string` | `"append"` | Write mode for quarantine table |

---

### `compliance`

System-level compliance defaults (GDPR, data residency). Inherited by all contracts.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `data_residency` | `string` | `null` | Required data residency region (e.g. `"EU"`, `"US"`) |
| `gdpr.enabled` | `bool` | `false` | Enable GDPR right-to-erasure support |
| `gdpr.erasure_strategy` | `string` | `"nullify"` | `"nullify"`, `"hash"`, `"delete"` |

> **Residency enforcement:** If a contract requires `data_residency: EU` but the target environment's `region` is `US`, the engine logs a compliance violation warning at load time.

---

### Cost Observability

The `cost:` block enables automatic compute cost estimation for every pipeline run in this system. This system-level configuration handles **how** cost is measured, while the budget and authoritative reporting currency are defined centrally in `_domain.yaml`.

| Field | Required | Default | Description |
| --- | --- | --- | --- |
| `provider` | No | `"none"` | `"none"` (disabled), `"manual"` (duration × rate), `"databricks_uc"` (billing API) |
| `attribution` | No | `"duration_proportional"` | `"duration_proportional"`, `"row_proportional"`, or `"direct"` |
| `currency` | No | Inherited | Must match `cost.currency` in `_domain.yaml`. Mismatches log a warning. |
| `rates.dbu_per_hour` | No | `0.22` | Databricks Jobs Compute DBU rate per hour |
| `rates.storage_per_gb_month` | No | `0.023` | Delta storage cost per GB per month |
| `cluster.min_nodes` | No | `1` | Minimum nodes in the cluster |
| `cluster.max_nodes` | No | `min_nodes` | Maximum nodes in the cluster |
| `cluster.scaling_assumption` | No | `"avg"` | How to estimate node count during run. Options: `"avg"`, `"peak"`, `"min"`, `"p75"` |

#### Provider Options

* `"none"`: Cost tracking disabled.
* `"manual"`: Estimates cost using the formula: `run_duration_seconds × dbu_per_hour × avg_nodes / 3600`.
* `"databricks_uc"`: Queries `system.billing.usage` by `run_id` tag. Falls back to manual if no billing row is found.

#### Cluster Scaling Assumptions

When using autoscaling clusters (`min_nodes` < `max_nodes`), the manual provider uses a scaling assumption to blend the hourly rate.

* `"avg"` – Uses `(min + max) / 2`. Most common default for varied workloads.
* `"peak"` – Uses `max`. Conservative estimation, assumes worst-case.
* `"min"` – Uses `min`. Optimistic estimation for steady-state workloads.
* `"p75"` – Uses `min + 0.75 × (max - min)`. Good for near-peak, spiky workloads.

> **Tip:** Start with `provider: "manual"` to get immediate cost visibility using duration-based estimates. Upgrade to `"databricks_uc"` when you need exact cost attribution from the Unity Catalog billing tables.

Cost data is recorded in the run log as `estimated_cost`, `cost_currency`, and `cost_confidence` columns. See the [Observability docs](../observability.md#7-pipeline-cost-intelligence) for analytical queries and SaaS integration.

---

## Platform-Portable Storage

Use YAML anchors to define storage patterns once and reference them across environments. This means you write your cloud connection details **once** and reuse them everywhere.

!!! example "Example: Multi-cloud storage with YAML anchors"

    ```yaml
    # ── Storage Anchors (define once) ────────────────────────────
    storage:
      azure: &azure_storage
        storage_account: "youraccount"
        container: "lakehouse"
        storage_root: "abfss://lakehouse@youraccount.dfs.core.windows.net"
        data_root: "{storage_root}/{domain}"
        quarantine_root: "{storage_root}/_quarantine"
        log_path: "{data_root}/_run_logs"

      aws: &aws_storage
        bucket: "your-data-lake"
        storage_root: "s3://your-data-lake"
        data_root: "{storage_root}/{domain}"
        quarantine_root: "{storage_root}/_quarantine"
        log_path: "{data_root}/_run_logs"

      gcp: &gcp_storage
        bucket: "your-data-lake"
        storage_root: "gs://your-data-lake"
        data_root: "{storage_root}/{domain}"

      local: &local_storage
        storage_root: "./lakehouse"
        data_root: "{storage_root}/{domain}"
        quarantine_root: "{storage_root}/_quarantine"
        log_path: "{data_root}/_run_logs"

    # ── Environments ─────────────────────────────────────────────
    # Use ${ENV_VAR} syntax to keep secrets out of source control.
    environments:
      dev:
        <<: *azure_storage
        catalog: "${DEV_CATALOG}"
        storage_account: "${DEV_STORAGE_ACCOUNT}"
      prod:
        <<: *azure_storage
        catalog: "${PROD_CATALOG}"
        storage_account: "${PROD_STORAGE_ACCOUNT}"
      aws:
        <<: *aws_storage
        catalog: "${AWS_GLUE_CATALOG}"
      local:
        <<: *local_storage
        catalog: "local"
    ```

### Supported Platforms

| Environment | Platform | URI Scheme |
| --- | --- | --- |
| `dev/staging/prod` | Databricks UC (Azure) | `abfss://...dfs.core.windows.net` |
| `fabric` | Microsoft Fabric OneLake | `abfss://...onelake.dfs.fabric.microsoft.com` |
| `synapse` | Microsoft Synapse Spark | `abfss://...dfs.core.windows.net` |
| `aws` | Amazon EMR / S3 | `s3://` |
| `gcp` | Google Cloud / GCS | `gs://` |
| `local` | Local filesystem | `./lakehouse` |
| `colab` | Google Colab | `/content/lake` |

---

## Placeholder Variables

Contracts use `{placeholder}` syntax that resolves from the system registry. This keeps your contracts **portable** — change the storage path in one place, all contracts update automatically.

| Placeholder | Source | Example Value |
| --- | --- | --- |
| `{domain}` | `domain:` | `marketing` |
| `{system}` | `system:` | `google_analytics` |
| `{bronze_layer}` | `bronze_layer:` (or inherited from domain) | `bronze` |
| `{silver_layer}` | `silver_layer:` | `silver` |
| `{gold_layer}` | `gold_layer:` | `gold` |
| `{domain_catalog}` | Environment-specific `catalog:` | `retail_marketing` |
| `{storage_root}` | Environment-specific | `abfss://...` |
| `{data_root}` | Computed | `{storage_root}/{domain}` |
| `{log_path}` | Computed | `{data_root}/_run_logs` |

### Usage in Contracts

!!! example "Example: Placeholder usage in a contract"

    ```yaml
    source:
      path: "{data_root}/{bronze_layer}_{system}_events"

    materialization:
      target_path: "{data_root}/{silver_layer}_{system}_sessions"

    metadata:
      run_log_table: "{log_path}"
    ```

---

## Environment Variable Resolution

Any string value in the `environments:` block that uses the `${ENV_VAR}` syntax is automatically resolved from your system's environment variables at load time. This keeps infrastructure names, storage accounts, and catalog identifiers **out of source control**.

### Syntax

Wrap an environment variable name in `${...}`:

```yaml
environments:
  prod:
    catalog: "${MY_PROD_CATALOG}"           # resolves os.environ["MY_PROD_CATALOG"]
    storage_account: "${MY_PROD_STORAGE}"   # resolves os.environ["MY_PROD_STORAGE"]
```

!!! warning "The entire value must be a single `${...}` expression"
    Partial interpolation like `"prefix-${VAR}-suffix"` is **not** supported. Use `{placeholder}` syntax (see above) for template composition within storage paths.

### Which Fields Support It?

All string fields inside `environments.<env_name>` are resolved, including:

| Field | Example |
| --- | --- |
| `catalog` | `"${RIDEFLOW_DEV_CATALOG}"` |
| `storage_account` | `"${RIDEFLOW_DEV_STORAGE_ACCOUNT}"` |
| `region` | `"${DEPLOY_REGION}"` |
| Any extra field | Custom fields added via `ConfigDict(extra="allow")` |

> **Note:** `local` and `colab` environments typically use hardcoded values since they don't contain real infrastructure secrets.

### Where to Set the Variables

| Runtime | How to Set |
| --- | --- |
| **Databricks** | Cluster environment variables or Databricks Secrets scope |
| **Azure DevOps** | Pipeline variables / variable groups |
| **GitHub Actions** | Repository secrets → `env:` block in workflow |
| **Local development** | `.env` file, shell exports, or IDE run config |
| **Google Colab** | `os.environ["KEY"] = "value"` in a setup cell |

### Example: Before and After

=== "❌ Hardcoded (secrets in source control)"

    ```yaml
    environments:
      prod:
        catalog: "rideflow-lakehouse-prod-001"
        storage_account: "sarideflowprodadls001"
    ```

=== "✅ Environment Variables (secrets externalized)"

    ```yaml
    environments:
      prod:
        catalog: "${RIDEFLOW_PROD_CATALOG}"
        storage_account: "${RIDEFLOW_PROD_STORAGE_ACCOUNT}"
    ```

### Missing Variables

If an environment variable is not set, the value resolves to an **empty string** (`""`). This will typically surface as a clear error when the pipeline tries to connect to storage — e.g., `abfss://domain@.dfs.core.windows.net` (missing account name).

---

## Contract Listing

List which contracts belong to this system. Set `active: false` to disable a contract without deleting it.

!!! example "Example: Contract registry"

    ```yaml
    contracts:
      - path: bronze/events_v1.0.yaml
        entity: events
        layer: bronze
        active: true

      - path: silver/sessions_v1.0.yaml
        entity: sessions
        layer: silver
        active: true

      - path: gold/dim_users_v1.0.yaml
        entity: dim_users
        layer: gold
        active: false    # Disabled — won't run in pipeline
    ```

---

## External Sources (Cross-Domain Lineage)

When your domain consumes tables from **another** domain's pipeline, declare them here so the DAG shows the full data lineage across teams.

> **Why this matters:** Without this, your data lineage stops at your domain boundary. With it, you can trace data from its origin all the way through to your final dashboard — even across team boundaries.

!!! example "Example: Cross-domain source declaration"

    ```yaml
    external_sources:
      - name: "silver_crm_customers"
        catalog_path: "catalog.silver.crm_customers"
        source_domain: "sales/crm"
        consumed_by: ["gold_customer_360"]
    ```

External nodes appear in the DAG with dashed borders. LakeLogic does **not** orchestrate the external pipeline — this is metadata-only for lineage tracking.
