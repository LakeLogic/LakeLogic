# Schema & Model

!!! abstract "Open Lakehouse Contract - the standard"
    LakeLogic implements the **[Open Lakehouse Contract (OLC)](https://lakelogic.github.io/open-lakehouse-contract/)**, the open
    standard for lakehouse data contracts. The authoritative field-level spec for
    **schema & model** is the OLC reference: **[schema & model reference →](https://lakelogic.github.io/open-lakehouse-contract/reference/schema/)**.
    This page shows how the LakeLogic runtime *applies* that spec (engine behavior,
    examples, and runtime-specific notes).


The `model:` block defines the expected schema — field names, types, constraints, PII/sensitive classification, and masking strategies.

---

## Field Definition

The `fields:` array is the core of your contract. It defines the exact shape of your data, including data types, nullability, descriptions, and data quality rules that the LakeLogic engine will enforce upstream.

```yaml
model:

  fields:
    - name: "customer_id"
      type: "long"                    # string | int | long | double | boolean | date | timestamp
      required: true                  # Generates automatic not_null rule
      pii: false
      classification: "public"       # public | internal | confidential | restricted
      description: "Unique customer identifier"

      # Generator hints (used by DataGenerator for synthetic data)
      min: 1
      max: 999999
      accepted_values: ["premium", "standard", "basic"]

      # Foreign key reference
      foreign_key:
        contract: "silver_agents"
        column: "agent_id"
        severity: "error"

      # Field-level quality rules
      rules:
        - name: "customer_id_positive"
          sql: "customer_id > 0"
          category: "correctness"
          severity: "error"
```

---

## PII Masking

LakeLogic provides native, declarative PII masking. By tagging a field with `pii: true` and choosing a strategy, the engine will automatically mask or encrypt the data during processing—ensuring sensitive information is secured before it ever lands in your downstream analytical tables.

```yaml
    - name: "email"
      type: "string"
      pii: true
      classification: "confidential"
      security_groups: ["pii-readers", "compliance-team"]
      masking: "partial"
```

### Masking Strategies

| Strategy | Output | Joinable | Reversible | GDPR |
|---|---|---|---|---|
| `nullify` | `NULL` | No | No | ✓ |
| `hash` | `a3f8b2c1d4...` (SHA-256) | Yes | No | ✓ |
| `redact` | `***REDACTED***` | No | No | ✓ |
| `partial` | `j***@company.com` | No | No | ✗ |
| `encrypt` | `enc:gAAAAABh...` (AES-256) | Yes | Yes | ✓ |

### Sensitive (Non-Personal) Fields

Not every confidential value is *personal* data. A bank account number, salary, internal API key, or negotiated price is **sensitive** — it must be hidden from unauthorized readers — but it is **not** subject to GDPR/HIPAA erasure (the "right to be forgotten"), and records like these often carry retention obligations that make erasure incorrect.

Tag these with `sensitive: true`. They mask through the **exact same** mechanism as `pii` — `security_groups` + a `masking:` strategy — and are tagged `COMMENT 'SENSITIVE'` in generated DDL, but they are **never** pulled into the GDPR/HIPAA erasure selectors.

```yaml
    - name: "bank_account"
      type: "string"
      sensitive: true
      masking: "hash"
      security_groups: ["finance-admins"]   # optional — omit to always mask
```

| Flag | Masked | Erased (right-to-be-forgotten) | DDL tag |
|---|---|---|---|
| `pii: true` | ✓ | ✓ | `PII` |
| `sensitive: true` | ✓ | ✗ | `SENSITIVE` |

A `sensitive` field with no `masking:` strategy **and** no `security_groups` raises the **`SENS-001`** lint warning — the sensitive-data counterpart of `PII-002`. If a field is somehow flagged both `pii` and `sensitive`, PII wins (it's the stronger classification).

### Custom Partial Masking Format

```yaml
      masking: "partial"
      masking_format: "{first1}***@{domain}"
      # Tokens: {first1}-{first9}, {last1}-{last9}, {domain}
      # "j***@company.com", "***-***-1234", "SW** ***"
```

### Key Injection

```yaml
      masking: "encrypt"
      # Keys are injected via the orchestrator using
      # the LAKELOGIC_PII_KEY environment variable.
```

### Decryption Workflows

Because `encrypt` uses symmetric encryption (Fernet / AES), the same key used for masking is required to decrypt. LakeLogic encrypts PII at rest during the pipeline run, so your Delta/Parquet files on storage are always protected — regardless of engine or platform.

#### Contract Example

```yaml
model:
  fields:
    - name: "user_id"
      type: "string"
      pii: true
      masking: "encrypt"
      security_groups: ["pii-readers", "compliance-team"]
      # Users NOT in these groups see: enc:gAAAAABh...
      # Users IN these groups can decrypt with the key

    - name: "email"
      type: "string"
      pii: true
      masking: "partial"
      masking_format: "{first1}***@{domain}"
      security_groups: ["pii-readers"]
      # Users NOT in these groups see: j***@company.com

    - name: "session_id"
      type: "long"
      pii: false
      # No masking — visible to everyone
```

#### 1. Encryption at Rest + Programmatic Decryption (Recommended)

LakeLogic physically writes encrypted values (`enc:gAAAAABh...`) into your Bronze/Silver tables. The raw files on ADLS, S3, or local disk are protected even if storage credentials leak.

**Encrypted data at rest (what everyone sees by default):**

| session_id | user_id | email |
| --- | --- | --- |
| 1001 | `enc:gAAAAABhX9...kQ2dE=` | `j***@company.com` |
| 1002 | `enc:gAAAAABhY1...pR7mA=` | `a***@gmail.com` |
| 1003 | `enc:gAAAAABhZ3...nT4bC=` | `m***@corp.co.uk` |

**Decrypted query (authorized users with the key):**

=== "PySpark"

    ```python
    import os
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = SparkSession.builder.getOrCreate()

    # 1. Prepare the same padded key used during pipeline masking
    raw_key = os.environ["LAKELOGIC_PII_KEY"]
    padded_key = (raw_key * ((16 // len(raw_key)) + 1))[:16]

    # 2. Read the encrypted table
    df = spark.read.format("delta").load("lakehouse/marketing/bronze_google_analytics_events")

    # 3. Decrypt the user_id column
    df = df.withColumn(
        "user_id",
        F.when(
            F.col("user_id").startswith("enc:"),
            F.expr(f"CAST(aes_decrypt(unbase64(substring(user_id, 5)), '{padded_key}') AS STRING)"),
        ).otherwise(F.col("user_id")),
    )
    ```

=== "Polars"

    ```python
    import os, base64, hashlib, polars as pl
    from cryptography.fernet import Fernet

    # 1. Build the Fernet cipher from the same pipeline key
    raw_key = os.environ["LAKELOGIC_PII_KEY"].encode("utf-8")
    fernet_key = base64.urlsafe_b64encode(hashlib.sha256(raw_key).digest())
    fern = Fernet(fernet_key)

    # 2. Read the encrypted table
    df = pl.read_delta("lakehouse/marketing/bronze_google_analytics_events")

    # 3. Decrypt the user_id column
    df = df.with_columns(
        pl.col("user_id")
        .map_elements(
            lambda v: fern.decrypt(v[4:].encode("ascii")).decode("utf-8") if v and v.startswith("enc:") else v,
            return_dtype=pl.Utf8,
        )
        .alias("user_id")
    )
    ```

**Decrypted output:**

| session_id | user_id | email |
| --- | --- | --- |
| 1001 | `USR-44821` | `j***@company.com` |
| 1002 | `USR-10392` | `a***@gmail.com` |
| 1003 | `USR-77564` | `m***@corp.co.uk` |

Works with Polars, Spark, or DuckDB — zero vendor lock-in. Delete the key to achieve **crypto-shredding** (GDPR Art. 17 Right to Erasure).

#### 2. Dynamic Column Masking (Databricks Unity Catalog)

If you are on Databricks, you can optionally layer Unity Catalog column masks on top. Use `MaskingEngine.generate_uc_masks()` to generate `CREATE FUNCTION` + `ALTER TABLE SET MASK` DDL tied to your contract's `security_groups`. Unauthorized users see encrypted values; authorized users see plaintext — transparently, with no application code required.

### Pipeline Masking vs Query-Time Access Control

LakeLogic always applies maximum PII protection by default. When the pipeline runs, every field with `pii: true` is masked before materialization — regardless of who triggered the pipeline. You will see this confirmed in the logs:

```log
PII masking: 1 field(s) for user_groups=(none): user_id→encrypt
```

`user_groups=(none)` means no special access groups were provided, so all PII fields are fully masked. This is the correct default behaviour.

#### The `user_groups` Escape Hatch

LakeLogic supports an optional `user_groups` parameter that can selectively skip masking for specific fields. This is designed for teams that **do not have enterprise-grade column-level security** (e.g., Unity Catalog, Ranger, or Lake Formation) and need to produce separate materialized outputs for different access tiers.

```yaml
    - name: user_id
      type: string
      pii: true
      masking: "encrypt"
      security_groups: ["fraud-team", "compliance-team"]
```

If you pass `user_groups=["fraud-team"]` into the `DataProcessor`, the engine skips masking for fields where the caller's group is listed in `security_groups`.

#### When to Use Each Approach

| Approach | Use When | Trade-Off |
|:---|:---|:---|
| **Pipeline masking (default)** | Always | Data is protected at rest — even if storage credentials leak |
| **Query-time RBAC** (Unity Catalog, Ranger) | You have enterprise governance tooling | Unmasking happens transparently at read time; raw PII never sits unprotected on disk |
| **`user_groups` at pipeline time** | No enterprise RBAC available; need separate masked/unmasked outputs | Unmasked PII is permanently written to the target table — secure the storage path with tight ACLs |

> [!IMPORTANT]
> **Recommended pattern:** Always mask at rest during the pipeline (the default). Enforce selective unmasking at **query time** using your platform's column-level security (Unity Catalog, Ranger, Lake Formation). Reserve `user_groups` for environments where query-time RBAC is not available and you need to produce a separate, tightly ACL'd materialization for a specific consumer team.

## Kimball Modeling Flags

When building analytical data models, specifically **Accumulating Snapshot Fact Tables**, you often track an entity (like an Order) as it moves through various stages (e.g., *Placed -> Shipped -> Delivered*). 

LakeLogic provides native contract flags to automate this lifecycle tracking so you don't have to write complex SQL merges:

* **`milestone: true`**: Tells the system this column represents a stage in a process. The pipeline will automatically preserve the timestamp of this milestone once it is set, preventing late-arriving source updates from accidentally over-writing the original event time.
* **`generated: true`**: Tells the system to completely ignore this column if it arrives from the upstream source system. Instead, LakeLogic will automatically generate the timestamp internally when the milestone condition is met.

```yaml
    - name: "shipped_date"
      type: "timestamp"
      nullable: true             # Milestones start as NULL until the event occurs
      milestone: true            # Locks the timestamp once the order ships
      generated: true            # LakeLogic generates this, ignoring the source API
```

#### Example: Order Lifecycle Tracking

Imagine you are tracking `orders`. The system receives repeated updates for `order_1` over several days as its `status` changes from `"placed"` to `"shipped"` to `"delivered"`.

**Day 1: Order is placed (Source System)**
The upstream API sends a new order. The `shipped_date` doesn't exist yet, so it is NULL in our Lakehouse.

| order_id | status | shipped_date (Lakehouse) |
| --- | --- | --- |
| order_1 | `placed` | `NULL` |

**Day 2: Order ships (Source System)**
The API sends an update for `order_1` with `status: "shipped"`. Because `shipped_date` has `generated: true`, LakeLogic ignores any date the API might have sent and automatically stamps the current extraction/processing time into the Silver table.

| order_id | status | shipped_date (Lakehouse) |
| --- | --- | --- |
| order_1 | `shipped` | `2026-04-12 09:14:00` |

**Day 3: Order is delivered (Source System)**
The API sends a final update for `order_1` with `status: "delivered"`. Because `shipped_date` has `milestone: true`, LakeLogic protects the historical timestamp. It prevents the record's `shipped_date` from being overwritten by NULL or updated to a newer date, permanently locking the milestone in the accumulating snapshot fact table!

| order_id | status | shipped_date (Lakehouse) |
| --- | --- | --- |
| order_1 | `delivered` | `2026-04-12 09:14:00` *(Preserved!)* |

---

## Lineage Columns

LakeLogic automatically injects audit and lineage metadata columns into every record as it flows through the medallion architecture. You can customize the column names and toggle specific tracker fields in your contract's `lineage:` block.

```yaml
lineage:
  enabled: true
  capture_source_path: true
  source_column_name: "_lakelogic_source"
  capture_timestamp: true
  timestamp_column_name: "_lakelogic_processed_at"
  capture_run_id: true
  run_id_column_name: "_lakelogic_run_id"
  capture_contract_name: true
  contract_name_column_name: "_lakelogic_contract_name"
  capture_domain: true
  domain_column_name: "_lakelogic_domain"
  capture_system: true
  system_column_name: "_lakelogic_system"
  capture_created_at: true
  created_at_column_name: "_lakelogic_created_at"
  capture_created_by: true
  created_by_column_name: "_lakelogic_created_by"
  created_by_override: "etl_pipeline_svc"

  # Upstream lineage
  preserve_upstream: ["_upstream_run_id", "_upstream_source"]
  upstream_prefix: "_upstream"
  run_id_source: "run_id"       # run_id | pipeline_run_id
```

---

## Schema Policy

The schema policy block acts as a gatekeeper, controlling how the pipeline should react when the upstream data source unexpectedly changes its schema (e.g., adding undocumented columns or changing data types).

```yaml
schema_policy:
  evolution: "allow"             # strict | compatible | allow  (default: allow)
  unknown_fields: "allow"        # quarantine | drop | allow    (default: allow)
```

!!! info "Default: `allow` for frictionless prototyping"
    Both `evolution` and `unknown_fields` default to `"allow"`. This means contracts without an explicit `schema_policy` block will accept new columns and schema changes automatically — ideal for rapid development and Bronze-layer ingestion. When you're ready to harden a production pipeline, explicitly set `evolution: "strict"` and/or `unknown_fields: "quarantine"` in your contract or `_system.yaml`.

### Evolution (`evolution`)

Controls how the engine reacts when the source data's schema changes (e.g., fields are added or data types change).

* **`strict`**
  * Any changes to the schema cause the pipeline to fail with a clear error.
  * **Use Case:** Highly regulated environments (Finance, Healthcare) or Bronze-to-Silver pipelines where you want explicit human approval before any contract evolution is allowed.
* **`compatible`** 
  * New fields are allowed, and "safe" type promotions (e.g., `INT` to `LONG`) are permitted, but dangerous type changes (e.g., `STRING` to `INT`) are routed to quarantine (and will only fail the pipeline if `fail_on_quarantine: true` is set).
  * **Use Case:** Agile environments where source teams frequently add new metrics or dimensions and you don't want the pipeline failing constantly, but you still need downstream queries to remain stable.
* **`allow`** (Default)
  * All schema changes, including type overrides and missing required fields, are allowed through.
  * **Use Case:** Ingesting raw JSON into the Landing/Bronze layer where you simply want 100% of the data persisted, regardless of structural changes upstream. Great for prototyping and rapid iteration.

### Unknown Fields (`unknown_fields`)

Controls what happens when a field arrives in the source data that is **not** explicitly defined in the contract's `model.fields` block.

* **`quarantine`**
  * The row is routed to the quarantine table so analytical tables stay clean, but no data is lost.
  * **Use Case:** Ensuring zero data loss while strictly maintaining the integrity of the Silver/Gold tables. You want to see the unexpected fields before deciding whether to modify the contract to include them.
* **`drop`**
  * The row proceeds through the pipeline, but the undefined field is silently stripped out. 
  * **Use Case:** Ingesting wide tables (e.g., a massive 500-column Salesforce export) where you only care about 15 specific fields. Dropping the rest saves compute and storage.
* **`allow`** (Default)
  * The row proceeds through the pipeline and the undefined field is actively preserved. It relies entirely on your physical `schema_evolution` (`allow` or `compatible`) permitting structural mutations of the target table, otherwise the final Delta write will fail.
  * **Use Case:** Highly resilient continuous-ingestion pipelines (like Bronze APIs) where your goal is zero data loss, preserving all unknown metadata columns for future analytical evaluation without halting the pipeline. Also the recommended default for prototyping.
---

## Server Schema Directives

While `schema_policy` governs the logical validation layer natively, the target storage engine and input parsing behaviors are governed by server-level directives. These map directly to physical database interactions and can be configured holistically via `server` in `_system.yaml` or overridden per-contract in the `server:` block.

```yaml
# Inside _system.yaml (Global Defaults — per-layer)
server:
  bronze:
    mode: "ingest"
    schema_policy:
      evolution: "append"           # accept new fields automatically
      unknown_fields: "allow"       # preserve undocumented columns
    cast_to_string: true            # "all strings" bronze pattern
    
  silver:
    mode: "validate"
    schema_policy:
      evolution: "strict"           # curated — no surprises
      unknown_fields: "quarantine"  # route unknown columns to quarantine

  gold:
    mode: "validate"
    schema_policy:
      evolution: "strict"           # business layer — tightly controlled
      unknown_fields: "quarantine"

# ─────────────────────────────────────────────────────────────

# Inside bronze_messy_api_v1.0.yaml (Local Override)
server:
  type: local
  path: "."
  schema_policy:
    evolution: "allow"              # This contract overrides the system default
    unknown_fields: "allow"
```

!!! warning "Deprecated Keys"
    The following root-level server keys are deprecated and will emit warnings during validation:

    - `schema_evolution` → use `schema_policy.evolution`
    - `allow_schema_drift` → use `schema_policy.unknown_fields`

    See [Schema Validation API](schema_api.md#deprecated-keys) for migration details.

### Cast to String (`cast_to_string`)

A boolean flag that instructs the engine to forcibly coerce all incoming data fields into strings, entirely bypassing any type definitions or inferences made by the source format.

* **`true`**
  * Every parsed field is immediately safely cast to `STRING`/`VARCHAR` during the initial read phase. 
  * **Use Case:** The recommended pattern for raw "Bronze" ingestion. By forcing everything to strings, you eliminate data type mismatch errors entirely (e.g., an upstream developer unexpectedly changing an `integer` status ID to an alphanumeric `string` code). You safely land 100% of the raw data, preserving its exact state, and perform structured type-casting and quality gating later in the pipeline when moving from Bronze to Silver.
* **`false`** (Default)
  * Fields maintain their native or inferred strong types during data parsing and materialization.

