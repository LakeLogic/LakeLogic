# Domain Configuration (`_domain.yaml`)

The domain config file sits at the root of each domain directory and provides **governance defaults** inherited by all systems within the domain.

> **Think of it like a department charter.** It answers: "Who owns marketing data? What quality standards do we hold ourselves to? Who gets called when something breaks?" Every team (system) within Marketing inherits these answers automatically.

```text
domains_retail/
└── marketing/
    ├── _domain.yaml          ← This file
    ├── google_analytics/
    │   └── _system.yaml      ← Inherits from _domain.yaml
    └── klaviyo/
        └── _system.yaml      ← Also inherits from _domain.yaml
```

---

## Complete Reference

!!! example "Example: Full domain configuration"

    ```yaml
    # Identity
    domain: marketing

    # ── Domain Ownership ─────────────────────────────────────────
    # Who owns this domain? Used for governance, notifications,
    # and cost allocation.
    ownership:
      domain_owner: "Marketing Analytics Team"
      team: data_engineering
      contacts:
        - name: Marketing Analytics Lead
          role: domain_lead             # Used for notification routing
          email: marketing-analytics@acme.com
          slack: "#marketing-data"
        - name: Data Engineering On-Call
          role: oncall
          email: data-oncall@acme.com
          slack: "#data-oncall"
      cost_center: marketing_analytics
      jira_project: MKTG

    # ── Service Level Objectives ─────────────────────────────────
    # Domain-wide SLOs inherited by all systems. Systems and
    # contracts can override specific values.
    slo:

      freshness:
        bronze:
          max_delay_minutes: 60
          check_column: "_lakelogic_loaded_at"
          max_source_delay_minutes: 120
        silver:
          max_delay_minutes: 240
          check_column: "_lakelogic_processed_at"
          max_source_delay_minutes: 300

      row_count:
        bronze:
          min_rows: 20
          max_rows: 100000
          warn_only: false
          anomaly:
            enabled: true
            lookback_runs: 14
            min_ratio: 0.5
            max_ratio: 2.0
            method: "median"
            min_runs_before_enforcement: 5
        silver:
          min_rows: 10
          max_rows: 50000
          warn_only: false
          anomaly:
            enabled: true
            lookback_runs: 14
            min_ratio: 0.5
            max_ratio: 2.0
            method: "median"
            min_runs_before_enforcement: 5

      quality:
        min_good_ratio: 0.95
        max_quarantine_ratio: 0.05
        by_severity:
          critical:
            min_good_ratio: 0.999
          high:
            min_good_ratio: 0.99
          medium:
            min_good_ratio: 0.95
          low:
            min_good_ratio: 0.90

      schedule:
        environments: ["prod", "staging"]
        expected_start_utc: "04:30"
        expected_completion_utc: "06:00"
        expected_duration_minutes: 45
        warn_if_duration_exceeds_minutes: 90
        timezone: "UTC"

    # ── Notifications ──────────────────────────────────────────
    # Domain-wide notification channels. All systems inherit these.
    # Systems can add their own channels (lists are concatenated).
    notifications:
      # Slack (webhook)
      - target: "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
        on_events: ["failure", "slo_breach"]
        subject_template: "[{{ event | upper }}] {{ domain }}/{{ system }}"

      # Microsoft Teams (webhook)
      - target: "https://outlook.office.com/webhook/YOUR-TEAMS-WEBHOOK"
        on_events: ["failure", "slo_breach", "schema_drift"]

      # Email via SMTP (password from env var)
      - target: "smtp://user:env:SMTP_PASSWORD@smtp.company.com:587/data-alerts@company.com"
        on_events: ["failure"]

      # Email via SendGrid (API key from env var)
      - target: "sendgrid://apikey:env:SENDGRID_API_KEY@company.com"
        on_events: ["failure", "slo_breach"]

      # Generic webhook (POST JSON payload)
      - target: "https://your-api.com/webhooks/data-quality"
        on_events: ["quarantine", "failure"]

    # ── Cost Governance ─────────────────────────────────────────
    # Domain-level cost policy. Sets the budget envelope and reporting
    # currency for all systems in this domain. System configs handle compute rates.
    cost:
      currency: "USD"
      budget:
        daily_limit: 50.00
        weekly_limit: 250.00
        monthly_limit: 800.00
        per_run_anomaly_multiplier: 3.0
        alert_channels: ["slack", "email"]

    # ── Extraction Defaults ─────────────────────────────────────
    # Domain-wide LLM extraction defaults (overridable per contract).
    extraction_defaults:
      provider: "azure_openai"
      model: "gpt-4o"
      temperature: 0.0
      max_cost_per_run: 50.00
      redact_pii_before_llm: true

    # ── Test Data Generation ───────────────────────────────────
    # Domain-wide synthetic data defaults for generate_stream().
    test_data:
      default_rows_per_batch: 100
      default_interval_minutes: 5
      default_invalid_ratio: 0.05
      output_root: "{data_root}/landing"

    # ── Layer Aliases ──────────────────────────────────────────
    # Override medallion layer names for the entire domain.
    # All contracts use {bronze_layer}, {silver_layer}, {gold_layer}
    # placeholders which resolve to these values.
    bronze_layer: bronze
    silver_layer: silver
    gold_layer: gold
    ```

---

## Property Reference

### `domain`

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `domain` | `string` | **Yes** | The domain name. Used in path resolution (`{domain}`), lineage, cost roll-ups, and DAG grouping. Must be unique across your organisation. |

---

### `ownership`

Defines who owns this domain. Used for governance dashboards, notification routing, and cost allocation.

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `domain_owner` | `string` | No | Human-readable team or person name |
| `team` | `string` | No | Team identifier (e.g. `data_engineering`, `marketing_ops`) |
| `cost_center` | `string` | No | Cost allocation code for chargebacks |
| `jira_project` | `string` | No | Jira project key for automated ticket creation |
| `contacts` | `list` | No | List of contact objects (see below) |

#### Contact Objects

Each contact in the `contacts` list supports:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `name` | `string` | Yes | Display name |
| `role` | `string` | No | `domain_lead`, `oncall`, `engineer`, etc. Used for notification routing |
| `email` | `string` | No | Email address — auto-generates email notifications |
| `slack` | `string` | No | Slack channel (e.g. `#marketing-data`) — auto-generates Slack notifications |

> **Auto-dispatch:** Contacts with `email` or `slack` fields automatically receive pipeline failure alerts — no separate notification config needed.

---

### `slo` (Service Level Objectives)

Domain-wide quality and reliability targets. All systems and contracts inherit these.

#### `slo.freshness`

Per-layer freshness monitoring. The engine checks the age of the most recent row.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `max_delay_minutes` | `int` | — | Maximum acceptable age of the newest row |
| `check_column` | `string` | `"_lakelogic_loaded_at"` | Column to check for freshness |
| `max_source_delay_minutes` | `int` | `null` | Maximum source-time delay (business timestamp) |
| `source_check_columns` | `list` | `[]` | Candidate source timestamp columns (first match wins) |
| `exclude_tables` | `list` | `[]` | Tables to skip freshness checks for |

#### `slo.row_count`

Per-layer row count thresholds and anomaly detection.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `min_rows` | `int` | `null` | Minimum expected rows per run |
| `max_rows` | `int` | `null` | Maximum expected rows per run |
| `warn_only` | `bool` | `false` | `true` = log warning instead of failing |
| `check_field` | `string` | `"counts_good"` | Run log column to check |
| `exclude_tables` | `list` | `[]` | Tables to skip row count checks for |

##### `slo.row_count.anomaly`

Statistical anomaly detection against historical baselines.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `enabled` | `bool` | `false` | Enable anomaly detection |
| `lookback_runs` | `int` | `14` | Number of historical runs to compare against |
| `min_ratio` | `float` | `0.5` | Alert if current count < `min_ratio × baseline` |
| `max_ratio` | `float` | `2.0` | Alert if current count > `max_ratio × baseline` |
| `method` | `string` | `"median"` | `"median"` or `"rolling_average"` |
| `min_runs_before_enforcement` | `int` | `5` | Don't enforce until this many runs exist |

#### `slo.quality`

Data quality thresholds applied after validation rules run.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `min_good_ratio` | `float` | `0.95` | Minimum ratio of rows passing all rules |
| `max_quarantine_ratio` | `float` | `0.05` | Maximum ratio of quarantined rows |
| `by_severity` | `dict` | `{}` | Per-severity overrides (see example) |

Each severity level (`critical`, `high`, `medium`, `low`) can define its own `min_good_ratio`.

#### `slo.schedule`

Pipeline scheduling expectations for SLA monitoring.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `environments` | `list` | `[]` | Only enforce in these environments (empty = all) |
| `expected_start_utc` | `string` | `null` | Expected pipeline start time |
| `expected_completion_utc` | `string` | `"06:00"` | Expected pipeline completion time |
| `expected_duration_minutes` | `int` | `null` | Expected total pipeline duration |
| `warn_if_duration_exceeds_minutes` | `int` | `null` | Warn if pipeline takes longer than this |
| `timezone` | `string` | `"UTC"` | Timezone for schedule calculations |
| `pipeline_cron` | `string` | `null` | Cron expression for expected run frequency |

---

### `notifications`

Domain-wide notification channels. Systems inherit these and can add their own (lists are concatenated, not replaced).

Each notification entry supports:

| Field | Type | Required | Description |
| --- | --- | --- | --- |
| `target` | `string` | **Yes** | URL for the notification channel |
| `on_events` | `list` | No | Events that trigger this notification (see below) |
| `subject_template` | `string` | No | Jinja2 template for notification subject line |

#### Supported Targets

| Type | URL Pattern |
| --- | --- |
| Slack webhook | `https://hooks.slack.com/services/...` |
| Microsoft Teams | `https://outlook.office.com/webhook/...` |
| Email (SMTP) | `smtp://user:env:PASS@host:port/recipient` |
| Email (SendGrid) | `sendgrid://apikey:env:KEY@sender.com` |
| Generic webhook | `https://your-api.com/webhooks/...` |

#### Supported Events

`failure`, `success`, `quarantine`, `slo_breach`, `schema_drift`, `partial`

> **Tip:** Use `env:VAR_NAME` in URLs to resolve secrets from environment variables at runtime (e.g., `smtp://user:env:SMTP_PASSWORD@...`).

---

### `cost`

Domain-level cost governance. Sets the budget envelope and reporting currency.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `currency` | `string` | `"USD"` | Reporting currency for all systems in this domain |
| `budget.daily_limit` | `float` | `null` | Maximum allowed daily spend |
| `budget.weekly_limit` | `float` | `null` | Maximum allowed weekly spend |
| `budget.monthly_limit` | `float` | `null` | Maximum allowed monthly spend |
| `budget.per_run_anomaly_multiplier` | `float` | `3.0` | Alert if a single run costs > `multiplier × median` |
| `budget.alert_channels` | `list` | `[]` | Which channels to alert on budget breach (`"slack"`, `"email"`) |

> **Note:** Compute rates (`dbu_per_hour`, `storage_per_gb_month`) are defined at the system level in `_system.yaml`, not here. The domain only sets the budget envelope and currency.

---

### `extraction_defaults`

Domain-wide defaults for LLM-powered data extraction. Overridable per system or contract.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `provider` | `string` | — | `"azure_openai"`, `"openai"`, `"anthropic"`, `"ollama"` |
| `model` | `string` | — | Model name (e.g. `"gpt-4o"`, `"claude-3-sonnet"`) |
| `temperature` | `float` | `0.0` | LLM temperature (0 = deterministic) |
| `max_cost_per_run` | `float` | `null` | Cost cap per extraction run |
| `redact_pii_before_llm` | `bool` | `true` | Mask PII fields before sending to LLM |

---

### `test_data`

Domain-wide defaults for synthetic test data generation via `generate_stream()`.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `default_rows_per_batch` | `int` | `100` | Number of rows per generated batch |
| `default_interval_minutes` | `int` | `5` | Interval between batches |
| `default_invalid_ratio` | `float` | `0.05` | Ratio of intentionally invalid rows |
| `output_root` | `string` | `null` | Output directory for generated data |

---

### Layer Aliases

Override medallion layer names for the entire domain. All contracts use `{bronze_layer}`, `{silver_layer}`, `{gold_layer}` placeholders which resolve to these values.

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `bronze_layer` | `string` | `"bronze"` | Name for the raw/ingestion layer |
| `silver_layer` | `string` | `"silver"` | Name for the cleansed/conformed layer |
| `gold_layer` | `string` | `"gold"` | Name for the business/aggregation layer |

---

## Inheritance Rules

> **Why this matters:** You configure governance **once** at the domain level. Every system and contract underneath automatically inherits those settings — saving you from copying the same SLOs, contacts, and notification channels into every file.

| Key Type | Merge Strategy | Example |
| --- | --- | --- |
| **Dict** (slo, ownership, materialization) | Deep merge — domain provides defaults, system overrides specific fields | Domain sets `slo.freshness.bronze.max_delay: 60`, system can override to `30` |
| **List** (notifications) | Concatenation — domain channels + system channels | Domain has Slack, system adds email = both fire |
| **Scalar** (domain, bronze_layer) | Domain wins if mismatch, warning logged | System says `gold_layer: curated` but domain says `gold` → domain enforced |

### Mismatch Detection

If a system's `_system.yaml` defines a scalar value that differs from `_domain.yaml`, the engine:

1. **Logs a warning**: `⚠ Config mismatch: _system.yaml has domain='sales' but _domain.yaml has domain='marketing'`
2. **Enforces the domain value** for consistency

> **Business value:** This prevents configuration drift — a common problem where teams accidentally misconfigure one system and break cross-domain assumptions.

---

## Notification Contact Resolution

When a pipeline event occurs (failure, slo_breach, quarantine), notifications are dispatched from three sources — ensuring the right people always get alerted:

1. **Contract-level** — explicit `quarantine.notifications` in the contract
2. **Registry-level** — `notifications` from `_system.yaml` / `_domain.yaml`
3. **Ownership contacts** — auto-resolved from `ownership.contacts`:
    - Each contact with `email` → generates an email notification
    - Each contact with `slack` → generates a Slack notification

All three sources are **deduplicated by target** — the same Slack channel won't receive duplicate alerts.

> **In practice:** A domain lead defines their contacts once in `_domain.yaml`. From that point on, every pipeline failure across every system in the domain will reach them automatically — no per-contract configuration needed.

