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
