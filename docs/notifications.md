# Notifications & Alerting

LakeLogic keeps you informed. When data fails a quality rule or is sent to **Quarantine**, you can automatically notify the right people via multiple channels.
Deliver the right alert to the right team, automatically.

## 1. Multi-Channel Support

LakeLogic includes built-in adapters for the most common communication tools:

-   **Slack**: Send messages to specific channels via Webhooks.
-   **Microsoft Teams**: Direct alerting to Team channels.
-   **SMTP (Email)**: Standard email notifications.
-   **SendGrid**: Reliable cloud-based email delivery.
-   **Generic Webhooks**: Trigger downstream systems or APIs.

> Note: Adapters are now functional, but require configuration (see examples below).


## 2. Install Notification Extras

If you want all optional secret providers, install:

```bash
uv pip install "lakelogic[notifications]"
# or
pip install "lakelogic[notifications]"
```

`lakelogic[all]` also includes these dependencies.

## 3. Configuration Example

You define your notification strategy directly in the YAML contract. You can have different people notified for different events.

```yaml
quarantine:
  target: s3://my-bucket/quarantine/
  notifications:
    # Notify Data Engineering in Slack for every quarantine event
    - type: slack
      target: "https://hooks.slack.com/services/..."
      on_events: ["quarantine"]

    # Notify via Microsoft Teams channel for critical failures
    - type: teams
      target: "https://company.webhook.office.com/webhookb2/..."
      on_events: ["failure", "slo_breach"]

    # Notify the Data Owner via SendGrid for critical failures
    - type: sendgrid
      target: "data-owner@company.com"
      api_key: "env:SENDGRID_API_KEY"
      from_email: "env:EMAIL_FROM_ADDRESS"
      from_name: "env:EMAIL_FROM_NAME"
      on_events: ["failure"]

    # SMTP example
    - type: smtp
      target: "alerts@company.com"
      smtp_host: "smtp.company.com"
      smtp_port: 587
      smtp_username: "env:SMTP_USER"
      smtp_password: "${ENV:SMTP_PASS}"
      from_email: "lakelogic@company.com"
      use_tls: true
      on_events: ["quarantine"]

    # Generic webhook
    - type: webhook
      target: "https://example.com/webhook"
      on_events: ["dataset_rule_failed"]
```

## 4. Template Your Alerts (All Channels)

LakeLogic notifications support full Jinja2 templating, meaning you have total control over both the message body and the subject/title.

You can customize notifications in two ways: inline directly in the YAML, or by pointing to an external file.

Supported keys:
- `subject_template` / `subject_template_file`
- `message_template` / `message_template_file`
- `body_template` / `body_template_file` (aliases for message)
- `template_context` (static dictionary merged into runtime context)

### Available Template Context
At runtime, the engine automatically injects a rich context into your templates, including:
- **`contract`**: Details like `.title`, `.dataset`, `.owner`, `.domain`, `.layer`, `.system`
- **`run_id`** / **`pipeline_run_id`**: For tracking and logs
- **`engine`**: The execution engine (e.g., `duckdb`, `spark`)
- **`timestamp_utc`**: Precise time of failure
- **`event`** & **`message`**: The default engine-generated text

### 1. Inline Customization

```yaml
quarantine:
  enabled: true
  notifications:
    - type: slack
      target: "env:SLACK_WEBHOOK"
      on_events: ["quarantine", "failure"]
      subject_template: "🔥 [{{ engine | upper }}] Quality Check Failed in {{ contract.domain }}"
      message_template: |
        *Dataset:* {{ contract.title }} (`{{ contract.layer }}`)
        *Owner:* {{ contract.owner }}
        *Event:* {{ event }}
        
        *Details:* {{ message }}
        
        See Run {{ run_id }} for full logs.
      template_context:
        severity_level: "P1"
```

File template example:

```yaml
quarantine:
  notifications:
    - type: smtp
      target: "alerts@company.com"
      smtp_host: "smtp.company.com"
      from_email: "lakelogic@company.com"
      on_events: ["failure"]
      subject_template_file: "templates/alerts/failure_subject.j2"
      message_template_file: "templates/alerts/failure_body.j2"
```

Template file paths are resolved relative to the contract file directory.

For lakehouse tables (Spark/Unity Catalog), you can use a table target:

```yaml
quarantine:
  target: "table:main.silver.quarantine_customers"
```

Format defaults and overrides:

- File targets default to **Parquet**. Override with a file suffix or `metadata.quarantine_format` (csv or parquet for non-Spark engines).
- Spark file targets can use `csv`, `parquet`, `delta`, `iceberg`, or `json`.
- Spark table targets default to **Iceberg**. Override with `metadata.quarantine_table_format` (for example, `delta`).

```yaml
metadata:
  quarantine_format: parquet
  quarantine_table_format: iceberg
```

## 5. Local Testing (Console)

A special `"console"` notification adapter is included to render notification templating to standard output (`stdout`) for local development and notebook testing. This lets you preview exactly what your alerts will look like without configuring or hitting real external endpoints.

```yaml
quarantine:
  enabled: true
  notifications:
    - type: console
      on_events: ["failure", "dataset_quality_check"]
      subject_template: "🔥 [LOCAL TEST] Alert in {{ contract.domain }}"
      message_template: |
        *Dataset:* {{ contract.title }}
        *Event:* {{ event }}
```

## 6. Azure Key Vault (Optional)

You can resolve secrets from Azure Key Vault by using `keyvault:` or `${AZURE_KEY_VAULT:...}`:

```yaml
quarantine:
  notifications:
    - type: sendgrid
      target: "data-owner@company.com"
      api_key: "keyvault:sendgrid-api-key"
      key_vault_url: "https://my-vault.vault.azure.net/"
      from_email: "lakelogic@company.com"
      on_events: ["failure"]
```

Install dependencies (or use `lakelogic[notifications]`):

```bash
pip install azure-identity azure-keyvault-secrets
```

## 7. AWS Secrets Manager (Optional)

```yaml
quarantine:
  notifications:
    - type: smtp
      target: "alerts@company.com"
      smtp_host: "smtp.company.com"
      smtp_password: "aws:lakelogic/smtp-password"
      from_email: "lakelogic@company.com"
      on_events: ["quarantine"]
```

Install dependencies (or use `lakelogic[notifications]`):

```bash
pip install boto3
```

## 8. GCP Secret Manager (Optional)

```yaml
quarantine:
  notifications:
    - type: sendgrid
      target: "data-owner@company.com"
      api_key: "gcp:sendgrid-api-key"
      gcp_project: "my-project"
      from_email: "lakelogic@company.com"
      on_events: ["failure"]
```

Install dependencies (or use `lakelogic[notifications]`):

```bash
pip install google-cloud-secret-manager
```

## 9. HashiCorp Vault (Optional)

```yaml
quarantine:
  notifications:
    - type: webhook
      target: "vault:secret/data/lakelogic#webhook_url"
      vault_url: "https://vault.company.com"
      vault_token: "env:VAULT_TOKEN"
      vault_kv_version: 2
      on_events: ["dataset_rule_failed"]
```

Install dependencies (or use `lakelogic[notifications]`):

```bash
pip install hvac
```

## 10. Encrypted Local Secrets File

You can store secrets locally in an **encrypted** JSON file and reference them with `local:`:

```yaml
quarantine:
  notifications:
    - type: smtp
      target: "alerts@company.com"
      smtp_host: "smtp.company.com"
      smtp_password: "local:smtp_password"
      secrets_file: "./secrets.enc"
      secrets_key: "env:LAKELOGIC_SECRETS_KEY"
      from_email: "lakelogic@company.com"
      on_events: ["quarantine"]
```

Create the encrypted file:

```bash
python - <<'PY'
from cryptography.fernet import Fernet
import json

key = Fernet.generate_key()
print("Set LAKELOGIC_SECRETS_KEY=", key.decode())

secrets = {"smtp_password": "super-secret"}
token = Fernet(key).encrypt(json.dumps(secrets).encode("utf-8"))
open("secrets.enc", "wb").write(token)
PY
```

Install dependency (or use `lakelogic[notifications]`):

```bash
pip install cryptography
```

## 11. Validation & Fail-Fast

Notifications now validate required fields when created. Missing config or unresolved secrets raise a `ValueError` immediately.

If you want to **continue the run** even when notifications fail, set:

```yaml
quarantine:
  strict_notifications: false
```

## 12. Disabling Notifications

LakeLogic provides an elegant cascading override structure to disable alerts. You can mute notifications across entire domains, mute specific systems, or mute noisy contracts individually using the `notifications_enabled: false` flag.

### Domain or System Level (Globally)

To completely disable all notifications (including slack/teams channels and auto-resolved domain ownership emails) for an entire system or domain, set `notifications_enabled: false` at the top level of your `_system.yaml` or `_domain.yaml`.

```yaml
# _system.yaml / _domain.yaml
domain: marketing
system: google_analytics
notifications_enabled: false  # Disables all notifications for this scope

storage:
  landing_root: ...
```

### Data Contract Level (Locally)

To disable notifications for a specific Data Contract, you must place the flag inside the `quarantine:` block. This is because contract-level alerts are primarily driven by quality failures and quarantine events.

```yaml
# bronze_google_analytics_events_v1.0.yaml
quarantine:
  enabled: true
  notifications_enabled: false   # Mutes alerts specifically for this contract
```

When this flag is set to `false` at any level in the hierarchy, the pipeline processor safely short-circuits before dispatching any events, ignoring both global channels and ownership contacts.

## 13. Secret Caching

LakeLogic caches resolved secrets during a run to avoid repeated provider calls.

## 14. How it Works

When LakeLogic finishes a run, it calculates the **Quarantine Ratio**. 

1.  If **Quarantined Records > 0**, it triggers a `quarantine` event.
2.  If a **dataset rule** fails, it triggers `failure` (you can also subscribe to `dataset_rule_failed` as an alias).
3.  If **schema drift** is detected and `allow_schema_drift: false`, it triggers `schema_drift`.
4.  It looks at your `notifications` list.
5.  It dispatches the message (total records processed, total quarantined, and reason) to your configured channels.

## 15. Pro Tip: Customizing Alerts

You can map specific **Quality Categories** to different channels. For example, you might want **PII Failures** to go to a Security-specific Slack channel, while **Completeness Failures** go to the general Data Engineering channel. 


## Supported Events

| Event | Trigger |
|---|---|
| `quarantine` | Rows quarantined during quality checks |
| `failure` | Pipeline run failed |
| `schema_drift` | Unexpected columns detected |
| `dataset_rule_failed` | Dataset-level rule breached |
| `slo_breach` | Service level objective not met |
| `slo_recovery` | SLO recovered after previous breach |

## Ownership-Based Auto-Routing

When `ownership.contacts` is defined in `_domain.yaml`, notifications are **automatically routed** to domain contacts:

```yaml
# _domain.yaml
ownership:
  contacts:
    - name: Marketing Analytics Lead
      role: domain_lead
      email: marketing-analytics@acme.com    # → auto email notification
      slack: "#marketing-data"               # → auto Slack notification
      teams: "https://outlook.office.com/webhook/..." # → auto Teams notification
      webhook: "json://api.company.com/alerts"        # → auto Webhook notification
    - name: Data Engineering On-Call
      role: oncall
      email: data-oncall@acme.com
      slack: "#data-oncall"
```

Each contact's `email` generates an email adapter; each `slack` generates a Slack adapter; each `teams` generates a Teams adapter; and each `webhook` generates a Generic Webhook adapter. These are **deduplicated** with explicit notification targets — no duplicates.

> **Note**: For bare email addresses (like `data-oncall@acme.com`) inside the ownership block to automatically dispatch alerts, the engine requires either a complete `mailto://` schema OR the `LAKELOGIC_SMTP_URI` global environment variable to securely provide the SMTP provider configuration at runtime.

## Template Variables

Templates use Jinja2 syntax. Available context:

| Variable | Description |
|---|---|
| `{{ event }}` | Event name (failure, quarantine, etc.) |
| `{{ message }}` | Notification body |
| `{{ run_id }}` | Pipeline run ID |
| `{{ timestamp_utc }}` | ISO timestamp |
| `{{ contract.title }}` | Contract title |
| `{{ contract.dataset }}` | Dataset name |
| `{{ contract.domain }}` | Domain name |
| `{{ contract.system }}` | System name |
| `{{ ownership.domain_owner }}` | Domain owner name |
| `{{ ownership.contacts }}` | Contact list |
| `{{ domain }}` | Domain (shorthand) |
| `{{ system }}` | System (shorthand) |

## 5. Local Testing (Dry Run)

LakeLogic includes a `type: "console"` notification adapter that renders payloads directly to stdout. This is perfect for local development and notebook testing, allowing you to preview how templates render without configuring external credentials.

You can use it in your `_system.yaml` or `_domain.yaml` like any other adapter:

```yaml
notifications:
  - type: console
    events:
      - pipeline_started
      - pipeline_completed
      - pipeline_failed
      - dataset_quality_check
```

When an event triggers, LakeLogic will output the notification to your terminal exactly as it would appear in the external system:

```text
============================================================
[LAKELOGIC NOTIFICATION]
Subject: [PROD] [marketing] [google_analytics] Pipeline Failed
------------------------------------------------------------
Pipeline Failure Alert
----------------------
Contract: google_analytics_events_v1.0
Run ID: 88f28a9b-11ed-4b2e-a34f-901c23a8811a

Error:
Failed to read source data from landing zone abfss://...

Please check the system logs.
============================================================
```

You can also test your domain ownership routing rules locally to see how different alert types map to contacts:

```python
from lakelogic.notifications.base import resolve_ownership_contacts

ownership = {
    "domain_owner": "Marketing Analytics Team",
    "contacts": [
        {"name": "Lead", "role": "domain_lead", "email": "lead@acme.com", "slack": "#data"},
        {"name": "OnCall", "role": "oncall", "email": "oncall@acme.com"}
    ]
}

channels = resolve_ownership_contacts(ownership, "failure")
for ch in channels:
    print(f"{ch['type']:6s} → {ch['target']}  (from {ch['_source']})")
```