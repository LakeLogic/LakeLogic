# Notifications & Alerting 🔔

LakeLogic keeps you informed. When data fails a quality rule or is sent to **Quarantine**, you can automatically notify the right people via multiple channels.
Deliver the right alert to the right team, automatically.

## 1. Multi-Channel Support 🔔

LakeLogic includes built-in adapters for the most common communication tools:

-   **Slack**: Send messages to specific channels via Webhooks.
-   **Microsoft Teams**: Direct alerting to Team channels.
-   **SMTP (Email)**: Standard email notifications.
-   **SendGrid**: Reliable cloud-based email delivery.
-   **Generic Webhooks**: Trigger downstream systems or APIs.

> Note: Adapters are now functional, but require configuration (see examples below).


## 2. Install Notification Extras 📦

If you want all optional secret providers, install:

```bash
uv pip install "lakelogic[notifications]"
# or
pip install "lakelogic[notifications]"
```

`lakelogic[all]` also includes these dependencies.

## 3. Configuration Example 🧩

You define your notification strategy directly in the YAML contract. You can have different people notified for different events.

```yaml
quarantine:
  target: s3://my-bucket/quarantine/
  notifications:
    # Notify Data Engineering in Slack for every quarantine event
    - type: slack
      target: "https://hooks.slack.com/services/..."
      on_events: ["quarantine"]

    # Notify the Data Owner via SendGrid for critical failures
    - type: sendgrid
      target: "data-owner@company.com"
      api_key: "env:SENDGRID_API_KEY"
      from_email: "lakelogic@company.com"
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

### Template Your Alerts (All Channels)

Notification channels support Jinja2 templates for subject and message:

- `subject_template` or `subject_template_file`
- `message_template` or `message_template_file`
- `body_template` / `body_template_file` (aliases for message template)
- `template_context` (static key/value map merged with runtime context)

Runtime template context includes:

- `event`, `message`, `subject`, `notification_type`
- `timestamp_utc`, `engine`, `run_id`, `pipeline_run_id`, `source_path`
- `contract.title`, `contract.version`, `contract.owner`, `contract.dataset`
- `contract.domain`, `contract.system`, `contract.layer`

Inline template example:

```yaml
quarantine:
  notifications:
    - type: slack
      target: "env:SLACK_WEBHOOK"
      on_events: ["quarantine", "failure"]
      subject_template: "[{{ event | upper }}] {{ contract.title }}"
      message_template: |
        Run: {{ run_id }}
        Engine: {{ engine }}
        Message: {{ message }}
      template_context:
        team: "data-platform"
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

## 4. Azure Key Vault (Optional) 🔒

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

## 5. AWS Secrets Manager (Optional) 🔒

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

## 6. GCP Secret Manager (Optional) 🔒

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

## 7. HashiCorp Vault (Optional) 🔒

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

## 8. Encrypted Local Secrets File 🔒

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

## 9. Validation & Fail-Fast ✅

Notifications now validate required fields when created. Missing config or unresolved secrets raise a `ValueError` immediately.

If you want to **continue the run** even when notifications fail, set:

```yaml
quarantine:
  strict_notifications: false
```

## 10. Secret Caching ⚡

LakeLogic caches resolved secrets during a run to avoid repeated provider calls.

## 11. How it Works 🧠

When LakeLogic finishes a run, it calculates the **Quarantine Ratio**. 

1.  If **Quarantined Records > 0**, it triggers a `quarantine` event.
2.  If a **dataset rule** fails, it triggers `failure` (you can also subscribe to `dataset_rule_failed` as an alias).
3.  If **schema drift** is detected and `allow_schema_drift: false`, it triggers `schema_drift`.
4.  It looks at your `notifications` list.
5.  It dispatches the message (total records processed, total quarantined, and reason) to your configured channels.

## 12. Pro Tip: Customizing Alerts 💡

You can map specific **Quality Categories** to different channels. For example, you might want **PII Failures** to go to a Security-specific Slack channel, while **Completeness Failures** go to the general Data Engineering channel. 
