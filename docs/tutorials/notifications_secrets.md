# Tutorial: Notifications & Secrets

Configure alerts and securely manage credentials.

## What You'll Learn

1. **Notification Channels** — Slack, Teams, Email, Discord, and 90+ more
2. **Secret Resolution** — Environment variables, cloud vaults
3. **Message Templates** — Built-in defaults and custom Jinja2 templates
4. **Event Triggers** — When to send alerts

## Files

The example files are located at:

```text
examples/04_compliance_governance/notifications_and_secrets/
└── notifications_secrets.ipynb   # Interactive tutorial
```

## Quick Start

The simplest notification config — just `target` and `on_events`:

```yaml
quarantine:
  notifications:
    - target: "env:TEAMS_WEBHOOK"
      on_events: [quarantine, failure]
```

That's it. LakeLogic auto-detects the channel from the target URL and uses
built-in Jinja2 templates to render a rich markdown notification.

## Notification Channels

### URL-based (Recommended)

LakeLogic uses [Apprise](https://github.com/caronc/apprise) by default,
which detects the channel from the URL scheme. No `type` field needed:

```yaml
quarantine:
  notifications:
    # Microsoft Teams
    - target: "env:TEAMS_WEBHOOK"
      on_events: [quarantine, failure]

    # Slack
    - target: "env:SLACK_WEBHOOK"
      on_events: [quarantine, schema_drift]

    # Email (SMTP)
    - target: "mailto://user:pass@smtp.gmail.com?to=alerts@company.com"
      on_events: [failure]

    # Multiple channels at once
    - targets:
        - "env:TEAMS_WEBHOOK"
        - "env:SLACK_WEBHOOK"
        - "mailto://user:pass@smtp.gmail.com?to=alerts@co.com"
      on_events: [failure, sla_breach]
```

> See the full list of [Apprise URL schemes](https://github.com/caronc/apprise/wiki)
> for Discord, Telegram, PagerDuty, Pushover, and more.

### Legacy Adapters

For backwards compatibility, you can set `type` explicitly:

```yaml
    - type: slack
      target: "env:SLACK_WEBHOOK"

    - type: teams
      target: "env:TEAMS_WEBHOOK"

    - type: smtp
      target: "alerts@company.com"
      smtp_host: "smtp.company.com"
      smtp_password: "env:SMTP_PASSWORD"

    - type: sendgrid
      api_key: "env:SENDGRID_API_KEY"
      to: "alerts@company.com"
```

## Message Templates

### Built-in Defaults

When no custom template is provided, LakeLogic uses event-specific
built-in templates that produce rich markdown notifications with run
details, contract metadata, and context tables.

Supported events with built-in templates:

| Event | Icon | Template |
|:------|:-----|:---------|
| `quarantine` | ⚠️ | `quarantine.md.j2` |
| `failure` | 🔴 | `failure.md.j2` |
| `schema_drift` | 🟡 | `schema_drift.md.j2` |
| `success` | ✅ | `success.md.j2` |
| `sla_breach` | 🕐 | `sla_breach.md.j2` |

### Inline Jinja2 Templates

Override the built-in templates with inline Jinja2:

```yaml
    - target: "env:TEAMS_WEBHOOK"
      subject_template: "[{{ event | upper }}] {{ contract.title }}"
      message_template: |
        Run ID: {{ run_id }}
        Engine: {{ engine }}
        Message: {{ message }}
```

### File-based Templates

Point to your own `.j2` files (paths relative to the contract):

```yaml
    - target: "env:SLACK_WEBHOOK"
      subject_template_file: "templates/alerts/subject.j2"
      message_template_file: "templates/alerts/body.j2"
```

### Available Template Variables

These variables are available in all templates:

| Variable | Example |
|:---------|:--------|
| `{{ event }}` | `quarantine` |
| `{{ message }}` | `5 rows quarantined` |
| `{{ run_id }}` | `abc-123` |
| `{{ pipeline_run_id }}` | `pipe-456` |
| `{{ engine }}` | `spark` |
| `{{ timestamp_utc }}` | `2026-02-27T23:35:07` |
| `{{ source_path }}` | `table:cat.bronze.tbl` |
| `{{ contract.title }}` | `Silver Zoopla Listing` |
| `{{ contract.version }}` | `1.0` |
| `{{ contract.owner }}` | `data-team` |
| `{{ contract.domain }}` | `property` |
| `{{ contract.system }}` | `zoopla` |
| `{{ contract.layer }}` | `silver` |

## Secret Resolution

LakeLogic resolves secrets at runtime. Your YAML is safe to commit!

### Environment Variables

```yaml
target: "env:SLACK_WEBHOOK"
# or
target: "${ENV:SLACK_WEBHOOK}"
```

### Azure Key Vault

```yaml
api_key: "keyvault:sendgrid-api-key"
key_vault_url: "https://my-vault.vault.azure.net/"
```

### AWS Secrets Manager

```yaml
smtp_password: "aws:lakelogic/prod/smtp-password"
```

### GCP Secret Manager

```yaml
target: "gcp:target-webhook-url"
gcp_project: "my-project-id"
```

### Encrypted Local File

```yaml
smtp_password: "local:smtp_password"
secrets_file: "./secrets.enc"
secrets_key: "env:LAKELOGIC_SECRETS_KEY"
```

## Event Types

| Event | When |
|:------|:-----|
| `quarantine` | Records failed quality rules |
| `failure` | Pipeline encountered an error |
| `schema_drift` | Unexpected columns detected |
| `sla_breach` | Freshness/availability SLA violated |
| `success` | Pipeline completed successfully |

## Run the Tutorial

Open `examples/04_compliance_governance/notifications_and_secrets/notifications_secrets.ipynb` for the full interactive walkthrough.

## Best Practices

1. **Never commit secrets** — Always use environment variables or vaults
2. **Use `strict_notifications: false`** — Pipeline continues if notification fails
3. **Test webhooks** — Verify URLs before production
4. **Rate limiting** — Don't spam channels with every row failure
5. **Use built-in templates** — Override only when you need custom formatting

## Next Steps

- [Patterns Overview](../playbooks.md) — Common data engineering recipes
- [Production Examples](https://github.com/lakelogic/LakeLogic/tree/main/examples) — Complete end-to-end pipeline
