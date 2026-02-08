# Notifications & Secrets

Configure alerts and securely manage credentials.

## What You'll Learn

1. **Notification Channels** - Slack, Teams, Email, Webhooks
2. **Secret Resolution** - Environment variables, cloud vaults
3. **Event Triggers** - When to send alerts

## Files

```
notifications_and_secrets/
└── notifications_secrets.ipynb   # Interactive tutorial
```

## Notification Channels

### Slack
```yaml
quarantine:
  notifications:
    - type: slack
      target: "env:SLACK_WEBHOOK"
      on_events: ["quarantine"]
```

### Microsoft Teams
```yaml
    - type: teams
      target: "env:TEAMS_WEBHOOK"
      on_events: ["quarantine", "sla_breach"]
```

### Email (SMTP)
```yaml
    - type: smtp
      target: "alerts@company.com"
      smtp_host: "smtp.company.com"
      smtp_password: "env:SMTP_PASSWORD"
```

### SendGrid
```yaml
    - type: sendgrid
      api_key: "env:SENDGRID_API_KEY"
      to: "alerts@company.com"
```

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
|-------|------|
| `quarantine` | Records failed quality rules |
| `sla_breach` | Freshness/availability SLA violated |
| `schema_drift` | Unexpected columns detected |
| `success` | Pipeline completed successfully |

## Run the Tutorial

Open `notifications_secrets.ipynb` for the full interactive walkthrough.

## Best Practices

1. **Never commit secrets** - Always use environment variables or vaults
2. **Use `strict_notifications: false`** - Pipeline continues if notification fails
3. **Test webhooks** - Verify URLs before production
4. **Rate limiting** - Don't spam channels with every row failure
