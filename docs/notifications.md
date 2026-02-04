# Notifications & Alerting 🔔

LakeGuard keeps you informed. When data fails a quality rule or is sent to **Quarantine**, you can automatically notify the right people via multiple channels.

## 1. Multi-Channel Support

LakeGuard includes built-in adapters for the most common communication tools:

-   **Slack**: Send messages to specific channels via Webhooks.
-   **Microsoft Teams**: Direct alerting to Team channels.
-   **SMTP (Email)**: Standard email notifications.
-   **SendGrid**: Reliable cloud-based email delivery.
-   **Generic Webhooks**: Trigger downstream systems or APIs.

> Note: The open-source demo ships with **log-only adapters** (they log a message instead of sending). Swap in real adapters as needed.

## 2. Configuration Example

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
      on_events: ["failure"]
```

## 3. How it Works

When LakeGuard finishes a run, it calculates the **Recovery Ratio**. 

1.  If **Quarantined Records > 0**, it triggers a `quarantine` or `quarantine_triggered` event.
2.  If a **dataset rule** fails, it triggers `dataset_rule_failed` (and `failure`).
3.  It looks at your `notifications` list.
4.  It dispatches the message (total records processed, total quarantined, and reason) to your configured channels.

## 💡 Pro Tip: Customizing Alerts

You can map specific **Quality Categories** to different channels. For example, you might want **PII Failures** to go to a Security-specific Slack channel, while **Completeness Failures** go to the general Data Engineering channel. 🛡️📢
