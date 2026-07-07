# Service Level Objectives (SLOs)

SLOs are **promises about your data's reliability**. They answer the question every stakeholder asks: *"Can I trust this data?"*

> **Think of SLOs like a delivery guarantee.** When you order next-day delivery, the retailer promises your package arrives within 24 hours. If it doesn't, they notify you and fix it. SLOs do the same for data — they promise freshness, completeness, and quality, and alert your team when those promises are broken.

SLOs can be defined at three levels, with contract-level taking highest precedence:

```yaml
_domain.yaml (defaults)  →  _system.yaml (overrides)  →  contract.yaml (overrides)
```

Contract values deep-merge with system/domain SLOs — field by field.

---

## Freshness

**Business value:** Ensures your dashboards and reports are never showing stale data. If the marketing team is making campaign decisions based on yesterday's data when they think it's today's — that's a freshness SLO breach.

> **Analogy:** Like checking the "use by" date on food. If the data's timestamp is older than the threshold, it's stale and shouldn't be served to consumers.

!!! example "Example: Customer data must be refreshed daily"

    ```yaml
    service_levels:
      freshness:
        threshold: "24h"              # "30m", "6h", "1d"
        field: "updated_at"           # MAX of this column vs current time
        description: "Customer data must be updated daily"

        # Source-time freshness (catches stale upstream data)
        source_field: "event_timestamp"
        source_threshold: "2h"
    ```

**Two checks in one:**

| Check | What It Catches |
| --- | --- |
| `threshold` + `field` | "Our pipeline hasn't run in 24 hours" |
| `source_threshold` + `source_field` | "The upstream system stopped sending data 2 hours ago" |

The second check is critical — without it, your pipeline could run on time but process zero new records, and you'd never know.

---

## Availability (Field Completeness)

**Business value:** Guarantees that critical fields actually have values. A customer table with 30% missing email addresses is technically "fresh" but practically useless for email campaigns.

> **Analogy:** Like checking that a form has been filled in completely. A job application without a name is valid structurally, but useless operationally.

!!! example "Example: 99.9% of rows must have a customer_id"

    ```yaml
      availability:
        threshold: 99.9               # % of rows with non-null value
        field: "customer_id"
    ```

---

## Row Count

**Business value:** Catches two common silent failures: (1) an upstream source that suddenly stops sending data, and (2) a misconfigured filter that accidentally drops 90% of rows. Both would pass quality rules but produce a suspiciously small dataset.

> **Analogy:** Like a restaurant checking the morning delivery. If you usually get 500 eggs and today you got 12, something went wrong upstream — even if all 12 eggs are perfect.

!!! example "Example: Row count bounds with anomaly detection"

    ```yaml
      row_count:
        min_rows: 100
        max_rows: 10000000
        check_field: "counts_source"   # counts_source | counts_good | counts_total
        skip_reprocess_days: 3         # Skip checks on large backfills
        warn_only: false               # true = log warning, false = fail pipeline

        # Anomaly detection (compares against recent history)
        anomaly:
          enabled: true
          lookback_runs: 14            # Compare against last 14 runs
          min_ratio: 0.5               # Alert if < 50% of baseline
          max_ratio: 2.0               # Alert if > 200% of baseline
          method: "median"             # median | rolling_average
          min_runs_before_enforcement: 5
    ```

**Anomaly detection** is the smart version of min/max — instead of hard-coding thresholds, it learns your baseline and alerts on deviations. This catches gradual drift (data volume slowly declining) that fixed thresholds miss.

---

## Quality

**Business value:** Sets the minimum "pass rate" for your data. If your order table has a 15% quarantine rate, something is fundamentally wrong with the source — even if each individual quality rule is working correctly.

> **Analogy:** Like a factory's defect rate. Even if every individual inspection catches bad products, a 20% defect rate means the manufacturing process itself needs fixing.

!!! example "Example: At least 99% of rows must pass all quality rules"

    ```yaml
      quality:
        min_good_ratio: 0.99          # 99% of rows must be clean
        max_quarantine_ratio: 0.01    # No more than 1% quarantined
    ```

---

## Schedule

**Business value:** Catches pipelines that start late or run too long — before they miss the hard deadline. If the finance team needs their data by 6am for the morning report, a schedule SLO ensures you get an early warning at 4:30am when the pipeline hasn't started yet, not a panicked call at 6:15am.

> **Analogy:** Like a train timetable with two alerts. The first alert fires when the train hasn't left the station on time. The second fires if the journey is taking longer than expected — giving you time to arrange a taxi before you miss your meeting.

!!! example "Example: Pipeline must start by 04:30 and finish by 06:00 UTC"

    ```yaml
      schedule:
        environments: ["prod", "staging"]     # Only enforced in these environments
        expected_start_utc: "04:30"           # Alert if pipeline hasn't started
        expected_completion_utc: "06:00"      # Hard deadline
        expected_duration_minutes: 45         # Baseline for anomaly comparison
        warn_if_duration_exceeds_minutes: 90  # Soft warning before deadline
        timezone: "UTC"
    ```

**Why `environments`?** Schedule SLOs are only enforced in `prod` and `staging` — local dev runs and notebooks are excluded so developers can test freely without triggering false alerts.

**Three levels of protection:**

| Check | When It Fires | Severity |
| --- | --- | --- |
| `expected_start_utc` | Pipeline hasn't started by this time | Warning |
| `warn_if_duration_exceeds_minutes` | Pipeline running longer than soft limit | Warning |
| `expected_completion_utc` | Pipeline hasn't finished by deadline | Error |

---

## Run Log Capture

Every SLO result is automatically recorded in the `_run_logs` table — giving you a full audit trail of data reliability over time.

| Field | Description |
| --- | --- |
| `slo_freshness_seconds` | Time since newest source record |
| `slo_freshness_pass` | Whether freshness threshold was met |
| `slo_row_count_min` / `max` | Configured thresholds |
| `slo_row_count_anomaly_pass` | Whether anomaly check passed |
| `slo_quality_pass` | Whether quality ratio met threshold |
| `slo_schedule_pass` | Whether pipeline ran on schedule |

> **Business value:** This run log data is what powers your **data reliability dashboard**. Instead of asking "is our data trustworthy?", your team can show stakeholders a chart of SLO compliance over the last 90 days — just like an uptime SLA for a web service.

SLO breaches emit `"slo_breach"` notification events that route through the [Notifications](../notifications.md) system — so the right people get alerted immediately.
