# Quality Rules

Quality rules are your data's **safety net** — and they're **SQL-powered**. Every rule, from simple null checks to complex business logic, is expressed as SQL under the hood. The YAML shorthand (`not_null:`, `range:`, `accepted_values:`) compiles to SQL, and you can always write custom SQL rules directly.

> **Bottom line:** If you can write a SQL `WHERE` clause, you can write a quality rule. No Python, no custom code — just SQL.

Quality validates data at two levels:

- **Row-level rules** — inspect each row individually and quarantine bad ones
- **Dataset-level rules** — check aggregate properties of the entire dataset

> **Think of it like airport security.** Row rules are like the X-ray scanner — each bag (row) goes through individually, and anything suspicious gets pulled aside (quarantined). Dataset rules are like checking the total passenger count against the manifest — if the numbers don't add up, something is wrong.

---

## Execution Timing

This is the **actual sequence** the engine follows for quality checks:

| Step | Stage | What Happens |
| --- | --- | --- |
| 1 | **Pre-transforms complete** | Raw data has been cleaned/renamed |
| 2 | **Pre quality rules** (default) | Validate source columns |
| 3 | **Good/bad split** | Quarantine rows failing error-severity rules |
| 4 | **Post-transforms complete** | Derived columns now exist |
| 5 | **Post quality rules** (`phase: post`) | Validate derived columns |

- **Pre-phase** rules can only reference **source** columns
- **Post-phase** rules can reference source **and** derived columns
- Errors tagged `[pre]` or `[post]` in `_lakelogic_errors` for traceability

---

## Configuration

```yaml
quality:
  enforce_required: true                 # Auto-generate not_null rules for required fields
  fail_pipeline_on_dataset_error: false  # If true, aborts the entire pipeline if a dataset rule fails
```

---

## Row-Level Rules

### Not Null

!!! example "Example: Require customer_id to be present"

    ```yaml
    row_rules:
      # Simple
      - not_null: "email"

      # With config
      - not_null:
          field: "customer_id"
          name: "customer_id_required"
          category: "completeness"
          description: "Customer ID must be present"
          severity: "error"          # error (quarantine) | warning (log) | info

      # Multiple fields
      - not_null:
          fields: ["email", "status", "created_at"]
          category: "completeness"
    ```

### Accepted Values

!!! example "Example: Restrict status to valid options"

    ```yaml
      - accepted_values:
          field: "status"
          values: ["ACTIVE", "INACTIVE", "PENDING", "SUSPENDED"]
          category: "consistency"
    ```

### Regex Pattern

!!! example "Example: Validate email format"

    ```yaml
      - regex_match:
          field: "email"
          pattern: "^[^@]+@[^@]+\\.[^@]+$"
          category: "correctness"
    ```

### Numeric Range

!!! example "Example: Validate age is realistic"

    ```yaml
      - range:
          field: "age"
          min: 18
          max: 120
          inclusive: true
          category: "correctness"
    ```

### Referential Integrity

!!! example "Example: Ensure country codes exist in reference data"

    ```yaml
      - referential_integrity:
          field: "country_code"
          reference: "dim_countries"
          key: "code"
          category: "consistency"
    ```

### Custom SQL Rule

!!! example "Example: Block disposable email addresses"

    ```yaml
      - name: "email_domain_valid"
        sql: "email NOT LIKE '%@temp-mail.%' AND email NOT LIKE '%@disposable.%'"
        category: "validity"
        severity: "error"

      # Post-phase rule (runs AFTER transforms)
      - name: "derived_date_not_null"
        sql: "event_date_parsed IS NOT NULL"
        phase: "post"
        category: "correctness"
    ```

---

## Dataset-Level Rules

These check aggregate properties of the entire good dataset — not individual rows.

### Uniqueness

!!! example "Example: Ensure no duplicate customer IDs"

    ```yaml
    dataset_rules:
      - unique: "customer_id"

      - unique:
          field: "email"
          name: "email_unique"
          category: "uniqueness"
          severity: "error"
    ```

### Null Ratio

!!! example "Example: Phone numbers can be null, but not more than 20%"

    ```yaml
      - null_ratio:
          field: "phone"
          max: 0.20                  # Max 20% null values
          category: "completeness"
    ```

### Row Count

!!! example "Example: Ensure dataset has between 1K and 10M rows"

    ```yaml
      - row_count_between:
          min: 1000
          max: 10000000
          category: "completeness"
    ```

### Custom SQL

!!! example "Example: At least 60% of customers must be active"

    ```yaml
      - name: "active_customer_ratio"
        sql: "SELECT SUM(CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END) / COUNT(*) FROM source"
        must_be_greater_than: 0.60
        category: "validity"
    ```

---

## Severity Levels

| Severity | Behaviour | Business Impact |
| --- | --- | --- |
| `error` | Quarantine the row (default) | Bad data never reaches downstream consumers |
| `warning` | Log the issue, keep the row | Data flows through, but the team is alerted |
| `info` | Log only, no action | Informational tracking for observability |

---

## Quarantine Configuration

When rows fail quality rules, they're sent to quarantine — a separate table where you can inspect and fix problems without blocking the pipeline.

!!! example "Example: Quarantine setup"

    ```yaml
    quarantine:
      enabled: true
      target: "s3://quarantine-bucket/customers"
      include_error_reason: true     # Include _lakelogic_errors column
      format: "parquet"              # parquet | csv | delta | json
      write_mode: "append"           # append | overwrite
      strict_notifications: true     # Fail pipeline if notification fails
    ```

See [Notifications](../../notifications.md) for alert configuration.
