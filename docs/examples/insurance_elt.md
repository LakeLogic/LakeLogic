# Insurance ELT (Bronze -> Silver -> Gold)

This is a realistic, end-to-end ELT example for an insurance company. It uses three upstream systems, CDC feeds, reference data, quarantine, notifications, and parallel execution.

## Systems and Dependencies

**Source systems (CDC feeds):**
- Policy Admin (policies)
- Claims System (claims)
- Billing (payments)

**Reference data (shared):**
- States
- Coverage types
- Claim status
- Policyholders

**Execution order:**
1. Reference + Bronze (parallel)
2. Silver Policies (must complete first)
3. Silver Claims + Silver Payments (parallel)
4. Gold (depends on Silver)

## Recommended Structure

This example mirrors the structure described in [Contract Organization](../organization.md):

```text
examples/05_production/insurance_elt/
  contracts/
    insurance/
      _registry.yaml
      warehouse/_registry.yaml
      policy_admin/
        bronze/bronze_policy_admin_policies_v1.yaml
        silver/silver_policies_v1.yaml
      claims_system/
        bronze/bronze_claims_claims_v1.yaml
        silver/silver_claims_v1.yaml
      billing/
        bronze/bronze_billing_payments_v1.yaml
        silver/silver_payments_v1.yaml
      warehouse/
        gold/gold_dim_policyholders_v1.yaml
        gold/gold_fact_claims_v1.yaml
    shared/
      reference/
        _registry.yaml
        silver/
          silver_reference_states_v1.yaml
          silver_reference_coverage_v1.yaml
          silver_reference_claim_status_v1.yaml
          silver_reference_policyholders_v1.yaml
  data/
    bronze/
    reference/
```

## Registry-Driven Pipeline (Scalable)

This mirrors a production pattern where a registry controls what entities run, and contracts define
`source`, `load_mode`, and `pattern`.

Example registry entry (bronze + silver share one registry):

```yaml
entries:
  - entity: policies
    enabled: true
    contracts:
      bronze: policy_admin/bronze/bronze_policy_admin_policies_v1.yaml
      silver: policy_admin/silver/silver_policies_v1.yaml
```

You can also point both stages to the **same contract** if you want Bronze/Silver to be controlled by runtime mode.

```bash
lakelogic-driver \
  --registry examples/05_production/insurance_elt/contracts/insurance/_registry.yaml \
  --reference-registry examples/05_production/insurance_elt/contracts/shared/reference/_registry.yaml \
  --gold-registry examples/05_production/insurance_elt/contracts/insurance/warehouse/_registry.yaml \
  --layers reference,bronze,silver,gold \
  --window last_success
```

Spark can use the same driver by switching engines.

When `--window last_success` is used, the driver consults `metadata.run_log_table`. If no log table exists or no entry is found, a full load is performed before incremental loads.
For the insurance example, the run log table uses DuckDB, so ensure `duckdb` is installed if you want `last_success` to work.

### Observability Outputs (Summary + Metrics)

Write a pipeline summary row to a table and emit metrics:

```bash
lakelogic-driver \
  --registry examples/05_production/insurance_elt/contracts/insurance/_registry.yaml \
  --reference-registry examples/05_production/insurance_elt/contracts/shared/reference/_registry.yaml \
  --gold-registry examples/05_production/insurance_elt/contracts/insurance/warehouse/_registry.yaml \
  --layers reference,bronze,silver,gold \
  --window last_success \
  --summary-table lakelogic.pipeline_runs \
  --summary-backend duckdb \
  --summary-database examples/05_production/insurance_elt/output/run_logs/lakelogic_pipeline_runs.duckdb \
  --metrics-path examples/05_production/insurance_elt/output/run_logs/pipeline_metrics.json
```

For Prometheus scraping, expose `/metrics`:

```bash
lakelogic-driver \
  --registry examples/05_production/insurance_elt/contracts/insurance/_registry.yaml \
  --reference-registry examples/05_production/insurance_elt/contracts/shared/reference/_registry.yaml \
  --gold-registry examples/05_production/insurance_elt/contracts/insurance/warehouse/_registry.yaml \
  --layers reference,bronze,silver,gold \
  --metrics-backend prometheus \
  --metrics-host 0.0.0.0 \
  --metrics-port 9100
```

To run only specific entities without editing registries:

```bash
lakelogic-driver \
  --registry examples/05_production/insurance_elt/contracts/insurance/_registry.yaml \
  --reference-registry examples/05_production/insurance_elt/contracts/shared/reference/_registry.yaml \
  --gold-registry examples/05_production/insurance_elt/contracts/insurance/warehouse/_registry.yaml \
  --layers bronze,silver,gold \
  --entities policies
```

To reprocess late-arriving data:

```bash
lakelogic-driver \
  --registry examples/05_production/insurance_elt/contracts/insurance/_registry.yaml \
  --reference-registry examples/05_production/insurance_elt/contracts/shared/reference/_registry.yaml \
  --gold-registry examples/05_production/insurance_elt/contracts/insurance/warehouse/_registry.yaml \
  --layers silver,gold \
  --reprocess-start-date 2026-02-01 \
  --reprocess-end-date 2026-02-05
```

For an explicit window range:

```bash
lakelogic-driver \
  --registry examples/05_production/insurance_elt/contracts/insurance/_registry.yaml \
  --reference-registry examples/05_production/insurance_elt/contracts/shared/reference/_registry.yaml \
  --gold-registry examples/05_production/insurance_elt/contracts/insurance/warehouse/_registry.yaml \
  --layers bronze,silver,gold \
  --window range \
  --window-start-date 2026-02-01 \
  --window-end-date 2026-02-05
```

### Incremental Windows with Dated Files

The bronze contracts use filename patterns like `claims_cdc*.csv`. If filenames include a date
(for example `claims_cdc_2026-02-05.csv`), the driver will pick files within the requested window.

Example:

```
examples/05_production/insurance_elt/data/bronze/
  claims_cdc_2026-02-05.csv
  claims_cdc_2026-02-06.csv
```

## Features Demonstrated

- **CDC/Increments**: `op` + `updated_at` with deduplication and delete filtering
- **Parallel Execution**: bronze + reference in parallel, then silver, then gold
- **Quarantine**: bad rows routed to quarantine folders
- **Notifications**: webhook-based alerts (env-driven)
- **Lineage**: `_lakelogic_*` columns
- **Reference data joins**: coverage, status, state
- **Gold facts/dimensions**: claims fact and policyholder dimension
- **Registry-driven execution**: enable/disable entities per system registry
- **Source metadata**: `source`, `load_mode`, `pattern` embedded in contracts

### Example Source Metadata (Contract)

```yaml
source:
  type: landing   # landing | stream | table
  path: ../../../../data/bronze
  load_mode: cdc  # full | incremental | cdc
  pattern: claims_cdc*.csv
  watermark_field: updated_at
  cdc_op_field: op
  cdc_delete_values: ["D"]
```

## Output Locations

```text
examples/05_production/insurance_elt/output/
  bronze/
  silver/
  gold/
  quarantine/
  run_logs/
```

## Notes

- Set `LAKELOGIC_ALERT_WEBHOOK` to receive webhook alerts.
- This example writes Parquet outputs; ensure `pyarrow` is available (`pip install "lakelogic[pandas]"` or `pip install pyarrow`).
- For lakehouse tables, switch to `table:` targets and set `quarantine_table_backend: spark`.
