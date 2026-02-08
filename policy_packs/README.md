# Policy Packs

Policy packs are reusable governance templates that enforce organizational standards across your data contracts. They provide baseline quality rules, schema policies, and approval gates for each layer of your Medallion architecture.

## What are Policy Packs?

Policy packs allow you to:

- **Standardize governance** across all contracts in your organization
- **Enforce layer-specific rules** (Bronze, Silver, Gold)
- **Reduce boilerplate** by inheriting common settings
- **Ensure compliance** with organizational data quality standards

## Available Policy Packs

### `baseline_bronze.yaml`

**Purpose**: Ingestion layer quality gates

**Key Features**:
- Strict schema evolution (no unexpected fields)
- Unknown fields quarantined (not dropped)
- Quarantine enabled with error reasons
- Minimal quality rules (basic not_null checks)

**Use Case**: Raw data ingestion from external sources where you want to capture everything but flag anomalies.

```yaml
# Apply to a Bronze contract
policy_pack: policy_packs/baseline_bronze.yaml
```

---

### `baseline_silver.yaml`

**Purpose**: Cleansed and validated data layer

**Key Features**:
- All Bronze features plus:
- Dataset-level quality rules (null ratio checks)
- Freshness SLAs (24-hour threshold)
- More stringent validation

**Use Case**: Business-ready data that's been cleansed and validated.

```yaml
# Apply to a Silver contract
policy_pack: policy_packs/baseline_silver.yaml
```

---

### `baseline_gold.yaml`

**Purpose**: Business-critical aggregates and KPIs

**Key Features**:
- Approval required for deployments
- Quarantine ratio threshold (5% max)
- Row count validation (must have at least 1 row)
- Strictest governance controls

**Use Case**: Production dashboards and executive reporting.

```yaml
# Apply to a Gold contract
policy_pack: policy_packs/baseline_gold.yaml
```

---

## How to Use Policy Packs

### 1. Reference in Your Contract

```yaml
version: "1.0.0"
dataset: customer_data

# Inherit baseline policies
policy_pack: policy_packs/baseline_silver.yaml

# Override or extend as needed
quality:
  row_rules:
    - not_null: customer_id
    - regex_match:
        field: email
        pattern: "^[^@]+@[^@]+\\.[^@]+$"
```

### 2. Layer-Specific Inheritance

```yaml
# Bronze contract
policy_pack: policy_packs/baseline_bronze.yaml

# Silver contract (stricter)
policy_pack: policy_packs/baseline_silver.yaml

# Gold contract (strictest)
policy_pack: policy_packs/baseline_gold.yaml
```

### 3. Override Specific Settings

```yaml
policy_pack: policy_packs/baseline_silver.yaml

# Override the freshness threshold
service_levels:
  freshness:
    field: updated_at
    threshold: 12h  # More strict than default 24h
```

---

## Creating Custom Policy Packs

You can create organization-specific policy packs:

```yaml
# policy_packs/finance_pii.yaml
name: finance_pii_protection
defaults:
  schema_policy:
    evolution: strict
    unknown_fields: quarantine
  quarantine:
    enabled: true
    include_error_reason: true
  metadata:
    data_classification: confidential
    approval_required: true

quality:
  row_rules:
    - not_null: ssn
    - regex_match:
        field: ssn
        pattern: "^\\d{3}-\\d{2}-\\d{4}$"
    - not_null: account_number
```

---

## Best Practices

1. **Start with baselines** - Use the provided policy packs as starting points
2. **Layer-appropriate strictness** - Bronze should be lenient, Gold should be strict
3. **Version your policies** - Track policy pack changes in Git
4. **Test policy changes** - Validate against existing contracts before rolling out
5. **Document exceptions** - If you override a policy, document why

---

## Policy Pack Hierarchy

```
Bronze (Lenient)
  ↓
Silver (Validated)
  ↓
Gold (Strict)
```

Each layer inherits and extends the previous layer's policies, creating a progressive quality gate system.

---

## Next Steps

- Review the [Driver documentation](../docs/driver.md) for registry-based policy enforcement
- See [Organization patterns](../docs/organization.md) for structuring contracts with policies
- Explore [Examples](../examples/) for real-world policy pack usage
