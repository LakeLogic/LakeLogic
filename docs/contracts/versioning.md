# Contract Versioning

LakeLogic uses **Semantic Versioning** (`Major.Minor.Patch`) to track how your data products evolve over time.

---

## Why Versioning Matters

> **Analogy:** Think of a data contract like an API. If your mobile app depends on API v1, you don't want the backend team silently changing the response format. Versioning gives downstream consumers a guarantee: "this contract won't break you."

---

## The Strategy: Major Pinning, Minor Floating

This is the industry gold standard for data contracts:

| Change Type | Version Bump | Impact | Example |
|---|---|---|---|
| **Breaking** | Major (`v2.0`) | Downstream must update | Deleting a column, changing a primary key, altering a core data type |
| **Additive** | Minor (`v1.3`) | Safe — floats automatically | Adding a new optional column, adding new metadata tags |
| **Fix** | Patch (`v1.2.1`) | Transparent | Fixing a typo in a description, adjusting a quality threshold |

---

## How It Works in Practice

Your downstream contracts pin to the **major version only**:

```yaml
# In a fact table contract
model:
  fields:
    - name: "customer_id"
      foreign_key:
        contract: "gold_sales_dim_accounts_v1"    # ← Pinned to v1
        column: "account_id"
```

This means:
- ✅ When `dim_accounts` adds a new column in `v1.3` → your fact table keeps working
- ✅ When `dim_accounts` fixes a description in `v1.2.1` → nothing changes for you
- 🛑 When `dim_accounts` releases `v2.0` (deleting columns) → your fact table stays on `v1` until you explicitly upgrade

---

## File Naming Convention

```
bronze_customers_v1.0.yaml      ← Major version 1
bronze_customers_v2.0.yaml      ← Major version 2 (breaking change)
```

Both can exist simultaneously. The pipeline runs whichever version is listed in `_system.yaml`:

```yaml
contracts:
  - path: bronze/customers_v1.0.yaml     # ← Active version
    entity: customers
    layer: bronze
    active: true

  # - path: bronze/customers_v2.0.yaml   # ← Ready but not yet active
  #   active: false
```

---

## Business Value

| Without Versioning | With Versioning |
|---|---|
| One team changes a schema, 12 downstream dashboards break silently | Breaking changes require a new major version — downstream teams upgrade on their schedule |
| "Who changed the customer table?" | Contract git history shows exactly what changed, when, and why |
| Rollbacks are guesswork | Revert to `v1.0.yaml` instantly |
