# Governance at Scale: Organizing Your Contracts 🏗️

> [!NOTE]
> These are **recommended patterns** for enterprise-grade data estates. Automatic registry resolution and contract discovery are planned features for the LakeLogic ecosystem.

As your data estate grows from 10 to 1,000+ tables, the way you organize your contracts determines whether your team maintains **agility** or drowns in "Contract Sprawl." 

Effective organization isn't just about folder structures; it's about **ownership, discoverability, and governance.**

---

## 1. Domain-First Ownership (Data Mesh)

We recommend organizing your repository by **Business Domain** rather than by technical layer. This aligns with **Data Mesh** principles, ensuring that the teams who know the data best are the ones who own the contracts.

### Global Scalability Pattern

contracts/
├── finance/                    <-- Domain (Ownership Boundary)
│   ├── _registry.yaml           <-- Central control plane for the domain
│   ├── sap_erp/                 <-- Source System
│   │   ├── bronze/              <-- Technical Layer
│   │   │   ├── bronze_erp_customers_v1.yml
│   │   │   └── bronze_erp_customers_v2.yml
│   │   └── silver/
│   │       └── silver_erp_customers_active.yml
│   └── payment_gateway/
├── marketing/
└── shared/                      <-- Global entities (e.g., Dim_Date, Dim_Geo)



**Business ROI:**
- **Clear Accountability**: Incidents are automatically routed to the domain owner.
- **Decoupled Growth**: Marketing can update their contracts without impacting Finance.
- **Silo Elimination**: Shared entities provide a "Single Source of Truth" across the company.

---

## 2. The "Registry" as a Control Plane

Instead of pointing production jobs to brittle file paths like `customers_v12_final_v2.yml`, use a **Domain Registry**. This acts as a logical pointer for your orchestration engine.


**File: `finance/_registry.yaml`**

```yaml
entries:
  - entity: customers
    layer: bronze
    active_version: v2
    contract_path: sap_erp/bronze/bronze_erp_customers_v2.yml
    
  - entity: customers
    layer: silver
    active_version: v1
    contract_path: sap_erp/silver/silver_erp_customers_v1.yml
```



### Why this matters for Governance:
- **Zero-Downtime Promotions**: To upgrade to `v3`, test the new YAML in a dev branch, then simply update the `active_version` in the registry. No code changes required.
- **Auditability**: The registry provides a central ledger of every active contract in your domain.
- **Multi-Version Support**: Run `v1` and `v2` in parallel during migrations by maintaining both entries in the registry.

---

## 3. Metadata Standards: Beyond Technical Types

Every contract should include governance-rich metadata. This transforms your YAML files into a **Searchable Data Catalog**.

| Field | Business Value |
| :--- | :--- |
| **`owner`** | Routing for data quality alerts and incident response. |
| **`status`** | Lifecycle management (`draft` → `active` → `deprecated`). |
| **`classification`** | Automated tagging for **GDPR, HIPAA, and CCPA** compliance. |
| **`sla_tier`** | Prioritizes engineering response during multi-system outages. |

---

## 4. Shared Templates: Eliminating "Logic Debt"

Standardizing 100s of contracts manually is impossible. LakeLogic uses **Standardized Templates** to bulk-apply corporate data standards (e.g., standard timestamps, PII masking rules) across all domains.

### ROI: The "Logic Reuse" Advantage
- **Reduce Maintenance**: Update a global "Silver Layer" template once, and it propagates to every contract.
- **Guaranteed Consistency**: Ensure "United States" is represented as `US` company-wide without rewriting lookup logic in every file.
- **Onboarding Speed**: New teams can bootstrap production-ready contracts in minutes using domain-specific starters.

---

## 5. Cross-Domain Integrity

Reference data (ISO country codes, currency lists) shouldn't be redefined. Organize them in a `shared/` domain and link to them using **Referential Integrity** rules.

1. **Shared Owner**: Publishes `silver_reference_countries`.
2. **Finance/Marketing Owners**: Point their `lookup` rules to the shared table.
3. **Safety**: Because the shared table has its own contract, downstream teams are guaranteed that the lookup data is valid and schema-compliant. 🛡️🌍

---

[See the Contract Template Reference](contract_template.md) | [Back to Home](index.md)
