# Compliance & Governance

LakeLogic bakes governance directly into the data contract layer. Every data product carries its own compliance metadata and reliability promises — ensuring your data is both legally sound and operationally reliable.

---

## 1. Compliance in a Data Mesh (Inheritance)

Compliance frameworks apply at different granularities. Forcing everything to the table level is repetitive; forcing everything to the domain level misses system-specific nuances.

LakeLogic solves this with **inheritance with overrides** using a deep merge strategy:

**Resolution order:** `_domain.yaml` → `_system.yaml` → `contract.yaml` (most specific wins)

If a contract overrides a single field (e.g. `consent_type`), it still inherits every other domain/system-level setting automatically — no copy-pasting required.

### What Belongs Where

| Level | File | Purpose | Examples |
|:---|:---|:---|:---|
| **Domain** | `_domain.yaml` | Compliance floor for the entire domain | GDPR applicability, DPO contact, default retention, legal basis, shared processors |
| **System** | `_system.yaml` | System-specific overrides | Data residency region, system-local retention period, HIPAA applicability |
| **Contract** | `contract.yaml` | Dataset-specific declarations | Consent type, DPIA status, special category data, AI risk tier, purpose of processing |

### Inheritance Matrix

| Framework Aspect | Domain Floor | System Override | Table Specifics |
|:---|:---:|:---:|:---:|
| **GDPR** (Applicability) | ✅ | ✅ | ✅ |
| `legal_basis` & `retention` | ✅ | ✅ | — |
| `shared_with` (Processors) | ✅ | ✅ | — |
| `data_residency` | — | ✅ | — |
| `dpia_required` | — | — | ✅ |
| `special_category_data` | — | — | ✅ |
| `automated_decision_making` | — | — | ✅ |
| **EU AI Act** (`risk_tier`) | — | — | ✅ |
| **HIPAA** (`phi_present`) | — | ✅ | ✅ |
| **SOX** (`icfr_relevant`) | ✅ | — | ✅ |
| **CCPA** / **LGPD** / **PIPEDA** | ✅ | ✅ | — |

### Example: Inheritance Across Levels

Below is a practical example showing how a single GDPR policy cascades from a domain default down to a specific contract.

#### Domain Level (`_domain.yaml`)

Sets the compliance floor — every system and table in this domain inherits these defaults.

```yaml
# _domain.yaml — "Marketing handles EU customers, GDPR applies to ALL datasets"
compliance:
  gdpr:
    applicable: true
    legal_basis: "legitimate_interest"
    dpo_contact: "dpo@company.com"
    jurisdiction: "EU/EEA"
    shared_with:
      - name: "Klaviyo Inc."
        role: "processor"
        country: "US"
        agreement: "DPA"
        mechanism: "SCCs"
  default_retention: "P24M"           # ISO 8601 duration
```

#### System Level (`_system.yaml`)

Overrides or extends defaults for Google Analytics specifically.

```yaml
# _system.yaml — "Google Analytics stores data in EU, shorter retention"
compliance:
  data_residency: "EU"
  retention_period: "P18M"             # ISO 8601 — overrides domain's P24M
```

#### Contract Level (`contract.yaml`)

Only adds dataset-specific overrides. Everything else is inherited.

```yaml
# contract.yaml — "This specific table needs opt-in consent"
compliance:
  gdpr:
    consent_type: "opt_in"            # Override domain's legitimate_interest default
    purpose: "Campaign attribution"
    special_category_data: false
    dpia_required: false
```

**Result after deep merge:** The final contract carries the domain's `dpo_contact`, `shared_with`, and `jurisdiction` — the system's `data_residency` and `retention_period` — and the contract's own `consent_type` and `purpose`. No duplication needed.

### Data Residency Validation (Shift-Left)

LakeLogic validates data residency at build time by comparing the contract's `compliance.data_residency` against the target environment's `region`.

```yaml
# _system.yaml — environments block
environments:
  prod_eu:
    catalog: "prod_global"
    storage_account: "salakelogicprod_eu"
    region: "EU"

  prod_us:
    catalog: "prod_global"
    storage_account: "salakelogicprod_us"
    region: "US"
```

If a contract declares `data_residency: "EU"` but you deploy to `prod_us`, the engine emits a **build-time warning** before any data moves:

```log
⚠ Compliance Violation [events]: Requires 'EU' data residency,
  but target environment 'prod_us' is 'US'.
```

This prevents accidental cross-region data transfers — a key GDPR Art. 44–49 safeguard — without requiring fragile URI string parsing.

---

## 2. Regulatory Compliance

The `compliance:` block in your contracts supports any regulatory framework your organisation is subject to. Below are **examples** of what can be captured. These are not exhaustive — add any fields relevant to your compliance posture.

### General Risk & DPO Review

LakeLogic allows you to define a generic risk classification and track Data Protection Officer (DPO) reviews directly within the compliance block. 

> [!IMPORTANT]
> If a dataset is classified as **High Risk** due to triggers like `AUTOMATED_DECISIONS` or `PROFILING`, a DPO Review and an associated `review_date` are strictly required.

```yaml
compliance:
  risk:
    level: "HIGH"                       # LOW | MEDIUM | HIGH | CRITICAL
    triggers:
      - "AUTOMATED_DECISIONS"
      - "PROFILING"
      - "CROSS_BORDER"
      - "PII"

  dpo_review:
    required: true                      
    review_date: "2027-11-30"           # YYYY-MM-DD
    ticket: "DPO-4059"                  # Reference to Jira/ServiceNow ticket
    reason: "High Risk due to Automated Profiling"
```

### GDPR (EU 2016/679)

```yaml
compliance:
  gdpr:
    applicable: true
    jurisdiction: "EU/EEA"
    legal_basis: "legitimate_interest"

    legal_basis_detail: >
      Processing based on legitimate interest for direct
      marketing under Art. 6(1)(f).

    purpose: "Customer engagement tracking"
    retention_period: "P24M"
    retention_basis: "Marketing consent validity"
    consent_type: "opt_in"              # opt_in | opt_out | implicit | not_required
    withdrawal_mechanism: true

    special_category_data: false      # Art. 9 (health, race, biometric)

    data_subject_rights:
      right_to_access: true           # Art. 15
      right_to_erasure: true          # Art. 17
      right_to_portability: true      # Art. 20
      right_to_restriction: true      # Art. 18
      automated_decision_making: false # Art. 22

    dpia_required: false
    dpia_status: "not_required"         # not_required | planned | in_progress | completed

    dpo_review:
      required: false
      reviewed_by: null
      review_date: null
      next_review: "2027-01-01"       # YYYY-MM-DD

    lawful_basis_expiry: "2027-01-01"  # YYYY-MM-DD — triggers review before this date

    data_subject_categories: ["retail_customers"]
    cross_border_transfer: true
    transfer_mechanism: "SCCs"

    shared_with:
      - name: "Klaviyo Inc."
        role: "processor"
        country: "US"
        agreement: "DPA"
        mechanism: "SCCs"
```

### EU AI Act (Regulation 2024/1689)

If your data feeds AI/ML systems, document the risk classification:

```yaml
  eu_ai_act:
    applicable: true
    risk_tier: "limited"              # prohibited | high | gpai | limited | minimal

    risk_tier_rationale: >
      Marketing automation data classified as LIMITED RISK under Art. 50.

    ai_system_purpose: "Marketing automation and personalisation"

    ai_systems_using_data:
      - name: "send_time_optimisation"
        risk_tier: "limited"
        description: "ML predicting optimal email send time"

    training_data_provenance: true    # Art. 10
    bias_examination: false           # Art. 10(2)(f)
    transparency_disclosure: true     # Art. 50
    human_oversight: true             # Art. 14
    logging_enabled: true             # Art. 12

    # High-Risk System Specifics
    conformity_assessment_required: false # Art. 43
    compliance_deadline: "2026-08-02"
    fundamental_rights_impact: false      # Art. 27
    post_market_monitoring: false         # Art. 72
    incident_reporting: false             # Art. 73
    annex_iii_category: null
```

### Other Frameworks

Each framework includes the **jurisdiction** it applies to, enabling rapid querying across the data mesh.

```yaml
  # — CCPA (California, US) ——————————————————————————
  ccpa:
    applicable: true
    jurisdiction: "US-CA"
    consumer_categories: ["california_residents"]
    data_categories_collected: ["identifiers", "commercial_info", "inferences"]
    sale_of_data: false
    sharing_for_cross_context: true
    opt_out_mechanism: true
    opt_out_url: "https://acme.com/privacy/opt-out"
    financial_incentive_offered: false
    sensitive_personal_info: false
    retention_period: "P24M"
    dsr_response_days: 45
    status: "compliant"

  # — HIPAA (United States) ——————————————————————————
  hipaa:
    applicable: false
    jurisdiction: "US"
    phi_present: false
    phi_categories: []
    covered_entity: false
    business_associate: false
    baa_in_place: false
    minimum_necessary: false
    encryption_at_rest: false
    encryption_in_transit: false
    audit_controls: false
    breach_notification:
      policy_in_place: false
      notification_days: 60

  # — SOX (United States) ————————————————————————————
  sox:
    applicable: false
    jurisdiction: "US"
    financial_data: false
    icfr_relevant: false
    controls:
      access_controls: false
      change_management: false
      audit_trail: false
      segregation_of_duties: false
    section_302: false
    section_404: false
    external_auditor: null
    retention_period: "P7Y"

  # — LGPD (Brazil) ——————————————————————————————————
  lgpd:
    applicable: true
    jurisdiction: "BR"
    legal_basis: "legitimate_interest"
    sensitive_data: false
    data_subject_categories: ["brazilian_residents"]
    purpose: "Marketing analytics"
    dpo_appointed: false
    anpd_registration: false
    cross_border_transfer: true
    transfer_mechanism: "standard_contractual_clauses"
    retention_period: "P24M"
    status: "in_progress"

  # — PIPEDA (Canada) ————————————————————————————————
  pipeda:
    applicable: true
    jurisdiction: "CA"
    province_override: null
    consent_obtained: true
    consent_type: "express"
    purpose_identified: true
    purpose: "Marketing analytics and customer engagement"
    limiting_collection: true
    individual_access: true
    accuracy_maintained: true
    safeguards:
      encryption: true
      access_controls: true
      audit_logging: true
    openness: true
    cross_border_transfer: true
    transfer_countries: ["US"]
    transfer_safeguards: "contractual_clauses"
    opc_registration: false
    retention_period: "P24M"
    status: "in_progress"

  # — BCBS 239 (Global Banking) ——————————————————————
  bcbs_239:
    applicable: false
    jurisdiction: "Global"
    principle_1_governance: false
    principle_2_data_architecture: false
    principle_3_accuracy: false
    principle_4_completeness: false
    principle_5_timeliness: false
    principle_6_adaptability: false
    risk_data_categories: []
    reporting_frequency: null
    stress_reporting_capability: false
```

#### Framework Selection Logic

LakeLogic can auto-suggest applicable frameworks based on contract metadata. For example:

- `ccpa`: triggers if `pii: true` AND `california_residents: true`
- `hipaa`: triggers if `pii_category` IN `[health, medical, phi]`
- `eu_ai_act`: triggers if `ai_systems_using_data` IS NOT EMPTY

---

## 3. Enforcement vs Documentation

The compliance block serves two purposes: some fields are **actively enforced** by the engine at build time, while others are **documentation** embedded in the contract for auditors and downstream tooling.

| Feature | Engine Behaviour |
|:---|:---|
| `data_residency` | ✅ Build-time warning if environment `region` mismatches |
| `retention_period` | Passed through in contract metadata. Use ISO 8601 duration (e.g. `"P24M"`) or plain text (e.g. `"24 months"`) |
| `dpo_review.next_review` | Passed through in contract metadata. Use ISO 8601 date format: `YYYY-MM-DD` |
| `gdpr.applicable` | Passed through in contract metadata |
| `consent_type` | Passed through in contract metadata |

---

## Business Value Summary

| What Governance Gives You | Without It |
|:---|:---|
| Audit-ready reports generated from your contract YAML | Manual spreadsheets maintained by legal |
| Data lineage shows exactly which PII flows where | "We think email goes to Klaviyo?" |
| Retention policies documented per data product | "We forgot to delete the 2019 data" |
| AI risk classification built into the data layer | Separate AI inventory that drifts from reality |
| Build-time region validation prevents illegal transfers | Discovered 6 months later by an auditor |

[:octicons-arrow-right-24: Next: Service Level Objectives (SLOs)](slo.md)
