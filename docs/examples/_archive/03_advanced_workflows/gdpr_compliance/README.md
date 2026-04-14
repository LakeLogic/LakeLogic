# GDPR Compliance — Right to Erasure & PII Masking

Implement GDPR Article 17 (Right to Erasure) and PII anonymisation
driven entirely by contract-level `pii: true` annotations.

## Components

- **customer_contract.yaml**: Contract with PII-annotated fields.
- **gdpr_erasure.py**: Right-to-be-Forgotten — erase specific subjects.
- **pii_masking.py**: Create anonymised datasets for dev/test.
- **data/**: Sample customer data.

## When to Use This Pattern

- **Data Subject Requests**: A customer requests deletion of their personal data.
- **Anonymised Dev Environments**: Create production-like datasets without real PII.
- **Compliance Audits**: Generate GDPR-compliant erasure reports.
- **Data Minimisation**: Hash PII to preserve referential integrity without exposing raw values.

## Three Erasure Strategies

| Strategy | Effect | Use Case |
|---|---|---|
| `nullify` | Sets PII fields to NULL | Default erasure — simple and complete |
| `hash` | SHA-256 one-way hash | Preserves JOINs across tables |
| `redact` | Replaces with `***REDACTED***` | Visible marker for auditing |

## Prerequisites

- Complete **02_core_patterns/** to understand field annotations.
- Understand the `pii: true` flag on `FieldDefinition`.

## Next Steps

- **08_compliance_governance/** for broader governance patterns.
- **07_production/** for scheduling regular PII scans.
