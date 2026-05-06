"""
PII Classification gate.

Enforces that PII/PHI fields have masking rules defined and compliance metadata.
"""

from typing import Any, Dict, List, Optional

from lakelogic.gates.base import ContractGate, GateResult, GateStatus


class PIIClassificationGate(ContractGate):
    """
    Enforces PII/PHI compliance standards.

    Rules:
    - Fields marked as pii=True must have a masking rule defined
    - Fields marked as phi=True must have compliance metadata (e.g., HIPAA)
    - PII fields must have a classification (public, internal, restricted, confidential)
    """

    VALID_MASKING_TYPES = {"nullify", "hash", "redact", "partial", "encrypt"}
    VALID_CLASSIFICATIONS = {"public", "internal", "restricted", "confidential"}

    def __init__(self, strict: bool = False):
        super().__init__("pii_classification", strict)

    def run(self, contract: Any, context: Optional[Dict[str, Any]] = None) -> GateResult:
        """
        Check PII field compliance.

        Args:
            contract: DataContract instance to validate.
            context: Optional context (unused for this gate).

        Returns:
            GateResult with PII compliance violations.
        """
        violations: List[str] = []
        pii_fields_count = 0
        compliant_count = 0

        if not contract.model or not contract.model.fields:
            return GateResult(
                gate_name=self.name,
                status=GateStatus.PASSED,
                message="No fields defined in contract (skipping PII check).",
                details={"fields_checked": 0},
            )

        for field in contract.model.fields:
            # Check if field is marked as PII or PHI
            is_pii = getattr(field, "pii", False)
            is_phi = getattr(field, "phi", False)

            if is_pii or is_phi:
                pii_fields_count += 1

                # Check for masking rule
                masking_rule = getattr(field, "masking", None)
                if not masking_rule:
                    violations.append(f"Field '{field.name}' marked as PII/PHI but has no masking rule defined")
                    continue

                # Validate masking type if it's a dict with 'type' key
                if isinstance(masking_rule, dict):
                    mask_type = masking_rule.get("type")
                    if mask_type and mask_type not in self.VALID_MASKING_TYPES:
                        violations.append(
                            f"Field '{field.name}' has invalid masking type: {mask_type}. "
                            f"Must be one of: {', '.join(self.VALID_MASKING_TYPES)}"
                        )
                        continue

                # Check classification metadata
                classification = getattr(field, "classification", None)
                if not classification:
                    violations.append(
                        f"Field '{field.name}' marked as PII/PHI but has no classification "
                        f"(must be one of: {', '.join(self.VALID_CLASSIFICATIONS)})"
                    )
                    continue

                if classification not in self.VALID_CLASSIFICATIONS:
                    violations.append(
                        f"Field '{field.name}' has invalid classification: {classification}. "
                        f"Must be one of: {', '.join(self.VALID_CLASSIFICATIONS)}"
                    )
                    continue

                compliant_count += 1

        if violations:
            return GateResult(
                gate_name=self.name,
                status=GateStatus.FAILED,
                message=f"PII/PHI compliance violations found ({len(violations)})",
                details={
                    "pii_fields": pii_fields_count,
                    "compliant": compliant_count,
                    "violations": len(violations),
                },
                violations=violations,
            )

        if pii_fields_count == 0:
            return GateResult(
                gate_name=self.name,
                status=GateStatus.PASSED,
                message="No PII/PHI fields in contract.",
                details={"pii_fields_checked": 0},
            )

        return GateResult(
            gate_name=self.name,
            status=GateStatus.PASSED,
            message=f"All {pii_fields_count} PII/PHI fields are compliant.",
            details={
                "pii_fields": pii_fields_count,
                "compliant": compliant_count,
            },
        )
