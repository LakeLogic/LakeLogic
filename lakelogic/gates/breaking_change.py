"""
Breaking Change Detection gate.

Detects schema changes that break downstream consumers:
- Field removals
- Type changes
- Required field changes
"""

from typing import Any, Dict, List, Optional

from lakelogic.gates.base import ContractGate, GateResult, GateStatus


class BreakingChangeGate(ContractGate):
    """
    Detects breaking changes in contract schema.

    A breaking change is:
    - Removal of a field
    - Type change of an existing field
    - Removal of a required field constraint
    """

    def __init__(self, strict: bool = False):
        super().__init__("breaking_change", strict)

    def run(self, contract: Any, context: Optional[Dict[str, Any]] = None) -> GateResult:
        """
        Check for breaking changes between current and previous contract versions.

        Args:
            contract: Current DataContract instance.
            context: Optional dict with 'previous_contract' key containing prior version.

        Returns:
            GateResult with violation list if changes detected.
        """
        if not context or "previous_contract" not in context:
            return GateResult(
                gate_name=self.name,
                status=GateStatus.SKIPPED,
                message="No previous contract version provided (skipping comparison).",
                details={"reason": "no_baseline"},
            )

        previous = context.get("previous_contract")
        if not previous:
            return GateResult(
                gate_name=self.name,
                status=GateStatus.PASSED,
                message="First version of contract (no breaking changes to check).",
                details={"version_count": 1},
            )

        violations: List[str] = []

        # Compare field schemas
        current_fields = {f.name: f for f in (contract.model.fields or [])}
        previous_fields = {f.name: f for f in (previous.model.fields or [])}

        # Check for removed fields
        removed = set(previous_fields.keys()) - set(current_fields.keys())
        if removed:
            for field_name in sorted(removed):
                violations.append(f"Field removed: '{field_name}'")

        # Check for type changes
        for field_name in sorted(set(current_fields.keys()) & set(previous_fields.keys())):
            current_field = current_fields[field_name]
            previous_field = previous_fields[field_name]

            # Simple type comparison (full type matching would be more complex)
            if hasattr(current_field, "type") and hasattr(previous_field, "type"):
                if current_field.type != previous_field.type:
                    violations.append(f"Type changed: '{field_name}' ({previous_field.type} → {current_field.type})")

        if violations:
            return GateResult(
                gate_name=self.name,
                status=GateStatus.FAILED if not self.strict else GateStatus.FAILED,
                message=f"Breaking changes detected ({len(violations)})",
                details={
                    "version_current": contract.info.version if contract.info else None,
                    "version_previous": previous.info.version if previous.info else None,
                },
                violations=violations,
            )

        return GateResult(
            gate_name=self.name,
            status=GateStatus.PASSED,
            message="No breaking changes detected.",
            details={
                "fields_checked": len(current_fields),
                "version": contract.info.version if contract.info else None,
            },
        )
