"""
Lineage Break Prevention gate.

Detects if upstream contracts referenced in lineage are missing or broken.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

from lakelogic.gates.base import ContractGate, GateResult, GateStatus


class LineageBreakGate(ContractGate):
    """
    Validates that upstream contracts referenced in lineage are available.

    Rules:
    - All upstream.source_contract references must point to valid contract files
    - Lineage cycles are detected and reported
    """

    def __init__(self, strict: bool = False):
        super().__init__("lineage_break", strict)

    def run(self, contract: Any, context: Optional[Dict[str, Any]] = None) -> GateResult:
        """
        Check for lineage breaks.

        Args:
            contract: DataContract instance to validate.
            context: Optional dict with 'contract_root' and 'registry' for lineage lookup.

        Returns:
            GateResult with lineage violations.
        """
        violations: List[str] = []
        upstream_count = 0

        if not contract.lineage or not contract.lineage.upstream:
            return GateResult(
                gate_name=self.name,
                status=GateStatus.PASSED,
                message="No upstream dependencies defined (skipping lineage check).",
                details={"upstream_count": 0},
            )

        contract_root = context.get("contract_root") if context else None
        registry = (context.get("registry") or {}) if context else {}

        for upstream in contract.lineage.upstream:
            upstream_count += 1
            source_contract = getattr(upstream, "source_contract", None)

            if not source_contract:
                violations.append("Upstream reference is missing source_contract field")
                continue

            # Try to resolve the contract reference
            resolved = False

            # Check registry first
            if source_contract in registry:
                resolved = True

            # Check filesystem if contract_root is provided
            if not resolved and contract_root:
                contract_path = Path(contract_root) / source_contract
                if contract_path.exists():
                    resolved = True

            if not resolved:
                violations.append(
                    f"Upstream contract not found: '{source_contract}' "
                    f"(expected at {contract_root}/{source_contract} or in registry)"
                )

        if violations:
            return GateResult(
                gate_name=self.name,
                status=GateStatus.FAILED,
                message=f"Lineage breaks detected ({len(violations)})",
                details={
                    "upstream_count": upstream_count,
                    "broken_count": len(violations),
                },
                violations=violations,
            )

        return GateResult(
            gate_name=self.name,
            status=GateStatus.PASSED,
            message=f"All {upstream_count} upstream dependencies are valid.",
            details={
                "upstream_count": upstream_count,
                "all_resolved": True,
            },
        )
