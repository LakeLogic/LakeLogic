"""
Contract Gates — Runtime enforcement of data quality and governance standards.

Gates are pluggable validators that run in CI/CD pipelines to enforce
contract standards before deployment. Each gate produces a pass/fail result
with detailed diagnostics.
"""

from lakelogic.gates.base import ContractGate, GateResult, GateStatus
from lakelogic.gates.breaking_change import BreakingChangeGate
from lakelogic.gates.lineage_break import LineageBreakGate
from lakelogic.gates.pii_classification import PIIClassificationGate

# Registry: gate name (as passed via --gates flag) → gate class
GATE_REGISTRY: dict[str, type[ContractGate]] = {
    "breaking_change": BreakingChangeGate,
    "pii_classification": PIIClassificationGate,
    "lineage_break": LineageBreakGate,
}

__all__ = [
    "ContractGate",
    "GateResult",
    "GateStatus",
    "BreakingChangeGate",
    "PIIClassificationGate",
    "LineageBreakGate",
    "GATE_REGISTRY",
]
