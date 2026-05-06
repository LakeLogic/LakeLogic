"""
Base classes for contract gates.

A gate is a pluggable validator that enforces specific contract standards
in CI/CD pipelines. Gates run statically (without data execution) and produce
pass/fail results with detailed diagnostics.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class GateStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    WARNING = "warning"
    SKIPPED = "skipped"


@dataclass
class GateResult:
    """Result of a single gate evaluation."""

    gate_name: str
    status: GateStatus
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    violations: List[str] = field(default_factory=list)

    def __bool__(self):
        """Gate result is truthy if status is PASSED or WARNING."""
        return self.status in (GateStatus.PASSED, GateStatus.WARNING)


class ContractGate(ABC):
    """
    Abstract base class for contract gates.

    Subclasses implement specific enforcement rules (breaking change detection,
    PII compliance, lineage validation, etc.) and return a GateResult.
    """

    def __init__(self, name: str, strict: bool = False):
        """
        Initialize a gate.

        Args:
            name: Human-readable gate name.
            strict: If True, treat warnings as failures.
        """
        self.name = name
        self.strict = strict

    @abstractmethod
    def run(self, contract: Any, context: Optional[Dict[str, Any]] = None) -> GateResult:
        """
        Execute the gate against a contract.

        Args:
            contract: DataContract instance to validate.
            context: Optional context (e.g., previous contract version for diff).

        Returns:
            GateResult indicating pass/fail and detailed diagnostics.
        """
        pass
