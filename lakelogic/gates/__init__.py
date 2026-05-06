"""
Contract Gates — Runtime enforcement of data quality and governance standards.

Gates are pluggable validators that run in CI/CD pipelines to enforce
contract standards before deployment. Each gate produces a pass/fail result
with detailed diagnostics.
"""

from lakelogic.gates.base import ContractGate, GateResult

__all__ = ["ContractGate", "GateResult"]
