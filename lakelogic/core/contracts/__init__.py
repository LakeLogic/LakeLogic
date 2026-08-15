"""Canonical OLC contract layer (alignment refactor).

Boundary:
    normalise_contract(dict)   # legacy / ODCS / shorthand  ->  canonical dict
        -> OLCContractV1        # the STRICT public standard (constraints in Pydantic)

Phase 1 (additive, non-breaking): this package exists ALONGSIDE the legacy
``DataContract``. Nothing here is wired into DataProcessor or the schema
generator yet — those are later phases of the staged migration.
"""
from lakelogic.core.contracts.normalise import (
    apply_odcs,
    apply_schema_policy_migration,
    apply_soft_deletes,
    normalise_contract,
)
from lakelogic.core.contracts.olc_v1 import OLCContractV1, StrictServer
from lakelogic.core.contracts.runtime import load_strict, to_runtime

__all__ = [
    "normalise_contract",
    "apply_odcs",
    "apply_soft_deletes",
    "apply_schema_policy_migration",
    "OLCContractV1",
    "StrictServer",
    "load_strict",
    "to_runtime",
]
