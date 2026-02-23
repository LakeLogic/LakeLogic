"""
lakelogic.adapters
------------------
Input-format adapters that convert external schema formats into
a ``DataContract`` so all downstream LakeLogic tooling works unchanged.

Current adapters
~~~~~~~~~~~~~~~~
- dbt      — reads ``schema.yml`` / ``sources.yml`` (dbt Core)
"""
from lakelogic.adapters.dbt import DbtAdapter, load_contract_from_dbt

__all__ = ["DbtAdapter", "load_contract_from_dbt"]
