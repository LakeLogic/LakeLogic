"""Project scaffolding — contracts in, a runnable medallion/dbt project out."""

from lakelogic.scaffold.dbt_project import scaffold_dbt_project
from lakelogic.scaffold.project import Provenance, ScaffoldResult, scaffold_project

__all__ = ["Provenance", "ScaffoldResult", "scaffold_dbt_project", "scaffold_project"]
