"""LakeLogic Registry — the mesh layer above individual OLC contracts.

`_domain.yaml` and `_system.yaml` express *organisation, defaults and inheritance*
across many data-product contracts. They are LakeLogic-specific (not part of the
portable Open Lakehouse Contract standard) and are validated here by strict manifests:

- :class:`DomainManifestV1` — a raw ``_domain.yaml`` (domain-wide governance defaults).
- :class:`SystemManifestV1` — a raw ``_system.yaml`` (a source/system + its contract index).

These validate the files **as authored** (pre-merge). How the two files, the environment
and an individual contract *combine* — precedence, order and provenance — is specified in
``docs/contracts/inheritance.md`` and executed by
:class:`lakelogic.core.registry.DomainRegistry` (the resolved runtime object).
"""

from __future__ import annotations

from lakelogic.registry.errors import RegistryValidationError
from lakelogic.registry.models import (
    IDENTITY_SCALARS,
    INHERITABLE_KEYS,
    DomainManifestV1,
    SystemManifestV1,
)
from lakelogic.registry.provenance import Origin, Provenance, Reason, ResolvedSystem
from lakelogic.registry.resolver import resolve_system

__all__ = [
    "DomainManifestV1",
    "SystemManifestV1",
    "RegistryValidationError",
    "INHERITABLE_KEYS",
    "IDENTITY_SCALARS",
    "resolve_system",
    "ResolvedSystem",
    "Provenance",
    "Origin",
    "Reason",
]
