"""Provenance for resolved registry config — *where did this value come from?*

After the domain → system merge, every top-level key on the effective config carries a
:class:`Provenance` record: which layer supplied it and why. This turns "why is
``slo.freshness.bronze.max_delay_minutes`` 30 here?" into a lookup instead of a log grep,
and gives agents a resolved-with-provenance estate to reason over.

Provenance is tracked per **top-level key**, and additionally per dotted leaf path inside
a deep-merged block (``merge._leaf_provenance``) — so ``ownership.business_owner.name`` has
its own record and the UI can say which of two sibling fields was inherited.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional


class Origin(str, Enum):
    """Which layer a resolved value came from."""

    DOMAIN = "domain"  # inherited wholesale from _domain.yaml
    SYSTEM = "system"  # declared on _system.yaml
    CONTRACT = "contract"  # declared on the contract itself — supersedes system and domain
    BOTH = "system+domain"  # merged (deep-merge or list concat)
    ENVIRONMENT = "environment"
    DEFAULT = "default"  # neither file set it; model default


class Reason(str, Enum):
    """Why a value has the origin it does — the merge rule that applied."""

    DECLARED = "declared"  # only this layer set it
    INHERITED = "inherited"  # system omitted it; domain supplied it
    OVERRIDDEN = "overridden"  # both set a scalar; the child value won
    DEEP_MERGED = "deep-merged"  # both dicts; merged, system fields win
    CONCATENATED = "concatenated"  # both lists; domain + system
    DOMAIN_LOCKED = "domain-locked"  # identity scalar / currency; domain wins
    CURRENCY_NORMALISED = "currency-normalised"
    ENV_BINDING = "env-binding"  # a value declared on the selected environment
    ENV_SUBSTITUTED = "env-substituted"  # references an environment variable ({catalog}, …)


@dataclass
class Provenance:
    """The origin of one resolved top-level key."""

    key: str
    origin: Origin
    reason: Reason
    source: Optional[str] = None  # the file that supplied the winning value

    def describe(self) -> str:
        return f"{self.origin.value} ({self.reason.value})"


@dataclass
class ResolvedSystem:
    """A domain → system merge result plus its provenance."""

    domain: str
    system: str
    config: Dict[str, Any]  # effective merged config
    provenance: Dict[str, Provenance] = field(default_factory=dict)
    domain_file: Optional[str] = None
    system_file: Optional[str] = None
    environment: Optional[str] = None

    def origin_of(self, key: str) -> Optional[Provenance]:
        return self.provenance.get(key)

    def keys_from(self, origin: Origin) -> list[str]:
        return sorted(k for k, p in self.provenance.items() if p.origin == origin)
