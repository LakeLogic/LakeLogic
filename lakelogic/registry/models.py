"""Strict manifest models for the LakeLogic Registry layer.

Two files, validated **as authored** (before any inheritance is applied):

- :class:`DomainManifestV1` — a raw ``_domain.yaml``.
- :class:`SystemManifestV1` — a raw ``_system.yaml``.

Design, mirroring the OLC strict model:
  * **Strict top-level keys** — an unknown top-level key is an error, so typos and
    misplaced keys (e.g. ``server_defaults`` instead of ``server``, or a ``contracts:``
    block in a ``_domain.yaml``) are caught rather than silently ignored. The one
    exception is ``x-*`` keys, always allowed for YAML anchor holders
    (``x-azure-storage: &azure_storage``) and vendor extensions.
  * **``key: null`` means unset** — a null-valued key falls back to the field default
    (so an empty ``contracts:`` index is fine); a null on a *required* field still fails.
  * **Governance blocks stay free-form** — ``compliance`` / ``ownership`` / ``cost`` /
    ``observatory`` / ``metadata`` etc. are ``Dict[str, Any]``, matching the lenient
    runtime (their internal shape varies by org and provider and is not the manifest's job
    to police).
  * **Structural pieces are typed** — the ``contracts`` index, ``storage`` and
    ``environments`` reuse the runtime sub-models from :mod:`lakelogic.core.registry`, so
    there is one shape definition, not two that can drift.

Validated against every maintained demo mesh (rideflow across all clouds, food
manufacturing, ra-rideflow): 100% of those ``_domain.yaml`` / ``_system.yaml`` files
parse. See ``tests/registry/test_manifests.py``.

The *legal key set* on each file follows the resolution spec
(``docs/contracts/inheritance.md``): every **inheritable** key and every **identity
scalar** may appear on either file; the **structural** keys (``contracts``, ``storage``,
``environments``, ``metadata``, ``external_sources``, ``system``) belong to the system
manifest only.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lakelogic.core.registry import (
    CloudReporting,
    EnvironmentConfig,
    RegistryContract,
    RegistrySLO,
    RegistryStorage,
)


class _StrictManifest(BaseModel):
    """Base for the registry manifests: strict on top-level keys, but tolerant of two
    real conventions — ``x-*`` keys (YAML anchor holders like ``x-azure-storage:
    &azure_storage``, and vendor extensions, mirroring OLC's namespaced ``extensions``)
    are always permitted. Any *other* undeclared top-level key is a hard error, so typos
    and misplaced keys are surfaced rather than silently ignored."""

    model_config = ConfigDict(extra="allow")

    @model_validator(mode="before")
    @classmethod
    def _reject_unknown_top_keys(cls, data: Any) -> Any:
        if isinstance(data, dict):
            # YAML authors write `key:` (null) to mean "empty/unset" — e.g. an empty
            # `contracts:` index. Drop null-valued keys so the field's default applies;
            # a null on a *required* field (e.g. `system:`) still surfaces as missing.
            data = {k: v for k, v in data.items() if v is not None}
            allowed = set(cls.model_fields)
            unknown = [str(k) for k in data if k not in allowed and not str(k).startswith("x-")]
            if unknown:
                raise ValueError(
                    "unknown top-level key(s) not permitted in registry manifest: "
                    + ", ".join(sorted(unknown))
                    + " (use an `x-` prefix for anchors / vendor extensions)"
                )
        return data


# Canonical key classes live in the dependency-free `keys` module (shared with the runtime
# merge); re-exported here for the models/validator that reference them.
from lakelogic.registry.keys import IDENTITY_SCALARS, INHERITABLE_KEYS  # noqa: E402,F401  (re-export)
from lakelogic.registry.ownership import Ownership, OwnershipError  # noqa: E402


class DomainManifestV1(_StrictManifest):
    """A raw ``_domain.yaml`` — domain-wide governance defaults inherited by every system
    and contract in the domain. Strict on top-level keys; governance blocks are free-form.
    """

    # ── identity ─────────────────────────────────────────────────────────────
    domain: str
    # Layer aliases + the global notifications switch — "identity scalars": under merge
    # the domain value is authoritative for consistency across the whole domain.
    bronze_layer: str = "bronze"
    silver_layer: str = "silver"
    gold_layer: str = "gold"
    notifications_enabled: Optional[bool] = None

    # ── inheritable governance defaults (free-form where the runtime is lenient) ──
    ownership: Dict[str, Any] = Field(default_factory=dict)
    slo: Optional[RegistrySLO] = None
    compliance: Dict[str, Any] = Field(default_factory=dict)
    cost: Dict[str, Any] = Field(default_factory=dict)
    observatory: Dict[str, Any] = Field(default_factory=dict)
    retention: Dict[str, Any] = Field(default_factory=dict)
    notifications: List[Dict[str, Any]] = Field(default_factory=list)
    # Operational blocks a domain MAY set as defaults for its systems.
    quarantine: Dict[str, Any] = Field(default_factory=dict)
    lineage: Dict[str, Any] = Field(default_factory=dict)
    materialization: Dict[str, Any] = Field(default_factory=dict)
    server: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_ownership_roles(self):
        """Check the ownership role blocks we define, leave the rest free-form.

        ``ownership`` stays a free-form dict per this module's design — an org's
        ``cost_center``/``jira_project``/``contacts`` shape is not the manifest's business.
        But the four role keys ARE ours, so a misspelt accountability category is a typo we
        can name at parse time rather than a question that silently never routes.
        """
        try:
            Ownership.parse(self.ownership, where="ownership")
        except OwnershipError as exc:
            raise ValueError(str(exc)) from exc
        return self


class SystemManifestV1(_StrictManifest):
    """A raw ``_system.yaml`` — one source/system, its storage/environment wiring, and the
    explicit index of the contracts it owns. Strict on top-level keys; the contract index
    and environments are typed for referential integrity.
    """

    # ── identity ─────────────────────────────────────────────────────────────
    system: str
    domain: Optional[str] = None  # inherited from _domain.yaml when omitted

    # ── structural (system-only) ─────────────────────────────────────────────
    contracts: List[RegistryContract] = Field(default_factory=list)
    storage: Optional[RegistryStorage] = None
    environments: Dict[str, EnvironmentConfig] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    external_sources: List[Dict[str, Any]] = Field(default_factory=list)
    cloud: Optional[CloudReporting] = None

    # ── identity scalars (a system may override; domain wins on mismatch) ────────
    bronze_layer: Optional[str] = None
    silver_layer: Optional[str] = None
    gold_layer: Optional[str] = None
    notifications_enabled: Optional[bool] = None

    # ── inheritable defaults a system may set/override ───────────────────────────
    ownership: Dict[str, Any] = Field(default_factory=dict)
    slo: Optional[RegistrySLO] = None
    compliance: Dict[str, Any] = Field(default_factory=dict)
    cost: Dict[str, Any] = Field(default_factory=dict)
    observatory: Dict[str, Any] = Field(default_factory=dict)
    retention: Dict[str, Any] = Field(default_factory=dict)
    notifications: List[Dict[str, Any]] = Field(default_factory=list)
    quarantine: Dict[str, Any] = Field(default_factory=dict)
    lineage: Dict[str, Any] = Field(default_factory=dict)
    materialization: Dict[str, Any] = Field(default_factory=dict)
    server: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _unique_entities(self) -> "SystemManifestV1":
        """Every contract ``entity`` must be unique within the system — the same
        referential-integrity rule the runtime enforces on the resolved registry."""
        seen: Dict[str, str] = {}
        for c in self.contracts:
            if c.entity in seen:
                raise ValueError(
                    f"duplicate contract entity '{c.entity}' "
                    f"(layers '{seen[c.entity]}' and '{c.layer}') — entities must be "
                    f"unique within a system"
                )
            seen[c.entity] = c.layer
        return self

    @model_validator(mode="after")
    def validate_ownership_roles(self):
        """Check the ownership role blocks we define, leave the rest free-form.

        ``ownership`` stays a free-form dict per this module's design — an org's
        ``cost_center``/``jira_project``/``contacts`` shape is not the manifest's business.
        But the four role keys ARE ours, so a misspelt accountability category is a typo we
        can name at parse time rather than a question that silently never routes.
        """
        try:
            Ownership.parse(self.ownership, where="ownership")
        except OwnershipError as exc:
            raise ValueError(str(exc)) from exc
        return self
