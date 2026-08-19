"""Strict registry manifest models — DomainManifestV1 / SystemManifestV1.

Two kinds of guard:
  * **Real-file smoke** — every in-repo RideFlow ``_domain.yaml`` / ``_system.yaml``
    (``examples/colab/assets/domains_rideflow``) must validate, so the strict schema
    tracks the shape actually authored across the mesh (not an idealised guess).
  * **Behavioural** — the strictness rules from ``docs/contracts/inheritance.md``:
    unknown top-level keys rejected, ``x-*`` anchors allowed, ``key: null`` treated as
    unset, and duplicate contract entities rejected.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lakelogic.registry import (
    DomainManifestV1,
    SystemManifestV1,
)

try:
    from ruamel.yaml import YAML

    def _load(text: str):
        return YAML(typ="safe").load(text)
except Exception:  # pragma: no cover
    import yaml

    def _load(text: str):
        return yaml.safe_load(text)


_ASSETS = Path(__file__).resolve().parents[2] / "examples" / "colab" / "assets" / "domains_rideflow"


def _read(p: Path) -> dict:
    return _load(p.read_text(encoding="utf-8")) or {}


# ── Real-file smoke ──────────────────────────────────────────────────────────


@pytest.mark.parametrize("path", sorted(_ASSETS.glob("*/_domain.yaml")), ids=lambda p: p.parent.name)
def test_real_domain_files_validate(path):
    DomainManifestV1.model_validate(_read(path))


@pytest.mark.parametrize("path", sorted(_ASSETS.glob("**/_system.yaml")), ids=lambda p: "/".join(p.parts[-3:-1]))
def test_real_system_files_validate(path):
    SystemManifestV1.model_validate(_read(path))


# ── Behavioural: strict top-level, x- anchors, null-as-unset ─────────────────


def test_unknown_top_level_key_rejected():
    """A typo like `server_defaults` (instead of `server`) is a hard error, not ignored."""
    with pytest.raises(ValueError, match="server_defaults"):
        SystemManifestV1.model_validate({"system": "s", "server_defaults": {}})
    with pytest.raises(ValueError, match="contracts"):
        # A system-only structural key does not belong on a domain manifest.
        DomainManifestV1.model_validate({"domain": "d", "contracts": []})


def test_x_prefix_anchor_keys_allowed():
    """`x-*` keys (YAML anchor holders / vendor extensions) are always permitted."""
    m = SystemManifestV1.model_validate(
        {
            "system": "google_analytics",
            "x-azure-storage": {"storage_root": "abfss://nondelta@acct.dfs.core.windows.net"},
        }
    )
    assert m.system == "google_analytics"


def test_null_valued_key_treated_as_unset():
    """`contracts:` (null) means an empty index — the field default applies."""
    m = SystemManifestV1.model_validate({"system": "s", "contracts": None})
    assert m.contracts == []
    # But a null on a *required* field still fails.
    with pytest.raises(ValueError, match="system"):
        SystemManifestV1.model_validate({"system": None, "domain": "d"})


# ── Behavioural: referential integrity + required identity ───────────────────


def test_duplicate_contract_entity_rejected():
    payload = {
        "system": "s",
        "contracts": [
            {"layer": "bronze", "entity": "orders", "path": "a.yaml"},
            {"layer": "silver", "entity": "orders", "path": "b.yaml"},
        ],
    }
    with pytest.raises(ValueError, match="duplicate contract entity 'orders'"):
        SystemManifestV1.model_validate(payload)


def test_domain_manifest_requires_domain():
    with pytest.raises(ValueError, match="domain"):
        DomainManifestV1.model_validate({"ownership": {"team": "x"}})


def test_inheritable_keys_allowed_on_both_manifests():
    """An inheritable governance key (e.g. `slo`, `notifications`) is legal on a system
    manifest, not only a domain one — matching the resolution spec."""
    m = SystemManifestV1.model_validate(
        {
            "system": "s",
            "notifications": [{"type": "slack", "target": "#x", "on_events": ["failed"]}],
            "compliance": {"data_residency": "EU"},
        }
    )
    assert m.compliance["data_residency"] == "EU"
