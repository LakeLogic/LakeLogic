"""Keys the platform writes must pass `registry validate` (Tagus Air, 2026-11-16).

Every generated `_system.yaml` carries `storage_mode` (the runtime DomainRegistry reads it) and a
domain confirmed through the business ontology carries `concepts`; the strict manifests rejected
both, so 15 of 17 registry files failed validation while the pipelines ran fine.
"""

from lakelogic.registry.models import DomainManifestV1, SystemManifestV1


def test_a_system_manifest_may_declare_its_storage_mode():
    m = SystemManifestV1.model_validate({"system": "movement", "domain": "operations", "storage_mode": "catalog"})
    assert m.storage_mode == "catalog"


def test_a_domain_manifest_may_list_its_confirmed_concepts():
    m = DomainManifestV1.model_validate(
        {"domain": "operations", "concepts": {"Movement message": {"identifier": "message_id", "aliases": ["MVT"]}}}
    )
    assert "Movement message" in m.concepts


def test_unknown_keys_are_still_rejected():
    import pytest

    with pytest.raises(ValueError, match="unknown top-level key"):
        SystemManifestV1.model_validate({"system": "s", "storage_mod": "catalog"})
