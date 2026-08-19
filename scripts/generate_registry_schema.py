"""Generate the JSON Schemas for the registry manifests from the Pydantic models.

Writes ``schemas/registry/{domain,system}-manifest-v1.schema.json`` — the language-agnostic
projection of :mod:`lakelogic.registry.models`, so editors and non-Python CI can validate
``_domain.yaml`` / ``_system.yaml`` without importing LakeLogic.

Run: ``python scripts/generate_registry_schema.py``. A drift-guard test asserts the checked-in
files match the models (``tests/registry/test_schema_export.py``).
"""

from __future__ import annotations

import json
from pathlib import Path

from lakelogic.registry.models import DomainManifestV1, SystemManifestV1

_OUT = Path(__file__).resolve().parents[1] / "schemas" / "registry"
_TARGETS = {
    "domain-manifest-v1.schema.json": DomainManifestV1,
    "system-manifest-v1.schema.json": SystemManifestV1,
}


def render(model) -> str:
    """Deterministic JSON for a model's schema (stable key order → clean diffs)."""
    return json.dumps(model.model_json_schema(), indent=2, sort_keys=True) + "\n"


def main() -> int:
    _OUT.mkdir(parents=True, exist_ok=True)
    for filename, model in _TARGETS.items():
        (_OUT / filename).write_text(render(model), encoding="utf-8")
        print(f"wrote {_OUT / filename}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
