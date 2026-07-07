"""Smoke tests for lakelogic.core.schema_api — validate_contract & contract_schema."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from lakelogic import validate_contract, contract_schema, contract_schema_json

ROOT = Path(__file__).parent.parent
ORDERS_CONTRACT = ROOT / "examples/06_advanced_workflows/synthetic_data_generation/contract_orders.yaml"


# ── helpers ───────────────────────────────────────────────────────────────────
def ok(tag, result):
    status = "OK " if result.valid else "ERR"
    n_err = len(result.error_only)
    n_warn = len(result.warnings)
    print(f"[{tag}] {status}  errors={n_err}  warnings={n_warn}")
    if not result.valid:
        for e in result.error_only:
            print(f"       ✗ [{e.field}] {e.message}")
    return result


# ── Test 1: valid contract file ───────────────────────────────────────────────
r = ok("1", validate_contract(ORDERS_CONTRACT))
assert r.valid, f"Expected valid, got errors: {r.error_only}"

# ── Test 2: valid dict ────────────────────────────────────────────────────────
r = ok(
    "2",
    validate_contract(
        {
            "version": "1.0.0",
            "info": {"title": "Test", "version": "1.0.0"},
            "model": {
                "fields": [
                    {"name": "id", "type": "integer", "required": True},
                    {"name": "status", "type": "string"},
                ]
            },
        }
    ),
)
assert r.valid

# ── Test 3: missing version ───────────────────────────────────────────────────
r = ok("3", validate_contract({"model": {"fields": []}}))
assert not r.valid
assert any("version" in e.field for e in r.error_only)

# ── Test 4: unknown field type (warning only) ─────────────────────────────────
r = ok(
    "4",
    validate_contract(
        {
            "version": "1.0",
            "model": {"fields": [{"name": "col", "type": "varchar(255)"}]},
        }
    ),
)
# Should be valid (warning, not error)
assert r.valid
assert any("type" in w.field for w in r.warnings)

# ── Test 5: duplicate field names ─────────────────────────────────────────────
r = ok(
    "5",
    validate_contract(
        {
            "version": "1.0",
            "model": {
                "fields": [
                    {"name": "id", "type": "integer"},
                    {"name": "id", "type": "string"},
                ]
            },
        }
    ),
)
assert not r.valid
assert any("Duplicate" in e.message for e in r.error_only)

# ── Test 6: invalid server mode ───────────────────────────────────────────────
r = ok(
    "6",
    validate_contract(
        {
            "version": "1.0",
            "server": {"type": "local", "path": "/tmp/data", "mode": "banana"},
        }
    ),
)
assert not r.valid

# ── Test 7: YAML string input ─────────────────────────────────────────────────
yaml_str = """
version: "1.0.0"
info:
  title: YAML String Test
  version: "1.0.0"
model:
  fields:
    - name: order_id
      type: integer
      required: true
    - name: amount
      type: float
"""
r = ok("7", validate_contract(yaml_str))
assert r.valid

# ── Test 8: JSON string input ─────────────────────────────────────────────────
r = ok(
    "8",
    validate_contract(
        json.dumps(
            {
                "version": "1.0",
                "info": {"title": "JSON test"},
                "model": {"fields": [{"name": "x", "type": "string"}]},
            }
        )
    ),
)
assert r.valid

# ── Test 9: contract_schema() returns a dict with expected keys ───────────────
schema = contract_schema()
assert isinstance(schema, dict)
assert schema.get("title") == "LakeLogic Data Contract"
assert "properties" in schema or "$defs" in schema
print(f"[9] OK   schema keys={len(schema)}, defs={len(schema.get('$defs', {}))}")

# ── Test 10: contract_schema_json() returns valid JSON ────────────────────────
raw_json = contract_schema_json()
parsed = json.loads(raw_json)
assert parsed.get("title") == "LakeLogic Data Contract"
print(f"[10] OK  contract_schema_json length={len(raw_json)} chars")

# ── Test 11: to_dict / to_json on ValidationResult ───────────────────────────
r = validate_contract({"version": "1.0"})
d = r.to_dict()
assert "valid" in d and "errors" in d
j = r.to_json()
assert json.loads(j)["valid"] == r.valid
print("[11] OK  ValidationResult serialisation")

# ── Test 12: top-level import symbols ─────────────────────────────────────────
from lakelogic import validate_contract, contract_schema, contract_schema_json

print("[12] OK  top-level import symbols")

print("\nALL schema_api TESTS PASSED")
