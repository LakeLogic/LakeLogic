# Schema Validation API

LakeLogic ships a public validation and schema-export API designed for editor integration, CI pipelines, and developer tooling. It validates contracts, system registries, and domain configs without executing a pipeline.

---

## Quick Start

```python
from lakelogic import validate_contract, contract_schema, contract_schema_json

# ── Validate a contract ──────────────────────────────────────
result = validate_contract("contracts/bronze_orders_v1.0.yaml")

if result.valid:
    print("✅ Contract is valid")
else:
    for err in result.error_only:
        print(f"  [{err.field}] {err.message}")

# ── Get JSON Schema for UI forms ─────────────────────────────
schema = contract_schema()           # Python dict
schema_json = contract_schema_json() # JSON string for REST API
```

---

## `validate_contract(source)`

Validates a LakeLogic contract, system registry, or domain config. Accepts multiple input formats.

### Parameters

| Parameter | Type | Description |
|---|---|---|
| `source` | `dict \| str \| Path` | A parsed dict, YAML/JSON string, or file path ending in `.yaml`/`.yml`/`.json` |

### Returns

A `ValidationResult` object:

| Attribute | Type | Description |
|---|---|---|
| `.valid` | `bool` | `True` when there are no error-severity issues (warnings are allowed) |
| `.errors` | `list[ValidationError]` | All errors and warnings |
| `.warnings` | `list[ValidationError]` | Convenience view — warning-severity items only |
| `.error_only` | `list[ValidationError]` | Convenience view — error-severity items only |
| `.contract` | `dict \| None` | The parsed contract dict (`None` if parsing failed) |

Each `ValidationError` has:

| Field | Type | Description |
|---|---|---|
| `.field` | `str` | Dot-path to the issue (e.g. `server.schema_policy.evolution`) |
| `.message` | `str` | Human-readable error message |
| `.severity` | `str` | `"error"` or `"warning"` |

### Input Formats

=== "Dict"

    ```python
    result = validate_contract({
        "version": "1.0",
        "model": {
            "fields": [
                {"name": "order_id", "type": "long", "required": True}
            ]
        }
    })
    ```

=== "YAML File"

    ```python
    result = validate_contract("contracts/bronze_orders_v1.0.yaml")
    ```

=== "YAML String"

    ```python
    yaml_str = open("contract.yaml").read()
    result = validate_contract(yaml_str)
    ```

---

## What Gets Validated

The validator checks different properties depending on the document type. LakeLogic auto-detects whether you are validating an individual contract or a system/domain configs based on the `server:` block structure.

### Individual Contracts

For a data product contract (e.g. `bronze_orders_v1.0.yaml`), the validator checks:

| Section | Checks |
|---|---|
| `version` | Required, must be a string |
| `tier` / `layer` | Known medallion layer (warns if missing) |
| `info` | Recommends `title` and `version` |
| `server` | Requires `type` and `path`; validates `mode`, `format`, `schema_policy` |
| `source` | Requires `type`; validates `load_mode`, `watermark_field` for incremental |
| `model.fields` | Validates names, types, duplicates, `pii` flags, constraints |
| `quality` | Validates row rules (name + sql), dataset rules, shorthand rules |
| `transformations` | Validates operation keys, `phase` values |
| `materialization` | Validates `strategy` |
| `service_levels` | Validates `availability` threshold |
| `downstream` | Validates consumer `type`, `name`, `platform` |

### System/Domain Configs

When the `server:` block contains per-layer keys (`bronze:`, `silver:`, `gold:`), the validator automatically switches to **system-level mode**:

- `type` and `path` are **not** required (these are inherited per-contract)
- Each layer's sub-block is validated independently for `mode`, `format`, and `schema_policy`

```python
# System-level server block — per-layer, no type/path required
result = validate_contract({
    "version": "1.0",
    "server": {
        "bronze": {
            "mode": "ingest",
            "schema_policy": {"evolution": "append", "unknown_fields": "allow"},
            "cast_to_string": True,
        },
        "silver": {
            "mode": "validate",
            "schema_policy": {"evolution": "strict", "unknown_fields": "quarantine"},
        },
    },
})
assert result.valid  # ✅ No type/path required at system level
```

---

## Schema Policy Validation

The `schema_policy` block controls how the engine reacts to schema drift. The validator enforces valid values:

### `evolution`

| Value | Description |
|---|---|
| `strict` | Any schema change fails the pipeline (default) |
| `compatible` | Safe promotions allowed, dangerous changes quarantined |
| `append` | New fields are accepted, missing fields added as NULL |
| `merge` | Schema is merged additively |
| `overwrite` | Target schema is replaced entirely |
| `allow` | All changes pass through |

### `unknown_fields`

| Value | Description |
|---|---|
| `quarantine` | Unknown fields send the row to quarantine (default) |
| `drop` | Unknown fields are silently stripped |
| `allow` | Unknown fields are preserved in the output |

!!! tip "Automatic Synchronization"
    The `SchemaPolicy` model automatically synchronizes `evolution` and `unknown_fields`. Setting `evolution: append` or `evolution: merge` auto-sets `unknown_fields: allow` unless explicitly overridden.

---

## Deprecated Keys

The following legacy keys are still accepted but will emit deprecation warnings:

| Legacy Key | Replacement | Warning |
|---|---|---|
| `schema_evolution` | `schema_policy.evolution` | `'schema_evolution' is deprecated — use 'schema_policy.evolution' instead` |
| `allow_schema_drift` | `schema_policy.unknown_fields` | `'allow_schema_drift' is deprecated — use 'schema_policy.unknown_fields' instead` |

```python
# Legacy keys produce warnings, not errors
result = validate_contract({
    "version": "1.0",
    "server": {
        "type": "local",
        "path": ".",
        "schema_evolution": "strict",       # ⚠️ deprecated
        "allow_schema_drift": False,        # ⚠️ deprecated
    },
})
assert result.valid    # Still valid — just warns
assert len(result.warnings) >= 2  # Deprecation warnings
```

---

## `contract_schema()`

Returns the JSON Schema for a LakeLogic contract as a Python dict. Use this to drive form validation, field pickers, and auto-complete in visual editors.

```python
from lakelogic import contract_schema

schema = contract_schema()
print(schema["properties"].keys())
# dict_keys(['version', 'info', 'server', 'source', 'model', 'quality', ...])
```

The schema is generated from the `DataContract` Pydantic model and augmented with LakeLogic-specific enum values (server types, evolution modes, quality categories, etc.).

---

## `contract_schema_json(indent=2)`

Returns the JSON Schema as a JSON string — suitable for REST API responses:

```python
from lakelogic import contract_schema_json

# Serve from your API
# GET /api/contract-schema → Content-Type: application/json
json_str = contract_schema_json()
```

---

## CI/CD Integration

Use `validate_contract` in CI pipelines to catch contract errors before deployment:

```yaml
# .github/workflows/validate-contracts.yml
- name: Validate all contracts
  run: |
    python -c "
    import glob, sys
    from lakelogic import validate_contract

    errors = 0
    for path in glob.glob('domains/**/contracts/**/*.yaml', recursive=True):
        result = validate_contract(path)
        if not result.valid:
            errors += 1
            print(f'❌ {path}')
            for err in result.error_only:
                print(f'   [{err.field}] {err.message}')
        else:
            print(f'✅ {path}')

    sys.exit(1 if errors else 0)
    "
```

---

## Server Defaults Inheritance

The `server:` block in `_system.yaml` defines per-layer defaults that are inherited by all contracts in that system:

```yaml
# _system.yaml — Global Defaults
server:
  bronze:
    mode: "ingest"
    format: "delta"
    schema_policy:
      evolution: "append"
      unknown_fields: "allow"
    cast_to_string: true
  silver:
    mode: "validate"
    format: "delta"
    schema_policy:
      evolution: "strict"
      unknown_fields: "quarantine"
```

Individual contracts can override any setting:

```yaml
# bronze_messy_api_v1.0.yaml — Local Override
server:
  type: local
  path: "."
  schema_policy:
    evolution: "allow"     # Override: this contract allows all changes
```

LakeLogic deep-merges system defaults into each contract at runtime. Contract-level settings always take precedence.

!!! note "Inheritance Hierarchy"
    `_domain.yaml` → `_system.yaml` → contract YAML. Each level overrides the one above, with individual contracts having the highest precedence.
