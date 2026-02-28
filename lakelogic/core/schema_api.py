"""
lakelogic.core.schema_api
=========================

Public contract validation and JSON Schema export API — designed as the
integration surface for LineageLogic's visual contract editor.

Two entry points:

    from lakelogic import validate_contract, contract_schema, contract_schema_json

``validate_contract(source)``
    Validate a contract dict, YAML string, or file path.
    Returns a ``ValidationResult`` with ``.valid``, ``.errors``, and ``.warnings``.

``contract_schema()``
    Return the JSON Schema dict for a LakeLogic contract.
    LineageLogic uses this to drive form validation and field pickers.

``contract_schema_json()``
    Return the JSON Schema as a JSON string (for REST API responses).

Integration flow
----------------
::

    # LineageLogic backend calls:
    schema  = contract_schema()          # once at startup — drives the UI form
    result  = validate_contract(draft)   # on every save / publish action

    # If result.valid:
    #   persist YAML, trigger LakeLogic pipeline
    # Else:
    #   return result.errors to the frontend for inline display

"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml

# ── Internal helpers ──────────────────────────────────────────────────────────

_KNOWN_TYPES = {
    # Primitive
    "string", "str",
    "integer", "int",
    "bigint", "long",
    "float", "double", "decimal", "numeric",
    "boolean", "bool",
    # Date / time
    "date", "datetime", "timestamp", "time",
    # Complex
    "list", "array",
    "map", "dict", "object",
    "struct",
    # Binary
    "bytes", "binary",
    # Nullable shorthand (e.g. "string?")
}

_KNOWN_LAYERS = {"bronze", "silver", "gold", "landing", "raw"}
_KNOWN_FORMATS = {"parquet", "delta", "csv", "json", "ndjson", "iceberg", "avro", "orc", "excel"}
_KNOWN_SERVER_TYPES = {"s3", "gcs", "adls", "azure", "local", "glue", "databricks", "bigquery"}
_KNOWN_MODES = {"validate", "ingest"}
_KNOWN_EVOLUTION = {"strict", "append", "merge", "overwrite", "compatible", "allow"}
_KNOWN_STRATEGIES = {"append", "merge", "scd2", "overwrite"}
_KNOWN_LOAD_MODES = {"full", "incremental", "cdc"}
_KNOWN_SEVERITY = {"error", "warning", "info"}
_KNOWN_CATEGORIES = {
    "correctness", "completeness", "consistency", "validity",
    "accuracy", "timeliness", "uniqueness", "integrity", "schema", "rule",
}


# ── Result types ──────────────────────────────────────────────────────────────

@dataclass
class ValidationError:
    """A single contract validation error with field path and message."""
    field: str          # e.g. "model.fields[2].type"
    message: str        # human-readable error message
    severity: str = "error"   # "error" | "warning"

    def to_dict(self) -> Dict[str, str]:
        return {"field": self.field, "message": self.message, "severity": self.severity}


@dataclass
class ValidationResult:
    """
    Result returned by ``validate_contract()``.

    Attributes
    ----------
    valid : bool
        True when there are no *error*-severity issues (warnings are allowed).
    errors : list[ValidationError]
        All errors and warnings found.
    warnings : list[ValidationError]
        Convenience view — only warning-severity items.
    contract : dict | None
        The parsed contract dict (None if parsing failed entirely).
    """
    valid: bool
    errors: List[ValidationError] = field(default_factory=list)
    contract: Optional[Dict[str, Any]] = None

    @property
    def warnings(self) -> List[ValidationError]:
        return [e for e in self.errors if e.severity == "warning"]

    @property
    def error_only(self) -> List[ValidationError]:
        return [e for e in self.errors if e.severity == "error"]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "valid": self.valid,
            "errors": [e.to_dict() for e in self.errors],
        }

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent)

    def __repr__(self) -> str:  # pragma: no cover
        status = "✅ valid" if self.valid else f"❌ {len(self.error_only)} error(s)"
        return f"ValidationResult({status}, {len(self.warnings)} warning(s))"


# ── Validator ─────────────────────────────────────────────────────────────────

class _ContractValidator:
    """Internal validator — walks a contract dict and collects issues."""

    def __init__(self, data: Dict[str, Any]) -> None:
        self._data = data
        self._issues: List[ValidationError] = []

    # ── Public ────────────────────────────────────────────────────────────────

    def validate(self) -> ValidationResult:
        d = self._data
        self._check_root(d)
        self._check_info(d.get("info", {}), "info")
        self._check_server(d.get("server"), "server")
        self._check_source(d.get("source"), "source")
        self._check_model(d.get("model"), "model")
        self._check_quality(d.get("quality"), "quality")
        self._check_transformations(d.get("transformations", []), "transformations")
        self._check_materialization(d.get("materialization"), "materialization")
        self._check_service_levels(d.get("service_levels"), "service_levels")
        errors = self._issues
        return ValidationResult(
            valid=not any(e.severity == "error" for e in errors),
            errors=errors,
            contract=self._data,
        )

    # ── Root ──────────────────────────────────────────────────────────────────

    def _check_root(self, d: Dict[str, Any]) -> None:
        if "version" not in d:
            self._err("version", "Required field 'version' is missing")
        elif not isinstance(d["version"], str):
            self._err("version", f"'version' must be a string, got {type(d['version']).__name__}")

    # ── Info ──────────────────────────────────────────────────────────────────

    def _check_info(self, info: Any, path: str) -> None:
        if not info:
            return
        if not isinstance(info, dict):
            self._err(path, "'info' must be a mapping")
            return
        if "title" not in info:
            self._warn(f"{path}.title", "'info.title' is missing — recommended for discoverability")
        if "version" not in info:
            self._warn(f"{path}.version", "'info.version' is missing")

    # ── Server ────────────────────────────────────────────────────────────────

    def _check_server(self, server: Any, path: str) -> None:
        if server is None:
            return
        if not isinstance(server, dict):
            self._err(path, "'server' must be a mapping")
            return
        for req in ("type", "path"):
            if req not in server:
                self._err(f"{path}.{req}", f"'server.{req}' is required")
        if "type" in server and server["type"] not in _KNOWN_SERVER_TYPES:
            self._warn(f"{path}.type", f"Unknown server type '{server['type']}'. "
                       f"Known: {sorted(_KNOWN_SERVER_TYPES)}")
        if "format" in server and server["format"] not in _KNOWN_FORMATS:
            self._warn(f"{path}.format", f"Unknown format '{server['format']}'. "
                       f"Known: {sorted(_KNOWN_FORMATS)}")
        if "mode" in server and server["mode"] not in _KNOWN_MODES:
            self._err(f"{path}.mode", f"'server.mode' must be one of {sorted(_KNOWN_MODES)}, "
                      f"got '{server['mode']}'")
        if "schema_evolution" in server and server["schema_evolution"] not in _KNOWN_EVOLUTION:
            self._warn(f"{path}.schema_evolution",
                       f"Unknown schema_evolution value '{server['schema_evolution']}'")

    # ── Source ────────────────────────────────────────────────────────────────

    def _check_source(self, source: Any, path: str) -> None:
        if source is None:
            return
        if not isinstance(source, dict):
            self._err(path, "'source' must be a mapping")
            return
        if "type" not in source:
            self._err(f"{path}.type", "'source.type' is required (landing | stream | table)")
        lm = source.get("load_mode", "full")
        if lm not in _KNOWN_LOAD_MODES:
            self._err(f"{path}.load_mode", f"'load_mode' must be one of {sorted(_KNOWN_LOAD_MODES)}, got '{lm}'")
        if lm == "incremental" and not source.get("watermark_field"):
            self._warn(f"{path}.watermark_field",
                       "Incremental load mode without 'watermark_field' — boundary detection may fail")

    # ── Model ─────────────────────────────────────────────────────────────────

    def _check_model(self, model: Any, path: str) -> None:
        if model is None:
            return
        if not isinstance(model, dict):
            self._err(path, "'model' must be a mapping")
            return
        fields = model.get("fields", [])
        if not isinstance(fields, list):
            self._err(f"{path}.fields", "'model.fields' must be a list")
            return
        if not fields:
            self._warn(f"{path}.fields", "'model.fields' is empty — contract has no schema definition")
        seen_names: set[str] = set()
        for i, f in enumerate(fields):
            fp = f"{path}.fields[{i}]"
            if not isinstance(f, dict):
                self._err(fp, "Each field must be a mapping")
                continue
            if "name" not in f:
                self._err(f"{fp}.name", "Field is missing 'name'")
            else:
                name = f["name"]
                if name in seen_names:
                    self._err(f"{fp}.name", f"Duplicate field name '{name}'")
                seen_names.add(name)
            if "type" not in f:
                self._err(f"{fp}.type", f"Field '{f.get('name', i)}' is missing 'type'")
            else:
                self._check_field_type(f["type"], f"{fp}.type", f.get("name", str(i)))
            # accepted_values must be a list
            if "accepted_values" in f and not isinstance(f["accepted_values"], list):
                self._err(f"{fp}.accepted_values", "'accepted_values' must be a list")
            # min/max must be numeric
            for bound in ("min", "max"):
                if bound in f and not isinstance(f[bound], (int, float)):
                    self._err(f"{fp}.{bound}", f"'{bound}' must be a number")
            # pii must be bool
            if "pii" in f and not isinstance(f["pii"], bool):
                self._warn(f"{fp}.pii", "'pii' should be a boolean (true/false)")

    def _check_field_type(self, type_val: Any, path: str, name: str) -> None:
        if not isinstance(type_val, str):
            self._err(path, f"Field '{name}': 'type' must be a string, got {type(type_val).__name__}")
            return
        cleaned = type_val.rstrip("?").lower()
        if cleaned not in _KNOWN_TYPES:
            self._warn(path, f"Field '{name}': unknown type '{type_val}'. "
                       f"Known types: {sorted(_KNOWN_TYPES)}")

    # ── Quality ───────────────────────────────────────────────────────────────

    def _check_quality(self, quality: Any, path: str) -> None:
        if quality is None:
            return
        if not isinstance(quality, dict):
            self._err(path, "'quality' must be a mapping")
            return
        self._check_rules(quality.get("row_rules", []), f"{path}.row_rules")
        self._check_rules(quality.get("dataset_rules", []), f"{path}.dataset_rules")

    def _check_rules(self, rules: Any, path: str) -> None:
        if not isinstance(rules, list):
            self._err(path, f"'{path}' must be a list")
            return
        for i, rule in enumerate(rules):
            rp = f"{path}[{i}]"
            if not isinstance(rule, dict):
                self._err(rp, "Each rule must be a mapping")
                continue
            # Business-friendly shorthand rules (not_null, accepted_values, etc.)
            shorthand_keys = {"not_null", "accepted_values", "regex_match", "range",
                              "referential_integrity", "lifecycle_window", "unique",
                              "null_ratio", "row_count_between"}
            if any(k in rule for k in shorthand_keys):
                continue  # shorthand forms — validated at runtime by DataProcessor
            # SQL-form rule
            if "name" not in rule:
                self._err(f"{rp}.name", "Rule is missing 'name'")
            if "sql" not in rule:
                self._err(f"{rp}.sql", f"Rule '{rule.get('name', i)}' is missing 'sql'")
            sev = rule.get("severity", "error")
            if sev not in _KNOWN_SEVERITY:
                self._warn(f"{rp}.severity", f"Unknown severity '{sev}'. "
                           f"Expected: {sorted(_KNOWN_SEVERITY)}")
            cat = rule.get("category", "correctness")
            if cat not in _KNOWN_CATEGORIES:
                self._warn(f"{rp}.category", f"Unknown category '{cat}'")

    # ── Transformations ───────────────────────────────────────────────────────

    def _check_transformations(self, txs: Any, path: str) -> None:
        if not isinstance(txs, list):
            self._err(path, "'transformations' must be a list")
            return
        _known_tx = {
            "rename", "derive", "lookup", "filter", "deduplicate", "select",
            "drop", "cast", "trim", "lower", "upper", "coalesce", "split",
            "explode", "map_values", "rollup", "join", "pivot", "unpivot", "sql",
        }
        for i, tx in enumerate(txs):
            tp = f"{path}[{i}]"
            if not isinstance(tx, dict):
                self._err(tp, "Each transformation must be a mapping")
                continue
            known = {k for k in tx if k in _known_tx}
            if not known:
                self._warn(tp, f"Transformation has no recognised operation key. "
                           f"Known: {sorted(_known_tx)}")
            phase = tx.get("phase", "post")
            if phase not in ("pre", "post"):
                self._err(f"{tp}.phase", f"'phase' must be 'pre' or 'post', got '{phase}'")

    # ── Materialization ───────────────────────────────────────────────────────

    def _check_materialization(self, mat: Any, path: str) -> None:
        if mat is None:
            return
        if not isinstance(mat, dict):
            self._err(path, "'materialization' must be a mapping")
            return
        strategy = mat.get("strategy", "append")
        if strategy not in _KNOWN_STRATEGIES:
            self._warn(f"{path}.strategy", f"Unknown materialization strategy '{strategy}'. "
                       f"Known: {sorted(_KNOWN_STRATEGIES)}")

    # ── Service Levels ────────────────────────────────────────────────────────

    def _check_service_levels(self, sl: Any, path: str) -> None:
        if sl is None:
            return
        if not isinstance(sl, dict):
            self._err(path, "'service_levels' must be a mapping")
            return
        avail = sl.get("availability")
        if avail is not None:
            if isinstance(avail, dict):
                threshold = avail.get("threshold")
                if threshold is not None and not isinstance(threshold, (int, float, str)):
                    self._warn(f"{path}.availability.threshold",
                               "'availability.threshold' should be a number or percentage string")
            elif not isinstance(avail, (int, float)):
                self._warn(f"{path}.availability",
                           "'availability' should be a number (e.g. 99.9) or an SLO object")

    # ── Helpers ───────────────────────────────────────────────────────────────

    def _err(self, field_path: str, message: str) -> None:
        self._issues.append(ValidationError(field=field_path, message=message, severity="error"))

    def _warn(self, field_path: str, message: str) -> None:
        self._issues.append(ValidationError(field=field_path, message=message, severity="warning"))


# ── Public API ────────────────────────────────────────────────────────────────

def validate_contract(
    source: Union[str, Path, Dict[str, Any]],
) -> ValidationResult:
    """
    Validate a LakeLogic contract.

    Parameters
    ----------
    source : str | Path | dict
        - A ``dict`` — the contract already parsed into Python.
        - A ``str`` that looks like YAML/JSON — parsed in-memory.
        - A ``Path`` or file-path string ending in ``.yaml``/``.yml``/``.json`` — read from disk.

    Returns
    -------
    ValidationResult
        ``.valid`` is True when there are no error-severity issues.
        ``.errors`` is a list of :class:`ValidationError` with ``.field``,
        ``.message``, and ``.severity``.

    Examples
    --------
    ::

        from lakelogic import validate_contract

        # From a dict
        result = validate_contract({"version": "1.0", "model": {"fields": []}})

        # From a YAML string
        result = validate_contract(open("contract.yaml").read())

        # From a file path
        result = validate_contract("contracts/bronze_orders.yaml")

        if result.valid:
            print("Contract is valid")
        else:
            for err in result.error_only:
                print(f"  [{err.field}] {err.message}")
    """
    data: Dict[str, Any]

    if isinstance(source, dict):
        data = source
    elif isinstance(source, (str, Path)):
        path = Path(source)
        if path.exists() and path.suffix in (".yaml", ".yml", ".json"):
            raw = path.read_text(encoding="utf-8")
        else:
            raw = str(source)
        try:
            if raw.lstrip().startswith("{"):
                data = json.loads(raw)
            else:
                data = yaml.safe_load(raw)
        except Exception as exc:
            return ValidationResult(
                valid=False,
                errors=[ValidationError("(parse)", f"Failed to parse contract: {exc}")],
            )
        if not isinstance(data, dict):
            return ValidationResult(
                valid=False,
                errors=[ValidationError("(root)", "Contract must be a YAML/JSON mapping at root level")],
            )
    else:
        return ValidationResult(
            valid=False,
            errors=[ValidationError("(input)", f"Unsupported source type: {type(source).__name__}")],
        )

    return _ContractValidator(data).validate()


def contract_schema() -> Dict[str, Any]:
    """
    Return the JSON Schema for a LakeLogic contract as a Python dict.

    Intended for LineageLogic's visual editor to use at startup — the schema
    drives form fields, type pickers, and real-time validation.

    The schema is generated from the ``DataContract`` Pydantic model and
    augmented with LakeLogic-specific enum values for enumerations.

    Returns
    -------
    dict
        A JSON Schema (draft-07 compatible) dict.

    Example
    -------
    ::

        from lakelogic import contract_schema
        schema = contract_schema()
        print(schema["properties"].keys())
        # dict_keys(['version', 'info', 'server', 'source', 'model', 'quality', ...])
    """
    try:
        from lakelogic.core.models import DataContract
        base = DataContract.model_json_schema()
    except Exception:
        base = {}

    # Augment with LakeLogic-specific metadata the Pydantic schema doesn't capture
    _augment_schema(base)
    return base


def contract_schema_json(indent: int = 2) -> str:
    """
    Return the JSON Schema for a LakeLogic contract as a JSON string.

    Suitable for serving from a REST endpoint:
    ``GET /api/contract-schema``  →  ``Content-Type: application/json``

    Parameters
    ----------
    indent : int
        JSON indentation level (default 2).

    Returns
    -------
    str
        JSON-encoded schema string.
    """
    return json.dumps(contract_schema(), indent=indent)


def _augment_schema(schema: Dict[str, Any]) -> None:
    """Inject LakeLogic-specific enum hints into the raw Pydantic schema."""
    props = schema.get("properties", {})

    # Top-level version hint
    if "version" in props:
        props["version"]["examples"] = ["1.0.0", "2.0.0"]
        props["version"]["description"] = "Semantic version of the contract (e.g. '1.0.0')"

    # server.type enum
    _set_nested_enum(schema, ["$defs", "Server", "properties", "type"],
                     sorted(_KNOWN_SERVER_TYPES),
                     "Storage backend type")
    _set_nested_enum(schema, ["$defs", "Server", "properties", "format"],
                     sorted(_KNOWN_FORMATS),
                     "File/table format")
    _set_nested_enum(schema, ["$defs", "Server", "properties", "mode"],
                     sorted(_KNOWN_MODES),
                     "Pipeline mode: 'validate' for quality gate, 'ingest' for raw→bronze")
    _set_nested_enum(schema, ["$defs", "Server", "properties", "schema_evolution"],
                     sorted(_KNOWN_EVOLUTION),
                     "Schema evolution policy")

    # source.load_mode
    _set_nested_enum(schema, ["$defs", "SourceConfig", "properties", "load_mode"],
                     sorted(_KNOWN_LOAD_MODES),
                     "Load strategy: full reload or incremental/CDC")

    # quality category
    _set_nested_enum(schema, ["$defs", "QualityRule", "properties", "category"],
                     sorted(_KNOWN_CATEGORIES),
                     "Quality dimension this rule measures")
    _set_nested_enum(schema, ["$defs", "QualityRule", "properties", "severity"],
                     sorted(_KNOWN_SEVERITY),
                     "Severity: error quarantines the row, warning logs only")

    # field type hint
    _set_nested_enum(schema, ["$defs", "FieldDefinition", "properties", "type"],
                     sorted(_KNOWN_TYPES),
                     "Column data type")

    # materialization strategy
    _set_nested_enum(schema, ["$defs", "Materialization", "properties", "strategy"],
                     sorted(_KNOWN_STRATEGIES),
                     "Write strategy for Silver/Gold outputs")

    # Add UI-friendly metadata
    schema["title"] = "LakeLogic Data Contract"
    schema["description"] = (
        "A LakeLogic contract defines the schema, quality rules, transformation "
        "pipeline, and operational settings for a data entity. "
        "See https://github.com/lakelogic/LakeLogic for full documentation."
    )
    schema["x-lakelogic-version"] = "1"


def _set_nested_enum(
    schema: Dict[str, Any],
    path: List[str],
    values: List[str],
    description: str,
) -> None:
    """Set enum + description at a nested schema path (creates nodes if missing)."""
    node = schema
    for key in path[:-1]:
        if key not in node:
            node[key] = {}
        node = node[key]
    last = path[-1]
    if last not in node:
        node[last] = {}
    node[last]["enum"] = values
    node[last]["description"] = description
