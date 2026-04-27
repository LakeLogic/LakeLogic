from __future__ import annotations

import json
import sys
import types

from lakelogic.core import schema_api as sa


def test_validation_result_and_error_helpers():
    warning = sa.ValidationError(field="info.title", message="missing title", severity="warning")
    error = sa.ValidationError(field="version", message="missing")
    result = sa.ValidationResult(valid=False, errors=[warning, error], contract={"version": "1.0"})

    assert warning.to_dict() == {"field": "info.title", "message": "missing title", "severity": "warning"}
    assert result.warnings == [warning]
    assert result.error_only == [error]
    assert result.to_dict()["valid"] is False
    assert json.loads(result.to_json())["errors"][0]["severity"] == "warning"


def test_validate_contract_parsing_paths(tmp_path):
    yaml_path = tmp_path / "contract.yaml"
    yaml_path.write_text("version: '1.0'\nmodel:\n  fields:\n    - name: id\n      type: integer\n", encoding="utf-8")
    assert sa.validate_contract(yaml_path).valid is True

    json_path = tmp_path / "contract.json"
    json_path.write_text(json.dumps({"version": "1.0", "model": {"fields": [{"name": "id", "type": "string"}]}}), encoding="utf-8")
    assert sa.validate_contract(json_path).valid is True

    yaml_result = sa.validate_contract("version: '1.0'\nmodel:\n  fields:\n    - name: id\n      type: string\n")
    assert yaml_result.valid is True

    json_result = sa.validate_contract('{"version":"1.0","model":{"fields":[{"name":"id","type":"integer"}]}}')
    assert json_result.valid is True

    parse_error = sa.validate_contract("version: [")
    assert parse_error.valid is False
    assert parse_error.errors[0].field == "(parse)"

    root_error = sa.validate_contract("[1, 2, 3]")
    assert root_error.valid is False
    assert root_error.errors[0].field == "(root)"

    input_error = sa.validate_contract(123)
    assert input_error.valid is False
    assert input_error.errors[0].field == "(input)"


def test_validate_contract_collects_nested_errors_and_warnings():
    contract = {
        "version": 1,
        "tier": 123,
        "info": "bad",
        "server": {
            "type": "mystery",
            "mode": "banana",
            "schema_evolution": True,
            "allow_schema_drift": True,
            "schema_policy": {"evolution": "wild", "unknown_fields": "sideways"},
        },
        "source": {"load_mode": "invalid"},
        "model": {
            "fields": [
                {"name": "id", "type": 5, "accepted_values": "x", "min": "0", "pii": "yes"},
                {"name": "id", "type": "custom_type", "max": "1"},
                "bad-field",
            ]
        },
        "quality": {
            "row_rules": [
                {"name": "broken"},
                {"sql": "select 1", "severity": "fatal", "category": "odd"},
                "bad-rule",
            ],
            "dataset_rules": "bad",
        },
        "transformations": [{"unknown": "x", "phase": "middle"}, "bad-transform"],
        "materialization": {"strategy": "mystery"},
        "service_levels": {"availability": {"threshold": []}},
        "downstream": [{"type": "weird", "platform": "odd"}, "bad-consumer"],
    }

    result = sa.validate_contract(contract)

    assert result.valid is False
    fields = {issue.field for issue in result.errors}
    assert "version" in fields
    assert "tier" in fields
    assert "info" in fields
    assert "server.path" in fields
    assert "server.mode" in fields
    assert "source.type" in fields
    assert "source.load_mode" in fields
    assert "model.fields[0].type" in fields
    assert "model.fields[0].accepted_values" in fields
    assert "model.fields[1].name" in fields
    assert "quality.row_rules[0].sql" in fields
    assert "quality.row_rules[1].severity" in fields
    assert "quality.dataset_rules" in fields
    assert "transformations[0]" in fields
    assert "transformations[0].phase" in fields
    assert "materialization.strategy" in fields
    assert "service_levels.availability.threshold" in fields
    assert "downstream[0].name" in fields
    assert "downstream[0].type" in fields
    assert "downstream[1]" in fields


def test_validate_contract_layered_server_blocks_and_incremental_warning():
    contract = {
        "version": "1.0",
        "layer": "mystery-layer",
        "info": {},
        "server": {
            "bronze": {"schema_policy": {"evolution": "append", "unknown_fields": "allow"}},
            "silver": {"type": "odd", "format": "xyz", "mode": "validate"},
        },
        "source": {"type": "landing", "load_mode": "incremental"},
        "model": {"fields": []},
        "quality": {"row_rules": [{"not_null": "id"}], "dataset_rules": []},
        "transformations": [],
        "service_levels": {"availability": 99.9},
        "downstream": [],
    }

    result = sa.validate_contract(contract)
    warning_fields = {issue.field for issue in result.warnings}

    assert result.valid is True
    assert "tier" in warning_fields
    assert "server.silver.type" in warning_fields
    assert "server.silver.format" in warning_fields
    assert "source.watermark_field" in warning_fields
    assert "model.fields" in warning_fields


def test_contract_schema_and_schema_json_augmentation(monkeypatch):
    fake_models = types.ModuleType("lakelogic.core.models")

    class FakeDataContract:
        @staticmethod
        def model_json_schema():
            return {"properties": {"version": {}, "tier": {}}, "$defs": {}}

    fake_models.DataContract = FakeDataContract
    monkeypatch.setitem(sys.modules, "lakelogic.core.models", fake_models)

    schema = sa.contract_schema()
    assert schema["title"] == "LakeLogic Data Contract"
    assert schema["properties"]["tier"]["enum"][0] == "bronze"
    assert schema["$defs"]["Server"]["properties"]["type"]["enum"]
    assert schema["$defs"]["Materialization"]["properties"]["strategy"]["enum"]

    schema_json = sa.contract_schema_json(indent=4)
    parsed = json.loads(schema_json)
    assert parsed["x-lakelogic-version"] == "1"


def test_set_nested_enum_creates_missing_nodes():
    schema = {}
    sa._set_nested_enum(schema, ["$defs", "Custom", "properties", "status"], ["a", "b"], "desc")
    assert schema["$defs"]["Custom"]["properties"]["status"]["enum"] == ["a", "b"]
    assert schema["$defs"]["Custom"]["properties"]["status"]["description"] == "desc"
