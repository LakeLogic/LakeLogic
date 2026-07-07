from __future__ import annotations

import json

from lakelogic.ai import data_generator as dg
from lakelogic.ai import edge_case_generator as ecg


class DummyResponse:
    def __init__(self, text, parsed=None, usage=None, fail_parse=False):
        self.text = text
        self._parsed = parsed
        self.usage = usage or {}
        self._fail_parse = fail_parse

    def as_json(self):
        if self._fail_parse:
            raise json.JSONDecodeError("bad", self.text, 0)
        return self._parsed


class DummyClient:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def chat(self, **kwargs):
        self.calls.append(kwargs)
        return self.response


def test_data_generator_builds_prompt_and_coerces_values(monkeypatch):
    fields = [
        {"name": "id", "type": "integer", "required": True, "description": "identifier", "min": 1},
        {"name": "active", "type": "boolean", "accepted_values": [True, False]},
        {"name": "country", "type": "string", "foreign_key": "dim_country.id"},
    ]
    prompt = dg._build_user_prompt(fields, {"row_rules": [{"sql": "id > 0"}]}, "orders", "Keep it UK-focused")

    assert "Dataset: orders" in prompt
    assert "foreign_key: dim_country.id" in prompt
    assert "Generate realistic sample values for EVERY field listed above." in prompt
    assert dg._coerce_value("7", "integer") == 7
    assert dg._coerce_value("3.5", "double") == 3.5
    assert dg._coerce_value("not-an-int", "integer") == "not-an-int"
    assert dg._coerce_value("not-a-float", "double") == "not-a-float"
    assert dg._coerce_value("yes", "boolean") is True
    assert dg._coerce_value(True, "boolean") is True
    assert dg._coerce_value(0, "boolean") is False
    assert dg._coerce_value(12, "string") == "12"


def test_data_generator_prompt_covers_all_optional_field_parts():
    prompt = dg._build_user_prompt(
        [
            {
                "name": "price",
                "type": "decimal",
                "required": False,
                "min": 0,
                "max": 10,
                "accepted_values": [1, 2],
                "foreign_key": "dim.id",
            }
        ],
        None,
        "",
        "",
    )

    assert ", accepted_values: [1, 2]" in prompt
    assert ", min: 0" in prompt
    assert ", max: 10" in prompt
    assert ", foreign_key: dim.id" in prompt


def test_data_generator_repairs_json_and_extracts_flat_or_nested(monkeypatch):
    fields = [{"name": "id", "type": "integer"}, {"name": "name", "type": "string"}]
    nested_response = DummyResponse(
        text="irrelevant",
        parsed={"fields": {"id": {"values": ["1", None]}, "name": ["alice"]}},
        usage={"prompt_tokens": 2, "completion_tokens": 3},
    )
    nested_client = DummyClient(nested_response)
    monkeypatch.setattr("lakelogic.ai.provider.get_llm_client", lambda **kwargs: nested_client)

    nested = dg.generate_realistic_pools(fields, provider="openai")
    assert nested == {"id": [1], "name": ["alice"]}

    broken = "```json\n{'id': [1, 2,], // comment\n 'name': ['bob']}\n```"
    repaired = dg._repair_llm_json(broken)
    assert repaired == {"id": [1, 2], "name": ["bob"]}

    truncated = dg._repair_llm_json('{"id": [1, 2]')
    assert truncated == {"id": [1, 2]}
    assert dg._repair_llm_json('{"id": [1,,]}') is None
    assert dg._repair_llm_json("not-json") is None
    assert dg._repair_llm_json("prefix {'bad': [1,]} suffix") == {"bad": [1]}
    assert dg._repair_llm_json("prefix {bad: nope} suffix") is None


def test_data_generator_handles_quality_rule_variants_and_flat_filtering(monkeypatch):
    prompt = dg._build_user_prompt(
        [{"name": "id", "type": "integer"}],
        {"row_rules": [{"not_null": "id"}, {"accepted_values": [1, 2]}, "freeform"]},
        "",
        "",
    )
    assert "NOT NULL: id" in prompt
    assert "ACCEPTED VALUES: [1, 2]" in prompt
    assert "- freeform" in prompt

    response = DummyResponse(
        text="ignored",
        parsed={"id": ["1", None], "ignored": ["x"], "name": "bad"},
    )
    monkeypatch.setattr("lakelogic.ai.provider.get_llm_client", lambda **kwargs: DummyClient(response))

    assert dg.generate_realistic_pools([{"name": "id", "type": "integer"}]) == {"id": [1]}


def test_data_generator_ignores_unknown_and_malformed_nested_values(monkeypatch):
    response = DummyResponse(
        text="ignored",
        parsed={
            "fields": {
                "unknown": {"values": ["x"]},
                "id": {"values": "bad"},
                "name": {"values": [None]},
            }
        },
    )
    monkeypatch.setattr("lakelogic.ai.provider.get_llm_client", lambda **kwargs: DummyClient(response))

    assert dg.generate_realistic_pools([{"name": "id", "type": "integer"}, {"name": "name", "type": "string"}]) == {}


def test_data_generator_handles_salvage_and_failure(monkeypatch):
    fields = [{"name": "id", "type": "integer"}]
    salvage_response = DummyResponse(text='{"id": ["9"]', fail_parse=True)
    salvage_client = DummyClient(salvage_response)
    monkeypatch.setattr("lakelogic.ai.provider.get_llm_client", lambda **kwargs: salvage_client)

    assert dg.generate_realistic_pools(fields) == {"id": [9]}

    failing_response = DummyResponse(text="not-json", fail_parse=True)
    failing_client = DummyClient(failing_response)
    monkeypatch.setattr("lakelogic.ai.provider.get_llm_client", lambda **kwargs: failing_client)

    assert dg.generate_realistic_pools(fields) == {}
    assert dg.generate_realistic_pools([]) == {}


def test_edge_case_generator_builds_prompt_and_coerces_values(monkeypatch):
    fields = [
        {"name": "amount", "type": "integer", "required": True, "min": 0, "max": 9, "description": "Amount value"},
        {"name": "status", "type": "string", "accepted_values": ["new", "done"]},
    ]
    prompt = ecg._build_user_prompt(
        fields,
        {
            "row_rules": [{"sql": "amount > 0"}, {"accepted_values": ["new", "done"]}],
            "dataset_rules": [{"unique": "amount"}],
        },
        "orders",
        "Break accepted values",
    )

    assert "Dataset: orders" in prompt
    assert "description: Amount value" in prompt
    assert "SQL: amount > 0" in prompt
    assert "ACCEPTED VALUES" in prompt
    assert "UNIQUE: amount" in prompt
    assert "Break accepted values" in prompt
    assert ecg._coerce_value(None, "integer") is None
    assert ecg._coerce_value("4", "integer") == 4
    assert ecg._coerce_value("oops", "integer") == "oops"
    assert ecg._coerce_value("2.5", "float") == 2.5
    assert ecg._coerce_value("oops", "float") == "oops"
    assert ecg._coerce_value("false", "boolean") == "false"


def test_edge_case_generator_prompt_covers_min_max_and_accepted_values():
    prompt = ecg._build_user_prompt(
        [{"name": "price", "type": "decimal", "required": False, "min": 0, "max": 10, "accepted_values": [1, 2]}],
        None,
        "",
        "",
    )

    assert ", accepted_values: [1, 2]" in prompt
    assert ", min: 0" in prompt
    assert ", max: 10" in prompt


def test_edge_case_generator_handles_prompt_variants_and_flat_filtering(monkeypatch):
    prompt = ecg._build_user_prompt(
        [{"name": "id", "type": "integer"}],
        {"row_rules": [{"not_null": "id"}, "freeform"], "dataset_rules": [{"name": "skip"}]},
        "",
        "",
    )
    assert "NOT NULL: id" in prompt
    assert "- freeform" in prompt
    assert "UNIQUE:" not in prompt

    response = DummyResponse(
        text="ignored",
        parsed={"id": ["3"], "ignored": ["x"], "name": "bad"},
    )
    monkeypatch.setattr("lakelogic.ai.provider.get_llm_client", lambda **kwargs: DummyClient(response))

    assert ecg.generate_edge_cases([{"name": "id", "type": "integer"}]) == {"id": [3]}


def test_edge_case_generator_ignores_unknown_and_malformed_nested_values(monkeypatch):
    response = DummyResponse(
        text="ignored",
        parsed={
            "fields": {
                "unknown": {"edge_cases": ["x"]},
                "id": {"edge_cases": "bad"},
            }
        },
    )
    monkeypatch.setattr("lakelogic.ai.provider.get_llm_client", lambda **kwargs: DummyClient(response))

    assert ecg.generate_edge_cases([{"name": "id", "type": "integer"}]) == {}


def test_edge_case_generator_handles_nested_flat_salvage_and_failure(monkeypatch):
    fields = [{"name": "amount", "type": "integer"}, {"name": "status", "type": "string"}]
    nested_response = DummyResponse(
        text="ignored",
        parsed={"fields": {"amount": {"edge_cases": ["0", "-1"]}, "status": ["deleted"]}},
        usage={"prompt_tokens": 5, "completion_tokens": 6},
    )
    monkeypatch.setattr("lakelogic.ai.provider.get_llm_client", lambda **kwargs: DummyClient(nested_response))
    assert ecg.generate_edge_cases(fields) == {"amount": [0, -1], "status": ["deleted"]}

    flat_response = DummyResponse(text="ignored", parsed={"amount": ["3"], "status": ["bad"]})
    monkeypatch.setattr("lakelogic.ai.provider.get_llm_client", lambda **kwargs: DummyClient(flat_response))
    assert ecg.generate_edge_cases(fields) == {"amount": [3], "status": ["bad"]}

    salvage_response = DummyResponse(text='{"amount": ["7"]', fail_parse=True)
    monkeypatch.setattr("lakelogic.ai.provider.get_llm_client", lambda **kwargs: DummyClient(salvage_response))
    assert ecg.generate_edge_cases(fields) == {"amount": [7]}

    failing_response = DummyResponse(text="not-json", fail_parse=True)
    monkeypatch.setattr("lakelogic.ai.provider.get_llm_client", lambda **kwargs: DummyClient(failing_response))
    assert ecg.generate_edge_cases(fields) == {}
    assert ecg.generate_edge_cases([]) == {}
