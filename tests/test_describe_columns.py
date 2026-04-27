from __future__ import annotations

import builtins
import sys
import types

import pytest

from lakelogic.core import describe_columns as dc


def test_build_prompt_includes_context_and_field_metadata():
    prompt = dc._build_prompt(
        [
            {
                "name": "customer_id",
                "type": "integer",
                "pii": True,
                "classification": "restricted",
                "examples": ["1001", "1002", "1003", "1004"],
            }
        ],
        domain="sales",
        system="crm",
        layer="silver",
    )

    assert "Generate descriptions for the following columns in a silver dataset." in prompt
    assert "Context: Domain: sales, Source system: crm" in prompt
    assert "[PII]" in prompt
    assert "[restricted]" in prompt
    assert "samples: ['1001', '1002', '1003']" in prompt


def test_provider_call_helpers(monkeypatch):
    class FakeOpenAI:
        def __init__(self):
            self.chat = types.SimpleNamespace(
                completions=types.SimpleNamespace(
                    create=lambda **kwargs: types.SimpleNamespace(
                        choices=[types.SimpleNamespace(message=types.SimpleNamespace(content='{"id": "desc"}'))]
                    )
                )
            )

    class FakeAnthropic:
        def __init__(self):
            self.messages = types.SimpleNamespace(
                create=lambda **kwargs: types.SimpleNamespace(content=[types.SimpleNamespace(text='{"id": "desc"}')])
            )

    class FakeHttpxResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"response": '{"id": "desc"}'}

    class FakeGenerationConfig:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

    class FakeGenerativeModel:
        def __init__(self, model):
            self.model = model

        def generate_content(self, prompt, generation_config):
            return types.SimpleNamespace(text='{"id": "desc"}')

    fake_google = types.SimpleNamespace(GenerativeModel=FakeGenerativeModel, GenerationConfig=FakeGenerationConfig)

    monkeypatch.setitem(sys.modules, "openai", types.SimpleNamespace(OpenAI=FakeOpenAI))
    monkeypatch.setitem(sys.modules, "anthropic", types.SimpleNamespace(Anthropic=FakeAnthropic))
    monkeypatch.setitem(sys.modules, "httpx", types.SimpleNamespace(post=lambda *args, **kwargs: FakeHttpxResponse()))
    monkeypatch.setitem(sys.modules, "google.generativeai", fake_google)
    monkeypatch.setenv("OLLAMA_BASE_URL", "http://ollama.test")

    assert dc._call_openai("prompt", "model") == {"id": "desc"}
    assert dc._call_anthropic("prompt", "model") == {"id": "desc"}
    assert dc._call_ollama("prompt", "model") == {"id": "desc"}
    assert dc._call_google("prompt", "model") == {"id": "desc"}


def test_provider_call_helpers_raise_import_errors(monkeypatch):
    original_import = builtins.__import__

    def blocked_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name in {"openai", "anthropic", "google.generativeai"}:
            raise ImportError("missing")
        return original_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", blocked_import)

    with pytest.raises(ImportError):
        dc._call_openai("prompt", "model")
    with pytest.raises(ImportError):
        dc._call_anthropic("prompt", "model")
    with pytest.raises(ImportError):
        dc._call_google("prompt", "model")


def test_describe_columns_filters_results_and_handles_failures(monkeypatch):
    fields = [{"name": "id", "type": "integer"}, {"name": "name", "type": "string"}]
    infos = []
    warnings = []
    monkeypatch.setattr(dc.logger, "info", infos.append)
    monkeypatch.setattr(dc.logger, "warning", warnings.append)
    monkeypatch.setitem(dc._PROVIDER_FN, "openai", lambda prompt, model: {"id": "Identifier", "extra": "skip"})
    monkeypatch.setitem(dc._PROVIDER_FN, "broken", lambda prompt, model: (_ for _ in ()).throw(ValueError("bad llm")))

    assert dc.describe_columns([]) == {}
    assert dc.describe_columns(fields, provider="missing") == {}
    assert dc.describe_columns(fields, provider="openai") == {"id": "Identifier"}
    assert any("Generating AI column descriptions" in message for message in infos)
    assert dc.describe_columns(fields, provider="broken") == {}
    assert any("Unknown AI provider 'missing'" in message for message in warnings)
    assert any("AI column description failed (broken/gpt-4o-mini): bad llm" in message for message in warnings)
