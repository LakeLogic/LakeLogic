from __future__ import annotations

import sys
import types

import pytest

from lakelogic.ai import provider as ai_provider


class _Usage:
    def __init__(self, prompt_tokens, completion_tokens):
        self.prompt_tokens = prompt_tokens
        self.completion_tokens = completion_tokens


def test_llm_response_as_json_strips_code_fences():
    response = ai_provider.LLMResponse('```json\n{"value": 1}\n```')

    assert response.as_json() == {"value": 1}


def test_openai_client_chat_supports_openai_and_azure(monkeypatch):
    captured = {}

    class FakeCompletions:
        def create(self, **kwargs):
            captured["create_kwargs"] = kwargs
            return types.SimpleNamespace(
                choices=[types.SimpleNamespace(message=types.SimpleNamespace(content='{"ok": true}'))],
                usage=_Usage(3, 5),
            )

    class FakeOpenAIClient:
        def __init__(self, **kwargs):
            captured.setdefault("openai_init", []).append(kwargs)
            self.chat = types.SimpleNamespace(completions=FakeCompletions())

    fake_openai = types.SimpleNamespace(OpenAI=FakeOpenAIClient, AzureOpenAI=FakeOpenAIClient)
    monkeypatch.setitem(sys.modules, "openai", fake_openai)

    client = ai_provider._OpenAIClient(api_key="key", model="model-a", base_url="http://example")
    response = client.chat([{"role": "user", "content": "hello"}], json_mode=True)

    azure_client = ai_provider._OpenAIClient(
        api_key="key",
        model="model-b",
        azure_endpoint="https://azure.example",
        azure_api_version="2024-01-01",
    )

    assert response.text == '{"ok": true}'
    assert response.usage == {"prompt_tokens": 3, "completion_tokens": 5}
    assert captured["create_kwargs"]["response_format"] == {"type": "json_object"}
    assert captured["openai_init"][0] == {"api_key": "key", "base_url": "http://example"}
    assert captured["openai_init"][1] == {
        "api_key": "key",
        "azure_endpoint": "https://azure.example",
        "api_version": "2024-01-01",
    }
    assert azure_client.model == "model-b"


def test_openai_client_raises_when_package_missing(monkeypatch):
    monkeypatch.setitem(sys.modules, "openai", None)

    with pytest.raises(ImportError):
        ai_provider._OpenAIClient()


def test_anthropic_client_chat(monkeypatch):
    class FakeAnthropic:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.messages = types.SimpleNamespace(create=self.create)

        def create(self, **kwargs):
            return types.SimpleNamespace(
                content=[types.SimpleNamespace(text="done")],
                usage=types.SimpleNamespace(input_tokens=7, output_tokens=11),
            )

    monkeypatch.setitem(sys.modules, "anthropic", types.SimpleNamespace(Anthropic=FakeAnthropic))

    client = ai_provider._AnthropicClient(api_key="secret", model="claude")
    response = client.chat(
        [
            {"role": "system", "content": "rules"},
            {"role": "user", "content": "hello"},
        ]
    )

    assert response.text == "done"
    assert response.usage == {"prompt_tokens": 7, "completion_tokens": 11}


def test_anthropic_client_raises_when_package_missing(monkeypatch):
    monkeypatch.setitem(sys.modules, "anthropic", None)

    with pytest.raises(ImportError):
        ai_provider._AnthropicClient()


def test_ollama_client_chat(monkeypatch):
    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "message": {"content": "result"},
                "prompt_eval_count": 2,
                "eval_count": 4,
            }

    def fake_post(url, json, timeout):
        assert url == "http://ollama.test/api/chat"
        assert json["format"] == "json"
        return FakeResponse()

    monkeypatch.setitem(sys.modules, "httpx", types.SimpleNamespace(post=fake_post))

    client = ai_provider._OllamaClient(model="llama", base_url="http://ollama.test")
    response = client.chat([{"role": "user", "content": "hi"}], json_mode=True)

    assert response.text == "result"
    assert response.usage == {"prompt_tokens": 2, "completion_tokens": 4}


def test_gemini_client_requires_api_key(monkeypatch):
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setitem(
        sys.modules, "google", types.SimpleNamespace(genai=types.SimpleNamespace(Client=lambda api_key: None))
    )

    with pytest.raises(ValueError):
        ai_provider._GeminiClient()


def test_gemini_client_raises_when_package_missing(monkeypatch):
    monkeypatch.setitem(sys.modules, "google", None)

    with pytest.raises(ImportError):
        ai_provider._GeminiClient(api_key="x")


def test_gemini_client_chat(monkeypatch):
    class FakePart:
        @staticmethod
        def from_text(text):
            return {"text": text}

    class FakeContent:
        def __init__(self, role, parts):
            self.role = role
            self.parts = parts

    class FakeConfig:
        def __init__(self, **kwargs):
            self.temperature = kwargs["temperature"]
            self.max_output_tokens = kwargs["max_output_tokens"]
            self.system_instruction = kwargs["system_instruction"]
            self.response_mime_type = None

    class FakeModels:
        def generate_content(self, **kwargs):
            return types.SimpleNamespace(
                text="gemini",
                usage_metadata=types.SimpleNamespace(prompt_token_count=13, candidates_token_count=17),
            )

    class FakeClient:
        def __init__(self, api_key):
            self.api_key = api_key
            self.models = FakeModels()

    fake_types = types.SimpleNamespace(Content=FakeContent, Part=FakePart, GenerateContentConfig=FakeConfig)
    fake_genai = types.SimpleNamespace(Client=FakeClient, types=fake_types)
    monkeypatch.setitem(sys.modules, "google", types.SimpleNamespace(genai=fake_genai))
    monkeypatch.setitem(sys.modules, "google.genai", fake_genai)

    client = ai_provider._GeminiClient(api_key="g-key", model="gemini-pro")
    response = client.chat(
        [
            {"role": "system", "content": "rules"},
            {"role": "user", "content": "hello"},
            {"role": "assistant", "content": "prior"},
        ],
        json_mode=True,
    )

    assert response.text == "gemini"
    assert response.usage == {"prompt_tokens": 13, "completion_tokens": 17}


def test_local_client_chat(monkeypatch):
    def fake_pipe(prompt, max_new_tokens, return_full_text):
        assert "System: rules" in prompt
        assert "hello" in prompt
        assert max_new_tokens == 22
        assert return_full_text is False
        return [{"generated_text": "local-output"}]

    fake_registry = types.SimpleNamespace(load_model=lambda task, model_override=None: fake_pipe)
    monkeypatch.setitem(sys.modules, "lakelogic.engines.model_registry", fake_registry)

    client = ai_provider._LocalClient(model="phi")
    response = client.chat(
        [{"role": "system", "content": "rules"}, {"role": "user", "content": "hello"}],
        max_tokens=22,
    )

    assert response.text == "local-output"
    assert response.usage == {}


def test_get_llm_client_dispatches_and_validates(monkeypatch):
    monkeypatch.setattr(ai_provider, "_OpenAIClient", lambda **kwargs: ("openai", kwargs))
    monkeypatch.setattr(ai_provider, "_AnthropicClient", lambda **kwargs: ("anthropic", kwargs))
    monkeypatch.setattr(ai_provider, "_OllamaClient", lambda **kwargs: ("ollama", kwargs))
    monkeypatch.setattr(ai_provider, "_GeminiClient", lambda **kwargs: ("gemini", kwargs))
    monkeypatch.setattr(ai_provider, "_LocalClient", lambda **kwargs: ("local", kwargs))
    monkeypatch.setenv("LAKELOGIC_AI_PROVIDER", "azure")
    monkeypatch.setenv("LAKELOGIC_AI_MODEL", "env-model")
    monkeypatch.setenv("AZURE_OPENAI_ENDPOINT", "https://azure.env")
    monkeypatch.setenv("OLLAMA_BASE_URL", "http://ollama.env")

    assert ai_provider.get_llm_client()[0] == "openai"
    assert ai_provider.get_llm_client(provider="azure")[1]["azure_endpoint"] == "https://azure.env"
    assert ai_provider.get_llm_client(provider="anthropic")[0] == "anthropic"
    assert ai_provider.get_llm_client(provider="ollama")[1]["base_url"] == "http://ollama.env"
    assert ai_provider.get_llm_client(provider="google")[0] == "gemini"
    assert ai_provider.get_llm_client(provider="local")[0] == "local"
    monkeypatch.setenv("LAKELOGIC_AI_PROVIDER", "")
    monkeypatch.setenv("LAKELOGIC_AI_MODEL", "")
    assert ai_provider.get_llm_client()[1]["model"] == "gpt-4o-mini"
    with pytest.raises(ValueError):
        ai_provider.get_llm_client(provider="unknown")
