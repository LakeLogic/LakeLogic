"""
lakelogic.ai.provider
---------------------
Provider-agnostic LLM client for contract enrichment.

Supports:
- OpenAI  (``openai``)     — requires ``OPENAI_API_KEY``
- Azure OpenAI (``azure``) — requires ``AZURE_OPENAI_ENDPOINT`` + ``AZURE_OPENAI_API_KEY``
- Anthropic (``anthropic``) — requires ``ANTHROPIC_API_KEY``
- Ollama  (``ollama``)     — local, no key required (default: http://localhost:11434)

Configuration is resolved from:
1. Explicit kwargs passed to ``get_llm_client()``
2. Environment variables (``LAKELOGIC_AI_PROVIDER``, ``LAKELOGIC_AI_MODEL``)
3. Sensible defaults (OpenAI / gpt-4o-mini)
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

from loguru import logger


# ---------------------------------------------------------------------------
# Unified response type
# ---------------------------------------------------------------------------


class LLMResponse:
    """Wrapper around an LLM completion result."""

    def __init__(self, text: str, usage: Optional[Dict[str, int]] = None) -> None:
        self.text = text
        self.usage = usage or {}

    def as_json(self) -> Any:
        """Parse the response text as JSON, stripping markdown fences."""
        cleaned = self.text.strip()
        # Strip ```json ... ``` wrappers
        if cleaned.startswith("```"):
            first_nl = cleaned.index("\n") if "\n" in cleaned else 3
            cleaned = cleaned[first_nl + 1 :]
            if cleaned.endswith("```"):
                cleaned = cleaned[:-3]
        return json.loads(cleaned.strip())


# ---------------------------------------------------------------------------
# Provider clients
# ---------------------------------------------------------------------------


class _OpenAIClient:
    """OpenAI / Azure OpenAI chat client."""

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        model: str = "gpt-4o-mini",
        base_url: Optional[str] = None,
        azure_endpoint: Optional[str] = None,
        azure_api_version: str = "2024-06-01",
    ) -> None:
        try:
            import openai
        except ImportError as exc:
            raise ImportError(
                "OpenAI provider requires the 'openai' package. Install with: pip install openai"
            ) from exc

        if azure_endpoint:
            self._client = openai.AzureOpenAI(
                api_key=api_key or os.getenv("AZURE_OPENAI_API_KEY"),
                azure_endpoint=azure_endpoint,
                api_version=azure_api_version,
            )
        else:
            self._client = openai.OpenAI(
                api_key=api_key or os.getenv("OPENAI_API_KEY"),
                base_url=base_url,
            )
        self.model = model

    def chat(self, messages: List[Dict[str, str]], **kwargs) -> LLMResponse:
        resp = self._client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=kwargs.get("temperature", 0.2),
            response_format={"type": "json_object"} if kwargs.get("json_mode") else None,
        )
        choice = resp.choices[0]
        usage = {}
        if resp.usage:
            usage = {
                "prompt_tokens": resp.usage.prompt_tokens,
                "completion_tokens": resp.usage.completion_tokens,
            }
        return LLMResponse(text=choice.message.content or "", usage=usage)


class _AnthropicClient:
    """Anthropic Claude client."""

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        model: str = "claude-sonnet-4-20250514",
    ) -> None:
        try:
            import anthropic
        except ImportError as exc:
            raise ImportError(
                "Anthropic provider requires the 'anthropic' package. Install with: pip install anthropic"
            ) from exc

        self._client = anthropic.Anthropic(
            api_key=api_key or os.getenv("ANTHROPIC_API_KEY"),
        )
        self.model = model

    def chat(self, messages: List[Dict[str, str]], **kwargs) -> LLMResponse:
        # Anthropic uses system param separately
        system_msg = ""
        chat_msgs = []
        for m in messages:
            if m["role"] == "system":
                system_msg = m["content"]
            else:
                chat_msgs.append(m)

        resp = self._client.messages.create(
            model=self.model,
            system=system_msg,
            messages=chat_msgs,
            max_tokens=kwargs.get("max_tokens", 4096),
            temperature=kwargs.get("temperature", 0.2),
        )
        text = resp.content[0].text if resp.content else ""
        usage = {
            "prompt_tokens": resp.usage.input_tokens if resp.usage else 0,
            "completion_tokens": resp.usage.output_tokens if resp.usage else 0,
        }
        return LLMResponse(text=text, usage=usage)


class _OllamaClient:
    """Ollama local LLM client (no API key required)."""

    def __init__(
        self,
        *,
        model: str = "llama3.1",
        base_url: str = "http://localhost:11434",
    ) -> None:
        self.model = model
        self.base_url = base_url.rstrip("/")

    def chat(self, messages: List[Dict[str, str]], **kwargs) -> LLMResponse:
        import httpx

        resp = httpx.post(
            f"{self.base_url}/api/chat",
            json={
                "model": self.model,
                "messages": messages,
                "stream": False,
                "options": {"temperature": kwargs.get("temperature", 0.2)},
                "format": "json" if kwargs.get("json_mode") else "",
            },
            timeout=120.0,
        )
        resp.raise_for_status()
        data = resp.json()
        return LLMResponse(
            text=data.get("message", {}).get("content", ""),
            usage={
                "prompt_tokens": data.get("prompt_eval_count", 0),
                "completion_tokens": data.get("eval_count", 0),
            },
        )


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def get_llm_client(
    provider: Optional[str] = None,
    model: Optional[str] = None,
    api_key: Optional[str] = None,
    **kwargs,
):
    """
    Create an LLM client for the given provider.

    Args:
        provider: ``openai`` | ``azure`` | ``anthropic`` | ``ollama``.
                  Defaults to env ``LAKELOGIC_AI_PROVIDER`` or ``openai``.
        model: Model name. Defaults to env ``LAKELOGIC_AI_MODEL`` or
               provider-specific default.
        api_key: API key override.
        **kwargs: Provider-specific options.

    Returns:
        Client instance with a ``.chat(messages)`` method.
    """
    provider = (provider or os.getenv("LAKELOGIC_AI_PROVIDER", "") or "openai").lower().strip()

    model = model or os.getenv("LAKELOGIC_AI_MODEL", "")

    logger.info(f"AI provider: {provider} | model: {model or '(default)'}")

    if provider in ("openai", ""):
        return _OpenAIClient(
            api_key=api_key,
            model=model or "gpt-4o-mini",
            base_url=kwargs.get("base_url"),
        )

    if provider == "azure":
        return _OpenAIClient(
            api_key=api_key,
            model=model or "gpt-4o-mini",
            azure_endpoint=kwargs.get("azure_endpoint") or os.getenv("AZURE_OPENAI_ENDPOINT"),
            azure_api_version=kwargs.get("azure_api_version", "2024-06-01"),
        )

    if provider == "anthropic":
        return _AnthropicClient(
            api_key=api_key,
            model=model or "claude-sonnet-4-20250514",
        )

    if provider == "ollama":
        return _OllamaClient(
            model=model or "llama3.1",
            base_url=kwargs.get("base_url") or os.getenv("OLLAMA_BASE_URL", "http://localhost:11434"),
        )

    raise ValueError(f"Unknown AI provider: {provider!r}. Supported: openai, azure, anthropic, ollama")
