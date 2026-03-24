"""
lakelogic.core.describe_columns
--------------------------------
AI-powered column description generator.

Uses the existing LLM infrastructure (OpenAI, Anthropic, Google, Ollama, local)
to generate human-readable descriptions for inferred schema columns.

Usage::

    from lakelogic.core.describe_columns import describe_columns

    descriptions = describe_columns(
        fields=[
            {"name": "customer_id", "type": "integer", "examples": ["1001"]},
            {"name": "email", "type": "string", "pii": True},
        ],
        provider="openai",
        model="gpt-4o-mini",
        domain="marketing",
        system="crm",
    )
    # → {"customer_id": "Unique identifier ...", "email": "Primary email ..."}
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

from loguru import logger


# ── Prompt Template ───────────────────────────────────────────────────────────

_SYSTEM_PROMPT = (
    "You are a data dictionary assistant for a data lakehouse platform. "
    "Generate concise, business-meaningful descriptions for dataset columns. "
    "Each description should be 1-2 sentences. Focus on what the column "
    "represents from a business perspective, not its technical type."
)

_USER_PROMPT_TEMPLATE = """\
Generate descriptions for the following columns in a {layer} dataset.
{context_line}

Columns:
{columns_block}

Respond ONLY with a JSON object mapping column names to descriptions.
Example: {{"order_id": "Unique identifier for each order placed by a customer."}}
"""


def _build_prompt(
    fields: List[Dict[str, Any]],
    *,
    domain: str = "",
    system: str = "",
    layer: str = "bronze",
) -> str:
    """Build the user prompt from field metadata."""
    context_parts = []
    if domain:
        context_parts.append(f"Domain: {domain}")
    if system:
        context_parts.append(f"Source system: {system}")
    context_line = ", ".join(context_parts) if context_parts else ""
    if context_line:
        context_line = f"Context: {context_line}"

    lines = []
    for f in fields:
        parts = [f"- {f['name']} ({f.get('type', 'string')})"]
        if f.get("pii"):
            parts.append("[PII]")
        if f.get("classification"):
            parts.append(f"[{f['classification']}]")
        if f.get("examples"):
            parts.append(f"samples: {f['examples'][:3]}")
        lines.append(" ".join(parts))

    return _USER_PROMPT_TEMPLATE.format(
        layer=layer,
        context_line=context_line,
        columns_block="\n".join(lines),
    )


# ── Provider Calls ────────────────────────────────────────────────────────────


def _call_openai(prompt: str, model: str) -> Dict[str, str]:
    """Call OpenAI and return parsed JSON."""
    try:
        from openai import OpenAI
    except ImportError:
        raise ImportError(
            "openai is required. Install with: pip install lakelogic[llm]"
        )
    client = OpenAI()
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
        max_tokens=2000,
        response_format={"type": "json_object"},
    )
    raw = response.choices[0].message.content or "{}"
    return json.loads(raw)


def _call_anthropic(prompt: str, model: str) -> Dict[str, str]:
    """Call Anthropic and return parsed JSON."""
    try:
        from anthropic import Anthropic
    except ImportError:
        raise ImportError(
            "anthropic is required. Install with: pip install lakelogic[llm]"
        )
    client = Anthropic()
    response = client.messages.create(
        model=model,
        max_tokens=2000,
        system=_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = response.content[0].text if response.content else "{}"
    return json.loads(raw)


def _call_ollama(prompt: str, model: str) -> Dict[str, str]:
    """Call local Ollama server and return parsed JSON."""
    import httpx

    base_url = os.environ.get("OLLAMA_BASE_URL", "http://localhost:11434")
    response = httpx.post(
        f"{base_url}/api/generate",
        json={
            "model": model,
            "prompt": f"{_SYSTEM_PROMPT}\n\n{prompt}",
            "stream": False,
            "format": "json",
        },
        timeout=120,
    )
    response.raise_for_status()
    raw = response.json().get("response", "{}")
    return json.loads(raw)


def _call_google(prompt: str, model: str) -> Dict[str, str]:
    """Call Google Generative AI and return parsed JSON."""
    try:
        import google.generativeai as genai
    except ImportError:
        raise ImportError(
            "google-generativeai is required. Install with: pip install google-generativeai"
        )
    gen_model = genai.GenerativeModel(model)
    response = gen_model.generate_content(
        f"{_SYSTEM_PROMPT}\n\n{prompt}",
        generation_config=genai.GenerationConfig(
            temperature=0.2,
            max_output_tokens=2000,
            response_mime_type="application/json",
        ),
    )
    raw = response.text or "{}"
    return json.loads(raw)


_PROVIDER_FN = {
    "openai": _call_openai,
    "azure_openai": _call_openai,
    "anthropic": _call_anthropic,
    "ollama": _call_ollama,
    "google": _call_google,
}


# ── Public API ────────────────────────────────────────────────────────────────


def describe_columns(
    fields: List[Dict[str, Any]],
    *,
    provider: str = "openai",
    model: Optional[str] = None,
    domain: str = "",
    system: str = "",
    layer: str = "bronze",
) -> Dict[str, str]:
    """
    Generate AI-powered descriptions for a list of schema columns.

    Sends all column metadata (name, type, PII flag, sample values) to the
    configured LLM provider in a single batch call and returns a mapping
    of ``{column_name: description}``.

    Parameters
    ----------
    fields : list of dict
        Field dicts as produced by ``ContractInferrer._infer_fields()``.
        Each dict should have at least ``name`` and ``type`` keys.
    provider : str
        LLM provider name. Options: ``openai``, ``anthropic``, ``ollama``,
        ``google``, ``azure_openai``. Default ``openai``.
    model : str, optional
        Model name. Defaults per provider:
        ``gpt-4o-mini`` (OpenAI), ``claude-sonnet-4-20250514`` (Anthropic),
        ``llama3.1`` (Ollama), ``gemini-2.0-flash`` (Google).
    domain : str
        Business domain context (e.g. ``"marketing"``).
    system : str
        Source system context (e.g. ``"google_analytics"``).
    layer : str
        Data layer (``"bronze"``, ``"silver"``, ``"gold"``).

    Returns
    -------
    dict
        ``{column_name: description}`` mapping. Empty dict on failure.
    """
    if not fields:
        return {}

    # Resolve defaults
    _DEFAULT_MODELS = {
        "openai": "gpt-4o-mini",
        "azure_openai": "gpt-4o-mini",
        "anthropic": "claude-sonnet-4-20250514",
        "ollama": "llama3.1",
        "google": "gemini-2.0-flash",
    }
    model = model or _DEFAULT_MODELS.get(provider, "gpt-4o-mini")

    # Build prompt
    prompt = _build_prompt(fields, domain=domain, system=system, layer=layer)

    # Call provider
    call_fn = _PROVIDER_FN.get(provider)
    if call_fn is None:
        logger.warning(f"Unknown AI provider {provider!r} for column descriptions")
        return {}

    try:
        logger.info(
            f"Generating AI column descriptions via {provider}/{model} "
            f"for {len(fields)} fields"
        )
        result = call_fn(prompt, model)

        # Validate: ensure keys match field names
        field_names = {f["name"] for f in fields}
        return {k: v for k, v in result.items() if k in field_names}

    except Exception as exc:
        logger.warning(f"AI column description failed ({provider}/{model}): {exc}")
        return {}
