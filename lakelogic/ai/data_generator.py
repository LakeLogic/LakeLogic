"""
lakelogic.ai.data_generator
----------------------------
LLM-powered realistic data generation for stress-testing data contracts.

Given a contract schema, asks an LLM to generate contextually realistic sample
values for each field.  These values are used as sampling pools by
``DataGenerator.generate()`` to produce rows that look like real production
data — not random strings or generic Faker output.

For example, a GA4 events contract would produce:
- event_name: ["page_view", "scroll", "click", "purchase", "add_to_cart", ...]
- device_category: ["desktop", "mobile", "tablet"]
- browser: ["Chrome", "Safari", "Firefox", "Edge", ...]
- session_duration: [45, 120, 300, 5, 0, 1800, ...]

Usage::

    from lakelogic.ai.data_generator import generate_realistic_pools

    pools = generate_realistic_pools(
        fields, quality_rules,
        dataset_name="Google Analytics Events",
        provider="ollama",
    )
    # → {"event_name": ["page_view", "scroll", ...], "browser": ["Chrome", ...], ...}
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from loguru import logger


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You generate realistic sample data for database schemas.

Return a JSON object mapping field names to value arrays:

{"field_name": ["val1", "val2", ...], "other_field": [1, 2, 3], ...}

HARD RULES:
- EXACTLY 1-2 values per field. NO MORE.
- Values must match the field's data type.
- String values must be SHORT (under 50 chars each).
- NO edge cases, NO nulls, NO intentionally bad data.
- NO rationale, NO explanations, NO markdown.
- For IDs: use short realistic identifiers (not UUIDs).
- For dates: use ISO-8601 strings from the last 90 days.
- For timestamps (*_at, *_time): include time component.
- For date fields (*_date, *_on): date-only.
- Respect temporal ordering (created < updated, order < ship < delivery).
- For accepted_values fields: pick ONLY from that list.
- Return ONLY the JSON object. Nothing else.
"""


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------


def _build_user_prompt(
    fields: List[Dict[str, Any]],
    quality_rules: Optional[Dict[str, Any]] = None,
    dataset_name: str = "",
) -> str:
    """Build the user prompt from schema and quality rules."""
    lines = []
    if dataset_name:
        lines.append(f"Dataset: {dataset_name}")
        lines.append("")

    lines.append("## Schema")
    lines.append("")
    for f in fields:
        parts = [f"- {f['name']} ({f.get('type', 'string')}"]
        if f.get("required"):
            parts.append(", required")
        else:
            parts.append(", nullable")
        if f.get("description"):
            parts.append(f", description: {f['description']}")
        if f.get("accepted_values"):
            parts.append(f", accepted_values: {f['accepted_values']}")
        if f.get("min") is not None:
            parts.append(f", min: {f['min']}")
        if f.get("max") is not None:
            parts.append(f", max: {f['max']}")
        if f.get("foreign_key"):
            parts.append(f", foreign_key: {f['foreign_key']}")
        parts.append(")")
        lines.append("".join(parts))

    if quality_rules:
        lines.append("")
        lines.append("## Quality Rules")
        lines.append("")
        row_rules = quality_rules.get("row_rules") or []
        for rule in row_rules:
            if isinstance(rule, dict):
                if rule.get("sql"):
                    lines.append(f"- SQL: {rule['sql']}")
                elif rule.get("not_null"):
                    lines.append(f"- NOT NULL: {rule['not_null']}")
                elif rule.get("accepted_values"):
                    lines.append(f"- ACCEPTED VALUES: {rule['accepted_values']}")
            else:
                lines.append(f"- {rule}")

    lines.append("")
    lines.append("Generate realistic sample values for EVERY field listed above.")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Type coercion
# ---------------------------------------------------------------------------


def _coerce_value(value: Any, ftype: str) -> Any:
    """Coerce an LLM-suggested value to the correct Python type."""
    if value is None:
        return None

    ftype_lower = ftype.lower().split("(")[0].strip()

    if ftype_lower in ("integer", "int", "int32", "int64", "long"):
        try:
            return int(value)
        except (ValueError, TypeError):
            return value

    if ftype_lower in ("double", "float", "float32", "float64", "decimal", "number"):
        try:
            return float(value)
        except (ValueError, TypeError):
            return value

    if ftype_lower in ("boolean", "bool"):
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() in ("true", "1", "yes")
        return bool(value)

    # String / date / timestamp — return as string
    return str(value) if value is not None else value


# ---------------------------------------------------------------------------
# Truncation recovery
# ---------------------------------------------------------------------------


def _salvage_truncated_json(raw: str) -> Optional[Dict[str, Any]]:
    """
    Attempt to repair truncated JSON from an LLM response.

    Strategy:
    1. Strip markdown fences if present.
    2. Walk backwards to find the last complete "values": [...] entry.
    3. Close any open brackets/braces.
    """
    import re

    text = raw.strip()
    # Strip markdown code fences
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    text = text.strip()

    if not text.startswith("{"):
        return None

    # Try progressively shorter substrings, closing brackets
    # Find the last complete field block
    best = None
    # Look for the last ']' that successfully closes a values array
    for i in range(len(text) - 1, 0, -1):
        if text[i] == "]":
            # Try closing from here
            candidate = text[: i + 1]
            # Close any open structures
            open_braces = candidate.count("{") - candidate.count("}")
            open_brackets = candidate.count("[") - candidate.count("]")
            suffix = "]" * max(0, open_brackets) + "}" * max(0, open_braces)
            try:
                best = json.loads(candidate + suffix)
                break
            except json.JSONDecodeError:
                continue

    return best


# ---------------------------------------------------------------------------
# Main function
# ---------------------------------------------------------------------------


def generate_realistic_pools(
    fields: List[Dict[str, Any]],
    quality_rules: Optional[Dict[str, Any]] = None,
    *,
    dataset_name: str = "",
    provider: Optional[str] = None,
    model: Optional[str] = None,
    api_key: Optional[str] = None,
    **provider_kwargs,
) -> Dict[str, List[Any]]:
    """
    Generate realistic value pools for each field using an LLM.

    Args:
        fields: List of field dicts from the contract schema.
        quality_rules: Optional quality rules dict from the contract.
        dataset_name: Dataset name for context (e.g. "Google Analytics Events").
        provider: LLM provider (openai, azure, anthropic, ollama).
        model: Model name override.
        api_key: API key override.

    Returns:
        Dict mapping field name → list of realistic values.
        Empty dict if the LLM call fails.
    """
    from lakelogic.ai.provider import get_llm_client

    if not fields:
        return {}

    user_prompt = _build_user_prompt(fields, quality_rules, dataset_name)

    client = get_llm_client(
        provider=provider,
        model=model,
        api_key=api_key,
        **provider_kwargs,
    )

    logger.info(f"Asking AI to generate realistic data for {len(fields)} fields...")

    response = client.chat(
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        json_mode=True,
        temperature=0.7,  # higher for creative, diverse values
        max_tokens=8192,  # needs room for 10-15 values × N fields
    )

    if response.usage:
        tokens = response.usage.get("prompt_tokens", 0) + response.usage.get("completion_tokens", 0)
        logger.info(f"AI data generation complete ({tokens} tokens used)")

    # Parse response — with truncation recovery
    data = None
    try:
        data = response.as_json()
    except (json.JSONDecodeError, ValueError):
        # LLM may have been truncated — try to salvage
        data = _salvage_truncated_json(response.text)
        if data:
            logger.warning("AI response was truncated; salvaged partial JSON")
        else:
            logger.error(f"Failed to parse AI data generation response")
            logger.debug(f"Raw response (first 500 chars): {response.text[:500]}")
            return {}

    # Build field name → type lookup
    field_types = {f["name"]: f.get("type", "string") for f in fields}

    # Extract and coerce values — handle both flat and nested formats:
    # Flat:   {"field_name": [val1, val2, ...]}
    # Nested: {"fields": {"field_name": {"values": [val1, ...]}}}
    pools: Dict[str, List[Any]] = {}

    # Detect format
    if "fields" in data and isinstance(data["fields"], dict):
        # Nested format
        ai_fields = data["fields"]
        for field_name, field_data in ai_fields.items():
            if field_name not in field_types:
                continue
            raw_values = field_data.get("values", []) if isinstance(field_data, dict) else (field_data if isinstance(field_data, list) else [])
            if not raw_values:
                continue
            ftype = field_types[field_name]
            coerced = [v for v in (_coerce_value(v, ftype) for v in raw_values) if v is not None]
            if coerced:
                pools[field_name] = coerced
    else:
        # Flat format
        for field_name, raw_values in data.items():
            if field_name not in field_types or not isinstance(raw_values, list):
                continue
            ftype = field_types[field_name]
            coerced = [v for v in (_coerce_value(v, ftype) for v in raw_values) if v is not None]
            if coerced:
                pools[field_name] = coerced

    total = sum(len(v) for v in pools.values())
    logger.info(f"Generated {total} realistic values across {len(pools)} fields")

    return pools
