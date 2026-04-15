"""
lakelogic.ai.edge_case_generator
--------------------------------
LLM-powered edge-case value generation for stress-testing data contracts.

Given a contract schema, asks an LLM to suggest realistic edge-case values
that are likely to break quality rules. These values are injected into the
invalid rows produced by ``DataGenerator.generate()``.

Examples of edge cases the LLM might suggest:
- Email: ``user+tag@domain.com``, ``user@127.0.0.1``, empty string
- Price: ``0.00``, ``-0.01``, ``99999999.99``, precision edge ``0.001``
- Status: wrong casing ``"ACTIVE"``, unknown value ``"deleted"``, empty string
- Dates: ``1970-01-01``, future dates, epoch zero
- IDs: ``0``, ``-1``, ``MAX_INT``, duplicates

Usage::

    from lakelogic.ai.edge_case_generator import generate_edge_cases

    edge_pools = generate_edge_cases(fields, quality_rules, provider="openai")
    # → {"email": ["user+tag@domain.com", "", ...], "revenue": [0, -1, ...]}
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from loguru import logger

from lakelogic.ai.data_generator import _salvage_truncated_json


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You generate edge-case test values for database schemas to stress-test quality rules.

Return a JSON object mapping field names to edge-case arrays:

{"field_name": ["edge1", "edge2"], "other_field": [0, -1, 999999]}

HARD RULES:
- EXACTLY 1-3 edge cases per field. NO MORE.
- All values must be SHORT (under 50 chars for strings).
- Include: nulls, empty strings, boundary values, wrong types, invalid formats.
- For numeric: 0, -1, MAX_INT, boundary violations.
- For dates: epoch (1970-01-01), far future, invalid (2023-02-29).
- For strings with accepted_values: values NOT in the list.
- NO rationale, NO explanations, NO markdown.
- Return ONLY the JSON object. Nothing else.
"""


# ---------------------------------------------------------------------------
# Edge case generator
# ---------------------------------------------------------------------------


def _build_user_prompt(
    fields: List[Dict[str, Any]],
    quality_rules: Optional[Dict[str, Any]] = None,
    dataset_name: str = "",
    custom_scenario: str = "",
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
                    av = rule["accepted_values"]
                    lines.append(f"- ACCEPTED VALUES: {av}")
            else:
                lines.append(f"- {rule}")

        dataset_rules = quality_rules.get("dataset_rules") or []
        for rule in dataset_rules:
            if isinstance(rule, dict) and rule.get("unique"):
                lines.append(f"- UNIQUE: {rule['unique']}")

    if custom_scenario:
        lines.append("")
        lines.append("## Custom Scenario / Instructions for Edge Cases")
        lines.append("")
        lines.append(f"{custom_scenario.strip()}")
        lines.append("")

    return "\n".join(lines)


def _coerce_value(value: Any, ftype: str) -> Any:
    """
    Attempt to coerce an LLM-suggested edge case to the correct Python type.

    If coercion fails (e.g. string "abc" for integer), the value is returned
    as-is — it's likely an intentionally invalid edge case.
    """
    if value is None:
        return None

    ftype_lower = ftype.lower().split("(")[0].strip()

    if ftype_lower in ("integer", "int", "int32", "int64", "long"):
        try:
            return int(value)
        except (ValueError, TypeError):
            return value  # intentionally invalid

    if ftype_lower in ("double", "float", "float32", "float64", "decimal", "number"):
        try:
            return float(value)
        except (ValueError, TypeError):
            return value

    if ftype_lower in ("boolean", "bool"):
        # Keep as-is — edge cases include strings "true", 0, 1, etc.
        return value

    # String / date / timestamp — return as-is
    return value


def generate_edge_cases(
    fields: List[Dict[str, Any]],
    quality_rules: Optional[Dict[str, Any]] = None,
    *,
    dataset_name: str = "",
    custom_scenario: str = "",
    provider: Optional[str] = None,
    model: Optional[str] = None,
    api_key: Optional[str] = None,
    **provider_kwargs,
) -> Dict[str, List[Any]]:
    """
    Generate edge-case value pools for each field using an LLM.

    Args:
        fields: List of field dicts from the contract schema.
        quality_rules: Optional quality rules dict from the contract.
        dataset_name: Dataset name for context.
        provider: LLM provider (openai, azure, anthropic, ollama).
        model: Model name override.
        api_key: API key override.

    Returns:
        Dict mapping field name → list of edge-case values.
        Empty dict if the LLM call fails.
    """
    from lakelogic.ai.provider import get_llm_client

    if not fields:
        return {}

    user_prompt = _build_user_prompt(fields, quality_rules, dataset_name, custom_scenario)

    client = get_llm_client(
        provider=provider,
        model=model,
        api_key=api_key,
        **provider_kwargs,
    )

    logger.info(f"Asking AI to generate edge cases for {len(fields)} fields...")

    response = client.chat(
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        json_mode=True,
        temperature=0.4,
        max_tokens=8192,
    )

    if response.usage:
        tokens = response.usage.get("prompt_tokens", 0) + response.usage.get("completion_tokens", 0)
        logger.info(f"Edge case generation complete ({tokens} tokens used)")

    # Parse response — with truncation recovery
    data = None
    try:
        data = response.as_json()
    except (json.JSONDecodeError, ValueError):
        # Try to salvage truncated JSON
        data = _salvage_truncated_json(response.text)
        if data:
            logger.warning("AI edge case response was truncated; salvaged partial JSON")
        else:
            logger.error("Failed to parse AI edge case response")
            logger.debug(f"Raw response (first 500 chars): {response.text[:500]}")
            return {}

    # Build field name → type lookup
    field_types = {f["name"]: f.get("type", "string") for f in fields}

    # Extract and coerce — handle both flat and nested formats
    edge_pools: Dict[str, List[Any]] = {}

    if "fields" in data and isinstance(data["fields"], dict):
        # Nested: {"fields": {"name": {"edge_cases": [...]}}}
        for field_name, field_data in data["fields"].items():
            if field_name not in field_types:
                continue
            field_vals = field_data.get("edge_cases", []) if isinstance(field_data, dict) else field_data
            raw = field_vals if isinstance(field_vals, list) else []
            if raw:
                ftype = field_types[field_name]
                edge_pools[field_name] = [_coerce_value(v, ftype) for v in raw]
    else:
        # Flat: {"field_name": [edge1, edge2, ...]}
        for field_name, raw in data.items():
            if field_name not in field_types or not isinstance(raw, list):
                continue
            ftype = field_types[field_name]
            edge_pools[field_name] = [_coerce_value(v, ftype) for v in raw]

    total = sum(len(v) for v in edge_pools.values())
    logger.info(f"Generated {total} edge cases across {len(edge_pools)} fields")

    return edge_pools
