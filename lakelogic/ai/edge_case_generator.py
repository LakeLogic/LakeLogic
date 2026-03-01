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


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are a QA engineer and data quality expert. You will receive a dataset schema
(field names, types, constraints) and quality rules.

Your job is to generate **realistic edge-case test values** for each field that are
likely to break quality rules or reveal bugs in data pipelines.

Return a JSON object with this structure:

{
  "fields": {
    "<field_name>": {
      "edge_cases": [<value1>, <value2>, ...],
      "rationale": "<brief explanation of why these are good edge cases>"
    }
  }
}

Guidelines for generating edge cases:

**String fields:**
- Empty string "", whitespace-only "   ", very long strings (200+ chars)
- Unicode/emoji: "Ñ", "日本語", "🔥"
- SQL injection attempts: "'; DROP TABLE --"
- Format violations: wrong email format, invalid URLs
- Case variations: "ACTIVE" vs "active" vs "Active"
- Leading/trailing whitespace: " value ", "value\\n"
- For constrained fields (IN list): values NOT in the list, similar misspellings

**Numeric fields (integer/float):**
- Zero (0), negative (-1), very large (2147483647, 99999999.99)
- Floating point precision: 0.1 + 0.2 = 0.30000000000000004
- Boundary values: min-1, max+1 if range is specified
- NaN-like strings if field might receive string input

**Date/timestamp fields:**
- Epoch zero: 1970-01-01
- Far future: 2099-12-31
- Far past: 1900-01-01
- Leap year edge: 2024-02-29
- Invalid dates: 2023-02-29, 2023-13-01
- Timezone edge cases: midnight UTC vs local

**Boolean fields:**
- null, 0, 1, "true", "false", "yes", "no", ""

**ID/key fields:**
- 0, -1, very large integers, duplicates
- UUID format violations if expected

**General:**
- null / None for every field (test nullability handling)
- Mix data types (string in numeric field, number in string field)

Generate 5-10 edge cases per field. Focus on cases that are REALISTIC — things that
actually show up in production data pipelines, not contrived examples.

Return ONLY valid JSON. No markdown, no explanation.
"""


# ---------------------------------------------------------------------------
# Edge case generator
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

    user_prompt = _build_user_prompt(fields, quality_rules, dataset_name)

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
        temperature=0.4,  # slightly higher for creative edge cases
    )

    if response.usage:
        tokens = response.usage.get("prompt_tokens", 0) + response.usage.get("completion_tokens", 0)
        logger.info(f"Edge case generation complete ({tokens} tokens used)")

    # Parse response
    try:
        data = response.as_json()
    except (json.JSONDecodeError, ValueError) as e:
        logger.error(f"Failed to parse AI edge case response: {e}")
        logger.debug(f"Raw response: {response.text[:500]}")
        return {}

    # Build field name → type lookup
    field_types = {f["name"]: f.get("type", "string") for f in fields}

    # Extract and coerce edge case values
    edge_pools: Dict[str, List[Any]] = {}
    ai_fields = data.get("fields") or {}

    for field_name, field_data in ai_fields.items():
        if field_name not in field_types:
            continue

        raw_cases = field_data.get("edge_cases") or []
        if not raw_cases:
            continue

        ftype = field_types[field_name]
        coerced = [_coerce_value(v, ftype) for v in raw_cases]
        edge_pools[field_name] = coerced

        rationale = field_data.get("rationale", "")
        if rationale:
            logger.debug(f"  {field_name}: {len(coerced)} edge cases — {rationale}")

    total = sum(len(v) for v in edge_pools.values())
    logger.info(f"Generated {total} edge cases across {len(edge_pools)} fields")

    return edge_pools
