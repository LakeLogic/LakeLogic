"""
lakelogic.ai.contract_enricher
------------------------------
LLM-powered contract enrichment: field descriptions, PII detection,
recommended SQL quality rules, and data layer classification.

Usage::

    from lakelogic.ai.contract_enricher import enrich_contract
    enriched = enrich_contract(contract_dict, sample_df, provider="openai")

The enricher sends **schema + sample values only** — never raw data at scale.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from loguru import logger


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """\
You are a data engineering expert. You will receive a dataset schema (field names, types)
and a small sample of values for each field.

Your job is to return a JSON object with the following structure:

{
  "fields": [
    {
      "name": "<field_name>",
      "description": "<1-2 sentence human-readable description of what this field represents>",
      "pii": true | false,
      "pii_type": "<PII category if pii=true, e.g. 'email', 'phone', 'national_id', 'address', 'name', null otherwise>",
      "pii_remediation": "<suggested masking strategy if pii=true,
        e.g. 'hash', 'redact', 'mask_partial', null otherwise>"
    }
  ],
  "suggested_rules": [
    {
      "sql": "<SQL expression that should be TRUE for valid rows>",
      "description": "<why this rule matters>"
    }
  ],
  "data_layer": "<bronze | silver | gold — your best guess based on field names and data patterns>",
  "domain": "<business domain, e.g. 'ecommerce', 'healthcare', 'finance', 'property', 'crm'>",
  "summary": "<1-2 sentence summary of what this dataset represents>"
}

Rules for suggested_rules:
- Use standard SQL expressions (no vendor-specific syntax)
- Include NOT NULL checks for fields that appear to be always populated
- Include range checks for numeric fields (e.g. "price >= 0")
- Include format checks for strings (e.g. "email LIKE '%@%.%'")
- Include referential checks where obvious (e.g. "status IN ('active', 'inactive', 'pending')")
- Include temporal logic where appropriate (e.g. "created_at <= CURRENT_TIMESTAMP")
- Be practical — only suggest rules that a data engineer would actually want

Rules for PII detection:
- Consider field NAMES, not just values (e.g. "national_insurance_number" is PII regardless of sample values)
- Common PII: email, phone, name, address, date_of_birth, SSN, IP address, credit card, passport
- Mark geographic data (city, postcode, country) as PII only if it could identify an individual
  when combined with other fields

Return ONLY valid JSON. No markdown, no explanation, no preamble.
"""


# ---------------------------------------------------------------------------
# Enricher
# ---------------------------------------------------------------------------


def _build_user_prompt(
    fields: List[Dict[str, Any]],
    sample_rows: List[Dict[str, Any]],
    dataset_name: str = "",
) -> str:
    """Build the user prompt with schema + sample data."""
    lines = []
    if dataset_name:
        lines.append(f"Dataset: {dataset_name}")
        lines.append("")

    lines.append("## Schema")
    lines.append("")
    for f in fields:
        nullable = "nullable" if not f.get("required") else "required"
        lines.append(f"- {f['name']} ({f.get('type', 'string')}, {nullable})")

    lines.append("")
    lines.append(f"## Sample Data ({len(sample_rows)} rows)")
    lines.append("")

    # Show samples as a compact JSON array (limit to 5 rows, 100 chars per value)
    truncated_rows = []
    for row in sample_rows[:5]:
        truncated = {}
        for k, v in row.items():
            sv = str(v) if v is not None else "null"
            if len(sv) > 100:
                sv = sv[:97] + "..."
            truncated[k] = sv
        truncated_rows.append(truncated)

    lines.append("```json")
    lines.append(json.dumps(truncated_rows, indent=2, default=str))
    lines.append("```")

    return "\n".join(lines)


def enrich_contract(
    contract: Dict[str, Any],
    sample_df=None,
    *,
    provider: Optional[str] = None,
    model: Optional[str] = None,
    api_key: Optional[str] = None,
    sample_size: int = 5,
    **provider_kwargs,
) -> Dict[str, Any]:
    """
    Enrich a contract dict with LLM-generated descriptions, PII flags, and rules.

    Args:
        contract: Raw contract dictionary (as produced by ``bootstrap``).
        sample_df: Optional DataFrame (polars or pandas) with sample data.
        provider: LLM provider name (openai, azure, anthropic, ollama).
        model: Model name override.
        api_key: API key override.
        sample_size: Number of sample rows to send to the LLM.
        **provider_kwargs: Extra provider options.

    Returns:
        Enriched contract dict with descriptions, PII flags, quality rules.
    """
    from lakelogic.ai.provider import get_llm_client

    fields = (contract.get("model") or {}).get("fields") or []
    if not fields:
        logger.warning("No fields in contract — skipping AI enrichment")
        return contract

    # Build sample rows from DataFrame
    sample_rows: List[Dict[str, Any]] = []
    if sample_df is not None:
        if hasattr(sample_df, "to_dicts"):  # Polars
            sample_rows = sample_df.head(sample_size).to_dicts()
        elif hasattr(sample_df, "to_dict"):  # Pandas
            sample_rows = sample_df.head(sample_size).to_dict(orient="records")

    dataset_name = ""
    info = contract.get("info") or {}
    if info.get("title"):
        dataset_name = info["title"]
    elif contract.get("dataset"):
        dataset_name = contract["dataset"]

    user_prompt = _build_user_prompt(fields, sample_rows, dataset_name)

    # Call the LLM
    client = get_llm_client(
        provider=provider,
        model=model,
        api_key=api_key,
        **provider_kwargs,
    )

    logger.info(f"Sending {len(fields)} fields + {len(sample_rows)} sample rows to AI for enrichment...")

    response = client.chat(
        messages=[
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        json_mode=True,
        temperature=0.2,
    )

    if response.usage:
        tokens = response.usage.get("prompt_tokens", 0) + response.usage.get("completion_tokens", 0)
        logger.info(f"AI enrichment complete ({tokens} tokens used)")

    # Parse the response
    try:
        enrichment = response.as_json()
    except (json.JSONDecodeError, ValueError) as e:
        logger.error(f"Failed to parse AI response as JSON: {e}")
        logger.debug(f"Raw response: {response.text[:500]}")
        return contract

    # ── Merge field descriptions + PII flags ──────────────────────────
    ai_fields = {f["name"]: f for f in (enrichment.get("fields") or [])}

    for field in fields:
        ai_field = ai_fields.get(field["name"])
        if not ai_field:
            continue

        if ai_field.get("description"):
            field["description"] = ai_field["description"]

        if ai_field.get("pii"):
            field["pii"] = True
            if ai_field.get("pii_type"):
                field["classification"] = ai_field["pii_type"]
            if ai_field.get("pii_remediation"):
                field["pii_remediation"] = ai_field["pii_remediation"]

    # ── Merge suggested quality rules ─────────────────────────────────
    ai_rules = enrichment.get("suggested_rules") or []
    if ai_rules:
        quality = contract.setdefault("quality", {})
        existing_row_rules = quality.get("row_rules") or []

        # Deduplicate — don't add AI rules that duplicate existing ones
        existing_sql = set()
        for r in existing_row_rules:
            if isinstance(r, dict) and r.get("sql"):
                existing_sql.add(r["sql"].strip().lower())

        for rule in ai_rules:
            sql = rule.get("sql", "").strip()
            if sql and sql.lower() not in existing_sql:
                entry: Dict[str, Any] = {"sql": sql}
                if rule.get("description"):
                    entry["description"] = rule["description"]
                existing_row_rules.append(entry)
                existing_sql.add(sql.lower())

        quality["row_rules"] = existing_row_rules

    # ── Merge metadata ────────────────────────────────────────────────
    if enrichment.get("summary"):
        info = contract.setdefault("info", {})
        if not info.get("description") or info["description"] == "Generated by lakelogic bootstrap.":
            info["description"] = enrichment["summary"]

    if enrichment.get("domain"):
        metadata = contract.setdefault("metadata", {})
        if not metadata.get("domain"):
            metadata["domain"] = enrichment["domain"]

    if enrichment.get("data_layer"):
        metadata = contract.setdefault("metadata", {})
        if not metadata.get("data_layer"):
            metadata["data_layer"] = enrichment["data_layer"]

    logger.info(
        f"AI enrichment applied: "
        f"{sum(1 for f in fields if f.get('description'))} descriptions, "
        f"{sum(1 for f in fields if f.get('pii'))} PII flags, "
        f"{len(ai_rules)} quality rules suggested"
    )

    return contract
