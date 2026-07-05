"""LLM judgment layer for contract review — the *advisory* complement to the
deterministic lint (:mod:`lakelogic.core.contract_lint`).

Handles the checks that need semantic judgment a structural linter can't do:
does a field's quality rules match its *intent*, might an ambiguous free-text
field hold PII, does a transform contradict its description, is business context
missing. It reuses the deterministic backbone's :class:`ContractFinding`.

**Advisory by construction:**
- every finding carries ``source="llm"`` and is clamped below ``critical``, so
  ``gate_severity`` (rules-only) never lets it block CI;
- it's **graceful** — with no API key it returns ``[]`` and the rules lint still
  runs; an LLM/network error also returns ``[]`` (never crashes the lint);
- the LLM call is injectable (``caller=``) so it's fully testable without a key.
"""

from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Optional

from loguru import logger
from pydantic import BaseModel

from lakelogic.core.contract_lint import ContractFinding, GovernanceContext, _layer

# Fields we surface to the model — enough for judgment, nothing sensitive.
_FIELD_KEYS = ("name", "type", "description", "pii", "masking", "rules", "accepted_values", "min", "max")


class _LLMFinding(BaseModel):
    """The structured shape instructor coerces the model output into."""

    check_id: str = "SEM-000"
    field: Optional[str] = None
    severity: str = "info"  # info | warning only (advisory)
    message: str = ""
    suggestion: Optional[str] = None


SYSTEM_PROMPT = """You are a senior data-governance reviewer inspecting a single data \
contract (its schema, existing quality rules, PII tags, and transformations).

Report only SEMANTIC gaps that a structural linter cannot catch — where a field's \
rules don't fit its meaning, an ambiguous field may hold PII, or a transform looks \
inconsistent with its description. Do NOT restate structural issues (missing masking, \
missing keys, missing SLOs) — those are already handled by the deterministic lint.

Use these check ids:
  SEM-001  a field whose quality rules don't fit its intent (e.g. a rating with no \
range rule, a status/enum field with no accepted-values rule, an amount with no \
non-negative rule).
  SEM-002  a free-text / ambiguous field (notes, comment, metadata, description) that \
plausibly contains PII but isn't tagged pii.
  SEM-003  a transformation whose SQL or description looks inconsistent with the \
field it produces.
  SEM-004  missing description / ownership / business context that governance needs.

Severity is 'info' or 'warning' ONLY (never higher). Be conservative — no finding is \
better than a false one. Return an empty list if nothing is clearly wrong."""


def _build_user_prompt(raw: Dict[str, Any], name: str, ctx: Optional[GovernanceContext]) -> str:
    fields = [
        {k: f.get(k) for k in _FIELD_KEYS if f.get(k) is not None}
        for f in ((raw.get("model") or {}).get("fields") or [])
        if isinstance(f, dict)
    ]
    payload = {
        "contract": name,
        "layer": _layer(raw),
        "primary_key": raw.get("primary_key"),
        "fields": fields,
        "quality": raw.get("quality"),
        "transformations": raw.get("transformations"),
    }
    return "Review this contract for semantic governance gaps and return findings.\n\n" + json.dumps(
        payload, default=str, indent=2
    )


def _default_caller(cfg, prompt: str) -> List[Dict[str, Any]]:
    """Real LLM call via the shared instructor client. Returns a list of dicts."""
    from lakelogic.ai.llm_client import _build_client

    client = _build_client(cfg.provider)
    resp = client.chat.completions.create(
        model=cfg.model,
        max_tokens=2048,
        max_retries=1,
        response_model=list[_LLMFinding],
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt},
        ],
    )
    return [r.model_dump() if isinstance(r, BaseModel) else dict(r) for r in resp]


def _to_finding(name: str, d: Dict[str, Any]) -> Optional[ContractFinding]:
    try:
        sev = (d.get("severity") or "info").lower()
        if sev not in ("info", "warning"):  # clamp — LLM findings are never critical
            sev = "warning" if sev in ("high", "error", "critical") else "info"
        return ContractFinding(
            contract=name,
            check_id=d.get("check_id") or "SEM-000",
            severity=sev,
            category="semantic",
            message=d.get("message") or "",
            field=d.get("field"),
            suggestion=d.get("suggestion"),
            source="llm",
        )
    except Exception:
        return None


def judge_contract(
    raw: Dict[str, Any],
    name: str,
    ctx: Optional[GovernanceContext] = None,
    *,
    config: Any = None,
    caller: Optional[Callable[[str], List[Dict[str, Any]]]] = None,
) -> List[ContractFinding]:
    """Run the LLM judgment checks on one contract. Advisory + graceful.

    `caller` (str prompt → list[dict]) is injectable for tests; when omitted, the
    real provider is used only if an API key is configured — otherwise ``[]``.
    """
    if caller is None:
        cfg = config
        if cfg is None:
            from lakelogic.ai.review_config import load_config

            cfg = load_config()
        if not getattr(cfg, "api_key_present", False) or getattr(cfg, "provider", "none") == "none":
            return []  # graceful: no key → skip LLM, rules lint still runs

        def caller(p: str, _cfg=cfg) -> List[Dict[str, Any]]:
            return _default_caller(_cfg, p)

    prompt = _build_user_prompt(raw, name, ctx)
    try:
        raw_findings = caller(prompt)
    except Exception as e:
        logger.warning(f"contract judge LLM call failed for {name}: {type(e).__name__}: {e}")
        return []

    out: List[ContractFinding] = []
    for d in raw_findings or []:
        if isinstance(d, BaseModel):
            d = d.model_dump()
        if not isinstance(d, dict):
            continue  # skip junk items rather than emitting empty findings
        f = _to_finding(name, d)
        if f is not None:
            out.append(f)
    return out
