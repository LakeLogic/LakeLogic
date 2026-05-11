"""
lakelogic.ai.llm_client
-----------------------
Tier 2 LLM client. Uses ``instructor`` to coerce model output into a
typed ``list[ReviewFinding]`` with auto-retry on malformed JSON.

Lazy imports: instructor / anthropic / openai are only imported inside
``review_batch`` so that Tier 1 keeps working when the LLM extras aren't
installed.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Optional

from loguru import logger

from lakelogic.ai.review_prompts import SYSTEM_PROMPT, build_review_prompt

if TYPE_CHECKING:
    from lakelogic.ai.code_reviewer import ReviewFinding


class LLMUnavailableError(RuntimeError):
    """Raised when the LLM extras (instructor + provider SDK) aren't installed."""


# Rough characters-per-token estimate for budget pre-check. The real tokeniser
# differs per model, but we only need to refuse oversized batches, not bill.
_CHARS_PER_TOKEN = 4

# Files larger than this many lines are truncated before being sent.
_MAX_LINES_PER_FILE = 500


def _file_type(path: Path) -> str:
    return {
        ".py": "python",
        ".sql": "sql",
        ".yaml": "yaml",
        ".yml": "yaml",
        ".json": "json",
        ".tf": "terraform",
    }.get(path.suffix, "unknown")


def _read_truncated(path: Path) -> str:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    lines = text.splitlines()
    if len(lines) > _MAX_LINES_PER_FILE:
        kept = lines[:_MAX_LINES_PER_FILE]
        kept.append(f"[... truncated at line {_MAX_LINES_PER_FILE} of {len(lines)} ...]")
        return "\n".join(kept)
    return text


def _build_payload(files: list[Path]) -> list[dict[str, str]]:
    return [{"path": str(p), "type": _file_type(p), "content": _read_truncated(p)} for p in files]


def _estimate_tokens(prompt: str) -> int:
    return len(prompt) // _CHARS_PER_TOKEN


def review_batch(
    files: list[Path],
    *,
    tier1_findings: Optional[list[dict]] = None,
    custom_rules: Optional[list[str]] = None,
    provider: str,
    model: str,
    max_tokens: int = 30_000,
    max_retries: int = 2,
) -> tuple[list["ReviewFinding"], dict[str, int]]:
    """Run one LLM review batch and return (findings, token_usage).

    Returns an empty findings list (with a warning finding appended) if the
    batch exceeds ``max_tokens`` or the SDKs aren't installed.
    """
    from lakelogic.ai.code_reviewer import ReviewFinding

    payload = _build_payload(files)
    user_prompt = build_review_prompt(payload, custom_rules=custom_rules, tier1_findings=tier1_findings)
    estimated = _estimate_tokens(SYSTEM_PROMPT + user_prompt)

    if estimated > max_tokens:
        logger.warning(f"Batch ~{estimated} tokens exceeds --max-tokens={max_tokens}; skipping LLM call")
        skip = ReviewFinding(
            file=str(files[0]) if files else ".",
            severity="warning",
            category="config",
            rule="batch_too_large",
            message=(
                f"LLM batch skipped: estimated {estimated} tokens > --max-tokens {max_tokens}. "
                "Lower --max-files or split the change."
            ),
            suggestion="Use --max-files to shrink the batch, or raise --max-tokens.",
        )
        return ([skip], {"total": 0})

    try:
        client = _build_client(provider)
    except LLMUnavailableError as e:
        logger.warning(str(e))
        return ([], {"total": 0})

    return _call_llm(client, model, user_prompt, max_retries=max_retries)


def _build_client(provider: str):
    """Lazy-build an instructor-patched client for the chosen provider."""
    try:
        import instructor  # type: ignore[import-not-found]
    except ImportError as e:
        raise LLMUnavailableError("instructor not installed. pip install lakelogic[ai] to enable Tier 2.") from e

    if provider == "anthropic":
        try:
            from anthropic import Anthropic  # type: ignore[import-not-found]
        except ImportError as e:
            raise LLMUnavailableError("anthropic SDK not installed. pip install lakelogic[ai]") from e
        return instructor.from_anthropic(Anthropic())

    if provider == "openai":
        try:
            from openai import OpenAI  # type: ignore[import-not-found]
        except ImportError as e:
            raise LLMUnavailableError("openai SDK not installed. pip install lakelogic[ai]") from e
        return instructor.from_openai(OpenAI())

    raise LLMUnavailableError(f"Unknown provider: {provider}")


def _call_llm(
    client,
    model: str,
    user_prompt: str,
    *,
    max_retries: int,
) -> tuple[list["ReviewFinding"], dict[str, int]]:
    from lakelogic.ai.code_reviewer import ReviewFinding

    try:
        # instructor exposes the same `.chat.completions.create` shape for both
        # Anthropic and OpenAI when patched. response_model coerces output into
        # the requested type with auto-retry on validation errors.
        response = client.chat.completions.create(
            model=model,
            max_tokens=4096,
            response_model=list[ReviewFinding],
            max_retries=max_retries,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
        )
    except Exception as e:  # pragma: no cover - network errors
        logger.warning(f"LLM call failed: {e}")
        skip = ReviewFinding(
            file=".",
            severity="warning",
            category="config",
            rule="llm_call_failed",
            message=f"Tier 2 LLM call failed: {type(e).__name__}. Tier 1 findings still apply.",
        )
        return ([skip], {"total": 0})

    # Best-effort token usage extraction; instructor surfaces it differently
    # per provider, so we tolerate absence.
    usage = {"total": 0}
    raw_usage = getattr(response, "usage", None) or getattr(response, "_raw_response", None)
    if raw_usage and hasattr(raw_usage, "usage"):
        u = raw_usage.usage
        usage["total"] = (getattr(u, "input_tokens", 0) or 0) + (getattr(u, "output_tokens", 0) or 0)

    return (list(response), usage)
