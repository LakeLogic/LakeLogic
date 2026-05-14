"""
lakelogic.ai.walkthrough
------------------------
Optional LLM-generated PR walkthrough: a prose summary of *what changed* in the
diff (separate from the per-finding review). Embedded into the GitHub /
Azure DevOps summary comment as a "Walkthrough" section.

Distinct from the review prompt: the review focuses on issues to flag; the
walkthrough focuses on orientation for human reviewers ("what does this PR do?").

Designed to be optional — gracefully degrades to None when:
* no API key
* the LLM SDK isn't installed
* the diff is too large for the token budget
* the LLM call fails
"""

from __future__ import annotations

import subprocess
from typing import Optional

from loguru import logger
from pydantic import BaseModel, Field

_WALKTHROUGH_SYSTEM_PROMPT = """\
You are a senior data platform engineer writing a concise PR walkthrough for
human reviewers. Read the unified diff and produce:

1. A 2-3 sentence "what" summary — what does this PR change at the architectural
   level (not "modified file X").
2. A bullet list of notable file groups with the *purpose* of each group.
3. (Optional) A Mermaid sequence diagram if the PR clearly changes a flow
   between components — leave empty if the changes don't warrant one.

Be concrete, skip filler. Reviewers are senior engineers — assume they read
code. Do NOT repeat issues a linter would catch (those are flagged elsewhere).

Return ONLY a JSON object matching this exact schema:
{
  "summary": "<2-3 sentence prose>",
  "highlights": ["<file group 1: purpose>", "<file group 2: purpose>"],
  "mermaid": "<mermaid diagram source, or empty string>"
}
"""


class WalkthroughResult(BaseModel):
    """LLM-generated PR walkthrough."""

    summary: str = ""
    highlights: list[str] = Field(default_factory=list)
    mermaid: str = ""


# Diff truncation: most LLMs handle 30-50K tokens. Conservatively cap input
# to ~25K characters of diff so the output budget stays generous.
_MAX_DIFF_CHARS = 25_000


def _collect_diff(base_ref: str) -> Optional[str]:
    """Return ``git diff <base_ref>...HEAD`` or None on failure."""
    try:
        proc = subprocess.run(
            ["git", "diff", "--stat", f"{base_ref}...HEAD"],
            capture_output=True,
            text=True,
            timeout=30,
            encoding="utf-8",
            errors="replace",
        )
        stat = proc.stdout if proc.returncode == 0 else ""

        proc = subprocess.run(
            ["git", "diff", f"{base_ref}...HEAD"],
            capture_output=True,
            text=True,
            timeout=30,
            encoding="utf-8",
            errors="replace",
        )
        if proc.returncode != 0:
            return None
        body = proc.stdout
    except (FileNotFoundError, subprocess.TimeoutExpired) as e:
        logger.warning(f"Could not collect diff for walkthrough: {e}")
        return None

    if len(body) > _MAX_DIFF_CHARS:
        body = body[:_MAX_DIFF_CHARS] + f"\n[... diff truncated at {_MAX_DIFF_CHARS} chars ...]"
    return f"## Diff stats\n{stat}\n## Diff\n{body}"


def generate_walkthrough(
    base_ref: str,
    *,
    provider: str,
    model: str,
    max_retries: int = 2,
) -> Optional[WalkthroughResult]:
    """Run one LLM call to produce a PR walkthrough. Returns None on any failure."""
    diff_text = _collect_diff(base_ref)
    if not diff_text or len(diff_text) < 100:
        return None  # nothing meaningful to summarise

    try:
        client = _build_client(provider)
    except _LLMUnavailable as e:
        logger.warning(f"Walkthrough skipped: {e}")
        return None

    try:
        result: WalkthroughResult = client.chat.completions.create(
            model=model,
            max_tokens=1500,
            response_model=WalkthroughResult,
            max_retries=max_retries,
            messages=[
                {"role": "system", "content": _WALKTHROUGH_SYSTEM_PROMPT},
                {"role": "user", "content": diff_text},
            ],
        )
    except Exception as e:  # pragma: no cover - network errors
        logger.warning(f"Walkthrough LLM call failed: {e}")
        return None

    return result


class _LLMUnavailable(RuntimeError):
    pass


def _build_client(provider: str):
    """Lazy-import instructor + provider SDK. Mirrors llm_client._build_client."""
    try:
        import instructor  # type: ignore[import-not-found]
    except ImportError as e:
        raise _LLMUnavailable("instructor not installed (pip install lakelogic[ai])") from e

    if provider == "anthropic":
        try:
            from anthropic import Anthropic  # type: ignore[import-not-found]
        except ImportError as e:
            raise _LLMUnavailable("anthropic SDK not installed") from e
        return instructor.from_anthropic(Anthropic())

    if provider == "openai":
        try:
            from openai import OpenAI  # type: ignore[import-not-found]
        except ImportError as e:
            raise _LLMUnavailable("openai SDK not installed") from e
        return instructor.from_openai(OpenAI())

    raise _LLMUnavailable(f"Unknown provider: {provider}")


def render_walkthrough_markdown(w: WalkthroughResult) -> str:
    """Render a WalkthroughResult as a markdown section for the summary comment."""
    parts = ["### 📝 Walkthrough", "", w.summary or "_(no summary)_"]
    if w.highlights:
        parts += ["", "**What changed:**"]
        for h in w.highlights:
            parts.append(f"- {h}")
    if w.mermaid and w.mermaid.strip():
        parts += ["", "**Flow diagram:**", "", "```mermaid", w.mermaid.strip(), "```"]
    return "\n".join(parts)
