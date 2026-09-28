"""Plain values for LakeLogic-owned tables.

Everything LakeLogic persists for its own use (``_lakelogic_run_log``,
``_lakelogic_slo_checks``, ``_lakelogic_logs``, ``_lakelogic_pipeline_runs``,
``_lakelogic_erasure_evidence``, ``_lakelogic_retention_evidence``) and the
``_lakelogic_*`` metadata columns it adds to rows is data for machines: it is
filtered, grouped and compared. An icon in a stored value ("✅ OK", "❌ failed")
breaks equality, bloats the payload and renders as mojibake in half the tools
that read it. Icons belong to console/log DISPLAY only (see
``lakelogic.core.slo.format_status``).

Values are produced plain at their source. :func:`plain_text` /
:func:`plain_value` are the single safety net every writer applies before it
persists, so a stray icon from user input (a rule name, an exception message)
never reaches a table either.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict

# Emoji / pictographs / decorative dingbats, plus the joiners and selectors that
# glue emoji sequences together. Arrows (U+2190..21FF), bullets and ordinary
# punctuation are deliberately NOT here: they are text, not icons.
_ICON_RE = re.compile(
    "["
    "\U0001f000-\U0001faff"  # mahjong .. symbols & pictographs ext-A (all emoji planes)
    "\U0001fb00-\U0001fbff"  # legacy computing symbols
    "⌀-⏿"  # misc technical (⏱ ⏳ ⏭ ⌛)
    "☀-➿"  # misc symbols + dingbats (☑ ⚠ ✅ ✔ ✖ ❌ ❓ ➖)
    "⬀-⯿"  # misc symbols & arrows block (⬆ ⭐ ⭕)
    "ℹ"  # ℹ information source
    "︎️"  # variation selectors (text / emoji presentation)
    "‍"  # zero-width joiner
    "⃣"  # combining enclosing keycap
    "\U000e0020-\U000e007f"  # tag characters (flag sequences)
    "]"
)
_SPACE_RUN_RE = re.compile(r"[ \t]{2,}")


def has_icon(value: Any) -> bool:
    """True when ``value`` (a string) contains an icon character."""
    return isinstance(value, str) and bool(_ICON_RE.search(value))


def plain_text(value: Any) -> Any:
    """Remove icons from a string; non-strings pass through unchanged.

    A string without icons is returned byte-identical (tracebacks and SQL keep
    their layout). When icons are removed, the spaces they leave behind are
    collapsed and each line is trimmed, so ``"✅ OK"`` becomes ``"OK"``.
    """
    if not isinstance(value, str) or not _ICON_RE.search(value):
        return value
    stripped = _ICON_RE.sub("", value)
    lines = [_SPACE_RUN_RE.sub(" ", line).strip() for line in stripped.split("\n")]
    return "\n".join(lines).strip()


def plain_value(value: Any) -> Any:
    """:func:`plain_text` applied recursively through dicts, lists and tuples.

    A string holding serialised JSON (``report_json``, ``slo_json``, …) is
    parsed, cleaned and re-serialised: ``json.dumps`` escapes an emoji as
    ``\\u2705``, which a character scan of the raw string would never see.
    """
    if isinstance(value, str):
        text = value.lstrip()
        if text[:1] in ("{", "[") and ("\\u" in value or _ICON_RE.search(value)):
            try:
                parsed = json.loads(value)
            except (ValueError, TypeError):
                return plain_text(value)
            cleaned = plain_value(parsed)
            if cleaned == parsed:
                return value
            return json.dumps(cleaned, default=str)
        return plain_text(value)
    if isinstance(value, dict):
        return {k: plain_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [plain_value(v) for v in value]
    if isinstance(value, tuple):
        return tuple(plain_value(v) for v in value)
    return value


def plain_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """A persisted row with every string value made plain. Keys are untouched."""
    return {k: plain_value(v) for k, v in record.items()}


# Legacy alias named in the owner directive.
_plain = plain_value
