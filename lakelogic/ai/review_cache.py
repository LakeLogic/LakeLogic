"""
lakelogic.ai.review_cache
-------------------------
Diff-hash cache for ``lakelogic review`` results.

Skips Tier 2 LLM calls when the diff hasn't changed since the last run
(typical "fix typo, re-push" cycle on a PR). Tier 1 always re-runs because
it's cheap.

Cache layout::

    ~/.cache/lakelogic/review/<sha256-of-files+sizes+mtimes>.json

Each entry stores the serialised ``ReviewReport`` from a previous run.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Optional

from loguru import logger


def _cache_root() -> Path:
    override = os.environ.get("LAKELOGIC_REVIEW_CACHE_DIR")
    if override:
        return Path(override)
    base = Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache")
    return base / "lakelogic" / "review"


def compute_cache_key(files: list[Path], extra: str = "") -> str:
    """Produce a stable hash of the file set + their content + an extra string.

    ``extra`` lets callers mix in provider/model so cached results from a
    different model don't get reused.
    """
    h = hashlib.sha256()
    for p in sorted(files, key=lambda x: x.as_posix()):
        h.update(p.as_posix().encode("utf-8"))
        h.update(b"\0")
        try:
            h.update(p.read_bytes())
        except OSError:
            h.update(b"<unreadable>")
        h.update(b"\0\0")
    if extra:
        h.update(extra.encode("utf-8"))
    return h.hexdigest()


def load_cached_report(key: str) -> Optional[dict]:
    """Return the cached report dict for ``key`` if present, else None."""
    path = _cache_root() / f"{key}.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        logger.warning(f"Failed to read cache {path}: {e}")
        return None


def save_cached_report(key: str, report: dict) -> None:
    """Persist a serialised report under ``key``. Best-effort."""
    root = _cache_root()
    try:
        root.mkdir(parents=True, exist_ok=True)
        (root / f"{key}.json").write_text(json.dumps(report, default=str), encoding="utf-8")
    except OSError as e:
        logger.warning(f"Failed to write cache: {e}")
