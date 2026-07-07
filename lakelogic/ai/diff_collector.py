"""
lakelogic.ai.diff_collector
---------------------------
Collect the set of files to review.

Two modes:

* ``--diff REF`` — files changed vs ``REF`` (e.g. ``origin/main``).
  Hard-errors on shallow clones (the #1 ADO/GH gotcha).
* No ``--diff`` — walk one or more paths and apply include/exclude globs.
"""

from __future__ import annotations

import fnmatch
import subprocess
from pathlib import Path
from typing import Optional

from loguru import logger


class ShallowCloneError(RuntimeError):
    """Raised when ``git diff`` can't resolve the base ref due to a shallow clone."""


_DEFAULT_EXCLUDES = (
    "**/node_modules/**",
    "**/__pycache__/**",
    "**/.venv/**",
    "**/.venv_*/**",
    "**/.git/**",
    "**/dist/**",
    "**/build/**",
    "**/.pytest_cache/**",
    "**/*.egg-info/**",
)

_REVIEWABLE_SUFFIXES = {".py", ".sql", ".yaml", ".yml", ".json", ".tf"}


def _matches_any(path: Path, patterns: tuple[str, ...] | list[str]) -> bool:
    s = path.as_posix()
    return any(fnmatch.fnmatch(s, pat) for pat in patterns)


def _is_shallow() -> bool:
    try:
        proc = subprocess.run(
            ["git", "rev-parse", "--is-shallow-repository"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return proc.stdout.strip().lower() == "true"
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _git_changed_files(ref: str) -> list[Path]:
    proc = subprocess.run(
        ["git", "diff", "--name-only", "--diff-filter=ACMRT", f"{ref}...HEAD"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    if proc.returncode != 0:
        if _is_shallow():
            raise ShallowCloneError(
                f"git diff against '{ref}' failed because this is a shallow clone.\n"
                "Fix:\n"
                "  GitHub Actions:  actions/checkout@v4 with: { fetch-depth: 0 }\n"
                "  Azure DevOps:    checkout: self with fetchDepth: 0\n"
                f"git stderr: {proc.stderr.strip()}"
            )
        raise RuntimeError(f"git diff failed: {proc.stderr.strip()}")
    return [Path(line) for line in proc.stdout.splitlines() if line.strip()]


def collect_changed_files(
    paths: list[Path],
    *,
    diff_ref: Optional[str] = None,
    include: Optional[list[str]] = None,
    exclude: Optional[list[str]] = None,
    max_files: int = 50,
) -> list[Path]:
    """Resolve the file list to review.

    Args:
        paths: Roots to walk when ``diff_ref`` is None. ``[Path('.')]`` by default.
        diff_ref: Git ref to diff against. If set, returns only changed files.
        include: Optional glob patterns (any-match).
        exclude: Optional glob patterns (any-match wins).
        max_files: Hard cap; logs a warning and truncates if exceeded.

    Returns:
        Sorted, de-duplicated list of existing files with reviewable suffixes.
    """
    excludes = list(_DEFAULT_EXCLUDES) + list(exclude or [])

    if diff_ref:
        candidates = _git_changed_files(diff_ref)
    else:
        candidates = []
        for root in paths or [Path(".")]:
            if root.is_file():
                candidates.append(root)
            else:
                candidates.extend(p for p in root.rglob("*") if p.is_file())

    out: list[Path] = []
    seen: set[Path] = set()
    for p in candidates:
        if p in seen:
            continue
        if not p.exists() or not p.is_file():
            continue
        if p.suffix not in _REVIEWABLE_SUFFIXES:
            continue
        if _matches_any(p, excludes):
            continue
        if include and not _matches_any(p, include):
            continue
        seen.add(p)
        out.append(p)

    out.sort()

    if len(out) > max_files:
        logger.warning(f"Truncating to --max-files={max_files} (had {len(out)})")
        out = out[:max_files]

    return out
