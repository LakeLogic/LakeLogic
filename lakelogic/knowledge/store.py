"""
Knowledge Base store — error patterns to remediation docs.

The store is backed by a single JSON/YAML file.  OSS ships with an empty
built-in store; operators can layer their own entries on top by pointing
LAKELOGIC_KB_PATH at a custom file.  The Cloud tier populates it with
Zeus-generated remediation content without requiring OSS changes.
"""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

_BUILTIN_KB_PATH = Path(__file__).parent / "builtin_entries.json"
_ENV_KB_PATH = os.environ.get("LAKELOGIC_KB_PATH")


@dataclass
class KnowledgeEntry:
    """A single error pattern → remediation mapping."""

    pattern_id: str
    title: str
    description: str
    remediation: str
    tags: List[str] = field(default_factory=list)
    examples: List[str] = field(default_factory=list)
    severity: str = "warning"  # info | warning | error

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "KnowledgeEntry":
        return cls(
            pattern_id=data["pattern_id"],
            title=data["title"],
            description=data.get("description", ""),
            remediation=data.get("remediation", ""),
            tags=data.get("tags", []),
            examples=data.get("examples", []),
            severity=data.get("severity", "warning"),
        )


class KnowledgeBase:
    """
    In-memory knowledge base loaded from JSON.

    Load order (later entries override earlier ones with the same pattern_id):
      1. Built-in OSS entries  (lakelogic/knowledge/builtin_entries.json — ships empty)
      2. Custom entries        ($LAKELOGIC_KB_PATH env var, if set)
    """

    def __init__(self, extra_path: Optional[Path] = None):
        self._entries: Dict[str, KnowledgeEntry] = {}
        self._load(_BUILTIN_KB_PATH)
        if _ENV_KB_PATH:
            self._load(Path(_ENV_KB_PATH))
        if extra_path:
            self._load(extra_path)

    def _load(self, path: Path) -> None:
        if not path.exists():
            return
        with open(path, "r", encoding="utf-8") as fh:
            raw = json.load(fh)
        for item in raw.get("entries", []):
            entry = KnowledgeEntry.from_dict(item)
            self._entries[entry.pattern_id] = entry

    def lookup(
        self,
        pattern_id: str,
        *,
        contract_name: Optional[str] = None,
    ) -> Optional[str]:
        """Return the remediation text for a pattern, or None if not found."""
        entry = self._entries.get(pattern_id)
        if entry is None:
            return None
        return entry.remediation

    def get(self, pattern_id: str) -> Optional[KnowledgeEntry]:
        """Return the full KnowledgeEntry, or None if not found."""
        return self._entries.get(pattern_id)

    def search(self, tag: str) -> List[KnowledgeEntry]:
        """Return all entries matching a tag."""
        return [e for e in self._entries.values() if tag in e.tags]

    def all_entries(self) -> List[KnowledgeEntry]:
        return list(self._entries.values())

    def __len__(self) -> int:
        return len(self._entries)
