"""
Schema drift detection for the Scanner.

Compares current table schema against a stored baseline to detect:
  - BREAKING: column removed, type narrowed, NOT NULL added
  - WARNING:  column added, type widened, nullable changed
  - INFO:     stats-only change

Baselines are stored in ~/.lakelogic/scanner_baselines.json by default.
When Observatory is connected, baselines are pushed/pulled from the SaaS
so they survive across machines and CI runs.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

# Type narrowing heuristic — listed from narrower to wider
_TYPE_WIDTH = [
    "boolean",
    "bool",
    "tinyint",
    "int8",
    "smallint",
    "int16",
    "int",
    "integer",
    "int32",
    "bigint",
    "int64",
    "long",
    "float",
    "float32",
    "real",
    "double",
    "float64",
    "decimal",
    "numeric",
    "string",
    "varchar",
    "text",
    "binary",
    "bytes",
    "date",
    "timestamp",
    "datetime",
    "array",
    "map",
    "struct",
]


def _type_rank(t: str) -> int:
    t = t.lower().split("(")[0].strip()  # strip precision e.g. decimal(10,2)
    for i, name in enumerate(_TYPE_WIDTH):
        if t == name:
            return i
    return len(_TYPE_WIDTH)  # unknown types ranked highest (safe)


def _is_narrowing(from_type: str, to_type: str) -> bool:
    return _type_rank(to_type) < _type_rank(from_type)


# ── Diff model ────────────────────────────────────────────────────────────────


@dataclass
class ColumnChange:
    name: str
    from_value: Any
    to_value: Any


@dataclass
class SchemaDiff:
    added: List[Dict[str, Any]] = field(default_factory=list)
    removed: List[Dict[str, Any]] = field(default_factory=list)
    type_changes: List[ColumnChange] = field(default_factory=list)
    nullable_changes: List[ColumnChange] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return not (self.added or self.removed or self.type_changes or self.nullable_changes)

    @property
    def has_breaking_changes(self) -> bool:
        if self.removed:
            return True
        if any(_is_narrowing(c.from_value, c.to_value) for c in self.type_changes):
            return True
        # NOT NULL added (nullable True → False)
        if any(c.from_value is True and c.to_value is False for c in self.nullable_changes):
            return True
        return False

    def severity(self) -> str:
        if self.has_breaking_changes:
            return "breaking"
        if self.added or self.type_changes or self.nullable_changes:
            return "warning"
        return "info"

    def summary(self) -> str:
        parts = []
        if self.removed:
            parts.append(f"{len(self.removed)} column(s) removed")
        if self.added:
            parts.append(f"{len(self.added)} column(s) added")
        if self.type_changes:
            parts.append(f"{len(self.type_changes)} type change(s)")
        if self.nullable_changes:
            parts.append(f"{len(self.nullable_changes)} nullability change(s)")
        return ", ".join(parts) if parts else "no changes"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "added": self.added,
            "removed": self.removed,
            "type_changes": [{"name": c.name, "from": c.from_value, "to": c.to_value} for c in self.type_changes],
            "nullable_changes": [
                {"name": c.name, "from": c.from_value, "to": c.to_value} for c in self.nullable_changes
            ],
            "severity": self.severity(),
            "summary": self.summary(),
        }


def compare_schemas(
    baseline: List[Dict[str, Any]],
    current: List[Dict[str, Any]],
) -> SchemaDiff:
    """Compare two schema field lists and return a SchemaDiff."""
    baseline_map = {f["name"]: f for f in baseline}
    current_map = {f["name"]: f for f in current}

    added = [f for name, f in current_map.items() if name not in baseline_map]
    removed = [f for name, f in baseline_map.items() if name not in current_map]

    type_changes: List[ColumnChange] = []
    nullable_changes: List[ColumnChange] = []

    for name, cur in current_map.items():
        if name not in baseline_map:
            continue
        base = baseline_map[name]
        if str(base.get("type", "")).lower() != str(cur.get("type", "")).lower():
            type_changes.append(
                ColumnChange(
                    name=name,
                    from_value=base.get("type"),
                    to_value=cur.get("type"),
                )
            )
        if base.get("nullable") != cur.get("nullable"):
            nullable_changes.append(
                ColumnChange(
                    name=name,
                    from_value=base.get("nullable"),
                    to_value=cur.get("nullable"),
                )
            )

    return SchemaDiff(
        added=added,
        removed=removed,
        type_changes=type_changes,
        nullable_changes=nullable_changes,
    )


# ── Baseline stores ───────────────────────────────────────────────────────────


class BaselineStore(ABC):
    @abstractmethod
    def get(self, table_name: str) -> Optional[List[Dict[str, Any]]]:
        """Return stored schema baseline for table_name, or None if not set."""

    @abstractmethod
    def save(self, table_name: str, schema: List[Dict[str, Any]]) -> None:
        """Persist schema as the new baseline for table_name."""


class LocalBaselineStore(BaselineStore):
    """
    Stores baselines in ~/.lakelogic/scanner_baselines.json.
    Simple and portable — no external dependencies.
    """

    DEFAULT_PATH = Path.home() / ".lakelogic" / "scanner_baselines.json"

    def __init__(self, path: Optional[str] = None):
        self.path = Path(path) if path else self.DEFAULT_PATH
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._data: Dict[str, Any] = {}
        self._load()

    def _load(self) -> None:
        if self.path.exists():
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    self._data = json.load(f)
            except Exception as exc:
                logger.warning(f"Could not load baseline store {self.path}: {exc}")
                self._data = {}

    def _flush(self) -> None:
        try:
            with open(self.path, "w", encoding="utf-8") as f:
                json.dump(self._data, f, indent=2, default=str)
        except Exception as exc:
            logger.warning(f"Could not save baseline store {self.path}: {exc}")

    def get(self, table_name: str) -> Optional[List[Dict[str, Any]]]:
        entry = self._data.get(table_name)
        if entry:
            return entry.get("schema")
        return None

    def save(self, table_name: str, schema: List[Dict[str, Any]]) -> None:
        import datetime

        self._data[table_name] = {
            "schema": schema,
            "saved_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }
        self._flush()
