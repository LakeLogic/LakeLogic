"""Load raw registry files off disk (YAML → dict), with domain discovery.

Thin, dependency-free helpers shared by the resolver and validator. The *merge* logic
lives in :mod:`lakelogic.registry.resolver`; this module only reads files.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from lakelogic.registry.validator import _load_yaml


def load_raw(path: Path) -> Dict[str, Any]:
    """Parse a YAML file to a dict (empty dict for empty/`null` files)."""
    data = _load_yaml(Path(path).read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


def domain_path_for(system_path: Path) -> Optional[Path]:
    """The ``_domain.yaml`` one directory above a ``_system.yaml``, if it exists."""
    candidate = Path(system_path).parent.parent / "_domain.yaml"
    return candidate if candidate.exists() else None


def load_pair(system_path: Path) -> Tuple[Dict[str, Any], Dict[str, Any], Optional[Path]]:
    """``(system_raw, domain_raw, domain_path)`` for a ``_system.yaml``.

    ``domain_raw`` is ``{}`` and ``domain_path`` is ``None`` when no sibling
    ``_domain.yaml`` exists.
    """
    system_raw = load_raw(system_path)
    dpath = domain_path_for(system_path)
    domain_raw = load_raw(dpath) if dpath else {}
    return system_raw, domain_raw, dpath
