"""Nested-unknown-key walker — re-exported from the public OLC standard.

The canonical implementation lives in ``olc.models._strict_keys`` (installed natively).
Kept as a stable import path; OLC is the source of truth.
"""
from __future__ import annotations

from olc.models._strict_keys import (
    collect_unknown_nested_keys,
    _accepted_input_keys,
    _alias_strings,
    _is_model,
    _iter_child_models,
    _unwrap_optional,
)

__all__ = [
    "collect_unknown_nested_keys",
    "_accepted_input_keys",
    "_alias_strings",
    "_is_model",
    "_iter_child_models",
    "_unwrap_optional",
]
