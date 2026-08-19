"""Canonical key classes for registry resolution — the single source of truth.

These mirror the lists the runtime resolver (``lakelogic.core.registry``) has always used
for domain → system inheritance. They live in their own dependency-free module so both the
runtime and the standalone resolver import the *same* definitions (no drift), without an
import cycle.

Order matters: the runtime folds inheritable keys in this exact order, and each merge is
trial-validated against the whole config so far — so the ordered tuples are authoritative,
with frozensets exposed for membership tests.
"""

from __future__ import annotations

# Keys whose values are *merged* down the domain → system chain (in this order).
INHERITABLE_KEYS_ORDER = (
    "slo",
    "ownership",
    "notifications",
    "quarantine",
    "compliance",
    "lineage",
    "materialization",
    "server",
    "cost",
    "observatory",
    "retention",
)

# Consistency-locked scalars: when both files set one and they differ, the DOMAIN wins.
IDENTITY_SCALARS_ORDER = (
    "domain",
    "bronze_layer",
    "silver_layer",
    "gold_layer",
    "notifications_enabled",
)

INHERITABLE_KEYS = frozenset(INHERITABLE_KEYS_ORDER)
IDENTITY_SCALARS = frozenset(IDENTITY_SCALARS_ORDER)
