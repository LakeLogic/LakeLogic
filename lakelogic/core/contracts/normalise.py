"""Explicit normalisation: legacy / ODCS / shorthand input -> canonical OLC dict.

Per the alignment plan, compatibility lives HERE, in one auditable place, rather
than scattered across ``DataContract``'s ``mode="before"`` validators. Those
validators are now thin shims that delegate to the functions in this module
(see ``lakelogic.core.models``), so the lenient runtime behaviour is unchanged
while the strict ``OLCContractV1`` path reuses the exact same transforms.

Transforms:
    * ``apply_odcs``                    — ODCS -> LakeLogic (identical to the old helper)
    * ``apply_soft_deletes``           — `soft_deletes:` shorthand -> materialization.*
    * ``apply_schema_policy_migration``— legacy schema_policy / server.schema_evolution
                                          / server.allow_schema_drift -> server.schema_policy

``normalise_contract`` composes the two the canonical path needs (ODCS +
soft-delete-consume). It intentionally does NOT run the schema-policy migration:
that transform MOVES a root-level ``schema_policy`` into ``server``, which
conflicts with the OLC schema treating ``schema_policy`` as a top-level field.
The legacy migration is preserved for ``DataContract`` (runtime) but left out of
the canonical form until the canonical location is settled.
"""
from __future__ import annotations

import copy
from typing import Any

from lakelogic.core.models import _convert_odcs_to_lakelogic


def apply_odcs(data: Any) -> Any:
    """ODCS YAML -> LakeLogic dict (pass-through when the input is not ODCS)."""
    if isinstance(data, dict):
        return _convert_odcs_to_lakelogic(data)
    return data


def apply_soft_deletes(data: Any, *, consume: bool = False) -> Any:
    """Expand the ``soft_deletes:`` shorthand into ``materialization.*`` fields.

    Mirrors the historical ``DataContract._soft_deletes_interceptor`` exactly.
    With ``consume=True`` (canonical path) the consumed ``soft_deletes`` key is
    removed afterwards; the lenient runtime shim uses ``consume=False`` to keep
    the original, byte-for-byte behaviour (``extra="allow"`` tolerates the key).
    """
    if not isinstance(data, dict):
        return data

    sd = data.get("soft_deletes")
    if not isinstance(sd, dict) or not sd.get("enabled", False):
        return data

    mat = data.setdefault("materialization", {})
    if not isinstance(mat, dict):
        return data

    if sd.get("flag_field") and not mat.get("soft_delete_column"):
        mat["soft_delete_column"] = sd["flag_field"]
    if sd.get("timestamp_field") and not mat.get("soft_delete_time_column"):
        mat["soft_delete_time_column"] = sd["timestamp_field"]
    if sd.get("reason_field") and not mat.get("soft_delete_reason_column"):
        mat["soft_delete_reason_column"] = sd["reason_field"]

    if not mat.get("soft_delete_column"):
        mat["soft_delete_column"] = "_lakelogic_is_deleted"
    if not mat.get("soft_delete_time_column"):
        mat["soft_delete_time_column"] = "_lakelogic_deleted_at"
    if not mat.get("soft_delete_reason_column"):
        mat["soft_delete_reason_column"] = "_lakelogic_delete_reason"

    if consume:
        data.pop("soft_deletes", None)  # canonical uses materialization.* only
    return data


def apply_schema_policy_migration(data: Any) -> Any:
    """Migrate legacy schema-policy shapes into ``server.schema_policy``.

    Mirrors the historical ``DataContract._schema_policy_migrator`` exactly:
    root-level ``schema_policy``, ``server.schema_evolution`` and
    ``server.allow_schema_drift`` -> ``server.schema_policy``.
    """
    if not isinstance(data, dict):
        return data

    server_block = data.get("server")
    if not isinstance(server_block, dict):
        if (
            data.get("schema_policy") is not None
            or data.get("schema_evolution") is not None
            or data.get("allow_schema_drift") is not None
        ):
            server_block = {}
            data["server"] = server_block
        else:
            return data

    policy = server_block.setdefault("schema_policy", {})

    # 1. Migrate root-level schema_policy
    root_policy = data.pop("schema_policy", None)
    if isinstance(root_policy, dict):
        if "evolution" in root_policy and "evolution" not in policy:
            policy["evolution"] = root_policy["evolution"]
        if "unknown_fields" in root_policy and "unknown_fields" not in policy:
            policy["unknown_fields"] = root_policy["unknown_fields"]

    # 2. Migrate server.schema_evolution
    legacy_evo = server_block.pop("schema_evolution", None)
    if legacy_evo:
        if "evolution" not in policy:
            policy["evolution"] = legacy_evo
        if legacy_evo == "strict" and "unknown_fields" not in policy:
            policy["unknown_fields"] = "quarantine"

    # 3. Migrate server.allow_schema_drift
    legacy_drift = server_block.pop("allow_schema_drift", None)
    if legacy_drift is not None and "unknown_fields" not in policy:
        policy["unknown_fields"] = "allow" if legacy_drift else "quarantine"

    return data


def normalise_contract(document: Any) -> Any:
    """Return a canonical-OLC dict from a possibly-legacy / ODCS / shorthand input.

    Non-dict inputs pass through unchanged; the input is deep-copied, never mutated.
    """
    if not isinstance(document, dict):
        return document

    doc = copy.deepcopy(document)
    doc = apply_odcs(doc)  # ODCS -> LakeLogic (pass-through if not ODCS)
    doc = apply_soft_deletes(doc, consume=True)
    return doc
