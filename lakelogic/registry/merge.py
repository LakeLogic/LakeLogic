"""The one domain → system merge — shared by the runtime and the standalone resolver.

This is the single implementation of the inheritance rules in
``docs/contracts/inheritance.md``. Both :meth:`lakelogic.core.registry.DomainRegistry.from_yaml`
(the runtime) and :func:`lakelogic.registry.resolver.resolve_system` (the provenance view)
call :func:`merge_domain_system`, so the rules can never drift between them.

It is dependency-free (no ``core`` import) to keep the import graph acyclic:
``core.registry`` imports this; this imports only ``keys`` and ``provenance``.
"""

from __future__ import annotations

from typing import Any, Callable, Dict, Optional, Tuple

from lakelogic.registry.keys import IDENTITY_SCALARS_ORDER, INHERITABLE_KEYS_ORDER
from lakelogic.registry.provenance import Origin, Provenance, Reason


def _deep_merge(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge *override* into *base*; override wins. Nested dicts merge,
    lists/scalars are replaced. (Moved here verbatim from ``core.registry`` so there is
    one definition; re-exported there for back-compat.)"""
    merged = dict(base)
    for key, val in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(val, dict):
            merged[key] = _deep_merge(merged[key], val)
        else:
            merged[key] = val
    return merged


def _leaf_provenance(
    domain_val: Dict[str, Any],
    system_val: Dict[str, Any],
    prefix: str,
    prov: Dict[str, Provenance],
    system_file: Optional[str],
    domain_file: Optional[str],
    *,
    child_origin: Origin = Origin.SYSTEM,
    parent_origin: Origin = Origin.DOMAIN,
) -> None:
    """Dotted-path provenance for every leaf under a deep-merged block.

    ``child_origin``/``parent_origin`` name the two layers being folded. They default to the
    domain → system merge; :func:`merge_scope` passes ``CONTRACT``/``SYSTEM`` to describe the
    contract-over-resolved-registry fold with the same code.
    """
    for k in sorted(set(domain_val) | set(system_val)):
        path = f"{prefix}.{k}"
        in_sys, in_dom = k in system_val, k in domain_val
        if in_sys and not in_dom:
            prov[path] = Provenance(path, child_origin, Reason.DECLARED, system_file)
        elif in_dom and not in_sys:
            prov[path] = Provenance(path, parent_origin, Reason.INHERITED, domain_file)
        else:
            dv, sv = domain_val[k], system_val[k]
            if isinstance(dv, dict) and isinstance(sv, dict):
                prov[path] = Provenance(path, Origin.BOTH, Reason.DEEP_MERGED, system_file)
                _leaf_provenance(dv, sv, path, prov, system_file, domain_file,
                                 child_origin=child_origin, parent_origin=parent_origin)
            else:
                prov[path] = Provenance(path, child_origin, Reason.OVERRIDDEN, system_file)


def merge_scope(
    inherited: Optional[Dict[str, Any]],
    declared: Optional[Dict[str, Any]],
    *,
    key: str,
    child_origin: Origin = Origin.CONTRACT,
    parent_origin: Origin = Origin.SYSTEM,
    declared_file: Optional[str] = None,
    inherited_file: Optional[str] = None,
) -> Tuple[Dict[str, Any], Dict[str, Provenance]]:
    """Fold one already-resolved block under a narrower scope's own declaration.

    This is the third step of the chain — ``domain → system`` is :func:`merge_domain_system`;
    this handles ``system → contract``, where a data product (a gold contract) supersedes the
    system it belongs to. Same deep-merge semantics, same per-leaf provenance, so the two
    steps can never disagree about what "overridden" means.

    Returns ``(merged, provenance)``. Either side may be empty.
    """
    inherited = inherited or {}
    declared = declared or {}
    prov: Dict[str, Provenance] = {}

    if not declared:
        merged = dict(inherited)
        if inherited:
            prov[key] = Provenance(key, parent_origin, Reason.INHERITED, inherited_file)
            _leaf_provenance(inherited, {}, key, prov, declared_file, inherited_file,
                             child_origin=child_origin, parent_origin=parent_origin)
        return merged, prov

    if not inherited:
        merged = dict(declared)
        prov[key] = Provenance(key, child_origin, Reason.DECLARED, declared_file)
        _leaf_provenance({}, declared, key, prov, declared_file, inherited_file,
                         child_origin=child_origin, parent_origin=parent_origin)
        return merged, prov

    merged = _deep_merge(inherited, declared)
    prov[key] = Provenance(key, Origin.BOTH, Reason.DEEP_MERGED, declared_file)
    _leaf_provenance(inherited, declared, key, prov, declared_file, inherited_file,
                     child_origin=child_origin, parent_origin=parent_origin)
    return merged, prov


def merge_domain_system(
    system_raw: Dict[str, Any],
    domain_raw: Dict[str, Any],
    *,
    system_file: Optional[str] = None,
    domain_file: Optional[str] = None,
    validate: Optional[Callable[[Dict[str, Any]], bool]] = None,
) -> Tuple[Dict[str, Any], Dict[str, Provenance]]:
    """Fold ``domain_raw`` defaults under ``system_raw`` values, returning the merged config
    and a per-key :class:`Provenance` map.

    Rules (see the spec):

    - **inheritable** keys — system-absent → inherit domain; both dict → deep-merge (system
      fields win); both list → concatenate (domain + system); else scalar → system wins.
      Each computed merge is offered to ``validate`` first; if it returns ``False`` the merge
      is skipped and the system value retained (the runtime's trial-validation safety).
    - **identity scalars** — domain wins on mismatch (consistency-locked); inherited when the
      system omits them.
    - ``cost.currency`` — domain-authoritative.

    ``validate`` receives the full candidate config (all merges applied so far, plus the one
    under test). Pass ``None`` to skip validation (pure merge).
    """
    merged: Dict[str, Any] = dict(system_raw)
    prov: Dict[str, Provenance] = {k: Provenance(k, Origin.SYSTEM, Reason.DECLARED, system_file) for k in system_raw}
    if not domain_raw:
        return merged, prov

    # ── inheritable keys (in the runtime's order — trial-validation is order-sensitive) ──
    for key in INHERITABLE_KEYS_ORDER:
        if key not in domain_raw:
            continue
        domain_val = domain_raw[key]
        system_val = system_raw.get(key)

        if system_val is None:
            candidate, origin, reason, source, leaves = domain_val, Origin.DOMAIN, Reason.INHERITED, domain_file, None
        elif isinstance(system_val, dict) and isinstance(domain_val, dict):
            candidate = _deep_merge(domain_val, system_val)
            origin, reason, source, leaves = Origin.BOTH, Reason.DEEP_MERGED, system_file, (domain_val, system_val)
        elif isinstance(system_val, list) and isinstance(domain_val, list):
            candidate = list(domain_val) + list(system_val)
            origin, reason, source, leaves = Origin.BOTH, Reason.CONCATENATED, system_file, None
        else:
            # scalar in both — system wins outright (no change).
            prov[key] = Provenance(key, Origin.SYSTEM, Reason.OVERRIDDEN, system_file)
            continue

        if validate is not None:
            trial = dict(merged)
            trial[key] = candidate
            if not validate(trial):
                # Incompatible merged shape — keep the system value (runtime safety).
                prov[key] = Provenance(key, Origin.SYSTEM, Reason.OVERRIDDEN, system_file)
                continue

        merged[key] = candidate
        prov[key] = Provenance(key, origin, reason, source)
        if leaves is not None:
            _leaf_provenance(leaves[0], leaves[1], key, prov, system_file, domain_file)

    # ── identity scalars (domain wins on mismatch) ──
    for key in IDENTITY_SCALARS_ORDER:
        domain_val = domain_raw.get(key)
        if domain_val is None:
            continue
        system_val = system_raw.get(key)
        if system_val is None:
            merged[key] = domain_val
            prov[key] = Provenance(key, Origin.DOMAIN, Reason.INHERITED, domain_file)
        elif system_val != domain_val:
            merged[key] = domain_val
            prov[key] = Provenance(key, Origin.DOMAIN, Reason.DOMAIN_LOCKED, domain_file)
        # equal → stays system-declared

    # ── cost.currency is domain-authoritative for roll-ups ──
    domain_cost = domain_raw.get("cost") or {}
    merged_cost = merged.get("cost")
    if isinstance(merged_cost, dict) and isinstance(domain_cost, dict):
        dcur, scur = domain_cost.get("currency"), merged_cost.get("currency")
        if dcur and scur and dcur != scur:
            merged_cost["currency"] = dcur
            prov["cost"] = Provenance("cost", Origin.BOTH, Reason.CURRENCY_NORMALISED, domain_file)

    return merged, prov
