"""Domain → system resolution with provenance (the standalone view).

A thin wrapper over :func:`lakelogic.registry.merge.merge_domain_system` — the *same* merge
the runtime uses — that loads the file pair off disk and packages the result as a
:class:`ResolvedSystem`. It records provenance but does **not** validate merged shapes
(``validate=None``); use ``lakelogic registry validate`` for that.

Scope: the config merge, plus (when an ``environment`` is given) an **annotation** of which
resolved values are environment-dependent. It does not perform the actual placeholder
substitution or per-contract injection — those remain the runtime's job in
:class:`lakelogic.core.registry.DomainRegistry`.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, Optional

from lakelogic.registry.loader import load_pair
from lakelogic.registry.merge import merge_domain_system
from lakelogic.registry.provenance import Origin, Provenance, Reason, ResolvedSystem

_PLACEHOLDER = re.compile(r"\{([a-zA-Z0-9_]+)\}")


def _drop_nulls(d: Dict[str, Any]) -> Dict[str, Any]:
    """A ``key: null`` means "unset" (same rule the manifests apply)."""
    return {k: v for k, v in d.items() if v is not None}


def _annotate_environment(merged: Dict[str, Any], environment: str, prov: Dict[str, Provenance]) -> None:
    """Layer environment provenance onto an already-merged config.

    Records the selected environment's own bindings (``environments.<env>.<field>`` →
    ``env-binding``), then flags every config value that references one of those variables
    via a ``{placeholder}`` as ``env-substituted`` — so a reader can see exactly which
    values change per environment (e.g. ``storage.domain_catalog`` referencing ``{catalog}``)
    without the resolver having to perform the substitution itself.
    """
    envs = merged.get("environments")
    env_block = envs.get(environment) if isinstance(envs, dict) else None
    if not isinstance(env_block, dict):
        return  # unknown environment — nothing to bind

    env_vars = set(env_block)  # only env-provided vars trigger "substituted" (not {domain}/{system})
    for field in env_block:
        prov[f"environments.{environment}.{field}"] = Provenance(
            f"environments.{environment}.{field}", Origin.ENVIRONMENT, Reason.ENV_BINDING, None
        )

    def _walk(node: Any, path: str) -> None:
        if isinstance(node, str):
            refs = {m for m in _PLACEHOLDER.findall(node) if m in env_vars}
            if refs and path:
                prov[path] = Provenance(path, Origin.ENVIRONMENT, Reason.ENV_SUBSTITUTED, None)
        elif isinstance(node, dict):
            for k, v in node.items():
                # Don't recurse into the `environments` block itself (bindings handled above).
                if path == "" and k == "environments":
                    continue
                _walk(v, f"{path}.{k}" if path else k)
        elif isinstance(node, list):
            for i, v in enumerate(node):
                _walk(v, f"{path}[{i}]")

    _walk(merged, "")


def resolve_system(system_path: Path | str, *, environment: Optional[str] = None) -> ResolvedSystem:
    """Resolve a ``_system.yaml`` against its sibling ``_domain.yaml``.

    Returns the effective merged config plus a per-key (and per-leaf, inside deep-merged
    blocks) :class:`~lakelogic.registry.provenance.Provenance` map. When ``environment`` is
    given, values that vary by environment are additionally annotated (see
    :func:`_annotate_environment`).
    """
    system_path = Path(system_path)
    system_raw, domain_raw, domain_path = load_pair(system_path)
    system_raw = _drop_nulls(system_raw)
    domain_raw = _drop_nulls(domain_raw)

    system_file = str(system_path)
    domain_file = str(domain_path) if domain_path else None

    merged, prov = merge_domain_system(system_raw, domain_raw, system_file=system_file, domain_file=domain_file)

    if environment:
        _annotate_environment(merged, environment, prov)

    return ResolvedSystem(
        domain=merged.get("domain") or domain_raw.get("domain") or "",
        system=merged.get("system") or "",
        config=merged,
        provenance=prov,
        domain_file=domain_file,
        system_file=system_file,
        environment=environment,
    )
