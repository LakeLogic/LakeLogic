"""Restatement impact analysis.

When a run *restates* already-materialized data (a reprocess of a date range, a
corrected batch, a late-arriving version), the data it produced replaces data
that consumers have already read.  Nothing in the pipeline tells those
consumers.  This module answers one question, **advisory only**:

    "Which declared consumers of the entities this run restated were NOT part
    of this run?"

Everything here is derived from the ``depends_on`` declared on the loaded
contracts.  That has a hard limit, stated in :data:`GRAPH_PROVENANCE` and
repeated in every rendered report: **a consumer that reads a table without
declaring it is invisible here.**  The downstream list is a lower bound, never
a completeness guarantee.

Nothing in this module raises for pipeline-shaped inputs, and nothing it
produces changes what a run executes.
"""

from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

GRAPH_PROVENANCE = (
    "Impact is derived from `depends_on` declared in the loaded contracts. "
    "A consumer that reads one of these tables WITHOUT declaring it is not "
    "visible here — this list is a lower bound, not a completeness guarantee."
)

IMPACT_DISCLAIMER = (
    "These consumers MAY now be built on superseded data. This is impact, not a "
    "verdict — LakeLogic has not checked whether their outputs actually differ."
)


# ── Layer 1: entity-keyed forward edges (shared with the topological sort) ───


@dataclass
class ForwardEdges:
    """Forward (upstream → downstream) edge index over a set of contracts.

    Keyed by ``entity``.  This is the exact index the runner's topological sort
    has always built; it is extracted here so the impact report and the sort
    read the same edges instead of keeping two copies.

    Attributes:
        by_entity: entity → contract, for every contract passed in.
        graph: entity → list of entities that declare it in ``depends_on``.
            Includes duplicates when a dependency is declared twice, matching
            the historical behaviour of the sort.
        in_degree: entity → number of *resolvable* dependencies it declares.
        unresolved: declared-dependency name → entities that declare it, for
            names that match no contract in the set.
    """

    by_entity: Dict[str, Any] = field(default_factory=dict)
    graph: Dict[str, List[str]] = field(default_factory=lambda: defaultdict(list))
    in_degree: Dict[str, int] = field(default_factory=dict)
    unresolved: Dict[str, List[str]] = field(default_factory=lambda: defaultdict(list))


def build_forward_edges(contracts: Sequence[Any]) -> ForwardEdges:
    """Build the entity-keyed forward edge index for *contracts*.

    Dependencies naming an entity that is not in *contracts* are recorded in
    ``unresolved`` rather than dropped.  They contribute no edge and no
    in-degree, which is what the topological sort has always done.
    """
    by_entity = {c.entity: c for c in contracts}
    graph: Dict[str, List[str]] = defaultdict(list)
    in_degree: Dict[str, int] = {c.entity: 0 for c in contracts}
    unresolved: Dict[str, List[str]] = defaultdict(list)

    for c in contracts:
        for dep in getattr(c, "depends_on", None) or []:
            if dep in by_entity:
                graph[dep].append(c.entity)
                in_degree[c.entity] += 1
            else:
                unresolved[dep].append(c.entity)

    return ForwardEdges(by_entity=by_entity, graph=graph, in_degree=in_degree, unresolved=unresolved)


def topological_order(contracts: Sequence[Any]) -> List[Any]:
    """Order *contracts* by ``depends_on`` (Kahn).

    Raises:
        ValueError: on a circular dependency.
    """
    edges = build_forward_edges(contracts)
    in_degree = dict(edges.in_degree)

    queue = deque(e for e, d in in_degree.items() if d == 0)
    ordered: List[Any] = []

    while queue:
        entity = queue.popleft()
        ordered.append(edges.by_entity[entity])
        for downstream in edges.graph[entity]:
            in_degree[downstream] -= 1
            if in_degree[downstream] == 0:
                queue.append(downstream)

    if len(ordered) != len(contracts):
        remaining = set(in_degree) - {c.entity for c in ordered}
        raise ValueError(f"Circular dependency detected among contracts: {remaining}")

    return ordered


# ── Layer 2: cross-layer downstream index ────────────────────────────────────


def node_id(layer: str, entity: str) -> str:
    """Stable identifier for a contract across layers."""
    return f"{layer}.{entity}"


@dataclass
class DownstreamIndex:
    """Transitive downstream lookup across layers.

    Attributes:
        nodes: node id → contract.
        edges: node id → node ids that declare it as a dependency.
        unresolved: one entry per ``depends_on`` name that matched no loaded
            contract: ``{"consumer", "consumer_layer", "declared_dependency"}``.
            Those consumers are *unknown*, not unaffected.
        ambiguous: one entry per ``depends_on`` name that matched contracts in
            more than one layer with no same-layer match.  Edges are drawn to
            every candidate (over-reporting rather than guessing).
    """

    nodes: Dict[str, Any] = field(default_factory=dict)
    edges: Dict[str, Set[str]] = field(default_factory=lambda: defaultdict(set))
    unresolved: List[Dict[str, str]] = field(default_factory=list)
    ambiguous: List[Dict[str, Any]] = field(default_factory=list)

    def downstream_of(self, seeds: Iterable[str]) -> Tuple[List[str], List[str]]:
        """Transitive downstream node ids of *seeds*.

        Traversal is visited-set guarded, so a dependency cycle terminates
        instead of hanging.

        Returns:
            ``(downstream, cyclic)`` — ``downstream`` is every reachable node
            excluding the seeds themselves, sorted; ``cyclic`` lists seeds that
            are reachable from themselves.
        """
        seed_set = {s for s in seeds if s in self.nodes}
        seen: Set[str] = set()
        queue = deque(seed_set)
        while queue:
            current = queue.popleft()
            for nxt in self.edges.get(current, ()):  # type: ignore[arg-type]
                if nxt not in seen:
                    seen.add(nxt)
                    queue.append(nxt)
        cyclic = sorted(seen & seed_set)
        return sorted(seen - seed_set), cyclic


def build_downstream_index(contracts: Sequence[Any]) -> DownstreamIndex:
    """Build a cross-layer downstream index from declared ``depends_on``.

    Resolution mirrors the runner's existing rules: a dependency name is
    resolved to a contract in the same layer when one exists, otherwise to the
    unique contract with that entity name in any layer.  Names matching
    contracts in several other layers are linked to all of them and recorded in
    ``ambiguous``; names matching nothing are recorded in ``unresolved``.
    """
    index = DownstreamIndex()
    by_entity: Dict[str, List[Any]] = defaultdict(list)
    for c in contracts:
        index.nodes[node_id(c.layer, c.entity)] = c
        by_entity[c.entity].append(c)

    for c in contracts:
        consumer = node_id(c.layer, c.entity)
        for dep in getattr(c, "depends_on", None) or []:
            # A contract whose entity name equals its declared dependency (a
            # common cross-layer naming pattern, e.g. silver `orders` reading
            # bronze `orders`) must not become its own downstream consumer.
            matches = by_entity.get(dep) or []
            candidates = [d for d in matches if node_id(d.layer, d.entity) != consumer]
            if matches and not candidates:
                # Self-reference only — not a downstream relationship.
                continue
            if not candidates:
                index.unresolved.append(
                    {
                        "consumer": consumer,
                        "consumer_layer": c.layer,
                        "declared_dependency": dep,
                    }
                )
                continue
            same_layer = [d for d in candidates if d.layer == c.layer]
            chosen = same_layer or candidates
            if len(chosen) > 1:
                index.ambiguous.append(
                    {
                        "consumer": consumer,
                        "declared_dependency": dep,
                        "candidates": sorted(node_id(d.layer, d.entity) for d in chosen),
                    }
                )
            for d in chosen:
                index.edges[node_id(d.layer, d.entity)].add(consumer)

    return index


# ── Layer 3: the restatement impact report ───────────────────────────────────


def is_restatement_run(
    reprocess_from: Optional[str] = None,
    reprocess_to: Optional[str] = None,
    reprocess_column: Optional[str] = None,
    reprocess_values: Optional[Sequence[Any]] = None,
) -> bool:
    """True when this run restates already-materialized data.

    This is the *existing* signal, not a new one: it is the same expression the
    processor uses for ``_is_reprocess`` (``core/processor.py``), the flag that
    tags the run-log entry ``stage="reprocess"`` so the incremental watermark
    reader ignores it.  A run that is a reprocess for the watermark is a
    restatement here — by construction the two can't disagree.
    """
    return bool(reprocess_from or reprocess_to or (reprocess_column and reprocess_values))


def _describe(contract: Any) -> Dict[str, Any]:
    """Best-effort ``{layer, entity, node_id, table}`` for a contract."""
    table = None
    cd = getattr(contract, "contract_dict", None) or {}
    if isinstance(cd, dict):
        info = cd.get("info") or {}
        if isinstance(info, dict):
            table = info.get("table_name") or info.get("title")
        table = table or cd.get("dataset")
    return {
        "node_id": node_id(contract.layer, contract.entity),
        "layer": contract.layer,
        "entity": contract.entity,
        "table": table,
    }


def build_restatement_impact(
    contracts: Sequence[Any],
    restated: Iterable[Tuple[str, str]],
    in_run_scope: Iterable[Tuple[str, str]],
) -> Dict[str, Any]:
    """Build the restatement impact report.

    Args:
        contracts: every contract loaded for the run (all layers). The graph is
            only as complete as this set.
        restated: ``(layer, entity)`` pairs this run actually rewrote.
        in_run_scope: ``(layer, entity)`` pairs this run targeted — used to
            split downstream consumers into "handled by this run" and the
            actionable "not in this run's targets".

    Returns:
        A JSON-serializable dict. On an internal failure it returns a report
        with ``error`` set rather than raising: a broken impact report must
        never break a pipeline.
    """
    report: Dict[str, Any] = {
        "graph_source": GRAPH_PROVENANCE,
        "disclaimer": IMPACT_DISCLAIMER,
        "restated": [],
        "downstream": [],
        "downstream_not_in_run_scope": [],
        "unknown_dependencies": [],
        "ambiguous_dependencies": [],
        "cyclic_dependencies": [],
        "error": None,
    }
    try:
        index = build_downstream_index(contracts)
        scope_ids = {node_id(layer, entity) for layer, entity in in_run_scope}
        seeds = [node_id(layer, entity) for layer, entity in restated]

        report["restated"] = [_describe(index.nodes[s]) for s in sorted(set(seeds)) if s in index.nodes]

        downstream_ids, cyclic = index.downstream_of(seeds)
        for nid in downstream_ids:
            entry = _describe(index.nodes[nid])
            entry["in_run_scope"] = nid in scope_ids
            report["downstream"].append(entry)
        report["downstream_not_in_run_scope"] = [d for d in report["downstream"] if not d["in_run_scope"]]
        report["cyclic_dependencies"] = cyclic

        # Honesty: a consumer whose declared dependency names an entity we did
        # not load is UNKNOWN — it may or may not consume a restated entity.
        report["unknown_dependencies"] = sorted(
            index.unresolved, key=lambda u: (u["consumer"], u["declared_dependency"])
        )
        report["ambiguous_dependencies"] = index.ambiguous
    except Exception as exc:  # pragma: no cover - defensive; advisory only
        report["error"] = f"{type(exc).__name__}: {exc}"
    return report


def format_restatement_impact(report: Dict[str, Any]) -> str:
    """Render *report* as an operator-readable block."""
    lines: List[str] = []
    lines.append("=" * 80)
    lines.append(" RESTATEMENT IMPACT (advisory — nothing was blocked or changed)")
    lines.append("=" * 80)

    if report.get("error"):
        lines.append(f"  Impact report unavailable: {report['error']}")
        lines.append("  The run is unaffected — this report is advisory only.")
        lines.append("=" * 80)
        return "\n".join(lines)

    restated = report.get("restated") or []
    lines.append(f"  Restated in this run ({len(restated)}):")
    if restated:
        for r in restated:
            lines.append(f"    • {r['node_id']}" + (f"  → {r['table']}" if r.get("table") else ""))
    else:
        lines.append("    (none — no entity was rewritten)")

    downstream = report.get("downstream") or []
    not_in_scope = report.get("downstream_not_in_run_scope") or []
    lines.append("")
    lines.append(f"  Declared downstream consumers ({len(downstream)}):")
    if downstream:
        for d in downstream:
            mark = "in this run" if d.get("in_run_scope") else "NOT IN THIS RUN'S TARGETS"
            lines.append(f"    • {d['node_id']:<40} [{mark}]")
    else:
        lines.append("    (none declared)")

    lines.append("")
    if not_in_scope:
        lines.append(f"  ⚠️  ACTION: {len(not_in_scope)} downstream contract(s) were not in this run's targets:")
        for d in not_in_scope:
            lines.append(f"    • {d['node_id']}" + (f"  → {d['table']}" if d.get("table") else ""))
        lines.append(f"    {IMPACT_DISCLAIMER}")
    else:
        lines.append("  ✅ Every declared downstream consumer was in this run's targets.")

    unknown = report.get("unknown_dependencies") or []
    if unknown:
        lines.append("")
        lines.append(f"  ❓ Unresolved dependencies ({len(unknown)}) — impact UNKNOWN, not 'none':")
        for u in unknown:
            lines.append(
                f"    • {u['consumer']} declares depends_on '{u['declared_dependency']}' — no such contract loaded"
            )

    ambiguous = report.get("ambiguous_dependencies") or []
    if ambiguous:
        lines.append("")
        lines.append(f"  ❓ Ambiguous dependencies ({len(ambiguous)}) — linked to every candidate:")
        for a in ambiguous:
            lines.append(f"    • {a['consumer']} declares '{a['declared_dependency']}' → {', '.join(a['candidates'])}")

    cyclic = report.get("cyclic_dependencies") or []
    if cyclic:
        lines.append("")
        lines.append(f"  ⚠️  Dependency cycle: {', '.join(cyclic)} is reachable from itself.")

    lines.append("")
    lines.append(f"  {report.get('graph_source', GRAPH_PROVENANCE)}")
    lines.append("=" * 80)
    return "\n".join(lines)
