"""Render the business ontology as SVG, from a folder of contracts.

The picture the platform's Business ontology page draws, as a file a repo doc can carry
(``lakelogic ontology contracts/ --out docs/images``). Replaces hand-placed diagram scripts:
positions, sizes and lines all come from :func:`lakelogic.core.business_map.derive_business_map`
and the same deterministic layout the page uses, so the doc and the app cannot disagree.

* Circle size: ``40 + 14 * sqrt(len(appears_in))`` â€” Silver contracts holding the identifier.
* Colour: the owning domain (the first Silver contract's domain), assigned by sorted name.
* Lines: stated relationships only (``name_only`` matches are hints, not drawn).
* Layout: golden-angle spiral by size, then springs, repulsion and a pull to the centre; no
  randomness, so the same contracts always give the same picture.

Pure apart from :func:`load_contract_folder`, which reads YAML files.
"""

from __future__ import annotations

import math
import os
from html import escape
from typing import Any, Dict, List, Optional, Sequence, Tuple

__all__ = ["load_contract_folder", "layout", "render_ontology_svg", "THEMES"]

#: The page's domain colours, in the same order.
DOMAIN_COLOURS = ["#0891b2", "#d97706", "#7c3aed", "#db2777", "#059669", "#64748b", "#2563eb", "#dc2626"]

THEMES: Dict[str, Dict[str, str]] = {
    "light": {"bg": "#ffffff", "ink": "#0b1620", "muted": "#5b6b77", "line": "#c3cdd4"},
    "dark": {"bg": "#0b0e12", "ink": "#e8eef0", "muted": "#9aa8b2", "line": "#33414c"},
}


def load_contract_folder(root: str) -> List[Dict[str, Any]]:
    """Every contract YAML under ``root`` (files with ``model`` or ``fields``), with the domain
    taken from the first folder under ``root`` and ``_source_file`` relative to it.
    ``_domain.yaml`` / ``_system.yaml`` are registry files, not contracts, and are skipped."""
    import yaml

    out: List[Dict[str, Any]] = []
    for base, _dirs, files in sorted(os.walk(root)):
        for f in sorted(files):
            if not f.endswith((".yaml", ".yml")) or f.startswith("_"):
                continue
            path = os.path.join(base, f)
            try:
                with open(path, encoding="utf-8") as fh:
                    doc = yaml.safe_load(fh)
            except Exception:  # noqa: BLE001 - an unreadable file is skipped, not fatal
                continue
            if not isinstance(doc, dict) or not (doc.get("model") or doc.get("fields")):
                continue
            rel = os.path.relpath(path, root)
            doc["_source_file"] = rel.replace(os.sep, "/")
            parts = rel.split(os.sep)
            doc.setdefault("_domain", parts[0] if len(parts) > 1 else None)
            out.append(doc)
    return out


def _measured(
    entities: Sequence[Dict[str, Any]], rels: Optional[Sequence[Dict[str, Any]]] = None
) -> List[Dict[str, Any]]:
    """The concepts drawn, most-used first (the page's ``drawnEntities``). Measured concepts always;
    a gold-only one (no Silver contract holds it) as a dashed outline when a stated link joins it to
    a measured concept. Lookups (``attribute_of``) are details, not circles. Without ``rels`` every
    gold-only concept is kept. A shared key (no table of its own) is drawn dashed; a calendar
    dimension is never drawn, so daily roll-ups do not hang off it as a hub."""
    cands = [
        e
        for e in entities
        if (e.get("contracts") or e.get("shared_key")) and not e.get("attribute_of") and not e.get("calendar")
    ]
    measured = {e["name"] for e in cands if e.get("appears_in")}
    linked = set()
    for r in rels or []:
        if r.get("kind") == "name_only" or r["from"] == r["to"]:
            continue
        if r["from"] in measured:
            linked.add(r["to"])
        if r["to"] in measured:
            linked.add(r["from"])
    ents = [e for e in cands if rels is None or e.get("appears_in") or e["name"] in linked]
    return sorted(ents, key=lambda e: (-len(e.get("appears_in") or []), e["name"]))


def _closest(a: Dict[str, float], b: Dict[str, float], c: Dict[str, float]) -> Tuple[float, float]:
    vx, vy = b["x"] - a["x"], b["y"] - a["y"]
    ln = vx * vx + vy * vy
    t = 0.0 if ln == 0 else max(0.0, min(1.0, ((c["x"] - a["x"]) * vx + (c["y"] - a["y"]) * vy) / ln))
    return a["x"] + t * vx, a["y"] + t * vy


def line_crossings(placed: Sequence[Dict[str, Any]], links: Sequence[Dict[str, Any]]) -> List[str]:
    """Lines passing through a circle that is not one of their ends (should be none)."""
    at = {p["name"]: p for p in placed}
    out = []
    for link in links:
        a, b = at.get(link["from"]), at.get(link["to"])
        if not a or not b:
            continue
        for c in placed:
            if c["name"] in (link["from"], link["to"]):
                continue
            px, py = _closest(a, b, c)
            if math.hypot(c["x"] - px, c["y"] - py) < c["r"]:
                out.append(f"{link['from']}->{link['to']} crosses {c['name']}")
    return out


def radius(appears_in: int) -> float:
    return 40 + 14 * math.sqrt(max(0, appears_in))


def owning_domain(e: Dict[str, Any]) -> Optional[str]:
    for c in e.get("contracts") or []:
        if c.get("layer") == "silver" and c.get("domain"):
            return c["domain"]
    if e.get("domains"):
        return e["domains"][0]
    return next((c["domain"] for c in e.get("contracts") or [] if c.get("domain")), None)


def stated_links(rels: Sequence[Dict[str, Any]], names: set) -> List[Dict[str, Any]]:
    return [
        r
        for r in rels
        if r["from"] != r["to"] and r["from"] in names and r["to"] in names and r.get("kind") != "name_only"
    ]


def layout(entities: Sequence[Dict[str, Any]], links: Sequence[Dict[str, Any]], iterations: int = 360):
    """The page's layout (``pages/business-ontology/ontology.ts``), step for step."""
    nodes = []
    for i, e in enumerate(_measured(entities)):
        angle = i * 2.399963
        dist = 0 if i == 0 else 110 * math.sqrt(i)
        nodes.append(
            {
                "name": e["name"],
                "x": math.cos(angle) * dist,
                "y": math.sin(angle) * dist * 1.25,
                "r": radius(len(e.get("appears_in") or [])),
                "vx": 0.0,
                "vy": 0.0,
            }
        )
    index = {n["name"]: i for i, n in enumerate(nodes)}
    edges = [(index[l["from"]], index[l["to"]]) for l in links if l["from"] in index and l["to"] in index]
    for it in range(iterations):
        cool = 1 - it / iterations
        for i in range(len(nodes)):
            for j in range(i + 1, len(nodes)):
                a, b = nodes[i], nodes[j]
                dx, dy = b["x"] - a["x"], b["y"] - a["y"]
                d = math.hypot(dx, dy)
                if d < 0.01:
                    dx, dy = 0.01 * (j - i), 0.01
                    d = math.hypot(dx, dy)
                mn = a["r"] + b["r"] + 80
                f = 6000 / (d * d) + ((mn - d) * 0.5 if d < mn else 0)
                fx, fy = dx / d * f, dy / d * f
                a["vx"] -= fx
                a["vy"] -= fy
                b["vx"] += fx
                b["vy"] += fy  # noqa: E702
        for i, j in edges:
            a, b = nodes[i], nodes[j]
            dx, dy = b["x"] - a["x"], b["y"] - a["y"]
            d = max(0.01, math.hypot(dx, dy))
            f = (d - (a["r"] + b["r"] + 150)) * 0.02
            a["vx"] += dx / d * f
            a["vy"] += dy / d * f
            b["vx"] -= dx / d * f
            b["vy"] -= dy / d * f  # noqa: E702
        # Keep circles off lines they are not part of (a line through a circle reads as a link).
        for i, j in edges:
            a, b = nodes[i], nodes[j]
            for k, c in enumerate(nodes):
                if k in (i, j):
                    continue
                px, py = _closest(a, b, c)
                dx, dy = c["x"] - px, c["y"] - py
                d = max(0.01, math.hypot(dx, dy))
                clear = c["r"] + 40  # tuned with the page: RideFlow 42 -> 16 crossings, Tagus 0
                if d >= clear:
                    continue
                f = (clear - d) * 1.2
                ux, uy = (dx, dy) if d >= 0.02 else (-(b["y"] - a["y"]), b["x"] - a["x"])
                u = max(0.01, math.hypot(ux, uy))
                c["vx"] += ux / u * f
                c["vy"] += uy / u * f  # noqa: E702
                a["vx"] -= ux / u * f * 0.25
                a["vy"] -= uy / u * f * 0.25  # noqa: E702
                b["vx"] -= ux / u * f * 0.25
                b["vy"] -= uy / u * f * 0.25  # noqa: E702
        for n in nodes:
            n["vx"] -= n["x"] * 0.008
            n["vy"] -= n["y"] * 0.005
            n["x"] += max(-30, min(30, n["vx"])) * cool
            n["y"] += max(-30, min(30, n["vy"])) * cool
            n["vx"] *= 0.5
            n["vy"] *= 0.5
    pad = 24
    min_x = min([0.0] + [n["x"] - n["r"] for n in nodes]) - pad
    min_y = min([0.0] + [n["y"] - n["r"] for n in nodes]) - pad
    max_x = max([0.0] + [n["x"] + n["r"] for n in nodes]) + pad
    max_y = max([0.0] + [n["y"] + n["r"] for n in nodes]) + pad
    placed = [{"name": n["name"], "x": n["x"], "y": n["y"], "r": n["r"]} for n in nodes]
    return placed, (min_x, min_y, max_x - min_x, max_y - min_y)


def _label_lines(name: str) -> List[str]:
    if len(name) <= 12 or " " not in name:
        return [name]
    mid = len(name) / 2
    best = min((i for i, ch in enumerate(name) if ch == " "), key=lambda i: abs(i - mid))
    return [name[:best], name[best + 1 :]]


def render_ontology_svg(
    business_map: Dict[str, Any], theme: str = "light", domains: Optional[Sequence[str]] = None
) -> str:
    """The ontology as a standalone SVG string in ``theme`` ("light" or "dark"), optionally only
    the concepts owned by ``domains`` (case-insensitive)."""
    t = THEMES[theme]
    entities = business_map.get("entities") or []
    rels_all = business_map.get("relationships") or []
    measured = _measured(entities, rels_all)
    if domains:
        wanted = {d.lower() for d in domains}
        measured = [e for e in measured if (owning_domain(e) or "unassigned").lower() in wanted]
    names = {e["name"] for e in measured}
    links = stated_links(business_map.get("relationships") or [], names)
    placed, (bx, by, bw, bh) = layout(measured, links)
    pos = {p["name"]: p for p in placed}
    by_name = {e["name"]: e for e in measured}
    # Colours are assigned over every measured domain, so a filtered picture keeps its colours.
    all_domains = sorted({owning_domain(e) or "unassigned" for e in _measured(entities, rels_all)})
    colour = {d: DOMAIN_COLOURS[i % len(DOMAIN_COLOURS)] for i, d in enumerate(all_domains)}
    domains = sorted({owning_domain(e) or "unassigned" for e in measured})
    fill_op = 0.16 if theme == "dark" else 0.12

    o = [
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{bx:.1f} {by:.1f} {bw:.1f} {bh:.1f}" '
        f'width="{bw:.0f}" height="{bh:.0f}" font-family="Inter, Segoe UI, Helvetica, Arial, sans-serif">',
        f'<rect x="{bx:.1f}" y="{by:.1f}" width="{bw:.1f}" height="{bh:.1f}" fill="{t["bg"]}"/>',
    ]
    for link in links:
        a, b = pos[link["from"]], pos[link["to"]]
        o.append(
            f'<line x1="{a["x"]:.1f}" y1="{a["y"]:.1f}" x2="{b["x"]:.1f}" y2="{b["y"]:.1f}" stroke="{t["line"]}" stroke-width="1.2"/>'
        )
    for p in placed:
        e = by_name[p["name"]]
        c = colour[owning_domain(e) or "unassigned"]
        n = len(e.get("appears_in") or [])
        shared = bool(e.get("shared_key"))
        gold = n == 0 or shared
        x, y, r = p["x"], p["y"], p["r"]
        o.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{r:.1f}" fill="{t["bg"]}"/>')
        if gold:
            o.append(
                f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{r:.1f}" fill="none" stroke="{c}" stroke-width="1.5" stroke-dasharray="5 4"/>'
            )
        else:
            o.append(
                f'<circle cx="{x:.1f}" cy="{y:.1f}" r="{r:.1f}" fill="{c}" fill-opacity="{fill_op}" stroke="{c}" stroke-width="1.5"/>'
            )
        if e.get("curated_in"):
            o.append(f'<circle cx="{x:.1f}" cy="{y - r + 10:.1f}" r="3" fill="{c}"/>')
        fs = 19 if n >= 6 else 15
        lines = _label_lines(e["name"])
        for i, ln in enumerate(lines):
            ly = y + (i - (len(lines) - 1) / 2) * fs * 1.15 - 5
            o.append(
                f'<text x="{x:.1f}" y="{ly:.1f}" font-size="{fs}" font-weight="600" fill="{t["ink"]}" '
                f'text-anchor="middle" dominant-baseline="middle">{escape(ln)}</text>'
            )
        cy = y + (len(lines) - 1) / 2 * fs * 1.15 + fs * 0.9
        sub_label = (
            f"Shared key · {n}" if shared else "Gold only" if gold else f"{n} {'contract' if n == 1 else 'contracts'}"
        )
        o.append(
            f'<text x="{x:.1f}" y="{cy:.1f}" font-size="12.5" fill="{t["muted"]}" '
            f'text-anchor="middle" dominant-baseline="middle">{sub_label}</text>'
        )
    # Legend, top-left: domain colours and what size means.
    lx, ly = bx + 16, by + 24
    o.append(f'<text x="{lx:.1f}" y="{ly:.1f}" font-size="13" font-weight="600" fill="{t["ink"]}">Owning domain</text>')
    for i, d in enumerate(domains):
        yy = ly + 22 + i * 20
        o.append(
            f'<circle cx="{lx + 6:.1f}" cy="{yy - 4:.1f}" r="5" fill="{colour[d]}" fill-opacity="{fill_op * 2}" stroke="{colour[d]}" stroke-width="1.5"/>'
        )
        o.append(
            f'<text x="{lx + 18:.1f}" y="{yy:.1f}" font-size="12" fill="{t["muted"]}">{escape(d.capitalize())}</text>'
        )
    base = ly + 22 + len(domains) * 20 + 10
    for i, line in enumerate(["Size: Silver contracts holding", "the concept's ID column."]):
        o.append(
            f'<text x="{lx:.1f}" y="{base + i * 15:.1f}" font-size="11.5" fill="{t["muted"]}">{escape(line)}</text>'
        )
    o.append("</svg>")
    return "\n".join(o)


def render_folder(
    root: str, out_dir: str, stem: str = "business_ontology", domains: Optional[Sequence[str]] = None
) -> Tuple[str, str]:
    """Write ``{stem}_light.svg`` and ``{stem}_dark.svg`` for the contracts under ``root``."""
    from lakelogic.core.business_map import derive_business_map

    business_map = derive_business_map(load_contract_folder(root))
    os.makedirs(out_dir, exist_ok=True)
    paths = []
    for theme in ("light", "dark"):
        path = os.path.join(out_dir, f"{stem}_{theme}.svg")
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(render_ontology_svg(business_map, theme, domains))
        paths.append(path)
    return paths[0], paths[1]
