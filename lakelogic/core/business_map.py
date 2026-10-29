"""Business map: entities and relationships derived from the contracts people already keep.

One function, :func:`derive_business_map`, owns the matching rules. The Concepts page, Studio
and Zeus all read the map from it; none keeps rules of its own (one definition, one function).

Deterministic and pure: no AI, no I/O. The same contracts always give the same map.

States — every entity, contract mapping and relationship carries one:

* ``confirmed`` — a person agreed it: an ``entity:`` / ``identifies:`` annotation in a contract,
  or a concept / relationship in the domain file (``_domain.yaml``). Merging the pull request
  that added it is the confirmation.
* ``declared`` — stated by the contract's structure: a primary key, a field ``foreign_key``,
  a gold ``*_sk`` join to a dimension.
* ``inferred`` — suggested from names, descriptions or lineage only. Never a fact.

Evidence kinds: ``named`` (a name match), ``key`` (primary / natural key), ``foreign_key``
(field ``foreign_key``), ``join`` (gold ``*_sk`` → dimension), ``lineage`` (upstream contract),
``description`` (a contract description), ``domain_file`` (``_domain.yaml``), ``annotation``
(``entity`` / ``identifies`` in a contract).

Cardinality is read from keys and never guessed. The referenced column is a key of its contract
and the referencing column is not unique in its own ⇒ ``one_to_many`` (from the referenced
entity to the referencing one); both unique ⇒ ``one_to_one``; anything else ⇒ ``unknown``.
A domain-file relationship states its own cardinality (a person's statement).

Relationship direction: ``from`` is the referenced (parent) entity, ``to`` the referencing one,
so ``Customer → Trip`` reads "a Customer has many Trips".

One entity across layers and systems. The entity is named from the contract's table name
(``dataset`` / ``info.table_name`` / file stem — never the display title) with the layer, the
table kind (dim, fact, agg …), any system the estate names (rideflow, stripe …), roll-up words
(daily, summary …) and variant words (scd2, master, profile …) removed and each word made
singular. The gold dimension or fact is the anchor; silver proposes what gold does not; bronze
and gold roll-ups never propose — they join the entity their name or lineage points at, else
they stay unmapped and become a question.

Relationship ``kind``: ``name_only`` when the only evidence is a column sharing another
entity's identifier name (always ``inferred``, cardinality ``unknown``; a UI hides these by
default), else the strongest evidence (``domain_file``, ``annotation``, ``foreign_key``,
``join``). The verb is a person's or a column ``role``'s, else ``relates to`` — never "has". One
line per pair of entities: a name-only guess beside a stated line for the same pair is folded in.

Questions are one per subject (an identifier name, a pair of entities, a group of unmapped
contracts), each with ``impact`` = the data contracts its answer settles, ranked by impact and
capped at ``question_limit``; ``coverage.questions_total`` counts all of them.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple

__all__ = ["derive_business_map"]

#: Columns that look like an identifier. Only these drive name-only links and questions: a
#: shared ``status`` or ``name`` column says nothing about identity.
_ID_LIKE = re.compile(r"_(id|key|code|number|no|sk)$")
_STATE_RANK = {"inferred": 0, "declared": 1, "confirmed": 2}
_LAYER_PREFIXES = ("bronze", "silver", "gold", "raw", "staging", "stg")
_GOLD_KIND_PREFIXES = (("dim_", "dimension"), ("fact_", "fact"), ("fct_", "fact"))
#: Layers whose contract names propose an entity. Bronze (and other raw layers) only join an
#: entity another layer already proposes — a raw feed name is not a business concept.
_PROPOSING_LAYERS = ("silver", "gold", "")
_LAYER_ORDER = {"silver": 0, "gold": 1, "": 2, "bronze": 3}
#: The verb when no contract or person names one. Never "has": that claims ownership nobody stated.
_NO_VERB = "relates to"
#: How many questions the map returns; ``coverage.questions_total`` counts them all.
QUESTION_LIMIT = 25


# ── Small helpers ─────────────────────────────────────────────────────────────


def _s(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def _singular(word: str) -> str:
    if word.endswith("ies") and len(word) > 4:
        return word[:-3] + "y"
    if word.endswith(("sses", "xes", "ches", "shes", "uses")):
        return word[:-2]
    if word.endswith("s") and not word.endswith(("ss", "us", "is")) and len(word) > 3:
        return word[:-1]
    return word


def _title(stem: str) -> str:
    """``trip_completed`` → ``Trip completed`` (the stem is already singular)."""
    words = " ".join(p for p in stem.split("_") if p)
    return words[:1].upper() + words[1:]


def _norm(name: str) -> str:
    return re.sub(r"[^a-z0-9]", "", name.lower())


def _evidence(kind: str, contract: Optional[str], column: Optional[str], detail: str) -> Dict[str, Any]:
    return {"kind": kind, "contract": contract, "column": column, "detail": detail}


def _add_evidence(target: List[Dict[str, Any]], item: Dict[str, Any]) -> None:
    if item not in target:
        target.append(item)


def _ev_key(e: Dict[str, Any]) -> Tuple[str, str, str, str]:
    return (e["kind"], e.get("contract") or "", e.get("column") or "", e.get("detail") or "")


# ── Reading one contract ──────────────────────────────────────────────────────


def _contract_name(c: Dict[str, Any], index: int) -> str:
    info = c.get("info") if isinstance(c.get("info"), dict) else {}
    for candidate in (
        c.get("name"),
        c.get("dataset"),
        info.get("table_name"),
        c.get("contract_file_name"),
        info.get("title"),
    ):
        if _s(candidate):
            name = _s(candidate)
            return re.sub(r"\.ya?ml$", "", name.rsplit("/", 1)[-1].rsplit("\\", 1)[-1])
    source_file = _s(c.get("source_file")) or _s(c.get("_source_file"))
    if source_file:
        return re.sub(r"\.ya?ml$", "", re.split(r"[\\/]", source_file)[-1])
    return f"contract_{index + 1}"


def _layer(c: Dict[str, Any], name: str) -> str:
    info = c.get("info") if isinstance(c.get("info"), dict) else {}
    for candidate in (
        c.get("layer"), c.get("tier"), info.get("layer"), info.get("tier"), c.get("target_layer"), c.get("_layer")
    ):
        value = _s(candidate).lower()
        if value:
            return value
    head = name.lower().split("_", 1)[0]
    return head if head in ("bronze", "silver", "gold") else ""


def _fields(c: Dict[str, Any]) -> List[Dict[str, Any]]:
    model = c.get("model") if isinstance(c.get("model"), dict) else {}
    fields = model.get("fields")
    if not fields:
        schema = c.get("schema")
        if isinstance(schema, dict):
            fields = schema.get("columns") or schema.get("fields")
        elif isinstance(schema, list):
            fields = schema
    return [f for f in (fields or []) if isinstance(f, dict) and _s(f.get("name"))]


def _as_list(value: Any) -> List[str]:
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    if isinstance(value, (list, tuple)):
        return [str(v).strip() for v in value if str(v).strip()]
    return []


def _keys(c: Dict[str, Any], fields: List[Dict[str, Any]]) -> Tuple[List[str], str]:
    pk = _as_list(c.get("primary_key"))
    if pk:
        return pk, "primary_key"
    nk = _as_list(c.get("natural_key"))
    if nk:
        return nk, "natural_key"
    flagged = [f["name"] for f in fields if f.get("primary_key") is True]
    return (flagged, "primary_key") if flagged else ([], "")


def _grain(c: Dict[str, Any], key: List[str]) -> Optional[str]:
    model = c.get("model") if isinstance(c.get("model"), dict) else {}
    for candidate in (model.get("grain"), c.get("grain")):
        if _s(candidate):
            return _s(candidate)
    grain_key = _as_list(model.get("grain_key")) or _as_list(c.get("grain_key"))
    if grain_key:
        return "one row per " + ", ".join(grain_key)
    if key:
        return "one row per " + ", ".join(key)
    return None


def _description(c: Dict[str, Any]) -> Optional[str]:
    info = c.get("info") if isinstance(c.get("info"), dict) else {}
    for candidate in (c.get("description"), info.get("description")):
        if _s(candidate):
            return _s(candidate)
    return None


def _owner(c: Dict[str, Any]) -> Optional[str]:
    info = c.get("info") if isinstance(c.get("info"), dict) else {}
    metadata = c.get("metadata") if isinstance(c.get("metadata"), dict) else {}
    ownership = c.get("ownership") if isinstance(c.get("ownership"), dict) else {}
    for candidate in (c.get("owner"), info.get("owner"), metadata.get("owner"), ownership.get("owner")):
        if isinstance(candidate, dict):
            candidate = candidate.get("name") or candidate.get("email")
        if _s(candidate):
            return _s(candidate)
    return None


def _slug(value: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^a-z0-9]+", "_", value.lower())).strip("_")


def _table_ref(path: str, system: str) -> str:
    """A table reference with platform placeholders resolved: ``{silver_layer}_x`` → ``silver_x``."""
    tail = re.split(r"[./\\:]", path.strip())[-1] if path.strip() else ""
    tail = re.sub(r"\{(\w+?)_layer\}", r"\1", tail)
    tail = tail.replace("{system}", system or "")
    tail = re.sub(r"\{[^}]*\}", "", tail)
    return _slug(tail)


def _system(c: Dict[str, Any]) -> str:
    info = c.get("info") if isinstance(c.get("info"), dict) else {}
    metadata = c.get("metadata") if isinstance(c.get("metadata"), dict) else {}
    return _slug(_s(c.get("system")) or _s(info.get("system")) or _s(metadata.get("system")))


def _machine_name(c: Dict[str, Any], name: str) -> str:
    """The table-style name a contract's entity is read from.

    A display title ("Gold — Dim Accounts (SCD2)") says the same as ``gold_dim_accounts`` but
    with noise, so the dataset / table name / file stem wins; the title is the last resort
    (its parenthetical qualifier dropped).
    """
    info = c.get("info") if isinstance(c.get("info"), dict) else {}
    system = _system(c)
    for candidate in (c.get("dataset"), info.get("table_name")):
        ref = _table_ref(_s(candidate), system)
        if ref:
            return ref
    source_file = _s(c.get("source_file")) or _s(c.get("_source_file"))
    if source_file:
        ref = _slug(re.sub(r"\.ya?ml$", "", re.split(r"[\\/]", source_file)[-1]))
        if ref:
            return ref
    return _slug(re.sub(r"\([^)]*\)", "", name))


#: Name parts that say how a table is shaped, not what it is about. Stripped before naming the
#: entity, so ``gold_fact_trip_completed_daily`` and ``silver_rideflow_trip_completed`` name
#: one thing.
_KIND_TOKENS = {
    "dim": "dimension", "dimension": "dimension", "fact": "fact", "fct": "fact",
    "agg": "aggregate", "mart": "aggregate", "rpt": "aggregate", "report": "aggregate",
}
#: Trailing parts that make a table a roll-up of an entity, not a new one.
_AGGREGATE_SUFFIXES = {
    "daily", "weekly", "monthly", "hourly", "yearly", "quarterly", "summary", "summaries",
    "agg", "aggregate", "aggregates", "rollup", "totals", "metrics", "kpi", "kpis", "stats",
}
#: Trailing parts that make a table a variant (history, a seed, a master copy) of an entity.
_VARIANT_SUFFIXES = {
    "scd1", "scd2", "history", "current", "latest", "snapshot", "snapshots", "master", "seed",
    "lookup", "ref", "reference", "profile", "profiles", "detail", "details", "record", "records",
    "360", "enriched", "validated", "cleaned", "unstructured",
}


def _stem_and_kind(machine: str, systems: List[str], own_system: str, layer: str = "") -> Tuple[str, str]:
    """``(stem, kind)``: the entity part of a table name, and dimension / fact / aggregate / ''.

    Rule: drop the version, the layer, the table-kind words (dim, fact, agg …) and any system
    prefix the estate names (rideflow, stripe …) from the front; drop roll-up words (daily,
    summary …) and variant words (scd2, master, profile …) from the end; singularise each word.
    """
    tokens = [t for t in re.sub(r"_v\d+(_\d+)*$", "", machine).split("_") if t]
    if tokens and tokens[0] in _LAYER_PREFIXES + ("landing",):
        tokens = tokens[1:]
    kind = ""
    ordered_systems = sorted({s for s in systems + [own_system] if s}, key=lambda s: (-len(s), s))
    changed = True
    while changed and len(tokens) > 1:
        changed = False
        if tokens[0] in _KIND_TOKENS:
            kind = kind or _KIND_TOKENS[tokens[0]]
            tokens = tokens[1:]
            changed = True
            continue
        for system in ordered_systems:
            parts = system.split("_")
            if tokens[: len(parts)] == parts and len(tokens) > len(parts):
                tokens = tokens[len(parts) :]
                changed = True
                break
    # Roll-up words make a roll-up only in gold: a silver ``fx_rates_daily`` is FX rates.
    rollups = _AGGREGATE_SUFFIXES if layer in ("gold", "") else set()
    while len(tokens) > 1 and (tokens[-1] in rollups or tokens[-1] in _VARIANT_SUFFIXES):
        if tokens[-1] in rollups:
            kind = "aggregate"
        tokens = tokens[:-1]
    return "_".join(_singular(t) for t in tokens), kind


def _is_unique_column(column: str, key: List[str], field: Optional[Dict[str, Any]]) -> bool:
    if key == [column]:
        return True
    if field and (field.get("unique") is True):
        return True
    return False


# ── The derivation ────────────────────────────────────────────────────────────


class _Contract:
    __slots__ = (
        "raw", "name", "source_file", "layer", "fields", "by_col", "key", "key_kind",
        "grain", "description", "stem", "kind", "entity", "mapping_state", "mapping_evidence", "owner",
        "machine", "system", "domain", "reads",
    )

    def __init__(self, raw: Dict[str, Any], index: int) -> None:
        self.raw = raw
        self.name = _contract_name(raw, index)
        self.source_file = _s(raw.get("source_file")) or _s(raw.get("_source_file")) or None
        self.machine = _machine_name(raw, self.name)
        self.system = _system(raw)
        info = raw.get("info") if isinstance(raw.get("info"), dict) else {}
        self.domain = _s(raw.get("_domain")) or _s(raw.get("domain")) or _s(info.get("domain")) or None
        self.layer = _layer(raw, self.machine)
        # Contracts this one reads: declared upstream, then the table its source names.
        reads = _as_list(raw.get("upstream")) + [
            _s(u.get("contract") or u.get("name"))
            for u in (raw.get("upstream_contracts") or [])
            if isinstance(u, dict)
        ]
        source = raw.get("source") if isinstance(raw.get("source"), dict) else {}
        if _s(source.get("path")) and _s(source.get("type")).lower() in ("table", ""):
            reads.append(_table_ref(_s(source.get("path")), self.system))
        self.reads = [r for r in reads if r]
        self.fields = _fields(raw)
        self.by_col = {f["name"]: f for f in self.fields}
        self.key, self.key_kind = _keys(raw, self.fields)
        self.grain = _grain(raw, self.key)
        self.description = _description(raw)
        self.owner = _owner(raw)
        self.stem, self.kind = "", ""
        self.entity: Optional[str] = None
        self.mapping_state = "inferred"
        self.mapping_evidence: List[Dict[str, Any]] = []


def _upgrade(current: str, new: str) -> str:
    return new if _STATE_RANK[new] > _STATE_RANK[current] else current


def derive_business_map(
    contracts: List[Dict[str, Any]],
    domains: Optional[Dict[str, Dict[str, Any]]] = None,
    question_limit: int = QUESTION_LIMIT,
) -> Dict[str, Any]:
    """Derive the business map (entities, relationships, open questions) from contracts.

    ``contracts`` are parsed contract dicts as loaded from YAML; ``domains`` maps a domain name
    to its parsed ``_domain.yaml`` (``concepts`` / ``relationships`` are read when present).
    See the module docstring for the states, evidence kinds and cardinality rule.
    """
    parsed = [_Contract(c, i) for i, c in enumerate(contracts or []) if isinstance(c, dict)]
    # Input order never changes the map.
    parsed.sort(key=lambda p: (p.name, p.machine, p.source_file or "", p.domain or ""))
    systems = sorted({p.system for p in parsed if p.system})
    for pc in parsed:
        pc.stem, pc.kind = _stem_and_kind(pc.machine, systems, pc.system, pc.layer)
    by_name: Dict[str, _Contract] = {}
    for pc in parsed:
        by_name.setdefault(pc.name.lower(), pc)
    for pc in parsed:
        by_name.setdefault(pc.machine, pc)

    entities: Dict[str, Dict[str, Any]] = {}  # normalised name -> entity
    alias_index: Dict[str, str] = {}  # normalised alias -> normalised entity name

    def entity(name: str, state: str) -> Dict[str, Any]:
        key = _norm(name)
        if key in alias_index:
            key = alias_index[key]
        ent = entities.get(key)
        if ent is None:
            ent = {
                "name": name,
                "definition": None,
                "also_called": [],
                "not_to_be_confused_with": None,
                "example": None,
                "identifier": None,
                "grain": None,
                "owner": None,
                "state": state,
                "properties": [],
                "contracts": [],
                "evidence": [],
                "conflicts": [],
            }
            entities[key] = ent
            alias_index[key] = key
        else:
            ent["state"] = _upgrade(ent["state"], state)
        return ent

    def find_entity(name: str) -> Optional[Dict[str, Any]]:
        key = alias_index.get(_norm(name))
        return entities.get(key) if key else None

    # 1. Domain-file concepts (confirmed). Domains read in name order; first definition wins.
    domain_relationships: List[Tuple[str, Dict[str, Any]]] = []
    for domain_name in sorted((domains or {}).keys()):
        doc = (domains or {}).get(domain_name) or {}
        concepts = doc.get("concepts") if isinstance(doc, dict) else None
        for concept_name in sorted((concepts or {}).keys()):
            spec = (concepts or {}).get(concept_name) or {}
            if not isinstance(spec, dict):
                spec = {}
            ent = entity(str(concept_name), "confirmed")
            for field_name in ("definition", "not_to_be_confused_with", "example", "identifier"):
                if ent[field_name] is None and _s(spec.get(field_name)):
                    ent[field_name] = _s(spec.get(field_name))
            for alias in _as_list(spec.get("also_called")):
                if alias not in ent["also_called"]:
                    ent["also_called"].append(alias)
                alias_index.setdefault(_norm(alias), _norm(ent["name"]))
            _add_evidence(
                ent["evidence"],
                _evidence("domain_file", None, None, f"concept '{concept_name}' in domain '{domain_name}'"),
            )
        for rel in (doc.get("relationships") or []) if isinstance(doc, dict) else []:
            if isinstance(rel, dict):
                domain_relationships.append((domain_name, rel))

    # 2. Map each contract to an entity.
    #    2a. `entity:` annotation (confirmed).
    for pc in parsed:
        annotated = _s(pc.raw.get("entity"))
        if annotated:
            ent = entity(annotated, "confirmed")
            pc.entity, pc.mapping_state = ent["name"], "confirmed"
            ev = _evidence("annotation", pc.name, None, f"entity: {annotated}")
            pc.mapping_evidence.append(ev)
            _add_evidence(ent["evidence"], ev)

    #    2b. A domain concept matched by name (the concept is confirmed; the mapping is inferred).
    #    2c. Otherwise silver / gold names propose an entity (inferred). The gold dimension or
    #        fact is the anchor; silver proposes what gold does not. Bronze and roll-ups
    #        (aggregates, daily / summary marts) never propose: they join the entity their name
    #        or their lineage points at.
    ordered = sorted(parsed, key=lambda p: (_LAYER_ORDER.get(p.layer, 2), p.name))
    anchors_first = sorted(
        parsed, key=lambda p: (0 if p.kind in ("dimension", "fact") else 1, _LAYER_ORDER.get(p.layer, 2), p.name)
    )
    for proposing in (True, False):
        for pc in anchors_first:
            if pc.entity or not pc.stem:
                continue
            is_proposer = pc.layer in _PROPOSING_LAYERS and pc.kind != "aggregate"
            if is_proposer != proposing:
                continue
            candidate = _title(pc.stem)
            ent = find_entity(candidate)
            if ent is None and is_proposer:
                ent = entity(candidate, "inferred")
            if ent is None:
                continue
            kind_label = {"dimension": "gold dimension", "fact": "gold fact", "aggregate": "roll-up"}.get(
                pc.kind, f"{pc.layer or 'contract'} name"
            )
            ev = _evidence("named", pc.name, None, f"{kind_label} '{pc.name}' names {ent['name']}")
            pc.entity, pc.mapping_state = ent["name"], "inferred"
            pc.mapping_evidence.append(ev)
            _add_evidence(ent["evidence"], ev)

    #    2d. Lineage: a still-unmapped contract follows the entity of a contract it reads
    #        (declared upstream, or the table its source names), or of one that reads it.
    readers: Dict[str, List[_Contract]] = {}
    for pc in parsed:
        for ref in pc.reads:
            readers.setdefault(ref.lower(), []).append(pc)
    for _ in range(3):  # bronze → silver → gold chains settle in a few passes
        for pc in ordered:
            if pc.entity:
                continue
            linked = [by_name.get(u.lower()) for u in sorted(pc.reads)]
            linked += readers.get(pc.machine, []) + readers.get(pc.name.lower(), [])
            for parent in linked:
                if parent is None or parent is pc or not parent.entity:
                    continue
                ent = find_entity(parent.entity)
                if ent is None:
                    continue
                own_reads = {r.lower() for r in pc.reads}
                verb = "reads" if parent.name.lower() in own_reads or parent.machine in own_reads else "is read by"
                ev = _evidence("lineage", pc.name, None, f"{verb} contract '{parent.name}'")
                pc.entity, pc.mapping_state = ent["name"], "inferred"
                pc.mapping_evidence.append(ev)
                _add_evidence(ent["evidence"], ev)
                break

    # 3. Identifier, grain, definitions, contract mappings.
    for pc in ordered:
        if not pc.entity:
            continue
        ent = find_entity(pc.entity)
        if ent is None:
            continue
        identifier_column: Optional[str] = None
        if ent["identifier"] and ent["identifier"] in pc.by_col:
            identifier_column = ent["identifier"]
        elif len(pc.key) == 1:
            identifier_column = pc.key[0]
        if pc.key:
            _add_evidence(
                ent["evidence"],
                _evidence("key", pc.name, ", ".join(pc.key), f"{pc.key_kind}: {', '.join(pc.key)}"),
            )
        ent["contracts"].append(
            {
                "name": pc.name,
                "source_file": pc.source_file,
                "layer": pc.layer or None,
                "domain": pc.domain,
                "identifier_column": identifier_column,
                "state": "confirmed" if pc.mapping_state == "confirmed" or ent["state"] == "confirmed" else "proposed",
                "evidence": list(pc.mapping_evidence),
            }
        )

    members: Dict[str, List[_Contract]] = {}
    for pc in ordered:
        if pc.entity:
            members.setdefault(_norm(pc.entity), []).append(pc)
    kind_rank = {"dimension": 0, "": 1, "fact": 2, "aggregate": 3}
    for ent in entities.values():
        # Roll-ups describe the entity least well: read identity and grain from them last.
        mapped = sorted(
            members.get(_norm(ent["name"]), []),
            key=lambda p: (kind_rank.get(p.kind, 1), _LAYER_ORDER.get(p.layer, 2), p.name),
        )
        ent["domains"] = sorted({p.domain for p in mapped if p.domain})
        if ent["identifier"] is None:
            # A business key over a warehouse surrogate (customer_id over customer_sk).
            single = [pc.key[0] for pc in mapped if len(pc.key) == 1 and pc.kind not in ("fact", "aggregate")]
            ent["identifier"] = next((k for k in single if not k.endswith("_sk")), single[0] if single else None)
        if ent["grain"] is None:
            for pc in mapped:
                if pc.grain:
                    ent["grain"] = pc.grain
                    break
        if ent["owner"] is None:
            ent["owner"] = next((pc.owner for pc in mapped if pc.owner), None)
        props: Dict[str, Dict[str, Any]] = {}
        for pc in mapped:
            for f in pc.fields:
                col = f["name"]
                prop = props.get(col)
                if prop is None:
                    prop = props[col] = {
                        "name": col, "type": _s(f.get("type")) or None, "key": False,
                        "required": False, "mapped": 0, "of": len(mapped),
                    }
                prop["mapped"] += 1
                prop["key"] = prop["key"] or col in pc.key
                prop["required"] = prop["required"] or f.get("required") is True
        ent["properties"] = sorted(props.values(), key=lambda p: (not p["key"], p["name"]))
        if ent["definition"] is None:
            values: Dict[str, List[str]] = {}
            for pc in mapped:
                if pc.layer in ("bronze",) or pc.kind == "aggregate" or not pc.description:
                    continue
                values.setdefault(pc.description, []).append(pc.name)
                _add_evidence(ent["evidence"], _evidence("description", pc.name, None, pc.description))
            if values:
                ordered_values = list(values.keys())
                ent["definition"] = ordered_values[0]
                if len(ordered_values) > 1:
                    ent["conflicts"].append(
                        {
                            "field": "definition",
                            "values": ordered_values,
                            "contracts": [names for names in values.values()],
                        }
                    )

    # 4. Relationships.
    relationships: Dict[Tuple[str, str], Dict[str, Any]] = {}

    def relationship(src: str, dst: str, verb: str, cardinality: str, state: str, ev: Dict[str, Any]) -> None:
        key = (src, dst)
        rel = relationships.get(key)
        if rel is None:
            rel = {"from": src, "verb": verb, "to": dst, "cardinality": cardinality, "state": state, "evidence": []}
            relationships[key] = rel
        else:
            if _STATE_RANK[state] > _STATE_RANK[rel["state"]]:
                rel["state"] = state
                if state == "confirmed":
                    rel["verb"] = verb
            if rel["cardinality"] == "unknown" and cardinality != "unknown":
                rel["cardinality"] = cardinality
            if rel["verb"] == _NO_VERB and verb != _NO_VERB:
                rel["verb"] = verb
        _add_evidence(rel["evidence"], ev)

    def resolve_contract(ref: str) -> Optional[_Contract]:
        ref_l = ref.lower()
        if ref_l in by_name:
            return by_name[ref_l]
        for pc in parsed:
            for n in (pc.machine, pc.name.lower()):
                if n.endswith("_" + ref_l) or ref_l.endswith("_" + n):
                    return pc
        return None

    def is_ref_key(parent: _Contract, column: str) -> bool:
        if _is_unique_column(column, parent.key, parent.by_col.get(column)):
            return True
        # A dimension's surrogate key is unique by construction.
        return parent.kind == "dimension" and column.endswith("_sk")

    def cardinality_for(child: _Contract, child_col: str, parent: _Contract, parent_col: str) -> str:
        if not is_ref_key(parent, parent_col):
            return "unknown"
        if _is_unique_column(child_col, child.key, child.by_col.get(child_col)):
            return "one_to_one"
        return "one_to_many"

    declared_columns: Dict[Tuple[str, str], bool] = {}

    for pc in sorted(parsed, key=lambda p: p.name):
        for f in pc.fields:
            col = f["name"]
            role = _s(f.get("role"))
            verb = role or _NO_VERB
            fk = f.get("foreign_key") if isinstance(f.get("foreign_key"), dict) else None
            identifies = _s(f.get("identifies"))
            if fk and _s(fk.get("contract")):
                parent = resolve_contract(_s(fk.get("contract")))
                parent_col = _s(fk.get("column")) or (parent.key[0] if parent and len(parent.key) == 1 else col)
                declared_columns[(pc.name, col)] = True
                if parent is None or not parent.entity or not pc.entity or parent.entity == pc.entity:
                    pass
                else:
                    is_join = col.endswith("_sk") or parent.kind == "dimension"
                    kind = "join" if is_join else "foreign_key"
                    detail = f"{pc.name}.{col} -> {parent.name}.{parent_col}"
                    relationship(
                        parent.entity, pc.entity, verb, cardinality_for(pc, col, parent, parent_col), "declared",
                        _evidence(kind, pc.name, col, detail),
                    )
            elif col.endswith("_sk") and pc.kind == "fact" and pc.entity:
                # Gold fact surrogate key without a foreign_key: the dimension it joins, by name.
                stem = col[: -len("_sk")]
                dim = None
                for other in sorted(parsed, key=lambda p: p.name):
                    if other.kind == "dimension" and other.entity and (
                        _norm(other.stem) in (_norm(stem), _norm(stem) + "s") or col in other.by_col
                    ):
                        dim = other
                        break
                if dim is not None and dim.entity != pc.entity:
                    declared_columns[(pc.name, col)] = True
                    relationship(
                        dim.entity, pc.entity, verb, cardinality_for(pc, col, dim, col), "declared",
                        _evidence("join", pc.name, col, f"gold fact {pc.name}.{col} joins dimension {dim.name}"),
                    )
            if identifies:
                target = find_entity(identifies) or entity(identifies, "confirmed")
                declared_columns[(pc.name, col)] = True
                detail = f"identifies: {identifies}" + (f" (role: {role})" if role else "")
                ev = _evidence("annotation", pc.name, col, detail)
                _add_evidence(target["evidence"], ev)
                if pc.entity and target["name"] != pc.entity:
                    card = "unknown"
                    ident = target["identifier"]
                    for other in parsed:
                        if other.entity == target["name"] and ident and other.key == [ident]:
                            card = "one_to_one" if _is_unique_column(col, pc.key, f) else "one_to_many"
                            break
                    relationship(target["name"], pc.entity, verb, card, "confirmed", ev)

    # 4b. Name-only matches (inferred, cardinality unknown): `<x>_id` equal to another entity's identifier.
    for pc in sorted(parsed, key=lambda p: p.name):
        if not pc.entity:
            continue
        for f in pc.fields:
            col = f["name"]
            if declared_columns.get((pc.name, col)) or pc.key == [col]:
                continue
            if not _ID_LIKE.search(col):
                continue
            for ent in sorted(entities.values(), key=lambda e: e["name"]):
                if ent["name"] == pc.entity or not ent["identifier"] or ent["identifier"] != col:
                    continue
                relationship(
                    ent["name"], pc.entity, _NO_VERB, "unknown", "inferred",
                    _evidence("named", pc.name, col, f"{pc.name}.{col} has the name of {ent['name']}'s identifier"),
                )

    # 4c. Domain-file relationships (confirmed).
    for domain_name, rel in domain_relationships:
        src, dst, verb = _s(rel.get("from")), _s(rel.get("to")), _s(rel.get("verb")) or _NO_VERB
        if not src or not dst:
            continue
        src_ent = find_entity(src) or entity(src, "confirmed")
        dst_ent = find_entity(dst) or entity(dst, "confirmed")
        card = _s(rel.get("cardinality")) or "unknown"
        if card not in ("one_to_one", "one_to_many", "many_to_one", "many_to_many", "unknown"):
            card = "unknown"
        a, b = src_ent["name"], dst_ent["name"]
        ev = _evidence("domain_file", None, None, f"'{a} {verb} {b}' in domain '{domain_name}'")
        if (a, b) in relationships:
            existing = relationships[(a, b)]
            existing["state"], existing["verb"] = "confirmed", verb
            if card != "unknown":
                existing["cardinality"] = card
            _add_evidence(existing["evidence"], ev)
        elif (b, a) in relationships and card in ("one_to_many", "many_to_one", "unknown"):
            # The keys read it the other way round; restate in the person's direction.
            existing = relationships.pop((b, a))
            flipped = {"one_to_many": "many_to_one", "many_to_one": "one_to_many"}.get(
                existing["cardinality"], existing["cardinality"]
            )
            relationships[(a, b)] = {
                "from": a, "verb": verb, "to": b,
                "cardinality": card if card != "unknown" else flipped,
                "state": "confirmed", "evidence": existing["evidence"] + [ev],
            }
        else:
            relationship(a, b, verb, card, "confirmed", ev)

    # 4d. One line per pair of concepts. A name-only guess beside a stated line for the same
    #     pair (either direction) adds nothing; two name-only guesses keep one direction.
    def rel_kind(rel: Dict[str, Any]) -> str:
        kinds = {e["kind"] for e in rel["evidence"]}
        for k in ("domain_file", "annotation", "foreign_key", "join"):
            if k in kinds:
                return k
        return "name_only"

    for (src, dst) in sorted(relationships):
        if (src, dst) not in relationships or (dst, src) not in relationships:
            continue
        a, b = relationships[(src, dst)], relationships[(dst, src)]
        a_guess, b_guess = rel_kind(a) == "name_only", rel_kind(b) == "name_only"
        if a_guess and (not b_guess or src > dst):
            for ev in a["evidence"]:
                _add_evidence(b["evidence"], ev)
            del relationships[(src, dst)]
        elif b_guess and not a_guess:
            for ev in b["evidence"]:
                _add_evidence(a["evidence"], ev)
            del relationships[(dst, src)]
    for rel in relationships.values():
        rel["kind"] = rel_kind(rel)

    # 5. Questions — one per subject (an identifier name, a pair of concepts, a group of
    #    unmapped contracts), ranked by how many data contracts the answer settles.
    questions: List[Dict[str, Any]] = []
    identifier_owner: Dict[str, str] = {}
    for e in sorted(entities.values(), key=lambda e: e["name"]):
        if e["identifier"]:
            identifier_owner.setdefault(e["identifier"], e["name"])
    alias_owner: Dict[str, str] = {}
    for e in sorted(entities.values(), key=lambda e: e["name"]):
        for alias in e["also_called"]:
            alias_owner[_norm(alias)] = e["name"]

    def ask(kind: str, text: str, subjects: List[str], contracts: List[str]) -> None:
        questions.append(
            {"kind": kind, "text": text, "subjects": subjects, "impact": len(set(contracts))}
        )

    # Same column name in contracts of DIFFERENT entities, at least one not settled by a key,
    # foreign key or annotation. Copies of one entity across layers are not a question.
    occurrences: Dict[str, List[Tuple[str, bool, str]]] = {}  # column -> [(contract, open, entity)]
    for pc in parsed:
        for f in pc.fields:
            col = f["name"]
            if not _ID_LIKE.search(col) or col.endswith("_sk"):
                continue
            is_open = not declared_columns.get((pc.name, col)) and not (pc.key == [col])
            occurrences.setdefault(col, []).append((pc.name, is_open, pc.entity or "?" + pc.name))
    for col in sorted(occurrences):
        occ = occurrences[col]
        concepts = sorted({e for _, _, e in occ if not e.startswith("?")})
        if not any(is_open for _, is_open, _ in occ) or len({e for _, _, e in occ}) < 2:
            continue
        owner = identifier_owner.get(col)
        subjects = sorted({f"{c}.{col}" for c, _, _ in occ})
        target = f"the same {owner}" if owner else "the same thing"
        where = f" (in {', '.join(concepts[:3])}{' and more' if len(concepts) > 3 else ''})" if concepts else ""
        ask(
            "same_identifier",
            f"Does {col} identify {target} in all {len(subjects)} data contracts that have it{where}?",
            subjects, [c for c, _, _ in occ],
        )
    # Alias-named columns (rider_id when Customer is also called Rider) — a different name for
    # what may be the same identifier. One question per column name.
    alias_hits: Dict[Tuple[str, str], List[str]] = {}
    for pc in parsed:
        for f in pc.fields:
            col = f["name"]
            m = re.match(r"^(.+)_id$", col)
            if not m or declared_columns.get((pc.name, col)) or col in identifier_owner:
                continue
            owner = alias_owner.get(_norm(m.group(1)))
            if owner:
                alias_hits.setdefault((col, owner), []).append(pc.name)
    for (col, owner), names in sorted(alias_hits.items()):
        ident = next((e["identifier"] for e in entities.values() if e["name"] == owner), None)
        subjects = sorted({f"{n}.{col}" for n in names}) + ([ident] if ident else [])
        ask(
            "same_identifier",
            f"Is {col} a {owner}" + (f" (the same as {ident})?" if ident else "?"),
            subjects, names,
        )
    for (src, dst) in sorted(relationships):
        rel = relationships[(src, dst)]
        if rel["cardinality"] == "unknown":
            ask(
                "cardinality",
                f"How many {dst} records can one {src} have, and how many {src} records can one {dst} have?",
                [src, dst], [e["contract"] for e in rel["evidence"] if e.get("contract")],
            )
    unmapped: Dict[str, List[str]] = {}
    for pc in parsed:
        if not pc.entity:
            unmapped.setdefault(pc.stem or pc.machine, []).append(pc.name)
    for stem, names in sorted(unmapped.items()):
        names = sorted(set(names))
        text = (
            f"Which business concept does {names[0]} hold?"
            if len(names) == 1
            else f"Which business concept do the {len(names)} data contracts named like '{stem}' hold?"
        )
        ask("unmapped_contract", text, names, names)

    kind_order = {"same_identifier": 0, "cardinality": 1, "unmapped_contract": 2}
    questions.sort(key=lambda q: (-q["impact"], kind_order.get(q["kind"], 3), q["text"]))
    questions_by_kind: Dict[str, int] = {}
    for q in questions:
        questions_by_kind[q["kind"]] = questions_by_kind.get(q["kind"], 0) + 1

    # 6. Order everything deterministically.
    entity_list = sorted(entities.values(), key=lambda e: e["name"])
    for ent in entity_list:
        ent.setdefault("domains", [])
        ent["contracts"].sort(key=lambda c: (_LAYER_ORDER.get(c["layer"] or "", 2), c["name"], c["source_file"] or ""))
        ent["evidence"].sort(key=_ev_key)
    rel_list = [relationships[k] for k in sorted(relationships)]
    for rel in rel_list:
        rel["evidence"].sort(key=_ev_key)

    by_state = {"declared": 0, "inferred": 0, "confirmed": 0}
    by_kind: Dict[str, int] = {}
    for rel in rel_list:
        by_state[rel["state"]] += 1
        by_kind[rel["kind"]] = by_kind.get(rel["kind"], 0) + 1

    return {
        "entities": entity_list,
        "relationships": rel_list,
        "questions": questions[: max(0, int(question_limit))],
        "coverage": {
            "contracts_total": len(parsed),
            "contracts_mapped": sum(1 for pc in parsed if pc.entity),
            "entities": len(entity_list),
            "relationships_by_state": by_state,
            "relationships_by_kind": by_kind,
            "questions_total": len(questions),
            "questions_by_kind": questions_by_kind,
        },
    }
