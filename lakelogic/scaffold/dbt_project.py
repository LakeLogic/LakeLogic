"""Scaffold a dbt project from data contracts (Snowflake-first).

``scaffold_dbt_project`` renders loose contract YAMLs into a complete dbt project::

    out/
      dbt_project.yml           # project + per-layer schema/materialization config
      packages.yml              # dbt_utils, only when a generated test needs it
      profiles.yml.example      # Snowflake profile skeleton (env vars, no secrets)
      contracts/<layer>/*.yaml  # the contracts, byte-for-byte — STILL canonical
      models/sources.yml        # bronze contracts + entry-layer inputs as dbt sources
      models/silver/<entity>.sql + schema.yml
      models/gold/<entity>.sql  + schema.yml
      README.md

dbt here is a **compile target for contracts, never a replacement**: the contract stays
the canonical design (shipped in-bundle, never re-serialized), the dbt files are derived
from it, and the existing read-direction adapter (:mod:`lakelogic.adapters.dbt`) is the
**drift gate** — importing the generated ``schema.yml`` back yields the same fields,
required flags, and rules, so a hand-edit to the dbt project is detectable against the
contract.

Mapping (aligned with the read adapter, so the round-trip closes):

============================  ==================================================
contract                      dbt
============================  ==================================================
bronze contract               ``sources.yml`` entry (raw landing is not a model)
silver/gold contract          model (``.sql`` + ``schema.yml`` entry)
``model.fields`` types        ``data_type`` (Snowflake names) + contract enforced
``required: true``            column test ``not_null``
``deduplicate`` transform     ``QUALIFY row_number()`` in SQL + ``unique`` test
                              (composite → ``dbt_utils.unique_combination_of_columns``)
``quality.row_rules`` SQL     ``dbt_utils.expression_is_true`` on the rule's column
``upstream``                  ``{{ ref(...) }}`` (models) / ``{{ source(...) }}``
``logic`` SQL                 the model body, verbatim
provenance                    SQL headers + ``meta.lakelogic`` + README
============================  ==================================================

A model with multiple inputs and no ``logic`` SQL is refused (fail-closed) — the
scaffold never invents a join.

Mapping edges, stated honestly:

* A row rule whose SQL names **no** declared field falls back to a *model-level*
  ``expression_is_true`` — the SQL survives, but the read adapter reclassifies a
  model-level test as a **dataset** rule on round-trip (rule category flips, text
  intact). Rules in practice name their column, which keeps them row rules.
* ``accepted_values`` tests and PII ``meta`` are **not yet emitted** — the read
  adapter understands both, but no contract producer speaks that vocabulary yet;
  write-direction support lands with a producer.
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import yaml
from loguru import logger

from lakelogic.scaffold.project import (
    Provenance,
    ScaffoldError,
    ScaffoldResult,
    _entity_of,
    _header,
    _iter_contract_paths,
    _layer_of,
    _load_and_validate,
)

_MODEL_LAYERS: Tuple[str, ...] = ("silver", "gold")
_SOURCE_NAME = "raw"

# lakelogic contract types → Snowflake data types. Chosen so the read adapter's
# ``_normalise_type`` maps them back onto the same lakelogic family (varchar→string,
# integer→integer, …; float→double is that adapter's canonical spelling of float).
_SNOWFLAKE_TYPES = {
    "string": "varchar",
    "integer": "integer",
    "float": "float",
    "double": "float",
    "timestamp": "timestamp_ntz",
    "date": "date",
    "boolean": "boolean",
}


def _sf_type(contract_type: str) -> str:
    return _SNOWFLAKE_TYPES.get(str(contract_type).lower(), "varchar")


def _ident(name: str) -> str:
    """A dbt-safe lowercase identifier (project names must be [a-z0-9_])."""
    return re.sub(r"[^a-z0-9_]", "_", name.lower()).strip("_") or "lakehouse"


def _dedup_keys(raw: dict) -> List[str]:
    """The columns a contract's ``deduplicate`` transformation keys on ('by' or 'on')."""
    for xf in raw.get("transformations") or ():
        if isinstance(xf, dict) and xf.get("deduplicate"):
            cfg = xf["deduplicate"] or {}
            return [str(c) for c in (cfg.get("by") or cfg.get("on") or [])]
    return []


def _dedup_ordering(raw: dict) -> str:
    """The ``ORDER BY`` clause deciding which duplicate survives, from the contract.

    The generated SQL must reproduce the contract's ordering. It previously emitted
    ``order by 1`` — ordering by the first projected column, i.e. an arbitrary
    survivor chosen by the generator rather than the author. That is the same silent
    data-loss the contract's required ``sort_by`` exists to prevent, just expressed
    in dbt instead of the engine, and it would diverge from what LakeLogic itself
    produces for the same contract.
    """
    for xf in raw.get("transformations") or ():
        if isinstance(xf, dict) and xf.get("deduplicate"):
            cfg = xf["deduplicate"] or {}
            cols = [str(c) for c in (cfg.get("sort_by") or [])]
            if cols:
                direction = "asc" if str(cfg.get("order", "desc")).lower() == "asc" else "desc"
                return ", ".join(f"{c} {direction}" for c in cols)
    return ""


def _fields_of(raw: dict) -> List[Dict[str, Any]]:
    model = raw.get("model") or {}
    return [f for f in (model.get("fields") or []) if isinstance(f, dict) and f.get("name")]


def _row_rules_of(raw: dict) -> List[Dict[str, Any]]:
    quality = raw.get("quality") or {}
    return [r for r in (quality.get("row_rules") or []) if isinstance(r, dict) and r.get("sql")]


def _rule_column(rule: Dict[str, Any], field_names: Sequence[str]) -> Optional[str]:
    """The column a row rule attaches to: the first *declared* field (in contract
    declaration order) whose name appears anywhere in the rule's SQL, or ``None``
    (→ model-level fallback; see the module docstring's mapping edges)."""
    sql = str(rule.get("sql", ""))
    tokens = set(re.findall(r"[A-Za-z_][A-Za-z0-9_]*", sql))
    for name in field_names:
        if name in tokens:
            return name
    return None


class _ModelPlan:
    """Everything the renderer needs for one silver/gold model (derived, not stored)."""

    def __init__(self, layer: str, entity: str, raw: dict, src_bytes: bytes) -> None:
        self.layer = layer
        self.entity = entity
        self.raw = raw
        self.src_bytes = src_bytes
        self.fields = _fields_of(raw)
        self.dedup_keys = _dedup_keys(raw)
        self.dedup_ordering = _dedup_ordering(raw)
        self.row_rules = _row_rules_of(raw)
        self.upstream = [str(u) for u in (raw.get("upstream") or [])]
        self.logic = (raw.get("logic") or "").strip() or None
        info = raw.get("info") or {}
        self.description = str(info.get("description") or info.get("title") or self.entity)


def _model_sql(
    plan: _ModelPlan,
    model_entities: set,
    source_entities: set,
    provenance: Optional[Provenance],
) -> str:
    """Render one model's SQL: header + (verbatim ``logic`` | generated cast/dedup body)."""
    header = _header(
        f"dbt model {plan.entity} ({plan.layer}) — compiled from its data contract "
        f"(contracts/{plan.layer}/{plan.entity}.yaml, the canonical design)",
        provenance,
    )
    if plan.logic:
        return f"{header}\n\n{plan.logic}\n"

    inputs: List[str] = []
    for up in plan.upstream:
        if up in model_entities:
            inputs.append(f"{{{{ ref('{up}') }}}}")
        elif up in source_entities:
            inputs.append(f"{{{{ source('{_SOURCE_NAME}', '{up}') }}}}")
        else:
            raise ScaffoldError(
                f"{plan.entity}: upstream {up!r} is neither a model nor a source in "
                "this project — the scaffold cannot reference it"
            )
    if not inputs:
        # Entry-layer model: its landing table is declared as a dbt source under the
        # model's own dataset name (point it at your landed raw data — see README).
        inputs = [f"{{{{ source('{_SOURCE_NAME}', '{plan.entity}') }}}}"]
    if len(inputs) > 1:
        raise ScaffoldError(
            f"{plan.entity}: {len(inputs)} upstream inputs but no `logic` SQL in the "
            "contract — the scaffold never invents a join; add the join SQL to the "
            "contract's `logic` field"
        )

    if plan.fields:
        select_cols = ",\n".join(
            f"        cast({f['name']} as {_sf_type(f.get('type', 'string'))}) "
            f"as {f['name']}"
            for f in plan.fields
        )
        body = [
            "with source_data as (",
            f"    select * from {inputs[0]}",
            "),",
            "",
            "typed as (",
            "    select",
            select_cols,
            "    from source_data",
            ")",
            "",
            "select * from typed",
        ]
    else:
        body = [f"select * from {inputs[0]}"]

    if plan.dedup_keys:
        partition = ", ".join(plan.dedup_keys)
        # The contract's own ordering, so the dbt model and the LakeLogic engine
        # keep the SAME row. `sort_by` is required, so this is normally present;
        # the fallback only covers a contract read through a lenient path.
        ordering = plan.dedup_ordering or ", ".join(f"{k} asc" for k in plan.dedup_keys)
        body.append(
            f"qualify row_number() over (partition by {partition} "
            f"order by {ordering}) = 1"
        )
        body.append(f"-- deduplicate: one row per ({partition}), per the contract")

    return header + "\n\n" + "\n".join(body) + "\n"


def _provenance_meta(provenance: Optional[Provenance]) -> Optional[Dict[str, str]]:
    if not provenance or not provenance.lines():
        return None
    pairs = (
        ("generator", provenance.generator),
        ("revision_hash", provenance.revision_hash),
        ("seal_hash", provenance.seal_hash),
        ("source", provenance.source),
    )
    return {key: value for key, value in pairs if value}


def _schema_entry(plan: _ModelPlan, provenance: Optional[Provenance]) -> Tuple[Dict[str, Any], bool]:
    """One model's ``schema.yml`` entry. Returns (entry, needs_dbt_utils)."""
    needs_utils = False
    field_names = [f["name"] for f in plan.fields]
    rules_by_col: Dict[Optional[str], List[Dict[str, Any]]] = {}
    for rule in plan.row_rules:
        rules_by_col.setdefault(_rule_column(rule, field_names), []).append(rule)

    single_unique = plan.dedup_keys[0] if len(plan.dedup_keys) == 1 else None

    columns: List[Dict[str, Any]] = []
    for f in plan.fields:
        col: Dict[str, Any] = {
            "name": f["name"],
            "data_type": _sf_type(f.get("type", "string")),
        }
        tests: List[Any] = []
        if f.get("required"):
            tests.append("not_null")
        if single_unique == f["name"]:
            tests.append("unique")
        for rule in rules_by_col.get(f["name"], ()):
            needs_utils = True
            tests.append({"dbt_utils.expression_is_true": {"expression": rule["sql"]}})
        if tests:
            col["tests"] = tests
        columns.append(col)

    entry: Dict[str, Any] = {"name": plan.entity, "description": plan.description}
    meta = _provenance_meta(provenance)
    if meta:
        entry["meta"] = {"lakelogic": meta}
    # dbt model contracts need every column typed; a thin (field-less) model skips it.
    if columns:
        entry["config"] = {"contract": {"enforced": True}}
        entry["columns"] = columns
    model_tests: List[Any] = []
    if len(plan.dedup_keys) > 1:
        needs_utils = True
        model_tests.append({
            "dbt_utils.unique_combination_of_columns": {
                "combination_of_columns": list(plan.dedup_keys)
            }
        })
    for rule in rules_by_col.get(None, ()):
        needs_utils = True
        model_tests.append({"dbt_utils.expression_is_true": {"expression": rule["sql"]}})
    if model_tests:
        entry["tests"] = model_tests
    return entry, needs_utils


def _yaml_doc(header: str, doc: Dict[str, Any]) -> str:
    return "\n".join(
        [header, yaml.safe_dump(doc, sort_keys=False, default_flow_style=False,
                                allow_unicode=True)]
    )


def scaffold_dbt_project(
    contracts: Union[Path, Sequence[Path]],
    out_dir: Union[str, Path],
    *,
    domain: Optional[str] = None,
    system: Optional[str] = None,
    profile: Optional[str] = None,
    provenance: Optional[Provenance] = None,
) -> ScaffoldResult:
    """Render contract YAMLs into a runnable dbt project (Snowflake-first).

    Same bars as :func:`~lakelogic.scaffold.project.scaffold_project`: everything is
    validated before anything is written (all-or-nothing), contract files are copied
    byte-for-byte, output is deterministic, and ``provenance`` lands only in generated
    files. ``profile`` names the dbt profile (default: the project name).
    """
    out = Path(out_dir)
    paths = _iter_contract_paths(contracts)

    loaded: List[Tuple[str, str, dict, bytes]] = []  # (layer, entity, raw, bytes)
    inferred_domain: Optional[str] = None
    inferred_system: Optional[str] = None
    seen: Dict[str, str] = {}
    for path in paths:
        raw, contract = _load_and_validate(path)
        layer = _layer_of(raw, contract, path)
        entity = _entity_of(raw, contract, path)
        if entity in seen:
            raise ScaffoldError(
                f"duplicate entity {entity!r} (layers {seen[entity]!r} and {layer!r}) — "
                "entities must be unique across the project"
            )
        seen[entity] = layer
        info = raw.get("info") or {}
        inferred_domain = inferred_domain or info.get("domain")
        inferred_system = inferred_system or info.get("system")
        loaded.append((layer, entity, raw, path.read_bytes()))

    reg_domain = (domain or inferred_domain or "default").strip()
    reg_system = (system or inferred_system or "lakehouse").strip()
    project_name = _ident(f"{reg_domain}_{reg_system}")
    profile_name = profile or project_name

    loaded.sort(key=lambda item: (("bronze", "silver", "gold").index(item[0]), item[1]))
    model_plans = [
        _ModelPlan(layer, entity, raw, data)
        for layer, entity, raw, data in loaded
        if layer in _MODEL_LAYERS
    ]
    bronze = [(entity, raw) for layer, entity, raw, _data in loaded if layer == "bronze"]
    model_entities = {p.entity for p in model_plans}
    source_entities = {entity for entity, _raw in bronze}
    # Entry-layer models (no upstream) implicitly declare their own landing source.
    implicit_sources = sorted(
        p.entity for p in model_plans if not p.upstream and not p.logic
    )

    # Validate every model BEFORE writing anything (all-or-nothing) — _model_sql raises
    # on unresolvable upstream / joins the scaffold refuses to invent.
    rendered_sql = {
        p.entity: _model_sql(p, model_entities, source_entities | set(implicit_sources),
                             provenance)
        for p in model_plans
    }
    schema_entries: Dict[str, List[Dict[str, Any]]] = {layer: [] for layer in _MODEL_LAYERS}
    needs_dbt_utils = False
    for p in model_plans:
        entry, needs = _schema_entry(p, provenance)
        schema_entries[p.layer].append(entry)
        needs_dbt_utils = needs_dbt_utils or needs

    files: List[Path] = []

    def _write(path: Path, text: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8", newline="\n")
        files.append(path)

    # -- contracts, byte-for-byte (the canonical design ships in-bundle) -----
    for layer, entity, _raw, data in loaded:
        dest = out / "contracts" / layer / f"{entity}.yaml"
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_bytes(data)
        files.append(dest)

    # -- dbt_project.yml -----------------------------------------------------
    project: Dict[str, Any] = {
        "name": project_name,
        "version": "1.0.0",
        "profile": profile_name,
        "model-paths": ["models"],
        "models": {
            project_name: {
                layer: {"+schema": layer, "+materialized": "table"}
                for layer in _MODEL_LAYERS
                if schema_entries[layer]
            }
        },
    }
    _write(out / "dbt_project.yml",
           _yaml_doc(_header(f"dbt project — {reg_domain}/{reg_system}", provenance),
                     project))

    # -- packages.yml (only when a generated test needs dbt_utils) ----------
    if needs_dbt_utils:
        _write(out / "packages.yml", _yaml_doc(
            _header("dbt packages required by the generated tests", provenance),
            {"packages": [{"package": "dbt-labs/dbt_utils", "version": [">=1.0.0", "<2.0.0"]}]},
        ))

    # -- profiles.yml.example (Snowflake, env-var driven, no secrets) --------
    _write(out / "profiles.yml.example", _yaml_doc(
        _header("Copy to ~/.dbt/profiles.yml and fill via environment variables — "
                "never commit credentials", provenance),
        {
            profile_name: {
                "target": "dev",
                "outputs": {
                    "dev": {
                        "type": "snowflake",
                        "account": "{{ env_var('SNOWFLAKE_ACCOUNT') }}",
                        "user": "{{ env_var('SNOWFLAKE_USER') }}",
                        "password": "{{ env_var('SNOWFLAKE_PASSWORD') }}",
                        "role": "{{ env_var('SNOWFLAKE_ROLE', 'TRANSFORMER') }}",
                        "database": "{{ env_var('SNOWFLAKE_DATABASE') }}",
                        "warehouse": "{{ env_var('SNOWFLAKE_WAREHOUSE') }}",
                        "schema": "silver",
                        "threads": 4,
                    }
                },
            }
        },
    ))

    # -- models/sources.yml --------------------------------------------------
    source_tables: List[Dict[str, Any]] = []
    for entity, raw in bronze:
        info = raw.get("info") or {}
        source_tables.append({
            "name": entity,
            "description": str(info.get("description") or info.get("title") or entity),
        })
    for entity in implicit_sources:
        source_tables.append({
            "name": entity,
            "description": "Landing table for the entry-layer model of the same name — "
                           "point this at your landed raw data.",
        })
    if source_tables:
        _write(out / "models" / "sources.yml", _yaml_doc(
            _header("Raw landing sources (bronze contracts + entry-layer inputs)",
                    provenance),
            {
                "version": 2,
                "sources": [{
                    "name": _SOURCE_NAME,
                    "schema": "{{ env_var('LAKELOGIC_RAW_SCHEMA', 'raw') }}",
                    "tables": source_tables,
                }],
            },
        ))

    # -- per-layer models + schema.yml ---------------------------------------
    for plan in model_plans:
        _write(out / "models" / plan.layer / f"{plan.entity}.sql",
               rendered_sql[plan.entity])
    for layer in _MODEL_LAYERS:
        if schema_entries[layer]:
            _write(out / "models" / layer / "schema.yml", _yaml_doc(
                _header(f"{layer} models — compiled from the canonical contracts; "
                        "the lakelogic dbt adapter reads this back as the drift gate",
                        provenance),
                {"version": 2, "models": schema_entries[layer]},
            ))

    # -- README.md -----------------------------------------------------------
    prov_block = ""
    if provenance and provenance.lines():
        prov_block = "\n".join(
            ["", "## Provenance", ""] + [f"- {line}" for line in provenance.lines()]
        ) + "\n"
    counts = {layer: sum(1 for p in model_plans if p.layer == layer)
              for layer in _MODEL_LAYERS}
    deps_line = "dbt deps\n" if needs_dbt_utils else ""
    _write(out / "README.md", f"""# {reg_domain}/{reg_system} — dbt project

Generated by `lakelogic scaffold --target dbt`. The **contracts in `contracts/` are the
canonical design** — the dbt models, tests, and sources are compiled from them; regenerate
this project rather than hand-editing generated files.

Models: {counts['silver']} silver / {counts['gold']} gold. Bronze contracts and
entry-layer inputs are declared as dbt **sources** (`models/sources.yml`) — point the
`{_SOURCE_NAME}` source (env `LAKELOGIC_RAW_SCHEMA`) at your landed raw data.

## Run (Snowflake)

```bash
cp profiles.yml.example ~/.dbt/profiles.yml   # then export the SNOWFLAKE_* env vars
{deps_line}dbt run
dbt test
```

## Drift gate

The generated `schema.yml` round-trips through the lakelogic dbt adapter — compare a
hand-edited project against the canonical contract:

```bash
lakelogic import-dbt --schema models/silver/schema.yml --output /tmp/readback/
```

## Validate contracts

```bash
lakelogic validate contracts/ --recursive
```
{prov_block}""")

    logger.info(
        "scaffolded dbt project with {} model(s) + {} source(s) into {}",
        len(model_plans), len(source_tables), out,
    )
    return ScaffoldResult(out_dir=out, registry_path=out / "dbt_project.yml",
                          files=tuple(files))
