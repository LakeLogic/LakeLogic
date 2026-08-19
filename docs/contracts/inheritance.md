---
title: Configuration Resolution & Inheritance
description: The authoritative specification for how _domain.yaml, _system.yaml, environment configuration and an individual data-product contract combine into one resolved contract — precedence, order, and provenance.
---

# Configuration Resolution & Inheritance

This is the **authoritative** description of how LakeLogic combines four configuration
layers into the single resolved contract a pipeline actually executes:

```
_domain.yaml   (domain-wide governance defaults)
    │  ▼ inherited by
_system.yaml   (a source/system + its contract index)
    │  ▼ bound to
environment    (dev / staging / prod / local… — substitution values)
    │  ▼ injected into
contract.yaml  (one portable OLC data-product contract)   ← what runs
```

Everything downstream — the registry models, the validator, the resolver, and agent
behaviour — MUST follow the precedence and order defined here. Where this page and any
other doc disagree, **this page wins**; [Domain Config](domain_config.md) and
[System Config](system_config.md) describe the *fields*, this page describes how they
*combine*.

!!! abstract "Source of truth"
    The behaviour below is implemented in
    [`lakelogic/core/registry.py`](https://github.com/lakelogic/LakeLogic) —
    `DomainRegistry.from_yaml()` (domain→system merge, environment binding,
    system→contract injection) — with two additional per-layer inheritance steps applied
    at execution time in `lakelogic/pipeline/runner.py` (materialization + server). This
    page is a faithful projection of that code, not an aspiration.

---

## 1. The boundary: registry vs. contract

There are **two distinct kinds of file**, and only one of them is an open standard:

| Layer | Files | Portable? | Owned by |
| --- | --- | --- | --- |
| **Registry** | `_domain.yaml`, `_system.yaml` | LakeLogic-specific | LakeLogic (this repo) |
| **Contract** | per-table `*.olc.yaml` | Portable ([OLC](https://github.com/LakeLogic/open-lakehouse-contract)) | the Open Lakehouse Contract standard |

The registry layer expresses **organisation, defaults and inheritance across many
contracts**. A contract expresses **one data product** and is validated by
`OLCContractV1`. The registry keys (`ownership`, `contracts:` index, per-layer
`server`/`materialization` maps, `storage`, `external_sources`, layer aliases, …) are
**not** part of OLC and are **not** subject to OLC conformance. See
[§11, the OLC boundary](#11-the-olc-boundary).

The output of resolution is always a plain contract dict — so a resolved contract is
still a valid OLC contract, just with the registry's defaults folded in.

---

## 2. Discovery

Resolution is anchored on a `_system.yaml`. Given its path:

- **Domain file** is the sibling one directory up: `<domain>/_domain.yaml`
  (`yaml_path.parent.parent / "_domain.yaml"`). If absent, the system runs with no
  domain defaults (not an error).
- **Contracts** are listed explicitly in `_system.yaml`'s `contracts:` index — each with
  a `layer`, `entity`, `path`, and `enabled`. Contracts are **never** auto-discovered by
  globbing; the index is the authority.

```
domains_rideflow/
├── marketplace/
│   ├── _domain.yaml                      ← domain defaults (base)
│   └── google_analytics/
│       ├── _system.yaml                  ← anchor; system overrides
│       └── contracts/
│           ├── bronze/…_app_events_v1.0.yaml   ← referenced by contracts: index
│           └── silver/…_sessions_v1.0.yaml
```

!!! warning "Entity uniqueness is enforced"
    Every `entity` in a system's `contracts:` index must be **globally unique within that
    system**. A duplicate raises at load time (`validate_unique_entities`) — it is a
    referential-integrity error, not a warning.

A referenced contract file that does not exist on disk is **not fatal**: it is logged
(`Contract file not found: …`) and that contract is marked `enabled = false` and skipped.

---

## 3. The resolution pipeline (authoritative order)

Resolution runs in this exact sequence. Order matters: each stage consumes the output of
the previous one.

1. **Load** `_system.yaml` (`raw`).
2. **Domain → System merge** — fold `_domain.yaml` defaults *under* the system values
   (§4). Produces the effective system config.
3. **Validate** — parse the merged config into the typed `DomainRegistry` model. The
   `observatory` block is normalised first.
4. **Environment binding** — pick `environments[<env>]`, build the substitution map, and
   resolve `{placeholders}` in storage/quarantine/metadata/lineage/notifications (§5).
5. **Per-contract injection** — for each *enabled* contract: resolve its path, load it,
   substitute placeholders, then inject system/domain defaults section-by-section (§6).
6. **Execution-time layer defaults** — at run time, `runner.py` applies per-layer
   `materialization` and `server` defaults for the contract's layer (§6.4).

The result of step 5/6 is the `contract_dict` each contract executes with.

---

## 4. Stage A — Domain → System merge

`_domain.yaml` provides the **base**; `_system.yaml` is the **override**. The merge is
**per-key and type-aware**. Two key sets are treated differently.

### 4.1 Inheritable keys (structured defaults)

```
slo · ownership · notifications · quarantine · compliance ·
lineage · materialization · server · cost · observatory · retention
```

For each of these, given the domain value `D` and system value `S`:

| Case | Rule | Winner |
| --- | --- | --- |
| system omits the key (`S is None`) | inherit `D` wholesale | domain (only value) |
| both are **dicts** | **deep merge** `D` then `S` | **system** (per-field) |
| both are **lists** | **concatenate** `D + S` | both kept (system appended) |
| both are **scalars** | keep system as-is | **system** |

!!! note "Deep-merge replaces nested lists"
    `_deep_merge` recurses into nested dicts but **replaces** (does not concatenate) any
    list found *inside* a merged dict. List concatenation applies only when the
    inheritable key is itself a top-level list (e.g. `notifications`).

**Trial validation.** Every merged value is trial-parsed through the `DomainRegistry`
model before being accepted. If the merged shape is invalid, the merge for that key is
**skipped with a warning** and the system's own value is retained — a malformed domain
default can never corrupt a system.

### 4.2 Identity scalars (consistency-locked)

```
domain · bronze_layer · silver_layer · gold_layer · notifications_enabled
```

These invert the usual rule. When **both** files define one and they **differ**, the
**domain value wins** and a mismatch warning is logged — a system cannot silently rename
the medallion layers or reassign its domain:

```
⚠ Config mismatch: _system.yaml has gold_layer='curated'
  but _domain.yaml has gold_layer='gold'. Domain value takes precedence.
```

If the system omits the scalar, it simply inherits the domain's.

### 4.3 Cost currency is domain-authoritative

`cost.currency` is special-cased on top of the normal `cost` deep-merge: the **domain's**
currency is the authoritative reporting currency for budget enforcement and Observatory
roll-ups. A differing `system.cost.currency` is overwritten to the domain currency (with
a warning); system **rates** still apply, but are reported in the domain currency. This
prevents mixed-currency aggregation.

---

## 5. Stage B — Environment binding & substitution

The merged registry is bound to one environment (`from_yaml(..., environment=<env>)`).

1. `environments[<env>]` becomes the substitution map `sub_map`. A missing environment is
   a warning, not an error — substitution simply has fewer values.
2. **Registry identity** is added: `domain`, `system`, `bronze_layer`, `silver_layer`,
   `gold_layer`.
3. **Two self-resolution passes** let environment values reference each other, e.g.
   `data_root: "abfss://{domain}@{storage_account}.dfs.core.windows.net"` resolves
   `{storage_account}` from a sibling env value. Two passes handle one level of chaining.
4. `storage.*` templates are formatted with `sub_map`; unresolved vars log a warning and
   are left intact.
5. `quarantine`, `metadata`, `lineage`, `compliance`, `observatory`, and `notifications`
   are then placeholder-resolved against `sub_map` **plus** the now-resolved `storage.*`
   fields (so they can reference e.g. `{quarantine_root}`).

### 5.1 Storage mode: `uc` vs `direct`

`storage_mode` selects how `*_root` placeholders resolve inside contracts:

- **`uc`** (default; Databricks): `*_root` keeps Unity Catalog Volume / table values.
- **`direct`** (Azure Functions / non-Spark): each `*_root` is overridden by its
  `*_path` cloud-URI equivalent (`landing_root → landing_path`, `bronze_root →
  bronze_path`, …) so contracts resolve to `abfss://`-style storage.

### 5.2 Layer aliases: naming vs file discovery

Layer aliases (`bronze_layer`, `silver_layer`, `gold_layer`) rename layers **in table and
storage naming** and inside contract *content*. They do **not** affect **file discovery**:
`{*_layer}` placeholders in a contract *path* always resolve to the canonical literals
`bronze`/`silver`/`gold`, because that is how files are laid out on disk.

---

## 6. Stage C — System → Contract injection

For each enabled contract, after its path is resolved and its own placeholders are
substituted, system/domain defaults are folded in. **The universal rule is
child-wins:** a value the contract already defines is never overwritten — defaults only
**fill gaps** (`setdefault` semantics), except `compliance`, which is a deep-merge with
the contract as the override.

Injection happens in this order:

### 6.1 `compliance`
`deep_merge(system_compliance, contract_compliance)` — contract fields win, system fills
gaps. After merge, `data_residency` is checked against the environment's `region`; a
mismatch logs a **compliance-violation warning** (it does not block).

### 6.2 `metadata` (+ environment + cost)
- The active `environment` is stamped into `metadata.environment` (`setdefault`, so a
  per-contract override wins). This is what the processor reads as the resolved
  environment for Observatory environment filters.
- System `metadata.*` keys fill gaps (`setdefault`).
- System `cost` is injected at `metadata.cost` (deep-merge, contract wins).

### 6.3 `lineage`, `quarantine`, `observatory`
Each system section fills gaps on the contract's block key-by-key (`setdefault`). A
contract's own `observatory` block is then re-validated and env-resolved.

### 6.4 `materialization` and `server` (per-layer)
These support a **global + per-layer** shape in the registry:

- `materialization._all` — defaults for **every** layer.
- `materialization.<layer>` — overrides for that layer (e.g. `gold`).

The effective default is `{**_all, **<layer>}` (layer overrides global), then folded into
the contract with **contract-wins** (`setdefault`). `server` follows the same per-layer
model, applied at execution time in `runner.py`: the layer's `server` (including nested
`schema_policy`) deep-merges under the contract's own `server`, or seeds a new `server`
block when the contract has none.

### 6.5 Extra pass-through keys
Any additional keys on a `contracts:` index entry (e.g. `schedule`, `frequency`) are
passed through onto the contract dict where the contract does not already define them.

!!! note "Notifications are a *union*, not an override"
    Notification **channels** don't follow child-wins. At dispatch time they are collected
    from three sources — the contract's own `quarantine.notifications`, the registry
    (`_system.yaml` + `_domain.yaml` `notifications`), and `ownership.contacts` — then
    **deduplicated by target** so a channel never fires twice. See
    [Notification Contact Resolution](domain_config.md#notification-contact-resolution).
    The global `notifications_enabled` switch (an identity scalar, §4.2) can disable all of
    them at once.

---

## 7. Precedence, in one table

Reading top to bottom = **most specific wins**, with the two documented inversions.

| Configuration | Winner (highest precedence first) | Merge |
| --- | --- | --- |
| Structured defaults (`slo`, `quality`, `lineage`, `quarantine`, `materialization`, `server`, `observatory`, `cost` rates, `compliance`) | **Contract › System › Domain** | deep-merge / gap-fill |
| List channels (`notifications`) | **Domain + System** (union), then contract may add its own | concatenate |
| Identity scalars (`domain`, `*_layer`, `notifications_enabled`) | **Domain › System** *(inverted)* | domain-locked |
| `cost.currency` | **Domain** *(authoritative)* | domain-locked |
| Environment values (`catalog`, `storage_account`, roots) | selected `environment` only | substitution |

> **The mental model:** the *closer to the data* a file is, the more it wins — **except**
> for the handful of settings whose whole purpose is domain-wide consistency (layer
> names, domain identity, reporting currency), which are deliberately locked at the top.

---

## 8. Provenance

**Available now** — the registry resolver produces a structured, queryable provenance map
for the domain → system merge: `lakelogic.registry.resolve_system(path)` returns a
`ResolvedSystem` whose `.provenance[key]` records the origin (`domain` / `system` /
`system+domain`) and the rule that applied (`inherited`, `deep-merged`, `concatenated`,
`domain-locked`, `currency-normalised`, …). Provenance is tracked **down to each leaf**
inside a deep-merged block via dotted paths — e.g.
`provenance["slo.freshness.bronze.max_delay_minutes"]` tells you which layer set that exact
value. The CLI surfaces it: `lakelogic registry explain <_system.yaml>` (add `--deep`, or
`--key slo.freshness` to trace one path).

**Runtime** still also emits **log lines** during full resolution — each unresolved
placeholder, residency mismatch, and per-contract injection is logged
(`INFO`/`TRACE`/`WARNING`), since environment substitution and per-contract injection live
in `core/registry.py`, not the standalone resolver yet.

The `environment` layer is annotated too: pass `environment=` (CLI `--env dev`) and the
selected env's bindings are recorded (`environments.dev.catalog → env-binding`) along with
every value that references an env variable (`storage.domain_catalog → env-substituted`),
so you can see exactly what changes per environment. The runtime and this resolver now share
**one merge implementation** (`registry.merge`), so they can't drift. The full record, per
resolved key:

- `origin` — `domain` | `system` | `environment` | `contract` | `default`,
- `source_file` and (where available) the line,
- `reason` — `inherited` | `overridden` | `deep-merged` | `concatenated` |
  `domain-locked` | `currency-normalised` | `skipped-invalid`.

This turns "why is `slo.freshness.bronze.max_delay_minutes` 30 here?" into a lookup
instead of a log grep, and gives agents (Builder/Engineer) a resolved-with-provenance
estate to reason over rather than raw files.

---

## 9. Determinism & edge cases

Resolution is **deterministic** — same inputs, same environment, same resolved output.
The behaviours to rely on:

| Situation | Behaviour |
| --- | --- |
| No `_domain.yaml` | System runs with no domain defaults (no error). |
| Duplicate `entity` in a system | **Hard error** at load (`validate_unique_entities`). |
| Referenced contract file missing | Warning; that contract `enabled = false`, skipped. |
| Domain default fails schema when merged | Merge **skipped**, system value retained, warning. |
| Scalar mismatch (`domain`, layers, `notifications_enabled`) | **Domain wins**, warning. |
| `cost.currency` mismatch | **Domain currency** enforced for roll-ups, warning. |
| Unknown environment name | Warning; substitution proceeds with identity vars only. |
| Unresolved `{placeholder}` | Warning; token left intact (never silently blanked). |
| `data_residency` ≠ env `region` | Compliance-violation **warning** (non-blocking). |

!!! danger "Not yet formalised"
    Two integrity properties are enforced only partially today and belong on the
    validator's roadmap: (a) **version compatibility** between a `contracts:` index entry
    and the contract file it points to, and (b) **cycle/ordering guarantees** for any
    future cross-system `external_sources` references. Until then, treat them as
    *conventions*, not guarantees.

---

## 10. Worked example

Given the RideFlow mesh — `marketplace/_domain.yaml` (Slack + email notifications,
per-layer SLOs, GBP budget, GDPR compliance) and `marketing/google_analytics/_system.yaml`
(per-layer `server.schema_policy`, per-layer `materialization`, two bronze + one silver
contract), resolving the `silver_google_analytics_sessions` contract for `environment:
dev` yields a contract dict where:

- **SLOs / quality** come from the domain (system defined none) — inherited wholesale.
- **Notifications** = domain Slack + email channels (system adds none) — concatenated;
  `notifications_enabled` from the system (`false`) is a scalar, but as an identity
  scalar the **domain** value wins if it also set one.
- **`server.schema_policy`** for the silver layer = the system's `silver` block
  (`evolution: strict`, `unknown_fields: quarantine`), gap-filled under the contract's
  own `server`.
- **`materialization`** = `_all` defaults overlaid with the `silver` block (`strategy:
  merge`, `merge_dedup_guard: true`), contract-wins.
- **`compliance`** = domain frameworks/residency deep-merged under any contract-level
  compliance; `data_residency: EU` checked against dev's region.
- **Paths** (`landing_root`, catalog, `storage_account`) resolved from
  `environments.dev` via two substitution passes.

---

## 11. The OLC boundary

OLC defines **individual, portable data-product contracts**. Multi-contract
**organisation, defaults and inheritance** — everything on this page — is implemented by
the **LakeLogic Registry** and is intentionally *not* part of OLC or its conformance
suite. A resolved contract is still a plain OLC contract; the registry only decides what
its fields are before it runs.

If the registry abstractions later prove portable across multiple providers and attract a
second implementation, they can be extracted as a companion open specification
(*Open Lakehouse Registry*) — rather than expanding OLC's surface from "one contract" to
"a mesh."

See also: [Domain Config (`_domain.yaml`)](domain_config.md) ·
[System Config (`_system.yaml`)](system_config.md) ·
[Contract Organization](../organization.md).
