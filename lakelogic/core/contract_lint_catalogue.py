"""The lint rule catalogue: what each rule checks, why it matters, a failing and a fixed example.

One entry per rule in :mod:`lakelogic.core.contract_lint`. The entries are PROVEN, not just
written: ``tests/test_contract_lint_catalogue.py`` runs every example through the linter, so
a failing example must raise exactly its rule and a fixed example must raise nothing, and every
rule the linter can raise must have an entry. Fixed examples are also valid OLC contracts unless
the entry says otherwise (``olc_valid=False``: the setting involved is a LakeLogic runtime
setting outside the OLC standard).

The docs page ``docs/contracts/lint_rules.md`` is generated from this module
(``scripts/generate_lint_catalogue.py``), and the OLC ``llms.txt`` links to it, so an LLM
writing a contract can read why each rule exists and what a correct contract looks like.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class RuleEntry:
    code: str
    title: str
    severity: str
    checks: str
    why: str
    failing: str
    fixed: str
    #: The resolved domain/system policy the rule is judged against, when it depends on one.
    context: Optional[Dict[str, Any]] = field(default=None)
    #: False when the fix uses a LakeLogic runtime setting the OLC standard does not define.
    olc_valid: bool = True


_FIELDS = """model:
  fields:
    - { name: trip_id, type: string, required: true }
    - { name: fare, type: float }
"""


def _contract(body: str, *, layer: Optional[str] = None, fields: str = _FIELDS) -> str:
    info = "info: { title: Trips, table_name: trips" + (f", target_layer: {layer}" if layer else "") + " }"
    return f"version: 1.0.0\n{info}\n{fields}{body}"


_WITH_UPDATED_AT = """model:
  fields:
    - { name: trip_id, type: string, required: true }
    - { name: fare, type: float }
    - { name: updated_at, type: timestamp }
"""

_EMAIL_MASKED = "    - { name: rider_email, type: string, pii: true, masking: hash }\n"

_SILVER_GUARDS = """quality:
  row_rules:
    - not_null: trip_id
service_levels:
  freshness: 24h
"""

CATALOGUE: Dict[str, RuleEntry] = {
    entry.code: entry
    for entry in [
        RuleEntry(
            code="PK-002",
            title="Merge, upsert, SCD2 or deduplicate without a key",
            severity="critical",
            checks=(
                "A `merge`/`upsert`/`scd2` strategy, or a `deduplicate`, with no `primary_key` "
                "(or dedup `key_columns`)."
            ),
            why=(
                "These operations match incoming rows to existing ones by key. Without a key there "
                "is nothing to match on, so the result depends on row order: rows are duplicated, "
                "overwritten or dropped differently on every run."
            ),
            failing=_contract("materialization:\n  strategy: merge\n  format: delta\n"),
            fixed=_contract("primary_key: [trip_id]\nmaterialization:\n  strategy: merge\n  format: delta\n"),
        ),
        RuleEntry(
            code="KEY-001",
            title="Deduplicate with no row ordering",
            severity="critical",
            checks="A `deduplicate` with no `sort_by` (or a `deduplicate_by_latest` with no `timestamp_column`).",
            why=(
                "Deduplication keeps one row per key and discards the rest. Without an ordering, "
                "which row survives is undefined, so the kept value can change between runs or "
                "engines. LakeLogic refuses to run this rather than guess: the wrong survivor is "
                "silent data loss."
            ),
            failing=_contract(
                "primary_key: [trip_id]\ntransformations:\n  - deduplicate: { on: [trip_id] }\n",
                fields=_WITH_UPDATED_AT,
            ),
            fixed=_contract(
                "primary_key: [trip_id]\ntransformations:\n"
                "  - deduplicate: { on: [trip_id], sort_by: [updated_at], order: desc }\n",
                fields=_WITH_UPDATED_AT,
            ),
        ),
        RuleEntry(
            code="KEY-002",
            title="Deduplicate sorted by its own key",
            severity="critical",
            checks="A `deduplicate` whose `sort_by` columns are all inside its `on` key.",
            why=(
                "Every row in a duplicate group has the same key, so sorting by the key orders "
                "nothing. The surviving row is as arbitrary as with no `sort_by` at all, but the "
                "contract now looks correct. Sort by a column that differs between duplicates, "
                "normally the record's version time."
            ),
            failing=_contract(
                "primary_key: [trip_id]\ntransformations:\n  - deduplicate: { on: [trip_id], sort_by: [trip_id] }\n",
                fields=_WITH_UPDATED_AT,
            ),
            fixed=_contract(
                "primary_key: [trip_id]\ntransformations:\n"
                "  - deduplicate: { on: [trip_id], sort_by: [updated_at], order: desc }\n",
                fields=_WITH_UPDATED_AT,
            ),
        ),
        RuleEntry(
            code="KEY-003",
            title="`natural_key` is deprecated",
            severity="info",
            checks="A `natural_key` on the contract.",
            why=(
                "`natural_key` is parsed but no engine reads it. `primary_key` is the business key "
                "every load matches on. A contract that states both says the same thing twice, or "
                "uses `natural_key` for what is really a uniqueness rule."
            ),
            failing=_contract("primary_key: [trip_id]\nnatural_key: [trip_id]\n"),
            fixed=_contract("primary_key: [trip_id]\n"),
        ),
        RuleEntry(
            code="TRF-001",
            title="A transformation no engine implements",
            severity="critical",
            checks="A transformation whose operation name is not one the runtime implements.",
            why=(
                "Unknown operations are accepted for forward compatibility, stored, and then "
                "skipped. The contract reads as if it aggregates, the pipeline reports success, and "
                "the table simply lacks that step. Found in practice: gold marts declaring "
                "`aggregate:` whose output was never aggregated."
            ),
            failing=_contract(
                "transformations:\n  - phase: post\n    aggregate: { group_by: [city], count: trips }\n",
                fields="model:\n  fields:\n    - { name: city, type: string }\n    - { name: trips, type: integer }\n",
            ),
            fixed=_contract(
                "transformations:\n  - phase: post\n"
                "    sql: SELECT city, COUNT(*) AS trips FROM source GROUP BY city\n",
                fields="model:\n  fields:\n    - { name: city, type: string }\n    - { name: trips, type: integer }\n",
            ),
        ),
        RuleEntry(
            code="PII-001",
            title="A field that looks like personal data is not tagged",
            severity="warning",
            checks=(
                "A field whose name looks like personal data (email, phone, date of birth, address, "
                "bank details…) without `pii: true`."
            ),
            why=(
                "Untagged personal data is invisible to masking, erasure and access rules, so it "
                "reaches gold and reports in the clear. Tag it, then decide its masking; or confirm "
                "it is not personal data."
            ),
            failing=_contract("", fields=_FIELDS + "    - { name: rider_email, type: string }\n"),
            fixed=_contract("", fields=_FIELDS + _EMAIL_MASKED),
        ),
        RuleEntry(
            code="PII-002",
            title="Personal data with no masking",
            severity="warning (critical when the domain mandates PII protection)",
            checks="A field tagged `pii: true` with no `masking` and no `security_groups`.",
            why=(
                "Tagging records that a field is personal data; masking is what protects it. "
                "Without a strategy the value is served unmasked to everyone who can read the table."
            ),
            failing=_contract("", fields=_FIELDS + "    - { name: rider_email, type: string, pii: true }\n"),
            fixed=_contract("", fields=_FIELDS + _EMAIL_MASKED),
        ),
        RuleEntry(
            code="SENS-001",
            title="Sensitive data with no masking",
            severity="warning",
            checks="A field tagged `sensitive: true` with no `masking` and no `security_groups`.",
            why=(
                "Confidential business data (salaries, pricing, contract terms) needs the same "
                "protection as personal data. Without masking or access groups it surfaces unmasked."
            ),
            failing=_contract("", fields=_FIELDS + "    - { name: driver_payout, type: float, sensitive: true }\n"),
            fixed=_contract(
                "", fields=_FIELDS + "    - { name: driver_payout, type: float, sensitive: true, masking: redact }\n"
            ),
        ),
        RuleEntry(
            code="DEL-001",
            title="A current-state entity with no delete strategy",
            severity="warning (critical when the domain declares an erasure policy)",
            checks="A silver or gold table with a key and a merge/upsert/SCD2/dedup load, but no delete strategy.",
            why=(
                "A merge only adds and updates. When a record is deleted at the source the table "
                "never hears about it, so stale rows live on: counted in metrics and impossible to "
                "erase on request. Declare how deletes arrive: CDC delete events with a soft-delete "
                "column, or, when every load is a full snapshot, `materialization.deletion: "
                "{strategy: snapshot_reconcile}`."
            ),
            failing=_contract(
                "primary_key: [trip_id]\nmaterialization:\n  strategy: merge\n  format: delta\n" + _SILVER_GUARDS,
                layer="silver",
            ),
            fixed=_contract(
                "primary_key: [trip_id]\nmaterialization:\n  strategy: merge\n  format: delta\n"
                "  soft_delete_column: is_deleted\n" + _SILVER_GUARDS,
                layer="silver",
                fields=_FIELDS + "    - { name: is_deleted, type: boolean }\n",
            ),
        ),
        RuleEntry(
            code="QLT-001",
            title="A silver table with no quality rules",
            severity="warning",
            checks="A silver table with no row rules, dataset rules or field-level rules.",
            why=(
                "Silver is the enforcement layer: raw data is validated there. With no rules "
                "nothing is checked and quarantine can never fire, so bad rows flow to gold "
                "unchallenged."
            ),
            failing=_contract("service_levels:\n  freshness: 24h\n", layer="silver"),
            fixed=_contract(_SILVER_GUARDS, layer="silver"),
        ),
        RuleEntry(
            code="SCD-001",
            title="SCD2 with no tracked columns",
            severity="critical",
            checks="`materialization.strategy: scd2` with no `materialization.scd2.track_columns`.",
            why=(
                "SCD2 opens a new version when a tracked attribute changes. With none listed, every "
                "load cuts a new version of every row: the history fills with versions that record "
                "no change, and becomes useless."
            ),
            failing=_contract("primary_key: [trip_id]\nmaterialization:\n  strategy: scd2\n  format: delta\n"),
            fixed=_contract(
                "primary_key: [trip_id]\nmaterialization:\n  strategy: scd2\n  format: delta\n"
                "  scd2:\n    track_columns: [fare]\n"
            ),
        ),
        RuleEntry(
            code="SRC-001",
            title="An unpartitioned bronze landing source",
            severity="warning",
            checks="A bronze contract reading a landing zone with no `source.partition`.",
            why=(
                "Without a partition layout every load lists and reads the whole landing zone. It "
                "gets slower as files accumulate, and incremental loads cannot prune to new data."
            ),
            failing=_contract("source:\n  type: landing\n  path: landing/trips/\n  format: json\n", layer="bronze"),
            fixed=_contract(
                "source:\n  type: landing\n  path: landing/trips/\n  format: json\n"
                '  partition:\n    format: "y_%Y/m_%m/d_%d"\n',
                layer="bronze",
            ),
        ),
        RuleEntry(
            code="VOL-001",
            title="No freshness or volume service level",
            severity="info",
            checks=(
                "A silver or gold table with no `service_levels.freshness`/`row_count`, in the contract "
                "or inherited from the domain."
            ),
            why=(
                "Without a freshness or volume expectation, a stalled feed or a half-empty load "
                "looks like success. Nobody is told until a consumer notices the numbers are wrong."
            ),
            failing=_contract("quality:\n  row_rules:\n    - not_null: trip_id\n", layer="silver"),
            fixed=_contract(_SILVER_GUARDS, layer="silver"),
        ),
        RuleEntry(
            code="STREAM-001",
            title="Append on a resumable stream",
            severity="warning",
            checks="A streaming or resumable load (Kafka, Event Hubs, a resumable trigger…) written with `append`.",
            why=(
                "Streams deliver at least once: after a restart the in-flight batch is replayed. "
                "With `append` the replayed rows are written again as duplicates. Merge on a key, "
                "or overwrite."
            ),
            failing=_contract(
                "source:\n  type: stream\n  path: events/trips/\n  format: json\n  trigger: available_now\n"
            ),
            fixed=_contract(
                "primary_key: [trip_id]\nsource:\n  type: stream\n  path: events/trips/\n  format: json\n"
                "  trigger: available_now\nmaterialization:\n  strategy: merge\n  format: delta\n"
            ),
            olc_valid=False,
        ),
        RuleEntry(
            code="STREAM-002",
            title="A continuous trigger",
            severity="info",
            checks="`trigger: continuous`.",
            why=(
                "A continuous trigger needs an always-on cluster, a cost paid every hour. Most "
                "freshness needs are met by `available_now` on scheduled or serverless compute; "
                "keep `continuous` for genuinely sub-minute requirements."
            ),
            failing=_contract(
                "primary_key: [trip_id]\nsource:\n  type: stream\n  path: events/trips/\n  format: json\n"
                "  trigger: continuous\nmaterialization:\n  strategy: merge\n  format: delta\n"
            ),
            fixed=_contract(
                "primary_key: [trip_id]\nsource:\n  type: stream\n  path: events/trips/\n  format: json\n"
                "  trigger: available_now\nmaterialization:\n  strategy: merge\n  format: delta\n"
            ),
            olc_valid=False,
        ),
        RuleEntry(
            code="PHS-001",
            title="A pre-phase rule reads a column only a post step creates",
            severity="critical",
            checks="A rule written `phase: pre` whose SQL reads a column created by a `phase: post` transformation.",
            why=(
                "Pre-phase rules run before post-phase transformations. The column does not exist "
                "yet, so the rule fails on every row and the whole batch is quarantined."
            ),
            failing=_contract(
                "transformations:\n  - phase: post\n    derive: { field: net, sql: fare - fee }\n"
                "quality:\n  row_rules:\n    - { name: net_not_negative, phase: pre, sql: net >= 0 }\n",
                fields=_FIELDS + "    - { name: fee, type: float }\n    - { name: net, type: float }\n",
            ),
            fixed=_contract(
                "transformations:\n  - phase: post\n    derive: { field: net, sql: fare - fee }\n"
                "quality:\n  row_rules:\n    - { name: net_not_negative, phase: post, sql: net >= 0 }\n",
                fields=_FIELDS + "    - { name: fee, type: float }\n    - { name: net, type: float }\n",
            ),
        ),
        RuleEntry(
            code="OWN-001",
            title="An owner with no group to fall back on",
            severity="warning",
            checks="An ownership role held by a named person with no `group`.",
            why=(
                "Questions about the data (what a field means, whether a change is safe) route to "
                "the owner. A lone person with no group is one holiday away from every design "
                "stalling. Name the team alongside the person."
            ),
            failing=_contract("ownership:\n  business_owner: { user: ada@example.com }\n"),
            fixed=_contract("ownership:\n  business_owner: { user: ada@example.com, group: rides-analytics }\n"),
            olc_valid=False,
        ),
        RuleEntry(
            code="OWN-002",
            title="Nobody accountable for blocking questions",
            severity="warning",
            checks="The resolved domain/system ownership names nobody who answers blocking questions.",
            why=(
                "Blocking questions hold a design until someone with authority answers. If no "
                "owner is accountable for them, every such question is unroutable and the design "
                "can never be approved. Judged against the domain's ownership, not the contract alone."
            ),
            failing=_contract(""),
            fixed=_contract(""),
            context={"ownership": {"technical_owner": {"group": "data-platform"}}},
        ),
    ]
}

#: The context that makes OWN-002's fixed example pass: a business owner with a group.
FIXED_CONTEXT: Dict[str, Dict[str, Any]] = {
    "OWN-002": {
        "ownership": {
            "technical_owner": {"group": "data-platform"},
            "business_owner": {
                "group": "rides-analytics",
                "accountable_for": ["blocking_questions"],
            },
        }
    }
}
