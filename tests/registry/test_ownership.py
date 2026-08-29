"""Ownership schema, inheritance and question routing.

Spec: ``docs/specs/ownership-and-question-routing.md`` (in the SaaS repo).

The load-bearing claims under test:
  * every existing repo still validates with the old five-key ownership block (AC-2);
  * the chain domain -> system -> contract genuinely supersedes at each step (AC-4/5/7);
  * a question routes to the *innermost* party accountable for its category, and an
    unclaimed category is reported as unroutable rather than dumped on the domain (AC-11/12).
"""

from __future__ import annotations

import pytest

from lakelogic.registry.merge import merge_domain_system, merge_scope
from lakelogic.registry.models import DomainManifestV1, SystemManifestV1
from lakelogic.registry.ownership import (
    CATEGORIES,
    Ownership,
    OwnershipError,
    Party,
    QuestionCategory,
    lint_ownership,
    resolve_route,
)
from lakelogic.registry.provenance import Origin, Reason


# ── AC-1 / AC-3: schema ──────────────────────────────────────────────────────


def test_party_requires_some_identity():
    """An owner with no name, user or group cannot be asked anything."""
    with pytest.raises(OwnershipError, match="at least one of name, user or group"):
        Party.parse({"accountable_for": ["cost"]}, where="ownership.business_owner")


def test_party_accepts_name_user_group():
    p = Party.parse(
        {
            "name": "Lena Ubeda",
            "user": "lena.ubeda@rideflow.com",
            "group": "marketplace-leads",
            "accountable_for": ["blocking_questions", "pii_signoff"],
            "notify": ["work_queue", "slack"],
            "may_waive_questions": True,
        },
        where="ownership.business_owner",
    )
    assert p.display == "Lena Ubeda"
    assert p.answers_for("pii_signoff")
    assert not p.answers_for("cost")
    assert p.may_waive_questions is True
    assert p.is_single_point is False


def test_unknown_category_names_the_offender_and_the_legal_set():
    """AC-3 — the error has to be actionable, not just 'invalid'."""
    with pytest.raises(OwnershipError) as exc:
        Party.parse(
            {"name": "X", "accountable_for": ["pii_signof"]},  # typo
            where="ownership.data_steward",
        )
    msg = str(exc.value)
    assert "pii_signof" in msg
    assert "pii_signoff" in msg  # the legal set is printed so the fix is obvious


def test_accountable_for_accepts_a_bare_string():
    p = Party.parse({"group": "g", "accountable_for": "cost"}, where="w")
    assert p.accountable_for == ("cost",)


def test_every_category_default_role_is_a_real_category():
    from lakelogic.registry.ownership import CATEGORY_DEFAULT_ROLES, ROLES

    assert set(CATEGORY_DEFAULT_ROLES) == set(CATEGORIES)
    for roles in CATEGORY_DEFAULT_ROLES.values():
        assert set(roles) <= ROLES


# ── AC-2: nothing existing breaks ────────────────────────────────────────────


LEGACY_OWNERSHIP = {
    "domain_owner": "Platform Data Engineering",
    "team": "data_engineering",
    "contacts": [{"name": "Platform Data Lead", "role": "oncall", "email": "x@y.com"}],
    "cost_center": "platform_engineering",
    "jira_project": "KAN",
}


def test_legacy_domain_manifest_still_validates():
    """AC-2 — the shape every customer repo has today, untouched."""
    m = DomainManifestV1.model_validate({"domain": "marketplace", "ownership": LEGACY_OWNERSHIP})
    assert m.ownership["domain_owner"] == "Platform Data Engineering"


def test_legacy_system_manifest_still_validates():
    m = SystemManifestV1.model_validate({"system": "rideflow", "ownership": LEGACY_OWNERSHIP})
    assert m.ownership["team"] == "data_engineering"


def test_free_form_ownership_keys_are_preserved():
    """Unknown keys inside ownership stay free-form — only role blocks are policed."""
    own = Ownership.parse({**LEGACY_OWNERSHIP, "some_org_field": {"a": 1}})
    assert own.raw["some_org_field"] == {"a": 1}
    assert own.roles == {}


def test_bad_role_block_is_rejected_at_manifest_parse():
    with pytest.raises(ValueError, match="accountable_for"):
        DomainManifestV1.model_validate(
            {"domain": "d", "ownership": {"business_owner": {"name": "L", "accountable_for": ["bogus"]}}}
        )


# ── AC-4 / AC-5 / AC-6: the merge chain ──────────────────────────────────────


DOMAIN_OWNERSHIP = {
    "business_owner": {
        "name": "Lena Ubeda",
        "user": "lena.ubeda@rideflow.com",
        "group": "marketplace-leads",
        "accountable_for": ["blocking_questions", "pii_signoff", "business_semantics", "cost"],
        "may_waive_questions": True,
    },
    "technical_owner": {"group": "data_engineering"},
}


def test_system_inherits_domain_ownership_wholesale():
    """AC-4 — a system that declares nothing gets the domain's owner."""
    merged, prov = merge_domain_system({"system": "rideflow"}, {"ownership": DOMAIN_OWNERSHIP})
    assert merged["ownership"]["business_owner"]["name"] == "Lena Ubeda"
    assert prov["ownership"].origin is Origin.DOMAIN
    assert prov["ownership"].reason is Reason.INHERITED


def test_system_override_keeps_uninvolved_siblings():
    """A system naming its own owner must not silently drop the inherited group."""
    merged, prov = merge_domain_system(
        {"ownership": {"business_owner": {"name": "Sofia Reyes"}}},
        {"ownership": DOMAIN_OWNERSHIP},
    )
    bo = merged["ownership"]["business_owner"]
    assert bo["name"] == "Sofia Reyes"          # overridden
    assert bo["group"] == "marketplace-leads"   # inherited, not lost
    assert prov["ownership.business_owner.name"].origin is Origin.SYSTEM
    assert prov["ownership.business_owner.name"].reason is Reason.OVERRIDDEN
    assert prov["ownership.business_owner.group"].origin is Origin.DOMAIN
    assert prov["ownership.business_owner.group"].reason is Reason.INHERITED


def test_accountable_for_narrows_rather_than_accumulating():
    """AC-6 — a system must be able to take LESS accountability, not only more.

    Top-level list keys concatenate under the merge rules; nested lists inside a dict block
    replace. This pins the nested behaviour, because concatenation here would mean a system
    could never hand business semantics back to the domain.
    """
    merged, prov = merge_domain_system(
        {"ownership": {"business_owner": {"accountable_for": ["pii_signoff"]}}},
        {"ownership": DOMAIN_OWNERSHIP},
    )
    assert merged["ownership"]["business_owner"]["accountable_for"] == ["pii_signoff"]
    leaf = prov["ownership.business_owner.accountable_for"]
    assert (leaf.origin, leaf.reason) == (Origin.SYSTEM, Reason.OVERRIDDEN)


def test_contract_supersedes_system_with_contract_provenance():
    """AC-5 + AC-8 — the third step of the chain, and it can explain itself."""
    resolved_system = {"business_owner": {"name": "Lena Ubeda", "group": "marketplace-leads"}}
    merged, prov = merge_scope(
        resolved_system,
        {"business_owner": {"name": "Marcus Bell"}},
        key="ownership",
        declared_file="contract:fact_trip_daily_kpis",
    )
    assert merged["business_owner"]["name"] == "Marcus Bell"
    assert merged["business_owner"]["group"] == "marketplace-leads"

    overridden = prov["ownership.business_owner.name"]
    assert overridden.origin is Origin.CONTRACT
    assert overridden.reason is Reason.OVERRIDDEN
    assert overridden.source == "contract:fact_trip_daily_kpis"

    inherited = prov["ownership.business_owner.group"]
    assert inherited.origin is Origin.SYSTEM
    assert inherited.reason is Reason.INHERITED


def test_merge_scope_handles_each_side_being_empty():
    inherited_only, prov = merge_scope({"a": 1}, {}, key="ownership")
    assert inherited_only == {"a": 1}
    assert prov["ownership"].origin is Origin.SYSTEM

    declared_only, prov = merge_scope({}, {"a": 1}, key="ownership")
    assert declared_only == {"a": 1}
    assert prov["ownership"].origin is Origin.CONTRACT

    nothing, prov = merge_scope(None, None, key="ownership")
    assert nothing == {} and prov == {}


def test_origin_contract_exists():
    """AC-8 — without this the UI cannot distinguish inherited from overridden-here."""
    assert Origin.CONTRACT.value == "contract"


# ── AC-11 / AC-12 / AC-13: routing ───────────────────────────────────────────


def test_routes_to_the_declared_accountable_party():
    route = resolve_route(QuestionCategory.PII_SIGNOFF, domain=DOMAIN_OWNERSHIP)
    assert route.routable
    assert route.party.display == "Lena Ubeda"
    assert route.scope == "domain"
    assert route.reason == "declared"


def test_innermost_scope_wins():
    """AC-11 — a contract's own steward beats the domain's business owner."""
    contract = {"data_steward": {"name": "Priya Nandan", "accountable_for": ["business_semantics"]}}
    route = resolve_route("business_semantics", contract=contract, domain=DOMAIN_OWNERSHIP)
    assert route.party.display == "Priya Nandan"
    assert route.scope == "contract"


def test_falls_back_to_role_default_when_nobody_declared_the_category():
    """source_authority is claimed by no party here; technical_owner is its default role."""
    route = resolve_route("source_authority", domain=DOMAIN_OWNERSHIP)
    assert route.party.group == "data_engineering"
    assert route.reason == "role-default"


def test_legacy_only_ownership_still_routes_somewhere():
    """AC-2 in behaviour, not just schema: an untouched repo must not go dark."""
    route = resolve_route("blocking_questions", domain=LEGACY_OWNERSHIP)
    assert route.routable
    assert route.party.group == "Platform Data Engineering"
    assert route.reason == "legacy-owner"


def test_unclaimed_category_is_unroutable_not_silently_reassigned():
    """AC-12 — the whole point. Guessing an owner is worse than admitting we have none."""
    bare = {"business_owner": {"name": "Lena", "accountable_for": ["cost"]}}
    route = resolve_route("source_authority", domain=bare)
    assert route.routable is False
    assert route.party is None
    assert route.scope is None
    assert "no accountable party" in route.reason
    assert route.describe().endswith("unroutable")


def test_operational_questions_fall_through_to_oncall_contact():
    route = resolve_route("operational", domain=LEGACY_OWNERSHIP)
    assert route.routable
    assert route.party.display == "Platform Data Lead"


def test_waiver_permission_is_carried_on_the_route():
    """AC-13 — waiving is a declared permission, not an assumption."""
    allowed = resolve_route("blocking_questions", domain=DOMAIN_OWNERSHIP)
    assert allowed.may_waive is True

    strict = {"business_owner": {"name": "Ana", "accountable_for": ["blocking_questions"]}}
    assert resolve_route("blocking_questions", domain=strict).may_waive is False


def test_unroutable_question_cannot_be_waived():
    assert resolve_route("source_authority", domain={"business_owner": {"name": "L"}}).may_waive is False


def test_unknown_category_is_rejected():
    with pytest.raises(OwnershipError, match="unknown question category"):
        resolve_route("not_a_category", domain=DOMAIN_OWNERSHIP)


def test_route_serialises_for_the_api():
    payload = resolve_route("pii_signoff", domain=DOMAIN_OWNERSHIP).to_dict()
    assert payload["scope"] == "domain"
    assert payload["routable"] is True
    assert payload["party"]["user"] == "lena.ubeda@rideflow.com"


# ── AC-9: lint ───────────────────────────────────────────────────────────────


def test_lint_flags_a_person_with_no_fallback_group():
    findings = lint_ownership({"business_owner": {"name": "Lena", "user": "l@x.com"}})
    assert any("no group" in f for f in findings)


def test_lint_is_quiet_when_a_group_exists():
    assert lint_ownership({"business_owner": {"name": "Lena", "group": "leads"}}) == []


def test_lint_flags_a_domain_with_nobody_on_blocking_questions():
    findings = lint_ownership({"technical_owner": {"group": "de"}}, is_domain=True)
    assert any("blocking_questions" in f for f in findings)


def test_lint_accepts_legacy_domain_as_covered():
    """The old fields count as coverage — otherwise every existing repo lights up red."""
    assert lint_ownership(LEGACY_OWNERSHIP, is_domain=True) == []


def test_lint_reports_a_broken_block_instead_of_raising():
    findings = lint_ownership({"business_owner": {"accountable_for": ["cost"]}})
    assert len(findings) == 1 and "at least one of name" in findings[0]


# ── AC-7: end to end through the runtime loader ──────────────────────────────


def _mesh(tmp_path, domain_yaml: str, system_yaml: str, contracts: dict):
    """A minimal on-disk mesh: _domain.yaml, _system.yaml and its contract files."""
    d = tmp_path / "d"
    (d / "s").mkdir(parents=True, exist_ok=True)
    (d / "_domain.yaml").write_text(domain_yaml, encoding="utf-8")
    for name, body in contracts.items():
        (d / "s" / name).write_text(body, encoding="utf-8")
    sp = d / "s" / "_system.yaml"
    sp.write_text(system_yaml, encoding="utf-8")
    return sp


_DOMAIN_YAML = """
domain: marketplace
ownership:
  business_owner:
    name: Lena Ubeda
    user: lena.ubeda@rideflow.com
    group: marketplace-leads
    accountable_for: [blocking_questions, business_semantics]
  technical_owner:
    group: data_engineering
"""

_SYSTEM_YAML = """
system: rideflow
domain: marketplace
contracts:
  - layer: gold
    entity: fact_trip_daily_kpis
    path: fact_trip_daily_kpis.yaml
    enabled: true
"""

_GOLD_CONTRACT = """
name: fact_trip_daily_kpis
layer: gold
ownership:
  business_owner:
    name: Marcus Bell
"""


def test_gold_contract_inherits_domain_ownership_through_from_yaml(tmp_path):
    """AC-7 — the chain has to survive the real loader, not just the merge helpers.

    Before ownership was added to the contract-injection loop, `ownership` was the one
    inheritable block a contract did NOT receive: it inherited its system's cost config but
    not its owner. This asserts the whole path.
    """
    from lakelogic.core.registry import DomainRegistry

    sp = _mesh(tmp_path, _DOMAIN_YAML, _SYSTEM_YAML, {"fact_trip_daily_kpis.yaml": _GOLD_CONTRACT})
    registry = DomainRegistry.from_yaml(str(sp))

    contract = registry.contracts[0].contract_dict
    own = contract["ownership"]

    # Declared on the contract — supersedes.
    assert own["business_owner"]["name"] == "Marcus Bell"
    # Inherited from the domain and NOT dropped by the override.
    assert own["business_owner"]["group"] == "marketplace-leads"
    assert own["business_owner"]["accountable_for"] == ["blocking_questions", "business_semantics"]
    # A whole role the contract never mentioned.
    assert own["technical_owner"]["group"] == "data_engineering"


def test_from_yaml_records_ownership_provenance(tmp_path):
    """AC-8 end to end — the loader can say which scope supplied each leaf."""
    from lakelogic.core.registry import DomainRegistry

    sp = _mesh(tmp_path, _DOMAIN_YAML, _SYSTEM_YAML, {"fact_trip_daily_kpis.yaml": _GOLD_CONTRACT})
    registry = DomainRegistry.from_yaml(str(sp))

    prov = registry._ownership_provenance["fact_trip_daily_kpis"]
    assert prov["ownership.business_owner.name"].origin is Origin.CONTRACT
    assert prov["ownership.business_owner.name"].reason is Reason.OVERRIDDEN
    assert prov["ownership.business_owner.group"].origin is Origin.SYSTEM
    assert prov["ownership.business_owner.group"].reason is Reason.INHERITED


def test_contract_without_ownership_still_inherits(tmp_path):
    from lakelogic.core.registry import DomainRegistry

    sp = _mesh(
        tmp_path, _DOMAIN_YAML, _SYSTEM_YAML,
        {"fact_trip_daily_kpis.yaml": "name: fact_trip_daily_kpis\nlayer: gold\n"},
    )
    registry = DomainRegistry.from_yaml(str(sp))
    own = registry.contracts[0].contract_dict["ownership"]
    assert own["business_owner"]["name"] == "Lena Ubeda"


def test_resolved_contract_ownership_routes_a_question(tmp_path):
    """The point of all of it: a gold product's question reaches a named person."""
    from lakelogic.core.registry import DomainRegistry

    sp = _mesh(tmp_path, _DOMAIN_YAML, _SYSTEM_YAML, {"fact_trip_daily_kpis.yaml": _GOLD_CONTRACT})
    registry = DomainRegistry.from_yaml(str(sp))

    route = resolve_route("business_semantics", contract=registry.contracts[0].contract_dict["ownership"])
    assert route.party.display == "Marcus Bell"
    assert route.party.group == "marketplace-leads"


# ── AC-9: lint checks in the CLI ─────────────────────────────────────────────


def _own_findings(raw, ctx=None):
    from lakelogic.core.contract_lint import review_contract_dict

    return [f for f in review_contract_dict(raw, "c", ctx) if f.check_id.startswith("OWN-")]


def test_own001_flags_a_lone_individual_on_the_contract():
    findings = _own_findings({"name": "c", "ownership": {"business_owner": {"name": "Lena", "user": "l@x.com"}}})
    assert [f.check_id for f in findings] == ["OWN-001"]
    assert findings[0].severity == "warning"
    assert "group" in findings[0].suggestion


def test_own001_is_quiet_when_a_group_exists():
    assert _own_findings({"name": "c", "ownership": {"business_owner": {"name": "Lena", "group": "leads"}}}) == []


def test_own001_flags_an_inherited_lone_individual():
    """The contract is clean but inherits the problem — flag it where it is read."""
    from lakelogic.core.contract_lint import GovernanceContext

    ctx = GovernanceContext(policy={"ownership": {"business_owner": {"name": "Lena", "user": "l@x.com"}}})
    assert [f.check_id for f in _own_findings({"name": "c"}, ctx)] == ["OWN-001"]


def test_own002_flags_nobody_accountable_for_blocking_questions():
    from lakelogic.core.contract_lint import GovernanceContext

    ctx = GovernanceContext(policy={"ownership": {"technical_owner": {"group": "de"}}})
    findings = _own_findings({"name": "c"}, ctx)
    assert [f.check_id for f in findings] == ["OWN-002"]
    assert "unroutable" in findings[0].message


def test_own002_is_quiet_for_a_legacy_domain():
    """AC-2 again — an untouched repo must not light up red."""
    from lakelogic.core.contract_lint import GovernanceContext

    ctx = GovernanceContext(policy={"ownership": LEGACY_OWNERSHIP})
    assert [f.check_id for f in _own_findings({"name": "c"}, ctx)] == []


def test_own002_is_silent_without_a_resolved_policy():
    """Standalone lint cannot see the domain file, so it must not accuse."""
    assert _own_findings({"name": "c"}) == []


def test_own002_contract_can_supply_the_missing_owner():
    from lakelogic.core.contract_lint import GovernanceContext

    ctx = GovernanceContext(policy={"ownership": {"technical_owner": {"group": "de"}}})
    raw = {"name": "c", "ownership": {"business_owner": {"name": "M", "group": "g",
                                                         "accountable_for": ["blocking_questions"]}}}
    assert _own_findings(raw, ctx) == []
