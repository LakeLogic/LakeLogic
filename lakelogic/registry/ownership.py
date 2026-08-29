"""Ownership — who answers for a domain, a system, and a data product.

The registry has always carried an ``ownership`` block, but with one person-shaped field
(``domain_owner``) doing four different jobs: answering what the data *means*, signing off
PII, being paged at 3am, and owning cost. Those are different people. This module gives each
job a named role, and gives each role a declared list of what it is **accountable for** — so
a blocking question can be routed to a person instead of to a team-shaped guess.

Design notes worth not re-deriving:

* **Additive, never a rename.** ``domain_owner`` keeps its meaning (the accountable *team*)
  and stays the fallback. Every existing ``_domain.yaml`` / ``_system.yaml`` in every
  customer repo still validates untouched — the new role blocks are optional siblings.
* **Readable first, resolvable second.** A :class:`Party` carries a display ``name`` *and* a
  stable ``user`` id. These files live in the customer's own git and are read by this
  package, which has no IAM and no network: a block containing only an opaque id could not
  be linted, printed, or read in a PR diff. The duplicated string is the price of the file
  standing on its own.
* **Accountability is declared on the person, not matrixed on the question.** A question
  carries a :class:`QuestionCategory`; a party lists the categories it owns. One enum,
  declared once, instead of a routing table maintained forever.
* **Merge is not reimplemented here.** ``ownership`` is already in ``INHERITABLE_KEYS_ORDER``
  and rides ``merge_domain_system``; nested lists (``accountable_for``, ``notify``) already
  *replace* rather than concatenate, because a dict block takes the deep-merge path. So a
  system can genuinely narrow accountability. :func:`resolve_party` walks the already-merged
  configs; it does not merge them.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, Sequence, Tuple


class QuestionCategory(str, Enum):
    """The kind of judgement a question needs — orthogonal to what it is *about*.

    Clarifications separately carry ``subject_kind`` (estate/domain/landing/table/asset),
    which answers "what is this question about". This answers "who is qualified to settle
    it". Both are kept.
    """

    BLOCKING_QUESTIONS = "blocking_questions"   # anything that holds a design
    BUSINESS_SEMANTICS = "business_semantics"   # what does this field/number mean
    PII_SIGNOFF = "pii_signoff"                 # classification and masking
    COST = "cost"                               # budget and per-run spend
    SOURCE_AUTHORITY = "source_authority"       # which source field is authoritative
    OPERATIONAL = "operational"                 # why did this run fail


CATEGORIES = frozenset(c.value for c in QuestionCategory)

# The role keys a scope may declare, in resolution-preference order.
ROLE_ORDER: Tuple[str, ...] = (
    "business_owner",
    "data_steward",
    "technical_owner",
    "support",
)
ROLES = frozenset(ROLE_ORDER)

# Which role answers a category when nobody declared `accountable_for` explicitly.
# Ordered — the first role present at a scope wins.
CATEGORY_DEFAULT_ROLES: Dict[str, Tuple[str, ...]] = {
    QuestionCategory.BLOCKING_QUESTIONS.value: ("business_owner", "data_steward"),
    QuestionCategory.BUSINESS_SEMANTICS.value: ("data_steward", "business_owner"),
    QuestionCategory.PII_SIGNOFF.value: ("data_steward", "business_owner"),
    QuestionCategory.COST.value: ("business_owner", "technical_owner"),
    QuestionCategory.SOURCE_AUTHORITY.value: ("technical_owner",),
    QuestionCategory.OPERATIONAL.value: ("support", "technical_owner"),
}

# Scopes, outermost first. A later scope supersedes an earlier one.
SCOPE_ORDER: Tuple[str, ...] = ("domain", "system", "contract")


class OwnershipError(ValueError):
    """An ownership block that cannot be interpreted."""


def _clean(value: Any) -> Optional[str]:
    s = str(value).strip() if value is not None else ""
    return s or None


class Party:
    """One accountable person or group, plus what they answer for.

    At least one of ``name`` / ``user`` / ``group`` must be present — a party with no
    identity at all is a governance hole wearing a hat.
    """

    __slots__ = ("name", "user", "group", "accountable_for", "notify",
                 "may_waive_questions", "escalation_policy", "extra")

    def __init__(
        self,
        *,
        name: Optional[str] = None,
        user: Optional[str] = None,
        group: Optional[str] = None,
        accountable_for: Optional[Sequence[str]] = None,
        notify: Optional[Sequence[str]] = None,
        may_waive_questions: bool = False,
        escalation_policy: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.name = _clean(name)
        self.user = _clean(user)
        self.group = _clean(group)
        self.accountable_for: Tuple[str, ...] = tuple(accountable_for or ())
        self.notify: Tuple[str, ...] = tuple(notify or ())
        self.may_waive_questions = bool(may_waive_questions)
        self.escalation_policy = _clean(escalation_policy)
        self.extra = dict(extra or {})

    # ── construction ─────────────────────────────────────────────────────────

    @classmethod
    def parse(cls, raw: Any, *, where: str) -> "Party":
        """Build a Party from a raw mapping, validating what we can name.

        ``where`` is a dotted path used in error messages (``marketplace.business_owner``)
        so a failure points at the file and key, not just the shape.
        """
        if not isinstance(raw, dict):
            raise OwnershipError(
                f"{where}: expected a mapping of owner fields, got {type(raw).__name__}"
            )

        known = {"name", "user", "group", "accountable_for", "notify",
                 "may_waive_questions", "escalation_policy"}
        accountable = raw.get("accountable_for") or []
        if isinstance(accountable, str):
            accountable = [accountable]
        if not isinstance(accountable, (list, tuple)):
            raise OwnershipError(
                f"{where}.accountable_for: expected a list of categories, "
                f"got {type(accountable).__name__}"
            )

        bad = [c for c in accountable if str(c) not in CATEGORIES]
        if bad:
            raise OwnershipError(
                f"{where}.accountable_for: unknown "
                f"{'categories' if len(bad) > 1 else 'category'} {sorted(map(str, bad))}. "
                f"Legal values: {sorted(CATEGORIES)}"
            )

        notify = raw.get("notify") or []
        if isinstance(notify, str):
            notify = [notify]

        party = cls(
            name=raw.get("name"),
            user=raw.get("user"),
            group=raw.get("group"),
            accountable_for=[str(c) for c in accountable],
            notify=[str(n) for n in notify],
            may_waive_questions=bool(raw.get("may_waive_questions", False)),
            escalation_policy=raw.get("escalation_policy"),
            extra={k: v for k, v in raw.items() if k not in known},
        )
        if not (party.name or party.user or party.group):
            raise OwnershipError(
                f"{where}: needs at least one of name, user or group — "
                f"an owner with no identity cannot be asked anything"
            )
        return party

    # ── behaviour ────────────────────────────────────────────────────────────

    def answers_for(self, category: str) -> bool:
        """True when this party explicitly declared accountability for *category*."""
        return category in self.accountable_for

    @property
    def display(self) -> str:
        """The most human thing we can say about this party."""
        return self.name or self.user or self.group or "unassigned"

    @property
    def is_single_point(self) -> bool:
        """A named individual with no group to fall back on when they are away."""
        return bool(self.user or self.name) and not self.group

    def to_dict(self) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for key in ("name", "user", "group", "escalation_policy"):
            val = getattr(self, key)
            if val:
                out[key] = val
        if self.accountable_for:
            out["accountable_for"] = list(self.accountable_for)
        if self.notify:
            out["notify"] = list(self.notify)
        if self.may_waive_questions:
            out["may_waive_questions"] = True
        out.update(self.extra)
        return out

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return f"Party({self.display!r}, accountable_for={list(self.accountable_for)})"


class Ownership:
    """A parsed ``ownership`` block at one scope.

    Unknown keys are preserved verbatim (``cost_center``, ``jira_project``, ``contacts``,
    ``domain_owner``, ``team``…): the block has always been free-form and this stays true,
    so nothing in an existing repo breaks. Only the four role keys are interpreted.
    """

    __slots__ = ("roles", "raw")

    def __init__(self, roles: Dict[str, Party], raw: Dict[str, Any]) -> None:
        self.roles = roles
        self.raw = raw

    @classmethod
    def parse(cls, raw: Any, *, where: str = "ownership") -> "Ownership":
        if raw is None:
            return cls({}, {})
        if not isinstance(raw, dict):
            raise OwnershipError(f"{where}: expected a mapping, got {type(raw).__name__}")
        roles: Dict[str, Party] = {}
        for role in ROLE_ORDER:
            if raw.get(role) is None:
                continue
            roles[role] = Party.parse(raw[role], where=f"{where}.{role}")
        return cls(roles, dict(raw))

    # ── legacy fallbacks ─────────────────────────────────────────────────────

    @property
    def team(self) -> Optional[str]:
        """``ownership.team`` — the long-standing group field."""
        return _clean(self.raw.get("team"))

    @property
    def domain_owner(self) -> Optional[str]:
        """``ownership.domain_owner`` — historically a team name, still the last resort."""
        return _clean(self.raw.get("domain_owner"))

    def oncall(self) -> Optional[Party]:
        """The first ``contacts`` entry with ``role: oncall``, as a Party."""
        contacts = self.raw.get("contacts")
        if not isinstance(contacts, list):
            return None
        for c in contacts:
            if isinstance(c, dict) and str(c.get("role", "")).lower() == "oncall":
                return Party(
                    name=c.get("name"),
                    user=c.get("email"),
                    group=self.team,
                    accountable_for=[QuestionCategory.OPERATIONAL.value],
                )
        return None

    def legacy_party(self) -> Optional[Party]:
        """``domain_owner``/``team`` expressed as a Party, for scopes that declare no roles.

        This is what keeps every untouched repo working: a file with only the old fields
        still resolves to *somebody*, just without category precision.
        """
        owner, team = self.domain_owner, self.team
        if not (owner or team):
            return None
        # `domain_owner` has always held a team name in practice, so it is carried as the
        # group rather than promoted to a person we cannot actually contact.
        return Party(group=owner or team, name=None, user=None)

    def party_for(self, category: str) -> Optional[Party]:
        """The party at THIS scope accountable for *category*, or None.

        Explicit ``accountable_for`` wins. Failing that, the role defaults for the category
        are tried in order. Failing that, this scope has no answer and the caller walks out.
        """
        for role in ROLE_ORDER:
            party = self.roles.get(role)
            if party is not None and party.answers_for(category):
                return party
        for role in CATEGORY_DEFAULT_ROLES.get(category, ()):
            party = self.roles.get(role)
            if party is not None:
                return party
        if category == QuestionCategory.OPERATIONAL.value:
            return self.oncall()
        return None

    def __bool__(self) -> bool:
        return bool(self.roles or self.raw)


class Route:
    """Where a question goes, and how we decided.

    ``scope`` is which level answered (``contract``/``system``/``domain``), so the UI can say
    "inherited from the marketplace domain" rather than showing a bare name.
    """

    __slots__ = ("party", "scope", "category", "reason")

    def __init__(
        self,
        party: Optional[Party],
        scope: Optional[str],
        category: str,
        reason: str,
    ) -> None:
        self.party = party
        self.scope = scope
        self.category = category
        self.reason = reason

    @property
    def routable(self) -> bool:
        return self.party is not None

    @property
    def may_waive(self) -> bool:
        return bool(self.party and self.party.may_waive_questions)

    def describe(self) -> str:
        if not self.routable:
            return f"{self.category}: unroutable"
        return f"{self.category} → {self.party.display} ({self.scope}, {self.reason})"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "category": self.category,
            "scope": self.scope,
            "reason": self.reason,
            "routable": self.routable,
            "may_waive": self.may_waive,
            "party": self.party.to_dict() if self.party else None,
        }

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return f"Route({self.describe()})"


def resolve_route(
    category: Any,
    *,
    contract: Any = None,
    system: Any = None,
    domain: Any = None,
) -> Route:
    """Route a question of *category* to the innermost accountable party.

    Walks ``contract → system → domain`` (innermost first, because the innermost scope
    supersedes) and returns the first party accountable for the category. Each argument is a
    raw ``ownership`` mapping or a parsed :class:`Ownership`; ``None`` means that scope
    declares nothing.

    When no scope claims the category, the legacy ``domain_owner``/``team`` fields are tried
    so untouched repos still route somewhere. When even that is empty the result is
    **unroutable** — deliberately not silently assigned to the domain team, because a
    question nobody owns is a finding, not a default.
    """
    cat = category.value if isinstance(category, QuestionCategory) else str(category)
    if cat not in CATEGORIES:
        raise OwnershipError(
            f"unknown question category {cat!r}. Legal values: {sorted(CATEGORIES)}"
        )

    scopes: List[Tuple[str, Ownership]] = []
    for scope_name, raw in (("contract", contract), ("system", system), ("domain", domain)):
        if raw is None:
            continue
        parsed = raw if isinstance(raw, Ownership) else Ownership.parse(raw, where=scope_name)
        scopes.append((scope_name, parsed))

    # Innermost first — `scopes` is already in contract → system → domain order.
    for scope_name, own in scopes:
        for role in ROLE_ORDER:
            party = own.roles.get(role)
            if party is not None and party.answers_for(cat):
                return Route(party, scope_name, cat, "declared")

    for scope_name, own in scopes:
        party = own.party_for(cat)
        if party is not None:
            return Route(party, scope_name, cat, "role-default")

    for scope_name, own in scopes:
        party = own.legacy_party()
        if party is not None:
            return Route(party, scope_name, cat, "legacy-owner")

    return Route(None, None, cat, "no accountable party at any scope")


def lint_ownership(raw: Any, *, where: str = "ownership", is_domain: bool = False) -> List[str]:
    """Advisory findings for one ownership block. Never raises — returns messages.

    Two checks, both of which describe a real failure that is invisible until it bites:
    a lone individual with no group (one holiday stalls every design), and a domain with
    nobody accountable for blocking questions (every question is unroutable).
    """
    findings: List[str] = []
    try:
        own = Ownership.parse(raw, where=where)
    except OwnershipError as exc:
        return [str(exc)]

    for role, party in own.roles.items():
        if party.is_single_point:
            findings.append(
                f"{where}.{role}: {party.display} has no group — if they are unavailable "
                f"nothing can be answered. Add a fallback group."
            )

    if is_domain:
        blocking = QuestionCategory.BLOCKING_QUESTIONS.value
        if own.party_for(blocking) is None and own.legacy_party() is None:
            findings.append(
                f"{where}: nobody is accountable for {blocking} — every blocking question "
                f"in this domain will be unroutable. Declare a business_owner."
            )
    return findings
