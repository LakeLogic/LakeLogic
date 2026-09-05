"""A contract that names its own owner must be the one paged.

The registry resolves ownership across three scopes — domain -> system -> data product
— and writes the deep-merged result onto each contract. Alert routing read the
REGISTRY-level block instead, i.e. only the outer two scopes. So a contract-scoped
owner was parsed, deep-merged, recorded with provenance, linted by OWN-001, and ranked
winning in `SCOPE_ORDER` ... then ignored at the moment an alert was sent.

The case it breaks: the `payments` domain is owned by the Payments team, but the gold
contract `gold_stripe_fact_payment_reconciliation` is jointly owned by Finance Ops, who
must be paged when reconciliation breaks. Before this, their alerts went to the domain
owner and Finance Ops heard nothing.
"""

from __future__ import annotations

from lakelogic.pipeline.runner import _ownership_for_contract

DOMAIN = {
    "domain_owner": "Payments Platform",
    "team": "payments",
    "contacts": [{"name": "Payments On-Call", "email": "payments-oncall@example.com"}],
}
FINANCE_OPS = {
    "domain_owner": "Payments Platform",  # inherited, unchanged
    "team": "payments",
    "contacts": [{"name": "Finance Ops", "email": "finance-ops@example.com"}],
}


class _Registry:
    def __init__(self, ownership):
        self.ownership = ownership


def test_the_contracts_own_owner_is_paged():
    resolved = _ownership_for_contract({"ownership": FINANCE_OPS}, _Registry(DOMAIN))
    assert resolved["contacts"][0]["email"] == "finance-ops@example.com"


def test_a_contract_without_its_own_block_still_pages_the_domain():
    resolved = _ownership_for_contract({"dataset": "orders"}, _Registry(DOMAIN))
    assert resolved["contacts"][0]["email"] == "payments-oncall@example.com"


def test_inherited_fields_survive_a_partial_override():
    """The registry deep-merges, so a contract overriding contacts keeps the rest."""
    resolved = _ownership_for_contract({"ownership": FINANCE_OPS}, _Registry(DOMAIN))
    assert resolved["domain_owner"] == "Payments Platform"
    assert resolved["team"] == "payments"


def test_contract_ownership_is_used_when_the_registry_declares_none():
    """Previously this discarded the only ownership information that existed."""
    resolved = _ownership_for_contract({"ownership": FINANCE_OPS}, _Registry({}))
    assert resolved["contacts"][0]["email"] == "finance-ops@example.com"


def test_an_empty_contract_block_does_not_shadow_the_domain():
    for empty in ({}, None):
        resolved = _ownership_for_contract({"ownership": empty}, _Registry(DOMAIN))
        assert resolved["contacts"][0]["email"] == "payments-oncall@example.com"


def test_no_ownership_anywhere_is_an_empty_dict_not_a_crash():
    assert _ownership_for_contract({}, _Registry({})) == {}
    assert _ownership_for_contract({}, object()) == {}
