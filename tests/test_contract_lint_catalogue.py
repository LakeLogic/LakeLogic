"""Every lint rule has a catalogue entry, and every catalogue example is proven.

The catalogue is what an LLM (or a person) reads to learn why a rule exists and what a correct
contract looks like. An example that does not do what its page says would teach the wrong thing,
so each one is run through the linter here.
"""

import inspect
import re

import pytest
from lakelogic.core.contract_yaml import load_contract_yaml

from lakelogic.core import contract_lint
from lakelogic.core.contract_lint import GovernanceContext, review_contract_dict
from lakelogic.core.contract_lint_catalogue import CATALOGUE, FIXED_CONTEXT


def _codes(contract_yaml, context=None):
    ctx = GovernanceContext(policy=context) if context else None
    return {f.check_id for f in review_contract_dict(load_contract_yaml(contract_yaml), "example", ctx)}


def _rules_the_linter_can_raise():
    source = inspect.getsource(contract_lint)
    return set(re.findall(r'"([A-Z]{2,6}-\d{3})"', source)) - {"PARSE-000"}


def test_every_rule_the_linter_raises_has_an_entry():
    missing = _rules_the_linter_can_raise() - set(CATALOGUE)
    assert not missing, f"lint rules with no catalogue entry: {sorted(missing)}"


def test_no_entry_for_a_rule_that_does_not_exist():
    stale = set(CATALOGUE) - _rules_the_linter_can_raise()
    assert not stale, f"catalogue entries for rules the linter no longer raises: {sorted(stale)}"


@pytest.mark.parametrize("code", sorted(CATALOGUE))
def test_the_failing_example_raises_exactly_its_rule(code):
    entry = CATALOGUE[code]
    assert _codes(entry.failing, entry.context) == {code}


@pytest.mark.parametrize("code", sorted(CATALOGUE))
def test_the_fixed_example_raises_nothing(code):
    entry = CATALOGUE[code]
    assert _codes(entry.fixed, FIXED_CONTEXT.get(code, entry.context)) == set()


@pytest.mark.parametrize("code", sorted(c for c, e in CATALOGUE.items() if e.olc_valid))
def test_the_fixed_example_is_a_valid_olc_contract(code):
    from olc.models import load_strict

    load_strict(load_contract_yaml(CATALOGUE[code].fixed))


@pytest.mark.parametrize("code", sorted(CATALOGUE))
def test_every_entry_explains_itself(code):
    entry = CATALOGUE[code]
    assert entry.title and entry.checks and len(entry.why) > 80, code
