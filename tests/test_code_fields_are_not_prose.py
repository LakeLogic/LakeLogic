"""A `<head>_code` column must not be filled with the head noun's prose.

THE LIVE FAILURE: `city_code` — typed as a short code by the contract, joined against a
cities dimension — came out of the generator as "Manchester", "Bristol", "Leeds". Anyone
reading the grid spots it, and nothing downstream joins.

The cause was a rule that is right for the OTHER tails: `city_name` means "the name of a
city", so it defers to the head noun. `city_code` means "the code of a city", and deferring
to the head hands back a city NAME. `country_code`, `currency_code`, `postal_code` and
`product_code` are all exact entries in the hint table purely to dodge this — one field at a
time, until the next one is found by eye.
"""

from lakelogic.core.generator import (
    _REALISTIC_POOLS,
    _is_code_shaped,
    _match_semantic_hint,
)


def test_city_code_does_not_resolve_to_a_city_name():
    assert _match_semantic_hint("city_code") != "city"


def test_city_name_still_does():
    """The head-noun rule is correct for name-like tails and must survive the fix — this is
    the bug it was written for (`city_name` used to produce a PERSON)."""
    assert _match_semantic_hint("city_name") == "city"


def test_an_unlisted_code_field_stops_rather_than_answering_with_prose():
    """`venue` has no hint, and even if it gained one, `venue_code` must not inherit it."""
    assert _match_semantic_hint("driver_code") != "name"
    assert _match_semantic_hint("customer_code") != "name"


def test_the_codes_that_were_listed_explicitly_are_unchanged():
    assert _match_semantic_hint("country_code") == "country_code"
    assert _match_semantic_hint("postal_code") == "postcode"


def test_a_code_shaped_head_may_still_be_inherited():
    assert _is_code_shaped("bothify(text='PROD-####-??')")
    assert _is_code_shaped("country_code")
    assert not _is_code_shaped("city")
    assert not _is_code_shaped("name")


def test_generated_city_codes_are_short_uppercase_and_geocodable():
    """Not three random letters: these are the codes `_CITY_GEO_COORDS` keys on, so a
    correlated lat/lng still lands in the right place."""
    from lakelogic.core.generator import _CITY_GEO_COORDS

    codes = _REALISTIC_POOLS["city_code"]
    assert codes, "city_code needs its own pool — 'city' is checked first and returns names"
    for code in codes:
        assert code.isupper() and len(code) == 3, code
        assert code.lower() in _CITY_GEO_COORDS, code
