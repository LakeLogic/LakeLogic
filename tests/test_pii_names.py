"""Name-based PII: one matcher (lakelogic.core.pii_names) shared by inferrer + profiler."""

from __future__ import annotations

import polars as pl
import pytest

from lakelogic.core import profile as P
from lakelogic.core.bootstrap import ContractInferrer
from lakelogic.core.pii_names import name_tokens, pii_columns_by_name

NOT_SENSITIVE = [
    "trip_type",
    "city_code",
    "surge_multiplier",
    "tip_amount",
    "rider_rating",
    "driver_rating",
    "file_name",
    "status_code",
    "trip_id",
    "cancelled_by",
    "cancellation_id",
    "cancellation_fee",
    "event_name",
    "screen_name",
    "pickup_lat",
    "pickup_lng",
    "shipping_method",
    "zippered",
]
SENSITIVE = [
    "email",
    "phone",
    "rider_email",
    "first_name",
    "date_of_birth",
    "ip_address",
    "home_address",
    "card_number",
    "riderEmail",
    "Customer-Name",
    "licence_number",
    "postcode",
    "user.ip",
    "dob",
]


def _inferrer(cols):
    # Non-PII numeric values so only the NAME can flag a column.
    df = pl.DataFrame({c: [1, 2, 3] for c in cols})
    return set(ContractInferrer.__new__(ContractInferrer)._detect_pii_lightweight(df))


def _profiler(cols):
    return P.detect_sensitive_columns(cols)


DETECTORS = [pytest.param(_inferrer, id="inferrer"), pytest.param(_profiler, id="profiler")]


@pytest.mark.parametrize("detect", DETECTORS)
@pytest.mark.parametrize("col", NOT_SENSITIVE)
def test_generic_names_not_pii(detect, col):
    assert detect([col, "amount"]) == set()


@pytest.mark.parametrize("detect", DETECTORS)
@pytest.mark.parametrize("col", SENSITIVE)
def test_person_names_pii(detect, col):
    assert detect([col, "amount"]) == {col}


@pytest.mark.parametrize("detect", DETECTORS)
def test_bare_name_needs_person_context(detect):
    assert detect(["name", "price"]) == set()
    assert detect(["name", "email"]) == {"name", "email"}


def test_types_and_tokens():
    assert name_tokens("riderEmail-Address.v2") == ["rider", "email", "address", "v2"]
    assert pii_columns_by_name(["date_of_birth", "ip_address", "card_number"]) == {
        "date_of_birth": "date_of_birth",
        "ip_address": "ip_address",
        "card_number": "credit_card",
    }


def test_inferrer_value_detection_unchanged():
    df = pl.DataFrame({"contact_info": ["a@b.com", "c@d.org"], "notes": ["x", "y"]})
    assert ContractInferrer.__new__(ContractInferrer)._detect_pii_lightweight(df) == {"contact_info": "email"}


def test_explicit_still_wins():
    assert P.detect_sensitive_columns(["city_code"], sensitive_columns=["city_code"]) == {"city_code"}
