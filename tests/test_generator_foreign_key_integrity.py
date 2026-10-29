"""Related test data follows field ``foreign_key`` declarations: 100% referential integrity.

Customer -> Trip -> Payment. ``trips.rider_id`` names its parent column differently
(``customers.customer_id``), so only the field-level ``foreign_key`` can link them; before it
was read, rider ids were free values and most trips referenced no customer.
"""

from __future__ import annotations

import pytest

from lakelogic.core.generator import DataGenerator

pytest.importorskip("polars")

CUSTOMERS = """
version: '1.0'
info: {title: customers}
primary_key: [customer_id]
model:
  fields:
    - {name: customer_id, type: string}
    - {name: full_name, type: string}
"""

TRIPS = """
version: '1.0'
info: {title: trips}
primary_key: [trip_id]
model:
  fields:
    - {name: trip_id, type: string}
    - name: rider_id
      type: string
      foreign_key: {contract: silver_customers, column: customer_id}
    - {name: fare, type: double}
"""

PAYMENTS = """
version: '1.0'
info: {title: payments}
primary_key: [payment_id]
model:
  fields:
    - {name: payment_id, type: string}
    - name: trip_id
      type: string
      foreign_key: {contract: silver_trips, column: trip_id}
    - {name: amount, type: double}
"""


def test_detects_field_foreign_key_with_different_column_name() -> None:
    import yaml

    rels = DataGenerator._detect_fk_relationships(
        "trips", yaml.safe_load(TRIPS), ["customers", "trips", "payments"], {"customers": ["customer_id"]}
    )
    assert rels == [{"fk_column": "rider_id", "ref_entity": "customers", "ref_column": "customer_id"}]


@pytest.mark.parametrize("seed", [1, 7, 42])
def test_generated_chain_has_full_referential_integrity(seed: int) -> None:
    out = DataGenerator.generate_related(
        contracts={"payments": PAYMENTS, "trips": TRIPS, "customers": CUSTOMERS},
        rows={"customers": 20, "trips": 60, "payments": 120},
        seed=seed,
    )
    customers = set(out["customers"].get_column("customer_id").to_list())
    trips = out["trips"]
    trip_ids = set(trips.get_column("trip_id").to_list())
    riders = trips.get_column("rider_id").to_list()
    pay_trips = out["payments"].get_column("trip_id").to_list()

    assert riders and pay_trips
    assert all(r in customers for r in riders), "every trip's rider is a generated customer"
    assert all(t in trip_ids for t in pay_trips), "every payment references a generated trip"
