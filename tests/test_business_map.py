"""derive_business_map: entities and relationships from RideFlow-like contracts.

Customer / Trip / Payment are declared by keys and foreign keys; Refund is inferred from its
name only; Trip is confirmed by the domain file; Customer's two contract descriptions disagree;
two unannotated ``customer_id`` columns raise a question; a name-only link has unknown
cardinality.
"""

from __future__ import annotations

import copy

from lakelogic.core.business_map import derive_business_map


def _contract(name, key, fields, description=None, **extra):
    c = {
        "name": name,
        "source_file": f"contracts/{name}.yaml",
        "primary_key": key,
        "model": {"fields": [dict({"name": f}) if isinstance(f, str) else f for f in fields]},
        "info": {"title": name},
    }
    if description:
        c["info"]["description"] = description
    c.update(extra)
    return c


CONTRACTS = [
    _contract("silver_customers", ["customer_id"], ["customer_id", "full_name"], "A person who requests trips."),
    _contract(
        "silver_trips",
        ["trip_id"],
        [
            "trip_id",
            {"name": "customer_id", "foreign_key": {"contract": "silver_customers", "column": "customer_id"}},
            "fare",
        ],
    ),
    _contract(
        "silver_payments",
        ["payment_id"],
        ["payment_id", {"name": "trip_id", "foreign_key": {"contract": "silver_trips", "column": "trip_id"}}, "customer_id"],
    ),
    _contract("silver_refunds", ["refund_id"], ["refund_id", "payment_id", "customer_id", "amount"]),
    _contract("gold_dim_customer", ["customer_sk"], ["customer_sk", "customer_id"], "A registered rider account."),
    _contract("bronze_misc_feed", [], ["blob"]),
]

DOMAINS = {
    "marketplace": {
        "concepts": {
            "Trip": {
                "definition": "A journey from pickup to drop-off.",
                "also_called": ["Ride"],
                "identifier": "trip_id",
            }
        },
        "relationships": [{"from": "Customer", "verb": "books", "to": "Trip", "cardinality": "one_to_many"}],
    }
}


def _by(items, **match):
    return next(i for i in items if all(i.get(k) == v for k, v in match.items()))


def test_shape_and_coverage() -> None:
    m = derive_business_map(copy.deepcopy(CONTRACTS), copy.deepcopy(DOMAINS))
    assert set(m) == {"entities", "relationships", "questions", "coverage"}
    assert [e["name"] for e in m["entities"]] == ["Customer", "Payment", "Refund", "Trip"]
    cov = m["coverage"]
    assert cov["contracts_total"] == 6
    assert cov["contracts_mapped"] == 5
    assert cov["entities"] == 4
    assert sum(cov["relationships_by_state"].values()) == len(m["relationships"])


def test_declared_keys_and_foreign_keys() -> None:
    m = derive_business_map(copy.deepcopy(CONTRACTS), copy.deepcopy(DOMAINS))
    customer = _by(m["entities"], name="Customer")
    assert customer["identifier"] == "customer_id"
    assert customer["state"] == "inferred"  # named only; keys declare its identifier, not the concept
    assert {c["name"] for c in customer["contracts"]} == {"silver_customers", "gold_dim_customer"}
    assert any(e["kind"] == "key" and e["contract"] == "silver_customers" for e in customer["evidence"])

    pay = _by(m["relationships"], **{"from": "Trip", "to": "Payment"})
    assert pay["state"] == "declared"
    assert pay["cardinality"] == "one_to_many"
    assert pay["evidence"][0]["kind"] == "foreign_key"
    assert pay["evidence"][0]["column"] == "trip_id"


def test_domain_concept_confirmed_and_domain_relationship() -> None:
    m = derive_business_map(copy.deepcopy(CONTRACTS), copy.deepcopy(DOMAINS))
    trip = _by(m["entities"], name="Trip")
    assert trip["state"] == "confirmed"
    assert trip["definition"] == "A journey from pickup to drop-off."
    assert trip["also_called"] == ["Ride"]
    assert any(e["kind"] == "domain_file" for e in trip["evidence"])

    books = _by(m["relationships"], **{"from": "Customer", "to": "Trip"})
    assert books["state"] == "confirmed"
    assert books["verb"] == "books"
    assert books["cardinality"] == "one_to_many"
    kinds = {e["kind"] for e in books["evidence"]}
    assert {"foreign_key", "domain_file"} <= kinds


def test_name_only_refund_is_inferred_with_unknown_cardinality() -> None:
    m = derive_business_map(copy.deepcopy(CONTRACTS), copy.deepcopy(DOMAINS))
    refund = _by(m["entities"], name="Refund")
    assert refund["state"] == "inferred"
    assert refund["evidence"][0]["kind"] in ("key", "named")
    link = _by(m["relationships"], **{"from": "Payment", "to": "Refund"})
    assert link["state"] == "inferred"
    assert link["cardinality"] == "unknown"
    assert any(q["kind"] == "cardinality" and q["subjects"] == ["Payment", "Refund"] for q in m["questions"])


def test_conflicting_definitions_flagged() -> None:
    m = derive_business_map(copy.deepcopy(CONTRACTS), copy.deepcopy(DOMAINS))
    customer = _by(m["entities"], name="Customer")
    assert customer["conflicts"] and customer["conflicts"][0]["field"] == "definition"
    assert set(customer["conflicts"][0]["values"]) == {"A person who requests trips.", "A registered rider account."}


def test_same_identifier_question_and_unmapped_contract() -> None:
    m = derive_business_map(copy.deepcopy(CONTRACTS), copy.deepcopy(DOMAINS))
    same = [q for q in m["questions"] if q["kind"] == "same_identifier"]
    q = _by(same, text=next(x["text"] for x in same if "customer_id" in x["text"]))
    assert "silver_payments.customer_id" in q["subjects"]
    assert "silver_refunds.customer_id" in q["subjects"]
    unmapped = [q for q in m["questions"] if q["kind"] == "unmapped_contract"]
    assert [q["subjects"] for q in unmapped] == [["bronze_misc_feed"]]


def test_annotations_confirm() -> None:
    contracts = copy.deepcopy(CONTRACTS)
    payments = _by(contracts, name="silver_payments")
    payments["entity"] = "Payment"
    payments["model"]["fields"][2].update(identifies="Customer", role="payer")
    m = derive_business_map(contracts, copy.deepcopy(DOMAINS))
    payment = _by(m["entities"], name="Payment")
    assert payment["state"] == "confirmed"
    assert _by(payment["contracts"], name="silver_payments")["state"] == "confirmed"
    assert _by(_by(m["entities"], name="Refund")["contracts"], name="silver_refunds")["state"] == "proposed"
    payer = _by(m["relationships"], **{"from": "Customer", "to": "Payment"})
    assert payer["state"] == "confirmed"
    assert payer["verb"] == "payer"
    assert payer["cardinality"] == "one_to_many"
    # The annotated column no longer raises the same-identifier question for payments.
    for q in m["questions"]:
        if q["kind"] == "same_identifier":
            assert "silver_payments.customer_id" not in q["subjects"] or len(q["subjects"]) > 1


def test_gold_sk_join_and_alias_question() -> None:
    contracts = [
        _contract("gold_dim_customer", ["customer_sk"], ["customer_sk", "customer_id"]),
        _contract("gold_fact_trips", ["trip_id"], ["trip_id", "customer_sk", "rider_id"]),
    ]
    domains = {"m": {"concepts": {"Customer": {"also_called": ["Rider"], "identifier": "customer_id"}}}}
    m = derive_business_map(contracts, domains)
    join = _by(m["relationships"], **{"from": "Customer", "to": "Trip"})
    assert join["state"] == "declared"
    assert join["evidence"][0]["kind"] == "join"
    assert join["cardinality"] == "one_to_many"
    assert any(q["kind"] == "same_identifier" and "gold_fact_trips.rider_id" in q["subjects"] for q in m["questions"])


def test_deterministic_and_does_not_mutate() -> None:
    contracts, domains = copy.deepcopy(CONTRACTS), copy.deepcopy(DOMAINS)
    first = derive_business_map(contracts, domains)
    assert contracts == CONTRACTS and domains == DOMAINS
    assert derive_business_map(list(reversed(copy.deepcopy(CONTRACTS))), copy.deepcopy(DOMAINS)) == first


def test_empty_inputs() -> None:
    m = derive_business_map([], None)
    assert m["entities"] == [] and m["relationships"] == [] and m["questions"] == []
    assert m["coverage"]["contracts_total"] == 0


def test_platform_fallback_keys_owner_and_properties() -> None:
    contracts = [
        {
            "_source_file": "contracts/m/s/trips.yaml",
            "_layer": "silver",
            "_domain": "marketplace",
            "dataset": "trips",
            "info": {"title": "trips", "owner": "data-team"},
            "primary_key": ["trip_id"],
            "model": {"fields": [{"name": "trip_id", "type": "string", "required": True}, {"name": "fare"}]},
        },
        {
            "dataset": "gold_fact_trips",
            "primary_key": ["trip_id"],
            "model": {"fields": [{"name": "trip_id", "type": "string"}]},
        },
    ]
    m = derive_business_map(contracts)
    trip = _by(m["entities"], name="Trip")
    silver = _by(trip["contracts"], name="trips")
    assert silver["source_file"] == "contracts/m/s/trips.yaml"
    assert silver["layer"] == "silver"
    assert silver["state"] == "proposed"
    assert trip["owner"] == "data-team"
    key = trip["properties"][0]
    assert key == {"name": "trip_id", "type": "string", "key": True, "required": True, "mapped": 2, "of": 2}
    assert _by(trip["properties"], name="fare")["mapped"] == 1


def _platform(dataset, layer, system, key, fields, domain="marketplace", source=None):
    """A contract as the platform loads it: a display title as ``name``, the table in ``dataset``."""
    title = f"{layer.title()} — {dataset.replace('_', ' ').title()} ({domain})"
    c = {
        "name": title,
        "dataset": dataset,
        "_layer": layer,
        "_domain": domain,
        "info": {"title": title, "system": system, "domain": domain},
        "primary_key": key,
        "model": {"fields": [{"name": f} for f in fields]},
    }
    if source:
        c["source"] = {"type": "table", "path": f"table:{{domain_catalog}}.{source}"}
    return c


ESTATE = [
    _platform("bronze_rideflow_trips", "bronze", "rideflow", [], ["trip_id", "rider_id"]),
    _platform("silver_rideflow_trips", "silver", "rideflow", ["trip_id"], ["trip_id", "rider_id"],
              source="{bronze_layer}_rideflow_trips"),
    _platform("gold_fact_trips", "gold", "rideflow", ["trip_id"], ["trip_id", "rider_sk", "date_key"]),
    _platform("gold_fact_trips_daily", "gold", "rideflow", ["date_key"], ["date_key", "trips"]),
    _platform("silver_rideflow_rider_profiles", "silver", "rideflow", ["rider_id"], ["rider_id"]),
    _platform("gold_dim_rider_scd2", "gold", "rideflow", ["rider_sk"], ["rider_sk", "rider_id"]),
    _platform("silver_stripe_charges", "silver", "stripe", ["charge_id"], ["charge_id", "rider_id", "trip_id"],
              domain="payments"),
    _platform("gold_agg_acquisition_roi", "gold", "hubspot", [], ["roi"], domain="marketing"),
]


def test_one_entity_across_layers_systems_and_rollups() -> None:
    m = derive_business_map(copy.deepcopy(ESTATE))
    assert [e["name"] for e in m["entities"]] == ["Charge", "Rider", "Trip"]
    trip = _by(m["entities"], name="Trip")
    names = {c["name"] for c in trip["contracts"]}
    assert len(names) == 4 and all("Trips" in n for n in names)  # bronze, silver, fact, daily roll-up
    assert trip["identifier"] == "trip_id"  # never the daily roll-up's date_key
    rider = _by(m["entities"], name="Rider")
    assert rider["identifier"] == "rider_id"  # the business key, not the dimension's surrogate
    assert len(rider["contracts"]) == 2
    assert _by(m["entities"], name="Charge")["domains"] == ["payments"]
    # A roll-up with nothing to roll up is not a new entity: it is asked about.
    assert m["coverage"]["contracts_mapped"] == len(ESTATE) - 1
    assert any(q["kind"] == "unmapped_contract" and "Acquisition" in q["subjects"][0] for q in m["questions"])


def test_name_only_links_are_marked_and_never_say_has() -> None:
    m = derive_business_map(copy.deepcopy(ESTATE))
    assert all(r["verb"] != "has" for r in m["relationships"])
    join = _by(m["relationships"], **{"from": "Rider", "to": "Trip"})
    assert join["kind"] == "join" and join["state"] == "declared"
    # silver_rideflow_trips.rider_id names the same pair: folded into the declared join.
    assert any(e["kind"] == "named" for e in join["evidence"])
    assert ("Trip", "Rider") not in {(r["from"], r["to"]) for r in m["relationships"]}
    guess = _by(m["relationships"], **{"from": "Trip", "to": "Charge"})
    assert guess["kind"] == "name_only"
    assert guess["state"] == "inferred" and guess["cardinality"] == "unknown"
    assert guess["verb"] == "relates to"
    assert m["coverage"]["relationships_by_kind"] == {"join": 1, "name_only": 2}


def test_questions_are_aggregated_ranked_and_capped() -> None:
    m = derive_business_map(copy.deepcopy(ESTATE))
    total = m["coverage"]["questions_total"]
    assert total == len(m["questions"]) and sum(m["coverage"]["questions_by_kind"].values()) == total
    rider_q = [q for q in m["questions"] if q["kind"] == "same_identifier" and "rider_id" in q["text"]]
    assert len(rider_q) == 1  # one per identifier name, not per column x contract
    assert rider_q[0]["impact"] == 5
    impacts = [q["impact"] for q in m["questions"]]
    assert impacts == sorted(impacts, reverse=True)

    capped = derive_business_map(copy.deepcopy(ESTATE), question_limit=1)
    assert len(capped["questions"]) == 1
    assert capped["questions"][0] == m["questions"][0]
    assert capped["coverage"]["questions_total"] == total
