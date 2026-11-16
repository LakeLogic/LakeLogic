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
        [
            "payment_id",
            {"name": "trip_id", "foreign_key": {"contract": "silver_trips", "column": "trip_id"}},
            "customer_id",
        ],
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
    _platform(
        "silver_rideflow_trips",
        "silver",
        "rideflow",
        ["trip_id"],
        ["trip_id", "rider_id"],
        source="{bronze_layer}_rideflow_trips",
    ),
    _platform("gold_fact_trips", "gold", "rideflow", ["trip_id"], ["trip_id", "rider_sk", "date_key"]),
    _platform("gold_fact_trips_daily", "gold", "rideflow", ["date_key"], ["date_key", "trips"]),
    _platform("silver_rideflow_rider_profiles", "silver", "rideflow", ["rider_id"], ["rider_id"]),
    _platform("gold_dim_rider_scd2", "gold", "rideflow", ["rider_sk"], ["rider_sk", "rider_id"]),
    _platform(
        "silver_stripe_charges",
        "silver",
        "stripe",
        ["charge_id"],
        ["charge_id", "rider_id", "trip_id"],
        domain="payments",
    ),
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


# ── Measures: appears_in / curated_in / facts ────────────────────────────────

MEASURE_ESTATE = [
    _contract("silver_rideflow_driver_profiles", ["driver_id"], ["driver_id", "city_code", "status"], system="rideflow"),
    _contract("silver_rideflow_trips", ["trip_id"], ["trip_id", "driver_id", "rider_id", "status"], system="rideflow"),
    _contract("silver_stripe_payouts", ["payout_id"], ["payout_id", "driver_id", "status"], system="stripe"),
    _contract("silver_checkr_checks", ["report_id"], ["report_id", "candidate_driver_id"], system="checkr"),
    _contract("gold_dim_driver_profiles", ["driver_sk"], ["driver_sk", "driver_id", "city_code"]),
    _contract("gold_fact_trips", ["trip_id"], ["trip_id", "driver_id", "fare"]),
    _contract("bronze_rideflow_trips", ["trip_id"], ["trip_id", "driver_id"], system="rideflow"),
]


def test_appears_in_counts_silver_contracts_with_the_identifier_column() -> None:
    m = derive_business_map(copy.deepcopy(MEASURE_ESTATE))
    driver = _by(m["entities"], name="Driver")
    # Exact name only: candidate_driver_id is a different column; bronze and gold are not counted.
    assert driver["appears_in"] == ["silver_rideflow_driver_profiles", "silver_rideflow_trips", "silver_stripe_payouts"]
    assert driver["curated_in"] == ["gold_dim_driver_profiles"]
    assert driver["facts"] == ["gold_fact_trips"]


def test_measures_skip_non_identifier_names() -> None:
    m = derive_business_map(copy.deepcopy(MEASURE_ESTATE))
    for ent in m["entities"]:
        assert set(ent) >= {"appears_in", "curated_in", "facts"}
        if ent["identifier"] in ("status", "Id") or not ent["measured_by"]:
            assert ent["appears_in"] == [] and ent["curated_in"] == [] and ent["facts"] == []


def test_measures_are_order_independent() -> None:
    a = derive_business_map(copy.deepcopy(MEASURE_ESTATE))
    b = derive_business_map(list(reversed(copy.deepcopy(MEASURE_ESTATE))))
    assert [(e["name"], e["appears_in"], e["curated_in"], e["facts"]) for e in a["entities"]] == [
        (e["name"], e["appears_in"], e["curated_in"], e["facts"]) for e in b["entities"]
    ]


# An airline estate: composite keys and code columns (Tagus Air shape).
AIRLINE = [
    _contract("silver_ssim_flight_legs", ["airline_designator", "flight_number", "leg_sequence", "period_from"],
              ["airline_designator", "flight_number", "leg_sequence", "period_from", "service_type"], system="ssim"),
    _contract("gold_fact_flight_legs", ["airline_designator", "flight_number", "leg_sequence", "period_from"],
              ["airline_designator", "flight_number", "leg_sequence", "period_from"]),
    _contract("silver_pss_pnr_segments", ["record_locator", "passenger_token", "segment_number"],
              ["record_locator", "passenger_token", "segment_number", "cabin", "flight_number", "status"], system="pss"),
    _contract("gold_dim_cabin", ["cabin"], ["cabin_sk", "cabin"]),
    _contract("silver_crm_people", ["status"], ["status"], system="crm"),
    _contract("gold_dim_status", ["status"], ["status_sk", "status"]),
]


def test_composite_key_entities_are_measured_on_the_full_key() -> None:
    m = derive_business_map(copy.deepcopy(AIRLINE))
    leg = next(e for e in m["entities"] if "silver_ssim_flight_legs" in e["appears_in"])
    assert leg["measured_by"] == ["airline_designator", "flight_number", "leg_sequence", "period_from"]
    assert leg["facts"] == ["gold_fact_flight_legs"]
    # flight_number alone in the PNR contract is not the whole key.
    assert "silver_pss_pnr_segments" not in leg["appears_in"]


def test_code_column_counts_when_a_gold_dimension_is_keyed_on_it() -> None:
    m = derive_business_map(copy.deepcopy(AIRLINE))
    cabin = _by(m["entities"], name="Cabin")
    assert cabin["measured_by"] == ["cabin"]
    assert cabin["appears_in"] == ["silver_pss_pnr_segments"]
    assert cabin["curated_in"] == ["gold_dim_cabin"]
    for e in m["entities"]:
        assert e["measured_by"] != ["status"]


def test_composite_identifier_columns_are_returned() -> None:
    m = derive_business_map(copy.deepcopy(AIRLINE))
    leg = next(e for e in m["entities"] if "silver_ssim_flight_legs" in e["appears_in"])
    assert leg["identifier"] is None
    assert leg["identifier_columns"] == ["airline_designator", "flight_number", "leg_sequence", "period_from"]
    cabin = _by(m["entities"], name="Cabin")
    assert cabin["identifier_columns"] == ["cabin"]


def test_code_only_dimension_is_an_attribute_of_the_concept_using_it() -> None:
    m = derive_business_map(copy.deepcopy(AIRLINE))
    cabin = _by(m["entities"], name="Cabin")
    pnr = next(e for e in m["entities"] if "silver_pss_pnr_segments" in e["appears_in"] and e["name"] != "Cabin")
    assert cabin["attribute_of"] == [pnr["name"]]
    assert pnr["attribute_of"] == []


def test_gold_fact_sql_and_composite_keys_infer_links() -> None:
    estate = copy.deepcopy(AIRLINE) + [
        _contract("silver_movement_messages", ["message_id"], ["message_id", "carrier", "flight_number"], system="mvt"),
        dict(
            _contract("gold_fact_connections_at_risk", ["record_locator", "passenger_token", "flight_date"],
                      ["record_locator", "passenger_token", "flight_date"]),
            transformations=[{"sql": "WITH legs AS (SELECT * FROM silver_ssim_flight_legs) "
                                     "SELECT * FROM silver_pss_pnr_segments AS a JOIN legs AS l ON a.flight_number = l.flight_number"}],
        ),
        _contract("gold_fact_leg_delays", ["airline_designator", "flight_number", "leg_sequence", "period_from", "delay_code"],
                  ["airline_designator", "flight_number", "leg_sequence", "period_from", "delay_code"]),
    ]
    m = derive_business_map(estate)
    kinds = {(r["from"], r["to"]): r["kind"] for r in m["relationships"]}
    risk = next(e["name"] for e in m["entities"] if any(c["name"] == "gold_fact_connections_at_risk" for c in e["contracts"]))
    sql_sources = {f for (f, t), k in kinds.items() if t == risk and k == "sql_join"}
    assert len(sql_sources) == 2  # flight leg and PNR segment, via the CTE and the join
    assert all(r["state"] == "inferred" for r in m["relationships"] if r["kind"] in ("sql_join", "composite_key"))
    delays = next(e["name"] for e in m["entities"] if any(c["name"] == "gold_fact_leg_delays" for c in e["contracts"]))
    assert "composite_key" in {k for (f, t), k in kinds.items() if t == delays}
    # flight_number alone (in the messages contract) is not the whole key: no link.
    msg = next(e["name"] for e in m["entities"] if "silver_movement_messages" in e["appears_in"])
    assert not any(k == "composite_key" and t == msg for (f, t), k in kinds.items())


def test_plain_id_concepts_are_measured_and_linked_by_name_id_columns() -> None:
    sf = [
        _contract("silver_salesforce_accounts", ["Id"], ["Id", "Name"], system="salesforce"),
        _contract("silver_salesforce_contacts", ["Id"], ["Id", "AccountId"], system="salesforce"),
        _contract("silver_salesforce_cases", ["Id"], ["Id", "AccountId", "ContactId"], system="salesforce"),
    ]
    m = derive_business_map(sf)
    acct = next(e for e in m["entities"] if any(c["name"] == "silver_salesforce_accounts" for c in e["contracts"]))
    assert acct["measured_by"] == [acct["name"].replace(" ", "") + "Id"]
    assert set(acct["appears_in"]) == {"silver_salesforce_accounts", "silver_salesforce_contacts", "silver_salesforce_cases"}
    refs = [r for r in m["relationships"] if r["kind"] == "reference_column"]
    assert {(r["from"], r["state"]) for r in refs} >= {(acct["name"], "inferred")}


def test_owner_falls_back_to_the_domain_files_owner() -> None:
    estate = [dict(_contract("silver_rideflow_driver_profiles", ["driver_id"], ["driver_id"], system="rideflow"), _domain="marketplace")]
    domains = {"marketplace": {"ownership": {"domain_owner": "Platform Data Engineering"}}}
    driver = _by(derive_business_map(copy.deepcopy(estate), domains)["entities"], name="Driver")
    assert driver["owner"] == "Platform Data Engineering"
    assert driver["owner_scope"] == "domain"
    # No domain file owner either: still unknown, not invented.
    alone = _by(derive_business_map(copy.deepcopy(estate))["entities"], name="Driver")
    assert alone["owner"] is None and alone["owner_scope"] is None


# Shared keys: business keys several concepts carry with no table of their own (Tagus Air shape).
SHARED = [
    _contract("silver_revenue_coupons", ["ticket_number", "coupon_number"],
              ["ticket_number", "coupon_number", "record_locator", "carrier", "flight_number", "flight_date"]),
    _contract("silver_baggage_bag_events", ["event_id"],
              ["event_id", "record_locator", "carrier", "flight_number", "flight_date"]),
    _contract("silver_crew_duties", ["crew_id", "flight_number", "flight_date"],
              ["crew_id", "carrier", "flight_number", "flight_date", "aircraft_registration", "status"]),
    _contract("silver_mro_work_orders", ["work_order_id"], ["work_order_id", "aircraft_registration", "status"]),
    _contract("silver_ssim_flight_legs", ["airline_designator", "flight_number", "leg_sequence"],
              ["airline_designator", "flight_number", "leg_sequence"]),
    _contract("gold_dim_carrier", ["carrier"], ["carrier_sk", "carrier"]),
    _contract("gold_dim_status", ["status"], ["status_sk", "status"]),
    _contract("gold_dim_date", ["date_key"], ["date_key", "calendar_date", "year", "month"]),
    _contract("gold_fact_coupons_daily", ["date_key"],
              [{"name": "date_key", "foreign_key": {"contract": "gold_dim_date", "column": "date_key"}}, "coupons"]),
]


def test_shared_keys_become_suggested_concepts_linking_their_holders() -> None:
    m = derive_business_map(copy.deepcopy(SHARED))
    shared = {e["name"]: e for e in m["entities"] if e["shared_key"]}
    assert shared["Record locator"]["identifier_columns"] == ["record_locator"]
    assert shared["Aircraft registration"]["appears_in"] == ["silver_crew_duties", "silver_mro_work_orders"]
    # A number is a key only with its companions: a carrier code a Gold lookup is keyed on, and a date.
    assert shared["Flight"]["identifier_columns"] == ["carrier", "flight_date", "flight_number"]
    assert all(e["evidence_state"] == "suggested" for e in shared.values())
    flight_links = {(r["to"], r["kind"]) for r in m["relationships"] if r["from"] == "Flight"}
    assert {kind for _, kind in flight_links} == {"shared_key", "partial_key"}
    # The schedule's flight_number without a carrier and date is a partial match, said as such.
    assert any(kind == "partial_key" for _, kind in flight_links)
    assert all(r["evidence_state"] == "suggested" for r in m["relationships"] if r["kind"] in ("shared_key", "partial_key"))


def test_generic_columns_never_make_or_complete_a_shared_key() -> None:
    m = derive_business_map(copy.deepcopy(SHARED))
    for e in m["entities"]:
        if e["shared_key"]:
            assert "status" not in e["identifier_columns"]
    # One holder is not shared: event_id belongs to bag events alone.
    assert not any(e["shared_key"] and e["identifier_columns"] == ["event_id"] for e in m["entities"])


def test_a_calendar_dimension_is_marked_not_a_concept() -> None:
    m = derive_business_map(copy.deepcopy(SHARED))
    date = next(e for e in m["entities"] if "gold_dim_date" in e["curated_in"] or e["identifier"] == "date_key")
    assert date["calendar"] is True
    assert not any(e["calendar"] for e in m["entities"] if e is not date)


def test_a_shared_key_confirmed_under_a_business_name_keeps_its_key_and_links() -> None:
    domain_files = {"passenger": {"concepts": {"Booking reference": {
        "definition": "A booking.", "also_called": ["Record locator", "PNR"]}}}}
    m = derive_business_map(copy.deepcopy(SHARED), domain_files)
    ref = next(e for e in m["entities"] if e["name"] == "Booking reference")
    assert ref["identifier_columns"] == ["record_locator"] and ref["shared_key"] is True
    assert ref["appears_in"] == ["silver_baggage_bag_events", "silver_revenue_coupons"]
    assert ref["evidence_state"] == "confirmed"
    assert {r["to"] for r in m["relationships"] if r["from"] == "Booking reference"} >= {"Bag event", "Coupon"} or \
        any(r["from"] == "Booking reference" for r in m["relationships"])
    assert not any(e["name"] == "Record locator" for e in m["entities"])
