"""One country follows a record through every relationship the simulator emits.

`country_code` is meant to be a partition key across a whole mesh. That only works if a
record and everything it relates to agree on country: partition a trip under GB and join it
to a driver partitioned under JP, and every cross-country join reads two partitions and every
"UK trips" figure silently includes Tokyo drivers.

The simulator gave riders and drivers a permanent home city, which was the right foundation,
but then broke it in two places:

  * a trip took its city from the RIDER and its driver from the WHOLE pool — a London rider
    was routinely driven by a Tokyo driver;
  * a driver-cancelled request named a driver from the whole pool too.

It also gave every rider and driver a `+44` phone number, whatever their city.

These tests pin the rule the fix establishes — a driver is only ever matched within their own
city — and check it end to end over a real seeded run rather than only on hand-built state,
because hand-built state is how the original tests passed while the rule was broken.
"""

from __future__ import annotations

import csv
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from lakelogic.core import streaming

START = datetime(2026, 9, 11, 0, 0, tzinfo=timezone.utc)

REQUEST = {
    "request_id": "REQ-001",
    "rider_id": "RDR-001",
    "trip_type": "ride",
    "city_code": "LON",
    "pickup_lat": "51.500000",
    "pickup_lng": "-0.120000",
    "dropoff_lat": "51.510000",
    "dropoff_lng": "-0.130000",
    "requested_at": "2026-09-11T08:00:00.000Z",
    "surge_multiplier": "1.25",
}


def _bare(tmp_path):
    return streaming.StreamingSimulator(
        entity_config={},
        landing_root=str(tmp_path),
        start_time=START,
        initial_riders=0,
        initial_drivers=0,
    )


# ── The map itself ───────────────────────────────────────────────────────────


def test_every_simulated_city_has_a_country():
    """A city added to the coordinates without a country would emit a blank partition key."""
    missing = set(streaming._CITY_COORDS) - set(streaming._CITY_COUNTRY)
    assert not missing, f"cities with no country: {sorted(missing)}"


def test_every_country_has_a_dialling_prefix():
    missing = set(streaming._CITY_COUNTRY.values()) - set(streaming._COUNTRY_DIAL)
    assert not missing, f"countries with no dialling prefix: {sorted(missing)}"


def test_the_map_agrees_with_the_reference_city_master():
    """The demo meshes seed `city_master` with these pairs. If the two ever disagree, a
    silver check against `city_master` would quarantine every row from that city."""
    assert streaming._CITY_COUNTRY == {
        "LON": "GB",
        "NYC": "US",
        "BER": "DE",
        "PAR": "FR",
        "TYO": "JP",
        "SYD": "AU",
    }


# ── Matching a driver ────────────────────────────────────────────────────────


def test_a_trip_is_driven_by_a_driver_from_its_own_city(tmp_path):
    """THE DEFECT: with a London and a Tokyo driver in the pool, a London trip must never
    get the Tokyo one. Repeated, because the old code was right some of the time by chance."""
    sim = _bare(tmp_path)
    sim._driver_ids = ["DRV-LON", "DRV-TYO"]
    sim._register_driver("DRV-LON", "LON")
    sim._register_driver("DRV-TYO", "TYO")
    for _ in range(50):
        sim._pending_requests = [dict(REQUEST)]
        (row,) = sim._gen_trip_completed(1, START)
        assert row["driver_id"] == "DRV-LON"


def test_with_no_local_driver_the_request_waits(tmp_path):
    """No driver works in London yet. The request stays pending for a later window rather
    than going to a driver from another country."""
    sim = _bare(tmp_path)
    sim._driver_ids = ["DRV-TYO"]
    sim._register_driver("DRV-TYO", "TYO")
    sim._pending_requests = [dict(REQUEST)]

    assert sim._gen_trip_completed(1, START) == []
    assert [r["request_id"] for r in sim._pending_requests] == ["REQ-001"]


def test_a_driver_cancellation_names_a_driver_from_that_city(tmp_path):
    sim = _bare(tmp_path)
    sim._driver_ids = ["DRV-LON", "DRV-TYO"]
    sim._register_driver("DRV-LON", "LON")
    sim._register_driver("DRV-TYO", "TYO")
    seen = set()
    for _ in range(60):
        sim._pending_requests = [dict(REQUEST)]
        (row,) = sim._gen_trip_cancellations(1, START)
        if row["cancelled_by"] == "driver":
            seen.add(row["driver_id"])
    assert seen == {"DRV-LON"}, f"a foreign driver cancelled a London request: {seen}"


def test_with_no_local_driver_a_cancellation_is_the_riders(tmp_path):
    """Nobody local could have cancelled, so it is not pinned on a foreign driver."""
    sim = _bare(tmp_path)
    sim._driver_ids = ["DRV-TYO"]
    sim._register_driver("DRV-TYO", "TYO")
    for _ in range(40):
        sim._pending_requests = [dict(REQUEST)]
        (row,) = sim._gen_trip_cancellations(1, START)
        assert row["driver_id"] in ("", None)
        assert row["cancelled_by"] == "rider"


# ── End to end, over a real seeded run ───────────────────────────────────────


def _run(tmp_path, windows=4):
    sim = streaming.StreamingSimulator.rideflow_marketplace(
        landing_root=str(tmp_path),
        window_minutes=60,
        start_time=START,
        seed=42,
        initial_riders=120,
        initial_drivers=60,
    )
    list(sim.run(num_windows=windows, micro_batches=2))
    return sim


def _rows(root: Path, entity: str):
    out = []
    for f in (root / entity).rglob("*.csv"):
        with open(f, encoding="utf-8") as h:
            out.extend(csv.DictReader(h))
    return out


@pytest.fixture(scope="module")
def landed(tmp_path_factory):
    root = tmp_path_factory.mktemp("rideflow")
    _run(root)
    return root


ENTITIES = [
    "rider_profiles",
    "driver_profiles",
    "trip_requests",
    "trip_completed",
    "trip_cancellations",
    "driver_telemetry",
    "rider_app_events",
]


@pytest.mark.parametrize("entity", ENTITIES)
def test_every_entity_carries_a_country_that_matches_its_city(landed, entity):
    """Every row, every entity — a blank or mismatched key would land in the wrong partition."""
    rows = _rows(landed, entity)
    assert rows, f"{entity}: nothing landed, so this test would pass for the wrong reason"
    bad = [r for r in rows if r.get("country_code") != streaming._CITY_COUNTRY.get(r.get("city_code"))]
    assert not bad, f"{entity}: {len(bad)} rows whose country does not follow their city, e.g. {bad[0]}"


def test_no_trip_joins_across_countries(landed):
    """The property the partition key depends on: rider, trip and driver share one country."""
    rider_cc = {r["rider_id"]: r["country_code"] for r in _rows(landed, "rider_profiles")}
    driver_cc = {d["driver_id"]: d["country_code"] for d in _rows(landed, "driver_profiles")}
    trips = _rows(landed, "trip_completed")
    assert trips, "no trips were completed — the check below would be vacuous"

    crossed = [
        t
        for t in trips
        if (t["driver_id"] in driver_cc and driver_cc[t["driver_id"]] != t["country_code"])
        or (t["rider_id"] in rider_cc and rider_cc[t["rider_id"]] != t["country_code"])
    ]
    assert not crossed, f"{len(crossed)} of {len(trips)} trips join across countries, e.g. {crossed[0]}"


def test_trips_really_span_several_countries(landed):
    """Guard on the guard: if every trip were in one city, the check above would pass
    without testing anything."""
    assert len({t["country_code"] for t in _rows(landed, "trip_completed")}) >= 3


def test_phone_numbers_carry_their_countrys_prefix(landed):
    for entity, key in (("rider_profiles", "rider_id"), ("driver_profiles", "driver_id")):
        for r in _rows(landed, entity):
            prefix = streaming._COUNTRY_DIAL[r["country_code"]]
            assert r["phone"].startswith(prefix), f"{entity} {r[key]} in {r['country_code']} has {r['phone']}"


def test_driver_cancellations_stay_in_country(landed):
    driver_cc = {d["driver_id"]: d["country_code"] for d in _rows(landed, "driver_profiles")}
    for c in _rows(landed, "trip_cancellations"):
        if c["driver_id"]:
            assert driver_cc.get(c["driver_id"]) == c["country_code"], c


# ── Resume ───────────────────────────────────────────────────────────────────


def test_resume_rebuilds_the_city_index(tmp_path):
    """A resumed run must still match drivers by city — otherwise the first window after a
    restart would fall back to having no local drivers and leave every request pending."""
    _run(tmp_path, windows=2)
    resumed = streaming.StreamingSimulator.rideflow_marketplace(
        landing_root=str(tmp_path),
        window_minutes=60,
        start_time=START,
        seed=42,
        initial_riders=0,
        initial_drivers=0,
    )
    list(resumed.run(num_windows=1, micro_batches=1, resume=True))
    by_city = defaultdict(set)
    for did, city in resumed._driver_cities.items():
        by_city[city].add(did)
    for city, ids in by_city.items():
        assert set(resumed._drivers_by_city.get(city, [])) >= ids, f"{city}: index missing resumed drivers"


def test_country_for_city_is_public_and_is_the_one_the_simulator_stamps():
    """The demo meshes generate landing data from CONTRACTS, not from this simulator, and still
    have to stamp the same country on a row. They import `country_for_city`, so a city added to
    the simulator reaches them — a second copy in each repo is what would drift."""
    from lakelogic.core.streaming import country_for_city

    for city in streaming._CITY_COORDS:
        assert country_for_city(city) == streaming.StreamingSimulator._country(city)
        assert len(country_for_city(city)) == 2, f"{city} has no ISO alpha-2 country"
    assert country_for_city("XYZ") == "", "an unknown city yields blank, never a wrong country"
