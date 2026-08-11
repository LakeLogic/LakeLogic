"""``generate_related`` reproducibility + per-entity seeding.

Covers two fixes:

* ``window_start`` / ``window_end`` are threaded to every entity, so timestamp columns
  are reproducible from ``(seed, window)`` instead of defaulting to ``datetime.now()``.
* ``unique_entity_seeds`` (default True) gives each entity a distinct seed derived from
  its name, so two entities with an identical schema no longer generate byte-identical
  rows (which could let a child FK satisfy referential integrity against the wrong parent).
"""
from __future__ import annotations

from datetime import datetime

import pytest

pd = pytest.importorskip("pandas")

from lakelogic.core import generator as gen

WINDOW = (datetime(2020, 1, 1), datetime(2021, 1, 1))


def _related(contracts, **kw):
    return gen.DataGenerator.generate_related(
        contracts, rows=8, seed=7, output_format="pandas", **kw
    )


def test_window_makes_timestamps_reproducible():
    contracts = {"events": {"event_id": "string", "created_at": "timestamp"}}
    a = _related(contracts, window_start=WINDOW[0], window_end=WINDOW[1])
    b = _related(contracts, window_start=WINDOW[0], window_end=WINDOW[1])
    # Same seed + same fixed window ⇒ identical, INCLUDING the timestamp column.
    assert a["events"]["created_at"].equals(b["events"]["created_at"])
    # And the timestamps actually fall inside the requested window.
    ts = pd.to_datetime(a["events"]["created_at"])
    assert ts.min() >= WINDOW[0] and ts.max() <= WINDOW[1]


def test_full_related_product_reproducible_with_window():
    contracts = {
        "riders": {"rider_id": "string", "name": "string", "updated_at": "timestamp"},
        "trips": {"trip_id": "string", "rider_id": "string", "fare": "float"},
    }
    rels = [{"child": "trips", "child_column": "rider_id",
             "parent": "riders", "parent_column": "rider_id"}]
    a = _related(contracts, relationships=rels, window_start=WINDOW[0], window_end=WINDOW[1])
    b = _related(contracts, relationships=rels, window_start=WINDOW[0], window_end=WINDOW[1])
    for name in contracts:
        assert a[name].equals(b[name])


def test_referential_integrity_holds_with_window():
    contracts = {
        "riders": {"rider_id": "string", "name": "string"},
        "trips": {"trip_id": "string", "rider_id": "string", "fare": "float"},
    }
    rels = [{"child": "trips", "child_column": "rider_id",
             "parent": "riders", "parent_column": "rider_id"}]
    out = _related(contracts, relationships=rels, window_start=WINDOW[0], window_end=WINDOW[1])
    parents = set(out["riders"]["rider_id"])
    children = set(out["trips"]["rider_id"])
    assert children <= parents


def test_unique_entity_seeds_toggle_for_identical_schemas():
    # Two entities with an IDENTICAL schema and no FK between them.
    contracts = {
        "table_a": {"id": "string", "amount": "float"},
        "table_b": {"id": "string", "amount": "float"},
    }
    # Default (unique_entity_seeds=False): legacy shared-seed ⇒ identical output.
    default = _related(contracts)
    assert default["table_a"].equals(default["table_b"])

    # Opt in (True): distinct per-entity seeds ⇒ distinct output.
    unique = _related(contracts, unique_entity_seeds=True)
    assert not unique["table_a"].equals(unique["table_b"])


def test_generation_is_still_deterministic_per_entity():
    contracts = {"a": {"id": "string", "n": "integer"}, "b": {"id": "string", "n": "integer"}}
    a = _related(contracts)
    b = _related(contracts)
    for name in contracts:
        assert a[name].equals(b[name])  # same seed ⇒ same per-entity output
