"""A declared primary key is unique in generated data.

Values are generated one field at a time with no memory of what was used, so a declared key
could repeat: ``TRI-####`` trip ids collided twice in 300 rows. A repeated key is not a key,
and it makes "the parent row this foreign key names" ambiguous for every child built from it —
found while testing country inheritance, where two trips sharing ``TRI-4969`` gave that id's
telemetry two different countries to agree with.

Redraws use their own seeded stream and touch only colliding rows, so a dataset with no
collision is byte-for-byte unchanged.
"""

from __future__ import annotations

from lakelogic.core.generator import DataGenerator


def _contract(pk_line: str, fields: str) -> str:
    return f"version: '1.0'\ninfo:\n  title: trips\n{pk_line}model:\n  fields:\n{fields}"


FIELDS = "    - {name: trip_id, type: string}\n    - {name: rider_id, type: string}\n    - {name: fare, type: double}\n"
WITH_PK = _contract("primary_key: [trip_id]\n", FIELDS)
WITHOUT_PK = _contract("", FIELDS)


def _ids(df, col="trip_id"):
    return df.get_column(col).to_list()


def test_the_collision_that_was_measured_is_gone():
    without = DataGenerator(WITHOUT_PK, seed=7).generate(rows=300)
    assert len(set(_ids(without))) < 300, "the fixture must reproduce a collision"
    with_pk = DataGenerator(WITH_PK, seed=7).generate(rows=300)
    assert len(set(_ids(with_pk))) == 300


def test_only_the_colliding_rows_change():
    without = DataGenerator(WITHOUT_PK, seed=7).generate(rows=300)
    with_pk = DataGenerator(WITH_PK, seed=7).generate(rows=300)
    duplicates = len(_ids(without)) - len(set(_ids(without)))
    changed = sum(1 for a, b in zip(_ids(without), _ids(with_pk)) if a != b)
    assert changed == duplicates
    # every other column is untouched
    assert without.get_column("rider_id").to_list() == with_pk.get_column("rider_id").to_list()
    assert without.get_column("fare").to_list() == with_pk.get_column("fare").to_list()


def test_redrawn_ids_keep_the_id_shape():
    with_pk = DataGenerator(WITH_PK, seed=7).generate(rows=300)
    assert all(str(v).startswith("TRI-") for v in _ids(with_pk))


def test_it_is_deterministic():
    a = DataGenerator(WITH_PK, seed=7).generate(rows=300)
    b = DataGenerator(WITH_PK, seed=7).generate(rows=300)
    assert _ids(a) == _ids(b)


def test_a_composite_key_is_unique_as_a_tuple():
    contract = _contract(
        "primary_key: [country_code, trip_id]\n",
        "    - {name: country_code, type: string, accepted_values: [GB, DE]}\n    - {name: trip_id, type: string}\n",
    )
    df = DataGenerator(contract, seed=3).generate(rows=400)
    pairs = list(zip(df.get_column("country_code").to_list(), df.get_column("trip_id").to_list()))
    pairs = [p for p in pairs if None not in p]
    assert len(set(pairs)) == len(pairs)


def test_a_field_level_primary_key_flag_counts_too():
    contract = _contract(
        "",
        "    - {name: trip_id, type: string, primary_key: true}\n    - {name: fare, type: double}\n",
    )
    df = DataGenerator(contract, seed=7).generate(rows=300)
    assert len(set(_ids(df))) == 300


def test_an_integer_key_is_unique():
    contract = _contract(
        "primary_key: [order_no]\n",
        "    - {name: order_no, type: integer}\n    - {name: amount, type: double}\n",
    )
    df = DataGenerator(contract, seed=11).generate(rows=2000)
    col = [v for v in df.get_column("order_no").to_list() if v is not None]
    assert len(set(col)) == len(col)


def test_a_key_drawn_from_a_foreign_key_pool_is_left_to_the_pool():
    # The pool is the only legal domain: inventing a value would break referential integrity.
    contract = _contract("primary_key: [rider_id]\n", "    - {name: rider_id, type: string}\n")
    pool = ["R1", "R2", "R3"]
    df = DataGenerator(contract, seed=1).generate(rows=20, reference_data={"rider_id": pool})
    assert set(df.get_column("rider_id").to_list()) <= set(pool)


# ── Unique must not cost valid, and a repeat that cannot be fixed is reported ──


def _warnings(fn):
    from loguru import logger

    lines = []
    sink = logger.add(lambda m: lines.append(str(m)), level="WARNING", format="{message}")
    try:
        result = fn()
    finally:
        logger.remove(sink)
    return result, lines


def test_a_key_smaller_than_the_rows_stays_inside_its_allowed_values():
    """Five allowed values, twenty rows: uniqueness is impossible. The last-resort suffix used
    to produce `A-2`, `A-3`… — out-of-domain values in rows labelled valid. They now keep an
    allowed value, repeat, and the repeat is reported."""
    contract = _contract(
        "primary_key: [code]\n",
        "    - {name: code, type: string, accepted_values: [A, B, C, D, E]}\n    - {name: fare, type: double}\n",
    )
    df, lines = _warnings(lambda: DataGenerator(contract, seed=3).generate(rows=20))
    assert set(_ids(df, "code")) <= {"A", "B", "C", "D", "E"}, "a value outside accepted_values was invented"
    assert any("could not be given a unique value" in line for line in lines)


def test_a_pattern_is_not_broken_by_the_fallback():
    """`TRI-####` plus a `-2` suffix no longer matches the pattern it was generated from."""
    contract = _contract(
        "primary_key: [trip_id]\n",
        "    - {name: trip_id, type: string, pattern: '^TRI-[0-9]$'}\n    - {name: fare, type: double}\n",
    )
    df, _ = _warnings(lambda: DataGenerator(contract, seed=5).generate(rows=25))
    import re

    assert all(re.fullmatch(r"^TRI-[0-9]$", v) for v in _ids(df) if v is not None)


def test_a_key_made_only_of_foreign_keys_reports_its_repeats():
    """Every key column comes from a parent's pool, so it cannot be redrawn — that is right.
    It used to return without a word; a repeated key is still a broken promise."""
    contract = _contract("primary_key: [rider_id]\n", "    - {name: rider_id, type: string}\n")
    df, lines = _warnings(
        lambda: DataGenerator(contract, seed=1).generate(rows=20, reference_data={"rider_id": ["R1", "R2"]})
    )
    assert set(_ids(df, "rider_id")) <= {"R1", "R2"}, "the pool is still the only legal domain"
    assert any("cannot be redrawn without breaking the relationship" in line for line in lines)


def test_a_key_with_room_still_comes_out_unique():
    """The guard on the guard: the domain check must not stop ordinary keys being made unique."""
    df, lines = _warnings(lambda: DataGenerator(WITH_PK, seed=11).generate(rows=300))
    ids = [v for v in _ids(df) if v is not None]
    assert len(ids) == len(set(ids))
    assert not any("could not be given a unique value" in line for line in lines)
