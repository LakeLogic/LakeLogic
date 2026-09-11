"""Synthetic related data keeps a shared attribute consistent across every relationship.

``generate_related`` guaranteed only that a foreign key exists in its parent. Every other
column was generated independently, so a GB rider's trip got a random ``country_code`` and a
driver from anywhere. Partitioned by country, that puts the trip under DE, its rider under GB
and its driver under JP — and every "UK trips" figure silently includes other countries.

The rule pinned here: a child takes ``inherit_columns`` from the parent row its foreign key
names; with two parents sharing the column (rider AND driver), the second foreign key is drawn
only from parents that match; chains (telemetry -> trip -> rider) stay consistent.
"""

from __future__ import annotations

import textwrap

import pytest

from lakelogic.core.generator import DataGenerator, _inherit_parent_attributes

COUNTRIES = ["GB", "DE", "FR"]


def _yaml(title: str, fields: list, partition_by: list | None = None) -> str:
    # Primary key declared, so generated ids are unique: a repeated parent id would make
    # "the parent row this key names" ambiguous, which is a fixture problem, not the rule.
    lines = ["version: '1.0'", "info:", f"  title: {title}", f"primary_key: [{fields[0][0]}]", "model:", "  fields:"]
    for name, ftype, accepted in fields:
        entry = f"    - {{name: {name}, type: {ftype}"
        if accepted:
            entry += f", accepted_values: [{', '.join(accepted)}]"
        lines.append(entry + "}")
    if partition_by:
        lines += ["materialization:", f"  partition_by: [{', '.join(partition_by)}]"]
    return "\n".join(lines) + "\n"


RIDERS = _yaml("riders", [("rider_id", "string", None), ("country_code", "string", COUNTRIES)])
DRIVERS = _yaml("drivers", [("driver_id", "string", None), ("country_code", "string", COUNTRIES)])
TRIPS = _yaml(
    "trips",
    [
        ("trip_id", "string", None),
        ("rider_id", "string", None),
        ("driver_id", "string", None),
        ("country_code", "string", COUNTRIES),
        ("fare", "double", None),
    ],
    partition_by=["country_code"],
)
PINGS = _yaml(
    "telemetry",
    [("ping_id", "string", None), ("trip_id", "string", None), ("country_code", "string", COUNTRIES)],
)

RELATIONSHIPS = [
    {"child": "trips", "child_column": "rider_id", "parent": "riders", "parent_column": "rider_id"},
    {"child": "trips", "child_column": "driver_id", "parent": "drivers", "parent_column": "driver_id"},
    {"child": "telemetry", "child_column": "trip_id", "parent": "trips", "parent_column": "trip_id"},
]

ROWS = {"riders": 40, "drivers": 30, "trips": 300, "telemetry": 600}


def _generate(**kw):
    return DataGenerator.generate_related(
        contracts={"riders": RIDERS, "drivers": DRIVERS, "trips": TRIPS, "telemetry": PINGS},
        rows=ROWS,
        relationships=RELATIONSHIPS,
        seed=7,
        **kw,
    )


def _country_of(df, key):
    """Parent key -> country, for keys that are UNAMBIGUOUS and KNOWN only.

    A key that repeats in the parent has no single "parent row" to agree with. A declared primary
    key is now unique (tests/test_generator_primary_key_unique.py), but the measure stays strict
    about it rather than assuming. A null country cannot disagree with anything, so it is not
    counted as a mismatch either.
    """
    keys = df.get_column(key).to_list()
    countries = df.get_column("country_code").to_list()
    seen = {}
    for k in keys:
        seen[k] = seen.get(k, 0) + 1
    return {k: c for k, c in zip(keys, countries) if seen[k] == 1 and c is not None}


def _mismatches(out):
    riders = _country_of(out["riders"], "rider_id")
    drivers = _country_of(out["drivers"], "driver_id")
    trips = out["trips"]
    trip_country = _country_of(trips, "trip_id")
    bad_rider = sum(
        1
        for r, c in zip(trips.get_column("rider_id").to_list(), trips.get_column("country_code").to_list())
        if r in riders and riders[r] != c
    )
    bad_driver = sum(
        1
        for d, c in zip(trips.get_column("driver_id").to_list(), trips.get_column("country_code").to_list())
        if d in drivers and drivers[d] != c
    )
    pings = out["telemetry"]
    bad_ping = sum(
        1
        for t, c in zip(pings.get_column("trip_id").to_list(), pings.get_column("country_code").to_list())
        if t in trip_country and trip_country[t] != c
    )
    return bad_rider, bad_driver, bad_ping


def test_without_inheritance_the_countries_disagree():
    # The defect, pinned so the fix below is shown to change something real.
    bad_rider, bad_driver, bad_ping = _mismatches(_generate())
    assert bad_rider > 0 and bad_driver > 0 and bad_ping > 0


def test_a_uk_riders_trips_all_occur_in_the_uk():
    out = _generate(inherit_columns=["country_code"])
    riders = _country_of(out["riders"], "rider_id")
    trips = out["trips"]
    gb_riders = {r for r, c in riders.items() if c == "GB"}
    assert gb_riders, "fixture should contain GB riders"
    gb_rider_trips = [
        c
        for r, c in zip(trips.get_column("rider_id").to_list(), trips.get_column("country_code").to_list())
        if r in gb_riders
    ]
    assert gb_rider_trips and set(gb_rider_trips) == {"GB"}


def test_every_link_agrees_rider_driver_and_telemetry():
    bad_rider, bad_driver, bad_ping = _mismatches(_generate(inherit_columns=["country_code"]))
    assert (bad_rider, bad_driver, bad_ping) == (0, 0, 0)


def test_the_partition_columns_can_drive_it():
    # trips declares partition_by: [country_code]; the flag inherits it without naming it.
    # Telemetry is checked too: it declares no partition_by of its own, and the first version
    # of the flag used only each child's own declaration — 404 pings disagreed with their trip
    # while this test discarded that number (`_`) and passed.
    bad_rider, bad_driver, bad_ping = _mismatches(_generate(inherit_partition_columns=True))
    assert (bad_rider, bad_driver, bad_ping) == (0, 0, 0)


def test_a_relationship_can_declare_its_own_inherit():
    rels = [dict(r) for r in RELATIONSHIPS]
    for r in rels:
        r["inherit"] = ["country_code"]
    out = DataGenerator.generate_related(
        contracts={"riders": RIDERS, "drivers": DRIVERS, "trips": TRIPS, "telemetry": PINGS},
        rows=ROWS,
        relationships=rels,
        seed=7,
    )
    assert _mismatches(out) == (0, 0, 0)


def test_it_is_deterministic_for_a_seed():
    a = _generate(inherit_columns=["country_code"])
    b = _generate(inherit_columns=["country_code"])
    assert a["trips"].equals(b["trips"]) and a["telemetry"].equals(b["telemetry"])


def test_default_output_is_unchanged_when_not_asked_for():
    # Opt-in: existing seeded output must stay byte-for-byte the same.
    a = _generate()
    b = _generate(inherit_columns=None, inherit_partition_columns=False)
    assert a["trips"].equals(b["trips"])


def test_a_child_with_no_matching_parent_is_counted_not_hidden():
    import polars as pl

    parent = pl.DataFrame({"driver_id": ["D1", "D2"], "country_code": ["DE", "DE"]})
    child = pl.DataFrame({"trip_id": ["T1", "T2"], "driver_id": ["D1", "D2"], "country_code": ["GB", "DE"]})
    out, unmatched = _inherit_parent_attributes(
        child,
        parent,
        fk="driver_id",
        pk="driver_id",
        set_attrs=[],
        redraw_on=["country_code"],
        rng=__import__("random").Random(1),
    )
    # T1 is GB and no GB driver exists: its key is left, and the disagreement is COUNTED.
    assert unmatched == 1
    assert out.get_column("driver_id").to_list()[1] == "D2"


def test_an_unknown_country_takes_the_next_parents_value_and_is_not_a_mismatch():
    import polars as pl

    parent = pl.DataFrame({"driver_id": ["D1"], "country_code": ["DE"]})
    child = pl.DataFrame(
        {"trip_id": ["T1"], "driver_id": ["D1"], "country_code": [None]},
        schema={"trip_id": pl.Utf8, "driver_id": pl.Utf8, "country_code": pl.Utf8},
    )
    out, unmatched = _inherit_parent_attributes(
        child,
        parent,
        fk="driver_id",
        pk="driver_id",
        set_attrs=[],
        redraw_on=["country_code"],
        rng=__import__("random").Random(1),
    )
    assert unmatched == 0
    assert out.get_column("country_code").to_list() == ["DE"]


def test_pandas_frames_work_too():
    import pandas as pd

    out = _generate(inherit_columns=["country_code"], output_format="pandas")
    riders = {r: c for r, c in zip(out["riders"]["rider_id"], out["riders"]["country_code"]) if not pd.isna(c)}
    trips = out["trips"]
    checked = [(r, c) for r, c in zip(trips["rider_id"], trips["country_code"]) if r in riders]
    assert checked and all(riders[r] == c for r, c in checked)


# ── partition_by declared ONCE, in _system.yaml ─────────────────────────────
# The recommended mesh pattern. The contracts then carry no partition_by at all, and the flag
# read only the contract file: 203 of 380 trips disagreed with their rider's country.

_SPECS = {
    "riders": [("rider_id", "string", None), ("country_code", "string", COUNTRIES)],
    "drivers": [("driver_id", "string", None), ("country_code", "string", COUNTRIES)],
    "trips": [
        ("trip_id", "string", None),
        ("rider_id", "string", None),
        ("driver_id", "string", None),
        ("country_code", "string", COUNTRIES),
        ("fare", "double", None),
    ],
    "telemetry": [("ping_id", "string", None), ("trip_id", "string", None), ("country_code", "string", COUNTRIES)],
}

_SYSTEM_HEADER = """domain: marketplace
system: rideflow
"""


def _layered(title: str, fields: list) -> str:
    """A contract with NO partition_by of its own, placed in the silver layer."""
    lines = _yaml(title, fields).splitlines()
    at = lines.index(f"  title: {title}") + 1
    lines.insert(at, "  target_layer: silver")
    return "\n".join(lines) + "\n"


def _write_mesh(root, system_materialization: str):
    (root / "_system.yaml").write_text(_SYSTEM_HEADER + system_materialization, encoding="utf-8")
    folder = root / "contracts" / "silver"
    folder.mkdir(parents=True)
    paths = {}
    for name, fields in _SPECS.items():
        f = folder / f"{name}.yaml"
        f.write_text(_layered(name, fields), encoding="utf-8")
        paths[name] = str(f)
    return paths


def _generate_files(paths, **kw):
    return DataGenerator.generate_related(contracts=paths, rows=ROWS, relationships=RELATIONSHIPS, seed=7, **kw)


_ALL_DEFAULT = """materialization:
  _all:
    partition_by: [country_code]
"""
_SILVER_DEFAULT = """materialization:
  silver:
    partition_by: [country_code]
"""
_GOLD_DEFAULT = """materialization:
  gold:
    partition_by: [country_code]
"""


def test_a_system_level_partition_by_is_honoured(tmp_path):
    """THE DEFECT: declared in _system.yaml under materialization._all, invisible before."""
    paths = _write_mesh(tmp_path, _ALL_DEFAULT)
    assert _mismatches(_generate_files(paths, inherit_partition_columns=True)) == (0, 0, 0)


def test_a_layer_default_is_honoured(tmp_path):
    """materialization.<layer> is the other place the registry reads a default from."""
    paths = _write_mesh(tmp_path, _SILVER_DEFAULT)
    assert _mismatches(_generate_files(paths, inherit_partition_columns=True)) == (0, 0, 0)


def test_a_default_for_another_layer_is_not(tmp_path):
    """A gold default must not reach silver contracts — the registry's precedence, not a guess."""
    paths = _write_mesh(tmp_path, _GOLD_DEFAULT)
    bad_rider, bad_driver, _ = _mismatches(_generate_files(paths, inherit_partition_columns=True))
    assert bad_rider > 0 and bad_driver > 0, "a gold-only default was applied to silver"


def test_with_nothing_to_inherit_it_says_so(tmp_path):
    """Asked for inheritance and found no partition key anywhere: a warning, not a silent no-op."""
    from loguru import logger

    lines = []
    sink = logger.add(lambda m: lines.append(str(m)), level="WARNING", format="{message}")
    try:
        _generate_files(_write_mesh(tmp_path, ""), inherit_partition_columns=True)
    finally:
        logger.remove(sink)
    assert any("no partition_by was found" in line for line in lines)


def test_a_malformed_system_yaml_does_not_stop_generation(tmp_path):
    """A broken _system.yaml is reported and skipped, never fatal. This path first called an
    undefined logger, so a malformed file would have crashed generation with a NameError."""
    from loguru import logger

    paths = _write_mesh(tmp_path, "")
    (tmp_path / "_system.yaml").write_text("materialization: [unclosed\n  - : :\n", encoding="utf-8")
    lines = []
    sink = logger.add(lambda m: lines.append(str(m)), level="WARNING", format="{message}")
    try:
        out = _generate_files(paths, inherit_partition_columns=True)
    finally:
        logger.remove(sink)
    assert set(out) == {"riders", "drivers", "trips", "telemetry"}
    assert any("for partition defaults" in line for line in lines)
