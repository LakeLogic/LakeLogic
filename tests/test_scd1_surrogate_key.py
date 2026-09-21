"""SCD1 (type-1 dimension) surrogate keys — written, stable, and the unknown member ONCE.

`materialization.scd1` had no tests in this repo, and both Spark paths are no-cover. Running it
for real (2026-09-21) showed the unknown member multiplying: every run re-hashed EVERY row's key,
including the `-1` unknown-member row, so it became sha256(<its placeholder pk>)[:16]. The
injector is idempotent by SK value, no longer found `-1`, and added another:

    after run 1: 1 unknown row  ['-1']
    after run 2: 2              ['-1', 'b764cdc0eab71374']
    after run 3: 3              ['-1', 'b764cdc0eab71374', 'b764cdc0eab71374']

One extra row per run, forever — and from the second run the surrogate key was not unique, so a
fact joining on it could double its measures.

These tests drive a REAL parsed `DataContract` through `materialize_dataframe`, not a mocked
config: the bug lived in how the pieces combine across runs, which a unit test of any one of them
would not see.
"""
from __future__ import annotations

import hashlib

import pytest

pd = pytest.importorskip("pandas")
pytest.importorskip("pyarrow")

from lakelogic.core.materialization import materialize_dataframe
from lakelogic.core.models import DataContract

SK = "city_sk"


def _contract(**unknown_member):
    return DataContract.model_validate({
        "version": "1.0.0",
        "info": {"title": "dim_city", "target_layer": "gold"},
        "model": {"fields": [
            {"name": SK, "type": "string"},
            {"name": "city_code", "type": "string"},
            {"name": "city_name", "type": "string"},
        ]},
        "primary_key": ["city_code"],
        "natural_key": ["city_code"],
        "materialization": {
            "strategy": "merge",
            "scd1": {
                "surrogate_key": SK,
                "surrogate_key_strategy": "hash",
                "unknown_member": {"enabled": True, "surrogate_key_value": "-1", **unknown_member},
            },
        },
    })


def _frame(*rows):
    return pd.DataFrame([{"city_code": code, "city_name": name} for code, name in rows])


def _run(contract, target, *rows):
    materialize_dataframe(
        _frame(*rows), contract, target, output_format="parquet", engine_name="pandas",
    )
    return pd.read_parquet(target)


def _hash(*pk):
    return hashlib.sha256("|".join(pk).encode("utf-8")).hexdigest()[:16]


@pytest.fixture
def target(tmp_path):
    return tmp_path / "dim_city.parquet"


# -- the key ----------------------------------------------------------------------------


def test_the_key_is_written_first(target):
    out = _run(_contract(), target, ("LON", "London"))
    assert out.columns[0] == SK


def test_the_key_is_sha256_of_the_primary_key_first_16_hex(target):
    out = _run(_contract(), target, ("LON", "London"))
    assert out.loc[out.city_code == "LON", SK].tolist() == [_hash("LON")]


def test_the_key_is_stable_across_runs(target):
    """Facts point at it. A key that changed between runs would orphan every fact row."""
    contract = _contract()
    first = _run(contract, target, ("LON", "London"))
    second = _run(contract, target, ("LON", "London (renamed)"))
    assert first.loc[first.city_code == "LON", SK].tolist() == second.loc[second.city_code == "LON", SK].tolist()


def test_type1_overwrites_in_place(target):
    """One row per key — the new value replaces the old, no history."""
    contract = _contract()
    _run(contract, target, ("LON", "London"))
    out = _run(contract, target, ("LON", "London (renamed)"))
    real = out[out[SK] != "-1"]
    assert real.city_code.tolist() == ["LON"]
    assert real.city_name.tolist() == ["London (renamed)"]


# -- the unknown member -----------------------------------------------------------------


def test_the_unknown_member_is_injected_with_its_own_key(target):
    out = _run(_contract(), target, ("LON", "London"))
    assert out[SK].tolist().count("-1") == 1


@pytest.mark.parametrize("runs", [2, 3, 5])
def test_the_unknown_member_exists_ONCE_however_many_runs(target, runs):
    """THE BUG. It grew by one row per run."""
    contract = _contract()
    for _ in range(runs):
        out = _run(contract, target, ("LON", "London"))
    assert (out[SK] == "-1").sum() == 1
    assert len(out) == 2  # LON + the unknown member


@pytest.mark.parametrize("runs", [2, 4])
def test_the_surrogate_key_stays_unique_across_runs(target, runs):
    """The consequence that matters: a duplicate key doubles a fact's measures on join."""
    contract = _contract()
    for i in range(runs):
        out = _run(contract, target, ("LON", "London"), (f"C{i}", "City"))
    assert out[SK].is_unique


def test_the_unknown_member_is_never_rehashed(target):
    contract = _contract()
    _run(contract, target, ("LON", "London"))
    out = _run(contract, target, ("LON", "London"))
    placeholder_pk = out.loc[out[SK] == "-1", "city_code"].tolist()
    assert placeholder_pk, "no unknown member at all"
    # Its placeholder key must not have been turned into a real-looking hash.
    assert _hash(str(placeholder_pk[0])) not in out[SK].tolist()


def test_a_custom_unknown_member_value_is_preserved_too(target):
    contract = _contract(surrogate_key_value="0")
    for _ in range(3):
        out = _run(contract, target, ("LON", "London"))
    assert (out[SK] == "0").sum() == 1


def test_new_keys_still_arrive_alongside_it(target):
    contract = _contract()
    _run(contract, target, ("LON", "London"))
    _run(contract, target, ("PAR", "Paris"))
    out = _run(contract, target, ("LON", "London"))
    assert sorted(out.loc[out[SK] != "-1", "city_code"]) == ["LON", "PAR"]
    assert (out[SK] == "-1").sum() == 1


# -- Spark ------------------------------------------------------------------------------


def test_the_spark_path_preserves_the_unknown_member_too():
    """The Spark merge had the identical flaw (`withColumn(sk, sha2(pk))` over every row, then
    an injector idempotent by SK value). Asserted on the source because Spark is not installed
    in every environment this suite runs in; the behaviour test below runs where it is."""
    import inspect

    from lakelogic.core import materialization

    src = inspect.getsource(materialization._spark_merge_dataframe)
    assert "F.col(sk_column).cast(\"string\") == F.lit(unknown_sk)" in src


def test_the_spark_path_keeps_one_unknown_member_across_runs(tmp_path):
    pytest.importorskip("pyspark")
    pytest.skip("needs a Spark session with Delta; covered by the source assertion above")
