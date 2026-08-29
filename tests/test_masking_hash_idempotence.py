"""`masking: hash` must be idempotent.

Masking is applied write-side, per contract run, and the silver/gold contract
templates propagate the `masking:` strategy downward. So a field hashed in
bronze was read back by silver, matched `masking: hash` on silver's own
contract, and was hashed AGAIN — sha256(salt + sha256(salt + value)) — with
gold making it three deep. Nothing raised and nothing was logged; the only
symptom was that the same person's key stopped matching across layers, so every
cross-layer join on a hashed column silently returned nothing.

These tests pin the fix: hashing a value already in the hash output shape
(64-char lowercase hex) leaves it unchanged, per value, on every engine.
"""

from __future__ import annotations

import hashlib

import pytest

from lakelogic.core.masking_engine import MaskingEngine, _apply_partial
from lakelogic.core.models import DataContract

pl = pytest.importorskip("polars")
pd = pytest.importorskip("pandas")

SALT = "s"
RAW = "alice@example.com"
HASHED = hashlib.sha256(f"{SALT}{RAW}".encode("utf-8")).hexdigest()


def _contract(strategy: str = "hash") -> DataContract:
    return DataContract(
        version="1.0.0",
        dataset="customers",
        model={
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "email", "type": "string", "pii": True, "masking": strategy},
            ]
        },
    )


def _engine(strategy: str = "hash") -> MaskingEngine:
    return MaskingEngine(_contract(strategy), hash_salt=SALT)


def _mask_polars(values, strategy: str = "hash"):
    df = pl.DataFrame({"id": list(range(len(values))), "email": values}, schema_overrides={"email": pl.Utf8})
    return list(_engine(strategy).apply(df, user_groups=[])["email"])


def _mask_pandas(values, strategy: str = "hash"):
    df = pd.DataFrame({"id": list(range(len(values))), "email": values})
    return list(_engine(strategy).apply(df, user_groups=[])["email"])


def _mask_duckdb(values, strategy: str = "hash"):
    duckdb = pytest.importorskip("duckdb")
    rel = duckdb.from_df(pd.DataFrame({"id": list(range(len(values))), "email": values}))
    out = _engine(strategy).apply(rel, user_groups=[])
    return list(out.fetchdf()["email"])


ENGINES = pytest.mark.parametrize(
    "mask", [_mask_polars, _mask_pandas, _mask_duckdb], ids=["polars", "pandas", "duckdb"]
)


# ── The core property ────────────────────────────────────────────────────────


@ENGINES
def test_raw_value_is_hashed(mask):
    assert mask([RAW]) == [HASHED]


@ENGINES
def test_already_hashed_value_passes_through_unchanged(mask):
    """mask(mask(x)) == mask(x) — the idempotence property, stated directly.

    Without the guard this returns sha256(salt + HASHED), and the bronze key no
    longer joins to the silver key.
    """
    once = mask([RAW])
    twice = mask(once)
    assert twice == once
    assert twice == [HASHED]


@ENGINES
def test_mixed_column_ends_with_every_value_hashed_exactly_once(mask):
    """A column part-masked by a backfill that mixed layers. The decision has to
    be per VALUE — a column-level decision would be wrong in both directions."""
    other_raw = "bob@example.com"
    other_hashed = hashlib.sha256(f"{SALT}{other_raw}".encode("utf-8")).hexdigest()

    out = mask([RAW, HASHED, other_raw])

    assert out == [HASHED, HASHED, other_hashed]  # every value hashed exactly once
    assert out[0] == out[1]  # the raw row and the pre-hashed row now MATCH


# ── Honesty: the skip must be visible ────────────────────────────────────────


@pytest.mark.parametrize("mask", [_mask_polars, _mask_pandas], ids=["polars", "pandas"])
def test_warning_names_the_column_and_the_count(mask, caplog):
    """Silent skipping would just trade one invisible behaviour for another."""
    from loguru import logger

    records: list[str] = []
    sink_id = logger.add(lambda m: records.append(m), level="WARNING", format="{message}")
    try:
        mask([RAW, HASHED, HASHED])
    finally:
        logger.remove(sink_id)

    hits = [r for r in records if "already" in r and "hash" in r]
    assert len(hits) == 1, f"expected exactly one warning per column, got: {hits}"
    assert "'email'" in hits[0]  # names the column
    assert "2 of 3" in hits[0]  # names how many were skipped


def test_warning_is_once_per_column_not_once_per_row():
    """500 already-hashed rows must produce ONE warning, not 500."""
    from loguru import logger

    records: list[str] = []
    sink_id = logger.add(lambda m: records.append(m), level="WARNING", format="{message}")
    try:
        _mask_polars([HASHED] * 500)
    finally:
        logger.remove(sink_id)

    assert len([r for r in records if "already" in r]) == 1


@ENGINES
def test_no_warning_when_nothing_is_skipped(mask):
    from loguru import logger

    records: list[str] = []
    sink_id = logger.add(lambda m: records.append(m), level="WARNING", format="{message}")
    try:
        mask([RAW])
    finally:
        logger.remove(sink_id)

    assert not [r for r in records if "already" in r]


# ── The shape check is exact, and hash-only ──────────────────────────────────


@ENGINES
@pytest.mark.parametrize(
    "value",
    [
        "a" * 63,  # 63 hex chars — not this function's output
        "a" * 65,  # 65 hex chars — not this function's output
        HASHED.upper(),  # uppercase hex — hexdigest() is lowercase
        "g" * 64,  # 64 chars but not hex
    ],
    ids=["63-hex", "65-hex", "uppercase-64-hex", "64-non-hex"],
)
def test_near_miss_shapes_are_not_treated_as_already_hashed(mask, value):
    expected = hashlib.sha256(f"{SALT}{value}".encode("utf-8")).hexdigest()
    assert mask([value]) == [expected]


@ENGINES
@pytest.mark.parametrize("strategy", ["nullify", "redact", "partial"])
def test_other_strategies_are_unaffected_by_the_shape_check(mask, strategy):
    """Only `hash` is idempotent-by-shape. A 64-hex value under nullify/redact/
    partial must still be masked exactly as it always was."""
    out = mask([HASHED], strategy)

    if strategy == "nullify":
        assert pd.isna(out[0])
    elif strategy == "redact":
        assert out[0] == "***REDACTED***"
    else:  # partial — masked exactly as _apply_partial always masked it
        assert out[0] == _apply_partial(HASHED)
        assert out[0] != HASHED


# ── Spark ────────────────────────────────────────────────────────────────────


def test_spark_hash_guard_is_a_native_expression_not_a_udf():
    """The Spark guard must stay a Catalyst expression (F.when + rlike). A UDF or a
    collect here would wreck the one engine where per-row Python actually hurts."""
    pytest.importorskip("pyspark", reason="pyspark not installed in this environment")

    import inspect

    from lakelogic.core import masking_engine

    src = inspect.getsource(masking_engine.MaskingEngine._apply_spark)
    hash_branch = src.split('elif strategy == "hash":')[1].split("elif strategy ==")[0]

    # Assert on CODE, not prose. The branch carries a comment saying "no UDF and no
    # collect", so a naive substring check on the raw source finds the very words it
    # is looking for and fails on its own documentation.
    code = " ".join(
        line for line in hash_branch.splitlines() if not line.strip().startswith("#")
    ).lower()

    assert "rlike" in code
    assert "f.when" in code
    assert "udf" not in code, "the Spark hash guard must stay a Catalyst expression"
    assert "collect" not in code, "collecting here would defeat the point of the guard"
