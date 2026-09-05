"""`float` must mean the same width everywhere — DDL, engine cast maps, Arrow.

The bug this pins: `core/ddl.py` CREATEs a `float` column as **FLOAT** (32-bit,
and the Arrow map says `float32`), while the Spark engine cast every `float`
column to **double**. So the pipeline built a 64-bit DataFrame and tried to write
it into the 32-bit table it had just created, and Delta refused:

    [DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields
    'cancellation_fee' and 'cancellation_fee'

Three of four silver contracts failed on every run because of it, and gold then
reported `succeeded` with zero source rows.

No single map was wrong in isolation — each was internally consistent, which is
exactly why the disagreement survived. The guard is therefore not "assert the one
line that broke": it walks every logical→physical map and asserts they agree on
what `float` and `double` mean. A new engine, or a second map inside an existing
engine, that widens `float` to 64-bit fails here.
"""
from __future__ import annotations

import pytest

from lakelogic.core.ddl import _TYPE_MAP

# Dialects with only ONE floating-point type, so they cannot honour the
# distinction and are exempt: BigQuery has FLOAT64 only; SQLite's REAL is an
# 8-byte IEEE double whatever you call it; and Snowflake's FLOAT / DOUBLE / REAL
# are three spellings of one 64-bit type. Asserting 32-bit for these would claim
# a precision the warehouse cannot give.
SINGLE_FLOAT_DIALECTS = {"bigquery", "sqlite", "snowflake"}

# Physical names that are 32- and 64-bit floating point. ``REAL`` here is
# PostgreSQL's REAL (4 bytes) — SQLite's same-named 8-byte type is exempted above.
BIT32 = {"float", "FLOAT", "float32", "Float32", "REAL", "FLOAT4"}
BIT64 = {
    "double", "DOUBLE", "float64", "Float64", "FLOAT64",
    "DOUBLE PRECISION", "FLOAT8",
}


def _width(physical) -> int:
    name = physical if isinstance(physical, str) else getattr(physical, "__name__", str(physical))
    if name in BIT32:
        return 32
    if name in BIT64:
        return 64
    pytest.fail(f"{name!r} is neither a known 32-bit nor 64-bit float type")


# ── the DDL map is the source of truth: it CREATEs the column ────────────────

def test_ddl_creates_float_as_32_bit():
    """Whatever else changes, this is the width the physical column actually has."""
    for dialect, physical in _TYPE_MAP["float"].items():
        if dialect in SINGLE_FLOAT_DIALECTS:
            continue
        assert _width(physical) == 32, f"{dialect} creates float as {physical}"


def test_ddl_creates_double_as_64_bit():
    for dialect, physical in _TYPE_MAP["double"].items():
        if dialect in SINGLE_FLOAT_DIALECTS:
            continue
        assert _width(physical) == 64, f"{dialect} creates double as {physical}"


def test_the_arrow_map_agrees_with_the_ddl():
    from lakelogic.core.ddl import _CONTRACT_TO_ARROW

    assert _CONTRACT_TO_ARROW["float"] == "float32"
    assert _CONTRACT_TO_ARROW["double"] == "float64"


# ── every Spark cast map must agree with it ──────────────────────────────────

def test_spark_cast_maps_agree_with_the_ddl():
    """BOTH of Spark's maps, not just the one named in the traceback.

    Spark carried two logical→physical maps that disagreed with each other:
    `_SPARK_CAST_TYPES` said double, `_CONTRACT_TO_SPARK_TYPE` said FLOAT. Fixing
    only the one in the stack trace would have left the other live.
    """
    from lakelogic.engines.spark import SparkAdapter

    for name in ("_SPARK_CAST_TYPES", "_CONTRACT_TO_SPARK_TYPE"):
        mapping = getattr(SparkAdapter, name)
        assert _width(mapping["float"]) == 32, f"{name} casts float to {mapping['float']}"
        assert _width(mapping["double"]) == 64, f"{name} casts double to {mapping['double']}"


def test_spark_type_helper_agrees_with_the_ddl():
    """`_to_spark_type()` is a THIRD mapping, reached by a different code path."""
    from lakelogic.engines.spark import SparkAdapter

    assert _width(SparkAdapter._to_spark_type(SparkAdapter, "float")) == 32
    assert _width(SparkAdapter._to_spark_type(SparkAdapter, "double")) == 64


def test_bootstrap_maps_already_agree():
    """These were always right — pinned so they can't drift the other way."""
    from lakelogic.core.bootstrap import _SPARK_TYPE_MAP, _SPARK_TYPE_TO_POLARS

    assert _SPARK_TYPE_MAP["float"] == "float"
    assert _SPARK_TYPE_MAP["double"] == "double"
    assert _SPARK_TYPE_TO_POLARS["float"] == "Float32"
    assert _SPARK_TYPE_TO_POLARS["double"] == "Float64"


# ── every other engine's cast map ───────────────────────────────────────────

def test_duckdb_cast_maps_agree_with_the_ddl():
    """DuckDB carried two maps that contradicted each other, like Spark did:
    the pipeline `_TYPE_MAP` said DOUBLE while `_DUCKDB_CAST_TYPES` said FLOAT."""
    from lakelogic.engines.duckdb import DuckDBAdapter

    assert _width(DuckDBAdapter._DUCKDB_CAST_TYPES["float"]) == 32
    assert _width(DuckDBAdapter._DUCKDB_CAST_TYPES["double"]) == 64


def test_polars_cast_map_agrees_with_the_ddl():
    from lakelogic.engines.polars import PolarsAdapter

    assert _width(str(PolarsAdapter._to_polars_dtype(PolarsAdapter, "float"))) == 32
    assert _width(str(PolarsAdapter._to_polars_dtype(PolarsAdapter, "double"))) == 64


def test_generated_data_uses_the_declared_width():
    """The generator feeds the same Polars path; a 64-bit generated column would
    re-introduce the mismatch on the very first write."""
    from lakelogic.core.generator import _polars_dtype

    assert _width(str(_polars_dtype("float"))) == 32
    assert _width(str(_polars_dtype("double"))) == 64


def test_generic_sql_cast_map_agrees_with_the_ddl():
    from lakelogic.engines.generic_sql import GenericSQLAdapter

    assert _width(GenericSQLAdapter._SQL_CAST_TYPES["float"]) == 32
    assert _width(GenericSQLAdapter._SQL_CAST_TYPES["double"]) == 64

# ── the same defect, for integers ────────────────────────────────────────────
#
# `float` was not special. `_to_spark_type` also widened `int`/`integer` to
# `long`, while the DDL CREATEs them as INT — so a contract declaring `integer`
# produced the identical failure on a different column:
#
#     [DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields
#     'estimated_eta_minutes' and 'estimated_eta_minutes'
#
# Found by running the pipeline after the float fix landed: the error simply
# moved to the next column. Hence this section — the width of EVERY numeric
# type must survive the trip from contract to column, not just floats.

INT_WIDTHS = {
    "TINYINT": 8, "tinyint": 8, "byte": 8, "Int8": 8,
    "SMALLINT": 16, "smallint": 16, "short": 16, "Int16": 16,
    "INT": 32, "int": 32, "INTEGER": 32, "integer": 32, "Int32": 32, "int32": 32,
    "BIGINT": 64, "bigint": 64, "long": 64, "Int64": 64, "int64": 64,
}


def _int_width(physical) -> int:
    name = physical if isinstance(physical, str) else getattr(physical, "__name__", str(physical))
    if name not in INT_WIDTHS:
        pytest.fail(f"{name!r} is not a known integer type")
    return INT_WIDTHS[name]


@pytest.mark.parametrize("logical,expected", [
    ("tinyint", 8), ("smallint", 16), ("int", 32), ("integer", 32),
    ("long", 64), ("bigint", 64),
])
def test_ddl_never_creates_an_integer_narrower_than_declared(logical, expected):
    """A dialect may WIDEN when it lacks the exact type — PostgreSQL has no
    TINYINT, so SMALLINT there is correct. It must never NARROW: that would
    silently truncate values the contract says are valid. The width that has to
    match exactly is the cast-vs-DDL pair below, which is what Delta compares."""
    for dialect, physical in _TYPE_MAP[logical].items():
        if dialect in SINGLE_FLOAT_DIALECTS:
            continue  # these dialects collapse the integer widths too
        assert _int_width(physical) >= expected, f"{dialect} creates {logical} as {physical}"


@pytest.mark.parametrize("logical,expected", [
    ("int", 32), ("integer", 32), ("long", 64), ("bigint", 64),
])
def test_spark_casts_integers_at_the_declared_width(logical, expected):
    """`_to_spark_type` widened int/integer to `long`; the DDL says INT."""
    from lakelogic.engines.spark import SparkAdapter

    assert _int_width(SparkAdapter._to_spark_type(SparkAdapter, logical)) == expected
    assert _int_width(SparkAdapter._SPARK_CAST_TYPES[logical]) == expected


def test_no_spark_cast_map_widens_a_type_the_ddl_narrowed():
    """The general invariant, stated once: for every logical type the DDL knows,
    no Spark cast map may disagree with it about width. This is the assertion
    that would have caught BOTH the float and the integer bug on day one."""
    from lakelogic.engines.spark import SparkAdapter

    widths = {**INT_WIDTHS, **{k: 32 for k in BIT32}, **{k: 64 for k in BIT64}}
    checked = 0
    for logical, dialects in _TYPE_MAP.items():
        ddl = dialects.get("databricks")
        if ddl not in widths:
            continue  # not a numeric type
        for source in (SparkAdapter._SPARK_CAST_TYPES.get(logical),
                       SparkAdapter._CONTRACT_TO_SPARK_TYPE.get(logical),
                       SparkAdapter._to_spark_type(SparkAdapter, logical)):
            if source is None or source not in widths:
                continue
            assert widths[source] == widths[ddl], (
                f"contract type {logical!r}: DDL creates {ddl} "
                f"({widths[ddl]}-bit) but a cast map produces {source} ({widths[source]}-bit)"
            )
            checked += 1
    assert checked > 10, "guard checked suspiciously few types — did a map get renamed?"
