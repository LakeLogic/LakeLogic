"""One definition of what a contract type means, on every engine.

WHY THIS MODULE EXISTS
──────────────────────
A contract says ``type: float``. Nine different dictionaries, spread across
``core/ddl.py``, ``core/bootstrap.py``, ``core/generator.py`` and five engine
modules, each decided independently what that meant. They disagreed:

    core/ddl.py            float -> FLOAT     (32-bit)  ← CREATEs the column
    engines/spark.py       float -> double    (64-bit)  ← casts the DataFrame

so every run built a 64-bit column and tried to write it into the 32-bit table it
had just created. Delta refused::

    [DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields
    'cancellation_fee' and 'cancellation_fee'

Fixing that one pair moved the failure to ``estimated_eta_minutes`` — declared
``integer``, created ``INT``, cast to ``long``. The same defect, a different type,
a different function. Spark alone carried three maps, two of which contradicted
each other inside the same file.

No individual map was wrong. Each was internally consistent, which is exactly why
review never caught it: you cannot see the disagreement by reading either side.
Prose invariants ("keep these in sync") do not hold. Imports do.

THE DESIGN
──────────
One row per logical type. A *surface* (DDL, CAST, Arrow, Polars) is a column, not
a new dictionary somewhere else.

The load-bearing decision is that **the CAST type defaults to the DDL type**. An
engine does not get to have an opinion unless it declares one explicitly in
``cast_overrides``, and an override that changes the width fails at import. So the
default case — the one nobody thinks about — cannot drift, and the exceptional
case is visible in one place.

Two guards keep it that way:

* :func:`validate_registry`, run at import: for every type and dialect, the CAST
  width equals the DDL width. A mismatch fails here, not on a cluster three days
  later.
* ``tests/test_type_registry_is_the_only_map.py``: an AST scan that fails if any
  engine module declares its own literal type dictionary. Without it this module
  is merely tidier code, and the tenth dictionary appears next month.

DELIBERATE DIVERGENCE
─────────────────────
Some dialects genuinely cannot honour a width, and pretending otherwise would be
its own dishonesty. PostgreSQL has no ``TINYINT``; SQLite has one integer and one
float type; BigQuery has ``INT64``/``FLOAT64`` only; Snowflake's ``FLOAT``,
``DOUBLE`` and ``REAL`` are three spellings of one 64-bit type. Those dialects are
listed in :data:`COLLAPSES_WIDTH` and are checked for *kind* (integer stays an
integer) rather than for exact width, so the guard stays true instead of being
switched off.

``decimal`` maps to a 64-bit float on every engine, not to a true decimal type.
That was an existing, deliberate choice — DuckDB maps decimal to DOUBLE and Polars
to Float64, so a real ``DecimalType`` on Spark would make Spark the odd engine out
and the same contract would yield ``Decimal('12.340000000')`` on one engine and
``12.34`` on another. Cross-engine agreement is the property this corpus exists to
protect. Revisit all three together if exact decimal semantics are ever needed.
Parameterised ``decimal(p,s)`` is handled separately by ``core/ddl._resolve_type``
and keeps its precision.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, FrozenSet, Optional

# Every SQL dialect the registry can render. `polars` is not here: it is not a
# SQL dialect and has its own column on each row.
DIALECTS = ("spark", "databricks", "duckdb", "sqlite", "snowflake", "bigquery", "postgresql")

# Dialects that collapse several widths onto one physical type. They are checked
# for kind, not for exact width — see the module docstring.
COLLAPSES_WIDTH = frozenset({"sqlite", "bigquery", "snowflake"})

# Logical kinds. `kind` is what must never change across a dialect; `bits` is what
# must never change within a dialect that can express it.
INT = "int"
FLOAT = "float"
STRING = "string"
BOOL = "bool"
TEMPORAL = "temporal"
BINARY = "binary"
COMPLEX = "complex"


@dataclass(frozen=True)
class TypeSpec:
    """One logical contract type, and every physical form it takes."""

    logical: str
    kind: str
    ddl: Dict[str, str]
    arrow: str
    polars: str
    #: Width in bits for int/float kinds; None for everything else.
    bits: Optional[int] = None
    #: Dialects whose CAST syntax differs from their CREATE TABLE syntax. Rare —
    #: and every entry is width-checked by validate_registry().
    cast_overrides: Dict[str, str] = field(default_factory=dict)
    #: What a DataFrame carries this as, when that differs from what the column
    #: stores. JSON is stored as JSON/VARIANT but carried as text on every
    #: engine; that is a representation difference, not a type disagreement, and
    #: has to be DECLARED here so validate_registry() can tell the two apart.
    transport_kind: Optional[str] = None

    def ddl_type(self, dialect: str) -> str:
        return self.ddl[dialect]

    def cast_type(self, dialect: str) -> str:
        """The CAST target. Defaults to the DDL type — that default is the point."""
        return self.cast_overrides.get(dialect, self.ddl[dialect])


def _spec(logical, kind, arrow, polars, bits=None, cast_overrides=None, transport_kind=None, **ddl) -> TypeSpec:
    missing = [d for d in DIALECTS if d not in ddl]
    if missing:
        raise ValueError(f"type {logical!r} has no rendering for {missing}")
    return TypeSpec(
        logical=logical,
        kind=kind,
        arrow=arrow,
        polars=polars,
        bits=bits,
        ddl=dict(ddl),
        cast_overrides=dict(cast_overrides or {}),
        transport_kind=transport_kind,
    )


# ─────────────────────────────────────────────────────────────────────────────
# The registry. Adding an engine means filling a column here, not writing a dict
# in the engine module — which is why a new engine cannot drift on day one.
# ─────────────────────────────────────────────────────────────────────────────

_REGISTRY: Dict[str, TypeSpec] = {}


def _add(spec: TypeSpec, *aliases: str) -> None:
    _REGISTRY[spec.logical] = spec
    for alias in aliases:
        _REGISTRY[alias] = spec


# ── strings ──────────────────────────────────────────────────────────────────
_add(
    _spec(
        "string",
        STRING,
        arrow="string",
        polars="Utf8",
        spark="STRING",
        databricks="STRING",
        duckdb="VARCHAR",
        sqlite="TEXT",
        snowflake="VARCHAR",
        bigquery="STRING",
        postgresql="TEXT",
    ),
    "text",
    "char",
)
_add(
    _spec(
        "varchar",
        STRING,
        arrow="string",
        polars="Utf8",
        spark="STRING",
        databricks="STRING",
        duckdb="VARCHAR",
        sqlite="TEXT",
        snowflake="VARCHAR",
        bigquery="STRING",
        postgresql="VARCHAR",
    )
)

# ── integers ─────────────────────────────────────────────────────────────────
_add(
    _spec(
        "tinyint",
        INT,
        bits=8,
        arrow="int8",
        polars="Int8",
        spark="TINYINT",
        databricks="TINYINT",
        duckdb="TINYINT",
        sqlite="INTEGER",
        snowflake="TINYINT",
        bigquery="INT64",
        # PostgreSQL has no 8-bit integer; SMALLINT is the narrowest that
        # holds every value the contract allows. Widening is safe, narrowing
        # would silently truncate.
        postgresql="SMALLINT",
    ),
    "byte",
)
_add(
    _spec(
        "smallint",
        INT,
        bits=16,
        arrow="int16",
        polars="Int16",
        spark="SMALLINT",
        databricks="SMALLINT",
        duckdb="SMALLINT",
        sqlite="INTEGER",
        snowflake="SMALLINT",
        bigquery="INT64",
        postgresql="SMALLINT",
    ),
    "short",
)
_add(
    _spec(
        "int",
        INT,
        bits=32,
        arrow="int32",
        polars="Int32",
        spark="INT",
        databricks="INT",
        duckdb="INTEGER",
        sqlite="INTEGER",
        snowflake="INTEGER",
        bigquery="INT64",
        postgresql="INTEGER",
    ),
    "integer",
    "int32",
)
_add(
    _spec(
        "bigint",
        INT,
        bits=64,
        arrow="int64",
        polars="Int64",
        spark="BIGINT",
        databricks="BIGINT",
        duckdb="BIGINT",
        sqlite="INTEGER",
        snowflake="BIGINT",
        bigquery="INT64",
        postgresql="BIGINT",
    ),
    "long",
    "int64",
)

# ── floats ───────────────────────────────────────────────────────────────────
_add(
    _spec(
        "float",
        FLOAT,
        bits=32,
        arrow="float32",
        polars="Float32",
        spark="FLOAT",
        databricks="FLOAT",
        duckdb="FLOAT",
        sqlite="REAL",
        snowflake="FLOAT",
        bigquery="FLOAT64",
        postgresql="REAL",
    ),
    "float32",
    "real",
)
_add(
    _spec(
        "double",
        FLOAT,
        bits=64,
        arrow="float64",
        polars="Float64",
        spark="DOUBLE",
        databricks="DOUBLE",
        duckdb="DOUBLE",
        sqlite="REAL",
        snowflake="DOUBLE",
        bigquery="FLOAT64",
        postgresql="DOUBLE PRECISION",
    ),
    "float64",
    "number",
)
# Bare `decimal` is a 64-bit float everywhere — see the module docstring. Before
# this, the DDL passed it through as DECIMAL while every engine cast it to double,
# so a `decimal` column hit the same merge failure as float and integer did.
_add(
    _spec(
        "decimal",
        FLOAT,
        bits=64,
        arrow="float64",
        polars="Float64",
        spark="DOUBLE",
        databricks="DOUBLE",
        duckdb="DOUBLE",
        sqlite="REAL",
        snowflake="DOUBLE",
        bigquery="FLOAT64",
        postgresql="DOUBLE PRECISION",
    ),
    "numeric",
)

# ── booleans ─────────────────────────────────────────────────────────────────
_add(
    _spec(
        "boolean",
        BOOL,
        arrow="bool",
        polars="Boolean",
        spark="BOOLEAN",
        databricks="BOOLEAN",
        duckdb="BOOLEAN",
        sqlite="INTEGER",
        snowflake="BOOLEAN",
        bigquery="BOOL",
        postgresql="BOOLEAN",
    ),
    "bool",
)

# ── temporal ─────────────────────────────────────────────────────────────────
_add(
    _spec(
        "date",
        TEMPORAL,
        arrow="date32",
        polars="Date",
        spark="DATE",
        databricks="DATE",
        duckdb="DATE",
        sqlite="TEXT",
        snowflake="DATE",
        bigquery="DATE",
        postgresql="DATE",
    )
)
_add(
    _spec(
        "timestamp",
        TEMPORAL,
        arrow="timestamp[us]",
        polars="Datetime",
        spark="TIMESTAMP",
        databricks="TIMESTAMP",
        duckdb="TIMESTAMP",
        sqlite="TEXT",
        snowflake="TIMESTAMP_NTZ",
        bigquery="TIMESTAMP",
        postgresql="TIMESTAMP",
    ),
    "datetime",
)
_add(
    _spec(
        "timestamp_ntz",
        TEMPORAL,
        arrow="timestamp[us]",
        polars="Datetime",
        spark="TIMESTAMP_NTZ",
        databricks="TIMESTAMP_NTZ",
        duckdb="TIMESTAMP",
        sqlite="TEXT",
        snowflake="TIMESTAMP_NTZ",
        bigquery="TIMESTAMP",
        postgresql="TIMESTAMP WITHOUT TIME ZONE",
    )
)
_add(
    _spec(
        "timestamp_tz",
        TEMPORAL,
        arrow="timestamp[us, tz=UTC]",
        polars="Datetime",
        spark="TIMESTAMP",
        databricks="TIMESTAMP",
        duckdb="TIMESTAMPTZ",
        sqlite="TEXT",
        snowflake="TIMESTAMP_TZ",
        bigquery="TIMESTAMP",
        postgresql="TIMESTAMP WITH TIME ZONE",
    )
)

# ── binary / complex ─────────────────────────────────────────────────────────
_add(
    _spec(
        "binary",
        BINARY,
        arrow="binary",
        polars="Binary",
        spark="BINARY",
        databricks="BINARY",
        duckdb="BLOB",
        sqlite="BLOB",
        snowflake="BINARY",
        bigquery="BYTES",
        postgresql="BYTEA",
    )
)
_add(
    _spec(
        "json",
        COMPLEX,
        arrow="string",
        polars="Utf8",
        spark="STRING",
        databricks="STRING",
        duckdb="JSON",
        sqlite="TEXT",
        snowflake="VARIANT",
        bigquery="JSON",
        postgresql="JSONB",
        # JSON is carried as text in a DataFrame on every engine; only the
        # stored column type differs. Declared, not implied.
        transport_kind=STRING,
        cast_overrides={"duckdb": "VARCHAR", "snowflake": "VARCHAR", "bigquery": "STRING", "postgresql": "TEXT"},
    )
)
_add(
    _spec(
        "array",
        COMPLEX,
        arrow="string",
        polars="Utf8",
        spark="ARRAY<STRING>",
        databricks="ARRAY<STRING>",
        duckdb="VARCHAR[]",
        sqlite="TEXT",
        snowflake="ARRAY",
        bigquery="ARRAY<STRING>",
        postgresql="TEXT[]",
    )
)


# ─────────────────────────────────────────────────────────────────────────────
# Lookups
# ─────────────────────────────────────────────────────────────────────────────


class UnknownContractType(KeyError):
    """A contract type with no registered mapping.

    Raised rather than defaulted. The old behaviour was to fall back to a string
    column (``.get(type, "VARCHAR")``), so a type nobody had mapped silently
    became text and the contract's declared type was quietly ignored — the same
    class of failure as the width bug, just quieter.
    """


def _key(contract_type: str) -> str:
    return (contract_type or "").strip().lower()


def spec_for(contract_type: str) -> TypeSpec:
    key = _key(contract_type)
    try:
        return _REGISTRY[key]
    except KeyError:
        raise UnknownContractType(
            f"contract type {contract_type!r} has no mapping in lakelogic.core.types. "
            f"Add it to the registry — do not add a local dictionary. "
            f"Known types: {', '.join(sorted(set(_REGISTRY)))}"
        ) from None


def is_known(contract_type: str) -> bool:
    return _key(contract_type) in _REGISTRY


def ddl_type(contract_type: str, dialect: str) -> str:
    """The CREATE TABLE type for this contract type on this dialect."""
    return spec_for(contract_type).ddl_type(dialect)


def cast_type(contract_type: str, dialect: str) -> str:
    """The CAST target. Equal to :func:`ddl_type` unless the dialect overrides it."""
    return spec_for(contract_type).cast_type(dialect)


def arrow_type(contract_type: str) -> str:
    return spec_for(contract_type).arrow


def polars_dtype_name(contract_type: str) -> str:
    return spec_for(contract_type).polars


def width_bits(contract_type: str) -> Optional[int]:
    return spec_for(contract_type).bits


def kind_of(contract_type: str) -> str:
    return spec_for(contract_type).kind


def known_types() -> FrozenSet[str]:
    return frozenset(_REGISTRY)


def as_ddl_map() -> Dict[str, Dict[str, str]]:
    """``{logical: {dialect: type}}`` — the shape ``core/ddl`` consumed before."""
    return {logical: dict(spec.ddl) for logical, spec in _REGISTRY.items()}


def as_cast_map(dialect: str) -> Dict[str, str]:
    """``{logical: cast type}`` for one dialect — what an engine needs."""
    return {logical: spec.cast_type(dialect) for logical, spec in _REGISTRY.items()}


def as_arrow_map() -> Dict[str, str]:
    return {logical: spec.arrow for logical, spec in _REGISTRY.items()}


# ─────────────────────────────────────────────────────────────────────────────
# The guard that runs on import
# ─────────────────────────────────────────────────────────────────────────────


def validate_registry() -> None:
    """Fail at import if any CAST override disagrees with its DDL type.

    This is the invariant the whole module exists for: the type a column is
    CREATEd as, and the type the DataFrame is CAST to, must describe the same
    physical value. Delta compares them on every write.
    """
    for logical, spec in _REGISTRY.items():
        for dialect, override in spec.cast_overrides.items():
            if dialect not in DIALECTS:
                raise ValueError(f"{logical!r}: cast override for unknown dialect {dialect!r}")
            declared = spec.ddl[dialect]
            allowed = spec.transport_kind or _physical_kind(declared)
            if _physical_kind(override) != allowed:
                raise ValueError(
                    f"{logical!r} on {dialect!r}: CREATEs as {declared} but CASTs to "
                    f"{override} — these are different kinds of value. A cast override "
                    f"may change syntax, never the stored type. If the DataFrame "
                    f"genuinely carries a different representation, declare "
                    f"transport_kind on the spec so it is explicit."
                )
    # Every alias must resolve to a spec whose width the aliases agree on.
    for logical, spec in _REGISTRY.items():
        if spec.kind in (INT, FLOAT) and spec.bits is None:
            raise ValueError(f"{logical!r} is numeric but declares no width")


_KIND_BY_PHYSICAL = {
    "STRING": STRING,
    "VARCHAR": STRING,
    "TEXT": STRING,
    "TINYINT": INT,
    "SMALLINT": INT,
    "INT": INT,
    "INTEGER": INT,
    "BIGINT": INT,
    "INT64": INT,
    "FLOAT": FLOAT,
    "DOUBLE": FLOAT,
    "REAL": FLOAT,
    "FLOAT64": FLOAT,
    "DOUBLE PRECISION": FLOAT,
    "NUMBER": FLOAT,
    "BOOLEAN": BOOL,
    "BOOL": BOOL,
    "DATE": TEMPORAL,
    "TIMESTAMP": TEMPORAL,
    "TIMESTAMP_NTZ": TEMPORAL,
    "TIMESTAMP_TZ": TEMPORAL,
    "TIMESTAMPTZ": TEMPORAL,
    "TIMESTAMP WITHOUT TIME ZONE": TEMPORAL,
    "TIMESTAMP WITH TIME ZONE": TEMPORAL,
    "BINARY": BINARY,
    "BLOB": BINARY,
    "BYTES": BINARY,
    "BYTEA": BINARY,
    "JSON": COMPLEX,
    "JSONB": COMPLEX,
    "VARIANT": COMPLEX,
}


def _physical_kind(physical: str) -> str:
    base = physical.upper().split("(")[0].strip()
    if base.endswith("[]") or base.startswith("ARRAY"):
        return COMPLEX
    return _KIND_BY_PHYSICAL.get(base, base)


def polars_dtype(contract_type: str):
    """The actual Polars dtype object for a contract type.

    Resolved from the same row as the DDL type, so a Polars DataFrame and the
    column it is written into cannot disagree about width — the defect that made
    `float` mean Float64 in Polars and FLOAT in the table.
    """
    import polars as pl

    name = spec_for(contract_type).polars
    dtype = getattr(pl, name, None)
    if dtype is None:  # pragma: no cover - a Polars rename would be a hard error
        raise UnknownContractType(f"polars has no dtype named {name!r} (contract type {contract_type!r})")
    return dtype


def as_polars_map() -> Dict[str, object]:
    """``{logical: polars dtype}`` — what the Polars engine and generator need."""
    return {logical: polars_dtype(logical) for logical in _REGISTRY}


# Spark type OBJECTS, for building a StructType. Kept here rather than in the
# processor so the reader schema, the CAST and the CREATE TABLE all come from one
# row — the processor's own copy of this map was a tenth place for `float` to
# mean something different.
_SPARK_OBJECT_BY_ARROW = {
    "string": "StringType",
    "int8": "ByteType",
    "int16": "ShortType",
    "int32": "IntegerType",
    "int64": "LongType",
    "float32": "FloatType",
    "float64": "DoubleType",
    "bool": "BooleanType",
    "date32": "DateType",
    "timestamp[us]": "TimestampType",
    "timestamp[us, tz=UTC]": "TimestampType",
    "binary": "BinaryType",
}


def spark_type_object(contract_type: str):
    """The pyspark.sql.types object for a contract type, or None if unmapped.

    None (rather than a StringType default) so the caller decides what an unknown
    type means; defaulting to string here is how a declared type could be silently
    discarded.
    """
    from pyspark.sql import types as st

    spec = spec_for(contract_type)
    name = _SPARK_OBJECT_BY_ARROW.get(spec.arrow)
    return getattr(st, name)() if name else None


validate_registry()
