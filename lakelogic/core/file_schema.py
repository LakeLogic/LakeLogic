"""Read a data file's SCHEMA — names and types — without materialising its rows.

Discovery needs to answer "what columns does this file have?" over parquet, csv, json and
excel, sitting in ADLS, S3, GCS or on disk. That is exactly the format and cloud knowledge
this framework already carries for reading data, so it belongs here rather than in a
consumer that reimplements a subset of it.

**Lazy where lazy exists.** ``scan_parquet`` / ``scan_csv`` / ``scan_ndjson`` build a plan
and ``collect_schema()`` resolves it; no frame is materialised. Excel has no lazy reader,
so it is read with ``n_rows=1`` — one row, because a workbook's types come from its cells
rather than a declared schema.

**Real types, not a header row.** A hand-rolled probe that splits the first CSV line can
only report every column as a string; polars infers ``Int64``, ``Datetime``, and a
``Struct`` for a nested JSON object. A caller that wants everything as text can ask for it
(``infer_types=False``) — the honest default is the type the data actually has.

**Credentials are passed in, never read from the environment.** ``storage_options`` goes
straight to the reader. A multi-tenant caller holds one tenant's credential at a time and
must never set it in the process environment, where the next tenant's scan would inherit
it.

What this does NOT do: no row values are returned, no file is copied, and nothing is
written. The row COUNT is reported only when a format states it in metadata — counting
lines in a CSV means reading the file, and an estimate presented as a count is worse than
saying it is unknown.
"""
from __future__ import annotations

import io
from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Tuple

from loguru import logger

#: Extensions per format, in the order a mixed directory resolves them.
#:
#: ``.xls`` is the legacy binary workbook and is NOT here: polars reads it only through
#: ``calamine``, and claiming support that depends on an optional engine is worse than
#: naming the gap. It reports as unsupported, like any other extension.
FORMAT_EXTENSIONS: Dict[str, Tuple[str, ...]] = {
    "parquet": (".parquet",),
    "csv": (".csv", ".tsv"),
    "json": (".json", ".ndjson", ".jsonl"),
    "excel": (".xlsx", ".xlsm"),
}

#: Every extension this module can read, flattened.
DATA_EXTENSIONS: Tuple[str, ...] = tuple(
    ext for exts in FORMAT_EXTENSIONS.values() for ext in exts
)


@dataclass(frozen=True)
class ProbedColumn:
    """One column: its name, position and type as the reader resolved it."""

    name: str
    ordinal: int
    data_type: str
    nullable: bool = True


@dataclass(frozen=True)
class ProbedSchema:
    """A file's shape. ``row_count`` is None whenever counting would mean reading rows."""

    columns: Tuple[ProbedColumn, ...]
    file_format: str
    row_count: Optional[int] = None


def format_of(path: str) -> Optional[str]:
    """Which format a path is, or None when this module cannot read it."""
    lower = str(path).lower()
    for fmt, exts in FORMAT_EXTENSIONS.items():
        if lower.endswith(exts):
            return fmt
    return None


def _columns(schema: Mapping[str, Any], *, infer_types: bool) -> Tuple[ProbedColumn, ...]:
    return tuple(
        ProbedColumn(
            name=str(name),
            ordinal=i + 1,
            # `str(dtype)` renders a polars type as text: "Int64", "Datetime(time_unit='us')",
            # "Struct({'x': Int64})". Nested shapes survive as their own notation rather than
            # being flattened — how deep to flatten is a contract decision, not discovery's.
            data_type="string" if not infer_types else str(dtype),
            nullable=True,
        )
        for i, (name, dtype) in enumerate(schema.items())
    )


def _parquet_row_count(lazy: Any) -> Optional[int]:
    """Row count from the parquet footer, or None if it cannot be had cheaply.

    ``select(len())`` over a lazy parquet scan is answered from footer metadata — the row
    groups are never read. Wrapped because a count is a nice-to-have: losing it must not
    cost the schema, which is what the caller actually needs.
    """
    try:
        return int(lazy.select(__import__("polars").len()).collect().item())
    except Exception as exc:  # noqa: BLE001
        logger.debug("probe_schema: no row count available ({}).", exc)
        return None


def probe_schema(
    path: str,
    *,
    storage_options: Optional[Mapping[str, Any]] = None,
    infer_types: bool = True,
) -> Optional[ProbedSchema]:
    """A file's columns and types, or None when it cannot be read.

    Returns None rather than raising: a scan walks thousands of files and one corrupt,
    empty or permission-denied object must under-report, never abort the walk. The reason
    is logged.

    ``storage_options`` reaches the underlying reader unchanged — account key, SAS, or a
    service principal (``tenant_id`` / ``client_id`` / ``client_secret``) for ``abfss://``,
    and the equivalents for ``s3://`` and ``gs://``.
    """
    import polars as pl

    fmt = format_of(path)
    if fmt is None:
        return None

    p = str(path)
    row_count: Optional[int] = None
    opts: Dict[str, Any] = {}
    if storage_options:
        opts["storage_options"] = dict(storage_options)

    try:
        if fmt == "parquet":
            lazy = pl.scan_parquet(p, **opts)
            schema = lazy.collect_schema()
            # Parquet STATES its row count in the footer, so this reads metadata, not rows.
            # No other format here can answer it without a full read, which is why the
            # field is None everywhere else rather than estimated.
            row_count = _parquet_row_count(lazy)
        elif fmt == "csv":
            # `infer_schema_length=0` reads no rows and types everything as string; the
            # default samples rows to type them. Both are metadata-only in the sense that
            # matters — no value is returned to the caller — but the sample is what makes
            # the types real, so it is the default.
            schema = pl.scan_csv(
                p,
                separator="\t" if p.lower().endswith(".tsv") else ",",
                **({} if infer_types else {"infer_schema_length": 0}),
                **opts,
            ).collect_schema()
        elif fmt == "json":
            schema = _json_schema(pl, p, opts)
        else:  # excel
            if opts:
                # polars reads Excel through a byte-oriented engine with no cloud plumbing.
                # Saying so beats a confusing failure deep inside the reader.
                logger.warning(
                    "probe_schema: excel over remote storage is not supported "
                    "({}) — download it or use a local path.",
                    p,
                )
                return None
            # One row, because a workbook has no declared schema: its types come from cells.
            schema = pl.read_excel(p, read_options={"n_rows": 1}).schema
    except Exception as exc:  # noqa: BLE001 — every reader raises its own shapes
        logger.warning("probe_schema: cannot read {} ({}) — skipping ({}).", p, fmt, exc)
        return None

    columns = _columns(schema, infer_types=infer_types)
    if not columns:
        # A file with no columns is not a dataset. Reporting it as one produces a contract
        # with nothing in it.
        return None
    return ProbedSchema(columns=columns, file_format=fmt, row_count=row_count)


def probe_schema_bytes(
    data: bytes,
    *,
    file_name: str,
    infer_types: bool = True,
) -> Optional[ProbedSchema]:
    """The same schema read, from bytes already in hand.

    For callers that reach storage through their own credentialed client rather than a URI
    — a multi-tenant scanner holding a per-org service principal, for instance. They have
    the bytes; what they lack is the format knowledge, which is what this module is for.

    ``file_name`` is used only to pick the reader: the bytes carry no extension.

    Eager, unavoidably — a buffer has no lazy scan. That is not a regression for the caller
    this exists for, which already had to fetch the whole object to read a footer.
    """
    import polars as pl

    fmt = format_of(file_name)
    if fmt is None:
        return None
    if not data:
        return None

    buf = io.BytesIO(data)
    row_count: Optional[int] = None
    try:
        if fmt == "parquet":
            schema = pl.read_parquet_schema(buf)
            buf.seek(0)
            row_count = _parquet_row_count(pl.scan_parquet(buf))
        elif fmt == "csv":
            schema = pl.read_csv(
                buf,
                separator="	" if file_name.lower().endswith(".tsv") else ",",
                n_rows=100 if infer_types else 0,
                infer_schema_length=None if infer_types else 0,
            ).schema
        elif fmt == "json":
            schema = _json_schema_bytes(pl, data)
        else:  # excel
            schema = pl.read_excel(buf, read_options={"n_rows": 1}).schema
    except Exception as exc:  # noqa: BLE001 — every reader raises its own shapes
        logger.warning("probe_schema_bytes: cannot read {} ({}) — skipping ({}).", file_name, fmt, exc)
        return None

    columns = _columns(schema, infer_types=infer_types)
    if not columns:
        return None
    return ProbedSchema(columns=columns, file_format=fmt, row_count=row_count)


def _json_schema_bytes(pl: Any, data: bytes) -> Mapping[str, Any]:
    """NDJSON first, then a JSON array — the same two shapes, from a buffer."""
    text = data.lstrip()
    if text[:1] == b"[":
        return pl.read_json(io.BytesIO(data)).schema
    return pl.read_ndjson(io.BytesIO(data)).schema


def _json_schema(pl: Any, path: str, opts: Dict[str, Any]) -> Mapping[str, Any]:
    """NDJSON first, then a JSON array.

    Both shapes actually land: an event stream writes one object per line, an API dump
    writes a single array. ``scan_ndjson`` is lazy and handles the common case; a file that
    opens with ``[`` is not NDJSON and needs the eager reader, which is why the fallback
    exists rather than a guess based on the extension.
    """
    try:
        return pl.scan_ndjson(path, **opts).collect_schema()
    except Exception:  # noqa: BLE001 — an array, or genuinely unreadable; try the other shape
        return pl.read_json(path, **opts).schema
