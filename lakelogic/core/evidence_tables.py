"""Durable evidence tables: ``_lakelogic_erasure_evidence`` and ``_lakelogic_retention_evidence``.

Written through the SAME configuration as the run log: the destination sits beside
``metadata.run_log_table`` (same backend, database, catalog and schema), and the table name
comes from :mod:`lakelogic.core.metadata_names`. Nothing is written unless a run-log
destination is configured — the same opt-in as the run log itself.

* identifier ``cat.schema.run_log`` → ``cat.schema._lakelogic_erasure_evidence``
* DuckDB directory-style ``./x/_logs`` → table ``_lakelogic_erasure_evidence`` inside the
  run log's own ``.duckdb`` file in that directory
* Delta path ``abfss://.../_logs`` → sibling path ``.../lakelogic_erasure_evidence``
  (underscore-free: Spark/Hadoop hide ``_``-prefixed paths)

Supported backends: spark, delta, duckdb, sqlite. Others log a warning and write nothing.
Best-effort: a failed evidence write never fails the action it records.

Rows never carry a subject identifier or a PII value — only counts.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from loguru import logger

from lakelogic.core.metadata_names import metadata_table_name, resolve_local_file
from lakelogic.core.plain_values import plain_record
from lakelogic.core.types import arrow_type, ddl_type, spark_type_object

#: Column name → contract type (resolved per backend by :mod:`lakelogic.core.types`).
EVIDENCE_COLUMNS: Dict[str, Dict[str, str]] = {
    "erasure_evidence": {
        "run_id": "string",
        "timestamp": "timestamp_tz",
        "profile": "string",
        "dataset": "string",
        "table_name": "string",
        "subject_count": "bigint",
        "rows_affected": "bigint",
        "reason": "string",
        "status": "string",
        "contract": "string",
        "contract_version": "string",
        "domain": "string",
        "system": "string",
    },
    "retention_evidence": {
        "run_id": "string",
        "timestamp": "timestamp_tz",
        "table_name": "string",
        "policy": "string",  # the retention window, e.g. "P7D"
        "cutoff": "timestamp_tz",
        "rows_expired": "bigint",
        # Added 2026-09-27: the outcome as one word (passed | breached | error | no_data) and the
        # check's own message. `policy` had been carrying both, emoji included.
        "status": "string",
        "detail": "string",
    },
}


def _looks_like_path(name: str) -> bool:
    return "/" in name or "\\" in name or name.startswith(".") or "://" in name


def resolve_evidence_target(kind: str, metadata: Dict[str, Any], engine_name: Optional[str] = None):
    """Return ``(backend, target, database)`` for ``kind``, or ``None`` when no run log is configured."""
    run_log_table = (metadata or {}).get("run_log_table")
    if not run_log_table or str(run_log_table) == "None" or "{" in str(run_log_table):
        return None
    run_log_table = str(run_log_table)
    backend = (metadata.get("run_log_backend") or "").lower()
    if not backend:
        backend = "spark" if engine_name == "spark" else "delta"

    if _looks_like_path(run_log_table):
        if backend == "duckdb":
            db = resolve_local_file("run_log", run_log_table, "duckdb")
            return backend, metadata_table_name(kind), str(db)
        parent = run_log_table.replace("\\", "/").rstrip("/").rsplit("/", 1)[0]
        return backend, f"{parent}/{metadata_table_name(kind, path_based=True)}", None

    parts = run_log_table.split(".")
    parts[-1] = metadata_table_name(kind, backend=metadata.get("platform") or backend)
    database = None
    if backend == "duckdb":
        from lakelogic.core.metadata_names import default_local_db_path

        database = metadata.get("run_log_database") or default_local_db_path("run_log", "duckdb")
        parts = parts[-2:]  # DuckDB: schema.table (catalog parts ignored, as for the run log)
    elif backend == "sqlite":
        from lakelogic.core.metadata_names import default_local_db_path

        database = metadata.get("run_log_database") or default_local_db_path("run_log", "sqlite")
        parts = ["_".join(parts)] if len(parts) > 1 else parts
    return backend, ".".join(parts), database


def _coerce(rows: List[Dict[str, Any]], cols: Dict[str, str]) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        rec = {}
        for c, t in cols.items():
            v = r.get(c)
            if t == "timestamp_tz" and isinstance(v, str):
                v = datetime.fromisoformat(v)
            if t == "timestamp_tz" and isinstance(v, datetime) and v.tzinfo is None:
                v = v.replace(tzinfo=timezone.utc)
            if t == "bigint" and v is not None:
                v = int(v)
            rec[c] = v
        # Evidence is data: no icons in any persisted value.
        out.append(plain_record(rec))
    return out


def write_evidence_rows(
    kind: str, rows: List[Dict[str, Any]], metadata: Dict[str, Any], engine_name: Optional[str] = None
) -> Optional[str]:
    """Append ``rows`` to the ``kind`` evidence table. Returns the target written, or None."""
    if not rows:
        return None
    cols = EVIDENCE_COLUMNS[kind]
    resolved = resolve_evidence_target(kind, metadata, engine_name)
    if resolved is None:
        return None
    backend, target, database = resolved
    records = _coerce(rows, cols)
    try:
        if backend == "duckdb":
            return _write_duckdb(target, database, records, cols)
        if backend == "sqlite":
            return _write_sqlite(target, database, records, cols)
        if backend == "delta":
            return _write_delta(target, records, cols)
        if backend == "spark":
            return _write_spark(target, records, cols)
        logger.warning(
            f"{metadata_table_name(kind)}: run-log backend '{backend}' is not supported for evidence "
            f"tables (spark, delta, duckdb, sqlite). The run-log row still records the action."
        )
        return None
    except Exception as exc:
        logger.warning(f"Failed to write {kind} to {target}: {exc}")
        return None


def _write_duckdb(target: str, database: str, records, cols) -> str:
    import duckdb

    db_path = Path(database)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(database=str(db_path))
    try:
        if "." in target:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {target.split('.')[0]}")
        ddl = ", ".join(f"{c} {ddl_type(t, 'duckdb')}" for c, t in cols.items())
        con.execute(f"CREATE TABLE IF NOT EXISTS {target} ({ddl})")
        # A table created before a column was added gains it, so an older estate keeps writing.
        for c, t in cols.items():
            con.execute(f"ALTER TABLE {target} ADD COLUMN IF NOT EXISTS {c} {ddl_type(t, 'duckdb')}")
        ph = ", ".join(["?"] * len(cols))
        for rec in records:
            con.execute(f"INSERT INTO {target} ({', '.join(cols)}) VALUES ({ph})", [rec[c] for c in cols])
    finally:
        con.close()
    logger.info(f"Wrote {len(records)} evidence row(s) to DuckDB {db_path}:{target}")
    return f"{db_path}:{target}"


def _write_sqlite(target: str, database: str, records, cols) -> str:
    import sqlite3

    db_path = Path(database)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(str(db_path))
    try:
        ddl = ", ".join(f"{c} {ddl_type(t, 'sqlite')}" for c, t in cols.items())
        con.execute(f"CREATE TABLE IF NOT EXISTS {target} ({ddl})")
        have = {row[1] for row in con.execute(f"PRAGMA table_info({target})")}
        for c, t in cols.items():
            if c not in have:
                con.execute(f"ALTER TABLE {target} ADD COLUMN {c} {ddl_type(t, 'sqlite')}")
        ph = ", ".join(["?"] * len(cols))
        for rec in records:
            vals = [rec[c].isoformat() if isinstance(rec[c], datetime) else rec[c] for c in cols]
            con.execute(f"INSERT INTO {target} ({', '.join(cols)}) VALUES ({ph})", vals)
        con.commit()
    finally:
        con.close()
    return f"{db_path}:{target}"


def _arrow_table(records, cols):
    import pyarrow as pa

    from lakelogic.core.ddl import _resolve_arrow_type

    def _arrow(t: str):
        typ = _resolve_arrow_type(t)
        # The registry says timestamp_tz is UTC-aware; keep the zone on the Arrow side too.
        if "tz=UTC" in arrow_type(t) and pa.types.is_timestamp(typ) and typ.tz is None:
            typ = pa.timestamp(typ.unit, tz="UTC")
        return typ

    schema = pa.schema([(c, _arrow(t)) for c, t in cols.items()])
    return pa.table([pa.array([r[c] for r in records], type=f.type) for c, f in zip(cols, schema)], schema=schema)


def _write_delta(target: str, records, cols) -> str:
    from deltalake import DeltaTable, write_deltalake

    from lakelogic.core.run_log import _build_cloud_opts, _is_cloud_path

    storage_options = _build_cloud_opts(target) if _is_cloud_path(target) else None
    try:
        DeltaTable(target, storage_options=storage_options)
        mode = "append"
    except Exception:
        mode = "overwrite"
    write_deltalake(
        target,
        _arrow_table(records, cols),
        mode=mode,
        storage_options=storage_options,
        schema_mode="merge" if mode == "append" else None,
    )
    return target


def _write_spark(target: str, records, cols) -> str:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructField, StructType

    spark = SparkSession.builder.getOrCreate()
    schema = StructType([StructField(c, spark_type_object(t), True) for c, t in cols.items()])
    parts = target.split(".")
    if len(parts) >= 2:
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS {'.'.join(parts[:-1])}")
    df = spark.createDataFrame([tuple(r[c] for c in cols) for r in records], schema=schema)
    # Several jobs of one domain (one per system) write the same evidence table at once. Letting
    # the first append create it raced: the losers failed with "table already exists" and the
    # evidence landed only on a job retry (2026-09-27, 3 of 4 marketing systems). Create it
    # explicitly first, tolerate losing that race, and retry the append on a concurrent write.
    ddl = ", ".join(f"`{c}` {schema[c].dataType.simpleString()}" for c in cols)
    try:
        spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({ddl}) USING DELTA")
    except Exception as exc:  # another writer created it between the check and the create
        if not _is_concurrency_conflict(exc):
            raise
    for attempt in range(_SPARK_APPEND_ATTEMPTS):
        try:
            df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(target)
            return target
        except Exception as exc:
            if attempt == _SPARK_APPEND_ATTEMPTS - 1 or not _is_concurrency_conflict(exc):
                raise
            time.sleep(0.5 * (2**attempt))
    return target


_SPARK_APPEND_ATTEMPTS = 5
_CONFLICT_MARKERS = (
    "already exists",
    "ALREADY_EXISTS",
    "ConcurrentAppend",
    "ConcurrentTransaction",
    "ConcurrentModification",
    "MetadataChanged",
    "ProtocolChanged",
    "DELTA_CONCURRENT",
)


def _is_concurrency_conflict(exc: Exception) -> bool:
    """A lost race with another writer (retryable), as opposed to a real failure."""
    text = f"{type(exc).__name__}: {exc}"
    return any(m in text for m in _CONFLICT_MARKERS)
