"""The erasure queue: ``_lakelogic_erasure_requests``.

People and DSR tools INSERT rows; LakeLogic owns the schema and every status transition.
The table sits beside ``metadata.run_log_table`` in the domain's schema — the same
resolution as the evidence tables (:func:`lakelogic.core.evidence_tables.resolve_evidence_target`).

Columns (see :data:`REQUEST_COLUMNS`)::

    request_id      the request's own id; becomes ``reason`` on every evidence row it produces
    framework       gdpr | hipaa
    subject_column  the column that identifies the subject (e.g. customer_id)
    subject_id      the subject's value in that column
    requested_at    when it was asked for
    requested_by    who asked
    reason          free text (ticket, legal basis)
    status          pending | completed | failed | dry_run
    processed_at    when LakeLogic last acted on it
    run_id          the pipeline run that acted on it
    system          optional: only this system's pipeline takes it (NULL = any system)

Status transitions
------------------
* ``pending`` → ``completed`` when every table in scope was erased (or none held the subject).
* ``pending`` → ``failed`` when a table's erasure failed; a failed request is not retried
  automatically — set it back to ``pending`` to retry.
* Dry run: ``pending`` → ``dry_run``, and a ``dry_run`` row is STILL in the pending set
  (:data:`OPEN_STATUSES`). A rehearsal is visible on the request but never consumes it:
  the next real run erases it.

Scope: the table is per domain schema. A job that runs one task per system must set
``system`` on each request, or the first system to run marks it done for the domain.

Supported backends: spark, duckdb, sqlite. Subject ids are never logged.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from loguru import logger

from lakelogic.core.types import ddl_type

KIND = "erasure_requests"

REQUEST_COLUMNS: Dict[str, str] = {
    "request_id": "string",
    "framework": "string",
    "subject_column": "string",
    "subject_id": "string",
    "requested_at": "timestamp_tz",
    "requested_by": "string",
    "reason": "string",
    "status": "string",
    "processed_at": "timestamp_tz",
    "run_id": "string",
    "system": "string",
}

FRAMEWORKS = ("gdpr", "hipaa")
STATUSES = ("pending", "completed", "failed", "dry_run")
#: Rows a run picks up. ``dry_run`` stays open: a rehearsal never consumes a request.
OPEN_STATUSES = ("pending", "dry_run")

_SUPPORTED = ("spark", "duckdb", "sqlite")


def resolve_requests_target(metadata: Dict[str, Any], engine_name: Optional[str] = None):
    """``(backend, table, database)`` for the requests table, or None when no run log is configured."""
    from lakelogic.core.evidence_tables import resolve_evidence_target

    return resolve_evidence_target(KIND, metadata, engine_name)


def _q(value: Any) -> str:
    return "NULL" if value is None else "'" + str(value).replace("'", "''") + "'"


def _ddl(backend: str) -> str:
    return ", ".join(f"{c} {ddl_type(t, backend)}" for c, t in REQUEST_COLUMNS.items())


def _open_where(frameworks: Iterable[str], system: Optional[str]) -> str:
    fw = ", ".join(_q(f) for f in frameworks)
    st = ", ".join(_q(s) for s in OPEN_STATUSES)
    where = f"status IN ({st}) AND lower(framework) IN ({fw})"
    if system:
        where += f" AND (system IS NULL OR system = '' OR system = {_q(system)})"
    return where


def _connect(backend: str, database: str):
    Path(database).parent.mkdir(parents=True, exist_ok=True)
    if backend == "duckdb":
        import duckdb

        return duckdb.connect(database=str(database))
    import sqlite3

    return sqlite3.connect(str(database))


def _check_backend(backend: str) -> bool:
    if backend in _SUPPORTED:
        return True
    logger.warning(f"_lakelogic_erasure_requests: backend '{backend}' is not supported ({', '.join(_SUPPORTED)}).")
    return False


def ensure_requests_table(metadata: Dict[str, Any], engine_name: Optional[str] = None, spark=None) -> Optional[str]:
    """Create the requests table if it does not exist (idempotent). Returns the table, or None."""
    resolved = resolve_requests_target(metadata, engine_name)
    if resolved is None:
        return None
    backend, target, database = resolved
    if not _check_backend(backend):
        return None
    if backend == "spark":
        spark = spark or _spark()
        parts = target.split(".")
        if len(parts) >= 2:
            spark.sql(f"CREATE SCHEMA IF NOT EXISTS {'.'.join(parts[:-1])}")
        try:
            spark.sql(f"CREATE TABLE IF NOT EXISTS {target} ({_ddl('spark')}) USING DELTA")
        except Exception as exc:  # another system's job created it at the same moment
            from lakelogic.core.evidence_tables import _is_concurrency_conflict

            if not _is_concurrency_conflict(exc):
                raise
        return target
    con = _connect(backend, database)
    try:
        if backend == "duckdb" and "." in target:
            con.execute(f"CREATE SCHEMA IF NOT EXISTS {target.split('.')[0]}")
        con.execute(f"CREATE TABLE IF NOT EXISTS {target} ({_ddl(backend)})")
        if backend == "sqlite":
            con.commit()
    finally:
        con.close()
    return target


def read_open_requests(
    metadata: Dict[str, Any],
    *,
    frameworks: Iterable[str] = FRAMEWORKS,
    system: Optional[str] = None,
    engine_name: Optional[str] = None,
    spark=None,
) -> List[Dict[str, Any]]:
    """Requests still to be erased (status in :data:`OPEN_STATUSES`), oldest first."""
    frameworks = [f.lower() for f in frameworks]
    target = ensure_requests_table(metadata, engine_name, spark=spark)
    if target is None:
        return []
    backend, _, database = resolve_requests_target(metadata, engine_name)
    sql = (
        f"SELECT {', '.join(REQUEST_COLUMNS)} FROM {target} WHERE {_open_where(frameworks, system)} "
        f"ORDER BY requested_at, request_id"
    )
    if backend == "spark":
        rows = (spark or _spark()).sql(sql).collect()
        return [r.asDict() for r in rows]
    con = _connect(backend, database)
    try:
        rows = con.execute(sql).fetchall()
    finally:
        con.close()
    return [dict(zip(REQUEST_COLUMNS, r)) for r in rows]


def mark_requests(
    metadata: Dict[str, Any],
    outcomes: Dict[str, str],
    *,
    run_id: Optional[str],
    engine_name: Optional[str] = None,
    spark=None,
) -> int:
    """Set ``status``/``processed_at``/``run_id`` for each ``request_id → status``. Returns rows touched."""
    if not outcomes:
        return 0
    bad = {s for s in outcomes.values() if s not in STATUSES}
    if bad:
        raise ValueError(f"Unknown erasure request status {sorted(bad)}; expected one of {STATUSES}")
    resolved = resolve_requests_target(metadata, engine_name)
    if resolved is None or not _check_backend(resolved[0]):
        return 0
    backend, target, database = resolved
    now = datetime.now(timezone.utc)
    # Only an OPEN request moves: a request someone closed meanwhile keeps its status.
    open_st = ", ".join(_q(s) for s in OPEN_STATUSES)
    if backend == "spark":
        spark = spark or _spark()
        values = ", ".join(f"({_q(rid)}, {_q(st)})" for rid, st in outcomes.items())
        spark.sql(
            f"MERGE INTO {target} AS t USING (SELECT * FROM VALUES {values} AS v(request_id, new_status)) AS s "
            f"ON t.request_id = s.request_id AND t.status IN ({open_st}) "
            f"WHEN MATCHED THEN UPDATE SET t.status = s.new_status, "
            f"t.processed_at = TIMESTAMP {_q(now.isoformat())}, t.run_id = {_q(run_id)}"
        )
        return len(outcomes)
    con = _connect(backend, database)
    ts = now if backend == "duckdb" else now.isoformat()
    touched = 0
    try:
        for rid, st in outcomes.items():
            cur = con.execute(
                f"UPDATE {target} SET status = ?, processed_at = ?, run_id = ? "
                f"WHERE request_id = ? AND status IN ({open_st})",
                [st, ts, run_id, rid],
            )
            touched += cur.rowcount if backend == "sqlite" else 0
        if backend == "sqlite":
            con.commit()
    finally:
        con.close()
    return touched


def _spark():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()
