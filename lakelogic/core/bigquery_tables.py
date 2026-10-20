"""BigQuery tables for LakeLogic's own records: the run log and the SLO checks.

The run-log, SLO-checks and SLO-validator code paths share these helpers so BigQuery is handled
one way: one client (the same project and location the ``bigquery`` engine uses), one quoting
rule, one create-or-widen step and one append.

Rows are appended with a LOAD job, not the streaming insert API: rows streamed into a table sit
in a buffer that blocks UPDATE and DELETE on them for up to about 30 minutes, and the erasure and
retention jobs must be able to change any row.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from loguru import logger

def bigquery_type(sql_type: str) -> str:
    """A declared column type as BigQuery names it (``VARCHAR`` -> ``STRING``), from the one
    type registry (:mod:`lakelogic.core.types`), so this table and the engines cannot disagree."""
    from lakelogic.core import types as _types

    return _types.as_cast_map("bigquery").get(str(sql_type).lower(), "STRING")


def _env(value: Optional[str]) -> Optional[str]:
    """``${VAR}`` → the variable's value; anything else unchanged."""
    if not value:
        return value
    text = str(value).strip()
    if text.startswith("${") and text.endswith("}"):
        return os.getenv(text[2:-1]) or None
    return text


def client(metadata: Optional[Mapping[str, Any]] = None) -> Optional[Any]:
    """A BigQuery client for the contract's project and location, or None when the BigQuery
    client library is not installed. Same settings as the ``bigquery`` engine, so log rows land
    in the project and region the data was written to."""
    metadata = metadata or {}
    try:
        from google.cloud import bigquery
    except Exception as exc:  # pragma: no cover - the cloud extra is not installed
        logger.warning(f"BigQuery log backend unavailable: {exc}")
        return None
    project = _env(
        metadata.get("bigquery_project") or os.getenv("BIGQUERY_PROJECT") or os.getenv("GOOGLE_CLOUD_PROJECT")
    )
    location = _env(
        metadata.get("bigquery_location") or os.getenv("BIGQUERY_LOCATION") or os.getenv("BQ_LOCATION")
    )
    return bigquery.Client(project=project or None, location=location or None)


def quote(table: str) -> str:
    """``project.dataset.table`` wrapped in backticks. Project ids usually contain ``-``, which
    BigQuery accepts only inside backticks; one pair around the whole path is valid."""
    text = str(table).strip().replace("`", "")
    return f"`{text}`"


#: Tables this process has already made sure of: a run writes its log once per contract, and
#: the table does not change shape between those writes.
_ENSURED: set = set()


def _existing_columns(bq: Any, table: str) -> Optional[set]:
    """The table's column names (lower-cased) from its metadata, or ``None`` when it does not
    exist. Metadata is read, not queried: no job, no cost, no wait."""
    try:
        meta = bq.get_table(table)
    except Exception as exc:  # noqa: BLE001 - only "not found" means create it
        if type(exc).__name__ == "NotFound":
            return None
        raise
    return {f.name.lower() for f in meta.schema}


def ensure_table(bq: Any, table: str, columns: Sequence[Tuple[str, str]]) -> None:
    """Create the table if it is missing, and add any column an older table lacks. A table
    written by an earlier release is widened, never recreated, so its history is kept.

    One metadata read, then at most one DDL statement. It used to run a CREATE and an ALTER per
    column (37 queries, about a minute and a half) before every run-log row (seen live on GCP).
    """
    if table in _ENSURED:
        return
    existing = _existing_columns(bq, table)
    if existing is None:
        defs = ", ".join(f"`{name}` {bigquery_type(sql_type)}" for name, sql_type in columns)
        bq.query(f"CREATE TABLE IF NOT EXISTS {quote(table)} ({defs})").result()
    else:
        missing = [(n, t) for n, t in columns if n.lower() not in existing]
        if missing:
            adds = ", ".join(f"ADD COLUMN IF NOT EXISTS `{n}` {bigquery_type(t)}" for n, t in missing)
            bq.query(f"ALTER TABLE {quote(table)} {adds}").result()
    _ENSURED.add(table)


def _cell(value: Any, bq_type: str) -> Any:
    """A value as the load job's JSON accepts it for a column of ``bq_type``."""
    if value is None:
        return None
    if bq_type == "STRING":
        if isinstance(value, (dict, list, tuple)):
            return json.dumps(value, default=str)
        return str(value)
    if bq_type == "INT64":
        try:
            return int(value)
        except (TypeError, ValueError):
            return None
    if bq_type == "FLOAT64":
        try:
            return float(value)
        except (TypeError, ValueError):
            return None
    if bq_type == "BOOL":
        return bool(value)
    return str(value)


def append_rows(
    bq: Any, table: str, columns: Sequence[Tuple[str, str]], records: Iterable[Mapping[str, Any]]
) -> int:
    """Append ``records`` (only the declared columns, typed) with a load job. Returns the count."""
    from google.cloud import bigquery

    types = [(name, bigquery_type(sql_type)) for name, sql_type in columns]
    rows: List[Dict[str, Any]] = [
        {name: _cell(rec.get(name), bq_type) for name, bq_type in types} for rec in records
    ]
    if not rows:
        return 0
    config = bigquery.LoadJobConfig(
        schema=[bigquery.SchemaField(name, bq_type) for name, bq_type in types],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    bq.load_table_from_json(rows, str(table).replace("`", ""), job_config=config).result()
    return len(rows)


def fetch_one(bq: Any, sql: str, params: Optional[Sequence[Any]] = None) -> Optional[Tuple[Any, ...]]:
    """The first row of ``sql`` as a tuple, or None. ``params`` bind positional ``?`` markers."""
    from google.cloud import bigquery

    config = None
    if params:
        config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter(None, _param_type(p), p) for p in params]
        )
    for row in bq.query(sql, job_config=config).result():
        return tuple(row.values())
    return None


def _param_type(value: Any) -> str:
    if isinstance(value, bool):
        return "BOOL"
    if isinstance(value, int):
        return "INT64"
    if isinstance(value, float):
        return "FLOAT64"
    return "STRING"
