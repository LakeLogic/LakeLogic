"""Data profiling: one versioned JSON document per source.

``lakelogic profile <source>`` / :func:`profile` measure a dataset where it lives and
return statistics only -- never rows. Three backends, one output shape:

* **Table (SQL pushdown)** -- one aggregate query per table, generated per dialect
  (:func:`build_profile_sql`). Exactly one aggregate row comes back; the executor refuses
  any result with more than one row, so no row-fetch path exists.
* **Parquet / Delta files** -- row counts, null counts and min/max from Parquet footers or
  the Delta log; a read happens only for distincts and string lengths (``read_distincts``).
* **CSV / JSON folders (landing zone)** -- always sampled: the newest ``max_files`` files,
  capped at ``max_bytes``, read with DuckDB (local or cloud paths), plus file checks.

Document contract (``PROFILE_VERSION``):

* A statistic that could not be computed is ``None`` ("not measured"), never ``0``.
* ``sampling`` always states what was read: ``method`` is ``full`` or ``sample``.
* Columns marked ``sensitive`` carry counts only: ``min``/``max`` are ``None`` and their
  values are never selected (for pushdown, the SQL does not even ask for them).
"""

from __future__ import annotations

import csv
import io
import json
import os
import re
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from lakelogic.core.pii_names import pii_columns_by_name

PROFILE_VERSION = "1.0"

COLUMN_KEYS = (
    "name",
    "type",
    "null_count",
    "null_pct",
    "distinct_approx",
    "min",
    "max",
    "len_min",
    "len_max",
    "sensitive",
)
DOCUMENT_KEYS = (
    "profile_version",
    "source",
    "profiled_at",
    "sampling",
    "row_count",
    "freshness",
    "columns",
    "file_checks",
)

PROFILE_JSON_SCHEMA: Dict[str, Any] = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "title": "LakeLogic data profile",
    "type": "object",
    "required": list(DOCUMENT_KEYS),
    "properties": {
        "profile_version": {"const": PROFILE_VERSION},
        "source": {
            "type": "object",
            "required": ["kind", "location"],
            "properties": {
                "kind": {"enum": ["table", "parquet", "delta", "csv", "json"]},
                "location": {"type": "string"},
                "dialect": {"type": ["string", "null"]},
            },
        },
        "profiled_at": {"type": "string"},
        "sampling": {
            "type": "object",
            "required": ["method", "rows_scanned", "files_scanned", "bytes"],
            "properties": {
                "method": {"enum": ["full", "sample"]},
                "rows_scanned": {"type": ["integer", "null"]},
                "files_scanned": {"type": ["integer", "null"]},
                "bytes": {"type": ["integer", "null"]},
            },
        },
        "row_count": {"type": ["integer", "null"]},
        "freshness": {
            "type": ["object", "null"],
            "required": ["column", "value"],
        },
        "columns": {
            "type": "array",
            "items": {
                "type": "object",
                "required": list(COLUMN_KEYS),
                "properties": {
                    "name": {"type": "string"},
                    "type": {"type": ["string", "null"]},
                    "null_count": {"type": ["integer", "null"]},
                    "null_pct": {"type": ["number", "null"]},
                    "distinct_approx": {"type": ["integer", "null"]},
                    "len_min": {"type": ["integer", "null"]},
                    "len_max": {"type": ["integer", "null"]},
                    "sensitive": {"type": "boolean"},
                },
            },
        },
        "file_checks": {"type": ["object", "null"]},
    },
}


# ─────────────────────────────────────────────────────────────────────────────
# Sensitive columns -- column-name tokens + the inferrer's value patterns
# ─────────────────────────────────────────────────────────────────────────────

_SENSITIVE_CLASSIFICATIONS = {"pii", "sensitive", "confidential", "restricted", "phi", "secret"}


def _contract_sensitive_columns(contract: Any) -> set:
    """Columns a contract marks sensitive (``pii: true`` or a sensitive classification)."""
    if contract is None:
        return set()
    if isinstance(contract, (str, Path)):
        import yaml

        with open(contract, encoding="utf-8") as fh:
            contract = yaml.safe_load(fh) or {}
    if not isinstance(contract, dict) and hasattr(contract, "model_dump"):
        contract = contract.model_dump()
    model = (contract or {}).get("model") or {}
    fields = model.get("fields") or contract.get("fields") or []
    out = set()
    for f in fields:
        if not isinstance(f, dict) or not f.get("name"):
            continue
        cls = str(f.get("classification") or "").lower()
        if f.get("pii") or f.get("sensitive") or cls in _SENSITIVE_CLASSIFICATIONS:
            out.add(f["name"])
    return out


def _sensitive_by_value(sample_df: Any) -> set:
    """Columns whose sampled string values match the inferrer's PII value patterns."""
    from lakelogic.core.bootstrap import ContractInferrer as CI

    out = set()
    for col in sample_df.columns:
        if CI._DATE_COLUMN_NAME.search(col.lower()):
            continue
        series = sample_df[col].drop_nulls()
        if series.dtype.__class__.__name__.lower() not in ("utf8", "string", "large_utf8", "categorical"):
            continue
        sample = [v for v in (str(x) for x in series.head(20).to_list()) if not CI._DATE_VALUE.match(v)]
        if not sample:
            continue
        for _t, pattern in CI._PII_VALUE_PATTERNS:
            if sum(1 for v in sample if pattern.match(v)) / len(sample) >= 0.5:
                out.add(col)
                break
    return out


def detect_sensitive_columns(
    columns: Iterable[str],
    *,
    contract: Any = None,
    sensitive_columns: Optional[Iterable[str]] = None,
    sample_df: Any = None,
) -> set:
    """Union of: explicit list, contract classification, column-name tokens, sampled values.

    Pushdown passes no ``sample_df`` (no rows are fetched), so detection is by column
    name only. File backends pass a small in-process sample so value patterns count too.
    """
    cols = list(columns)
    found = set(sensitive_columns or ()) | _contract_sensitive_columns(contract)
    found |= set(pii_columns_by_name(cols))  # one matcher, shared with the inferrer
    if sample_df is not None:
        found |= _sensitive_by_value(sample_df)
    return {c for c in cols if c in found}


# ─────────────────────────────────────────────────────────────────────────────
# Type classification + per-dialect SQL
# ─────────────────────────────────────────────────────────────────────────────

_COMPLEX_RE = re.compile(
    r"^(struct|array|map|list|variant|object|json|geography|geometry|binary|blob|bytea|varbinary)", re.I
)
_STRING_RE = re.compile(r"(char|string|text|varchar|utf8|uuid|enum)", re.I)
_TEMPORAL_RE = re.compile(r"^(timestamp|datetime|date|time)", re.I)


def _type_class(type_name: Optional[str]) -> str:
    t = (type_name or "").strip()
    if not t or _COMPLEX_RE.search(t) or t.upper().endswith("[]"):
        return "complex"
    if _STRING_RE.search(t):
        return "string"
    if _TEMPORAL_RE.search(t):
        return "temporal"
    if re.search(r"bool", t, re.I):
        return "boolean"
    return "orderable"


@dataclass(frozen=True)
class _Dialect:
    name: str
    quote: Callable[[str], str]
    approx_distinct: Optional[str]  # format with {c}; None -> exact COUNT(DISTINCT)
    length: str  # format with {c}
    bool_minmax: bool = False


def _dq(n: str) -> str:
    return '"' + n.replace('"', '""') + '"'


def _bq(n: str) -> str:
    return "`" + n.replace("`", "``") + "`"


def _sq(n: str) -> str:
    return "[" + n.replace("]", "]]") + "]"


DIALECTS: Dict[str, _Dialect] = {
    "duckdb": _Dialect("duckdb", _dq, "APPROX_COUNT_DISTINCT({c})", "LENGTH(CAST({c} AS VARCHAR))"),
    "databricks": _Dialect("databricks", _bq, "APPROX_COUNT_DISTINCT({c})", "LENGTH(CAST({c} AS STRING))"),
    "spark": _Dialect("spark", _bq, "APPROX_COUNT_DISTINCT({c})", "LENGTH(CAST({c} AS STRING))"),
    "snowflake": _Dialect("snowflake", _dq, "APPROX_COUNT_DISTINCT({c})", "LENGTH(TO_VARCHAR({c}))"),
    "postgres": _Dialect("postgres", _dq, None, "LENGTH(CAST({c} AS TEXT))"),
    "tsql": _Dialect("tsql", _sq, "APPROX_COUNT_DISTINCT({c})", "LEN(CAST({c} AS NVARCHAR(MAX)))"),
}
_DIALECT_ALIASES = {
    "sqlserver": "tsql",
    "mssql": "tsql",
    "fabric": "tsql",
    "postgresql": "postgres",
    "polars": "duckdb",
}


def _dialect(name: str) -> _Dialect:
    key = _DIALECT_ALIASES.get(name.lower(), name.lower())
    if key not in DIALECTS:
        raise ValueError(f"Unsupported profile dialect: {name!r}. Supported: {sorted(DIALECTS)}")
    return DIALECTS[key]


def _quote_table(table: str, d: _Dialect) -> str:
    """Quote each part of a dotted table name unless the caller already quoted it."""
    if any(ch in table for ch in '"`[('):
        return table
    return ".".join(d.quote(p) for p in table.split("."))


@dataclass
class _Plan:
    """What the aggregate query computes: alias -> (column, stat)."""

    sql: str
    aliases: Dict[str, Tuple[Optional[str], str]]


def build_profile_sql(
    table: str,
    columns: Sequence[Tuple[str, str]],
    dialect: str = "duckdb",
    *,
    sensitive: Iterable[str] = (),
    stats: Iterable[str] = ("nulls", "distinct", "minmax", "length"),
) -> _Plan:
    """One aggregate-only SELECT for a whole table.

    Every select item is an aggregate, there is no GROUP BY, no LIMIT and no ``*``
    projection -- the result is always exactly one row. Sensitive columns never get
    MIN/MAX (their values must not leave the engine). Complex types get counts only.
    """
    d = _dialect(dialect)
    sens = set(sensitive)
    want = set(stats)
    items: List[str] = ["COUNT(*) AS rc"]
    aliases: Dict[str, Tuple[Optional[str], str]] = {"rc": (None, "row_count")}
    for i, (name, typ) in enumerate(columns):
        c = d.quote(name)
        tc = _type_class(typ)

        def add(stat: str, expr: str) -> None:
            alias = f"c{i}_{stat}"
            items.append(f"{expr} AS {alias}")
            aliases[alias] = (name, stat)

        if "nulls" in want:
            add("nn", f"COUNT({c})")
        if tc == "complex":
            continue
        if "distinct" in want:
            add("nd", d.approx_distinct.format(c=c) if d.approx_distinct else f"COUNT(DISTINCT {c})")
        if "minmax" in want and name not in sens and (tc != "boolean" or d.bool_minmax):
            add("mn", f"MIN({c})")
            add("mx", f"MAX({c})")
        if "length" in want and tc == "string":
            add("ln", f"MIN({d.length.format(c=c)})")
            add("lx", f"MAX({d.length.format(c=c)})")
    sql = "SELECT\n  " + ",\n  ".join(items) + f"\nFROM {_quote_table(table, d)}"
    return _Plan(sql=sql, aliases=aliases)


# ─────────────────────────────────────────────────────────────────────────────
# Executors -- reuse the connection types infer_contract already accepts
# ─────────────────────────────────────────────────────────────────────────────


class AggregateRowError(RuntimeError):
    """A profile query returned something other than exactly one aggregate row."""


class SqlExecutor:
    """Runs SQL that returns a handful of rows (schema lookup) or one aggregate row."""

    dialect = "duckdb"

    def query(self, sql: str) -> Tuple[List[str], List[tuple]]:  # pragma: no cover - interface
        raise NotImplementedError

    def columns(self, table: str) -> List[Tuple[str, str]]:  # pragma: no cover - interface
        raise NotImplementedError

    def aggregate_row(self, sql: str) -> Dict[str, Any]:
        """The only data path: one row of aggregates, or an error."""
        names, rows = self.query(sql)
        if len(rows) != 1:
            raise AggregateRowError(f"Profile query must return exactly one aggregate row; got {len(rows)}.")
        return dict(zip([n.lower() for n in names], rows[0]))


class DuckDBExecutor(SqlExecutor):
    dialect = "duckdb"

    def __init__(self, conn: Any = None, database: Optional[str] = None):
        import duckdb

        self.conn = conn if conn is not None else duckdb.connect(database or ":memory:", read_only=bool(database))

    def query(self, sql: str):
        cur = self.conn.execute(sql)
        names = [d[0] for d in cur.description]
        return names, cur.fetchmany(2)  # one aggregate row; a 2nd row is an error, never a scan

    def columns(self, table: str):
        cur = self.conn.execute(f"DESCRIBE {table}")
        return [(r[0], str(r[1])) for r in cur.fetchall()]


class SparkExecutor(SqlExecutor):
    dialect = "spark"

    def __init__(self, spark: Any):
        self.spark = spark
        if "databricks" in str(
            getattr(getattr(spark, "conf", None), "get", lambda *_: "")(
                "spark.databricks.clusterUsageTags.clusterId", ""
            )
            or ""
        ):
            self.dialect = "databricks"

    def query(self, sql: str):
        df = self.spark.sql(sql)
        return list(df.columns), [tuple(r) for r in df.take(2)]

    def columns(self, table: str):
        return [(f.name, f.dataType.simpleString()) for f in self.spark.table(table).schema.fields]


class SqlAlchemyExecutor(SqlExecutor):
    def __init__(self, engine: Any):
        if isinstance(engine, str):
            from sqlalchemy import create_engine

            engine = create_engine(engine)
        self.engine = engine
        name = engine.dialect.name
        self.dialect = {"mssql": "tsql", "postgresql": "postgres"}.get(name, name)

    def query(self, sql: str):
        from sqlalchemy import text

        with self.engine.connect() as cx:
            res = cx.execute(text(sql))
            return list(res.keys()), [tuple(r) for r in res.fetchmany(2)]

    def columns(self, table: str):
        from sqlalchemy import inspect

        schema, _, name = table.rpartition(".")
        return [(c["name"], str(c["type"])) for c in inspect(self.engine).get_columns(name, schema=schema or None)]


class DatabricksStatementExecutor(SqlExecutor):
    """Databricks SQL Statement Execution API (the same API the SaaS scan uses).

    Credentials come from arguments or ``DATABRICKS_HOST`` / ``DATABRICKS_TOKEN`` /
    ``DATABRICKS_WAREHOUSE_ID``; nothing is logged.
    """

    dialect = "databricks"

    def __init__(
        self,
        host: Optional[str] = None,
        token: Optional[str] = None,
        warehouse_id: Optional[str] = None,
        timeout_s: int = 120,
    ):
        self.host = (host or os.environ.get("DATABRICKS_HOST") or "").rstrip("/")
        self.token = token or os.environ.get("DATABRICKS_TOKEN")
        self.warehouse_id = warehouse_id or os.environ.get("DATABRICKS_WAREHOUSE_ID")
        self.timeout_s = timeout_s
        if not (self.host and self.token and self.warehouse_id):
            raise ValueError(
                "Databricks profiling needs host, token and warehouse_id (or DATABRICKS_HOST/TOKEN/WAREHOUSE_ID)."
            )
        if not self.host.startswith("http"):
            self.host = "https://" + self.host

    def query(self, sql: str):
        return self._run(sql, row_limit=2)  # one aggregate row; a 2nd row is an error

    def _run(self, sql: str, row_limit: Optional[int]):
        import time as _time

        import requests

        hdr = {"Authorization": f"Bearer {self.token}"}
        body = {
            "statement": sql,
            "warehouse_id": self.warehouse_id,
            "wait_timeout": "30s",
            "disposition": "INLINE",
            "format": "JSON_ARRAY",
        }
        if row_limit is not None:
            body["row_limit"] = row_limit
        r = requests.post(f"{self.host}/api/2.0/sql/statements", json=body, headers=hdr, timeout=60)
        r.raise_for_status()
        data = r.json()
        deadline = _time.monotonic() + self.timeout_s
        while data.get("status", {}).get("state") in ("PENDING", "RUNNING"):
            if _time.monotonic() > deadline:
                raise TimeoutError("Databricks profile query timed out")
            _time.sleep(2)
            r = requests.get(f"{self.host}/api/2.0/sql/statements/{data['statement_id']}", headers=hdr, timeout=60)
            r.raise_for_status()
            data = r.json()
        state = data.get("status", {}).get("state")
        if state != "SUCCEEDED":
            raise RuntimeError(
                f"Databricks statement {state}: {data.get('status', {}).get('error', {}).get('message')}"
            )
        names = [c["name"] for c in data["manifest"]["schema"]["columns"]]
        rows = [tuple(r) for r in (data.get("result") or {}).get("data_array") or []]
        return names, rows

    def columns(self, table: str):
        parts = table.replace("`", "").split(".")
        if len(parts) != 3:
            raise ValueError("Databricks tables must be catalog.schema.table")
        cat, sch, tbl = (p.replace("'", "") for p in parts)
        # Schema metadata only (information_schema) -- not table rows.
        sql = (
            f"SELECT column_name, full_data_type FROM `{cat}`.information_schema.columns "
            f"WHERE table_schema = '{sch}' AND table_name = '{tbl}' ORDER BY ordinal_position"
        )
        _, rows = self._run(sql, row_limit=None)
        return [(r[0], r[1]) for r in rows]


def executor_for(connection: Any, dialect: Optional[str] = None) -> SqlExecutor:
    """Pick an executor for the connection types ``infer_contract`` already accepts."""
    if isinstance(connection, SqlExecutor):
        ex = connection
    else:
        t = type(connection).__name__
        mod = type(connection).__module__ or ""
        if "SparkSession" in t or "pyspark" in mod:
            ex = SparkExecutor(connection)
        elif "DuckDBPyConnection" in t or "duckdb" in mod:
            ex = DuckDBExecutor(connection)
        elif "Engine" in t or (isinstance(connection, str) and "://" in connection):
            ex = SqlAlchemyExecutor(connection)
        else:
            raise TypeError(f"Unsupported connection type for profiling: {t}")
    if dialect:
        ex.dialect = dialect
    return ex


# ─────────────────────────────────────────────────────────────────────────────
# Shared assembly
# ─────────────────────────────────────────────────────────────────────────────


def _jsonable(v: Any) -> Any:
    if v is None or isinstance(v, (bool, int, str)):
        return v
    if isinstance(v, float):
        return v if v == v and v not in (float("inf"), float("-inf")) else None
    if isinstance(v, Decimal):
        return float(v)
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, (date, time)):
        return v.isoformat()
    if isinstance(v, bytes):
        return None
    return str(v)


def _int_or_none(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _column_entry(
    name: str,
    typ: Optional[str],
    row_count: Optional[int],
    sensitive: bool,
    *,
    null_count=None,
    distinct=None,
    mn=None,
    mx=None,
    ln=None,
    lx=None,
) -> Dict[str, Any]:
    null_count = _int_or_none(null_count)
    null_pct = None
    if null_count is not None and row_count:
        null_pct = round(100.0 * null_count / row_count, 2)
    distinct = _int_or_none(distinct)
    if distinct is not None and null_count is not None and row_count is not None:
        # HyperLogLog can overshoot; distinct values cannot exceed non-null values.
        distinct = min(distinct, row_count - null_count)
    return {
        "name": name,
        "type": typ,
        "null_count": null_count,
        "null_pct": null_pct,
        "distinct_approx": distinct,
        "min": None if sensitive else _jsonable(mn),
        "max": None if sensitive else _jsonable(mx),
        "len_min": _int_or_none(ln),
        "len_max": _int_or_none(lx),
        "sensitive": bool(sensitive),
    }


_FRESHNESS_HINT = re.compile(r"(updated|modified|ingest|load|processed|event|created|_at$|_ts$|timestamp)", re.I)


def _freshness(columns: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Newest timestamp/date column and its max. Sensitive columns never qualify."""
    best = None
    for c in columns:
        if c["sensitive"] or c["max"] is None or _type_class(c["type"]) != "temporal":
            continue
        key = (str(c["max"]), bool(_FRESHNESS_HINT.search(c["name"])))
        if best is None or key > best[0]:
            best = (key, c)
    if best is None:
        return None
    return {"column": best[1]["name"], "value": best[1]["max"]}


def _document(
    kind: str, location: str, *, dialect=None, sampling, row_count, columns, file_checks=None, extra=None
) -> Dict[str, Any]:
    doc = {
        "profile_version": PROFILE_VERSION,
        "source": {"kind": kind, "location": location, "dialect": dialect},
        "profiled_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sampling": sampling,
        "row_count": _int_or_none(row_count),
        "freshness": _freshness(columns),
        "columns": columns,
        "file_checks": file_checks,
    }
    if extra:
        doc.update(extra)
    return doc


def _columns_from_row(
    row: Dict[str, Any],
    plan: _Plan,
    schema: Sequence[Tuple[str, str]],
    sens: set,
    row_count: Optional[int],
    overrides: Optional[Dict[str, Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    per: Dict[str, Dict[str, Any]] = {n: {} for n, _ in schema}
    for alias, (col, stat) in plan.aliases.items():
        if col is not None:
            per[col][stat] = row.get(alias)
    out = []
    for name, typ in schema:
        s = per[name]
        nn = s.get("nn")
        null_count = (row_count - int(nn)) if (nn is not None and row_count is not None) else None
        vals = dict(
            null_count=null_count, distinct=s.get("nd"), mn=s.get("mn"), mx=s.get("mx"), ln=s.get("ln"), lx=s.get("lx")
        )
        if overrides and name in overrides:
            vals.update({k: v for k, v in overrides[name].items()})
        out.append(_column_entry(name, typ, row_count, name in sens, **vals))
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Backend A -- table pushdown
# ─────────────────────────────────────────────────────────────────────────────


def profile_table(
    table: str,
    connection: Any,
    *,
    dialect: Optional[str] = None,
    contract: Any = None,
    sensitive_columns: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """Profile a table with ONE aggregate query pushed down to the engine."""
    ex = executor_for(connection, dialect)
    schema = ex.columns(table)
    sens = detect_sensitive_columns([n for n, _ in schema], contract=contract, sensitive_columns=sensitive_columns)
    plan = build_profile_sql(table, schema, ex.dialect, sensitive=sens)
    row = ex.aggregate_row(plan.sql)
    rc = _int_or_none(row.get("rc"))
    cols = _columns_from_row(row, plan, schema, sens, rc)
    return _document(
        "table",
        table,
        dialect=ex.dialect,
        sampling={"method": "full", "rows_scanned": rc, "files_scanned": None, "bytes": None},
        row_count=rc,
        columns=cols,
        extra={"pushdown": {"sql": plan.sql, "rows_returned": 1}},
    )


# ─────────────────────────────────────────────────────────────────────────────
# Backend B -- Parquet / Delta metadata
# ─────────────────────────────────────────────────────────────────────────────


def _sql_list(paths: Sequence[str]) -> str:
    return "[" + ", ".join("'" + p.replace("\\", "/").replace("'", "''") + "'" for p in paths) + "]"


def _read_stats_with_duckdb(
    reader_sql: str, schema, sens, stats=("distinct", "length")
) -> Tuple[Dict[str, Any], _Plan]:
    import duckdb

    con = duckdb.connect()
    con.execute(f"CREATE VIEW _profile_src AS SELECT * FROM {reader_sql}")
    plan = build_profile_sql("_profile_src", schema, "duckdb", sensitive=sens, stats=stats)
    return DuckDBExecutor(con).aggregate_row(plan.sql), plan


def _merge_minmax(cur, new, pick):
    if new is None:
        return cur
    if cur is None:
        return new
    try:
        return pick(cur, new)
    except TypeError:
        return cur


def _parquet_footer_stats(files: Sequence[str]) -> Tuple[int, List[Tuple[str, str]], Dict[str, Dict[str, Any]]]:
    """Row count, schema and per-column null/min/max from footers only (no data pages)."""
    import pyarrow.parquet as pq

    total = 0
    schema: List[Tuple[str, str]] = []
    acc: Dict[str, Dict[str, Any]] = {}
    for f in files:
        md = pq.ParquetFile(f).metadata
        if not schema:
            arrow = md.schema.to_arrow_schema()
            schema = [(fld.name, str(fld.type)) for fld in arrow]
            acc = {n: {"null_count": 0, "mn": None, "mx": None, "complete": True} for n, _ in schema}
        total += md.num_rows
        for rg in range(md.num_row_groups):
            g = md.row_group(rg)
            for ci in range(g.num_columns):
                col = g.column(ci)
                name = col.path_in_schema.split(".")[0]
                if name not in acc or "." in col.path_in_schema:
                    if name in acc:
                        acc[name]["complete"] = False
                    continue
                st = col.statistics
                if st is None or not st.has_null_count:
                    acc[name]["null_count"] = None
                elif acc[name]["null_count"] is not None:
                    acc[name]["null_count"] += st.null_count
                if st is None or not st.has_min_max:
                    if g.num_rows - (st.null_count if st is not None and st.has_null_count else 0) > 0:
                        acc[name]["complete"] = False
                else:
                    acc[name]["mn"] = _merge_minmax(acc[name]["mn"], st.min, min)
                    acc[name]["mx"] = _merge_minmax(acc[name]["mx"], st.max, max)
    for a in acc.values():
        if not a["complete"]:
            a["mn"] = a["mx"] = None
    return total, schema, acc


def _list_files(location: str, pattern: str) -> List[Dict[str, Any]]:
    """filename/size/last_modified for every file under ``location`` (local or cloud).

    Uses DuckDB ``read_blob`` with only metadata columns projected, so contents are not read.
    """
    import duckdb

    loc = location.replace("\\", "/").rstrip("/")
    glob = loc if any(ch in loc for ch in "*?[") else f"{loc}/{pattern}"
    try:
        rows = duckdb.sql(f"SELECT filename, size, last_modified FROM read_blob('{glob}')").fetchall()
    except duckdb.IOException:
        return []
    out = []
    for fn, size, mtime in rows:
        if isinstance(mtime, datetime) and mtime.tzinfo is not None:
            mtime = mtime.astimezone(timezone.utc)
        out.append({"path": fn, "size": int(size or 0), "last_modified": mtime})
    return out


def profile_parquet(
    location: str,
    *,
    read_distincts: bool = True,
    contract: Any = None,
    sensitive_columns: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """Profile Parquet files from footers; read only for distincts/lengths."""
    p = Path(location)
    if p.is_file():
        files = [str(p)]
    else:
        files = sorted(f["path"] for f in _list_files(location, "**/*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files under {location}")
    rc, schema, acc = _parquet_footer_stats(files)
    nbytes = sum(os.path.getsize(f) for f in files if os.path.exists(f)) or None
    return _metadata_profile(
        "parquet", location, files, rc, schema, acc, nbytes, read_distincts, contract, sensitive_columns
    )


def profile_delta(
    location: str,
    *,
    read_distincts: bool = True,
    contract: Any = None,
    sensitive_columns: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """Profile a Delta table from its log's per-file stats (numRecords, nullCount, min/max)."""
    import pyarrow as pa
    from deltalake import DeltaTable

    dt = DeltaTable(location)
    arrow_schema = pa.schema(dt.schema().to_arrow()) if hasattr(dt.schema(), "to_arrow") else dt.schema().to_pyarrow()
    schema = [(f.name, str(f.type)) for f in arrow_schema]
    actions = pa.table(dt.get_add_actions(flatten=True)).to_pylist()
    files = list(dt.file_uris())
    rc: Optional[int] = 0
    acc = {n: {"null_count": 0, "mn": None, "mx": None, "complete": True} for n, _ in schema}
    for a in actions:
        nr = a.get("num_records")
        rc = None if (rc is None or nr is None) else rc + int(nr)
        for n in acc:
            nc = a.get(f"null_count.{n}")
            if nc is None:
                acc[n]["null_count"] = None
            elif acc[n]["null_count"] is not None:
                acc[n]["null_count"] += int(nc)
            mn, mx = a.get(f"min.{n}"), a.get(f"max.{n}")
            if mn is None and mx is None:
                if not (nr is not None and nc is not None and int(nc) == int(nr)):
                    acc[n]["complete"] = False
            else:
                acc[n]["mn"] = _merge_minmax(acc[n]["mn"], mn, min)
                acc[n]["mx"] = _merge_minmax(acc[n]["mx"], mx, max)
    for a in acc.values():
        if not a["complete"]:
            a["mn"] = a["mx"] = None
    nbytes = sum(int(a.get("size_bytes") or 0) for a in actions) or None
    return _metadata_profile(
        "delta", location, files, rc, schema, acc, nbytes, read_distincts, contract, sensitive_columns
    )


def _metadata_profile(kind, location, files, rc, schema, acc, nbytes, read_distincts, contract, sensitive_columns):
    sample_df = None
    if read_distincts:
        import duckdb

        sample_df = duckdb.sql(f"SELECT * FROM read_parquet({_sql_list(files)}, union_by_name=true) LIMIT 20").pl()
    sens = detect_sensitive_columns(
        [n for n, _ in schema], contract=contract, sensitive_columns=sensitive_columns, sample_df=sample_df
    )
    overrides = {
        n: {"null_count": a["null_count"], "mn": a["mn"], "mx": a["mx"], "distinct": None, "ln": None, "lx": None}
        for n, a in acc.items()
    }
    if read_distincts:
        row, plan = _read_stats_with_duckdb(f"read_parquet({_sql_list(files)}, union_by_name=true)", schema, sens)
        for alias, (col, stat) in plan.aliases.items():
            if col is None:
                continue
            key = {"nd": "distinct", "ln": "ln", "lx": "lx"}.get(stat)
            if key:
                overrides[col][key] = row.get(alias)
    cols = [_column_entry(n, t, rc, n in sens, **overrides[n]) for n, t in schema]
    return _document(
        kind,
        location,
        sampling={
            "method": "full",
            "rows_scanned": rc if read_distincts else 0,
            "files_scanned": len(files),
            "bytes": nbytes,
        },
        row_count=rc,
        columns=cols,
        extra={
            "stats_source": {
                "row_count": "metadata",
                "nulls_min_max": "metadata",
                "distinct_length": "read" if read_distincts else "not measured",
            }
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# Backend C -- CSV / JSON landing folders (sampled) + file checks
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_MAX_FILES = 10
DEFAULT_MAX_BYTES = 64 * 1024 * 1024


def _read_bytes(path: str, limit: int) -> bytes:
    if os.path.exists(path):
        with open(path, "rb") as fh:
            return fh.read(limit)
    import fsspec

    with fsspec.open(path, "rb") as fh:
        return fh.read(limit)


def _guess_encoding(raw: bytes) -> str:
    if raw.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    if raw.startswith((b"\xff\xfe", b"\xfe\xff")):
        return "utf-16"
    try:
        raw.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError as exc:
        # A multi-byte char cut at the byte cap is still UTF-8.
        if exc.start >= len(raw) - 3:
            return "utf-8"
        return "cp1252"


def _guess_delimiter(text: str) -> Optional[str]:
    head = "\n".join(text.splitlines()[:20])
    if not head:
        return None
    try:
        return csv.Sniffer().sniff(head, delimiters=",;|\t").delimiter
    except csv.Error:
        return None


def _csv_file_check(path: str, raw: bytes, truncated: bool) -> Dict[str, Any]:
    enc = _guess_encoding(raw)
    text = raw.decode(enc, errors="replace")
    if truncated:  # drop the partial last line so it is not miscounted as ragged
        text = text[: text.rfind("\n") + 1] if "\n" in text else ""
    delim = _guess_delimiter(text) or ","
    rows = list(csv.reader(io.StringIO(text), delimiter=delim))
    rows = [r for r in rows if r]
    header = [h.strip() for h in rows[0]] if rows else None
    data = rows[1:]
    ragged = sum(1 for r in data if header is not None and len(r) != len(header))
    return {
        "path": path,
        "encoding": enc,
        "delimiter": delim,
        "header": header,
        "data_rows": len(data),
        "ragged_rows": ragged,
        "empty": len(data) == 0,
    }


def _json_file_check(path: str, raw: bytes, truncated: bool) -> Dict[str, Any]:
    enc = _guess_encoding(raw)
    text = raw.decode(enc, errors="replace")
    if truncated:
        text = text[: text.rfind("\n") + 1] if "\n" in text else ""
    stripped = text.strip()
    malformed = 0
    records = 0
    if stripped.startswith("["):
        try:
            records = len(json.loads(stripped))
        except ValueError:
            malformed = 1
    else:
        for line in stripped.splitlines():
            if not line.strip():
                continue
            try:
                json.loads(line)
                records += 1
            except ValueError:
                malformed += 1
    return {
        "path": path,
        "encoding": enc,
        "delimiter": None,
        "header": None,
        "data_rows": records,
        "malformed_rows": malformed,
        "empty": records == 0,
    }


def profile_files(
    location: str,
    *,
    fmt: Optional[str] = None,
    max_files: int = DEFAULT_MAX_FILES,
    max_bytes: int = DEFAULT_MAX_BYTES,
    contract: Any = None,
    sensitive_columns: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """Sampled profile of a CSV/JSON folder: newest ``max_files`` files within ``max_bytes``."""
    loc = location.replace("\\", "/")
    if fmt is None:
        low = loc.lower()
        fmt = "json" if (".json" in low or ".ndjson" in low or ".jsonl" in low) else "csv"
    exts = ("*.json", "*.jsonl", "*.ndjson") if fmt == "json" else ("*.csv", "*.txt", "*.tsv")
    p = Path(location)
    if p.is_file():
        listing = [
            {
                "path": str(p),
                "size": p.stat().st_size,
                "last_modified": datetime.fromtimestamp(p.stat().st_mtime, timezone.utc),
            }
        ]
    else:
        listing = []
        for ext in exts:
            listing += _list_files(location, f"**/{ext}")
    if not listing:
        raise FileNotFoundError(f"No {fmt} files under {location}")

    # Newest first; ties broken by path so the latest partition (y_/m_/d_) wins.
    epoch = datetime.min.replace(tzinfo=timezone.utc)
    ordered = sorted(listing, key=lambda f: (f["last_modified"] or epoch, f["path"]), reverse=True)
    chosen: List[Dict[str, Any]] = []
    budget = max_bytes
    for f in ordered:
        if len(chosen) >= max_files or budget <= 0:
            break
        take = min(f["size"], budget)
        chosen.append({**f, "read_bytes": take, "truncated": take < f["size"]})
        budget -= take

    checks = []
    for f in chosen:
        raw = _read_bytes(f["path"], f["read_bytes"]) if f["read_bytes"] else b""
        fc = (_json_file_check if fmt == "json" else _csv_file_check)(f["path"], raw, f["truncated"])
        fc["size"] = f["size"]
        fc["truncated"] = f["truncated"]
        checks.append(fc)

    readable = [c for c in checks if not c["empty"] and not c["truncated"]]
    partial = [c for c in checks if not c["empty"] and c["truncated"]]
    columns: List[Dict[str, Any]] = []
    rc: Optional[int] = None
    if readable or partial:
        import duckdb

        con = duckdb.connect()
        if readable:
            paths = _sql_list([c["path"] for c in readable])
            if fmt == "json":
                reader = f"read_json_auto({paths}, union_by_name=true, ignore_errors=true)"
            else:
                enc = _duck_encoding(readable[0]["encoding"])
                reader = f"read_csv({paths}, union_by_name=true, ignore_errors=true, encoding='{enc}')"
            con.execute(f"CREATE VIEW _profile_src AS SELECT * FROM {reader}")
        else:
            # Only a single oversize file: profile the capped prefix that was read.
            c0 = partial[0]
            raw = _read_bytes(c0["path"], max_bytes)
            raw = raw[: raw.rfind(b"\n") + 1]
            tmp = io.BytesIO(raw)
            import pyarrow.csv as pacsv
            import pyarrow.json as pajson

            tbl = (
                pajson.read_json(tmp)
                if fmt == "json"
                else pacsv.read_csv(
                    tmp, parse_options=pacsv.ParseOptions(delimiter=c0["delimiter"] or ",", newlines_in_values=True)
                )
            )
            con.register("_profile_src", tbl)
        schema = [(r[0], str(r[1])) for r in con.execute("DESCRIBE _profile_src").fetchall()]
        sample_df = con.execute("SELECT * FROM _profile_src LIMIT 20").pl()
        sens = detect_sensitive_columns(
            [n for n, _ in schema], contract=contract, sensitive_columns=sensitive_columns, sample_df=sample_df
        )
        plan = build_profile_sql("_profile_src", schema, "duckdb", sensitive=sens)
        row = DuckDBExecutor(con).aggregate_row(plan.sql)
        rc = _int_or_none(row.get("rc"))
        columns = _columns_from_row(row, plan, schema, sens, rc)

    headers = {tuple(c["header"]) for c in checks if c.get("header")}
    newest = max((f["last_modified"] for f in listing if f["last_modified"]), default=None)
    file_checks = {
        "file_count": len(listing),
        "files_sampled": len(chosen),
        "newest_file": max(listing, key=lambda f: (f["last_modified"] or epoch, f["path"]))["path"],
        "newest_file_time": newest.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if newest else None,
        "empty_files": [c["path"] for c in checks if c["empty"]],
        "header_consistent": (len(headers) <= 1) if fmt != "json" else None,
        "header_variants": [list(h) for h in sorted(headers)] if len(headers) > 1 else [],
        "ragged_rows": sum(c.get("ragged_rows", 0) for c in checks) if fmt != "json" else None,
        "malformed_rows": sum(c.get("malformed_rows", 0) for c in checks) if fmt == "json" else None,
        "encodings": sorted({c["encoding"] for c in checks}),
        "delimiters": sorted({c["delimiter"] for c in checks if c["delimiter"]}) if fmt != "json" else None,
        "files": [{k: v for k, v in c.items() if k != "header"} for c in checks],
    }
    sample = len(chosen) < len(listing) or any(c["truncated"] for c in checks)
    return _document(
        fmt,
        location,
        sampling={
            "method": "sample" if sample else "full",
            "rows_scanned": rc,
            "files_scanned": len(chosen),
            "bytes": sum(f["read_bytes"] for f in chosen),
            "max_files": max_files,
            "max_bytes": max_bytes,
            "selection": "newest files first",
        },
        row_count=rc,
        columns=columns,
        file_checks=file_checks,
    )


def _duck_encoding(enc: str) -> str:
    return {"utf-8-sig": "utf-8", "cp1252": "latin-1", "utf-16": "utf-16"}.get(enc, "utf-8")


# ─────────────────────────────────────────────────────────────────────────────
# Public entry point
# ─────────────────────────────────────────────────────────────────────────────


def profile(
    source: str,
    *,
    connection: Any = None,
    dialect: Optional[str] = None,
    fmt: Optional[str] = None,
    max_files: int = DEFAULT_MAX_FILES,
    max_bytes: int = DEFAULT_MAX_BYTES,
    read_distincts: bool = True,
    contract: Any = None,
    sensitive_columns: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """Profile a table (with ``connection``) or a file location; returns the profile document.

    Examples::

        lakelogic.profile("main.trips", connection=duckdb.connect("lake.duckdb"))
        lakelogic.profile("landing/rider_profiles/", max_files=5, max_bytes=16_000_000)
        lakelogic.profile("silver/trips", fmt="delta")
    """
    common = dict(contract=contract, sensitive_columns=sensitive_columns)
    if connection is not None:
        return profile_table(source, connection, dialect=dialect, **common)
    s = str(source)
    low = s.lower().rstrip("/\\")
    if fmt == "delta" or (fmt is None and os.path.isdir(os.path.join(s, "_delta_log"))):
        return profile_delta(s, read_distincts=read_distincts, **common)
    if fmt == "parquet" or (fmt is None and (low.endswith(".parquet") or _has_ext(s, ".parquet"))):
        return profile_parquet(s, read_distincts=read_distincts, **common)
    return profile_files(s, fmt=fmt, max_files=max_files, max_bytes=max_bytes, **common)


def _has_ext(location: str, ext: str) -> bool:
    if not os.path.isdir(location):
        return False
    for _root, _dirs, names in os.walk(location):
        if any(n.lower().endswith(ext) for n in names):
            return True
    return False
