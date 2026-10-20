"""Check a table against its contract's rules, without running a pipeline (#570).

A scheduled check of the agreed rules — from the platform on a schedule, or from the client's
own orchestrator — that answers "does the data meet the rules right now?" and reports it in
the SAME per-rule record a pipeline run sends (``run_log.build_rule_results``), so one history
holds both.

How a row rule is judged: a row FAILS a rule when the rule's SQL is not true for it — false OR
null — exactly as the pipeline's rule filter keeps only rows where it is true. All row rules
run in ONE aggregate query; if that query fails (one bad rule breaks it), each rule is retried
alone, and a rule that still cannot run reads ``not_evaluated`` (never passed).

Dataset rules are whole-table queries written for the pipeline's engine; they are reported
``not_evaluated`` here rather than guessed at.

Counts only: no value leaves the table.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from lakelogic.core.run_log import MEASUREMENT_VERSION, build_rule_results, rules_fingerprint

#: What a check row is tagged as when it reaches the platform: a check, not a pipeline run.
RECORD_TYPE = "quality_check"
ENGINE = "quality_check"


def _once(rules: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """An IDENTICAL repeat of a rule (same name and definition, e.g. a not-null rule written out
    beside the field's own ``required``) is one rule: checked and reported once, not twice."""
    seen, out = set(), []
    for r in rules:
        key = json.dumps(r, sort_keys=True, default=str)  # the WHOLE rule: shorthand forms have no sql
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out


def _row_rules(contract: Dict[str, Any]) -> List[Dict[str, Any]]:
    q = (contract or {}).get("quality") or {}
    return _once([r for r in (q.get("row_rules") or []) if isinstance(r, dict) and r.get("name") and r.get("sql")])


def _dataset_rules(contract: Dict[str, Any]) -> List[Dict[str, Any]]:
    q = (contract or {}).get("quality") or {}
    return _once([r for r in (q.get("dataset_rules") or []) if isinstance(r, dict)])


_TABLE = "__lakelogic_checked_table__"


def _expand_dataset(rule: Dict[str, Any]) -> Any:
    """One dataset rule as the engines run it (``QualityRule``: one scalar SQL + its bound),
    through the SAME expansion the pipeline uses (``EngineAdapter._expand_dataset_rule``), so
    ``unique`` / ``null_ratio`` / ``row_count_between`` mean here what they mean in a run.
    None: a form the expansion does not know (left not evaluated, never passed)."""
    from types import SimpleNamespace

    from lakelogic.core.models import Quality
    from lakelogic.engines.base import EngineAdapter

    try:
        spec = Quality(dataset_rules=[rule]).dataset_rules[0]
        shim = SimpleNamespace(contract=SimpleNamespace(dataset=_TABLE), _quote_ident=lambda n: n)
        return EngineAdapter._expand_dataset_rule(shim, spec)
    except Exception:  # noqa: BLE001
        return None


def _bound_passed(rule: Any, value: Any) -> Optional[bool]:
    if value is None:
        return None
    v = float(value)
    if rule.must_be_less_than is not None:
        return v < float(rule.must_be_less_than)
    if rule.must_be_greater_than is not None:
        return v > float(rule.must_be_greater_than)
    if getattr(rule, "must_be_between", None) is not None:
        lo, hi = rule.must_be_between
        return float(lo) <= v <= float(hi)
    return None


def _check_dataset_rules(
    executor: Any, table: str, contract: Dict[str, Any], rows_checked: Optional[int], errors: List[Dict[str, str]]
) -> List[Dict[str, Any]]:
    """Each dataset rule's own verdict. A rule that cannot be expanded or run is
    ``not_evaluated`` (with its error), never passed."""
    out = []
    for i, raw in enumerate(_dataset_rules(contract)):
        rule = _expand_dataset(raw)
        name = getattr(rule, "name", None) or raw.get("name") or f"dataset_rule_{i + 1}"
        entry = {
            "name": name,
            "kind": "dataset",
            "category": getattr(rule, "category", None) or raw.get("category"),
            "status": "not_evaluated",
            "failed_rows": None,
            "rows_evaluated": None,
        }
        if rule is not None and rule.sql:
            try:
                row = executor.aggregate_row(rule.sql.replace(_TABLE, table)) or {}
                value = next(iter(row.values()), None)
                passed = _bound_passed(rule, value)
                if passed is not None:
                    entry.update(
                        status="passed" if passed else "failed",
                        rows_evaluated=rows_checked,
                        value=float(value),
                        # unique: the value IS the duplicate rows; other bounds are not row counts
                        failed_rows=int(value) if name.endswith("_unique") or "unique" in raw else None,
                    )
            except Exception as exc:  # noqa: BLE001
                errors.append({"rule": name, "error": f"{type(exc).__name__}: {str(exc)[:200]}"})
        out.append(entry)
    return out


def _fails(sql: str) -> str:
    """1 for a row the rule does not keep (false or null), else 0."""
    return f"CASE WHEN ({sql}) THEN 0 ELSE 1 END"


def _int(v: Any) -> Optional[int]:
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def check_rules(
    executor: Any, table: str, contract: Dict[str, Any], *, dataset_table: Optional[str] = None
) -> Dict[str, Any]:
    """Evaluate ``contract``'s row rules on ``table`` through ``executor`` (``aggregate_row(sql)``
    returns one row as a dict). Returns the per-rule record plus ``rows_checked``.

    Dataset rules (unique, null ratio, row count) judge the WHOLE table — ``dataset_table``
    when ``table`` is only the changed rows of an incremental check; a subset cannot say
    whether a key is unique across the table."""
    rules = _row_rules(contract)
    rows_checked: Optional[int] = None
    failed: Dict[str, int] = {}
    unrun: List[str] = []
    errors: List[Dict[str, str]] = []
    if rules:
        cols = ", ".join(f"SUM({_fails(r['sql'])}) AS r{i}" for i, r in enumerate(rules))
        try:
            row = executor.aggregate_row(f"SELECT COUNT(*) AS rows_checked, {cols} FROM {table}") or {}
            rows_checked = _int(row.get("rows_checked"))
            for i, r in enumerate(rules):
                failed[r["name"]] = _int(row.get(f"r{i}")) or 0
        except Exception:  # noqa: BLE001 — one bad rule must not hide the others
            for r in rules:
                try:
                    row = (
                        executor.aggregate_row(
                            f"SELECT COUNT(*) AS rows_checked, SUM({_fails(r['sql'])}) AS f FROM {table}"
                        )
                        or {}
                    )
                    rows_checked = _int(row.get("rows_checked")) if rows_checked is None else rows_checked
                    failed[r["name"]] = _int(row.get("f")) or 0
                except Exception as exc:  # noqa: BLE001
                    unrun.append(r["name"])
                    errors.append({"rule": r["name"], "error": f"{type(exc).__name__}: {str(exc)[:200]}"})
    failures = [{"name": n, "count": c} for n, c in failed.items() if c > 0]
    results = build_rule_results(rules, [], [], failures, rows_checked)
    for e in results:
        if e["name"] in unrun:
            e.update(status="not_evaluated", failed_rows=None, rows_evaluated=None)
    results += _check_dataset_rules(
        executor, dataset_table or table, contract, rows_checked if dataset_table is None else None, errors
    )
    return {
        "rule_results": results,
        "rows_checked": rows_checked,
        "rules_fingerprint": rules_fingerprint(rules, _dataset_rules(contract)),
        "measurement_version": MEASUREMENT_VERSION,
        "errors": errors,
    }


def run_log_payload(
    dataset: str,
    result: Dict[str, Any],
    *,
    started_at: str,
    finished_at: str,
    environment: Optional[str] = None,
    source: str = "client",
    contract_version: Optional[str] = None,
) -> Dict[str, Any]:
    """The ``RunLogIngest`` body for one check, as the pipeline sends a run.

    ``status`` is ``failed`` only when nothing could be evaluated (the check itself broke); a
    rule failing is a RESULT of a successful check, carried per rule."""
    evaluated = [r for r in result["rule_results"] if r["status"] != "not_evaluated"]
    broke = bool(result["rule_results"]) and not evaluated and bool(result.get("errors"))
    n_failed = sum(1 for r in result["rule_results"] if r["status"] == "failed")
    return {
        "contract_name": dataset,
        "dataset": dataset,
        "status": "failed" if broke else "success",
        "engine": ENGINE,
        "started_at": started_at,
        "finished_at": finished_at,
        "rows_input": result.get("rows_checked"),
        "rules_evaluated": len(evaluated),
        "rules_passed": len(evaluated) - n_failed,
        "rules_failed": n_failed,
        "error_message": "; ".join(e["error"] for e in result.get("errors") or [])[:1000] or None,
        "metadata": {
            "record_type": RECORD_TYPE,
            "engine": ENGINE,
            "source": source,
            "dataset": dataset,
            "environment": environment,
            "contract_version": contract_version,
            "rule_results": result["rule_results"],
            "rules_fingerprint": result.get("rules_fingerprint"),
            "measurement_version": result.get("measurement_version"),
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# The client path: run the check where the data is, and report it (#570)
# ─────────────────────────────────────────────────────────────────────────────


class SparkExecutor:
    """``aggregate_row`` over a Spark session — Databricks and Fabric notebooks / jobs."""

    def __init__(self, spark: Any):
        self.spark = spark

    def aggregate_row(self, sql: str) -> Dict[str, Any]:
        row = self.spark.sql(sql).first()
        return row.asDict() if row is not None else {}


class DuckDBExecutor:
    """``aggregate_row`` over a DuckDB connection — local files and tests."""

    def __init__(self, con: Any):
        self.con = con

    def aggregate_row(self, sql: str) -> Dict[str, Any]:
        cur = self.con.execute(sql)
        names = [d[0] for d in cur.description]
        row = cur.fetchone()
        return dict(zip(names, row)) if row is not None else {}


def load_contract_doc(contract: Any) -> Dict[str, Any]:
    """A contract as a dict: a mapping, a YAML path, or YAML text."""
    import os

    import yaml

    if isinstance(contract, dict):
        return contract
    text = str(contract)
    if os.path.exists(text):
        with open(text, encoding="utf-8") as fh:
            text = fh.read()
    doc = yaml.safe_load(text)
    if not isinstance(doc, dict):
        raise ValueError("The contract is not a YAML mapping.")
    return doc


def dataset_name(doc: Dict[str, Any]) -> str:
    """The dataset name telemetry reports — the SAME name a pipeline run sends (``dataset``, then
    ``info.table_name``), so a check and a run of one table land in one history."""
    info = doc.get("info") or {}
    return str(doc.get("dataset") or info.get("table_name") or info.get("title") or "unknown")


def send(
    payload: Dict[str, Any],
    endpoint: str,
    api_key: Optional[str],
    *,
    timeout: float = 10.0,
    spool: Optional[Dict[str, Any]] = None,
) -> bool:
    """POST the check to the platform's run-log ingest, as a pipeline run is sent: the same
    ``X-API-Key`` header, and buffered for retry on a transient failure. Never raises."""
    import requests

    from lakelogic.core.observatory_spool import flush_spool, spool_payload

    headers = {"Content-Type": "application/json"}
    if api_key:
        headers["X-API-Key"] = api_key
    cfg = {"spool": spool or {}}
    try:
        resp = requests.post(endpoint, json=payload, headers=headers, timeout=timeout)
    except Exception:  # noqa: BLE001 — unreachable: buffer, never fail the caller's job
        spool_payload(cfg, payload)
        return False
    if resp.status_code < 300:
        flush_spool(cfg, endpoint, headers)
        return True
    if resp.status_code in (408, 429) or resp.status_code >= 500:
        spool_payload(cfg, payload)
    return False


#: Columns of the local results table (``lakelogic_quality_checks``; name from
#: ``metadata_names``, see ``results_table_name``): ONE ROW PER RULE PER CHECK, so a
#: team can query "which rules failed this week" in its own lakehouse without LakeLogic.
RESULT_COLUMNS = (
    ("check_id", "string"),
    ("checked_at", "string"),
    ("dataset", "string"),
    ("table_name", "string"),
    ("environment", "string"),
    ("source", "string"),
    ("rule_name", "string"),
    ("rule_kind", "string"),
    ("category", "string"),
    ("status", "string"),
    ("failed_rows", "bigint"),
    ("rows_evaluated", "bigint"),
    ("rows_checked", "bigint"),
    ("rules_fingerprint", "string"),
    ("contract_version", "string"),
    ("measurement_version", "bigint"),
    ("lakelogic_version", "string"),
    ("error", "string"),
    # What the check covered: "full" (every row) or "changed" (rows written since `from_version`).
    ("scope", "string"),
    ("from_version", "bigint"),
    ("to_version", "bigint"),
    ("table_id", "string"),
)


def results_table_name(schema: str, *, backend: Optional[str] = None) -> str:
    """``<schema>.lakelogic_quality_checks``: the results table's name from the ONE naming
    module (``metadata_names``), the same on every platform."""
    from lakelogic.core.metadata_names import metadata_table_name

    return f"{schema}.{metadata_table_name('quality_checks', backend=backend)}"


def result_rows(payload: Dict[str, Any], *, table: str, check_id: str) -> List[Dict[str, Any]]:
    """The ``lakelogic_quality_checks`` rows for one check: the SAME per-rule record sent to LakeLogic."""
    try:
        from lakelogic import __version__ as ll_version
    except Exception:  # noqa: BLE001
        ll_version = None
    meta = payload.get("metadata") or {}
    return [
        {
            "check_id": check_id,
            "checked_at": payload.get("finished_at"),
            "dataset": payload.get("dataset"),
            "table_name": table,
            "environment": meta.get("environment"),
            "source": meta.get("source"),
            "rule_name": r.get("name"),
            "rule_kind": r.get("kind"),
            "category": r.get("category"),
            "status": r.get("status"),
            "failed_rows": r.get("failed_rows"),
            "rows_evaluated": r.get("rows_evaluated"),
            "rows_checked": payload.get("rows_input"),
            "rules_fingerprint": meta.get("rules_fingerprint"),
            "contract_version": meta.get("contract_version"),
            "measurement_version": meta.get("measurement_version"),
            "lakelogic_version": ll_version,
            "error": payload.get("error_message"),
            "scope": meta.get("scope"),
            "from_version": meta.get("from_version"),
            "to_version": meta.get("to_version"),
            "table_id": meta.get("table_id"),
        }
        for r in meta.get("rule_results") or []
    ]


def write_results(
    rows: List[Dict[str, Any]], results_table: str, *, spark: Any = None, duckdb_con: Any = None
) -> Optional[str]:
    """Append ``rows`` to ``results_table`` (created on first write). Returns an error sentence,
    or None when written. Never raises: a results table never fails the check it records."""
    if not rows:
        return None
    try:
        if spark is not None:
            from pyspark.sql.types import LongType, StringType, StructField, StructType

            schema = StructType(
                [StructField(n, LongType() if t == "bigint" else StringType(), True) for n, t in RESULT_COLUMNS]
            )
            cols = [n for n, _ in RESULT_COLUMNS]
            data = [tuple(r.get(c) for c in cols) for r in rows]
            (
                spark.createDataFrame(data, schema)
                .write.mode("append")
                .option("mergeSchema", "true")
                .saveAsTable(results_table)
            )
            return None
        if duckdb_con is not None:
            cols_sql = ", ".join(f'"{n}" {"BIGINT" if t == "bigint" else "VARCHAR"}' for n, t in RESULT_COLUMNS)
            duckdb_con.execute(f"CREATE TABLE IF NOT EXISTS {results_table} ({cols_sql})")
            for n, t in RESULT_COLUMNS:
                duckdb_con.execute(
                    f'ALTER TABLE {results_table} ADD COLUMN IF NOT EXISTS "{n}" '
                    f"{'BIGINT' if t == 'bigint' else 'VARCHAR'}"
                )
            marks = ", ".join("?" for _ in RESULT_COLUMNS)
            duckdb_con.executemany(
                f"INSERT INTO {results_table} VALUES ({marks})", [[r.get(n) for n, _ in RESULT_COLUMNS] for r in rows]
            )
            return None
        return "no Spark session or DuckDB connection to write with"
    except Exception as exc:  # noqa: BLE001
        return f"{type(exc).__name__}: {str(exc)[:300]}"


#: A full check at least this often, even when only changed rows are due: a safety net for
#: rows changed by something that did not leave a trace in the table's change feed.
FULL_EVERY_DAYS = 7
_CHANGED = "_change_type IN ('insert', 'update_postimage')"


#: Rewrites a change feed cannot be trusted across: the next check reads every row.
_REWRITES = ("RESTORE", "CREATE OR REPLACE", "REPLACE TABLE", "CLONE")


def _table_state(executor: Any, table: str) -> Dict[str, Any]:
    """``{version, table_id, operation}`` of a Delta table — METADATA ONLY (the Delta log, never
    the data files), so it costs the same on 100 rows or 100 billion. Through the check's own
    executor: a Spark session or a remote read-only SQL warehouse. Unknown fields are None (not
    a Delta table: a full check)."""
    out: Dict[str, Any] = {"version": None, "table_id": None, "operation": None}
    try:
        row = executor.aggregate_row(f"DESCRIBE HISTORY {table} LIMIT 1") or {}
        if row.get("version") is not None:
            out.update(version=int(row["version"]), operation=str(row.get("operation") or "") or None)
    except Exception:  # noqa: BLE001 — not a Delta table
        return out
    try:
        out["table_id"] = (executor.aggregate_row(f"DESCRIBE DETAIL {table}") or {}).get("id")
    except Exception:  # noqa: BLE001 — the id only guards a recreated table
        pass
    return out


def _delta_version(executor: Any, table: str) -> Optional[int]:
    return _table_state(executor, table)["version"]


def _last_check(spark: Any, results_table: str, dataset: str) -> Optional[Dict[str, Any]]:
    """The newest check of ``dataset`` recorded in ``results_table``, and its newest full check."""
    try:
        safe = dataset.replace("'", "''")
        where = f"WHERE dataset = '{safe}' AND to_version IS NOT NULL ORDER BY to_version DESC LIMIT 200"
        try:
            rows = spark.sql(
                f"SELECT to_version, rules_fingerprint, scope, checked_at, table_id FROM {results_table} {where}"
            ).collect()
        except Exception:  # noqa: BLE001 — a results table from before table_id: added on next write
            rows = spark.sql(
                f"SELECT to_version, rules_fingerprint, scope, checked_at FROM {results_table} {where}"
            ).collect()
    except Exception:  # noqa: BLE001 — no table yet, or no history: a full check
        return None
    if not rows:
        return None
    last = rows[0].asDict()
    full = next((r.asDict() for r in rows if r["scope"] == "full"), None)
    try:
        verdicts = [
            r.asDict()
            for r in spark.sql(
                f"SELECT rule_name, rule_kind, category, status, failed_rows, rows_evaluated FROM {results_table} "
                f"WHERE dataset = '{safe}' AND to_version = {int(last['to_version'])} "
                f"AND checked_at = (SELECT MAX(checked_at) FROM {results_table} WHERE dataset = '{safe}' "
                f"AND to_version = {int(last['to_version'])})"
            ).collect()
        ]
    except Exception:  # noqa: BLE001
        verdicts = []
    return {**last, "last_full_at": (full or {}).get("checked_at"), "verdicts": verdicts}


class ResultsTableMemory:
    """Where an incremental check REMEMBERS the last check (the version it ended at, the rules it
    ran, each rule's verdict): the client's own ``lakelogic_quality_checks`` table. The platform
    passes its own memory (its database), so both paths make the SAME decision (``plan_scope``)
    from one ``check_contract`` — a memory needs only ``last(dataset)`` and ``record(...)``."""

    def __init__(self, results_table: str, *, spark: Any = None, duckdb_con: Any = None):
        self.results_table, self.spark, self.duckdb_con = results_table, spark, duckdb_con

    def last(self, dataset: str) -> Optional[Dict[str, Any]]:
        return _last_check(self.spark, self.results_table, dataset) if self.spark is not None else None

    def record(self, payload: Dict[str, Any], *, table: str) -> Optional[str]:
        """Append the check's rows; returns an error text, or None when written."""
        import uuid

        return write_results(
            result_rows(payload, table=table, check_id=str(uuid.uuid4())),
            self.results_table,
            spark=self.spark,
            duckdb_con=self.duckdb_con,
        )


def memory_from_payload(payload: Dict[str, Any], previous: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """A check's memory as a plain dict (``last()``'s shape), for a store that keeps one record
    per table — the platform's schedule. ``previous`` carries ``last_full_at`` across checks."""
    meta = payload.get("metadata") or {}
    checked_at = payload.get("started_at") or payload.get("timestamp")
    full = meta.get("scope") == "full"
    return {
        "to_version": meta.get("to_version"),
        "table_id": meta.get("table_id"),
        "rules_fingerprint": meta.get("rules_fingerprint"),
        "scope": meta.get("scope"),
        "checked_at": checked_at,
        "last_full_at": checked_at if full else (previous or {}).get("last_full_at"),
        "verdicts": [
            {
                "rule_name": r.get("name"),
                "rule_kind": r.get("kind"),
                "category": r.get("category"),
                "status": r.get("status"),
                "failed_rows": r.get("failed_rows"),
                "rows_evaluated": r.get("rows_evaluated"),
            }
            for r in meta.get("rule_results") or []
        ],
    }


def plan_scope(
    *,
    last: Optional[Dict[str, Any]],
    current_version: Optional[int],
    fingerprint: Optional[str],
    now_iso: str,
    full_every_days: int = FULL_EVERY_DAYS,
    table_id: Optional[str] = None,
    operation: Optional[str] = None,
) -> Dict[str, Any]:
    """Full or changed-rows check, and why. Pure.

    full: no Delta version; no earlier check; the rules changed (old rows were never judged by
    the new rules); or the last full check is ``full_every_days`` old. skip: nothing written
    since the last check. A rule that was failing forces a full check once anything is written, so
    a recovery is reported only when the whole table passes. Otherwise changed rows from
    ``last.to_version + 1``."""
    from datetime import datetime

    if current_version is None:
        return {"scope": "full", "reason": "not a Delta table, so changes cannot be read"}
    if not last:
        return {"scope": "full", "reason": "first check"}
    # THE SAME TABLE? A version number means nothing across a drop and re-create (it restarts at
    # 0), a history that went backwards, or a restore / replace the change feed cannot span.
    if table_id and last.get("table_id") and str(last["table_id"]) != str(table_id):
        return {"scope": "full", "reason": "the table was recreated"}
    if last.get("to_version") is not None and current_version < int(last["to_version"]):
        return {"scope": "full", "reason": "the table's history was rewritten"}
    if (
        operation
        and current_version > int(last.get("to_version") or -1)
        and any(operation.upper().startswith(op) for op in _REWRITES)
    ):
        return {"scope": "full", "reason": f"the table was rewritten ({operation.lower()})"}
    if fingerprint and last.get("rules_fingerprint") and last["rules_fingerprint"] != fingerprint:
        return {"scope": "full", "reason": "the rules changed"}
    try:
        age = (datetime.fromisoformat(str(now_iso)) - datetime.fromisoformat(str(last.get("last_full_at")))).days
    except (TypeError, ValueError):
        age = full_every_days
    if age >= full_every_days:
        return {"scope": "full", "reason": f"last full check {age} days ago"}
    prev = int(last["to_version"])
    if current_version > prev and any(v.get("status") == "failed" for v in last.get("verdicts") or []):
        # A failing rule's bad rows are older than the change: new rows passing says nothing about
        # them. Only a full check can say the table passes again (and so report a recovery).
        return {"scope": "full", "reason": "a rule was failing, so every row is checked to see if it was fixed"}
    if current_version <= prev:
        return {"scope": "skip", "reason": f"no change since version {prev}"}
    return {"scope": "changed", "from_version": prev + 1, "reason": f"rows changed since version {prev}"}


def check_contract(
    contract: Any,
    *,
    table: Optional[str] = None,
    spark: Any = None,
    duckdb_con: Any = None,
    executor: Any = None,
    endpoint: Optional[str] = None,
    api_key: Optional[str] = None,
    environment: Optional[str] = None,
    dataset: Optional[str] = None,
    results_table: Optional[str] = None,
    incremental: bool = True,
    memory: Any = None,
    source: str = "client",
) -> Dict[str, Any]:
    """Check one table against its contract's rules where the data lives, and report it.

    ``table`` is the table to read (``catalog.schema.table``; on Fabric ``schema.table``);
    default: the contract's ``info.table_name``. Pass a Spark session, a DuckDB connection or
    any ``executor``. With ``endpoint`` (or env ``LAKELOGIC_OBSERVATORY_ENDPOINT``) the result is
    sent to LakeLogic as ``source: client``; nothing but counts leaves the table.

    With ``results_table`` (or env ``LAKELOGIC_QUALITY_CHECKS_TABLE``) the same per-rule rows are
    appended to that table in the client's lakehouse — named by ``results_table_name``, e.g.
    ``<catalog>.lakelogic.lakelogic_quality_checks``.

    ``memory``: where the last check is remembered (``ResultsTableMemory``, or the platform's
    own store). Default: the results table, when one is named. With a memory and a Delta table
    the check is INCREMENTAL (``plan_scope``); without one, every row is checked.

    Returns ``{"result", "payload", "sent", "results_table", "results_table_error", "scope"}``;
    ``result``/``payload`` are None only when nothing changed and nothing was remembered."""
    import os
    from datetime import datetime, timezone

    doc = load_contract_doc(contract)
    if executor is None:
        if spark is not None:
            executor = SparkExecutor(spark)
        elif duckdb_con is not None:
            executor = DuckDBExecutor(duckdb_con)
        else:
            raise ValueError("Pass a Spark session, a DuckDB connection or an executor.")
    target = table or str((doc.get("info") or {}).get("table_name") or "")
    if not target:
        raise ValueError("Name the table to check (the contract has no info.table_name).")
    started = datetime.now(timezone.utc).isoformat()
    name = dataset or dataset_name(doc)
    if "{" in name and not isinstance(contract, dict) and os.path.exists(str(contract)):
        import re as _re

        name = _re.sub(r"_v\d+(\.\d+)*$", "", os.path.splitext(os.path.basename(str(contract)))[0])
    results_table = results_table or os.environ.get("LAKELOGIC_QUALITY_CHECKS_TABLE")

    if memory is None and results_table:
        memory = ResultsTableMemory(results_table, spark=spark, duckdb_con=duckdb_con)

    # INCREMENTAL: a Delta version (read through the executor) + a memory of the last check.
    from lakelogic.core.run_log import rules_fingerprint as _fp

    scope: Dict[str, Any] = {"scope": "full", "reason": "nothing remembers the last check"}
    state = _table_state(executor, target)
    version = state["version"]
    last = memory.last(name) if (incremental and memory is not None) else None
    if incremental and memory is not None:
        scope = plan_scope(
            last=last,
            current_version=version,
            fingerprint=_fp(_row_rules(doc), _dataset_rules(doc)),
            now_iso=started,
            table_id=state["table_id"],
            operation=state["operation"],
        )
    carried = None
    if scope["scope"] == "skip":
        # NOTHING WAS WRITTEN, SO THE LAST VERDICT STILL HOLDS. Sending nothing would read as a
        # missed check (Stale) on a quiet, healthy table; this says "unchanged" and carries each
        # rule's verdict forward from the version it was judged at, with 0 rows read.
        last = last or {}
        carried = [
            {
                "name": v["rule_name"],
                "kind": v.get("rule_kind"),
                "category": v.get("category"),
                "status": v.get("status"),
                "failed_rows": v.get("failed_rows"),
                "rows_evaluated": v.get("rows_evaluated"),
            }
            for v in last.get("verdicts") or []
        ]
        if not carried:
            return {
                "result": None,
                "payload": None,
                "sent": False,
                "results_table": None,
                "results_table_error": None,
                "scope": scope,
            }
        scope = {"scope": "unchanged", "reason": scope["reason"], "from_version": version}
    read_from = target
    if scope["scope"] == "unchanged":
        from lakelogic.core.run_log import MEASUREMENT_VERSION as _MV
        from lakelogic.core.run_log import rules_fingerprint as _fp2

        result = {
            "rule_results": carried,
            "rows_checked": 0,
            "errors": [],
            "rules_fingerprint": _fp2(_row_rules(doc), _dataset_rules(doc)),
            "measurement_version": _MV,
        }
    if scope["scope"] == "changed":
        read_from = (
            f"(SELECT * FROM table_changes('{target}', {scope['from_version']}, {version}) "
            f"WHERE {_CHANGED}) AS _changed"
        )
    if scope["scope"] != "unchanged":
        result = check_rules(executor, read_from, doc, dataset_table=target)
    if (
        scope["scope"] == "changed"
        and result["rule_results"]
        and all(r["status"] == "not_evaluated" for r in result["rule_results"] if r["kind"] == "row")
    ):
        # The change feed is off for this table: say so and check every row instead.
        scope = {"scope": "full", "reason": "change data feed is not enabled on the table"}
        read_from = target
        result = check_rules(executor, read_from, doc)
    payload = run_log_payload(
        name,
        result,
        started_at=started,
        finished_at=datetime.now(timezone.utc).isoformat(),
        environment=environment,
        source=source,
        contract_version=str(doc.get("version") or "") or None,
    )
    payload["metadata"].update(
        scope=scope["scope"],
        scope_reason=scope.get("reason"),
        from_version=scope.get("from_version") if scope["scope"] in ("changed", "unchanged") else None,
        to_version=version,
        table_id=state["table_id"],
    )
    endpoint = endpoint or os.environ.get("LAKELOGIC_OBSERVATORY_ENDPOINT")
    api_key = api_key or os.environ.get("LAKELOGIC_API_KEY")
    sent = send(payload, endpoint, api_key) if endpoint else False
    # Remember this check (the client's ``lakelogic_quality_checks``, or the platform's store).
    written, write_error = None, None
    if memory is not None:
        write_error = memory.record(payload, table=target)
        written = getattr(memory, "results_table", None) if not write_error else None
    return {
        "result": result,
        "payload": payload,
        "sent": sent,
        "results_table": written,
        "results_table_error": write_error,
        "scope": scope,
    }
