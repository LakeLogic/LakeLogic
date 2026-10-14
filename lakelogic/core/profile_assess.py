"""Profile a table AND assess it: statistics, checks and findings in one call.

``profile_table`` measures; this adds what a person acts on. It reads the table (or the sample
the settings ask for, see :mod:`lakelogic.core.profile_scope`), runs the checks on exactly the
rows it read, and returns the findings (:mod:`lakelogic.core.profile_findings`). Only counts,
summaries and values seen at least ``MIN_VALUE_COUNT`` times in non-sensitive columns leave the
engine -- never rows.

    from lakelogic.core.profile_assess import assess_table, assess_tables
    entry = assess_table("main.sales.orders", spark, contract="contracts/orders.yaml",
                         settings={"mode": "sample", "sample_unit": "percent", "sample_percent": 10})
    run = assess_tables(["main.sales.orders", ...], spark, settings={"paths": ["main.sales"]})

The LakeLogic service calls the same functions, so a run in your own platform and a run in the
service produce the same findings.
"""
from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

from lakelogic.core import profile as core
from lakelogic.core import profile_findings as pf
from lakelogic.core import profile_scope as ps

_TEXT_TYPE = {"databricks": "STRING", "spark": "STRING", "tsql": "NVARCHAR(4000)"}


def load_contract(contract: Any) -> Optional[Dict[str, Any]]:
    """A contract as a dict: a path to its YAML, a dict, or a model object."""
    if contract is None:
        return None
    if isinstance(contract, (str, Path)):
        from lakelogic.core.contract_yaml import load_contract_yaml

        with open(contract, encoding="utf-8") as fh:
            return load_contract_yaml(fh) or {}
    if not isinstance(contract, dict) and hasattr(contract, "model_dump"):
        return contract.model_dump()
    return dict(contract)


def contract_name(doc: Optional[Dict[str, Any]]) -> Optional[str]:
    if not doc:
        return None
    info = doc.get("info") or {}
    return info.get("title") or doc.get("name") or info.get("name") or doc.get("dataset")


class _OnRelation(core.SqlExecutor):
    """Core's executor, pointed at a sample subquery: columns come from the real table."""

    def __init__(self, ex: core.SqlExecutor, table: str):
        self._ex, self._table = ex, table
        self.dialect = ex.dialect

    def query(self, sql):
        return self._ex.query(sql)

    def aggregate_row(self, sql):
        return self._ex.aggregate_row(sql)

    def columns(self, _t):
        return self._ex.columns(self._table)


def relation_for(ex: core.SqlExecutor, from_sql: str) -> pf.Relation:
    """The checks' view of ``from_sql`` on ``ex``: its quoting, its text type, its row fetch."""
    d = core._dialect(ex.dialect)
    return pf.Relation(from_sql, d.quote, lambda sql, limit: ex.rows(sql, limit),
                       text_type=_TEXT_TYPE.get(d.name, "VARCHAR"))


def findings_entry(name: str, doc: Dict[str, Any], rel: pf.Relation, *, contract: Optional[Dict[str, Any]],
                   sampled: bool, entity: Optional[str] = None) -> Dict[str, Any]:
    """Run the checks on ``rel`` (the rows ``doc`` describes), add the findings, hide text ranges.

    The step every route shares -- a warehouse table, a sampled subquery, files read with
    DuckDB -- once the profile document and the relation it read exist."""
    rules = pf.contract_rules(contract) if contract is not None else None
    checks = pf.run_checks(rel, entity or name, doc, rules)
    ref = {"name": contract_name(contract)} if contract is not None else None
    findings = pf.findings_for(name, doc, checks, ref, rules, sampled=sampled)
    domain = ((contract or {}).get("info") or {}).get("domain") if contract else None
    for f in findings:
        f["domain"] = domain
    pf.hide_text_ranges(doc)
    return {"name": name, "status": "profiled", "error": None, "profile": doc, "contract": ref,
            "checks": checks, "findings": findings, "finding_counts": pf.count_by_kind(findings),
            "metrics": ps.table_metrics(doc)}


def assess_table(table: str, connection: Any, *, settings: Optional[Dict[str, Any]] = None,
                 contract: Any = None, sensitive_columns: Optional[Sequence[str]] = None,
                 dialect: Optional[str] = None) -> Dict[str, Any]:
    """Profile ``table`` (fully or sampled per ``settings``), check it, and return its findings."""
    s = ps.normalize_settings(settings)
    ex = core.executor_for(connection, dialect)
    cdoc = load_contract(contract)
    pii = list(sensitive_columns or []) + (pf.contract_rules(cdoc)["pii"] if cdoc else [])
    d = core._dialect(ex.dialect)
    quoted = core._quote_table(table, d)
    sampled = s["mode"] == ps.MODE_SAMPLE
    if sampled:
        schema = ex.columns(table)
        sql, selection = ps.sample_sql(quoted, schema, s, d.quote, dialect=d.name)
        from_sql = f"({sql}) AS _s"
        doc = core.profile_table(from_sql, _OnRelation(ex, table), contract=cdoc, sensitive_columns=pii)
        doc["sampling"] = {"method": "sample", "rows_scanned": doc.get("row_count"), "selection": selection}
        doc["source"] = {**(doc.get("source") or {}), "location": table}
    else:
        from_sql = quoted
        doc = core.profile_table(table, ex, contract=cdoc, sensitive_columns=pii)
    return findings_entry(table, doc, relation_for(ex, from_sql), contract=cdoc, sampled=sampled)


def assess_tables(every: Sequence[str], connection: Any, *, settings: Optional[Dict[str, Any]] = None,
                  contracts: Optional[Dict[str, Any]] = None,
                  contract_for: Optional[Callable[[str], Any]] = None,
                  time_budget_seconds: Optional[float] = None, max_tables: int = ps.MAX_TABLES,
                  clock: Callable[[], float] = time.monotonic,
                  on_progress: Optional[Callable[[int, int, Optional[str]], None]] = None,
                  dialect: Optional[str] = None) -> Dict[str, Any]:
    """A whole run: choose the tables (``paths`` / ``tables``), assess each, and summarise.

    One unreadable table is recorded as ``failed`` and the run goes on; once the time budget is
    spent the rest are ``skipped``. Returns ``{"settings", "tables", "summary"}`` -- the shape the
    LakeLogic service stores, so a run made here can be sent to it unchanged."""
    s = ps.normalize_settings(settings, max_tables=max_tables)
    names = ps.choose_tables(every, s, max_tables)
    ex = core.executor_for(connection, dialect)
    deadline = clock() + time_budget_seconds if time_budget_seconds else None
    lookup = contract_for or (lambda n: (contracts or {}).get(n))
    out: List[Dict[str, Any]] = []
    for i, name in enumerate(names):
        if deadline is not None and clock() >= deadline:
            out.append({"name": name, "status": "skipped", "error": "Not profiled: the run's time budget was reached."})
            continue
        if on_progress:
            on_progress(i, len(names), name)
        try:
            out.append(assess_table(name, ex, settings=s, contract=lookup(name)))
        except Exception as exc:  # noqa: BLE001 -- one unreadable table must not end the run
            out.append({"name": name, "status": "failed", "error": f"{type(exc).__name__}: {str(exc)[:300]}"})
    if on_progress:
        on_progress(len(names), len(names), None)
    summary = ps.summarize(out, sources_found=len(every))
    summary["settings"] = {**s, "label": ps.sampling_label(s)}
    summary["tables_chosen"] = len(names)
    return {"settings": s, "tables": ps.order_worst_first(out), "summary": summary}


# ─────────────────────────────────────────────────────────────────────────────
# Files: a landing folder (local, or any path DuckDB can read: s3://, gs://, abfss://)
# ─────────────────────────────────────────────────────────────────────────────

#: A full scan of a folder still stops somewhere, so one huge folder cannot stall the run;
#: the result's ``sampling`` says what was actually read.
FULL_MAX_FILES = 2_000
FULL_MAX_BYTES = 2 * 1024 * 1024 * 1024
SAMPLE_MAX_FILES = 10
SAMPLE_MAX_BYTES = 64 * 1024 * 1024


def folder_format(folder: str) -> str:
    """parquet, json or csv -- by the files a local folder holds."""
    import os

    names = [f.lower() for _d, _s, fs in os.walk(folder) for f in fs]
    if any(n.endswith(".parquet") for n in names):
        return "parquet"
    if any(n.endswith((".json", ".jsonl", ".ndjson")) for n in names):
        return "json"
    return "csv"


def assess_files(location: str, *, name: Optional[str] = None, fmt: Optional[str] = None,
                 settings: Optional[Dict[str, Any]] = None, contract: Any = None,
                 sensitive_columns: Optional[Sequence[str]] = None,
                 max_files: Optional[int] = None, max_bytes: Optional[int] = None) -> Dict[str, Any]:
    """Profile a folder of files, then check the SAME rows the statistics came from.

    A full scan reads up to ``FULL_MAX_FILES`` / ``FULL_MAX_BYTES``; a sample reads the newest
    ``SAMPLE_MAX_FILES`` files (``SAMPLE_MAX_BYTES``), then draws the rows or percentage asked for.
    File checks (encoding, headers, ragged rows) run on full scans only."""
    import duckdb

    s = ps.normalize_settings(settings)
    cdoc = load_contract(contract)
    pii = list(sensitive_columns or []) + (pf.contract_rules(cdoc)["pii"] if cdoc else [])
    full = s["mode"] == ps.MODE_FULL
    fmt = fmt or folder_format(location)
    loc = location.replace("\\", "/").rstrip("/")
    if fmt == "parquet":
        doc = core.profile_parquet(location, sensitive_columns=pii)
        reader = f"read_parquet('{loc}/**/*.parquet', union_by_name=true)"
    else:
        doc = core.profile_files(
            location, fmt=fmt,
            max_files=max_files or (FULL_MAX_FILES if full else SAMPLE_MAX_FILES),
            max_bytes=max_bytes or (FULL_MAX_BYTES if full else SAMPLE_MAX_BYTES),
            sensitive_columns=pii)
        files = [str(f["path"]).replace("\\", "/") for f in ((doc.get("file_checks") or {}).get("files") or [])
                 if not f.get("empty") and not f.get("truncated")]
        if not files:
            pf.hide_text_ranges(doc)
            ref = {"name": contract_name(cdoc)} if cdoc is not None else None
            return {"name": name or location, "status": "profiled", "error": None, "profile": doc,
                    "contract": ref, "metrics": ps.table_metrics(doc),
                    "checks": {"errors": [{"check": "all", "error": "No complete file was read."}]},
                    "findings": [], "finding_counts": pf.count_by_kind([])}
        paths = "[" + ", ".join("'" + p.replace("'", "''") + "'" for p in files) + "]"
        reader = (f"read_json_auto({paths}, union_by_name=true, ignore_errors=true)" if fmt == "json"
                  else f"read_csv({paths}, union_by_name=true, ignore_errors=true)")
    con = duckdb.connect()
    try:
        con.execute(f"CREATE VIEW _src AS SELECT * FROM {reader}")
        rel_name = "_src"
        if not full:
            schema = [(r[0], str(r[1])) for r in con.execute("DESCRIBE _src").fetchall()]
            sql, selection = ps.sample_sql("_src", schema, s, core._dq, dialect="duckdb")
            con.execute(f"CREATE TABLE _sample AS {sql}")
            total = con.execute("SELECT COUNT(*) FROM _src").fetchone()[0]
            sdoc = core.profile_table("_sample", core.DuckDBExecutor(con), sensitive_columns=pii)
            sdoc["source"] = doc.get("source")
            sdoc["file_checks"] = None
            sdoc["sampling"] = {"method": "sample", "rows_scanned": sdoc.get("row_count"),
                                "rows_available": int(total), "selection": selection,
                                "files_scanned": (doc.get("sampling") or {}).get("files_scanned"),
                                "file_checks": "run on full scans only"}
            doc, rel_name = sdoc, "_sample"
        rel = pf.Relation(rel_name, core._dq, lambda sql, limit: con.execute(sql).fetchmany(limit))
        return findings_entry(name or location, doc, rel, contract=cdoc, sampled=not full)
    finally:
        con.close()
