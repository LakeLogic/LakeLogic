"""The erasure queue ``_lakelogic_erasure_requests``: people insert rows, LakeLogic erases and closes them.

Local backends (duckdb, sqlite) stand in for the Delta table; the passes themselves are
stubbed, since what is under test is the queue: which rows are read, how each one moves,
and that the evidence each pass writes names its request.
"""

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from lakelogic.core import erasure_requests as er
from lakelogic.core.evidence_tables import resolve_evidence_target
from lakelogic.pipeline.runner import LakehousePipeline


def _md(tmp_path, backend="duckdb", system="stripe"):
    return {
        "run_log_table": "payments._pipeline_run_log",
        "run_log_backend": backend,
        "run_log_database": str(tmp_path / f"log.{backend}"),
        "domain": "payments",
        "system": system,
    }


def _insert(md, rows):
    backend, target, database = er.resolve_requests_target(md)
    er.ensure_requests_table(md)
    con = er._connect(backend, database)
    try:
        for r in rows:
            rec = {
                "requested_at": datetime(2026, 9, 1, tzinfo=timezone.utc),
                "status": "pending",
                "framework": "gdpr",
                "subject_column": "customer_id",
                **r,
            }
            if backend == "sqlite":
                rec["requested_at"] = rec["requested_at"].isoformat()
            cols = ", ".join(rec)
            con.execute(f"INSERT INTO {target} ({cols}) VALUES ({', '.join(['?'] * len(rec))})", list(rec.values()))
        if backend == "sqlite":
            con.commit()
    finally:
        con.close()


def _statuses(md):
    backend, target, database = er.resolve_requests_target(md)
    con = er._connect(backend, database)
    try:
        return {r[0]: (r[1], r[2]) for r in con.execute(f"SELECT request_id, status, run_id FROM {target}").fetchall()}
    finally:
        con.close()


# ── Naming and resolution ──────────────────────────────────────────────────


def test_the_table_sits_beside_the_run_log_in_the_domain_schema():
    md = {"run_log_table": "`cat`.payments._pipeline_run_log", "run_log_backend": "spark"}
    assert er.resolve_requests_target(md) == ("spark", "`cat`.payments._lakelogic_erasure_requests", None)
    assert er.resolve_requests_target(md) == resolve_evidence_target("erasure_requests", md)
    assert er.resolve_requests_target({}) is None


@pytest.mark.parametrize("backend", ["duckdb", "sqlite"])
def test_ensure_is_idempotent_and_has_the_owned_schema(tmp_path, backend):
    md = _md(tmp_path, backend)
    assert er.ensure_requests_table(md) == er.ensure_requests_table(md)
    backend, target, database = er.resolve_requests_target(md)
    con = er._connect(backend, database)
    try:
        cols = [d[0] for d in con.execute(f"SELECT * FROM {target} LIMIT 0").description]
    finally:
        con.close()
    assert cols == list(er.REQUEST_COLUMNS)
    assert target.endswith("_lakelogic_erasure_requests")


# ── Reading and marking ────────────────────────────────────────────────────


@pytest.mark.parametrize("backend", ["duckdb", "sqlite"])
def test_only_open_requests_in_scope_are_read(tmp_path, backend):
    md = _md(tmp_path, backend)
    _insert(md, [
        {"request_id": "R1", "subject_id": "c1"},
        {"request_id": "R2", "subject_id": "c2", "status": "completed"},
        {"request_id": "R3", "subject_id": "c3", "status": "dry_run"},
        {"request_id": "R4", "subject_id": "c4", "framework": "hipaa", "subject_column": "patient_id"},
        {"request_id": "R5", "subject_id": "c5", "system": "other"},
        {"request_id": "R6", "subject_id": "c6", "system": "stripe"},
        {"request_id": "R7", "subject_id": "c7", "status": "failed"},
    ])
    got = [r["request_id"] for r in er.read_open_requests(md, frameworks=["gdpr"], system="stripe")]
    assert got == ["R1", "R3", "R6"]
    both = {r["request_id"] for r in er.read_open_requests(md, system="stripe")}
    assert both == {"R1", "R3", "R4", "R6"}


@pytest.mark.parametrize("backend", ["duckdb", "sqlite"])
def test_marking_moves_only_open_requests(tmp_path, backend):
    md = _md(tmp_path, backend)
    _insert(md, [
        {"request_id": "R1", "subject_id": "c1"},
        {"request_id": "R2", "subject_id": "c2", "status": "completed", "run_id": "old"},
    ])
    er.mark_requests(md, {"R1": "completed", "R2": "failed"}, run_id="run-9")
    assert _statuses(md) == {"R1": ("completed", "run-9"), "R2": ("completed", "old")}
    with pytest.raises(ValueError):
        er.mark_requests(md, {"R1": "done"}, run_id="x")


# ── The pipeline entry point ───────────────────────────────────────────────


def _pipeline(md, *, fail_on=None, raise_on=None):
    p = LakehousePipeline.__new__(LakehousePipeline)
    p.run_id = "run-1"
    p.engine = "duckdb"
    p.spark = None
    contract = SimpleNamespace(entity="silver_charges", contract_dict={"metadata": md})
    p.registry = SimpleNamespace(domain="payments", system="stripe", get_active_contracts=lambda: [contract])
    p.calls = []

    def _pass(framework):
        def run(active, col, ids, strategy, salt, dry_run, partition_filter=None, case_ref=None):
            p.calls.append((framework, col, list(ids), dry_run, case_ref))
            if raise_on == case_ref:
                raise RuntimeError("boom")
            return {"tables": 1, "failed": int(fail_on == case_ref)}

        return run

    p._execute_gdpr_pass = _pass("gdpr")
    p._execute_hipaa_pass = _pass("hipaa")
    return p


def test_each_request_runs_its_own_pass_and_is_closed(tmp_path):
    md = _md(tmp_path)
    _insert(md, [
        {"request_id": "R1", "subject_id": "c1"},
        {"request_id": "R2", "subject_id": "p2", "framework": "hipaa", "subject_column": "patient_id"},
        {"request_id": "R3", "subject_id": "c3"},
        {"request_id": "R4", "subject_id": "c4"},
    ])
    p = _pipeline(md, fail_on="R3", raise_on="R4")
    out = p.process_erasure_requests(dry_run=False)
    assert out == {"R1": "completed", "R2": "completed", "R3": "failed", "R4": "failed"}
    assert ("gdpr", "customer_id", ["c1"], False, "R1") in p.calls
    assert ("hipaa", "patient_id", ["p2"], False, "R2") in p.calls
    assert _statuses(md)["R1"] == ("completed", "run-1")
    # Closed requests are not taken again.
    assert _pipeline(md).process_erasure_requests(dry_run=False) == {}


def test_a_dry_run_never_consumes_a_request(tmp_path):
    md = _md(tmp_path)
    _insert(md, [{"request_id": "R1", "subject_id": "c1"}])
    p = _pipeline(md)
    assert p.process_erasure_requests(dry_run=True) == {"R1": "dry_run"}
    assert p.calls[0][3] is True
    assert _statuses(md)["R1"] == ("dry_run", "run-1")
    # Still open: a later real run erases it.
    assert _pipeline(md).process_erasure_requests(dry_run=False) == {"R1": "completed"}


def test_frameworks_filter_leaves_other_requests_pending(tmp_path):
    md = _md(tmp_path)
    _insert(md, [
        {"request_id": "R1", "subject_id": "c1"},
        {"request_id": "R2", "subject_id": "p2", "framework": "hipaa", "subject_column": "patient_id"},
    ])
    assert _pipeline(md).process_erasure_requests(dry_run=False, frameworks=["hipaa"]) == {"R2": "completed"}
    assert _statuses(md)["R1"][0] == "pending"


def test_subject_ids_are_never_logged(tmp_path):
    from loguru import logger

    md = _md(tmp_path)
    _insert(md, [{"request_id": "R1", "subject_id": "secret-subject-42"}])
    lines = []
    sink = logger.add(lambda m: lines.append(str(m)), level="DEBUG")
    try:
        _pipeline(md).process_erasure_requests(dry_run=False)
    finally:
        logger.remove(sink)
    assert lines and not any("secret-subject-42" in line for line in lines)


# ── Evidence names the request ─────────────────────────────────────────────


def test_hipaa_evidence_reason_carries_the_request_id(monkeypatch):
    """The HIPAA pass passes case_ref through to the evidence row's ``reason``."""
    p = LakehousePipeline.__new__(LakehousePipeline)
    p.run_id = "run-1"
    p.engine = "spark"
    p.registry = SimpleNamespace(domain="payments", system="stripe", compliance={})
    written = []
    monkeypatch.setattr(p, "_write_erasure_evidence_row", lambda *a, **k: written.append(k), raising=False)
    monkeypatch.setattr(p, "_apply_path_based_erasure", lambda **k: 1, raising=False)

    import lakelogic.core.hipaa as hipaa
    import lakelogic.pipeline.runner as runner

    field = SimpleNamespace(name="patient_id")
    dc = SimpleNamespace(model=SimpleNamespace(fields=[field]))
    monkeypatch.setattr(runner, "DataContract", lambda **k: dc)
    monkeypatch.setattr(hipaa, "_get_phi_column_names", lambda d: ["name"])
    monkeypatch.setattr(hipaa, "generate_hipaa_erasure_report", lambda *a, **k: {})
    monkeypatch.setattr(runner, "RemoteObserver", lambda: SimpleNamespace(report=lambda r: None), raising=False)

    c = SimpleNamespace(entity="silver_patients", layer="silver",
                        contract_dict={"materialization": {"target_path": "/tmp/x"}})
    out = p._execute_hipaa_pass([c], "patient_id", ["p1"], "nullify", "", False, case_ref="REQ-7")
    assert written and written[0]["reason"] == "REQ-7"
    assert out == {"tables": 1, "failed": 0}


def test_gdpr_evidence_reason_carries_the_request_id(monkeypatch):
    from lakelogic.core import evidence_tables

    p = LakehousePipeline.__new__(LakehousePipeline)
    p.run_id = "run-1"
    p.engine = "spark"
    p.registry = SimpleNamespace(domain="payments", system="stripe")
    rows = []
    monkeypatch.setattr(evidence_tables, "write_evidence_rows",
                        lambda kind, r, md, engine_name=None: rows.extend(r))
    dc = SimpleNamespace(metadata={"run_log_table": "`cat`.payments._pipeline_run_log"}, dataset="d",
                         info=SimpleNamespace(title="t", version="1", table_name=None))
    p._record_gdpr_evidence(
        SimpleNamespace(entity="e", layer="silver", contract_dict={}), dc, None, [],
        subject_col="customer_id", subject_ids=["c1"], strategy="nullify", affected=1, dry_run=False,
        partition_filter=None, pii_cols=["email"], case_ref="REQ-8", status="completed",
    )
    assert rows[0]["reason"] == "REQ-8"
